package bloomsearch

// The flush half of the write path: the FIFO flush worker that writes
// buffered partitions out as data blocks and commits file metadata.

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// compressionEncoders holds compression-related objects. Encoders come from
// the package codec pools (see codec_pool.go): call finalizeCompression to
// flush the stream, then release to recycle the encoders.
type compressionEncoders struct {
	writer        io.Writer
	zstdEncoder   *zstd.Encoder
	zstdLevel     int
	snappyEncoder *snappy.Writer
}

// createCompressionWriter creates appropriate compression writer based on configuration
func (b *BloomSearchEngine) createCompressionWriter(dest io.Writer) (*compressionEncoders, error) {
	encoders := &compressionEncoders{}

	switch b.config.RowDataCompression {
	case CompressionZstd:
		zstdEncoder, err := getPooledZstdEncoder(dest, b.config.ZstdCompressionLevel)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}
		encoders.zstdEncoder = zstdEncoder
		encoders.zstdLevel = b.config.ZstdCompressionLevel
		encoders.writer = zstdEncoder
	case CompressionSnappy:
		encoders.snappyEncoder = getPooledSnappyWriter(dest)
		encoders.writer = encoders.snappyEncoder
	default:
		encoders.writer = dest
	}

	return encoders, nil
}

// finalizeCompression closes any compression encoders and finalizes compression
func (e *compressionEncoders) finalizeCompression() error {
	if e.zstdEncoder != nil {
		if err := e.zstdEncoder.Close(); err != nil {
			return fmt.Errorf("failed to close zstd encoder: %w", err)
		}
	}
	if e.snappyEncoder != nil {
		if err := e.snappyEncoder.Close(); err != nil {
			return fmt.Errorf("failed to close snappy encoder: %w", err)
		}
	}
	return nil
}

// release returns the encoders to the codec pools. Call only after
// finalizeCompression succeeded; encoders in an uncertain state (failed
// finalize, abandoned buffer) are simply dropped for GC by not releasing.
func (e *compressionEncoders) release() {
	if e.zstdEncoder != nil {
		putPooledZstdEncoder(e.zstdEncoder, e.zstdLevel)
		e.zstdEncoder = nil
	}
	if e.snappyEncoder != nil {
		putPooledSnappyWriter(e.snappyEncoder)
		e.snappyEncoder = nil
	}
	e.writer = nil
}

type flushRequest struct {
	partitionBuffers map[string]*partitionBuffer
	doneChans        []chan error
}

func (b *BloomSearchEngine) flushWorker() {
	defer b.wg.Done()

	shuttingDown := false
	for {
		if !shuttingDown {
			select {
			case <-b.ctx.Done():
				shuttingDown = true
			case flushReq := <-b.flushChan:
				b.handleFlush(b.flushCtx, flushReq)
			}
			continue
		}

		select {
		case flushReq := <-b.flushChan:
			b.handleFlush(b.flushCtx, flushReq)
		case <-b.ingestDone:
			// The ingest worker has exited, so every flush request it will
			// ever produce is already in the channel; drain them all.
			for {
				select {
				case flushReq := <-b.flushChan:
					b.handleFlush(b.flushCtx, flushReq)
				default:
					b.logger.Debug("flush worker stopped")
					return
				}
			}
		}
	}
}

// abortFileWriter discards a partially written file so it can never become
// visible: writers implementing Abort discard without publishing (Close on a
// rename-on-close writer would publish a corrupt file); other writers are
// closed unless a Close was already attempted. The pointer is then
// tombstoned so the store forgets whatever was reserved for it. Cleanup is
// best effort — the caller's original error is what gets reported.
func (b *BloomSearchEngine) abortFileWriter(ctx context.Context, writer io.WriteCloser, filePointerBytes []byte, closeAttempted bool) {
	if aborter, ok := writer.(interface{ Abort() error }); ok {
		aborter.Abort()
	} else if !closeAttempted {
		writer.Close()
	}
	b.dataStore.TombstoneFile(ctx, filePointerBytes)
}

// handleFlush writes one file from a flush request and acks its done
// channels. A request without partition buffers is ack-only: it exists purely
// to order a Flush ack behind earlier flush work. On any failure after
// CreateFile the writer is aborted and the pointer tombstoned before the
// error is delivered, so no partial file stays visible or leaks a handle.
func (b *BloomSearchEngine) handleFlush(ctx context.Context, flushReq flushRequest) {
	// Once the shutdown deadline has aborted flush work, queued requests must
	// not start any store work: a ctx-ignoring store would happily keep
	// creating files after Stop already returned. Report the abandonment to
	// every waiter instead (best effort — ctx is already canceled, so only
	// ready channels receive it).
	if err := ctx.Err(); err != nil {
		b.logger.Warn("flush abandoned: shutdown deadline expired before the flush could run",
			"partitions", len(flushReq.partitionBuffers), "waiters", len(flushReq.doneChans))
		sendToChannelsWithContext(ctx, flushReq.doneChans, fmt.Errorf("flush abandoned: %w", err))
		return
	}

	if len(flushReq.partitionBuffers) == 0 {
		sendToChannelsWithContext(ctx, flushReq.doneChans, nil)
		return
	}

	fileMetadata := FileMetadata{
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		DataBlocks:             make([]DataBlockMetadata, 0),
	}

	// Stream write to data store
	writer, filePointerBytes, err := b.dataStore.CreateFile(ctx)
	if err != nil {
		// Report to the logger as well as the done channels: async ingesters
		// with nil done channels would otherwise never see flush failures.
		b.logger.Warn("flush failed to create file", "error", err)
		sendToChannelsWithContext(ctx, flushReq.doneChans, fmt.Errorf("failed to create file: %w", err))
		return
	}

	// fail aborts the partial file and reports err to every waiter.
	fail := func(err error, closeAttempted bool) {
		b.abortFileWriter(ctx, writer, filePointerBytes, closeAttempted)
		sendToChannelsWithContext(ctx, flushReq.doneChans, err)
	}

	currentOffset := 0

	// The file-level filters are built from the union of the blocks' entry
	// sets, so they too are sized for exact distinct counts.
	fileEntries := newBloomEntrySets()

	// Block filter sections are buffered here while the row data streams out,
	// then written as one contiguous region after the last block — the layout
	// that lets a query read every block's filters in one request. See
	// blockFilterRegionWriter for the memory this costs.
	var filterRegion blockFilterRegionWriter

	// For each partition buffer, write the data block's row data to the data
	// store and buffer its filter section.
	for _, partitionBuffer := range flushReq.partitionBuffers {
		// Finalize compression encoders before writing, then recycle them (the
		// stream is complete; the compressed bytes live in the buffer).
		var compressedData []byte
		if err := partitionBuffer.compressionEncoders.finalizeCompression(); err != nil {
			fail(fmt.Errorf("failed to finalize compression: %w", err), false)
			return
		}
		partitionBuffer.compressionEncoders.release()
		compressedData = partitionBuffer.buffer.Bytes()

		// Build the block's filters right-sized from the measured distinct
		// entry counts and buffer them for the file's filter region.
		blockFilters := partitionBuffer.entries.buildFilters(b.config.BloomFalsePositiveRate)
		filterSection, err := encodeFilterSection(&blockFilters)
		if err != nil {
			fail(fmt.Errorf("failed to encode bloom filters: %w", err), false)
			return
		}
		filterOffset, filterSize := filterRegion.add(filterSection)

		// Calculate hash of compressed row data (CRC32C)
		rowDataHash := crc32.Checksum(compressedData, crc32cTable)

		// Write the row data buffer
		if _, err := writer.Write(compressedData); err != nil {
			fail(fmt.Errorf("failed to write data block: %w", err), false)
			return
		}

		partitionBuffer.entries.unionInto(fileEntries)

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:   partitionBuffer.partitionID,
			Rows:          partitionBuffer.rowCount,
			RowDataOffset: currentOffset,
			RowDataSize:   len(compressedData),
			// Region-relative until filterRegion.finish rebases it below.
			BloomFilterOffset:      filterOffset,
			BloomFilterSize:        filterSize,
			MinMaxIndexes:          partitionBuffer.minMaxIndexes,
			Compression:            b.config.RowDataCompression,
			UncompressedSize:       partitionBuffer.uncompressedSize,
			RowDataHash:            rowDataHash,
			HasRowDataHash:         true,
			BloomEntryCounts:       partitionBuffer.entries.counts(),
			BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		})

		currentOffset += len(compressedData)
	}

	// Every block's row data is out: write their filter sections as one
	// contiguous region and rebase the blocks' filter offsets onto it.
	regionSize, err := filterRegion.finish(writer, currentOffset, fileMetadata.DataBlocks)
	if err != nil {
		fail(err, false)
		return
	}
	fileMetadata.BlockFilterRegionOffset = currentOffset
	fileMetadata.BlockFilterRegionSize = regionSize

	fileMetadata.BloomFilters = fileEntries.buildFilters(b.config.BloomFalsePositiveRate)
	fileMetadata.BloomEntryCounts = fileEntries.counts()

	// Write final metadata to data store and footer
	if err := WriteFileFooter(writer, &fileMetadata); err != nil {
		fail(fmt.Errorf("failed to write file metadata and footer: %w", err), false)
		return
	}

	if err := writer.Close(); err != nil {
		fail(fmt.Errorf("failed to close file writer: %w", err), true)
		return
	}

	if err := b.metaStore.Update(ctx, []WriteOperation{
		{
			FileMetadata:     &fileMetadata,
			FilePointerBytes: filePointerBytes,
		},
	}, nil); err != nil {
		// The file is fully written and published but never became
		// referenced; tombstone the orphan. The writer is already closed, so
		// no abort.
		b.dataStore.TombstoneFile(ctx, filePointerBytes)
		sendToChannelsWithContext(ctx, flushReq.doneChans, fmt.Errorf("failed to store file metadata: %w", err))
		return
	}

	sendToChannelsWithContext(ctx, flushReq.doneChans, nil)
}
