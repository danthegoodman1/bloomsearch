package bloomsearch

// The ingest half of the write path: the single-threaded ingest actor, row
// indexing into bloom entry sets, and buffering up to the flush trigger.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/tidwall/gjson"
)

// bloomEntrySets accumulates the distinct bloom entries — field paths,
// tokens, and field::token pairs — of a set of rows. Ingest and merge collect
// entries into these sets instead of inserting into filters directly, so that
// filters can be built right-sized from exact distinct counts once the rows
// are known (bloom hashing then happens on the flush/merge path, off the
// ingest actor).
type bloomEntrySets struct {
	fields      map[string]struct{}
	tokens      map[string]struct{}
	fieldTokens map[string]struct{}

	// Reused indexing scratch: the path walker's buffer, a token fold buffer,
	// and a field::token key buffer. Entry strings are only materialized when
	// an entry is absent from its set, so re-seen paths/tokens/pairs cost no
	// allocation. Not safe for concurrent use — each set has one owner (a
	// partition buffer on the ingest actor, or the merge goroutine).
	walker   pathWalker
	tokenBuf []byte
	keyBuf   []byte
}

func newBloomEntrySets() *bloomEntrySets {
	return &bloomEntrySets{
		fields:      make(map[string]struct{}),
		tokens:      make(map[string]struct{}),
		fieldTokens: make(map[string]struct{}),
	}
}

// indexRow walks a marshaled row through the shared walker and records every
// bloom entry it produces: every path (including intermediate object/array
// paths) as a field entry, leaf-value tokens as token entries, and
// exact-leaf-path::token pairs as field-token entries. The row is parsed
// through an unsafe string view of rowBytes, which is safe because rowBytes is
// never mutated; strings the view yields may be retained in the sets (they
// keep the row's backing array alive through GC, exactly as substrings of the
// previous per-row copy did).
func (s *bloomEntrySets) indexRow(rowBytes []byte, tokenizer ValueTokenizerFunc) {
	fastTokens := isBasicWhitespaceLowerTokenizer(tokenizer)
	s.walker.walk(gjson.Parse(unsafeString(rowBytes)), ".", func(path []byte, value gjson.Result, isLeaf bool) bool {
		if _, ok := s.fields[string(path)]; !ok {
			s.fields[string(path)] = struct{}{}
		}
		if !isLeaf {
			return true
		}
		text, ok := leafTokenInput(value)
		if !ok {
			return true
		}
		if fastTokens {
			// Zero-alloc equivalent of BasicWhitespaceLowerTokenizer: fold each
			// whitespace-separated word into the reused token buffer.
			forEachWord(text, func(word string) bool {
				s.tokenBuf = appendFoldedWord(s.tokenBuf[:0], word)
				if _, ok := s.tokens[string(s.tokenBuf)]; !ok {
					s.tokens[string(s.tokenBuf)] = struct{}{}
				}
				s.addFieldToken(path, unsafeString(s.tokenBuf))
				return true
			})
			return true
		}
		for _, token := range tokenizer(text) {
			if _, ok := s.tokens[token]; !ok {
				s.tokens[token] = struct{}{}
			}
			s.addFieldToken(path, token)
		}
		return true
	})
}

// addFieldToken records the field::token pair for a leaf, building the joined
// key (same layout as makeFieldTokenKey) in the reused key buffer and only
// materializing it when absent. token may be a transient view (e.g. of the
// fold buffer): it is only appended into the key buffer, never retained.
func (s *bloomEntrySets) addFieldToken(path []byte, token string) {
	s.keyBuf = append(s.keyBuf[:0], path...)
	s.keyBuf = append(s.keyBuf, "::"...)
	s.keyBuf = append(s.keyBuf, token...)
	if _, ok := s.fieldTokens[string(s.keyBuf)]; !ok {
		s.fieldTokens[string(s.keyBuf)] = struct{}{}
	}
}

// unionInto adds every entry to dst.
func (s *bloomEntrySets) unionInto(dst *bloomEntrySets) {
	for entry := range s.fields {
		dst.fields[entry] = struct{}{}
	}
	for entry := range s.tokens {
		dst.tokens[entry] = struct{}{}
	}
	for entry := range s.fieldTokens {
		dst.fieldTokens[entry] = struct{}{}
	}
}

func (s *bloomEntrySets) counts() BloomEntryCounts {
	return BloomEntryCounts{
		Fields:      len(s.fields),
		Tokens:      len(s.tokens),
		FieldTokens: len(s.fieldTokens),
	}
}

// buildFilters builds bloom filters sized for exactly the accumulated entries
// at the given false positive rate.
func (s *bloomEntrySets) buildFilters(falsePositiveRate float64) BloomFilters {
	return BloomFilters{
		FieldBloomFilter:      buildSizedBloomFilter(s.fields, falsePositiveRate),
		TokenBloomFilter:      buildSizedBloomFilter(s.tokens, falsePositiveRate),
		FieldTokenBloomFilter: buildSizedBloomFilter(s.fieldTokens, falsePositiveRate),
	}
}

// buildSizedBloomFilter builds a filter sized for exactly the given entries.
// NewWithEstimates degenerates at n=0 (zero bits, NaN hash count), so an
// empty set is sized for one entry; with nothing inserted it still correctly
// tests negative.
func buildSizedBloomFilter(entries map[string]struct{}, falsePositiveRate float64) *bloom.BloomFilter {
	filter := bloom.NewWithEstimates(uint(max(len(entries), 1)), falsePositiveRate)
	for entry := range entries {
		filter.AddString(entry)
	}
	return filter
}

type ingestRequest struct {
	rows       []map[string]any
	doneChan   chan error
	forceFlush bool // if true, this is a force flush request
}

type partitionBuffer struct {
	partitionID         string
	rowCount            int
	minMaxIndexes       map[string]MinMaxIndex
	buffer              bytes.Buffer
	entries             *bloomEntrySets
	compressionEncoders *compressionEncoders
	uncompressedSize    int
}

// IngestRows queues rows for ingestion by the actor. Returns
// ErrEngineStopped once Stop has begun; a nil return means the batch was
// accepted and will either be made durable or receive an error on doneChan.
//
// Done-channel delivery is blocking: provide a buffered channel or actively
// receive from it. An abandoned unbuffered doneChan stalls the flush worker
// (deliberate backpressure) until the engine's Stop deadline aborts delivery.
func (b *BloomSearchEngine) IngestRows(ctx context.Context, rows []map[string]any, doneChan chan error) error {
	b.stateMu.RLock()
	defer b.stateMu.RUnlock()

	if b.stopped {
		return ErrEngineStopped
	}

	req := &ingestRequest{rows: rows, doneChan: doneChan}

	// Sending under the read lock means Stop cannot set stopped (and cancel
	// b.ctx) until this send lands, so the shutdown drain always sees it. The
	// ingest worker keeps consuming until b.ctx is canceled, so the send
	// cannot block Stop indefinitely.
	select {
	case b.ingestChan <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush forces a flush of any buffered data and waits for it — and for every
// flush queued before it — to complete. With nothing buffered it still waits
// behind all in-flight flush work (all flushes go through one FIFO worker),
// so a nil return means every row ingested before Flush was called is
// durable. Returns ErrEngineStopped once Stop has begun.
func (b *BloomSearchEngine) Flush(ctx context.Context) error {
	b.stateMu.RLock()
	if b.stopped {
		b.stateMu.RUnlock()
		return ErrEngineStopped
	}

	doneChan := make(chan error, 1)
	req := &ingestRequest{forceFlush: true, doneChan: doneChan}

	select {
	case b.ingestChan <- req:
		b.stateMu.RUnlock()
		// Wait for flush to complete (once committed, let it finish)
		return <-doneChan
	case <-ctx.Done():
		b.stateMu.RUnlock()
		return ctx.Err()
	}
}

func (b *BloomSearchEngine) ingestWorker() {
	defer func() {
		close(b.ingestDone)
		b.wg.Done()
	}()

	// Local state owned by the ingestion actor
	partitionBuffers := make(map[string]*partitionBuffer)
	doneChans := make([]chan error, 0)
	bufferedRowCount := 0
	bufferedBytes := 0
	var bufferStartTime time.Time

	// Create a ticker for periodic time-based flush checks
	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			b.logger.Debug("ingest worker stopping; draining accepted requests")
			// Stop set the stopped flag before canceling b.ctx, so ingestChan
			// can no longer receive new requests: draining until empty
			// processes every accepted batch. Requests are processed with the
			// flush context — b.ctx is already canceled and using it would
			// fail the shutdown flush and drop done-channel delivery.
			for {
				select {
				case req := <-b.ingestChan:
					b.processIngestRequest(
						b.flushCtx,
						req,
						partitionBuffers,
						&doneChans,
						&bufferedRowCount,
						&bufferedBytes,
						&bufferStartTime,
					)
				default:
					// Flush any remaining buffered data (and ack any
					// remaining waiters) before exiting.
					b.flushBufferedData(
						partitionBuffers,
						&doneChans,
						&bufferedRowCount,
						&bufferedBytes,
						&bufferStartTime,
					)
					return
				}
			}
		case req := <-b.ingestChan:
			// Process the batch of rows
			b.processIngestRequest(
				b.flushCtx,
				req,
				partitionBuffers,
				&doneChans,
				&bufferedRowCount,
				&bufferedBytes,
				&bufferStartTime,
			)
		case <-ticker.C:
			// Check for time-based flush
			if bufferedRowCount > 0 && !bufferStartTime.IsZero() && time.Since(bufferStartTime) >= b.config.MaxBufferedTime {
				b.flushBufferedData(
					partitionBuffers,
					&doneChans,
					&bufferedRowCount,
					&bufferedBytes,
					&bufferStartTime,
				)
			}
		}
	}
}

// flushBufferedData flushes the current buffered data and resets the buffer
// state. With no buffered partitions but pending done channels it still
// enqueues an ack-only flush request, so waiters are acked in FIFO order
// behind every flush already queued or in flight.
func (b *BloomSearchEngine) flushBufferedData(
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
) {
	if len(partitionBuffers) == 0 && len(*doneChans) == 0 {
		return
	}

	// Copy data before sending to flush worker
	partitionBuffersCopy := make(map[string]*partitionBuffer)
	for k, v := range partitionBuffers {
		partitionBuffersCopy[k] = v
	}
	doneChannsCopy := make([]chan error, len(*doneChans))
	copy(doneChannsCopy, *doneChans)

	b.triggerFlush(partitionBuffersCopy, doneChannsCopy)

	// Reset local state
	for k := range partitionBuffers {
		delete(partitionBuffers, k)
	}
	*doneChans = make([]chan error, 0)
	*bufferedRowCount = 0
	*bufferedBytes = 0
	*bufferStartTime = time.Time{}
}

func (b *BloomSearchEngine) processIngestRequest(
	ctx context.Context,
	req *ingestRequest,
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
) {
	// If this is a force flush request, route it through the flush FIFO even
	// with nothing buffered: flushBufferedData enqueues an ack-only flush
	// request in that case, so the ack is ordered behind all in-flight flush
	// work and Flush never returns before earlier rows are durable.
	if req.forceFlush {
		*doneChans = append(*doneChans, req.doneChan)
		b.flushBufferedData(
			partitionBuffers,
			doneChans,
			bufferedRowCount,
			bufferedBytes,
			bufferStartTime,
		)
		return
	}

	// An empty batch has nothing to make durable: ack immediately and leave
	// the buffers untouched (no empty partition buffer, no 0-row block).
	if len(req.rows) == 0 {
		sendOptionalWithContext(ctx, req.doneChan, nil)
		return
	}

	// Group rows by partition ID
	partitionedRows := make(map[string][]map[string]any)
	if b.config.PartitionFunc != nil {
		for _, row := range req.rows {
			partitionID := b.config.PartitionFunc(row)
			partitionedRows[partitionID] = append(partitionedRows[partitionID], row)
		}
	} else {
		partitionedRows[""] = req.rows
	}

	// Serialize and validate every row before mutating any partition buffer or
	// bloom filter, so a mid-batch error rejects the whole batch and leaves
	// the buffered state exactly as it was. Indexing walks the marshaled JSON
	// bytes, the same canonical representation query-time row verification
	// walks (see forEachPathValue in tokenizer.go).
	partitionedRowBytes := make(map[string][][]byte, len(partitionedRows))
	for partitionID, rows := range partitionedRows {
		rowBytesList := make([][]byte, len(rows))
		for i, row := range rows {
			rowBytes, err := json.Marshal(row)
			if err != nil {
				sendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to serialize row: %w", err))
				return
			}

			// Check if row is too large for uint32 length prefix
			if len(rowBytes) > 0xFFFFFFFF {
				sendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("row too large: %d bytes exceeds maximum of %d bytes", len(rowBytes), 0xFFFFFFFF))
				return
			}

			rowBytesList[i] = rowBytes
		}
		partitionedRowBytes[partitionID] = rowBytesList
	}

	// Create partition buffers if they don't exist. The compression encoder is
	// created before the buffer is registered, and buffers created for this
	// batch are removed on failure — a half-constructed entry (nil encoder)
	// must never persist, or the next ingest for that partition panics.
	createdPartitions := make([]string, 0)
	for partitionID := range partitionedRows {
		if partitionBuffers[partitionID] == nil {
			newBuffer := &partitionBuffer{
				partitionID:      partitionID,
				minMaxIndexes:    make(map[string]MinMaxIndex),
				buffer:           bytes.Buffer{},
				entries:          newBloomEntrySets(),
				uncompressedSize: 0,
				rowCount:         0,
			}
			encoders, err := b.createCompressionWriter(&newBuffer.buffer)
			if err != nil {
				for _, createdID := range createdPartitions {
					delete(partitionBuffers, createdID)
				}
				sendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to create compression writer: %w", err))
				return
			}
			newBuffer.compressionEncoders = encoders
			partitionBuffers[partitionID] = newBuffer
			createdPartitions = append(createdPartitions, partitionID)
		}
	}

	// If we haven't saved the buffer start time yet, do it
	if bufferStartTime.IsZero() {
		*bufferStartTime = time.Now()
	}

	// Track if we should flush
	shouldFlush := false

	var lengthBytes [LengthPrefixSize]byte

	// Process each partition
	for partitionID, rows := range partitionedRows {
		partitionBuffer := partitionBuffers[partitionID]
		rowBytesList := partitionedRowBytes[partitionID]

		// Process each row
		for i, row := range rows {
			rowBytes := rowBytesList[i]

			// Record the row's bloom entries in the partition's dedup sets.
			// Filters are built from these sets at flush time, sized for the
			// exact distinct counts (see handleFlush).
			partitionBuffer.entries.indexRow(rowBytes, b.config.Tokenizer)

			// Check for minmax indexes, reading Go-native values from the
			// original row so numeric identity is preserved (conversions clamp
			// out-of-range values, see min_max.go)
			for _, index := range b.config.MinMaxIndexes {
				if value, ok := row[index]; ok {
					minVal, maxVal, isNumeric := ConvertToMinMaxInt64(value)
					if !isNumeric {
						continue
					}

					if existingIndex, exists := partitionBuffer.minMaxIndexes[index]; exists {
						partitionBuffer.minMaxIndexes[index] = UpdateMinMaxIndex(existingIndex, minVal, maxVal)
					} else {
						partitionBuffer.minMaxIndexes[index] = MinMaxIndex{
							Min: minVal,
							Max: maxVal,
						}
					}
				}
			}

			// Write length prefix (uint32) followed by row bytes. The
			// destination is an in-memory buffer, so failures here are
			// exceptional; they are still reported rather than swallowed.
			binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(rowBytes)))
			if _, err := partitionBuffer.compressionEncoders.writer.Write(lengthBytes[:]); err != nil {
				sendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to buffer row length: %w", err))
				return
			}
			if _, err := partitionBuffer.compressionEncoders.writer.Write(rowBytes); err != nil {
				sendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to buffer row data: %w", err))
				return
			}

			// Increment stats
			partitionBuffer.uncompressedSize += len(rowBytes) + LengthPrefixSize
			partitionBuffer.rowCount += 1

			// Use uncompressed size for flush decisions since compression may buffer data
			uncompressedRowSize := len(rowBytes) + LengthPrefixSize
			*bufferedBytes += uncompressedRowSize
			*bufferedRowCount += 1
		}

		// Check partition-level limits
		if !shouldFlush {
			partitionUncompressedBytes := partitionBuffer.uncompressedSize

			if partitionBuffer.rowCount >= b.config.MaxRowGroupRows {
				b.logger.Debug("flush triggered: partition hit max rows",
					"partition", partitionBuffer.partitionID, "rows", partitionBuffer.rowCount, "maxRowGroupRows", b.config.MaxRowGroupRows)
				shouldFlush = true
			} else if partitionUncompressedBytes >= b.config.MaxRowGroupBytes {
				b.logger.Debug("flush triggered: partition hit max uncompressed bytes",
					"partition", partitionBuffer.partitionID, "bytes", partitionUncompressedBytes, "maxRowGroupBytes", b.config.MaxRowGroupBytes)
				shouldFlush = true
			}
		}
	}

	// If we haven't decided to flush based on partition limits, check buffer-level limits
	if !shouldFlush {
		if *bufferedRowCount >= b.config.MaxBufferedRows {
			b.logger.Debug("flush triggered: buffer hit max rows",
				"rows", *bufferedRowCount, "maxBufferedRows", b.config.MaxBufferedRows)
			shouldFlush = true
		}

		if !shouldFlush && *bufferedBytes >= b.config.MaxBufferedBytes {
			b.logger.Debug("flush triggered: buffer hit max bytes",
				"bytes", *bufferedBytes, "maxBufferedBytes", b.config.MaxBufferedBytes)
			shouldFlush = true
		}

		if !shouldFlush && time.Since(*bufferStartTime) >= b.config.MaxBufferedTime {
			b.logger.Debug("flush triggered: buffer hit max time",
				"buffered", time.Since(*bufferStartTime), "maxBufferedTime", b.config.MaxBufferedTime)
			shouldFlush = true
		}
	}

	// Store the doneChan
	*doneChans = append(*doneChans, req.doneChan)

	// Trigger flush if needed
	if shouldFlush {
		b.logger.Debug("flush starting",
			"partitions", len(partitionBuffers), "rows", *bufferedRowCount, "bytes", *bufferedBytes)
		b.flushBufferedData(
			partitionBuffers,
			doneChans,
			bufferedRowCount,
			bufferedBytes,
			bufferStartTime,
		)
	}
}

// triggerFlush enqueues a flush request for the flush worker. The send
// blocks: the ingest actor is the only producer and the flush worker the only
// consumer, so flushes execute strictly FIFO — Flush acks cannot overtake
// flushes queued before them — and flushing never runs inline on the ingest
// actor. A full channel simply applies backpressure to ingest. If the
// shutdown deadline expires while the flush worker is wedged, waiters are
// told (best effort) instead of being dropped silently.
func (b *BloomSearchEngine) triggerFlush(partitionBuffers map[string]*partitionBuffer, doneChans []chan error) {
	flushReq := flushRequest{
		partitionBuffers: partitionBuffers,
		doneChans:        doneChans,
	}

	select {
	case b.flushChan <- flushReq:
		// Successfully queued for flush
	case <-b.flushCtx.Done():
		// Shutdown deadline expired: the flush worker will not take this
		// request. Deliver the failure to ready waiters; flushCtx is already
		// canceled so blocked channels are given up immediately.
		b.logger.Warn("flush abandoned: shutdown deadline expired before the flush could be queued",
			"partitions", len(partitionBuffers), "waiters", len(doneChans))
		sendToChannelsWithContext(b.flushCtx, doneChans, fmt.Errorf("flush abandoned: %w", b.flushCtx.Err()))
	}
}
