package bloomsearch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration")
)

// dataBlockJob represents a job to process a data block
type dataBlockJob struct {
	filePointer   []byte
	blockMetadata DataBlockMetadata
}

// makeFieldTokenKey creates a key for field-token bloom filter entries
func makeFieldTokenKey(field, token string) string {
	return field + "::" + token
}

type PartitionFunc func(row map[string]any) string

type ingestRequest struct {
	rows       []map[string]any
	doneChan   chan error
	forceFlush bool // if true, this is a force flush request
}

func (r *ingestRequest) reset() {
	r.rows = nil
	r.doneChan = nil
	r.forceFlush = false
}

type flushRequest struct {
	partitionBuffers map[string]*partitionBuffer
	doneChans        []chan error
}

type BloomSearchEngine struct {
	config    BloomSearchEngineConfig
	metaStore MetaStore
	dataStore DataStore

	ingestChan  chan *ingestRequest
	flushChan   chan flushRequest
	requestPool *sync.Pool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	querySemaphore chan struct{}

	fileFieldBloomFilter      *bloom.BloomFilter
	fileTokenBloomFilter      *bloom.BloomFilter
	fileFieldTokenBloomFilter *bloom.BloomFilter
}

type BlockStats struct {
	FilePointer        []byte
	BlockOffset        int
	RowsProcessed      int64
	BytesProcessed     int64
	TotalRows          int64
	TotalBytes         int64
	Duration           time.Duration
	BloomFilterSkipped bool
}

type BloomSearchEngineConfig struct {
	Tokenizer     ValueTokenizerFunc
	PartitionFunc PartitionFunc

	MinMaxIndexes []string

	MaxRowGroupBytes int
	MaxRowGroupRows  int
	MaxFileSize      int

	MaxBufferedRows  int
	MaxBufferedBytes int
	MaxBufferedTime  time.Duration

	IngestBufferSize int

	// The maximum number of total data blocks that can be processed concurrently across all queries
	MaxQueryConcurrency int

	// Bloom filter parameters
	FileBloomExpectedItems uint
	BloomFalsePositiveRate float64

	// Compression configuration
	RowDataCompression CompressionType

	// Compression level for zstd (1-22, higher = better compression, slower)
	// Ignored for snappy compression
	ZstdCompressionLevel int
}

type partitionBuffer struct {
	partitionID           string
	rowCount              int
	minMaxIndexes         map[string]MinMaxIndex
	buffer                bytes.Buffer
	fieldBloomFilter      *bloom.BloomFilter
	tokenBloomFilter      *bloom.BloomFilter
	fieldTokenBloomFilter *bloom.BloomFilter
	zstdEncoder           *zstd.Encoder
	snappyEncoder         *snappy.Writer
	uncompressedSize      int
}

func DefaultBloomSearchEngineConfig() BloomSearchEngineConfig {
	return BloomSearchEngineConfig{
		Tokenizer: BasicWhitespaceLowerTokenizer,

		MaxRowGroupBytes: 10 * 1024 * 1024,
		MaxRowGroupRows:  10000,
		MaxFileSize:      10 * 1024 * 1024 * 1024,

		MaxBufferedRows:  1000,
		MaxBufferedBytes: 1 * 1024 * 1024,
		MaxBufferedTime:  10 * time.Second, // this is designed for async writing

		IngestBufferSize: 1_000,

		MaxQueryConcurrency: 1_000,

		FileBloomExpectedItems: 100_000,
		BloomFalsePositiveRate: 0.001,

		// Default to Snappy for fast decompression
		RowDataCompression:   CompressionSnappy,
		ZstdCompressionLevel: 3, // Balanced compression/speed for zstd
	}
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) (*BloomSearchEngine, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Tokenizer == nil {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: tokenizer is required", ErrInvalidConfig)
	}

	if config.FileBloomExpectedItems == 0 {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: BloomExpectedItems must be greater than 0", ErrInvalidConfig)
	}

	if config.BloomFalsePositiveRate <= 0 || config.BloomFalsePositiveRate >= 1 {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: BloomFalsePositiveRate must be between 0 and 1", ErrInvalidConfig)
	}

	if config.MaxQueryConcurrency <= 0 {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: MaxQueryConcurrency must be greater than 0", ErrInvalidConfig)
	}

	return &BloomSearchEngine{
		config:    config,
		metaStore: metaStore,
		dataStore: dataStore,

		ingestChan: make(chan *ingestRequest, config.IngestBufferSize),
		flushChan:  make(chan flushRequest, 1), // Buffered flush channel
		requestPool: &sync.Pool{
			New: func() interface{} {
				return &ingestRequest{}
			},
		},
		ctx:    ctx,
		cancel: cancel,

		querySemaphore: make(chan struct{}, config.MaxQueryConcurrency),

		fileFieldBloomFilter:      bloom.NewWithEstimates(config.FileBloomExpectedItems, config.BloomFalsePositiveRate),
		fileTokenBloomFilter:      bloom.NewWithEstimates(config.FileBloomExpectedItems, config.BloomFalsePositiveRate),
		fileFieldTokenBloomFilter: bloom.NewWithEstimates(config.FileBloomExpectedItems, config.BloomFalsePositiveRate),
	}, nil
}

// Start begins the ingestion and flush workers
func (b *BloomSearchEngine) Start() {
	b.wg.Add(2)
	go b.ingestWorker()
	go b.flushWorker()
}

// Stop gracefully shuts down the engine with a timeout
func (b *BloomSearchEngine) Stop(ctx context.Context) error {
	// Signal workers to stop
	b.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Workers finished gracefully
		return nil
	case <-ctx.Done():
		// Timeout occurred
		return fmt.Errorf("shutdown timeout exceeded: %w", ctx.Err())
	}
}

// IngestRows queues rows for ingestion by the actor
func (b *BloomSearchEngine) IngestRows(ctx context.Context, rows []map[string]any, doneChan chan error) error {
	req := b.requestPool.Get().(*ingestRequest)
	req.rows = rows
	req.doneChan = doneChan

	select {
	case b.ingestChan <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return context.Canceled
	}
}

// Flush forces a flush of any buffered data
func (b *BloomSearchEngine) Flush(ctx context.Context) error {
	req := b.requestPool.Get().(*ingestRequest)
	req.rows = nil
	req.forceFlush = true
	doneChan := make(chan error, 1)
	req.doneChan = doneChan

	select {
	case b.ingestChan <- req:
		// Wait for flush to complete (once committed, let it finish)
		return <-doneChan
	case <-ctx.Done():
		req.reset()
		b.requestPool.Put(req)
		return ctx.Err()
	case <-b.ctx.Done():
		req.reset()
		b.requestPool.Put(req)
		return context.Canceled
	}
}

func (b *BloomSearchEngine) ingestWorker() {
	defer b.wg.Done()

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
			fmt.Println("ingestWorker context done")
			// Flush any remaining buffered data before exiting
			if bufferedRowCount > 0 {
				b.flushBufferedData(partitionBuffers, &doneChans, &bufferedRowCount, &bufferedBytes, &bufferStartTime)
			}
			return
		case req := <-b.ingestChan:
			// Process the batch of rows
			b.processIngestRequest(b.ctx, req, partitionBuffers, &doneChans, &bufferedRowCount, &bufferedBytes, &bufferStartTime)
		case <-ticker.C:
			// Check for time-based flush
			if bufferedRowCount > 0 && !bufferStartTime.IsZero() && time.Since(bufferStartTime) >= b.config.MaxBufferedTime {
				b.flushBufferedData(partitionBuffers, &doneChans, &bufferedRowCount, &bufferedBytes, &bufferStartTime)
			}
		}
	}
}

// flushBufferedData flushes the current buffered data and resets the buffer state
func (b *BloomSearchEngine) flushBufferedData(partitionBuffers map[string]*partitionBuffer, doneChans *[]chan error, bufferedRowCount *int, bufferedBytes *int, bufferStartTime *time.Time) {
	if len(partitionBuffers) == 0 {
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

	// Note: Don't reset file-level bloom filters here since the flush worker needs them
	// They will be reset in handleFlush after the flush completes
}

func (b *BloomSearchEngine) processIngestRequest(ctx context.Context, req *ingestRequest, partitionBuffers map[string]*partitionBuffer, doneChans *[]chan error, bufferedRowCount *int, bufferedBytes *int, bufferStartTime *time.Time) {
	// Process the request and return to pool at the end
	defer func() {
		req.reset()
		b.requestPool.Put(req)
	}()

	// If this is a force flush request, immediately trigger a flush
	if req.forceFlush {
		if *bufferedRowCount > 0 {
			// Add the force flush doneChan to the list before flushing
			*doneChans = append(*doneChans, req.doneChan)
			b.flushBufferedData(partitionBuffers, doneChans, bufferedRowCount, bufferedBytes, bufferStartTime)
		} else {
			// No buffered data, signal completion immediately
			TryWriteToChannels([]chan error{req.doneChan}, nil)
		}
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

	// Create partition buffers if they don't exist
	for partitionID := range partitionedRows {
		if partitionBuffers[partitionID] == nil {
			partitionBuffers[partitionID] = &partitionBuffer{
				partitionID:           partitionID,
				minMaxIndexes:         make(map[string]MinMaxIndex),
				buffer:                bytes.Buffer{},
				fieldBloomFilter:      bloom.NewWithEstimates(uint(b.config.MaxRowGroupRows), b.config.BloomFalsePositiveRate),
				tokenBloomFilter:      bloom.NewWithEstimates(uint(b.config.MaxRowGroupRows), b.config.BloomFalsePositiveRate),
				fieldTokenBloomFilter: bloom.NewWithEstimates(uint(b.config.MaxRowGroupRows), b.config.BloomFalsePositiveRate),
				uncompressedSize:      0,
				rowCount:              0,
			}
			var err error
			if b.config.RowDataCompression == CompressionZstd {
				partitionBuffers[partitionID].zstdEncoder, err = zstd.NewWriter(&partitionBuffers[partitionID].buffer, zstd.WithEncoderLevel(zstd.EncoderLevel(b.config.ZstdCompressionLevel)))
				if err != nil {
					SendWithContext(ctx, req.doneChan, fmt.Errorf("failed to create zstd encoder: %w", err))
					return
				}
			} else if b.config.RowDataCompression == CompressionSnappy {
				partitionBuffers[partitionID].snappyEncoder = snappy.NewWriter(&partitionBuffers[partitionID].buffer)
			}

		}
	}

	// If we haven't saved the buffer start time yet, do it
	if bufferStartTime.IsZero() {
		*bufferStartTime = time.Now()
	}

	// Track if we should flush
	shouldFlush := false

	// Process each partition
	for partitionID, rows := range partitionedRows {
		partitionBuffer := partitionBuffers[partitionID]

		// Process each row
		for _, row := range rows {
			// Add info to bloom filters
			uniqueFields := UniqueFields(row, ".")
			for _, field := range uniqueFields {
				partitionBuffer.fieldBloomFilter.AddString(field.Path)
				// Also add to file-level bloom filters
				b.fileFieldBloomFilter.AddString(field.Path)

				for _, value := range field.Values {
					tokens := b.config.Tokenizer(value)
					for _, token := range tokens {
						partitionBuffer.tokenBloomFilter.AddString(token)
						partitionBuffer.fieldTokenBloomFilter.AddString(makeFieldTokenKey(field.Path, token))
						// Also add to file-level bloom filters
						b.fileTokenBloomFilter.AddString(token)
						b.fileFieldTokenBloomFilter.AddString(makeFieldTokenKey(field.Path, token))
					}
				}
			}

			// Check for minmax indexes
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

			// Serialize and store the row
			rowBytes, err := json.Marshal(row)
			if err != nil {
				SendWithContext(ctx, req.doneChan, fmt.Errorf("failed to serialize row: %w", err))
				return
			}

			// Check if row is too large for uint32 length prefix
			if len(rowBytes) > 0xFFFFFFFF {
				SendWithContext(ctx, req.doneChan, fmt.Errorf("row too large: %d bytes exceeds maximum of %d bytes", len(rowBytes), 0xFFFFFFFF))
				return
			}

			// Write length prefix (uint32) followed by row bytes
			lengthBytes := make([]byte, LengthPrefixSize)
			binary.LittleEndian.PutUint32(lengthBytes, uint32(len(rowBytes)))
			switch b.config.RowDataCompression {
			case CompressionZstd:
				partitionBuffer.zstdEncoder.Write(lengthBytes)
				partitionBuffer.zstdEncoder.Write(rowBytes)
			case CompressionSnappy:
				// Use streaming compression for Snappy
				partitionBuffer.snappyEncoder.Write(lengthBytes)
				partitionBuffer.snappyEncoder.Write(rowBytes)
			default:
				partitionBuffer.buffer.Write(lengthBytes)
				partitionBuffer.buffer.Write(rowBytes)
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
				fmt.Printf("FLUSH TRIGGER: Partition '%s' hit max rows (%d >= %d)\n",
					partitionBuffer.partitionID, partitionBuffer.rowCount, b.config.MaxRowGroupRows)
				shouldFlush = true
			} else if partitionUncompressedBytes >= b.config.MaxRowGroupBytes {
				fmt.Printf("FLUSH TRIGGER: Partition '%s' hit max uncompressed bytes (%d >= %d)\n",
					partitionBuffer.partitionID, partitionUncompressedBytes, b.config.MaxRowGroupBytes)
				shouldFlush = true
			}
		}
	}

	// If we haven't decided to flush based on partition limits, check buffer-level limits
	if !shouldFlush {
		if *bufferedRowCount >= b.config.MaxBufferedRows {
			fmt.Printf("FLUSH TRIGGER: Buffer hit max rows (%d >= %d)\n",
				*bufferedRowCount, b.config.MaxBufferedRows)
			shouldFlush = true
		}

		if !shouldFlush && *bufferedBytes >= b.config.MaxBufferedBytes {
			fmt.Printf("FLUSH TRIGGER: Buffer hit max bytes (%d >= %d)\n",
				*bufferedBytes, b.config.MaxBufferedBytes)
			shouldFlush = true
		}

		if !shouldFlush && time.Since(*bufferStartTime) >= b.config.MaxBufferedTime {
			fmt.Printf("FLUSH TRIGGER: Buffer hit max time (%v >= %v)\n",
				time.Since(*bufferStartTime), b.config.MaxBufferedTime)
			shouldFlush = true
		}
	}

	// Store the doneChan
	*doneChans = append(*doneChans, req.doneChan)

	// Trigger flush if needed
	if shouldFlush {
		// Log flush details
		fmt.Printf("FLUSH STARTING: %d partitions, %d total rows, %d total bytes\n",
			len(partitionBuffers), *bufferedRowCount, *bufferedBytes)
		for partitionID, partition := range partitionBuffers {
			fmt.Printf("  Partition '%s': %d rows, %d bytes\n",
				partitionID, partition.rowCount, partition.buffer.Len())
		}
		b.flushBufferedData(partitionBuffers, doneChans, bufferedRowCount, bufferedBytes, bufferStartTime)
	}
}

func (b *BloomSearchEngine) triggerFlush(partitionBuffers map[string]*partitionBuffer, doneChans []chan error) {
	flushReq := flushRequest{
		partitionBuffers: partitionBuffers,
		doneChans:        doneChans,
	}

	// Send to flush worker (non-blocking)
	select {
	case b.flushChan <- flushReq:
		// Successfully queued for flush
	default:
		// Flush channel is full, handle directly (should be rare)
		b.handleFlush(flushReq)
	}
}

func (b *BloomSearchEngine) flushWorker() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			fmt.Println("flushWorker context done")
			return
		case flushReq := <-b.flushChan:
			b.handleFlush(flushReq)
		}
	}
}

func (b *BloomSearchEngine) handleFlush(flushReq flushRequest) {
	fileMetadata := FileMetadata{
		FieldBloomFilter:      b.fileFieldBloomFilter,
		TokenBloomFilter:      b.fileTokenBloomFilter,
		FieldTokenBloomFilter: b.fileFieldTokenBloomFilter,
		DataBlocks:            make([]DataBlockMetadata, 0),
	}

	// Stream write to data store
	writer, filePointerBytes, err := b.dataStore.CreateFile(b.ctx)
	if err != nil {
		fmt.Println("failed to create file: %w", err)
		// Write error to all done channels
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to create file: %w", err))
		return
	}

	fileHash := xxhash.New()
	currentOffset := 0

	// For each partition buffer, write the data block to the data store
	for _, partitionBuffer := range flushReq.partitionBuffers {
		// Finalize compression encoders before writing
		var compressedData []byte
		switch b.config.RowDataCompression {
		case CompressionZstd:
			if partitionBuffer.zstdEncoder != nil {
				if err := partitionBuffer.zstdEncoder.Close(); err != nil {
					TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to close zstd encoder: %w", err))
					return
				}
			}
			compressedData = partitionBuffer.buffer.Bytes()
		case CompressionSnappy:
			// Close the streaming snappy encoder to finalize compression
			if partitionBuffer.snappyEncoder != nil {
				if err := partitionBuffer.snappyEncoder.Close(); err != nil {
					TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to close snappy encoder: %w", err))
					return
				}
			}
			compressedData = partitionBuffer.buffer.Bytes()
		default:
			compressedData = partitionBuffer.buffer.Bytes()
		}
		// Create data block bloom filters struct
		dataBlockBloomFilters := &DataBlockBloomFilters{
			FieldBloomFilter:      partitionBuffer.fieldBloomFilter,
			TokenBloomFilter:      partitionBuffer.tokenBloomFilter,
			FieldTokenBloomFilter: partitionBuffer.fieldTokenBloomFilter,
		}

		// Serialize bloom filters and get hash
		bloomFiltersBytes, bloomFiltersHashBytes := dataBlockBloomFilters.Bytes()

		// Write bloom filters to data block
		if _, err := writer.Write(bloomFiltersBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write bloom filters: %w", err))
			return
		}

		// Write bloom filters hash
		if _, err := writer.Write(bloomFiltersHashBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write bloom filters hash: %w", err))
			return
		}

		// Calculate hash of compressed row data
		rowDataHash := xxhash.Sum64(compressedData)

		// Write the row data buffer
		if _, err := writer.Write(compressedData); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write data block: %w", err))
			return
		}

		// Stream hash calculation without copying data
		if _, err := fileHash.Write(bloomFiltersBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}
		if _, err := fileHash.Write(bloomFiltersHashBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}
		if _, err := fileHash.Write(compressedData); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}

		bloomFiltersSize := len(bloomFiltersBytes) + HashSize
		dataBlockSize := bloomFiltersSize + len(compressedData)

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:      partitionBuffer.partitionID,
			Rows:             partitionBuffer.rowCount,
			Offset:           currentOffset,
			Size:             dataBlockSize,
			BloomFiltersSize: bloomFiltersSize,
			MinMaxIndexes:    partitionBuffer.minMaxIndexes,
			Compression:      b.config.RowDataCompression,
			UncompressedSize: partitionBuffer.uncompressedSize,
			RowDataHash:      rowDataHash,
		})

		currentOffset += dataBlockSize
	}

	// Write final metadata to data store and footer
	metadataBytes, metadataHashBytes := fileMetadata.Bytes()

	// Write file metadata
	if _, err := writer.Write(metadataBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata: %w", err))
		return
	}

	if _, err := writer.Write(metadataHashBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata hash: %w", err))
		return
	}

	metadataLengthBytes := make([]byte, LengthPrefixSize)
	binary.LittleEndian.PutUint32(metadataLengthBytes, uint32(len(metadataBytes)))
	if _, err := writer.Write(metadataLengthBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata length: %w", err))
		return
	}

	versionBytes := make([]byte, VersionPrefixSize)
	binary.LittleEndian.PutUint32(versionBytes, FileVersion)
	if _, err := writer.Write(versionBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file version: %w", err))
		return
	}

	if _, err := writer.Write([]byte(MagicBytes)); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write magic bytes: %w", err))
		return
	}

	if err := writer.Close(); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to close file writer: %w", err))
		return
	}

	if err := b.metaStore.Update(b.ctx, []WriteOperation{
		{
			FileMetadata:     &fileMetadata,
			FilePointerBytes: filePointerBytes,
		},
	}, nil); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to store file metadata: %w", err))
		return
	}

	// Reset file-level bloom filters for the next file (now that flush is complete)
	b.fileFieldBloomFilter = bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate)
	b.fileTokenBloomFilter = bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate)
	b.fileFieldTokenBloomFilter = bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate)

	TryWriteToChannels(flushReq.doneChans, nil)
}

// evaluateBloomFilters tests if bloom filters match the bloom query
func (b *BloomSearchEngine) evaluateBloomFilters(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	bloomQuery *BloomQuery,
) bool {
	if bloomQuery == nil || len(bloomQuery.Groups) == 0 {
		return true // No bloom filtering needed, since it's only used to DISQUALIFY files
	}

	// Evaluate each group
	groupResults := make([]bool, len(bloomQuery.Groups))
	for i, group := range bloomQuery.Groups {
		groupResults[i] = b.evaluateBloomGroup(fieldFilter, tokenFilter, fieldTokenFilter, &group)
	}

	// Combine group results based on query combinator
	if bloomQuery.Combinator == CombinatorOR {
		// Any group can match
		for _, result := range groupResults {
			if result {
				return true
			}
		}
		return false
	}

	// Default AND behavior: all groups must match
	for _, result := range groupResults {
		if !result {
			return false
		}
	}
	return true
}

// evaluateBloomGroup tests if bloom filters match a single bloom group
func (b *BloomSearchEngine) evaluateBloomGroup(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	group *BloomGroup,
) bool {
	if len(group.Conditions) == 0 {
		return true // Empty group matches everything, since we can't disqualify it
	}

	// Evaluate each condition in the group
	conditionResults := make([]bool, len(group.Conditions))
	for i, condition := range group.Conditions {
		conditionResults[i] = b.evaluateBloomCondition(fieldFilter, tokenFilter, fieldTokenFilter, &condition)
	}

	// Combine condition results based on group combinator
	if group.Combinator == CombinatorOR {
		// Any condition can match
		for _, result := range conditionResults {
			if result {
				return true
			}
		}
		return false
	}

	// Default AND behavior: all conditions must match
	for _, result := range conditionResults {
		if !result {
			return false
		}
	}
	return true
}

// evaluateBloomCondition tests if bloom filters match a single bloom condition
func (b *BloomSearchEngine) evaluateBloomCondition(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	condition *BloomCondition,
) (result bool) {
	switch condition.Type {
	case BloomField:
		result = fieldFilter.TestString(condition.Field)
	case BloomToken:
		result = tokenFilter.TestString(condition.Token)
	case BloomFieldToken:
		key := makeFieldTokenKey(condition.Field, condition.Token)
		result = fieldTokenFilter.TestString(key)
	default:
		result = false // We don't know what this is, so it's invalid
	}
	return
}

// Query executes a query and sends results to the provided channels.
// The result channel feeds individual matching rows. Canceling the context
// will stop all workers. When the result channel closes, no more work is happening.
//
// The error channel is written to for any errors that occur per-worker. If a worker writes to this channel,
// it has stopped processing.
//
// Example usage:
//
//	resultChan := make(chan map[string]any, 1000)
//	errorChan := make(chan error, 100)
//	err := engine.Query(ctx, query, resultChan, errorChan, nil)
//	if err != nil { return err }
//	for {
//	  select {
//	  case <-ctx.Done():
//	    return ctx.Err()
//	  case row, ok := <-resultChan:
//	    if !ok { return nil } // done
//	    // process row
//	  case err := <-errorChan:
//	    return err
//	  }
//	}
func (b *BloomSearchEngine) Query(ctx context.Context, query *Query, resultChan chan<- map[string]any, errorChan chan<- error, statsChan chan<- BlockStats) error {
	maybeFiles, err := b.metaStore.GetMaybeFilesForQuery(ctx, query.Prefilter)
	if err != nil {
		return err
	}

	// Test file-level bloom filters, using concurrency only above a threshold
	const concurrencyThreshold = 20

	var matchingFiles []MaybeFile
	if len(maybeFiles) < concurrencyThreshold {
		// Sequential evaluation for small numbers of files
		matchingFiles = make([]MaybeFile, 0, len(maybeFiles))
		for _, maybeFile := range maybeFiles {
			if b.evaluateBloomFilters(
				maybeFile.Metadata.FieldBloomFilter,
				maybeFile.Metadata.TokenBloomFilter,
				maybeFile.Metadata.FieldTokenBloomFilter,
				query.Bloom,
			) {
				matchingFiles = append(matchingFiles, maybeFile)
			}
		}
	} else {
		// Concurrent evaluation for larger numbers of files
		var fileWg sync.WaitGroup
		matchingFilesChan := make(chan MaybeFile, len(maybeFiles))

		for _, maybeFile := range maybeFiles {
			fileWg.Add(1)
			go func(maybeFile MaybeFile) {
				defer fileWg.Done()

				if err := SendWithContext(ctx, b.querySemaphore, struct{}{}); err != nil {
					return
				}
				defer func() { <-b.querySemaphore }()

				if b.evaluateBloomFilters(
					maybeFile.Metadata.FieldBloomFilter,
					maybeFile.Metadata.TokenBloomFilter,
					maybeFile.Metadata.FieldTokenBloomFilter,
					query.Bloom,
				) {
					SendWithContext(ctx, matchingFilesChan, maybeFile)
				}
			}(maybeFile)
		}

		go func() {
			fileWg.Wait()
			close(matchingFilesChan) // close to tell the range below to stop
		}()

		for matchingFile := range matchingFilesChan {
			matchingFiles = append(matchingFiles, matchingFile)
		}
	}

	var allJobs []dataBlockJob
	for _, matchingFile := range matchingFiles {
		for _, blockMetadata := range matchingFile.Metadata.DataBlocks {
			allJobs = append(allJobs, dataBlockJob{
				filePointer:   matchingFile.PointerBytes,
				blockMetadata: blockMetadata,
			})
		}
	}

	var wg sync.WaitGroup
	workerCtx, workerCancel := context.WithCancel(ctx)
	for _, job := range allJobs {
		wg.Add(1)
		go func(job dataBlockJob) {
			defer wg.Done()

			if err := SendWithContext(workerCtx, b.querySemaphore, struct{}{}); err != nil {
				return
			}
			defer func() { <-b.querySemaphore }()

			b.processDataBlock(workerCtx, job, resultChan, errorChan, query.Bloom, statsChan)
		}(job)
	}

	// Close result channel when all workers are done
	go func() {
		defer workerCancel()
		wg.Wait()
		close(resultChan) // closing this indicates that all workers are done, and no more errors can come either
	}()

	return nil
}

// processDataBlock processes a specific data block job
func (b *BloomSearchEngine) processDataBlock(
	ctx context.Context,
	job dataBlockJob,
	resultChan chan<- map[string]any,
	errorChan chan<- error,
	bloomQuery *BloomQuery,
	statsChan chan<- BlockStats,
) {
	blockStartTime := time.Now()
	var bloomFilterSkipped bool

	// Always send stats when we exit, regardless of success/failure
	defer func() {
		duration := time.Since(blockStartTime)

		blockStats := BlockStats{
			FilePointer:        job.filePointer,
			BlockOffset:        job.blockMetadata.Offset,
			RowsProcessed:      int64(job.blockMetadata.Rows), // Full block rows
			BytesProcessed:     int64(job.blockMetadata.Size), // Full block size
			TotalRows:          int64(job.blockMetadata.Rows),
			TotalBytes:         int64(job.blockMetadata.Size),
			Duration:           duration,
			BloomFilterSkipped: bloomFilterSkipped,
		}

		TryWriteChannel(statsChan, blockStats)
	}()
	file, err := b.dataStore.OpenFile(ctx, job.filePointer)
	if err != nil {
		SendWithContext(ctx, errorChan, fmt.Errorf("failed to open file: %w", err))
		return
	}
	defer file.Close()

	blockBloomFilters, err := ReadDataBlockBloomFilters(file, job.blockMetadata)
	if err != nil {
		SendWithContext(ctx, errorChan, fmt.Errorf("failed to read data block bloom filters: %w", err))
		return
	}

	if !b.evaluateBloomFilters(
		blockBloomFilters.FieldBloomFilter,
		blockBloomFilters.TokenBloomFilter,
		blockBloomFilters.FieldTokenBloomFilter,
		bloomQuery,
	) {
		bloomFilterSkipped = true
		return
	}

	rowDataOffset := int64(job.blockMetadata.Offset + job.blockMetadata.BloomFiltersSize)
	if _, err := file.Seek(rowDataOffset, 0); err != nil {
		SendWithContext(ctx, errorChan, fmt.Errorf("failed to seek to row data: %w", err))
		return
	}

	// Calculate compressed row data size (no trailing hash now)
	compressedRowDataSize := job.blockMetadata.Size - job.blockMetadata.BloomFiltersSize

	// Create a limited reader for the compressed row data
	limitedReader := io.LimitReader(file, int64(compressedRowDataSize))

	// Create appropriate decompression reader based on compression type
	var rowDataReader io.Reader
	switch job.blockMetadata.Compression {
	case CompressionNone:
		// Direct streaming from file
		rowDataReader = limitedReader

	case CompressionSnappy:
		// For streaming snappy, we need to read all compressed data first for hash verification
		compressedRowData := make([]byte, compressedRowDataSize)
		if _, err := io.ReadFull(limitedReader, compressedRowData); err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read compressed row data: %w", err))
			return
		}

		// Verify hash if available
		if job.blockMetadata.RowDataHash != 0 {
			computedHash := xxhash.Sum64(compressedRowData)
			if computedHash != job.blockMetadata.RowDataHash {
				SendWithContext(ctx, errorChan, fmt.Errorf("row data hash mismatch: expected %x, got %x", job.blockMetadata.RowDataHash, computedHash))
				return
			}
		}

		// Use streaming snappy decompression to match streaming compression
		compressedReader := bytes.NewReader(compressedRowData)
		snappyReader := snappy.NewReader(compressedReader)
		rowDataReader = snappyReader

	case CompressionZstd:
		// For Zstd, we can use streaming decompression, but we still need to read the full compressed data
		// because zstd.NewReader doesn't accept a size-limited reader reliably
		compressedRowData := make([]byte, compressedRowDataSize)
		if _, err := io.ReadFull(limitedReader, compressedRowData); err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read compressed row data: %w", err))
			return
		}

		// Verify hash if available
		if job.blockMetadata.RowDataHash != 0 {
			computedHash := xxhash.Sum64(compressedRowData)
			if computedHash != job.blockMetadata.RowDataHash {
				SendWithContext(ctx, errorChan, fmt.Errorf("row data hash mismatch: expected %x, got %x", job.blockMetadata.RowDataHash, computedHash))
				return
			}
		}

		decoder, err := zstd.NewReader(nil)
		if err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to create zstd decoder: %w", err))
			return
		}
		defer decoder.Close()

		decompressedData, err := decoder.DecodeAll(compressedRowData, make([]byte, 0, job.blockMetadata.UncompressedSize))
		if err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to decompress zstd data: %w", err))
			return
		}
		rowDataReader = bytes.NewReader(decompressedData)

	default:
		SendWithContext(ctx, errorChan, fmt.Errorf("unsupported compression type: %s", job.blockMetadata.Compression))
		return
	}

	// Now read individual rows from the decompressed stream
	for {
		lengthBytes := make([]byte, LengthPrefixSize)
		n, err := rowDataReader.Read(lengthBytes)
		if err == io.EOF {
			break // End of data
		}
		if err != nil || n != LengthPrefixSize {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read row length: %w", err))
			return
		}

		rowLength := binary.LittleEndian.Uint32(lengthBytes)

		rowData := make([]byte, rowLength)
		n, err = rowDataReader.Read(rowData)
		if err != nil || n != int(rowLength) {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read row data: %w", err))
			return
		}

		if !TestJSONForBloomQuery(rowData, bloomQuery, ".", b.config.Tokenizer) {
			continue
		}

		row := make(map[string]any)
		if err := json.Unmarshal(rowData, &row); err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to unmarshal row: %w", err))
			return
		}

		SendWithContext(ctx, resultChan, row)
	}
}

// merge will evaluate and merge data files to optimize query performance.
func (b *BloomSearchEngine) merge(ctx context.Context) error {
	// Get all files for evaluation
	maybeFiles, err := b.metaStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		return err
	}

	fmt.Printf("Found %d files for merge evaluation\n", len(maybeFiles))

	// Convert to merge candidates with file-level statistics
	var mergeCandidates []fileMergeCandidate
	for _, maybeFile := range maybeFiles {
		candidate := fileMergeCandidate{
			filePointer: maybeFile.PointerBytes,
			metadata:    maybeFile.Metadata,
			statistics:  b.calculateFileStatistics(maybeFile.Metadata),
		}
		mergeCandidates = append(mergeCandidates, candidate)
	}

	// Group files into merge groups (this handles sorting internally)
	mergeGroups := b.identifyFileMergeGroups(mergeCandidates)

	fmt.Println("\nFILE MERGE GROUPS:")
	mergeGroupCount := 0
	totalMergeFiles := 0

	// Show actual merge groups (multiple files)
	for _, group := range mergeGroups {
		totalSize := 0
		totalRows := 0
		totalBlocks := 0
		allPartitions := make(map[string]bool)

		for _, candidate := range group {
			totalSize += candidate.statistics.totalSize
			totalRows += candidate.statistics.totalRows
			totalBlocks += candidate.statistics.blockCount
			for _, partitionID := range candidate.statistics.partitionIDs {
				allPartitions[partitionID] = true
			}
		}

		partitionList := make([]string, 0, len(allPartitions))
		for partitionID := range allPartitions {
			partitionList = append(partitionList, partitionID)
		}

		fmt.Printf("  Group %d: %d files, Partitions: %v, TotalSize: %d, TotalRows: %d, TotalBlocks: %d\n",
			mergeGroupCount, len(group), partitionList, totalSize, totalRows, totalBlocks)
		mergeGroupCount++
		totalMergeFiles += len(group)
	}

	// Show remaining single files that won't be merged
	singleFileCount := 0
	for _, candidate := range mergeCandidates {
		// Check if this file is already in a merge group
		inMergeGroup := false
		for _, group := range mergeGroups {
			for _, groupFile := range group {
				if string(candidate.filePointer) == string(groupFile.filePointer) {
					inMergeGroup = true
					break
				}
			}
			if inMergeGroup {
				break
			}
		}

		if !inMergeGroup {
			fmt.Printf("  Single file %d: Partitions: %v, TotalSize: %d, TotalRows: %d, TotalBlocks: %d\n",
				singleFileCount, candidate.statistics.partitionIDs, candidate.statistics.totalSize,
				candidate.statistics.totalRows, candidate.statistics.blockCount)
			singleFileCount++
		}
	}

	fmt.Printf("\nSUMMARY: %d merge groups (%d files), %d single files, %d total files\n",
		len(mergeGroups), totalMergeFiles, singleFileCount, len(mergeCandidates))

	// TODO: Execute merges by reading entire files and writing new merged files
	// TODO: Update metastore to replace old files with new merged files
	// TODO: if row groups are compressed, we need to create a decompression and compression stream

	return nil
}

// fileMergeCandidate represents a file that could be merged
type fileMergeCandidate struct {
	filePointer []byte
	metadata    FileMetadata
	statistics  fileStatistics
}

// fileStatistics contains pre-calculated statistics about a file
type fileStatistics struct {
	partitionIDs []string
	totalSize    int
	totalRows    int
	blockCount   int
}

// calculateFileStatistics computes basic statistics for a file
func (b *BloomSearchEngine) calculateFileStatistics(metadata FileMetadata) fileStatistics {
	stats := fileStatistics{
		partitionIDs: make([]string, 0),
	}

	partitionSet := make(map[string]bool)

	for _, block := range metadata.DataBlocks {
		// Track unique partitions
		if !partitionSet[block.PartitionID] {
			partitionSet[block.PartitionID] = true
			stats.partitionIDs = append(stats.partitionIDs, block.PartitionID)
		}

		// Accumulate totals
		stats.totalSize += block.Size
		stats.totalRows += block.Rows
		stats.blockCount++
	}

	// Sort partition IDs for consistent ordering
	sort.Strings(stats.partitionIDs)

	return stats
}

// identifyFileMergeGroups groups files that should be merged together
func (b *BloomSearchEngine) identifyFileMergeGroups(files []fileMergeCandidate) [][]fileMergeCandidate {
	if len(files) == 0 {
		return nil
	}

	// Sort files by size first, then by partition for better data locality
	sort.Slice(files, func(i, j int) bool {
		a, b := files[i], files[j]

		// Primary: Sort by size (smaller first for better packing)
		if a.statistics.totalSize != b.statistics.totalSize {
			return a.statistics.totalSize < b.statistics.totalSize
		}

		// Secondary: Sort by first partition for data locality
		// This groups similar partitions together when sizes are equal
		aPartition := ""
		if len(a.statistics.partitionIDs) > 0 {
			aPartition = a.statistics.partitionIDs[0]
		}
		bPartition := ""
		if len(b.statistics.partitionIDs) > 0 {
			bPartition = b.statistics.partitionIDs[0]
		}

		return aPartition < bPartition
	})

	var mergeGroups [][]fileMergeCandidate
	used := make(map[int]bool)

	// Simple greedy approach: try to pack files efficiently
	for i, file := range files {
		if used[i] {
			continue
		}

		currentGroup := []fileMergeCandidate{file}
		currentGroupSize := file.statistics.totalSize
		used[i] = true

		// Add compatible files to this group
		for j := i + 1; j < len(files); j++ {
			if used[j] {
				continue
			}

			candidate := files[j]
			newSize := currentGroupSize + candidate.statistics.totalSize

			if newSize <= b.config.MaxFileSize {
				currentGroup = append(currentGroup, candidate)
				currentGroupSize = newSize
				used[j] = true
			}
		}

		// Only add groups with multiple files
		if len(currentGroup) > 1 {
			mergeGroups = append(mergeGroups, currentGroup)
		}
	}

	return mergeGroups
}
