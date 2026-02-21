package bloomsearch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"
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

// compressionEncoders holds compression-related objects
type compressionEncoders struct {
	writer        io.Writer
	zstdEncoder   *zstd.Encoder
	snappyEncoder *snappy.Writer
}

// createCompressionWriter creates appropriate compression writer based on configuration
func (b *BloomSearchEngine) createCompressionWriter(dest io.Writer) (*compressionEncoders, error) {
	encoders := &compressionEncoders{}

	switch b.config.RowDataCompression {
	case CompressionZstd:
		var err error
		encoders.zstdEncoder, err = zstd.NewWriter(dest, zstd.WithEncoderLevel(zstd.EncoderLevel(b.config.ZstdCompressionLevel)))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}
		encoders.writer = encoders.zstdEncoder
	case CompressionSnappy:
		encoders.snappyEncoder = snappy.NewBufferedWriter(dest)
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

// writeBloomFiltersWithHash serializes bloom filters and writes them with their hash to a writer
func (b *BloomSearchEngine) writeBloomFiltersWithHash(writer io.Writer, bloomFilters *BloomFilters) ([]byte, []byte, int, error) {
	// Serialize bloom filters and get hash
	bloomFiltersBytes, bloomFiltersHashBytes := bloomFilters.Bytes()

	// Write bloom filters
	if _, err := writer.Write(bloomFiltersBytes); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to write bloom filters: %w", err)
	}

	// Write bloom filters hash
	if _, err := writer.Write(bloomFiltersHashBytes); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to write bloom filters hash: %w", err)
	}

	// Return bloom filter bytes, hash bytes, and total size written
	return bloomFiltersBytes, bloomFiltersHashBytes, len(bloomFiltersBytes) + len(bloomFiltersHashBytes), nil
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
	fileBloomFilters BloomFilters
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
	ingestDone  chan struct{}

	querySemaphore chan struct{}
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

type MergeStats struct {
	FilesProcessed     int64
	RowGroupsProcessed int64
	RowsProcessed      int64
	BytesProcessed     int64
	Duration           time.Duration
	RowsPerSecond      float64
	BytesPerSecond     float64
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

	// Maximum number of files to merge together in a single merge operation
	MaxFilesToMergePerOperation int
}

type partitionBuffer struct {
	partitionID           string
	rowCount              int
	minMaxIndexes         map[string]MinMaxIndex
	buffer                bytes.Buffer
	fieldBloomFilter      *bloom.BloomFilter
	tokenBloomFilter      *bloom.BloomFilter
	fieldTokenBloomFilter *bloom.BloomFilter
	compressionEncoders   *compressionEncoders
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

		MaxFilesToMergePerOperation: 10,
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
		ingestDone:     make(chan struct{}),
	}, nil
}

func (b *BloomSearchEngine) newFileLevelBloomFilters() (*bloom.BloomFilter, *bloom.BloomFilter, *bloom.BloomFilter) {
	return bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate),
		bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate),
		bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate)
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
	fileFieldBloomFilter, fileTokenBloomFilter, fileFieldTokenBloomFilter := b.newFileLevelBloomFilters()

	// Create a ticker for periodic time-based flush checks
	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			fmt.Println("ingestWorker context done")
			// Flush any remaining buffered data before exiting
			if bufferedRowCount > 0 {
				b.flushBufferedData(
					partitionBuffers,
					&doneChans,
					&bufferedRowCount,
					&bufferedBytes,
					&bufferStartTime,
					&fileFieldBloomFilter,
					&fileTokenBloomFilter,
					&fileFieldTokenBloomFilter,
				)
			}
			return
		case req := <-b.ingestChan:
			// Process the batch of rows
			b.processIngestRequest(
				b.ctx,
				req,
				partitionBuffers,
				&doneChans,
				&bufferedRowCount,
				&bufferedBytes,
				&bufferStartTime,
				&fileFieldBloomFilter,
				&fileTokenBloomFilter,
				&fileFieldTokenBloomFilter,
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
					&fileFieldBloomFilter,
					&fileTokenBloomFilter,
					&fileFieldTokenBloomFilter,
				)
			}
		}
	}
}

// flushBufferedData flushes the current buffered data and resets the buffer state
func (b *BloomSearchEngine) flushBufferedData(
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
	fileFieldBloomFilter **bloom.BloomFilter,
	fileTokenBloomFilter **bloom.BloomFilter,
	fileFieldTokenBloomFilter **bloom.BloomFilter,
) {
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

	b.triggerFlush(
		partitionBuffersCopy,
		doneChannsCopy,
		BloomFilters{
			FieldBloomFilter:      *fileFieldBloomFilter,
			TokenBloomFilter:      *fileTokenBloomFilter,
			FieldTokenBloomFilter: *fileFieldTokenBloomFilter,
		},
	)

	// Reset local state
	for k := range partitionBuffers {
		delete(partitionBuffers, k)
	}
	*doneChans = make([]chan error, 0)
	*bufferedRowCount = 0
	*bufferedBytes = 0
	*bufferStartTime = time.Time{}
	*fileFieldBloomFilter, *fileTokenBloomFilter, *fileFieldTokenBloomFilter = b.newFileLevelBloomFilters()
}

func (b *BloomSearchEngine) processIngestRequest(
	ctx context.Context,
	req *ingestRequest,
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
	fileFieldBloomFilter **bloom.BloomFilter,
	fileTokenBloomFilter **bloom.BloomFilter,
	fileFieldTokenBloomFilter **bloom.BloomFilter,
) {
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
			b.flushBufferedData(
				partitionBuffers,
				doneChans,
				bufferedRowCount,
				bufferedBytes,
				bufferStartTime,
				fileFieldBloomFilter,
				fileTokenBloomFilter,
				fileFieldTokenBloomFilter,
			)
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
			partitionBuffers[partitionID].compressionEncoders, err = b.createCompressionWriter(&partitionBuffers[partitionID].buffer)
			if err != nil {
				SendWithContext(ctx, req.doneChan, fmt.Errorf("failed to create compression writer: %w", err))
				return
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
				(*fileFieldBloomFilter).AddString(field.Path)

				for _, value := range field.Values {
					tokens := b.config.Tokenizer(value)
					for _, token := range tokens {
						partitionBuffer.tokenBloomFilter.AddString(token)
						partitionBuffer.fieldTokenBloomFilter.AddString(makeFieldTokenKey(field.Path, token))
						// Also add to file-level bloom filters
						(*fileTokenBloomFilter).AddString(token)
						(*fileFieldTokenBloomFilter).AddString(makeFieldTokenKey(field.Path, token))
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
			partitionBuffer.compressionEncoders.writer.Write(lengthBytes)
			partitionBuffer.compressionEncoders.writer.Write(rowBytes)

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
		b.flushBufferedData(
			partitionBuffers,
			doneChans,
			bufferedRowCount,
			bufferedBytes,
			bufferStartTime,
			fileFieldBloomFilter,
			fileTokenBloomFilter,
			fileFieldTokenBloomFilter,
		)
	}
}

func (b *BloomSearchEngine) triggerFlush(partitionBuffers map[string]*partitionBuffer, doneChans []chan error, fileBloomFilters BloomFilters) {
	flushReq := flushRequest{
		partitionBuffers: partitionBuffers,
		doneChans:        doneChans,
		fileBloomFilters: fileBloomFilters,
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

	shuttingDown := false
	for {
		if !shuttingDown {
			select {
			case <-b.ctx.Done():
				shuttingDown = true
			case flushReq := <-b.flushChan:
				b.handleFlush(flushReq)
			}
			continue
		}

		select {
		case flushReq := <-b.flushChan:
			b.handleFlush(flushReq)
		case <-b.ingestDone:
			for {
				select {
				case flushReq := <-b.flushChan:
					b.handleFlush(flushReq)
				default:
					fmt.Println("flushWorker context done")
					return
				}
			}
		}
	}
}

func (b *BloomSearchEngine) handleFlush(flushReq flushRequest) {
	fileMetadata := FileMetadata{
		BloomFilters:           flushReq.fileBloomFilters,
		BloomExpectedItems:     b.config.FileBloomExpectedItems,
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		DataBlocks:             make([]DataBlockMetadata, 0),
	}

	// Stream write to data store
	writer, filePointerBytes, err := b.dataStore.CreateFile(b.ctx)
	if err != nil {
		fmt.Println("failed to create file: %w", err)
		// Write error to all done channels
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to create file: %w", err))
		return
	}

	currentOffset := 0

	// For each partition buffer, write the data block to the data store
	for _, partitionBuffer := range flushReq.partitionBuffers {
		// Finalize compression encoders before writing
		var compressedData []byte
		if err := partitionBuffer.compressionEncoders.finalizeCompression(); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to finalize compression: %w", err))
			return
		}
		compressedData = partitionBuffer.buffer.Bytes()
		// Create data block bloom filters struct
		dataBlockBloomFilters := &BloomFilters{
			FieldBloomFilter:      partitionBuffer.fieldBloomFilter,
			TokenBloomFilter:      partitionBuffer.tokenBloomFilter,
			FieldTokenBloomFilter: partitionBuffer.fieldTokenBloomFilter,
		}

		// Write bloom filters and hash
		_, _, bloomFiltersSize, err := b.writeBloomFiltersWithHash(writer, dataBlockBloomFilters)
		if err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write bloom filters: %w", err))
			return
		}

		// Calculate hash of compressed row data (CRC32C)
		rowDataHash := crc32.Checksum(compressedData, crc32cTable)

		// Write the row data buffer
		if _, err := writer.Write(compressedData); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write data block: %w", err))
			return
		}

		// No file-level hash currently needed

		dataBlockSize := bloomFiltersSize + len(compressedData)

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:            partitionBuffer.partitionID,
			Rows:                   partitionBuffer.rowCount,
			Offset:                 currentOffset,
			Size:                   dataBlockSize,
			BloomFiltersSize:       bloomFiltersSize,
			MinMaxIndexes:          partitionBuffer.minMaxIndexes,
			Compression:            b.config.RowDataCompression,
			UncompressedSize:       partitionBuffer.uncompressedSize,
			RowDataHash:            rowDataHash,
			BloomExpectedItems:     uint(b.config.MaxRowGroupRows),
			BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		})

		currentOffset += dataBlockSize
	}

	// Write final metadata to data store and footer
	if err := b.writeFileMetadataAndFooter(writer, &fileMetadata); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata and footer: %w", err))
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

	TryWriteToChannels(flushReq.doneChans, nil)
}

// evaluateBloomFilters tests if bloom filters match the bloom query
func (b *BloomSearchEngine) evaluateBloomFilters(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	bloomQuery *BloomQuery,
) bool {
	if bloomQuery == nil || bloomQuery.Expression == nil {
		return true // No bloom filtering needed, since it's only used to DISQUALIFY files
	}

	return b.evaluateBloomExpression(fieldFilter, tokenFilter, fieldTokenFilter, bloomQuery.Expression)
}

// evaluateBloomExpression tests if bloom filters match a bloom expression tree
func (b *BloomSearchEngine) evaluateBloomExpression(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	expression *BloomExpression,
) bool {
	if expression == nil {
		return true
	}

	switch expression.expressionType {
	case bloomExpressionCondition:
		if expression.condition == nil {
			return true
		}
		return b.evaluateBloomCondition(fieldFilter, tokenFilter, fieldTokenFilter, expression.condition)
	case bloomExpressionOr:
		if len(expression.children) == 0 {
			return false
		}
		for i := range expression.children {
			if b.evaluateBloomExpression(fieldFilter, tokenFilter, fieldTokenFilter, &expression.children[i]) {
				return true
			}
		}
		return false
	case bloomExpressionAnd:
		for i := range expression.children {
			if !b.evaluateBloomExpression(fieldFilter, tokenFilter, fieldTokenFilter, &expression.children[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
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
		// If the filter is nil (e.g., not present in metadata), we cannot disqualify
		if fieldFilter == nil {
			fmt.Println("[WARNING] fieldFilter is nil, how did your file not have this filter?")
			return true
		}
		result = fieldFilter.TestString(condition.Field)
	case BloomToken:
		if tokenFilter == nil {
			fmt.Println("[WARNING] tokenFilter is nil, how did your file not have this filter?")
			return true
		}
		result = tokenFilter.TestString(condition.Token)
	case BloomFieldToken:
		if fieldTokenFilter == nil {
			fmt.Println("[WARNING] fieldTokenFilter is nil, how did your file not have this filter?")
			return true
		}
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
	if query == nil {
		query = NewQuery().Build()
	}

	rowBloomQuery := query.Bloom
	if rowBloomQuery == nil {
		rowBloomQuery = &BloomQuery{}
	}

	compiledRegexQuery, err := CompileRegexQuery(query.Regex)
	if err != nil {
		return fmt.Errorf("failed to compile regex query: %w", err)
	}

	pruneBloomQuery := AndBloomQueries(rowBloomQuery, RegexFieldGuardBloomQuery(query.Regex))

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
				maybeFile.Metadata.BloomFilters.FieldBloomFilter,
				maybeFile.Metadata.BloomFilters.TokenBloomFilter,
				maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter,
				pruneBloomQuery,
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
					maybeFile.Metadata.BloomFilters.FieldBloomFilter,
					maybeFile.Metadata.BloomFilters.TokenBloomFilter,
					maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter,
					pruneBloomQuery,
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

			b.processDataBlock(workerCtx, job, resultChan, errorChan, rowBloomQuery, pruneBloomQuery, compiledRegexQuery, statsChan)
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
	rowBloomQuery *BloomQuery,
	pruneBloomQuery *BloomQuery,
	regexQuery *compiledRegexQuery,
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
		pruneBloomQuery,
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

	// Create hash-calculating reader to verify hash while streaming
	hashReader := newHashCalculatingReader(limitedReader, int64(compressedRowDataSize))

	// Create appropriate decompression reader based on compression type
	var rowDataReader io.Reader
	switch job.blockMetadata.Compression {
	case CompressionNone:
		rowDataReader = hashReader
	case CompressionSnappy:
		rowDataReader = snappy.NewReader(hashReader)
	case CompressionZstd:
		decoder, err := zstd.NewReader(hashReader)
		if err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to create zstd decoder: %w", err))
			return
		}
		rowDataReader = decoder
		defer decoder.Close()
	default:
		SendWithContext(ctx, errorChan, fmt.Errorf("unsupported compression type: %s", job.blockMetadata.Compression))
		return
	}

	// Now read individual rows from the decompressed stream
	var lengthBytes [LengthPrefixSize]byte
	rowBuf := make([]byte, 0)
	for {
		n, err := io.ReadFull(rowDataReader, lengthBytes[:])
		if err == io.EOF {
			break // End of data
		}
		if err != nil || n != LengthPrefixSize {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read row length: %w", err))
			return
		}

		rowLength := binary.LittleEndian.Uint32(lengthBytes[:])

		if cap(rowBuf) < int(rowLength) {
			rowBuf = make([]byte, int(rowLength))
		} else {
			rowBuf = rowBuf[:int(rowLength)]
		}

		n, err = io.ReadFull(rowDataReader, rowBuf)
		if err != nil || n != int(rowLength) {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read row data: %w", err))
			return
		}

		rowValue := gjson.ParseBytes(rowBuf)
		if !TestGJSONForQuery(rowValue, rowBloomQuery, regexQuery, ".", b.config.Tokenizer) {
			continue
		}

		row := make(map[string]any)
		if err := json.Unmarshal(rowBuf, &row); err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to unmarshal row: %w", err))
			return
		}

		SendWithContext(ctx, resultChan, row)
	}

	// Verify hash after all data has been read
	if job.blockMetadata.RowDataHash != 0 {
		computedHash := hashReader.Sum32()
		if computedHash != job.blockMetadata.RowDataHash {
			SendWithContext(ctx, errorChan, fmt.Errorf("row data hash mismatch: expected %x, got %x", job.blockMetadata.RowDataHash, computedHash))
			return
		}
	}
}

// Merge executes file merging to optimize storage and query performance
func (b *BloomSearchEngine) Merge(ctx context.Context) (*MergeStats, error) {
	return b.merge(ctx)
}

// merge will evaluate and merge data files to optimize query performance.
func (b *BloomSearchEngine) merge(ctx context.Context) (*MergeStats, error) {
	mergeStartTime := time.Now()

	// Get all files for evaluation
	maybeFiles, err := b.metaStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		return nil, err
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

	// Calculate statistics for files that will be merged
	var totalFilesProcessed int64
	var totalRowGroupsProcessed int64
	var totalRowsProcessed int64
	var totalBytesProcessed int64

	for _, group := range mergeGroups {
		for _, candidate := range group {
			totalFilesProcessed++
			totalRowGroupsProcessed += int64(len(candidate.metadata.DataBlocks))
			for _, block := range candidate.metadata.DataBlocks {
				totalRowsProcessed += int64(block.Rows)
				totalBytesProcessed += int64(block.Size)
			}
		}
	}

	// Execute merges for each group
	var writeOps []WriteOperation
	var deleteOps []DeleteOperation

	for groupIndex, group := range mergeGroups {
		fmt.Printf("Merging group %d with %d files...\n", groupIndex, len(group))

		newFilePointer, newFileMetadata, err := b.executeMergeGroup(ctx, group)
		if err != nil {
			return nil, fmt.Errorf("failed to merge group %d: %w", groupIndex, err)
		}

		// Add new file to write operations
		writeOps = append(writeOps, WriteOperation{
			FileMetadata:     newFileMetadata,
			FilePointerBytes: newFilePointer,
		})

		// Add old files to delete operations
		for _, candidate := range group {
			deleteOps = append(deleteOps, DeleteOperation{
				FilePointerBytes: candidate.filePointer,
			})
		}

		fmt.Printf("Successfully merged group %d into new file\n", groupIndex)
	}

	// Update metastore: add new files and remove old ones
	if len(writeOps) > 0 {
		fmt.Printf("Updating metastore: adding %d new files, removing %d old files\n", len(writeOps), len(deleteOps))
		if err := b.metaStore.Update(ctx, writeOps, deleteOps); err != nil {
			return nil, fmt.Errorf("failed to update metastore after merge: %w", err)
		}
		fmt.Printf("Metastore update completed successfully\n")
	}

	// Calculate final statistics
	duration := time.Since(mergeStartTime)
	stats := &MergeStats{
		FilesProcessed:     totalFilesProcessed,
		RowGroupsProcessed: totalRowGroupsProcessed,
		RowsProcessed:      totalRowsProcessed,
		BytesProcessed:     totalBytesProcessed,
		Duration:           duration,
	}

	// Calculate rates
	if duration.Seconds() > 0 {
		stats.RowsPerSecond = float64(totalRowsProcessed) / duration.Seconds()
		stats.BytesPerSecond = float64(totalBytesProcessed) / duration.Seconds()
	}

	return stats, nil
}

// dataBlocksAreMergeable checks if two data blocks can be merged together
func (b *BloomSearchEngine) dataBlocksAreMergeable(block1, block2 DataBlockMetadata) bool {
	// Must have the same partition ID
	if block1.PartitionID != block2.PartitionID {
		return false
	}

	if block1.BloomExpectedItems != block2.BloomExpectedItems ||
		block1.BloomFalsePositiveRate != block2.BloomFalsePositiveRate {
		return false
	}

	// Check if merging would exceed size limits
	combinedRows := block1.Rows + block2.Rows
	combinedUncompressedSize := block1.UncompressedSize + block2.UncompressedSize

	if combinedRows > b.config.MaxRowGroupRows {
		return false
	}

	if combinedUncompressedSize > b.config.MaxRowGroupBytes {
		return false
	}

	return true
}

// mergeBloomFiltersStruct merges two bloom filters by performing Merge operation
func (b *BloomSearchEngine) mergeBloomFiltersStruct(filters1, filters2 *BloomFilters) (*BloomFilters, error) {
	// Copy the first filter
	merged := &BloomFilters{
		FieldBloomFilter:      filters1.FieldBloomFilter.Copy(),
		TokenBloomFilter:      filters1.TokenBloomFilter.Copy(),
		FieldTokenBloomFilter: filters1.FieldTokenBloomFilter.Copy(),
	}

	// Merge the second filter into the copy
	// This will fail if the filters have incompatible parameters (different m, k values)
	if err := merged.FieldBloomFilter.Merge(filters2.FieldBloomFilter); err != nil {
		return nil, fmt.Errorf("failed to merge bloom filters: %w", err)
	}
	if err := merged.TokenBloomFilter.Merge(filters2.TokenBloomFilter); err != nil {
		return nil, fmt.Errorf("failed to merge bloom filters: %w", err)
	}
	if err := merged.FieldTokenBloomFilter.Merge(filters2.FieldTokenBloomFilter); err != nil {
		return nil, fmt.Errorf("failed to merge bloom filters: %w", err)
	}

	return merged, nil
}

// mergeMinMaxIndexes merges minmax indexes from two data blocks
func (b *BloomSearchEngine) mergeMinMaxIndexes(indexes1, indexes2 map[string]MinMaxIndex) map[string]MinMaxIndex {
	merged := make(map[string]MinMaxIndex)

	// Copy all indexes from first block
	for key, index := range indexes1 {
		merged[key] = index
	}

	// Merge indexes from second block
	for key, index2 := range indexes2 {
		if index1, exists := merged[key]; exists {
			// Merge the indexes
			merged[key] = UpdateMinMaxIndex(index1, index2.Min, index2.Max)
		} else {
			// Add new index
			merged[key] = index2
		}
	}

	return merged
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

// identifyFileMergeGroups groups files that should be merged together using smart row group merging
func (b *BloomSearchEngine) identifyFileMergeGroups(files []fileMergeCandidate) [][]fileMergeCandidate {
	if len(files) == 0 {
		return nil
	}

	// Group files by bloom filter parameters
	type bloomFilterParams struct {
		expectedItems     uint
		falsePositiveRate float64
	}

	parameterGroups := make(map[bloomFilterParams][]fileMergeCandidate)
	for _, file := range files {
		params := bloomFilterParams{
			expectedItems:     file.metadata.BloomExpectedItems,
			falsePositiveRate: file.metadata.BloomFalsePositiveRate,
		}
		parameterGroups[params] = append(parameterGroups[params], file)
	}

	var mergeGroups [][]fileMergeCandidate
	totalFilesInGroups := 0

	for _, compatibleFiles := range parameterGroups {
		if len(compatibleFiles) < 2 {
			continue
		}

		// Sort files by potential for merging (smaller files first, then by partition locality)
		sort.Slice(compatibleFiles, func(i, j int) bool {
			a, b := compatibleFiles[i], compatibleFiles[j]

			// Primary: Prefer files with smaller average block sizes (more opportunity for merging)
			aAvgBlockSize := a.statistics.totalSize / max(a.statistics.blockCount, 1)
			bAvgBlockSize := b.statistics.totalSize / max(b.statistics.blockCount, 1)

			if aAvgBlockSize != bAvgBlockSize {
				return aAvgBlockSize < bAvgBlockSize
			}

			// Secondary: Sort by total size (smaller first)
			return a.statistics.totalSize < b.statistics.totalSize
		})

		// Track which files have already been assigned to a group
		fileAssigned := make(map[int]bool)

		// Greedy approach: try to group files that can benefit from row group merging
		for i, file := range compatibleFiles {
			if fileAssigned[i] {
				continue
			}

			if totalFilesInGroups >= b.config.MaxFilesToMergePerOperation {
				break
			}

			currentGroup := []fileMergeCandidate{file}
			currentGroupSize := file.statistics.totalSize
			fileAssigned[i] = true

			// Add compatible files to this group
			for j := i + 1; j < len(compatibleFiles); j++ {
				if fileAssigned[j] {
					continue
				}

				if totalFilesInGroups+len(currentGroup)+1 > b.config.MaxFilesToMergePerOperation {
					break
				}

				candidate := compatibleFiles[j]

				newSize := currentGroupSize + candidate.statistics.totalSize
				if newSize > b.config.MaxFileSize {
					continue
				}

				if b.hasCompatibleRowGroups(currentGroup, candidate) {
					currentGroup = append(currentGroup, candidate)
					currentGroupSize = newSize
					fileAssigned[j] = true
				}
			}

			if len(currentGroup) > 1 {
				mergeGroups = append(mergeGroups, currentGroup)
				totalFilesInGroups += len(currentGroup)
			}
		}

		if totalFilesInGroups >= b.config.MaxFilesToMergePerOperation {
			break
		}
	}

	return mergeGroups
}

// hasCompatibleRowGroups checks if a candidate file has row groups that can be merged with existing group
func (b *BloomSearchEngine) hasCompatibleRowGroups(currentGroup []fileMergeCandidate, candidate fileMergeCandidate) bool {
	// Check if candidate has row groups that can be merged with any in the current group
	for _, groupFile := range currentGroup {
		for _, candidateBlock := range candidate.metadata.DataBlocks {
			for _, groupBlock := range groupFile.metadata.DataBlocks {
				if b.dataBlocksAreMergeable(candidateBlock, groupBlock) {
					return true
				}
			}
		}
	}
	return false
}

// executeMergeGroup merges a group of files with smart row group merging
func (b *BloomSearchEngine) executeMergeGroup(ctx context.Context, group []fileMergeCandidate) ([]byte, *FileMetadata, error) {
	// Initialize new file-level bloom filters by merging from source files
	newFileFieldBloomFilter := bloom.NewWithEstimates(group[0].metadata.BloomExpectedItems, group[0].metadata.BloomFalsePositiveRate)
	newFileTokenBloomFilter := bloom.NewWithEstimates(group[0].metadata.BloomExpectedItems, group[0].metadata.BloomFalsePositiveRate)
	newFileFieldTokenBloomFilter := bloom.NewWithEstimates(group[0].metadata.BloomExpectedItems, group[0].metadata.BloomFalsePositiveRate)

	// Merge file-level bloom filters from all source files
	for _, candidate := range group {
		if err := newFileFieldBloomFilter.Merge(candidate.metadata.BloomFilters.FieldBloomFilter); err != nil {
			return nil, nil, fmt.Errorf("failed to merge file field bloom filter: %w", err)
		}
		if err := newFileTokenBloomFilter.Merge(candidate.metadata.BloomFilters.TokenBloomFilter); err != nil {
			return nil, nil, fmt.Errorf("failed to merge file token bloom filter: %w", err)
		}
		if err := newFileFieldTokenBloomFilter.Merge(candidate.metadata.BloomFilters.FieldTokenBloomFilter); err != nil {
			return nil, nil, fmt.Errorf("failed to merge file field-token bloom filter: %w", err)
		}
	}

	// Create new file for writing
	writer, filePointerBytes, err := b.dataStore.CreateFile(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create merge file: %w", err)
	}
	defer writer.Close()

	var newDataBlocks []DataBlockMetadata
	currentOffset := 0

	// Collect all data blocks from all files with their file references
	var allBlocks []blockWithFile
	openFiles := make(map[string]io.ReadSeekCloser)

	// Open all files and collect blocks
	for _, candidate := range group {
		fileKey := string(candidate.filePointer)
		if openFiles[fileKey] == nil {
			sourceFile, err := b.dataStore.OpenFile(ctx, candidate.filePointer)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to open source file for merge: %w", err)
			}
			openFiles[fileKey] = sourceFile
			defer sourceFile.Close()
		}

		for _, blockMetadata := range candidate.metadata.DataBlocks {
			allBlocks = append(allBlocks, blockWithFile{
				block:       blockMetadata,
				file:        openFiles[fileKey],
				filePointer: candidate.filePointer,
				processed:   false,
			})
		}
	}

	// Group blocks by partition for potential merging
	partitionBlocks := make(map[string][]int) // partition -> indices into allBlocks
	for i, block := range allBlocks {
		partitionBlocks[block.block.PartitionID] = append(partitionBlocks[block.block.PartitionID], i)
	}

	// Process each partition
	for partitionID, blockIndices := range partitionBlocks {
		err := b.processPartitionBlocks(writer, allBlocks, blockIndices, partitionID, &currentOffset, &newDataBlocks)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to process partition %s: %w", partitionID, err)
		}
	}

	// Create new file metadata
	newFileMetadata := &FileMetadata{
		BloomFilters: BloomFilters{
			FieldBloomFilter:      newFileFieldBloomFilter,
			TokenBloomFilter:      newFileTokenBloomFilter,
			FieldTokenBloomFilter: newFileFieldTokenBloomFilter,
		},
		BloomExpectedItems:     b.config.FileBloomExpectedItems,
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		DataBlocks:             newDataBlocks,
	}

	// Write file metadata and footer
	if err := b.writeFileMetadataAndFooter(writer, newFileMetadata); err != nil {
		return nil, nil, fmt.Errorf("failed to write file metadata: %w", err)
	}

	return filePointerBytes, newFileMetadata, nil
}

// blockWithFile represents a data block with its associated file handle
type blockWithFile struct {
	block       DataBlockMetadata
	file        io.ReadSeeker
	filePointer []byte
	processed   bool
}

// processPartitionBlocks handles merging data blocks for a single partition
func (b *BloomSearchEngine) processPartitionBlocks(writer io.Writer, allBlocks []blockWithFile, blockIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata) error {
	// Group mergeable blocks together
	var mergeGroups [][]int // groups of block indices that can be merged
	processed := make(map[int]bool)

	for _, blockIdx := range blockIndices {
		if processed[blockIdx] {
			continue
		}

		currentGroup := []int{blockIdx}
		currentRows := allBlocks[blockIdx].block.Rows
		currentSize := allBlocks[blockIdx].block.UncompressedSize
		processed[blockIdx] = true

		// Find blocks that can be merged with this one
		for _, otherIdx := range blockIndices {
			if processed[otherIdx] {
				continue
			}

			otherBlock := allBlocks[otherIdx].block
			if b.dataBlocksAreMergeable(allBlocks[blockIdx].block, otherBlock) {
				// Check if adding this block would exceed limits
				if currentRows+otherBlock.Rows <= b.config.MaxRowGroupRows &&
					currentSize+otherBlock.UncompressedSize <= b.config.MaxRowGroupBytes {
					currentGroup = append(currentGroup, otherIdx)
					currentRows += otherBlock.Rows
					currentSize += otherBlock.UncompressedSize
					processed[otherIdx] = true
				}
			}
		}

		mergeGroups = append(mergeGroups, currentGroup)
	}

	// Process each merge group
	for _, group := range mergeGroups {
		if len(group) == 1 {
			// Single block, just copy it
			blockIdx := group[0]
			err := b.copyDataBlock(writer, allBlocks[blockIdx], currentOffset, newDataBlocks)
			if err != nil {
				return fmt.Errorf("failed to copy data block: %w", err)
			}
		} else {
			// Multiple blocks, merge them
			err := b.mergeDataBlocks(writer, allBlocks, group, partitionID, currentOffset, newDataBlocks)
			if err != nil {
				return fmt.Errorf("failed to merge data blocks: %w", err)
			}
		}
	}

	return nil
}

// copyDataBlock copies a single data block to the output file
func (b *BloomSearchEngine) copyDataBlock(writer io.Writer, blockWithFile blockWithFile, currentOffset *int, newDataBlocks *[]DataBlockMetadata) error {
	// Seek to the start of the source block
	if _, err := blockWithFile.file.Seek(int64(blockWithFile.block.Offset), 0); err != nil {
		return fmt.Errorf("failed to seek to source block: %w", err)
	}

	// Stream copy the entire block (bloom filters + hash + row data) as raw bytes
	copied, err := io.CopyN(writer, blockWithFile.file, int64(blockWithFile.block.Size))
	if err != nil {
		return fmt.Errorf("failed to copy block data: %w", err)
	}
	if copied != int64(blockWithFile.block.Size) {
		return fmt.Errorf("incomplete copy: expected %d bytes, copied %d bytes", blockWithFile.block.Size, copied)
	}

	// Create new block metadata with updated offset (everything else stays the same)
	newBlockMetadata := blockWithFile.block // copy the struct
	newBlockMetadata.Offset = *currentOffset

	*newDataBlocks = append(*newDataBlocks, newBlockMetadata)
	*currentOffset += newBlockMetadata.Size

	return nil
}

// mergeDataBlocks merges multiple data blocks into a single optimized data block using streaming
func (b *BloomSearchEngine) mergeDataBlocks(writer io.Writer, allBlocks []blockWithFile, groupIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata) error {
	// Create streaming readers for each block
	var readers []*dataBlockRowReader
	var mergedBloomFilters *BloomFilters
	var mergedMinMaxIndexes map[string]MinMaxIndex

	for i, blockIdx := range groupIndices {
		blockWithFile := allBlocks[blockIdx]

		// Create streaming reader for this block
		reader, err := b.newDataBlockRowReader(blockWithFile.file, blockWithFile.block)
		if err != nil {
			return fmt.Errorf("failed to create row reader for data block: %w", err)
		}
		readers = append(readers, reader)

		// Merge bloom filters
		if i == 0 {
			mergedBloomFilters = reader.bloomFilters
			mergedMinMaxIndexes = blockWithFile.block.MinMaxIndexes
		} else {
			mergedBloomFilters, err = b.mergeBloomFiltersStruct(mergedBloomFilters, reader.bloomFilters)
			if err != nil {
				return fmt.Errorf("failed to merge bloom filters: %w", err)
			}
			mergedMinMaxIndexes = b.mergeMinMaxIndexes(mergedMinMaxIndexes, blockWithFile.block.MinMaxIndexes)
		}
	}

	// Stream merge the data blocks
	newBlockMetadata, err := b.streamMergeDataBlocks(writer, readers, partitionID, mergedBloomFilters, mergedMinMaxIndexes, *currentOffset)
	if err != nil {
		return fmt.Errorf("failed to stream merge data blocks: %w", err)
	}

	*newDataBlocks = append(*newDataBlocks, *newBlockMetadata)
	*currentOffset += newBlockMetadata.Size

	return nil
}

// streamMergeDataBlocks performs streaming merge of multiple data block readers
func (b *BloomSearchEngine) streamMergeDataBlocks(writer io.Writer, readers []*dataBlockRowReader, partitionID string, bloomFilters *BloomFilters, minMaxIndexes map[string]MinMaxIndex, offset int) (*DataBlockMetadata, error) {
	// Serialize and write bloom filters
	_, _, bloomFiltersSize, err := b.writeBloomFiltersWithHash(writer, bloomFilters)
	if err != nil {
		return nil, fmt.Errorf("failed to write bloom filters: %w", err)
	}

	// Prepare compressed row data
	var compressedData bytes.Buffer
	uncompressedSize := 0
	rowCount := 0

	compressionEncoders, err := b.createCompressionWriter(&compressedData)
	if err != nil {
		return nil, fmt.Errorf("failed to create compression writer: %w", err)
	}
	rowDataWriter := compressionEncoders.writer

	// Stream merge all readers (simple round-robin for now, could be more sophisticated)
	for {
		hasData := false

		// Check each reader and write any available rows
		for _, reader := range readers {
			if reader.hasMore && reader.err == nil {
				rowBytes := reader.getCurrentRow()
				if rowBytes != nil {
					hasData = true

					if len(rowBytes) > 0xFFFFFFFF {
						return nil, fmt.Errorf("row too large: %d bytes exceeds maximum", len(rowBytes))
					}

					// Write length prefix and row data
					lengthBytes := make([]byte, LengthPrefixSize)
					binary.LittleEndian.PutUint32(lengthBytes, uint32(len(rowBytes)))

					if _, err := rowDataWriter.Write(lengthBytes); err != nil {
						return nil, fmt.Errorf("failed to write row length: %w", err)
					}
					if _, err := rowDataWriter.Write(rowBytes); err != nil {
						return nil, fmt.Errorf("failed to write row data: %w", err)
					}

					uncompressedSize += len(rowBytes) + LengthPrefixSize
					rowCount++
				}
			} else {
			}

			// Check for reader errors
			if reader.err != nil {
				return nil, fmt.Errorf("error reading from data block: %w", reader.err)
			}
		}

		// If no readers have data, we're done
		if !hasData {
			break
		}
	}

	// Finalize compression
	if err := compressionEncoders.finalizeCompression(); err != nil {
		return nil, fmt.Errorf("failed to finalize compression: %w", err)
	}

	// Calculate hash and write compressed data
	rowDataHash := crc32.Checksum(compressedData.Bytes(), crc32cTable)
	if _, err := writer.Write(compressedData.Bytes()); err != nil {
		return nil, fmt.Errorf("failed to write compressed row data: %w", err)
	}

	totalSize := bloomFiltersSize + compressedData.Len()

	return &DataBlockMetadata{
		PartitionID:            partitionID,
		Rows:                   rowCount,
		Offset:                 offset,
		Size:                   totalSize,
		BloomFiltersSize:       bloomFiltersSize,
		MinMaxIndexes:          minMaxIndexes,
		Compression:            b.config.RowDataCompression,
		UncompressedSize:       uncompressedSize,
		RowDataHash:            rowDataHash,
		BloomExpectedItems:     uint(b.config.MaxRowGroupRows),
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
	}, nil
}

// dataBlockRowReader provides streaming access to rows from a data block
type dataBlockRowReader struct {
	rowDataReader io.Reader
	bloomFilters  *BloomFilters
	hasMore       bool
	currentRow    []byte
	err           error
	hashReader    *hashCalculatingReader
	expectedHash  uint32
	hashVerified  bool
	zstdDecoder   *zstd.Decoder
}

// newDataBlockRowReader creates a streaming reader for a data block
func (b *BloomSearchEngine) newDataBlockRowReader(file io.ReadSeeker, blockMetadata DataBlockMetadata) (*dataBlockRowReader, error) {
	// Read bloom filters first
	blockBloomFilters, err := ReadDataBlockBloomFilters(file, blockMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filters: %w", err)
	}

	// Seek to row data
	rowDataOffset := int64(blockMetadata.Offset + blockMetadata.BloomFiltersSize)
	if _, err := file.Seek(rowDataOffset, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to row data: %w", err)
	}

	// Create streaming reader for compressed data with hash calculation
	compressedRowDataSize := blockMetadata.Size - blockMetadata.BloomFiltersSize
	limitedReader := io.LimitReader(file, int64(compressedRowDataSize))

	// Create hash-calculating reader to verify hash while streaming
	hashReader := newHashCalculatingReader(limitedReader, int64(compressedRowDataSize))

	// Create appropriate decompression reader based on compression type
	var rowDataReader io.Reader
	var zstdDec *zstd.Decoder
	switch blockMetadata.Compression {
	case CompressionNone:
		rowDataReader = hashReader
	case CompressionSnappy:
		rowDataReader = snappy.NewReader(hashReader)
	case CompressionZstd:
		// Use streaming zstd decompression
		decoder, err := zstd.NewReader(hashReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		rowDataReader = decoder
		zstdDec = decoder
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", blockMetadata.Compression)
	}

	// Hash will be verified when EOF is reached (or no verification needed if no hash)
	hashAlreadyVerified := blockMetadata.RowDataHash == 0

	reader := &dataBlockRowReader{
		rowDataReader: rowDataReader,
		bloomFilters:  blockBloomFilters,
		hasMore:       true,
		hashReader:    hashReader,
		expectedHash:  blockMetadata.RowDataHash,
		hashVerified:  hashAlreadyVerified,
	}
	if zstdDec != nil {
		reader.zstdDecoder = zstdDec
	}

	// Read the first row to initialize
	reader.next()

	return reader, nil
}

// next reads the next row from the stream
func (r *dataBlockRowReader) next() {
	if !r.hasMore || r.err != nil {
		return
	}

	var lengthBytes [LengthPrefixSize]byte
	n, err := io.ReadFull(r.rowDataReader, lengthBytes[:])
	if err == io.EOF {
		r.hasMore = false
		// Verify hash if we haven't already and we have a hash to verify
		if !r.hashVerified && r.expectedHash != 0 && r.hashReader != nil {
			computedHash := r.hashReader.Sum32()
			if computedHash != r.expectedHash {
				r.err = fmt.Errorf("row data hash mismatch: expected %x, got %x", r.expectedHash, computedHash)
				r.closeDecoder()
				return
			}
			r.hashVerified = true
		}
		r.closeDecoder()
		return
	}
	if err != nil || n != LengthPrefixSize {
		r.err = fmt.Errorf("failed to read row length: %w", err)
		r.hasMore = false
		r.closeDecoder()
		return
	}

	rowLength := binary.LittleEndian.Uint32(lengthBytes[:])
	if cap(r.currentRow) < int(rowLength) {
		r.currentRow = make([]byte, int(rowLength))
	} else {
		r.currentRow = r.currentRow[:int(rowLength)]
	}
	n, err = io.ReadFull(r.rowDataReader, r.currentRow)
	if err != nil || n != int(rowLength) {
		r.err = fmt.Errorf("failed to read row data: %w", err)
		r.hasMore = false
		r.closeDecoder()
		return
	}
}

// closeDecoder closes the zstd decoder if present (idempotent)
func (r *dataBlockRowReader) closeDecoder() {
	if r.zstdDecoder != nil {
		r.zstdDecoder.Close()
		r.zstdDecoder = nil
	}
}

// getCurrentRow returns the current row and advances to the next
func (r *dataBlockRowReader) getCurrentRow() []byte {
	if !r.hasMore || r.err != nil {
		return nil
	}

	// Copy to preserve contents across advance when buffers are reused
	out := make([]byte, len(r.currentRow))
	copy(out, r.currentRow)
	r.next() // advance to next row
	return out
}

// writeFileMetadataAndFooter writes the file metadata and footer to complete the file
func (b *BloomSearchEngine) writeFileMetadataAndFooter(writer io.Writer, metadata *FileMetadata) error {
	// Write file metadata
	metadataBytes, metadataHashBytes := metadata.Bytes()
	if _, err := writer.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write file metadata: %w", err)
	}
	if _, err := writer.Write(metadataHashBytes); err != nil {
		return fmt.Errorf("failed to write file metadata hash: %w", err)
	}

	// Write metadata length
	metadataLengthBytes := make([]byte, LengthPrefixSize)
	binary.LittleEndian.PutUint32(metadataLengthBytes, uint32(len(metadataBytes)))
	if _, err := writer.Write(metadataLengthBytes); err != nil {
		return fmt.Errorf("failed to write file metadata length: %w", err)
	}

	// Write version
	versionBytes := make([]byte, VersionPrefixSize)
	binary.LittleEndian.PutUint32(versionBytes, FileVersion)
	if _, err := writer.Write(versionBytes); err != nil {
		return fmt.Errorf("failed to write file version: %w", err)
	}

	// Write magic bytes
	if _, err := writer.Write([]byte(MagicBytes)); err != nil {
		return fmt.Errorf("failed to write magic bytes: %w", err)
	}

	return nil
}

// hashCalculatingReader wraps an io.Reader and calculates checksum as data is read
type hashCalculatingReader struct {
	reader    io.Reader
	hasher    hash.Hash32
	totalRead int64
	limit     int64
}

func newHashCalculatingReader(reader io.Reader, limit int64) *hashCalculatingReader {
	return &hashCalculatingReader{
		reader: reader,
		hasher: crc32.New(crc32cTable),
		limit:  limit,
	}
}

func (h *hashCalculatingReader) Read(p []byte) (n int, err error) {
	if h.totalRead >= h.limit {
		return 0, io.EOF
	}

	// Don't read more than our limit
	if int64(len(p)) > h.limit-h.totalRead {
		p = p[:h.limit-h.totalRead]
	}

	n, err = h.reader.Read(p)
	if n > 0 {
		h.hasher.Write(p[:n])
		h.totalRead += int64(n)
	}
	return n, err
}

func (h *hashCalculatingReader) Sum64() uint64 {
	// Maintain compatibility with callers expecting Sum64; derive from 32-bit checksum
	return uint64(h.hasher.Sum32())
}

func (h *hashCalculatingReader) Sum32() uint32 {
	return h.hasher.Sum32()
}
