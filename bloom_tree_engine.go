package bloomsearch

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration")
)

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
}

type BloomSearchEngineConfig struct {
	Tokenizer     ValueTokenizerFunc
	PartitionFunc PartitionFunc

	MinMaxIndexes []string

	MaxRowGroupBytes int
	MaxRowGroupRows  int

	MaxBufferedRows  int
	MaxBufferedBytes int
	MaxBufferedTime  time.Duration

	IngestBufferSize int // size of the ingestion channel buffer

	// Bloom filter parameters
	BloomFilterExpectedElements  uint
	BloomFilterFalsePositiveRate float64
}

type partitionBuffer struct {
	partitionID           string
	rowCount              int
	minMaxIndexes         map[string]MinMaxIndex
	buffer                []byte
	fieldBloomFilter      *bloom.BloomFilter
	tokenBloomFilter      *bloom.BloomFilter
	fieldTokenBloomFilter *bloom.BloomFilter
}

func DefaultBloomSearchEngineConfig() BloomSearchEngineConfig {
	return BloomSearchEngineConfig{
		Tokenizer: BasicWhitespaceTokenizer,

		MaxRowGroupBytes: 10 * 1024 * 1024,
		MaxRowGroupRows:  10000,

		MaxBufferedRows:  1000,
		MaxBufferedBytes: 1 * 1024 * 1024,
		MaxBufferedTime:  10 * time.Second, // this is designed for async writing

		IngestBufferSize: 1_000,

		BloomFilterExpectedElements:  100_000,
		BloomFilterFalsePositiveRate: 0.001,
	}
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) (*BloomSearchEngine, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Tokenizer == nil {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: tokenizer is required", ErrInvalidConfig)
	}

	if config.BloomFilterExpectedElements == 0 {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: BloomFilterExpectedElements must be greater than 0", ErrInvalidConfig)
	}

	if config.BloomFilterFalsePositiveRate <= 0 || config.BloomFilterFalsePositiveRate >= 1 {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: BloomFilterFalsePositiveRate must be between 0 and 1", ErrInvalidConfig)
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
		b.requestPool.Put(req)
		return ctx.Err()
	case <-b.ctx.Done():
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
			b.processIngestRequest(req, partitionBuffers, &doneChans, &bufferedRowCount, &bufferedBytes, &bufferStartTime)
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
}

func (b *BloomSearchEngine) processIngestRequest(req *ingestRequest, partitionBuffers map[string]*partitionBuffer, doneChans *[]chan error, bufferedRowCount *int, bufferedBytes *int, bufferStartTime *time.Time) {
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
				buffer:                make([]byte, 0),
				fieldBloomFilter:      bloom.NewWithEstimates(b.config.BloomFilterExpectedElements, b.config.BloomFilterFalsePositiveRate),
				tokenBloomFilter:      bloom.NewWithEstimates(b.config.BloomFilterExpectedElements, b.config.BloomFilterFalsePositiveRate),
				fieldTokenBloomFilter: bloom.NewWithEstimates(b.config.BloomFilterExpectedElements, b.config.BloomFilterFalsePositiveRate),
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
				for _, value := range field.Values {
					tokens := b.config.Tokenizer(value)
					for _, token := range tokens {
						partitionBuffer.tokenBloomFilter.AddString(token)
						partitionBuffer.fieldTokenBloomFilter.AddString(field.Path + "::" + token)
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
				req.doneChan <- fmt.Errorf("failed to serialize row: %w", err)
				return
			}

			// Check if row is too large for uint32 length prefix
			if len(rowBytes) > 0xFFFFFFFF {
				req.doneChan <- fmt.Errorf("row too large: %d bytes exceeds maximum of %d bytes", len(rowBytes), 0xFFFFFFFF)
				return
			}

			// Write length prefix (uint32) followed by row bytes
			lengthBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(lengthBytes, uint32(len(rowBytes)))
			partitionBuffer.buffer = append(partitionBuffer.buffer, lengthBytes...)
			partitionBuffer.buffer = append(partitionBuffer.buffer, rowBytes...)

			// Increment stats
			*bufferedBytes += len(rowBytes) + 4 // +4 for length prefix
			partitionBuffer.rowCount += 1
			*bufferedRowCount += 1
		}

		// Check partition-level limits
		if !shouldFlush {
			partitionBytes := len(partitionBuffer.buffer)

			if partitionBuffer.rowCount >= b.config.MaxRowGroupRows ||
				partitionBytes >= b.config.MaxRowGroupBytes {
				shouldFlush = true
			}
		}
	}

	// If we haven't decided to flush based on partition limits, check buffer-level limits
	if !shouldFlush {
		if *bufferedRowCount >= b.config.MaxBufferedRows {
			shouldFlush = true
		}

		if !shouldFlush && *bufferedBytes >= b.config.MaxBufferedBytes {
			shouldFlush = true
		}

		if !shouldFlush && time.Since(*bufferStartTime) >= b.config.MaxBufferedTime {
			shouldFlush = true
		}
	}

	// Store the doneChan
	*doneChans = append(*doneChans, req.doneChan)

	// Trigger flush if needed
	if shouldFlush {
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
	// Merge bloom filters for file-level bloom filters
	fileFieldBloomFilter := bloom.NewWithEstimates(b.config.BloomFilterExpectedElements, b.config.BloomFilterFalsePositiveRate)
	fileTokenBloomFilter := bloom.NewWithEstimates(b.config.BloomFilterExpectedElements, b.config.BloomFilterFalsePositiveRate)
	fileFieldTokenBloomFilter := bloom.NewWithEstimates(b.config.BloomFilterExpectedElements, b.config.BloomFilterFalsePositiveRate)
	for _, partitionBuffer := range flushReq.partitionBuffers {
		fileFieldBloomFilter.Merge(partitionBuffer.fieldBloomFilter)
		fileTokenBloomFilter.Merge(partitionBuffer.tokenBloomFilter)
		fileFieldTokenBloomFilter.Merge(partitionBuffer.fieldTokenBloomFilter)
	}

	// Calculate final metadata
	fileMetadata := FileMetadata{
		FieldBloomFilter:      fileFieldBloomFilter,
		TokenBloomFilter:      fileTokenBloomFilter,
		FieldTokenBloomFilter: fileFieldTokenBloomFilter,
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
		partitionHash := xxhash.Sum64(partitionBuffer.buffer)

		// Write the entire buffer + hash
		if _, err := writer.Write(partitionBuffer.buffer); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write data block: %w", err))
			return
		}

		// Write the partition hash as uint64 (8 bytes)
		hashBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(hashBytes, partitionHash)
		if _, err := writer.Write(hashBytes); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write data block hash: %w", err))
			return
		}

		if _, err := fileHash.Write(partitionBuffer.buffer); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}

		// Also include the hash in the file hash
		if _, err := fileHash.Write(hashBytes); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:   partitionBuffer.partitionID,
			BloomFilter:   partitionBuffer.fieldBloomFilter,
			Rows:          partitionBuffer.rowCount,
			Offset:        currentOffset,
			Size:          len(partitionBuffer.buffer) + 8, // +8 for the uint64 hash
			MinMaxIndexes: partitionBuffer.minMaxIndexes,
		})

		// Update offset for next data block (buffer + hash)
		currentOffset += len(partitionBuffer.buffer) + 8
	}

	// Write final metadata to data store and footer
	metadataBytes, metadataHashBytes := fileMetadata.Bytes()

	// Write file metadata
	if _, err := writer.Write(metadataBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata: %w", err))
		return
	}

	// Write file metadata hash (uint64)
	if _, err := writer.Write(metadataHashBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata hash: %w", err))
		return
	}

	// Write file metadata length (uint32)
	metadataLengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(metadataLengthBytes, uint32(len(metadataBytes)))
	if _, err := writer.Write(metadataLengthBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file metadata length: %w", err))
		return
	}

	// Write file version (uint32)
	versionBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(versionBytes, FileVersion)
	if _, err := writer.Write(versionBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write file version: %w", err))
		return
	}

	// Write magic bytes (8 bytes)
	if _, err := writer.Write([]byte(MagicBytes)); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write magic bytes: %w", err))
		return
	}

	// Close the writer to finalize the file
	if err := writer.Close(); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to close file writer: %w", err))
		return
	}

	// Write to meta store
	if err := b.metaStore.WriteFileMetadata(b.ctx, &fileMetadata, filePointerBytes); err != nil {
		TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to store file metadata: %w", err))
		return
	}

	// Signal completion
	TryWriteToChannels(flushReq.doneChans, nil)
}
