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

	querySemaphore chan struct{} // Global semaphore for query concurrency
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

	MaxBufferedRows  int
	MaxBufferedBytes int
	MaxBufferedTime  time.Duration

	IngestBufferSize int

	// The maximum number of total data blocks that can be processed concurrently across all queries
	MaxQueryConcurrency int

	// Bloom filter parameters
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64
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

		MaxQueryConcurrency: 1_000,

		BloomExpectedItems:     100_000,
		BloomFalsePositiveRate: 0.001,
	}
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) (*BloomSearchEngine, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if config.Tokenizer == nil {
		cancel() // make the linter happy
		return nil, fmt.Errorf("%w: tokenizer is required", ErrInvalidConfig)
	}

	if config.BloomExpectedItems == 0 {
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
				buffer:                make([]byte, 0),
				fieldBloomFilter:      bloom.NewWithEstimates(b.config.BloomExpectedItems, b.config.BloomFalsePositiveRate),
				tokenBloomFilter:      bloom.NewWithEstimates(b.config.BloomExpectedItems, b.config.BloomFalsePositiveRate),
				fieldTokenBloomFilter: bloom.NewWithEstimates(b.config.BloomExpectedItems, b.config.BloomFalsePositiveRate),
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
						partitionBuffer.fieldTokenBloomFilter.AddString(makeFieldTokenKey(field.Path, token))
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
	fileFieldBloomFilter := bloom.NewWithEstimates(b.config.BloomExpectedItems, b.config.BloomFalsePositiveRate)
	fileTokenBloomFilter := bloom.NewWithEstimates(b.config.BloomExpectedItems, b.config.BloomFalsePositiveRate)
	fileFieldTokenBloomFilter := bloom.NewWithEstimates(b.config.BloomExpectedItems, b.config.BloomFalsePositiveRate)
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

		// Write the row data buffer
		if _, err := writer.Write(partitionBuffer.buffer); err != nil {
			// Write error to all done channels
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write data block: %w", err))
			return
		}

		// Calculate hash of the entire data block (bloom filters + hash + row data)
		dataBlockBytes := make([]byte, 0, len(bloomFiltersBytes)+8+len(partitionBuffer.buffer))
		dataBlockBytes = append(dataBlockBytes, bloomFiltersBytes...)
		dataBlockBytes = append(dataBlockBytes, bloomFiltersHashBytes...)
		dataBlockBytes = append(dataBlockBytes, partitionBuffer.buffer...)
		dataBlockHash := xxhash.Sum64(dataBlockBytes)

		dataBlockHashBytes := make([]byte, HashSize)
		binary.LittleEndian.PutUint64(dataBlockHashBytes, dataBlockHash)
		if _, err := writer.Write(dataBlockHashBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to write data block hash: %w", err))
			return
		}

		if _, err := fileHash.Write(dataBlockBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}

		if _, err := fileHash.Write(dataBlockHashBytes); err != nil {
			TryWriteToChannels(flushReq.doneChans, fmt.Errorf("failed to hash file data: %w", err))
			return
		}

		bloomFiltersSize := len(bloomFiltersBytes) + HashSize
		dataBlockSize := bloomFiltersSize + len(partitionBuffer.buffer) + HashSize

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:      partitionBuffer.partitionID,
			Rows:             partitionBuffer.rowCount,
			Offset:           currentOffset,
			Size:             dataBlockSize,
			BloomFiltersSize: bloomFiltersSize,
			MinMaxIndexes:    partitionBuffer.minMaxIndexes,
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

	if err := b.metaStore.WriteFileMetadata(b.ctx, &fileMetadata, filePointerBytes); err != nil {
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
) bool {
	switch condition.Type {
	case BloomField:
		return fieldFilter.TestString(condition.Field)
	case BloomToken:
		return tokenFilter.TestString(condition.Token)
	case BloomFieldToken:
		return fieldTokenFilter.TestString(makeFieldTokenKey(condition.Field, condition.Token))
	default:
		return false // We don't know what this is, so it's invalid
	}
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
	// Get maybe files from meta store
	maybeFiles, err := b.metaStore.GetMaybeFilesForQuery(ctx, query.Prefilter)
	if err != nil {
		return err
	}

	// Test file-level bloom filters - use concurrency only when beneficial
	const concurrencyThreshold = 20 // Minimum files to justify concurrency overhead

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

				// Acquire global semaphore
				if err := SendWithContext(ctx, b.querySemaphore, struct{}{}); err != nil {
					return // Context cancelled while waiting for semaphore
				}
				defer func() { <-b.querySemaphore }()

				// Test the file-level bloom filters
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

		// Collect matching files
		for matchingFile := range matchingFilesChan {
			matchingFiles = append(matchingFiles, matchingFile)
		}
	}

	// Collect all data blocks from all files to process concurrently
	var allJobs []dataBlockJob
	for _, matchingFile := range matchingFiles {
		for _, blockMetadata := range matchingFile.Metadata.DataBlocks {
			allJobs = append(allJobs, dataBlockJob{
				filePointer:   matchingFile.PointerBytes,
				blockMetadata: blockMetadata,
			})
		}
	}

	// Start worker goroutines - one for each data block
	var wg sync.WaitGroup
	workerCtx, workerCancel := context.WithCancel(ctx)
	for _, job := range allJobs {
		wg.Add(1)
		go func(job dataBlockJob) {
			defer wg.Done()

			// Acquire global semaphore
			if err := SendWithContext(workerCtx, b.querySemaphore, struct{}{}); err != nil {
				return // Context cancelled while waiting for semaphore
			}
			defer func() { <-b.querySemaphore }()

			b.processDataBlock(workerCtx, job, resultChan, errorChan, query.Bloom, statsChan)
		}(job)
	}

	// Close result channel when all workers are done
	go func() {
		defer workerCancel() // Cancel worker context when done
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
	var rowsProcessed, bytesProcessed int64
	var bloomFilterSkipped bool

	// Always send stats when we exit, regardless of success/failure
	defer func() {
		duration := time.Since(blockStartTime)

		blockStats := BlockStats{
			FilePointer:        job.filePointer,
			BlockOffset:        job.blockMetadata.Offset,
			RowsProcessed:      rowsProcessed,
			BytesProcessed:     bytesProcessed,
			TotalRows:          int64(job.blockMetadata.Rows),
			TotalBytes:         int64(job.blockMetadata.Size),
			Duration:           duration,
			BloomFilterSkipped: bloomFilterSkipped,
		}

		TryWriteChannel(statsChan, blockStats)
	}()
	// Open the file for reading
	file, err := b.dataStore.OpenFile(ctx, job.filePointer)
	if err != nil {
		SendWithContext(ctx, errorChan, fmt.Errorf("failed to open file: %w", err))
		return
	}
	defer file.Close()

	// Read the data block bloom filters
	blockBloomFilters, err := ReadDataBlockBloomFilters(file, job.blockMetadata)
	if err != nil {
		SendWithContext(ctx, errorChan, fmt.Errorf("failed to read data block bloom filters: %w", err))
		return
	}

	// Test the data block bloom filters against the query
	if !b.evaluateBloomFilters(
		blockBloomFilters.FieldBloomFilter,
		blockBloomFilters.TokenBloomFilter,
		blockBloomFilters.FieldTokenBloomFilter,
		bloomQuery,
	) {
		bloomFilterSkipped = true
		return
	}

	// Read rows from this data block as raw bytes
	// Seek to the start of row data (after bloom filters)
	rowDataOffset := int64(job.blockMetadata.Offset + job.blockMetadata.BloomFiltersSize)
	if _, err := file.Seek(rowDataOffset, 0); err != nil {
		SendWithContext(ctx, errorChan, fmt.Errorf("failed to seek to row data: %w", err))
		return
	}

	// Calculate the size of row data (excluding the final data block hash)
	rowGroupSize := job.blockMetadata.Size - job.blockMetadata.BloomFiltersSize - HashSize

	for bytesRead := 0; bytesRead < rowGroupSize; {
		// Read the row length prefix (uint32)
		lengthBytes := make([]byte, LengthPrefixSize)
		if _, err := file.Read(lengthBytes); err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read row length: %w", err))
			return
		}
		bytesRead += LengthPrefixSize

		// Extract the row length
		rowLength := binary.LittleEndian.Uint32(lengthBytes)

		// Read the row data as raw bytes
		rowData := make([]byte, rowLength)
		if _, err := file.Read(rowData); err != nil {
			SendWithContext(ctx, errorChan, fmt.Errorf("failed to read row data: %w", err))
			return
		}
		bytesRead += int(rowLength)

		// Track bytes processed for all rows, whether they match the query or not
		bytesProcessed += int64(LengthPrefixSize + rowLength)
		rowsProcessed++

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
