package bloomsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

type PartitionFunc func(row map[string]any) string

type ingestRequest struct {
	rows     []map[string]any
	doneChan chan error
}

type flushRequest struct {
	partitionBuffers map[string]*partitionBuffer
	doneChans        []chan error
}

type BloomSearchEngine struct {
	config    BloomSearchEngineConfig
	metaStore MetaStore
	dataStore DataStore

	ingestChan chan ingestRequest
	flushChan  chan flushRequest
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
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
}

type partitionBuffer struct {
	rowCount              int
	minMaxIndexes         map[string]MinMaxIndex
	buffer                [][]byte // serialized rows
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

		IngestBufferSize: 1000, // buffered channel size for ingestion requests
	}
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) *BloomSearchEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &BloomSearchEngine{
		config:    config,
		metaStore: metaStore,
		dataStore: dataStore,

		ingestChan: make(chan ingestRequest, config.IngestBufferSize),
		flushChan:  make(chan flushRequest, 1), // Buffered flush channel
		ctx:        ctx,
		cancel:     cancel,
	}
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
	req := ingestRequest{
		rows:     rows,
		doneChan: doneChan,
	}

	select {
	case b.ingestChan <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
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
			return
		case req := <-b.ingestChan:
			// Process the batch of rows
			b.processIngestRequest(req, partitionBuffers, &doneChans, &bufferedRowCount, &bufferedBytes, &bufferStartTime)
		}
	}
}

func (b *BloomSearchEngine) processIngestRequest(req ingestRequest, partitionBuffers map[string]*partitionBuffer, doneChans *[]chan error, bufferedRowCount *int, bufferedBytes *int, bufferStartTime *time.Time) {
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
				minMaxIndexes:         make(map[string]MinMaxIndex),
				buffer:                make([][]byte, 0),
				fieldBloomFilter:      bloom.NewWithEstimates(1000000, 0.01),
				tokenBloomFilter:      bloom.NewWithEstimates(1000000, 0.01),
				fieldTokenBloomFilter: bloom.NewWithEstimates(1000000, 0.01),
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
					partitionBuffer.tokenBloomFilter.AddString(value)
					partitionBuffer.fieldTokenBloomFilter.AddString(field.Path + "::" + value)
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

			// Increment stats
			*bufferedBytes += len(rowBytes)
			partitionBuffer.buffer = append(partitionBuffer.buffer, rowBytes)
			partitionBuffer.rowCount += 1
			*bufferedRowCount += 1
		}

		// Check partition-level limits
		if !shouldFlush {
			partitionBytes := 0
			for _, rowBytes := range partitionBuffer.buffer {
				partitionBytes += len(rowBytes)
			}

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
		*doneChans = (*doneChans)[:0] // Clear slice but keep capacity
		*bufferedRowCount = 0
		*bufferedBytes = 0
		*bufferStartTime = time.Time{}
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
	// TODO: merge bloom filters
	// TODO: calculate final metadata
	// TODO: write to data store
	// TODO: write to meta store

	// For now, just signal completion
	for _, doneChan := range flushReq.doneChans {
		if doneChan != nil {
			doneChan <- nil
		}
	}
}
