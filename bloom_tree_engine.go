package bloomsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

type PartitionFunc func(row map[string]any) string

type BloomSearchEngine struct {
	config    BloomSearchEngineConfig
	metaStore MetaStore
	dataStore DataStore

	bufferedRowCount *atomic.Int64
	bufferedBytes    *atomic.Int64

	bufferStartTimeMilli *atomic.Int64 // the time since we started buffering rows

	partitionBuffersMu sync.Mutex
	// BEGIN PROTECTED SECTION
	partitionBuffers map[string]*partitionBuffer
	doneChans        []chan error
	// END PROTECTED SECTION
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
	}
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) *BloomSearchEngine {
	return &BloomSearchEngine{
		config:               config,
		metaStore:            metaStore,
		dataStore:            dataStore,
		bufferedRowCount:     &atomic.Int64{},
		bufferedBytes:        &atomic.Int64{},
		bufferStartTimeMilli: &atomic.Int64{},
		partitionBuffers:     make(map[string]*partitionBuffer),
		doneChans:            make([]chan error, 0),
	}
}

// IngestRow ingests a row into the engine, buffering it until flush.
//
// The caller may provide a doneChan to signal that the row has been durably stored (or errored). Otherwise it may provide nil and
// not wait for durability.
func (b *BloomSearchEngine) IngestRows(ctx context.Context, rows []map[string]any, doneChan chan error) error {
	// Group rows by partition ID
	partitionedRows := make(map[string][]map[string]any)
	if b.config.PartitionFunc != nil {
		for _, row := range rows {
			partitionID := b.config.PartitionFunc(row)
			partitionedRows[partitionID] = append(partitionedRows[partitionID], row)
		}
	} else {
		partitionedRows[""] = rows
	}

	// Hold the global lock for the entire operation to avoid deadlocks with flush
	b.partitionBuffersMu.Lock()
	defer b.partitionBuffersMu.Unlock()

	if b.partitionBuffers == nil {
		b.partitionBuffers = make(map[string]*partitionBuffer)
	}

	// Create partition buffers if they don't exist
	for partitionID := range partitionedRows {
		if b.partitionBuffers[partitionID] == nil {
			b.partitionBuffers[partitionID] = &partitionBuffer{
				minMaxIndexes: make(map[string]MinMaxIndex),
				buffer:        make([][]byte, 0),
				// TODO: make configurable
				fieldBloomFilter:      bloom.NewWithEstimates(1000000, 0.01),
				tokenBloomFilter:      bloom.NewWithEstimates(1000000, 0.01),
				fieldTokenBloomFilter: bloom.NewWithEstimates(1000000, 0.01),
			}
		}
	}

	// If we haven't saved the buffer start time yet, do it
	if b.bufferStartTimeMilli.Load() == 0 {
		b.bufferStartTimeMilli.CompareAndSwap(0, time.Now().UnixMilli())
	}

	// Track if we should flush
	shouldFlush := &atomic.Bool{}

	// Now for each partition we can concurrently serialize rows and store in the buffers
	wg := sync.WaitGroup{}

	for partitionID, rows := range partitionedRows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			partitionBuffer := b.partitionBuffers[partitionID]

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
							// Skip values that aren't numeric
							continue
						}

						if existingIndex, exists := partitionBuffer.minMaxIndexes[index]; exists {
							// Update existing min/max with the new min/max values
							partitionBuffer.minMaxIndexes[index] = UpdateMinMaxIndex(existingIndex, minVal, maxVal)
						} else {
							// Create new min/max index with the converted min/max values
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
					doneChan <- fmt.Errorf("failed to serialize row: %w", err)
					return
				}

				// Increment stats
				b.bufferedBytes.Add(int64(len(rowBytes)))
				partitionBuffer.buffer = append(partitionBuffer.buffer, rowBytes)
				partitionBuffer.rowCount += 1
				b.bufferedRowCount.Add(1)
			}

			// Check partition-level limits after processing all rows for this partition
			if !shouldFlush.Load() {
				partitionBytes := 0
				for _, rowBytes := range partitionBuffer.buffer {
					partitionBytes += len(rowBytes)
				}

				if partitionBuffer.rowCount >= b.config.MaxRowGroupRows ||
					partitionBytes >= b.config.MaxRowGroupBytes {
					shouldFlush.Store(true)
				}
			}
		}()
	}
	wg.Wait()

	// If we haven't decided to flush based on partition limits, check buffer-level limits
	if !shouldFlush.Load() {
		// Check buffered row count
		if b.bufferedRowCount.Load() >= int64(b.config.MaxBufferedRows) {
			shouldFlush.Store(true)
		}

		// Check buffered bytes
		if !shouldFlush.Load() && b.bufferedBytes.Load() >= int64(b.config.MaxBufferedBytes) {
			shouldFlush.Store(true)
		}

		// Check buffered time
		if !shouldFlush.Load() {
			startTime := b.bufferStartTimeMilli.Load()
			if startTime > 0 {
				elapsed := time.Duration(time.Now().UnixMilli()-startTime) * time.Millisecond
				if elapsed >= b.config.MaxBufferedTime {
					shouldFlush.Store(true)
				}
			}
		}
	}

	// Store the doneChan
	b.doneChans = append(b.doneChans, doneChan)

	// Trigger flush if needed
	if shouldFlush.Load() {
		go b.flush()
	}

	return nil
}

func (b *BloomSearchEngine) flush() {
	// TODO: atomically swap active buffers and store reference here
	// TODO: merge bloom filters
	// TODO: calculate final metadata
	// TODO: write to data store
	// TODO: write to meta store

	// Reset counters after flush
	b.bufferedRowCount.Store(0)
	b.bufferedBytes.Store(0)
	b.bufferStartTimeMilli.Store(0)
}
