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
	partitionMu           sync.Mutex
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
		config:    config,
		metaStore: metaStore,
		dataStore: dataStore,
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

	// Get partition buffer references in a local map copy so we don't need to hold the lock
	b.partitionBuffersMu.Lock()
	// Manually unlocking below to avoid an extra function call with `go func()`` for defer use.
	// Don't lose track of this lock!

	b.doneChans = append(b.doneChans, doneChan)

	if b.partitionBuffers == nil {
		b.partitionBuffers = make(map[string]*partitionBuffer)
	}

	partitionReferences := map[string]*partitionBuffer{}
	for partitionID := range partitionedRows {
		if b.partitionBuffers[partitionID] == nil {
			// Doesn't exist, we need to create it
			b.partitionBuffers[partitionID] = &partitionBuffer{
				minMaxIndexes:         make(map[string]MinMaxIndex),
				buffer:                make([][]byte, 0),
				fieldBloomFilter:      bloom.NewWithEstimates(1000000, 0.01), // TODO: make configurable
				tokenBloomFilter:      bloom.NewWithEstimates(1000000, 0.01), // TODO: make configurable
				fieldTokenBloomFilter: bloom.NewWithEstimates(1000000, 0.01), // TODO: make configurable
			}
		}
		partitionReferences[partitionID] = b.partitionBuffers[partitionID]
	}

	b.partitionBuffersMu.Unlock()

	// If we haven't saved the buffer start time yet, do it
	if b.bufferStartTimeMilli.Load() == 0 {
		b.bufferStartTimeMilli.CompareAndSwap(0, time.Now().UnixMilli())
	}

	// Now for each partitions we can concurrently serialize rows and store in the buffers
	wg := sync.WaitGroup{}
	for partitionID, rows := range partitionedRows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			partitionReferences[partitionID].partitionMu.Lock()
			defer partitionReferences[partitionID].partitionMu.Unlock()

			// Process each row
			for _, row := range rows {
				// Add info to bloom filters
				uniqueFields := UniqueFields(row, ".")
				for _, field := range uniqueFields {
					partitionReferences[partitionID].fieldBloomFilter.AddString(field.Path)
					for _, value := range field.Values {
						partitionReferences[partitionID].tokenBloomFilter.AddString(value)
						partitionReferences[partitionID].fieldTokenBloomFilter.AddString(field.Path + "::" + value)
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

						if existingIndex, exists := partitionReferences[partitionID].minMaxIndexes[index]; exists {
							// Update existing min/max with the new min/max values
							partitionReferences[partitionID].minMaxIndexes[index] = UpdateMinMaxIndex(existingIndex, minVal, maxVal)
						} else {
							// Create new min/max index with the converted min/max values
							partitionReferences[partitionID].minMaxIndexes[index] = MinMaxIndex{
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
				partitionReferences[partitionID].buffer = append(partitionReferences[partitionID].buffer, rowBytes)
				partitionReferences[partitionID].rowCount += 1
				b.bufferedRowCount.Add(1)
			}
		}()
	}
	wg.Wait()

	return nil
}

func (b *BloomSearchEngine) flush() {
	// TODO: atomically swap active buffers and store reference here
	// TODO: merge bloom filters
	// TODO: calculate final metadata
	// TODO: write to data store
	// TODO: write to meta store
}
