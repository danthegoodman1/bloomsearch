package bloomsearch

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Simple test to verify flush and read-back functionality
func TestFileSystemStoreFlushAndRead(t *testing.T) {
	// Create test directory for file system data store
	testDir := "./test_data/flush_read_test"
	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore // FileSystemDataStore implements both interfaces

	// Cleanup test directory after test completes (success or failure)
	t.Cleanup(func() {
		os.RemoveAll(testDir)
		fmt.Println("🧹 Cleaned up test directory")
	})

	// Create config with small row limit to trigger flush quickly
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 3                // Flush after 3 rows
	config.MaxBufferedBytes = 1024 * 1024     // Large byte limit (won't trigger)
	config.MaxBufferedTime = 10 * time.Second // Large time limit (won't trigger)
	config.FileBloomExpectedItems = 100       // Much smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Slightly higher false positive rate
	config.RowDataCompression = CompressionNone

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	assert.NoError(t, err)

	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	}()

	nowMilli := float64(time.Now().UnixMilli()) // float because default json unmarshal is float64

	// Create test data - exactly 3 rows to trigger flush
	testRows := []map[string]any{
		{"id": 1, "name": "Alice", "age": 30, "city": "New York", "timestamp": nowMilli},
		{"id": 2, "name": "Bob", "age": 25, "city": "Boston", "timestamp": nowMilli},
		{"id": 3, "name": "Charlie", "age": 35, "city": "Chicago", "timestamp": nowMilli},
	}

	// Channel to wait for flush completion
	doneChan := make(chan error, 1)

	// Ingest rows - this should trigger automatic flush due to MaxBufferedRows=3
	ctx := context.Background()
	err = engine.IngestRows(ctx, testRows, doneChan)
	assert.NoError(t, err)

	// Wait for flush to complete
	fmt.Println("Waiting for flush to complete...")
	select {
	case err := <-doneChan:
		assert.NoError(t, err)
		fmt.Println("Flush completed successfully!")

	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not complete within timeout")
	}

	// Now test reading back the metadata
	fmt.Println("\n--- Testing GetMaybeFilesForQuery ---")

	// Read back all files (no query conditions)
	maybeFiles, err := dataStore.GetMaybeFilesForQuery(ctx, nil)
	assert.NoError(t, err)
	assert.Len(t, maybeFiles, 1, "Expected exactly 1 file")

	fmt.Printf("Found %d files:\n", len(maybeFiles))

	maybeFile := maybeFiles[0]
	fmt.Printf("\n=== File ===\n")
	fmt.Printf("File Path: %s\n", string(maybeFile.PointerBytes))
	fmt.Printf("Data Blocks: %d\n", len(maybeFile.Metadata.DataBlocks))

	// Verify metadata structure
	assert.Len(t, maybeFile.Metadata.DataBlocks, 1, "Expected exactly 1 data block")
	assert.NotNil(t, maybeFile.Metadata.BloomFilters.FieldBloomFilter, "Field bloom filter should exist")
	assert.NotNil(t, maybeFile.Metadata.BloomFilters.TokenBloomFilter, "Token bloom filter should exist")
	assert.NotNil(t, maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter, "Field+Token bloom filter should exist")

	block := maybeFile.Metadata.DataBlocks[0]
	assert.Equal(t, 3, block.Rows, "Expected 3 rows in the block")

	// Print data block information
	fmt.Printf("  Block:\n")
	fmt.Printf("    Partition ID: %s\n", block.PartitionID)
	fmt.Printf("    Rows: %d\n", block.Rows)
	fmt.Printf("    Offset: %d\n", block.Offset)
	fmt.Printf("    Size: %d\n", block.Size)
	fmt.Printf("    MinMax Indexes: %v\n", block.MinMaxIndexes)

	// Print bloom filter info
	fmt.Printf("  Bloom Filters:\n")
	fmt.Printf("    Field Filter: %v\n", maybeFile.Metadata.BloomFilters.FieldBloomFilter != nil)
	fmt.Printf("    Token Filter: %v\n", maybeFile.Metadata.BloomFilters.TokenBloomFilter != nil)
	fmt.Printf("    Field+Token Filter: %v\n", maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter != nil)

	fmt.Printf("  Matching Data Blocks: %d\n", len(maybeFile.Metadata.DataBlocks))

	// === Read back the actual row data ===
	fmt.Println("\n--- Reading back row data ---")
	file, err := dataStore.OpenFile(ctx, maybeFile.PointerBytes)
	assert.NoError(t, err)
	defer file.Close()

	fmt.Printf("\n  Reading rows from Block:\n")

	// Seek to the block offset
	_, err = file.Seek(int64(block.Offset), 0)
	assert.NoError(t, err)

	// First, read the bloom filters from the beginning of the data block
	bloomFiltersSize := block.BloomFiltersSize
	bloomFiltersBytes := make([]byte, bloomFiltersSize-HashSize) // exclude hash
	_, err = file.Read(bloomFiltersBytes)
	assert.NoError(t, err)

	// Read the bloom filters hash
	bloomFiltersHashBytes := make([]byte, HashSize)
	_, err = file.Read(bloomFiltersHashBytes)
	assert.NoError(t, err)

	// Verify and parse bloom filters
	bloomFilters, err := DataBlockBloomFiltersFromBytesWithHash(bloomFiltersBytes, bloomFiltersHashBytes)
	assert.NoError(t, err)
	assert.NotNil(t, bloomFilters.FieldBloomFilter, "Field bloom filter should exist")
	assert.NotNil(t, bloomFilters.TokenBloomFilter, "Token bloom filter should exist")
	assert.NotNil(t, bloomFilters.FieldTokenBloomFilter, "Field+Token bloom filter should exist")

	fmt.Printf("  Bloom filters loaded successfully from data block\n")

	// Now read the row data
	// (block.Size - BloomFiltersSize) gives us the row data size
	rowDataSize := block.Size - block.BloomFiltersSize
	bytesRead := 0
	rowCount := 0
	readRows := make([]map[string]any, 0, block.Rows)

	for bytesRead < rowDataSize && rowCount < block.Rows {
		// Read row length (uint32)
		lengthBytes := make([]byte, 4)
		_, err := file.Read(lengthBytes)
		assert.NoError(t, err)
		rowLength := binary.LittleEndian.Uint32(lengthBytes)
		bytesRead += 4

		// Read row data
		rowBytes := make([]byte, rowLength)
		_, err = file.Read(rowBytes)
		assert.NoError(t, err)
		bytesRead += int(rowLength)

		// Parse JSON row
		var row map[string]any
		err = json.Unmarshal(rowBytes, &row)
		assert.NoError(t, err)

		fmt.Printf("    Row %d: %v\n", rowCount+1, row)
		readRows = append(readRows, row)
		rowCount++
	}

	// Verify we read the expected number of rows
	assert.Len(t, readRows, 3, "Should have read exactly 3 rows")

	// Verify some of the data (names should match what we wrote)
	names := make([]string, len(readRows))
	for i, row := range readRows {
		name, ok := row["name"].(string)
		assert.True(t, ok, "Name should be a string")
		names[i] = name
	}

	assert.Contains(t, names, "Alice")
	assert.Contains(t, names, "Bob")
	assert.Contains(t, names, "Charlie")
	assert.Equal(t, nowMilli, readRows[0]["timestamp"])
	assert.Equal(t, nowMilli, readRows[1]["timestamp"])
	assert.Equal(t, nowMilli, readRows[2]["timestamp"])

	fmt.Println("\n✅ Test completed successfully!")
}
