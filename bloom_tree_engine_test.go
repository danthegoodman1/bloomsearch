/*
I am manually inspecting these files, they look good
*/

package bloomsearch

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

func TestBloomTreeEngineFlushMaxRows(t *testing.T) {
	// Create test directory for file system data store
	dataStore := NewFileSystemDataStore("./test_data/max_rows")
	metaStore := &NullMetaStore{}

	// Create config with small row limit to trigger flush quickly
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 3                // Flush after 3 rows
	config.MaxBufferedBytes = 1024 * 1024     // Large byte limit (won't trigger)
	config.MaxBufferedTime = 10 * time.Second // Large time limit (won't trigger)
	config.BloomExpectedItems = 100           // Much smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Slightly higher false positive rate

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	}()

	// Create test data - exactly 3 rows to trigger flush
	testRows := []map[string]any{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "age": 25},
		{"id": 3, "name": "Charlie", "age": 35},
	}

	// Channel to wait for flush completion
	doneChan := make(chan error, 1)

	// Ingest rows - this should trigger automatic flush due to MaxBufferedRows=3
	ctx := context.Background()
	err = engine.IngestRows(ctx, testRows, doneChan)
	if err != nil {
		t.Fatalf("Failed to ingest rows: %v", err)
	}

	// Wait for flush to complete
	fmt.Println("Waiting for flush triggered by max rows...")
	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		fmt.Println("Flush completed successfully! (triggered by max rows)")

	case <-time.After(5 * time.Second):
		t.Fatalf("Flush did not complete within timeout")
	}
}

func TestBloomTreeEngineFlushMaxBytes(t *testing.T) {
	// Create test directory for file system data store
	dataStore := NewFileSystemDataStore("./test_data/max_bytes")
	metaStore := &NullMetaStore{}

	// Create config with small byte limit to trigger flush quickly
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 100              // Large row limit (won't trigger)
	config.MaxBufferedBytes = 200             // Small byte limit (will trigger)
	config.MaxBufferedTime = 10 * time.Second // Large time limit (won't trigger)
	config.BloomExpectedItems = 100           // Much smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Slightly higher false positive rate

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	}()

	// Create test data with large values to trigger byte limit
	largeValue := strings.Repeat("X", 50) // 50 character string
	testRows := []map[string]any{
		{"id": 1, "name": "Alice", "data": largeValue},
		{"id": 2, "name": "Bob", "data": largeValue},
		{"id": 3, "name": "Charlie", "data": largeValue},
	}

	// Channel to wait for flush completion
	doneChan := make(chan error, 1)

	// Ingest rows - this should trigger automatic flush due to MaxBufferedBytes=200
	ctx := context.Background()
	err = engine.IngestRows(ctx, testRows, doneChan)
	if err != nil {
		t.Fatalf("Failed to ingest rows: %v", err)
	}

	// Wait for flush to complete
	fmt.Println("Waiting for flush triggered by max bytes...")
	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		fmt.Println("Flush completed successfully! (triggered by max bytes)")

	case <-time.After(5 * time.Second):
		t.Fatalf("Flush did not complete within timeout")
	}
}

func TestBloomTreeEngineFlushMaxTime(t *testing.T) {
	// Create test directory for file system data store
	dataStore := NewFileSystemDataStore("./test_data/max_time")
	metaStore := &NullMetaStore{}

	// Create config with small time limit to trigger flush quickly
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 100             // Large row limit (won't trigger)
	config.MaxBufferedBytes = 1024 * 1024    // Large byte limit (won't trigger)
	config.MaxBufferedTime = 1 * time.Second // Small time limit (will trigger)
	config.BloomExpectedItems = 100          // Much smaller bloom filter
	config.BloomFalsePositiveRate = 0.01     // Slightly higher false positive rate

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	}()

	// Create small test data that won't trigger row/byte limits
	testRows := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}

	// Channel to wait for flush completion
	doneChan := make(chan error, 1)

	// Ingest rows - this should trigger automatic flush due to MaxBufferedTime=1s
	ctx := context.Background()
	err = engine.IngestRows(ctx, testRows, doneChan)
	if err != nil {
		t.Fatalf("Failed to ingest rows: %v", err)
	}

	// Wait for flush to complete (should happen after ~1 second)
	fmt.Println("Waiting for flush triggered by max time...")
	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
		fmt.Println("Flush completed successfully! (triggered by max time)")

	case <-time.After(3 * time.Second):
		t.Fatalf("Flush did not complete within timeout")
	}
}

func TestEvaluateBloomFilters(t *testing.T) {
	// Create a simple engine for testing
	config := DefaultBloomSearchEngineConfig()
	engine, err := NewBloomSearchEngine(config, &NullMetaStore{}, NewFileSystemDataStore("./test_data/bloom_test"))
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Create file metadata with populated bloom filters
	fileMetadata := &FileMetadata{
		FieldBloomFilter:      bloom.NewWithEstimates(100, 0.01),
		TokenBloomFilter:      bloom.NewWithEstimates(100, 0.01),
		FieldTokenBloomFilter: bloom.NewWithEstimates(100, 0.01),
	}

	// Add some test data to the bloom filters
	fileMetadata.FieldBloomFilter.AddString("user.name")
	fileMetadata.FieldBloomFilter.AddString("user.age")
	fileMetadata.TokenBloomFilter.AddString("alice")
	fileMetadata.TokenBloomFilter.AddString("30")
	fileMetadata.FieldTokenBloomFilter.AddString(makeFieldTokenKey("user.name", "alice"))
	fileMetadata.FieldTokenBloomFilter.AddString(makeFieldTokenKey("user.age", "30"))

	tests := []struct {
		name     string
		query    *BloomQuery
		expected bool
	}{
		{
			name:     "nil query should return true",
			query:    nil,
			expected: true,
		},
		{
			name:     "field exists should return true",
			query:    NewQueryWithGroupCombinator(CombinatorAND).Field("user.name").Build().Bloom,
			expected: true,
		},
		{
			name:     "field does not exist should return false",
			query:    NewQueryWithGroupCombinator(CombinatorAND).Field("nonexistent.field").Build().Bloom,
			expected: false,
		},
		{
			name:     "token exists should return true",
			query:    NewQueryWithGroupCombinator(CombinatorAND).Token("alice").Build().Bloom,
			expected: true,
		},
		{
			name:     "field-token exists should return true",
			query:    NewQueryWithGroupCombinator(CombinatorAND).FieldToken("user.name", "alice").Build().Bloom,
			expected: true,
		},
		{
			name:     "OR condition with one match should return true",
			query:    NewQueryWithGroupCombinator(CombinatorAND).Or().Field("nonexistent.field").Field("user.name").Build().Bloom,
			expected: true,
		},
		{
			name:     "AND condition with one mismatch should return false",
			query:    NewQueryWithGroupCombinator(CombinatorAND).And().Field("nonexistent.field").Field("user.name").Build().Bloom,
			expected: false,
		},
		{
			name:     "multiple groups with OR combinator should return true",
			query:    NewQueryWithGroupCombinator(CombinatorOR).And().Field("nonexistent.field").And().FieldToken("user.name", "alice").Build().Bloom,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.evaluateBloomFilters(
				fileMetadata.FieldBloomFilter,
				fileMetadata.TokenBloomFilter,
				fileMetadata.FieldTokenBloomFilter,
				tt.query,
			)
			if result != tt.expected {
				t.Errorf("evaluateBloomFilters() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBloomSearchEngineQueryEndToEnd(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/query_test"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	// Create test directory for file system data store
	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	// Create config
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 2                // Flush after 2 rows
	config.MaxBufferedBytes = 1024 * 1024     // Large byte limit
	config.MaxBufferedTime = 10 * time.Second // Large time limit
	config.BloomExpectedItems = 100           // Smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Higher false positive rate

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	}()

	// Create test data - use float64 for numeric values since JSON unmarshaling converts to float64
	testRows := []map[string]any{
		{"id": 1.0, "name": "Alice", "level": "error", "service": "auth"},
		{"id": 2.0, "name": "Bob", "level": "info", "service": "payment"},
	}

	// Ingest rows and wait for flush
	ctx := context.Background()
	doneChan := make(chan error, 1)
	err = engine.IngestRows(ctx, testRows, doneChan)
	if err != nil {
		t.Fatalf("Failed to ingest rows: %v", err)
	}

	// Wait for flush to complete
	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Flush did not complete within timeout")
	}

	// Test queries that should match
	testCases := []struct {
		name                string
		query               *Query
		expectedResultCount int
		expectedRows        []map[string]any
	}{
		{
			name:                "field search for 'level' should match both rows",
			query:               NewQueryWithGroupCombinator(CombinatorAND).Field("level").Build(),
			expectedResultCount: 2,
			expectedRows:        testRows, // Both rows have 'level' field
		},
		{
			name:                "token search for 'Alice' should match first row",
			query:               NewQueryWithGroupCombinator(CombinatorAND).Token("Alice").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has 'Alice'
		},
		{
			name:                "field-token search for 'level:error' should match first row",
			query:               NewQueryWithGroupCombinator(CombinatorAND).FieldToken("level", "error").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has level=error
		},
		{
			name:                "field search for nonexistent field should not match",
			query:               NewQueryWithGroupCombinator(CombinatorAND).Field("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
		{
			name:                "token search for nonexistent token should not match",
			query:               NewQueryWithGroupCombinator(CombinatorAND).Token("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute query
			resultChan := make(chan map[string]any, 100)
			errorChan := make(chan error, 10)
			err := engine.Query(ctx, tc.query, resultChan, errorChan)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Collect results - rely on channel closing, not timeouts
			var results []map[string]any
			var queryErr error

			// Collect all results until channel closes
			for result := range resultChan {
				results = append(results, result)
			}

			// Check for any errors (non-blocking)
			select {
			case err := <-errorChan:
				if err != nil {
					queryErr = err
				}
			default:
				// No error waiting
			}

			// Verify results
			if queryErr != nil {
				t.Fatalf("Query error: %v", queryErr)
			}

			// Check result count
			if len(results) != tc.expectedResultCount {
				t.Errorf("Expected %d results but got %d", tc.expectedResultCount, len(results))
			}

			// Check that we got the expected rows
			if tc.expectedResultCount > 0 {
				// Convert results to a map for easy lookup
				resultMap := make(map[any]map[string]any)
				for _, result := range results {
					if id, ok := result["id"]; ok {
						resultMap[id] = result
					}
				}

				// Check each expected row
				for _, expectedRow := range tc.expectedRows {
					if id, ok := expectedRow["id"]; ok {
						if actualRow, found := resultMap[id]; found {
							// Check each field in the expected row
							for key, expectedValue := range expectedRow {
								if actualValue, exists := actualRow[key]; !exists || actualValue != expectedValue {
									t.Errorf("Expected row %v to have %s=%v, but got %v", id, key, expectedValue, actualValue)
								}
							}
						} else {
							t.Errorf("Expected row with id=%v not found in results", id)
						}
					}
				}
			}
		})
	}
}
