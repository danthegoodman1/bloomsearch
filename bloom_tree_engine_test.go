/*
I am manually inspecting these files, they look good
*/

package bloomsearch

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// Test helper functions

// flushAndWait ingests a batch of rows and waits for flush completion
func flushAndWait(t *testing.T, engine *BloomSearchEngine, ctx context.Context, batch []map[string]any, batchName string) {
	doneChan := make(chan error, 1)
	err := engine.IngestRows(ctx, batch, doneChan)
	if err != nil {
		t.Fatalf("Failed to ingest %s: %v", batchName, err)
	}

	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Flush failed for %s: %v", batchName, err)
		}
		t.Logf("Successfully flushed %s", batchName)
	case <-time.After(5 * time.Second):
		t.Fatalf("Flush did not complete within timeout for %s", batchName)
	}
}

// runQueryTest executes a query and verifies the results
func runQueryTest(t *testing.T, engine *BloomSearchEngine, ctx context.Context, testName string, queryBuilder *QueryBuilder, expectedResultCount int, expectedRows []map[string]any) {
	t.Run(testName, func(t *testing.T) {
		query := queryBuilder.Build()

		// Execute query
		resultChan := make(chan map[string]any, 100)
		errorChan := make(chan error, 10)
		statsChan := make(chan BlockStats, 10)
		err := engine.Query(ctx, query, resultChan, errorChan, statsChan)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		// Print stats as they come in
		go func() {
			for stat := range statsChan {
				t.Logf("Block %s[%d]: %s rows/s, %s",
					string(stat.FilePointer), stat.BlockOffset,
					FormatRate(stat.RowsProcessed, stat.Duration),
					FormatBytesPerSecond(stat.BytesProcessed, stat.Duration))
			}
		}()

		var results []map[string]any
		var queryErr error

		for result := range resultChan {
			results = append(results, result)
		}

		select {
		case err := <-errorChan:
			if err != nil {
				queryErr = err
			}
		default:
		}

		// Verify results
		if queryErr != nil {
			t.Fatalf("Query error: %v", queryErr)
		}

		// Check result count
		if expectedResultCount == -1 {
			// Skip count validation when expectedResultCount is -1
			// This is used when merging can expand results due to merged MinMax indexes
			t.Logf("Query '%s' returned %d results (count validation skipped due to merge behavior)", testName, len(results))
		} else if len(results) != expectedResultCount {
			t.Errorf("Expected %d results but got %d", expectedResultCount, len(results))
		}

		// Check that we got the expected rows
		if expectedResultCount > 0 && expectedRows != nil {
			// Convert results to a map for easy lookup
			resultMap := make(map[any]map[string]any)
			for _, result := range results {
				if id, ok := result["id"]; ok {
					resultMap[id] = result
				}
			}

			// Check each expected row
			for _, expectedRow := range expectedRows {
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

func TestBloomTreeEngineFlushMaxRows(t *testing.T) {
	// Create test directory for file system data store
	dataStore := NewFileSystemDataStore("./test_data/max_rows")
	metaStore := &NullMetaStore{}

	// Create config with small row limit to trigger flush quickly
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 3                // Flush after 3 rows
	config.MaxBufferedBytes = 1024 * 1024     // Large byte limit (won't trigger)
	config.MaxBufferedTime = 10 * time.Second // Large time limit (won't trigger)
	config.FileBloomExpectedItems = 100       // Much smaller bloom filter
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
	config.FileBloomExpectedItems = 100       // Much smaller bloom filter
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
	config.FileBloomExpectedItems = 100      // Much smaller bloom filter
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
		BloomFilters: BloomFilters{
			FieldBloomFilter:      bloom.NewWithEstimates(100, 0.01),
			TokenBloomFilter:      bloom.NewWithEstimates(100, 0.01),
			FieldTokenBloomFilter: bloom.NewWithEstimates(100, 0.01),
		},
	}

	// Add some test data to the bloom filters
	fileMetadata.BloomFilters.FieldBloomFilter.AddString("user.name")
	fileMetadata.BloomFilters.FieldBloomFilter.AddString("user.age")
	fileMetadata.BloomFilters.TokenBloomFilter.AddString("alice")
	fileMetadata.BloomFilters.TokenBloomFilter.AddString("30")
	fileMetadata.BloomFilters.FieldTokenBloomFilter.AddString(makeFieldTokenKey("user.name", "alice"))
	fileMetadata.BloomFilters.FieldTokenBloomFilter.AddString(makeFieldTokenKey("user.age", "30"))

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
			query:    NewQuery().Field("user.name").Build().Bloom,
			expected: true,
		},
		{
			name:     "field does not exist should return false",
			query:    NewQuery().Field("nonexistent.field").Build().Bloom,
			expected: false,
		},
		{
			name:     "token exists should return true",
			query:    NewQuery().Token("alice").Build().Bloom,
			expected: true,
		},
		{
			name:     "field-token exists should return true",
			query:    NewQuery().FieldToken("user.name", "alice").Build().Bloom,
			expected: true,
		},
		{
			name:     "OR condition with one match should return true",
			query:    NewQuery().Match(Or(Field("nonexistent.field"), Field("user.name"))).Build().Bloom,
			expected: true,
		},
		{
			name:     "AND condition with one mismatch should return false",
			query:    NewQuery().Match(And(Field("nonexistent.field"), Field("user.name"))).Build().Bloom,
			expected: false,
		},
		{
			name:     "multiple groups with OR combinator should return true",
			query:    NewQuery().Match(Or(Field("nonexistent.field"), FieldToken("user.name", "alice"))).Build().Bloom,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.evaluateBloomFilters(
				fileMetadata.BloomFilters.FieldBloomFilter,
				fileMetadata.BloomFilters.TokenBloomFilter,
				fileMetadata.BloomFilters.FieldTokenBloomFilter,
				tt.query,
			)
			if result != tt.expected {
				t.Errorf("evaluateBloomFilters() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBloomSearchEngineQueryEndToEndUncompressed(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/query_test_uncompressed"
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
	config.FileBloomExpectedItems = 100       // Smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Higher false positive rate
	config.RowDataCompression = CompressionNone

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
			query:               NewQuery().Field("level").Build(),
			expectedResultCount: 2,
			expectedRows:        testRows, // Both rows have 'level' field
		},
		{
			name:                "token search for 'alice' should match first row",
			query:               NewQuery().Token("alice").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has 'Alice' (tokenized as 'alice')
		},
		{
			name:                "field-token search for 'level:error' should match first row",
			query:               NewQuery().FieldToken("level", "error").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has level=error
		},
		{
			name:                "field search for nonexistent field should not match",
			query:               NewQuery().Field("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
		{
			name:                "token search for nonexistent token should not match",
			query:               NewQuery().Token("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute query
			resultChan := make(chan map[string]any, 100)
			errorChan := make(chan error, 10)
			statsChan := make(chan BlockStats, 10)
			err := engine.Query(ctx, tc.query, resultChan, errorChan, statsChan)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Print stats as they come in
			go func() {
				for stat := range statsChan {
					t.Logf("Block %s[%d]: %s rows/s, %s",
						string(stat.FilePointer), stat.BlockOffset,
						FormatRate(stat.RowsProcessed, stat.Duration),
						FormatBytesPerSecond(stat.BytesProcessed, stat.Duration))
				}
			}()

			var results []map[string]any
			var queryErr error

			for result := range resultChan {
				results = append(results, result)
			}

			select {
			case err := <-errorChan:
				if err != nil {
					queryErr = err
				}
			default:
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

func TestBloomSearchEngineQueryEndToEndZstd(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/query_test_zstd"
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
	config.FileBloomExpectedItems = 100       // Smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Higher false positive rate
	config.RowDataCompression = CompressionZstd

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
			query:               NewQuery().Field("level").Build(),
			expectedResultCount: 2,
			expectedRows:        testRows, // Both rows have 'level' field
		},
		{
			name:                "token search for 'alice' should match first row",
			query:               NewQuery().Token("alice").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has 'Alice' (tokenized as 'alice')
		},
		{
			name:                "field-token search for 'level:error' should match first row",
			query:               NewQuery().FieldToken("level", "error").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has level=error
		},
		{
			name:                "field search for nonexistent field should not match",
			query:               NewQuery().Field("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
		{
			name:                "token search for nonexistent token should not match",
			query:               NewQuery().Token("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute query
			resultChan := make(chan map[string]any, 100)
			errorChan := make(chan error, 10)
			statsChan := make(chan BlockStats, 10)
			err := engine.Query(ctx, tc.query, resultChan, errorChan, statsChan)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Print stats as they come in
			go func() {
				for stat := range statsChan {
					t.Logf("Block %s[%d]: %s rows/s, %s",
						string(stat.FilePointer), stat.BlockOffset,
						FormatRate(stat.RowsProcessed, stat.Duration),
						FormatBytesPerSecond(stat.BytesProcessed, stat.Duration))
				}
			}()

			var results []map[string]any
			var queryErr error

			for result := range resultChan {
				results = append(results, result)
			}

			select {
			case err := <-errorChan:
				if err != nil {
					queryErr = err
				}
			default:
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
func TestBloomSearchEngineQueryEndToEndSnappy(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/query_test_snappy"
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
	config.FileBloomExpectedItems = 100       // Smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Higher false positive rate
	config.RowDataCompression = CompressionSnappy

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
			query:               NewQuery().Field("level").Build(),
			expectedResultCount: 2,
			expectedRows:        testRows, // Both rows have 'level' field
		},
		{
			name:                "token search for 'alice' should match first row",
			query:               NewQuery().Token("alice").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has 'Alice' (tokenized as 'alice')
		},
		{
			name:                "field-token search for 'level:error' should match first row",
			query:               NewQuery().FieldToken("level", "error").Build(),
			expectedResultCount: 1,
			expectedRows:        []map[string]any{testRows[0]}, // Only first row has level=error
		},
		{
			name:                "field search for nonexistent field should not match",
			query:               NewQuery().Field("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
		{
			name:                "token search for nonexistent token should not match",
			query:               NewQuery().Token("nonexistent").Build(),
			expectedResultCount: 0,
			expectedRows:        []map[string]any{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Execute query
			resultChan := make(chan map[string]any, 100)
			errorChan := make(chan error, 10)
			statsChan := make(chan BlockStats, 10)
			err := engine.Query(ctx, tc.query, resultChan, errorChan, statsChan)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Print stats as they come in
			go func() {
				for stat := range statsChan {
					t.Logf("Block %s[%d]: %s rows/s, %s",
						string(stat.FilePointer), stat.BlockOffset,
						FormatRate(stat.RowsProcessed, stat.Duration),
						FormatBytesPerSecond(stat.BytesProcessed, stat.Duration))
				}
			}()

			var results []map[string]any
			var queryErr error

			for result := range resultChan {
				results = append(results, result)
			}

			select {
			case err := <-errorChan:
				if err != nil {
					queryErr = err
				}
			default:
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

func TestBloomSearchEngineMergeEndToEndUncompressed(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/merge_test_uncompressed"
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
	config.FileBloomExpectedItems = 100       // Smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Higher false positive rate
	config.RowDataCompression = CompressionNone
	config.MaxFilesToMergePerOperation = 5 // Allow merging up to 5 files

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

	ctx := context.Background()

	// First batch of test data
	batch1 := []map[string]any{
		{"id": 1.0, "name": "Alice", "level": "error", "service": "auth"},
		{"id": 2.0, "name": "Bob", "level": "info", "service": "payment"},
	}

	// Second batch of test data
	batch2 := []map[string]any{
		{"id": 3.0, "name": "Charlie", "level": "warn", "service": "auth"},
		{"id": 4.0, "name": "Diana", "level": "error", "service": "database"},
	}

	// Third batch of test data
	batch3 := []map[string]any{
		{"id": 5.0, "name": "Eve", "level": "info", "service": "payment"},
		{"id": 6.0, "name": "Frank", "level": "debug", "service": "cache"},
	}

	// Fourth batch of test data (will use different bloom filter parameters)
	batch4 := []map[string]any{
		{"id": 7.0, "name": "Grace", "level": "warn", "service": "monitoring"},
		{"id": 8.0, "name": "Henry", "level": "info", "service": "logging"},
	}

	// All test data combined for verification
	allTestData := make([]map[string]any, 0)
	allTestData = append(allTestData, batch1...)
	allTestData = append(allTestData, batch2...)
	allTestData = append(allTestData, batch3...)
	allTestData = append(allTestData, batch4...)

	// Insert and flush batch 1
	flushAndWait(t, engine, ctx, batch1, "batch1")

	// Insert and flush batch 2
	flushAndWait(t, engine, ctx, batch2, "batch2")

	// Insert and flush batch 3
	flushAndWait(t, engine, ctx, batch3, "batch3")

	// Stop current engine to create a new one with different bloom parameters
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = engine.Stop(stopCtx)
	stopCancel()
	if err != nil {
		t.Fatalf("Failed to stop engine: %v", err)
	}

	// Create new engine with different bloom filter parameters for batch 4
	// This will create a file that can't be merged with the others
	configDifferent := DefaultBloomSearchEngineConfig()
	configDifferent.MaxBufferedRows = 2
	configDifferent.MaxBufferedBytes = 1024 * 1024
	configDifferent.MaxBufferedTime = 10 * time.Second
	configDifferent.FileBloomExpectedItems = 200  // Different expected items
	configDifferent.BloomFalsePositiveRate = 0.02 // Different false positive rate
	configDifferent.RowDataCompression = CompressionNone
	configDifferent.MaxFilesToMergePerOperation = 5

	engineDifferent, err := NewBloomSearchEngine(configDifferent, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine with different config: %v", err)
	}

	engineDifferent.Start()

	// Insert and flush batch 4 with different bloom parameters
	doneChan := make(chan error, 1)
	err = engineDifferent.IngestRows(ctx, batch4, doneChan)
	if err != nil {
		t.Fatalf("Failed to ingest batch4: %v", err)
	}

	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Flush failed for batch4: %v", err)
		}
		t.Logf("Successfully flushed batch4 with different bloom parameters")
	case <-time.After(5 * time.Second):
		t.Fatalf("Flush did not complete within timeout for batch4")
	}

	// Stop the different engine
	stopCtx2, stopCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	err = engineDifferent.Stop(stopCtx2)
	stopCancel2()
	if err != nil {
		t.Fatalf("Failed to stop different engine: %v", err)
	}

	// Recreate original engine for merge and query operations
	engine, err = NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to recreate original engine: %v", err)
	}

	engine.Start()

	t.Logf("All batches inserted and flushed. Total rows: %d", len(allTestData))

	// Test queries that should match ALL data (before merge)
	t.Log("=== Testing queries BEFORE merge ===")

	// All rows have 'level' field
	runQueryTest(t, engine, ctx, "field search for 'level' should match all rows",
		NewQuery().Field("level"),
		len(allTestData), allTestData)

	// Only Alice and Eve have tokens that include 'alice' and 'eve'
	runQueryTest(t, engine, ctx, "token search for 'alice' should match first row",
		NewQuery().Token("alice"),
		1, []map[string]any{batch1[0]})

	// Alice and Diana have level=error
	expectedErrorRows := []map[string]any{batch1[0], batch2[1]}
	runQueryTest(t, engine, ctx, "field-token search for 'level:error' should match error rows",
		NewQuery().FieldToken("level", "error"),
		2, expectedErrorRows)

	// Auth service appears in batch1[0] and batch2[0]
	expectedAuthRows := []map[string]any{batch1[0], batch2[0]}
	runQueryTest(t, engine, ctx, "field-token search for 'service:auth' should match auth rows",
		NewQuery().FieldToken("service", "auth"),
		2, expectedAuthRows)

	// Test queries targeting data from the single unmergeable file (batch 4)
	runQueryTest(t, engine, ctx, "token search for 'grace' should match single file row",
		NewQuery().Token("grace"),
		1, []map[string]any{batch4[0]})

	runQueryTest(t, engine, ctx, "token search for 'henry' should match single file row",
		NewQuery().Token("henry"),
		1, []map[string]any{batch4[1]})

	runQueryTest(t, engine, ctx, "field-token search for 'service:monitoring' should match single file row",
		NewQuery().FieldToken("service", "monitoring"),
		1, []map[string]any{batch4[0]})

	runQueryTest(t, engine, ctx, "field-token search for 'service:logging' should match single file row",
		NewQuery().FieldToken("service", "logging"),
		1, []map[string]any{batch4[1]})

	// Test cross-file queries that should match data from both merged and single files
	runQueryTest(t, engine, ctx, "field-token search for 'level:warn' should match rows from merged and single files",
		NewQuery().FieldToken("level", "warn"),
		2, []map[string]any{batch2[0], batch4[0]}) // Charlie and Grace

	// Test nonexistent queries
	runQueryTest(t, engine, ctx, "field search for nonexistent field should not match",
		NewQuery().Field("nonexistent"),
		0, []map[string]any{})

	runQueryTest(t, engine, ctx, "token search for nonexistent token should not match",
		NewQuery().Token("nonexistent"),
		0, []map[string]any{})

	// Now perform merge
	t.Log("=== Performing merge operation ===")
	mergeStats, err := engine.Merge(ctx)
	if err != nil {
		t.Fatalf("Merge operation failed: %v", err)
	}

	fmt.Printf("Merge stats: %+v\n", mergeStats)

	t.Log("Merge operation completed successfully")

	// Test the same queries AFTER merge to ensure data integrity
	t.Log("=== Testing queries AFTER merge ===")

	// All rows should still have 'level' field
	runQueryTest(t, engine, ctx, "AFTER MERGE: field search for 'level' should match all rows",
		NewQuery().Field("level"),
		len(allTestData), allTestData)

	// Alice should still be found
	runQueryTest(t, engine, ctx, "AFTER MERGE: token search for 'alice' should match first row",
		NewQuery().Token("alice"),
		1, []map[string]any{batch1[0]})

	// Error level rows should still match
	runQueryTest(t, engine, ctx, "AFTER MERGE: field-token search for 'level:error' should match error rows",
		NewQuery().FieldToken("level", "error"),
		2, expectedErrorRows)

	// Auth service rows should still match
	runQueryTest(t, engine, ctx, "AFTER MERGE: field-token search for 'service:auth' should match auth rows",
		NewQuery().FieldToken("service", "auth"),
		2, expectedAuthRows)

	// Test that single file data is still accessible after merge
	runQueryTest(t, engine, ctx, "AFTER MERGE: token search for 'grace' should match single file row",
		NewQuery().Token("grace"),
		1, []map[string]any{batch4[0]})

	runQueryTest(t, engine, ctx, "AFTER MERGE: token search for 'henry' should match single file row",
		NewQuery().Token("henry"),
		1, []map[string]any{batch4[1]})

	runQueryTest(t, engine, ctx, "AFTER MERGE: field-token search for 'service:monitoring' should match single file row",
		NewQuery().FieldToken("service", "monitoring"),
		1, []map[string]any{batch4[0]})

	runQueryTest(t, engine, ctx, "AFTER MERGE: field-token search for 'service:logging' should match single file row",
		NewQuery().FieldToken("service", "logging"),
		1, []map[string]any{batch4[1]})

	// Test cross-file queries still work after merge
	runQueryTest(t, engine, ctx, "AFTER MERGE: field-token search for 'level:warn' should match rows from merged and single files",
		NewQuery().FieldToken("level", "warn"),
		2, []map[string]any{batch2[0], batch4[0]}) // Charlie and Grace

	// Nonexistent queries should still return nothing
	runQueryTest(t, engine, ctx, "AFTER MERGE: field search for nonexistent field should not match",
		NewQuery().Field("nonexistent"),
		0, []map[string]any{})

	runQueryTest(t, engine, ctx, "AFTER MERGE: token search for nonexistent token should not match",
		NewQuery().Token("nonexistent"),
		0, []map[string]any{})

	t.Log("=== All tests passed! Merge operation preserved data integrity ===")

}

func TestBloomSearchEngineMergeWithPartitionsAndMinMax(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/merge_test_partitions_minmax"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	// Create test directory for file system data store
	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	// Create partition function that partitions by service type
	partitionFunc := func(row map[string]any) string {
		if service, ok := row["service"].(string); ok {
			switch service {
			case "auth", "user":
				return "auth_partition"
			case "payment", "billing":
				return "financial_partition"
			case "api", "gateway":
				return "api_partition"
			default:
				return "other_partition"
			}
		}
		return "unknown_partition"
	}

	// Create config with partitions and minmax indexes
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 3                // Flush after 3 rows
	config.MaxBufferedBytes = 1024 * 1024     // Large byte limit
	config.MaxBufferedTime = 10 * time.Second // Large time limit
	config.FileBloomExpectedItems = 100       // Smaller bloom filter
	config.BloomFalsePositiveRate = 0.01      // Higher false positive rate
	config.RowDataCompression = CompressionNone
	config.MaxFilesToMergePerOperation = 5
	config.PartitionFunc = partitionFunc                          // Enable partitioning
	config.MinMaxIndexes = []string{"timestamp", "response_time"} // Enable minmax indexes

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

	ctx := context.Background()

	// Create test data with different partitions and minmax values
	// Each batch will have mixed partitions to test merging behavior

	// Batch 1: Mixed auth/financial partitions, early timestamps
	batch1 := []map[string]any{
		{"id": 1.0, "service": "auth", "action": "login", "timestamp": 1000.0, "response_time": 50.0, "level": "info"},
		{"id": 2.0, "service": "payment", "action": "charge", "timestamp": 1100.0, "response_time": 120.0, "level": "info"},
		{"id": 3.0, "service": "user", "action": "profile", "timestamp": 1200.0, "response_time": 30.0, "level": "debug"},
	}

	// Batch 2: More auth/financial partitions, mid timestamps
	batch2 := []map[string]any{
		{"id": 4.0, "service": "auth", "action": "logout", "timestamp": 2000.0, "response_time": 25.0, "level": "info"},
		{"id": 5.0, "service": "billing", "action": "invoice", "timestamp": 2100.0, "response_time": 200.0, "level": "warn"},
		{"id": 6.0, "service": "user", "action": "update", "timestamp": 2200.0, "response_time": 75.0, "level": "info"},
	}

	// Batch 3: API partition, later timestamps
	batch3 := []map[string]any{
		{"id": 7.0, "service": "api", "action": "get", "timestamp": 3000.0, "response_time": 15.0, "level": "debug"},
		{"id": 8.0, "service": "gateway", "action": "route", "timestamp": 3100.0, "response_time": 45.0, "level": "info"},
		{"id": 9.0, "service": "api", "action": "post", "timestamp": 3200.0, "response_time": 100.0, "level": "info"},
	}

	// Batch 4: Mixed partitions, wide timestamp range to test minmax merging
	batch4 := []map[string]any{
		{"id": 10.0, "service": "auth", "action": "reset", "timestamp": 500.0, "response_time": 300.0, "level": "error"},         // Earlier timestamp
		{"id": 11.0, "service": "payment", "action": "refund", "timestamp": 4000.0, "response_time": 5.0, "level": "info"},       // Later timestamp
		{"id": 12.0, "service": "monitoring", "action": "alert", "timestamp": 2500.0, "response_time": 1000.0, "level": "error"}, // Other partition
	}

	// All test data combined for verification
	allTestData := make([]map[string]any, 0)
	allTestData = append(allTestData, batch1...)
	allTestData = append(allTestData, batch2...)
	allTestData = append(allTestData, batch3...)
	allTestData = append(allTestData, batch4...)

	// Helper function to flush and wait
	flushAndWait := func(batch []map[string]any, batchName string) {
		doneChan := make(chan error, 1)
		err := engine.IngestRows(ctx, batch, doneChan)
		if err != nil {
			t.Fatalf("Failed to ingest %s: %v", batchName, err)
		}

		select {
		case err := <-doneChan:
			if err != nil {
				t.Fatalf("Flush failed for %s: %v", batchName, err)
			}
			t.Logf("Successfully flushed %s", batchName)
		case <-time.After(5 * time.Second):
			t.Fatalf("Flush did not complete within timeout for %s", batchName)
		}
	}

	// Insert and flush all batches
	flushAndWait(batch1, "batch1")
	flushAndWait(batch2, "batch2")
	flushAndWait(batch3, "batch3")
	flushAndWait(batch4, "batch4")

	t.Logf("All batches inserted and flushed. Total rows: %d", len(allTestData))

	// Test queries BEFORE merge to establish baseline
	t.Log("=== Testing queries BEFORE merge ===")

	// Test partition-specific queries
	authPartitionRows := []map[string]any{batch1[0], batch1[2], batch2[0], batch2[2], batch4[0]} // auth/user service rows
	runQueryTest(t, engine, ctx, "Partition query: auth_partition should match auth/user service rows",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("auth_partition"))),
		len(authPartitionRows), authPartitionRows)

	financialPartitionRows := []map[string]any{batch1[1], batch2[1], batch4[1]} // payment/billing service rows
	runQueryTest(t, engine, ctx, "Partition query: financial_partition should match payment/billing service rows",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("financial_partition"))),
		len(financialPartitionRows), financialPartitionRows)

	apiPartitionRows := []map[string]any{batch3[0], batch3[1], batch3[2]} // api/gateway service rows
	runQueryTest(t, engine, ctx, "Partition query: api_partition should match api/gateway service rows",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("api_partition"))),
		len(apiPartitionRows), apiPartitionRows)

	otherPartitionRows := []map[string]any{batch4[2]} // monitoring service row
	runQueryTest(t, engine, ctx, "Partition query: other_partition should match monitoring service row",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("other_partition"))),
		len(otherPartitionRows), otherPartitionRows)

	// Test MinMax index range queries
	// NOTE: MinMax indexes work at the block level, so queries might return more results
	// than expected if blocks contain data outside the exact query range
	runQueryTest(t, engine, ctx, "MinMax query: timestamp <= 1200 should match early rows (may include more due to block-level MinMax)",
		NewQuery().MatchPrefilter(MinMax("timestamp", NumericLessThanEqual(1200))),
		-1, nil) // Skip validation due to block-level MinMax behavior

	midTimestampRows := []map[string]any{batch2[0], batch2[1], batch2[2]} // timestamp between 2000-2200
	runQueryTest(t, engine, ctx, "MinMax query: timestamp between 2000-2200 should match mid rows",
		NewQuery().MatchPrefilter(MinMax("timestamp", NumericBetween(2000, 2200))),
		len(midTimestampRows), midTimestampRows)

	runQueryTest(t, engine, ctx, "MinMax query: response_time <= 30 should match fast responses (may include more due to block-level MinMax)",
		NewQuery().MatchPrefilter(MinMax("response_time", NumericLessThanEqual(30))),
		-1, nil) // Skip validation due to block-level MinMax behavior

	slowResponseRows := []map[string]any{batch2[1], batch4[0], batch4[2]} // response_time >= 200
	runQueryTest(t, engine, ctx, "MinMax query: response_time >= 200 should match slow responses",
		NewQuery().MatchPrefilter(MinMax("response_time", NumericGreaterThanEqual(200))),
		len(slowResponseRows), slowResponseRows)

	// Test combined partition + MinMax queries
	runQueryTest(t, engine, ctx, "Combined query: auth_partition + early timestamp should match auth early rows (may include more due to block-level MinMax)",
		NewQuery().
			MatchPrefilter(PrefilterAnd(
				Partition(PartitionEquals("auth_partition")),
				MinMax("timestamp", NumericLessThanEqual(1200)),
			)),
		-1, nil) // Skip validation due to block-level MinMax behavior

	financialSlowRows := []map[string]any{batch2[1]} // financial partition + slow response
	runQueryTest(t, engine, ctx, "Combined query: financial_partition + slow response should match billing row",
		NewQuery().
			MatchPrefilter(PrefilterAnd(
				Partition(PartitionEquals("financial_partition")),
				MinMax("response_time", NumericGreaterThanEqual(200)),
			)),
		len(financialSlowRows), financialSlowRows)

	// Test bloom filter queries combined with partitions/minmax
	authInfoRows := []map[string]any{batch1[0], batch2[0], batch2[2]} // auth partition + info level
	runQueryTest(t, engine, ctx, "Bloom + partition query: auth_partition + info level",
		NewQuery().
			MatchPrefilter(Partition(PartitionEquals("auth_partition"))).
			FieldToken("level", "info"),
		len(authInfoRows), authInfoRows)

	// Now perform merge
	t.Log("=== Performing merge operation ===")
	mergeStats, err := engine.Merge(ctx)
	if err != nil {
		t.Fatalf("Merge operation failed: %v", err)
	}

	fmt.Printf("Merge stats: %+v\n", mergeStats)

	t.Log("Merge operation completed successfully")

	// Test the same queries AFTER merge to ensure data integrity
	t.Log("=== Testing queries AFTER merge ===")

	// Test partition-specific queries after merge
	runQueryTest(t, engine, ctx, "AFTER MERGE: Partition query: auth_partition should match auth/user service rows",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("auth_partition"))),
		len(authPartitionRows), authPartitionRows)

	runQueryTest(t, engine, ctx, "AFTER MERGE: Partition query: financial_partition should match payment/billing service rows",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("financial_partition"))),
		len(financialPartitionRows), financialPartitionRows)

	runQueryTest(t, engine, ctx, "AFTER MERGE: Partition query: api_partition should match api/gateway service rows",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("api_partition"))),
		len(apiPartitionRows), apiPartitionRows)

	runQueryTest(t, engine, ctx, "AFTER MERGE: Partition query: other_partition should match monitoring service row",
		NewQuery().MatchPrefilter(Partition(PartitionEquals("other_partition"))),
		len(otherPartitionRows), otherPartitionRows)

	// Test MinMax index range queries after merge
	// NOTE: After merging, MinMax indexes may have expanded ranges due to block combination
	// This means some queries may return more results than before merge

	// After merge, the merged MinMax indexes will have expanded ranges,
	// so we need to account for the fact that more data might match
	runQueryTest(t, engine, ctx, "AFTER MERGE: MinMax query: timestamp <= 1200 should match early rows and possibly more due to merged ranges",
		NewQuery().MatchPrefilter(MinMax("timestamp", NumericLessThanEqual(1200))),
		-1, nil) // Use -1 to skip count validation since merging can expand matches

	runQueryTest(t, engine, ctx, "AFTER MERGE: MinMax query: timestamp between 2000-2200 should match mid rows and possibly more due to merged ranges",
		NewQuery().MatchPrefilter(MinMax("timestamp", NumericBetween(2000, 2200))),
		-1, nil) // Use -1 to skip count validation since merging can expand matches

	runQueryTest(t, engine, ctx, "AFTER MERGE: MinMax query: response_time <= 30 should match fast responses and possibly more due to merged ranges",
		NewQuery().MatchPrefilter(MinMax("response_time", NumericLessThanEqual(30))),
		-1, nil) // Use -1 to skip count validation since merging can expand matches

	runQueryTest(t, engine, ctx, "AFTER MERGE: MinMax query: response_time >= 200 should match slow responses and possibly more due to merged ranges",
		NewQuery().MatchPrefilter(MinMax("response_time", NumericGreaterThanEqual(200))),
		-1, nil) // Use -1 to skip count validation since merging can expand matches

	// Test combined partition + MinMax queries after merge
	runQueryTest(t, engine, ctx, "AFTER MERGE: Combined query: auth_partition + early timestamp should match auth early rows and possibly more due to merged ranges",
		NewQuery().
			MatchPrefilter(PrefilterAnd(
				Partition(PartitionEquals("auth_partition")),
				MinMax("timestamp", NumericLessThanEqual(1200)),
			)),
		-1, nil) // Use -1 to skip count validation since merging can expand matches

	runQueryTest(t, engine, ctx, "AFTER MERGE: Combined query: financial_partition + slow response should match billing row and possibly more due to merged ranges",
		NewQuery().
			MatchPrefilter(PrefilterAnd(
				Partition(PartitionEquals("financial_partition")),
				MinMax("response_time", NumericGreaterThanEqual(200)),
			)),
		-1, nil) // Use -1 to skip count validation since merging can expand matches

	// Test bloom filter queries combined with partitions/minmax after merge
	runQueryTest(t, engine, ctx, "AFTER MERGE: Bloom + partition query: auth_partition + info level",
		NewQuery().
			MatchPrefilter(Partition(PartitionEquals("auth_partition"))).
			FieldToken("level", "info"),
		len(authInfoRows), authInfoRows)

	// Test edge cases with minmax indexes after merge
	// Query for timestamp range that should expand due to merging
	runQueryTest(t, engine, ctx, "AFTER MERGE: MinMax query: expanded timestamp range 500-2500 should include merged ranges",
		NewQuery().MatchPrefilter(MinMax("timestamp", NumericBetween(500, 2500))),
		-1, nil) // Skip validation since merging expands ranges

	// Test that all data is still accessible
	runQueryTest(t, engine, ctx, "AFTER MERGE: All data should still be accessible",
		NewQuery().Field("id"),
		len(allTestData), allTestData)

	t.Log("=== All partition and MinMax tests passed! Merge operation preserved data integrity ===")
}

func TestBloomSearchEngineMergeDifferentCompressionConfigs(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/merge_different_compression"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore
	ctx := context.Background()

	// Step 1: Create first file with NO compression
	config1 := DefaultBloomSearchEngineConfig()
	config1.MaxBufferedRows = 2
	config1.RowDataCompression = CompressionNone

	engine1, err := NewBloomSearchEngine(config1, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine1: %v", err)
	}

	engine1.Start()

	// Insert first batch (uncompressed)
	batch1 := []map[string]any{
		{"id": 1.0, "name": "Alice"},
		{"id": 2.0, "name": "Bob"},
	}

	flushAndWait(t, engine1, ctx, batch1, "uncompressed batch")

	// Stop first engine
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = engine1.Stop(stopCtx)
	cancel()
	if err != nil {
		t.Fatalf("Failed to stop engine1: %v", err)
	}

	// Step 2: Create second file with ZSTD compression
	config2 := DefaultBloomSearchEngineConfig()
	config2.MaxBufferedRows = 2
	config2.RowDataCompression = CompressionZstd

	engine2, err := NewBloomSearchEngine(config2, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine2: %v", err)
	}

	engine2.Start()

	// Insert second batch (zstd compressed)
	batch2 := []map[string]any{
		{"id": 3.0, "name": "Charlie"},
		{"id": 4.0, "name": "Diana"},
	}

	flushAndWait(t, engine2, ctx, batch2, "zstd batch")

	// Stop second engine
	stopCtx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	err = engine2.Stop(stopCtx2)
	cancel2()
	if err != nil {
		t.Fatalf("Failed to stop engine2: %v", err)
	}

	// Step 3: Create third file with Snappy compression
	config3 := DefaultBloomSearchEngineConfig()
	config3.MaxBufferedRows = 2
	config3.RowDataCompression = CompressionSnappy

	engine3, err := NewBloomSearchEngine(config3, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine3: %v", err)
	}

	engine3.Start()

	// Insert third batch (snappy compressed)
	batch3 := []map[string]any{
		{"id": 5.0, "name": "Eve"},
		{"id": 6.0, "name": "Frank"},
	}

	flushAndWait(t, engine3, ctx, batch3, "snappy batch")

	// Stop third engine
	stopCtx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	err = engine3.Stop(stopCtx3)
	cancel3()
	if err != nil {
		t.Fatalf("Failed to stop engine3: %v", err)
	}

	// Step 4: Create final engine and merge all three files
	finalEngine, err := NewBloomSearchEngine(config1, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create final engine: %v", err)
	}

	finalEngine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		finalEngine.Stop(ctx)
	}()

	// Perform merge
	t.Log("Merging files with different compression configs (None, Zstd, Snappy)...")
	mergeStats, err := finalEngine.Merge(ctx)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	fmt.Printf("Merge stats: %+v\n", mergeStats)

	t.Log("Merge completed successfully!")

	// Step 5: Verify all data is accessible after merge
	allData := []map[string]any{
		{"id": 1.0, "name": "Alice"},
		{"id": 2.0, "name": "Bob"},
		{"id": 3.0, "name": "Charlie"},
		{"id": 4.0, "name": "Diana"},
		{"id": 5.0, "name": "Eve"},
		{"id": 6.0, "name": "Frank"},
	}

	runQueryTest(t, finalEngine, ctx, "All data should be accessible after merge",
		NewQuery().Field("name"),
		6, allData)

	t.Log("SUCCESS: Files with different compression configs (None, Zstd, Snappy) were merged successfully!")
}

func TestBloomSearchEngineQueryRegexFinalStageAndOr(t *testing.T) {
	testDir := "./test_data/query_test_regex_final_stage"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 4
	config.MaxBufferedBytes = 1024 * 1024
	config.MaxBufferedTime = 10 * time.Second
	config.FileBloomExpectedItems = 100
	config.BloomFalsePositiveRate = 0.01
	config.RowDataCompression = CompressionNone

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

	ctx := context.Background()
	rows := []map[string]any{
		{"id": 1.0, "service": "payment", "message": "timeout while charging"},
		{"id": 2.0, "service": "payment", "message": "payment succeeded"},
		{"id": 3.0, "service": "auth", "message": "timeout while login"},
		{"id": 4.0, "service": "auth", "message": "login ok"},
	}
	flushAndWait(t, engine, ctx, rows, "regex final-stage rows")

	runQueryTest(t, engine, ctx, "Regex should run as final stage with internal AND/OR",
		NewQuery().
			Field("service").
			MatchRegex(
				RegexAnd(
					RegexOr(
						FieldRegex("service", "^payment$"),
						FieldRegex("service", "^auth$"),
					),
					FieldRegex("message", "timeout"),
				),
			),
		2,
		[]map[string]any{rows[0], rows[2]},
	)
}

func TestBloomSearchEngineQueryInvalidRegexReturnsError(t *testing.T) {
	testDir := "./test_data/query_test_regex_invalid"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	config := DefaultBloomSearchEngineConfig()
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	resultChan := make(chan map[string]any, 10)
	errorChan := make(chan error, 10)
	statsChan := make(chan BlockStats, 10)
	query := NewQuery().FieldRegex("message", "[unterminated(").Build()

	err = engine.Query(context.Background(), query, resultChan, errorChan, statsChan)
	if err == nil {
		t.Fatalf("expected regex compile error but got nil")
	}
	if !strings.Contains(err.Error(), "failed to compile regex query") {
		t.Fatalf("expected compile regex error, got: %v", err)
	}
}

func TestBloomSearchEngineRegexFieldGuardPrunesFiles(t *testing.T) {
	testDir := "./test_data/query_test_regex_pruning"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 1
	config.MaxBufferedBytes = 1024 * 1024
	config.MaxBufferedTime = 10 * time.Second
	config.FileBloomExpectedItems = 100
	config.BloomFalsePositiveRate = 0.01
	config.RowDataCompression = CompressionNone

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

	ctx := context.Background()
	flushAndWait(t, engine, ctx, []map[string]any{
		{"id": 1.0, "service": "payment"},
	}, "row without regex field")
	flushAndWait(t, engine, ctx, []map[string]any{
		{"id": 2.0, "service": "auth", "message": "timeout"},
	}, "row with regex field")

	resultChan := make(chan map[string]any, 10)
	errorChan := make(chan error, 10)
	statsChan := make(chan BlockStats, 10)

	query := NewQuery().
		FieldRegex("message", "timeout").
		Build()

	err = engine.Query(ctx, query, resultChan, errorChan, statsChan)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	var results []map[string]any
	for row := range resultChan {
		results = append(results, row)
	}

	select {
	case queryErr := <-errorChan:
		if queryErr != nil {
			t.Fatalf("Query returned error: %v", queryErr)
		}
	default:
	}

	time.Sleep(50 * time.Millisecond)
	var stats []BlockStats
	for {
		select {
		case stat := <-statsChan:
			stats = append(stats, stat)
		default:
			goto doneStats
		}
	}

doneStats:
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0]["id"] != 2.0 {
		t.Fatalf("expected row id=2.0, got %v", results[0]["id"])
	}
	if len(stats) != 1 {
		t.Fatalf("expected only one block to be processed due regex field pruning, got %d", len(stats))
	}
}

func TestBloomSearchEngineQueryProcessesAllBlocksWithBoundedConcurrency(t *testing.T) {
	testDir := "./test_data/query_test_bounded_workers"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 1
	config.MaxBufferedBytes = 1024 * 1024
	config.MaxBufferedTime = 10 * time.Second
	config.FileBloomExpectedItems = 100
	config.BloomFalsePositiveRate = 0.01
	config.RowDataCompression = CompressionNone
	config.MaxQueryConcurrency = 2

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

	ctx := context.Background()
	const totalRows = 40

	for i := 0; i < totalRows; i++ {
		row := map[string]any{
			"id":      float64(i),
			"service": "workerpool",
		}
		flushAndWait(t, engine, ctx, []map[string]any{row}, fmt.Sprintf("bounded worker row %d", i))
	}

	maybeFiles, err := dataStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to read metadata for verification: %v", err)
	}

	totalBlocks := 0
	for _, maybeFile := range maybeFiles {
		totalBlocks += len(maybeFile.Metadata.DataBlocks)
	}
	if totalBlocks <= config.MaxQueryConcurrency {
		t.Fatalf("test setup invalid: expected more blocks (%d) than MaxQueryConcurrency (%d)", totalBlocks, config.MaxQueryConcurrency)
	}

	resultChan := make(chan map[string]any, totalRows)
	errorChan := make(chan error, totalRows)
	statsChan := make(chan BlockStats, totalRows)

	if err := engine.Query(ctx, NewQuery().Build(), resultChan, errorChan, statsChan); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	resultCount := 0
	for range resultChan {
		resultCount++
	}

	if resultCount != totalRows {
		t.Fatalf("Expected %d results but got %d", totalRows, resultCount)
	}

	select {
	case queryErr := <-errorChan:
		if queryErr != nil {
			t.Fatalf("Query returned error: %v", queryErr)
		}
	default:
	}
}

type blockingFirstFlushWriteStore struct {
	base               *FileSystemDataStore
	mu                 sync.Mutex
	createCount        int
	firstWriteStarted  chan struct{}
	unblockFirstWriter chan struct{}
}

func newBlockingFirstFlushWriteStore(rootDir string) *blockingFirstFlushWriteStore {
	return &blockingFirstFlushWriteStore{
		base:               NewFileSystemDataStore(rootDir),
		firstWriteStarted:  make(chan struct{}),
		unblockFirstWriter: make(chan struct{}),
	}
}

func (s *blockingFirstFlushWriteStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	writer, pointerBytes, err := s.base.CreateFile(ctx)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	s.createCount++
	createNumber := s.createCount
	s.mu.Unlock()

	if createNumber != 1 {
		return writer, pointerBytes, nil
	}

	return &blockingWriteCloser{
		writer:        writer,
		startedSignal: s.firstWriteStarted,
		unblockSignal: s.unblockFirstWriter,
	}, pointerBytes, nil
}

func (s *blockingFirstFlushWriteStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	return s.base.OpenFile(ctx, filePointerBytes)
}

func (s *blockingFirstFlushWriteStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error) {
	return s.base.GetMaybeFilesForQuery(ctx, query)
}

func (s *blockingFirstFlushWriteStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return s.base.Update(ctx, writes, deletes)
}

type blockingWriteCloser struct {
	writer        io.WriteCloser
	startedSignal chan struct{}
	unblockSignal <-chan struct{}
	once          sync.Once
}

func (w *blockingWriteCloser) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.startedSignal)
		<-w.unblockSignal
	})
	return w.writer.Write(p)
}

func (w *blockingWriteCloser) Close() error {
	return w.writer.Close()
}

func TestBloomSearchEngineFileLevelBloomRetainsInFlightRowsAcrossFlushes(t *testing.T) {
	testDir := "./test_data/query_test_file_level_bloom_lost_rows"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := newBlockingFirstFlushWriteStore(testDir)
	metaStore := dataStore

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 1000
	config.MaxBufferedBytes = 1024 * 1024
	config.MaxBufferedTime = 1 * time.Hour
	config.FileBloomExpectedItems = 100
	config.BloomFalsePositiveRate = 0.01
	config.RowDataCompression = CompressionNone

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

	ctx := context.Background()
	token := "inflightlosttoken"

	if err := engine.IngestRows(ctx, []map[string]any{
		{"id": 1.0, "message": "first_row"},
	}, nil); err != nil {
		t.Fatalf("Failed to ingest first row: %v", err)
	}

	firstFlushErrChan := make(chan error, 1)
	go func() {
		firstFlushErrChan <- engine.Flush(ctx)
	}()

	select {
	case <-dataStore.firstWriteStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for first flush to begin writing")
	}

	if err := engine.IngestRows(ctx, []map[string]any{
		{"id": 2.0, "message": token},
	}, nil); err != nil {
		t.Fatalf("Failed to ingest second row: %v", err)
	}

	secondFlushErrChan := make(chan error, 1)
	go func() {
		secondFlushErrChan <- engine.Flush(ctx)
	}()

	close(dataStore.unblockFirstWriter)

	select {
	case err := <-firstFlushErrChan:
		if err != nil {
			t.Fatalf("First flush failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for first flush completion")
	}

	select {
	case err := <-secondFlushErrChan:
		if err != nil {
			t.Fatalf("Second flush failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for second flush completion")
	}

	queryAllResults := make(chan map[string]any, 10)
	queryAllErrors := make(chan error, 10)
	queryAllStats := make(chan BlockStats, 10)
	if err := engine.Query(ctx, NewQuery().Build(), queryAllResults, queryAllErrors, queryAllStats); err != nil {
		t.Fatalf("Failed to start all-rows query: %v", err)
	}

	allRows := make([]map[string]any, 0, 2)
	for row := range queryAllResults {
		allRows = append(allRows, row)
	}
	select {
	case queryErr := <-queryAllErrors:
		if queryErr != nil {
			t.Fatalf("All-rows query failed: %v", queryErr)
		}
	default:
	}

	if len(allRows) != 2 {
		t.Fatalf("Expected 2 total rows after two flushes, got %d", len(allRows))
	}

	tokenQueryResults := make(chan map[string]any, 10)
	tokenQueryErrors := make(chan error, 10)
	tokenQueryStats := make(chan BlockStats, 10)
	if err := engine.Query(ctx, NewQuery().Token(token).Build(), tokenQueryResults, tokenQueryErrors, tokenQueryStats); err != nil {
		t.Fatalf("Failed to start token query: %v", err)
	}

	var tokenRows []map[string]any
	for row := range tokenQueryResults {
		tokenRows = append(tokenRows, row)
	}
	select {
	case queryErr := <-tokenQueryErrors:
		if queryErr != nil {
			t.Fatalf("Token query failed: %v", queryErr)
		}
	default:
	}

	if len(tokenRows) != 1 {
		t.Fatalf("Expected token query to return 1 row after bloom filter fix, got %d", len(tokenRows))
	}
	if tokenRows[0]["id"] != 2.0 {
		t.Fatalf("Expected token query to return row id=2.0, got %v", tokenRows[0]["id"])
	}
}

func TestBloomSearchEngineStopFlushesPendingBufferedRows(t *testing.T) {
	testDir := "./test_data/stop_drops_pending_flush"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore

	tokenizerEntered := make(chan struct{})
	releaseTokenizer := make(chan struct{})
	var blockOnce sync.Once

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedRows = 10_000
	config.MaxBufferedBytes = 1024 * 1024 * 1024
	config.MaxBufferedTime = 1 * time.Hour
	config.RowDataCompression = CompressionNone
	config.Tokenizer = func(value any) []string {
		blockOnce.Do(func() {
			close(tokenizerEntered)
			<-releaseTokenizer
		})
		return BasicWhitespaceLowerTokenizer(value)
	}

	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()

	ctx := context.Background()
	if err := engine.IngestRows(ctx, []map[string]any{
		{"id": 1.0, "message": "pending_flush_row"},
	}, nil); err != nil {
		t.Fatalf("Failed to ingest row: %v", err)
	}

	select {
	case <-tokenizerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for ingest worker to enter tokenizer")
	}

	stopResult := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopResult <- engine.Stop(stopCtx)
	}()

	time.Sleep(50 * time.Millisecond)
	close(releaseTokenizer)

	select {
	case stopErr := <-stopResult:
		if stopErr != nil {
			t.Fatalf("Stop failed: %v", stopErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for Stop to complete")
	}

	if got := len(engine.flushChan); got != 0 {
		t.Fatalf("Expected no pending flush requests after stop, got %d", got)
	}

	maybeFiles, err := dataStore.GetMaybeFilesForQuery(context.Background(), nil)
	if err != nil {
		t.Fatalf("Failed to read files after stop: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("Expected 1 flushed file after stop, got %d", len(maybeFiles))
	}
	if len(maybeFiles[0].Metadata.DataBlocks) != 1 {
		t.Fatalf("Expected 1 data block in flushed file, got %d", len(maybeFiles[0].Metadata.DataBlocks))
	}
	if maybeFiles[0].Metadata.DataBlocks[0].Rows != 1 {
		t.Fatalf("Expected flushed data block to contain 1 row, got %d", maybeFiles[0].Metadata.DataBlocks[0].Rows)
	}
}
