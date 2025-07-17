package bloomsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// TestGenerateSyntheticData generates ~100GB of random data for performance testing
func TestGenerateSyntheticData(t *testing.T) {
	// Clean up test directory before starting
	testDir := "./test_data/performance_test"
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Failed to clean up test directory: %v", err)
	}

	// Create filesystem data/meta store
	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore // FileSystemDataStore implements both interfaces

	// Create config for data generation (starting with 1GB test, then scale to 100GB)
	config := DefaultBloomSearchEngineConfig()

	// Set partition function to distribute data across 10 partitions
	config.PartitionFunc = func(row map[string]any) string {
		partitionNum := rand.Intn(10)
		return fmt.Sprintf("%02d", partitionNum)
	}

	config.MaxRowGroupBytes = 10 * 1024 * 1024 // 10MB row groups
	// config.MaxRowGroupBytes = 1 * 1024 * 1024 * 1024 // 1GB row groups (for 100GB test)
	config.MaxRowGroupRows = 1000000 // Very large row limit (ensure byte limit hits first)
	// config.MaxRowGroupRows = 1000000                 // Large row limit (for 100GB test)

	// Set buffering to trigger on row group size, not buffer limits
	config.MaxBufferedRows = 2000000 // Large buffer row limit (don't trigger on row count)
	// config.MaxBufferedRows = 100000                 // Large buffer row limit (for 100GB test)
	config.MaxBufferedBytes = 200 * 1024 * 1024 // 200MB buffer (larger than row group so row group limit hits first)
	// config.MaxBufferedBytes = 1 * 1024 * 1024 * 1024 // 1GB buffer (for 100GB test)
	config.MaxBufferedTime = 60 * time.Minute // Large time limit (won't trigger)

	// Set file size to ~100MB (was 10GB for 100GB test)
	config.MaxFileSize = 100 * 1024 * 1024 // 100MB max file size
	// config.MaxFileSize = 10 * 1024 * 1024 * 1024    // 10GB max file size (for 100GB test)

	// Configure bloom filters for datasets
	config.FileBloomExpectedItems = 100000 // 100K items per file
	// config.FileBloomExpectedItems = 10000000        // 10M items per file (for 100GB test)
	config.BloomFalsePositiveRate = 0.001 // 0.1% false positive rate

	// Disable minmax indexes (but keep partitions enabled)
	config.MinMaxIndexes = []string{}

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()

	// Generate synthetic data
	ctx := context.Background()
	targetDataSize := int64(1 * 1024 * 1024 * 1024) // 1GB target (was 100GB for full test)
	// targetDataSize := int64(100 * 1024 * 1024 * 1024) // 100GB target (for full test)

	fmt.Printf("Starting data generation, target: %s\n", formatBytes(targetDataSize))
	fmt.Printf("Config: %dMB row groups, %dMB max file size\n",
		config.MaxRowGroupBytes/(1024*1024),
		config.MaxFileSize/(1024*1024))

	var totalBytesGenerated int64
	batchSize := 10 // Small batches for frequent progress updates (engine handles internal batching) // Generate 1000 rows per batch
	var batchNumber int

	rand.Seed(time.Now().UnixNano()) // Seed random generator

	startTime := time.Now()

	for totalBytesGenerated < targetDataSize {
		// Generate a batch of synthetic rows
		batch := make([]map[string]any, batchSize)
		var batchBytes int

		for i := 0; i < batchSize; i++ {
			row := generateSyntheticRow()
			batch[i] = row
			batchBytes += estimateRowSize(row)
		}

		// Ingest the batch (no need to wait for individual batches)
		err = engine.IngestRows(ctx, batch, nil)
		if err != nil {
			t.Fatalf("Failed to ingest batch %d: %v", batchNumber, err)
		}

		totalBytesGenerated += int64(batchBytes)
		batchNumber++

		if batchNumber%2000 == 0 {
			elapsed := time.Since(startTime)
			percentComplete := float64(totalBytesGenerated) / float64(targetDataSize) * 100
			throughput := float64(totalBytesGenerated) / elapsed.Seconds()

			fmt.Printf("Batch %d: Generated %s (%.1f%%), throughput: %s/s\n",
				batchNumber,
				formatBytes(totalBytesGenerated),
				percentComplete,
				formatBytes(int64(throughput)))
		}
	}

	// Stop the engine to ensure all data is written
	fmt.Println("Stopping engine to flush remaining data...")
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = engine.Stop(stopCtx)
	if err != nil {
		t.Fatalf("Engine stop failed: %v", err)
	}

	totalTime := time.Since(startTime)
	avgThroughput := float64(totalBytesGenerated) / totalTime.Seconds()

	fmt.Printf("\n=== Data Generation Complete ===\n")
	fmt.Printf("Total data generated: %s\n", formatBytes(totalBytesGenerated))
	fmt.Printf("Total time: %v\n", totalTime)
	fmt.Printf("Average throughput: %s/s\n", formatBytes(int64(avgThroughput)))
	fmt.Printf("Total batches: %d\n", batchNumber)

	// Verify final state
	fmt.Println("\n=== Verifying Final State ===")

	// Check files created
	maybeFiles, err := dataStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to get files for verification: %v", err)
	}

	totalFiles := len(maybeFiles)
	var totalRowGroups int
	var totalFileSize int64

	for i, file := range maybeFiles {
		totalRowGroups += len(file.Metadata.DataBlocks)
		totalFileSize += int64(file.Size)

		fmt.Printf("File %d: %d row groups, %s\n",
			i+1,
			len(file.Metadata.DataBlocks),
			formatBytes(int64(file.Size)))
	}

	fmt.Printf("\nSummary:\n")
	fmt.Printf("Total files: %d\n", totalFiles)
	fmt.Printf("Total row groups: %d\n", totalRowGroups)
	fmt.Printf("Total file size: %s\n", formatBytes(totalFileSize))
	fmt.Printf("Average row groups per file: %.1f\n", float64(totalRowGroups)/float64(totalFiles))

	// Verify expectations
	expectedFiles := 10            // ~1GB / 100MB per file (was ~100GB / 10GB per file)
	expectedRowGroupsPerFile := 10 // ~100MB / 10MB per row group (was ~10GB / 1GB per row group)
	// expectedFiles := 10            // ~100GB / 10GB per file (for 100GB test)
	// expectedRowGroupsPerFile := 10 // ~10GB / 1GB per row group (for 100GB test)

	if totalFiles < expectedFiles-2 || totalFiles > expectedFiles+2 {
		t.Logf("Warning: Expected ~%d files, got %d", expectedFiles, totalFiles)
	}

	avgRowGroupsPerFile := float64(totalRowGroups) / float64(totalFiles)
	if avgRowGroupsPerFile < float64(expectedRowGroupsPerFile-2) || avgRowGroupsPerFile > float64(expectedRowGroupsPerFile+2) {
		t.Logf("Warning: Expected ~%d row groups per file, got %.1f", expectedRowGroupsPerFile, avgRowGroupsPerFile)
	}

	fmt.Printf("\n✅ Data generation test completed successfully!\n")
}

// Helper function to format bytes in human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// TestInspectGeneratedFiles inspects the files created by TestGenerateSyntheticData
func TestInspectGeneratedFiles(t *testing.T) {
	testDir := "./test_data/performance_test"

	// Create filesystem stores to read the data
	dataStore := NewFileSystemDataStore(testDir)

	ctx := context.Background()

	// Get all files
	maybeFiles, err := dataStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to get files: %v", err)
	}

	fmt.Printf("=== GENERATED FILES INSPECTION ===\n")
	fmt.Printf("Total files found: %d\n\n", len(maybeFiles))

	totalSize := int64(0)
	totalRowGroups := 0
	totalRows := 0
	partitionCounts := make(map[string]int)

	for i, maybeFile := range maybeFiles {
		fmt.Printf("File %d:\n", i+1)
		fmt.Printf("  Pointer: %x\n", maybeFile.PointerBytes)
		fmt.Printf("  Data blocks: %d\n", len(maybeFile.Metadata.DataBlocks))

		fileSize := 0
		fileRows := 0
		filePartitions := make(map[string]int)

		for j, block := range maybeFile.Metadata.DataBlocks {
			fmt.Printf("    Block %d: partition='%s', rows=%d, size=%s\n",
				j+1, block.PartitionID, block.Rows, formatBytes(int64(block.Size)))
			fileSize += block.Size
			fileRows += block.Rows
			filePartitions[block.PartitionID] += block.Rows
			partitionCounts[block.PartitionID] += block.Rows
		}

		fmt.Printf("  File totals: %d rows, %s\n", fileRows, formatBytes(int64(fileSize)))
		fmt.Printf("  Partitions in file: %v\n", filePartitions)
		fmt.Printf("\n")

		totalSize += int64(fileSize)
		totalRowGroups += len(maybeFile.Metadata.DataBlocks)
		totalRows += fileRows
	}

	fmt.Printf("=== SUMMARY ===\n")
	fmt.Printf("Total files: %d\n", len(maybeFiles))
	fmt.Printf("Total row groups: %d\n", totalRowGroups)
	fmt.Printf("Total rows: %d\n", totalRows)
	fmt.Printf("Total size: %s\n", formatBytes(totalSize))
	fmt.Printf("Average file size: %s\n", formatBytes(totalSize/int64(len(maybeFiles))))
	fmt.Printf("Average row groups per file: %.1f\n", float64(totalRowGroups)/float64(len(maybeFiles)))

	fmt.Printf("\n=== PARTITION DISTRIBUTION ===\n")
	for partition, count := range partitionCounts {
		fmt.Printf("Partition '%s': %d rows\n", partition, count)
	}
}

// TestQueryPerformance queries the synthetic data and measures performance
func TestQueryPerformance(t *testing.T) {
	// Use the same test directory from data generation test
	testDir := "./test_data/performance_test"

	// Verify test data exists
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("Performance test data not found. Run TestGenerateSyntheticData first.")
	}

	// Create filesystem data/meta store pointing to existing data
	dataStore := NewFileSystemDataStore(testDir)
	metaStore := dataStore // FileSystemDataStore implements both interfaces

	// Create config for querying (should match generation config)
	config := DefaultBloomSearchEngineConfig()
	config.PartitionFunc = func(row map[string]any) string {
		partitionNum := rand.Intn(10)
		return fmt.Sprintf("%02d", partitionNum)
	}
	config.MaxRowGroupBytes = 10 * 1024 * 1024
	config.MaxRowGroupRows = 1000000
	config.MaxBufferedRows = 200000
	config.MaxBufferedBytes = 200 * 1024 * 1024
	config.MaxBufferedTime = 60 * time.Minute
	config.MaxFileSize = 100 * 1024 * 1024
	config.FileBloomExpectedItems = 100000
	config.BloomFalsePositiveRate = 0.001
	config.MinMaxIndexes = []string{}
	config.MaxQueryConcurrency = 100

	// Create and start engine
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		engine.Stop(ctx)
	}()

	ctx := context.Background()

	// Get files info for context
	maybeFiles, err := dataStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to get files for query performance test: %v", err)
	}

	fmt.Printf("=== QUERY PERFORMANCE TEST ===\n")
	fmt.Printf("Testing against %d files\n", len(maybeFiles))

	// Define test queries
	testQueries := []struct {
		name        string
		query       *Query
		description string
	}{
		{
			name:        "field_match",
			query:       NewQueryWithGroupCombinator(CombinatorAND).Field("SbdXwyPEKen").Build(),
			description: "Match a field",
		},
		{
			name:        "token_match",
			query:       NewQueryWithGroupCombinator(CombinatorAND).Token("apple").Build(),
			description: "Match a token",
		},
		{
			name: "field_token_match",
			// query: NewQueryWithGroupCombinator(CombinatorAND).FieldToken("loJ7iQtrw", "n0DQx").Build(),
			query:       NewQueryWithGroupCombinator(CombinatorAND).FieldToken("b9DVOMloi", "apple").Build(),
			description: "Match a field:token",
		},
	}

	// Execute queries and measure performance
	for _, testQuery := range testQueries {
		t.Run(testQuery.name, func(t *testing.T) {
			fmt.Printf("\n--- Query: %s ---\n", testQuery.name)
			fmt.Printf("Description: %s\n", testQuery.description)

			// Channels for query execution
			resultChan := make(chan map[string]any, 1000)
			errorChan := make(chan error, 10)
			statsChan := make(chan BlockStats, 100)

			// Track aggregate metrics
			var totalDuration time.Duration
			var totalRowsProcessed int64
			var totalBytesProcessed int64
			var totalResultsReturned int64
			var blockCount int
			var allResults []map[string]any
			var peakRowsPerSec float64
			var peakBytesPerSec float64

			// Start query
			startTime := time.Now()
			err := engine.Query(ctx, testQuery.query, resultChan, errorChan, statsChan)
			if err != nil {
				t.Fatalf("Query failed to start: %v", err)
			}

			// Process stats and results concurrently
			resultsDone := make(chan bool)

			// Process stats in background (don't wait for this to complete)
			go func() {
				for stat := range statsChan {
					blockCount++
					totalDuration += stat.Duration
					totalRowsProcessed += int64(stat.RowsProcessed)
					totalBytesProcessed += int64(stat.BytesProcessed)

					// Calculate individual block throughput and track peaks
					if stat.Duration.Seconds() > 0 {
						blockRowsPerSec := float64(stat.RowsProcessed) / stat.Duration.Seconds()
						blockBytesPerSec := float64(stat.BytesProcessed) / stat.Duration.Seconds()

						if blockRowsPerSec > peakRowsPerSec {
							peakRowsPerSec = blockRowsPerSec
						}
						if blockBytesPerSec > peakBytesPerSec {
							peakBytesPerSec = blockBytesPerSec
						}
					}

					// fmt.Printf("  Block %s[%d]: %s rows/s, %s bytes/s, %d rows, %s skipped=%t\n",
					// 	string(stat.FilePointer)[:8], // Show first 8 chars of pointer
					// 	stat.BlockOffset,
					// 	FormatRate(stat.RowsProcessed, stat.Duration),
					// 	FormatBytesPerSecond(stat.BytesProcessed, stat.Duration),
					// 	stat.RowsProcessed,
					// 	stat.Duration,
					// 	stat.BloomFilterSkipped)
				}
			}()

			// Count results in background and collect all results
			go func() {
				defer func() { resultsDone <- true }()
				for result := range resultChan {
					totalResultsReturned++
					// Collect all results
					allResults = append(allResults, result)
				}
			}()

			// Wait for query completion (only wait for resultChan to close)
			select {
			case err := <-errorChan:
				if err != nil {
					t.Fatalf("Query error: %v", err)
				}
			case <-resultsDone:
				fmt.Printf("Query complete: Results channel closed\n")
			case <-time.After(10 * time.Second):
				t.Fatalf("Query timeout after 10 seconds (processed %d blocks so far)", blockCount)
			}

			queryTime := time.Since(startTime)

			// Calculate system throughput (user perspective - wall clock)
			var systemRowsPerSec, systemBytesPerSec float64
			if queryTime.Seconds() > 0 {
				systemRowsPerSec = float64(totalRowsProcessed) / queryTime.Seconds()
				systemBytesPerSec = float64(totalBytesProcessed) / queryTime.Seconds()
			}

			// Report performance metrics
			fmt.Printf("\n=== Query Results: %s ===\n", testQuery.name)
			fmt.Printf("Total query time: %v\n", queryTime)
			fmt.Printf("Blocks processed: %d\n", blockCount)
			fmt.Printf("Total rows processed: %d\n", totalRowsProcessed)
			fmt.Printf("Total bytes processed: %s\n", formatBytes(totalBytesProcessed))
			fmt.Printf("Results returned: %d\n", totalResultsReturned)
			fmt.Printf("System throughput: %.0f rows/sec, %s/sec\n", systemRowsPerSec, formatBytes(int64(systemBytesPerSec)))
			fmt.Printf("Peak worker throughput: %.0f rows/sec, %s/sec\n", peakRowsPerSec, formatBytes(int64(peakBytesPerSec)))
			fmt.Printf("Concurrency factor: %.1fx (combined worker time: %v)\n", totalDuration.Seconds()/queryTime.Seconds(), totalDuration)

			if totalRowsProcessed > 0 {
				selectivity := float64(totalResultsReturned) / float64(totalRowsProcessed) * 100
				fmt.Printf("Query selectivity: %.2f%% (%d results / %d rows)\n",
					selectivity, totalResultsReturned, totalRowsProcessed)
			}

			if blockCount == 0 {
				t.Errorf("No blocks were processed for query %s", testQuery.name)
			}

			// Print all found results
			if len(allResults) > 0 {
				fmt.Printf("\n--- Found Results ---\n")
				for i, result := range allResults {
					resultJSON, _ := json.MarshalIndent(result, "", "  ")
					fmt.Printf("Result %d: %s\n", i+1, string(resultJSON))
				}
			} else {
				fmt.Printf("\n--- No Results Found ---\n")
			}

			fmt.Printf("✅ Query %s completed successfully\n", testQuery.name)
		})
	}

	fmt.Printf("\n=== QUERY PERFORMANCE TEST COMPLETE ===\n")
}

// Helper function to generate random alphanumeric string
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// Helper function to generate a synthetic row
func generateSyntheticRow() map[string]any {
	// Pick 1-5 keys
	numKeys := 1 + rand.Intn(5)
	row := make(map[string]any)

	for i := 0; i < numKeys; i++ {
		// Generate key: 3-12 characters
		keyLength := 3 + rand.Intn(10)
		key := generateRandomString(keyLength)

		// Pick 1-3 to decide value type: 1=string, 2-3=array of strings
		valueType := 1 + rand.Intn(3)

		if valueType == 1 {
			// Single string value: 3-12 characters
			valueLength := 3 + rand.Intn(10)
			row[key] = generateRandomString(valueLength)
		} else {
			// Array of strings: 1-3 strings, each 3-12 characters
			arraySize := 1 + rand.Intn(3)
			values := make([]string, arraySize)
			for j := 0; j < arraySize; j++ {
				valueLength := 3 + rand.Intn(10)
				values[j] = generateRandomString(valueLength)
			}
			row[key] = values
		}
	}

	return row
}

// Helper function to estimate row size in bytes
func estimateRowSize(row map[string]any) int {
	// Use JSON marshaling for accurate size estimation
	rowBytes, err := json.Marshal(row)
	if err != nil {
		return 0
	}
	// Add 4 bytes for the length prefix that the engine adds
	return len(rowBytes) + 4
}
