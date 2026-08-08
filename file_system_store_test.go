package bloomsearch

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

// newFileSystemStoreEngine builds and starts an engine backed by a
// FileSystemDataStore in dir, using it as both DataStore and MetaStore.
func newFileSystemStoreEngine(t *testing.T, dir string, mutate func(*BloomSearchEngineConfig)) (*BloomSearchEngine, *FileSystemDataStore) {
	t.Helper()

	store := NewFileSystemDataStore(dir)
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	if mutate != nil {
		mutate(&config)
	}

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	})
	return engine, store
}

// collectQueryRows runs a query and drains the result channel, failing the
// test on any worker error.
func collectQueryRows(t *testing.T, engine *BloomSearchEngine, query *Query) []map[string]any {
	t.Helper()

	ctx := context.Background()
	res, err := engine.Query(ctx, query)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer res.Close()

	var rows []map[string]any
	for res.Next() {
		rows = append(rows, res.Row())
	}
	if err := res.Err(); err != nil {
		t.Fatalf("query worker error: %v", err)
	}
	return rows
}

// TestFileSystemStoreSkipsUnreadableFiles is the regression for one bad .dat
// file failing every query: garbage and truncated files in the directory are
// skipped, and queries still return the rows from valid files.
func TestFileSystemStoreSkipsUnreadableFiles(t *testing.T) {
	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, nil)

	ctx := context.Background()
	rows := []map[string]any{
		{"id": "r1", "name": "alice"},
		{"id": "r2", "name": "bob"},
		{"id": "r3", "name": "carol"},
	}
	if err := engine.IngestRows(ctx, rows, nil); err != nil {
		t.Fatalf("failed to ingest rows: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// A .dat file that was never a bloom file.
	if err := os.WriteFile(filepath.Join(dir, "garbage.dat"), []byte("not a bloom file"), 0o644); err != nil {
		t.Fatalf("failed to write garbage file: %v", err)
	}

	// A .dat file truncated mid-write: the first half of a valid file.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	var validFile string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".dat") && entry.Name() != "garbage.dat" {
			validFile = filepath.Join(dir, entry.Name())
			break
		}
	}
	if validFile == "" {
		t.Fatal("no flushed .dat file found")
	}
	validBytes, err := os.ReadFile(validFile)
	if err != nil {
		t.Fatalf("failed to read valid file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "truncated.dat"), validBytes[:len(validBytes)/2], 0o644); err != nil {
		t.Fatalf("failed to write truncated file: %v", err)
	}

	maybeFiles, err := store.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		t.Fatalf("GetMaybeFilesForQuery failed: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("expected 1 readable file, got %d", len(maybeFiles))
	}

	got := collectQueryRows(t, engine, NewQuery().Token("alice").Build())
	if len(got) != 1 || got[0]["id"] != "r1" {
		t.Fatalf("expected exactly row r1, got %v", got)
	}

	all := collectQueryRows(t, engine, nil)
	if len(all) != len(rows) {
		t.Fatalf("expected %d rows, got %d: %v", len(rows), len(all), all)
	}
}

// TestFileSystemStoreCreateFileNeverClobbersExisting is the regression for a
// colliding name draw silently overwriting a committed file: os.Rename
// replaces its destination, so CreateFile must reserve the final ".dat" path
// exclusively and redraw on collision — with a committed ".dat" or an
// orphaned ".tmp" — never letting Close rename over a file it does not own.
func TestFileSystemStoreCreateFileNeverClobbersExisting(t *testing.T) {
	dir := t.TempDir()
	store := NewFileSystemDataStore(dir)

	committedPath := filepath.Join(dir, "bloom-collide.dat")
	committedBytes := []byte("committed bloom file bytes")
	if err := os.WriteFile(committedPath, committedBytes, 0o600); err != nil {
		t.Fatalf("failed to seed committed file: %v", err)
	}

	orphanTempPath := filepath.Join(dir, "bloom-orphan.tmp")
	orphanBytes := []byte("aborted write leftovers")
	if err := os.WriteFile(orphanTempPath, orphanBytes, 0o600); err != nil {
		t.Fatalf("failed to seed orphan temp file: %v", err)
	}

	// Force the first draw to collide with the committed ".dat", the second
	// with the orphaned ".tmp", and only the third to be free.
	draws := []string{"bloom-collide", "bloom-orphan", "bloom-fresh"}
	store.drawFileName = func() string {
		if len(draws) == 0 {
			t.Fatal("CreateFile drew more names than expected")
		}
		name := draws[0]
		draws = draws[1:]
		return name
	}

	writer, pointer, err := store.CreateFile(context.Background())
	if err != nil {
		t.Fatalf("CreateFile failed: %v", err)
	}
	wantPointer := filepath.Join(dir, "bloom-fresh.dat")
	if string(pointer) != wantPointer {
		t.Fatalf("expected pointer %s, got %s", wantPointer, pointer)
	}

	payload := []byte("newly written file")
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// The committed file is untouched by the full create/write/close cycle.
	if got, err := os.ReadFile(committedPath); err != nil || string(got) != string(committedBytes) {
		t.Fatalf("committed file was altered: contents %q, err %v", got, err)
	}

	// The orphaned ".tmp" is untouched, and the reservation taken while
	// probing its name was released.
	if got, err := os.ReadFile(orphanTempPath); err != nil || string(got) != string(orphanBytes) {
		t.Fatalf("orphan temp file was altered: contents %q, err %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "bloom-orphan.dat")); !os.IsNotExist(err) {
		t.Fatalf("reservation for redrawn name was not released: %v", err)
	}

	// The new file landed at the fresh path with the written bytes, and its
	// ".tmp" was renamed away.
	if got, err := os.ReadFile(wantPointer); err != nil || string(got) != string(payload) {
		t.Fatalf("new file has wrong contents: %q, err %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "bloom-fresh.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp file was not renamed away: %v", err)
	}
}

// TestFileSystemStoreConcurrentFlushQuery runs continuous ingest/flush cycles
// while querying in a loop. In-progress files are written under a ".tmp" name
// and only renamed to ".dat" on Close, so queries must never fail on partial
// files.
func TestFileSystemStoreConcurrentFlushQuery(t *testing.T) {
	dir := t.TempDir()
	engine, _ := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.MaxBufferedRows = 20
	})

	ctx := context.Background()
	const batches = 40
	const batchSize = 25 // above MaxBufferedRows so every batch triggers a flush

	// Durability is awaited through a doneChan per batch rather than through
	// the final Flush() alone: a force-flush that finds an empty buffer acks
	// immediately, with no ordering against flushes still queued or in flight
	// (Phase 3 lifecycle work). Per-batch done delivery happens only after
	// writer.Close() and MetaStore.Update, so once every batch has acked, all
	// rows are durable and visible to queries.
	ingestDone := make(chan error, 1)
	go func() {
		doneChans := make([]chan error, 0, batches)
		for batch := 0; batch < batches; batch++ {
			rows := make([]map[string]any, batchSize)
			for i := range rows {
				rows[i] = map[string]any{
					"id":      fmt.Sprintf("row-%d-%d", batch, i),
					"service": "payment",
					"message": "connection timeout retry",
				}
			}
			doneChan := make(chan error, 1)
			if err := engine.IngestRows(ctx, rows, doneChan); err != nil {
				ingestDone <- fmt.Errorf("ingest failed: %w", err)
				return
			}
			doneChans = append(doneChans, doneChan)
		}
		for batch, doneChan := range doneChans {
			if err := <-doneChan; err != nil {
				ingestDone <- fmt.Errorf("flush covering batch %d failed: %w", batch, err)
				return
			}
		}
		ingestDone <- engine.Flush(ctx)
	}()

	query := NewQuery().FieldToken("service", "payment").Build()
	queryUntil := func(stop <-chan error) error {
		for {
			select {
			case err := <-stop:
				return err
			default:
			}

			res, err := engine.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}
			for res.Next() {
			}
			if err := res.Err(); err != nil {
				return fmt.Errorf("query worker error: %w", err)
			}
		}
	}

	if err := queryUntil(ingestDone); err != nil {
		t.Fatal(err)
	}

	// All ingested rows are durable; a final query must see every row.
	rows := collectQueryRows(t, engine, query)
	if len(rows) != batches*batchSize {
		t.Fatalf("expected %d rows after final flush, got %d", batches*batchSize, len(rows))
	}

	// No stray .tmp files: every writer was closed successfully.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Fatalf("stray temp file left behind: %s", entry.Name())
		}
	}
}
