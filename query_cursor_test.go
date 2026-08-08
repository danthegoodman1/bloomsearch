package bloomsearch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"runtime"
	"testing"
	"time"
)

// Phase 4 tests: the engine-owned Results cursor. Completion is unambiguous
// (Next returns false exactly once iteration is over), error delivery is
// deterministic (block errors join into Err, cancellation is terminal), and
// no caller-owned channels exist to misuse.

// errInjectedBlockFault is the sentinel returned by faultingDataStore for the
// poisoned file pointer.
var errInjectedBlockFault = errors.New("injected block fault")

// faultingDataStore delegates to a FileSystemDataStore but fails OpenFile for
// one chosen file pointer.
type faultingDataStore struct {
	base        *FileSystemDataStore
	failPointer string
}

func (s *faultingDataStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	return s.base.CreateFile(ctx)
}

func (s *faultingDataStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	if s.failPointer != "" && string(filePointerBytes) == s.failPointer {
		return nil, errInjectedBlockFault
	}
	return s.base.OpenFile(ctx, filePointerBytes)
}

func (s *faultingDataStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	return s.base.TombstoneFile(ctx, filePointerBytes)
}

// TestQueryCursorBasic: every row is delivered exactly once, Next returns
// false at the end (and stays false), Err is nil, and Stats totals are
// correct for a fully scanned dataset.
func TestQueryCursorBasic(t *testing.T) {
	engine := newTestEngine(t, nil)

	const files = 2
	const rowsPerFile = 5
	for f := 0; f < files; f++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": float64(f*rowsPerFile + i), "service": "cursor"}
		}
		ingestAndFlush(t, engine, rows)
	}

	res, err := engine.Query(context.Background(), NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	seen := make(map[float64]int)
	for res.Next() {
		id, ok := res.Row()["id"].(float64)
		if !ok {
			t.Fatalf("row missing id: %v", res.Row())
		}
		seen[id]++
	}

	if res.Next() {
		t.Fatal("Next returned true after iteration completed")
	}
	if res.Row() != nil {
		t.Fatalf("Row after completion should be nil, got %v", res.Row())
	}
	if err := res.Err(); err != nil {
		t.Fatalf("Err after clean completion should be nil, got %v", err)
	}

	const totalRows = files * rowsPerFile
	if len(seen) != totalRows {
		t.Fatalf("expected %d distinct rows, got %d", totalRows, len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("row %v delivered %d times, expected exactly once", id, count)
		}
	}

	stats := res.Stats()
	if stats.BlocksProcessed != files {
		t.Fatalf("expected %d blocks processed, got %d", files, stats.BlocksProcessed)
	}
	if stats.BlocksSkipped != 0 {
		t.Fatalf("expected 0 blocks skipped, got %d", stats.BlocksSkipped)
	}
	if stats.RowsScanned != totalRows {
		t.Fatalf("expected %d rows scanned, got %d", totalRows, stats.RowsScanned)
	}
	if stats.RowsMatched != totalRows {
		t.Fatalf("expected %d rows matched, got %d", totalRows, stats.RowsMatched)
	}
	if stats.BytesScanned <= 0 {
		t.Fatalf("expected positive bytes scanned, got %d", stats.BytesScanned)
	}
	if stats.Duration <= 0 {
		t.Fatalf("expected positive duration, got %v", stats.Duration)
	}
	if len(stats.BlockStats) != files {
		t.Fatalf("expected %d block stats entries, got %d", files, len(stats.BlockStats))
	}
}

// TestQueryCursorBlockErrorContinues: one block fails mid-query; the other
// blocks' rows are still delivered, the failure surfaces from Err via
// errors.Is, and Stats reflect the failed block.
func TestQueryCursorBlockErrorContinues(t *testing.T) {
	metaStore := NewFileSystemDataStore(t.TempDir())
	dataStore := &faultingDataStore{base: metaStore}

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	})

	// Three files of two rows each; batch k holds ids {2k, 2k+1}.
	const batches = 3
	for k := 0; k < batches; k++ {
		ingestAndFlush(t, engine, []map[string]any{
			{"id": float64(2 * k), "service": "faulty"},
			{"id": float64(2*k + 1), "service": "faulty"},
		})
	}

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, metaStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(maybeFiles) != batches {
		t.Fatalf("expected %d files, got %d", batches, len(maybeFiles))
	}
	dataStore.failPointer = string(maybeFiles[0].PointerBytes)

	res, err := engine.Query(ctx, NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	gotIDs := make(map[float64]bool)
	for res.Next() {
		gotIDs[res.Row()["id"].(float64)] = true
	}

	// The two healthy files' rows all arrived: four rows forming complete
	// batch pairs ({2k, 2k+1}).
	if len(gotIDs) != 4 {
		t.Fatalf("expected 4 rows from the healthy blocks, got %d: %v", len(gotIDs), gotIDs)
	}
	for id := range gotIDs {
		pair := id + 1
		if int(id)%2 == 1 {
			pair = id - 1
		}
		if !gotIDs[pair] {
			t.Fatalf("row %v arrived without its batch pair %v: %v", id, pair, gotIDs)
		}
	}

	// The block failure surfaces from Err without having stopped the query.
	if err := res.Err(); !errors.Is(err, errInjectedBlockFault) {
		t.Fatalf("expected Err to wrap the injected fault, got %v", err)
	}

	stats := res.Stats()
	if len(stats.BlockStats) != batches {
		t.Fatalf("expected %d block stats entries, got %d", batches, len(stats.BlockStats))
	}
	failed := 0
	for _, block := range stats.BlockStats {
		if block.BloomFilterSkipped {
			t.Fatalf("no block should be bloom-skipped, got %+v", block)
		}
		if block.RowsProcessed == 0 {
			failed++
			if block.TotalRows != 2 {
				t.Fatalf("failed block should still report its total rows, got %+v", block)
			}
		}
	}
	if failed != 1 {
		t.Fatalf("expected exactly 1 failed block with zero rows scanned, got %d", failed)
	}
	if stats.RowsScanned != 4 || stats.RowsMatched != 4 {
		t.Fatalf("expected 4 rows scanned and matched, got %d/%d", stats.RowsScanned, stats.RowsMatched)
	}
}

// TestQueryCursorCancellation: canceling the query context mid-iteration is
// terminal — Next returns false promptly, Err matches the context error, and
// the block workers drain without leaking goroutines.
func TestQueryCursorCancellation(t *testing.T) {
	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		config.MaxQueryConcurrency = 2
	})

	// Enough rows that plenty remain buffered and unscanned at cancel time.
	const files = 4
	const rowsPerFile = 300
	for f := 0; f < files; f++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": float64(f*rowsPerFile + i), "service": "cancelme"}
		}
		ingestAndFlush(t, engine, rows)
	}

	goroutinesBefore := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := engine.Query(ctx, NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	delivered := 0
	for res.Next() {
		delivered++
		if delivered == 10 {
			break
		}
	}
	if delivered != 10 {
		t.Fatalf("expected to read 10 rows before canceling, got %d", delivered)
	}

	cancel()

	// Next must return false promptly (bounded by workers observing the
	// cancellation), dropping any buffered rows.
	nextStart := time.Now()
	if res.Next() {
		t.Fatal("Next returned a row after cancellation was observable")
	}
	if elapsed := time.Since(nextStart); elapsed > 5*time.Second {
		t.Fatalf("Next took %v to observe cancellation", elapsed)
	}

	if err := res.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Err to match context.Canceled, got %v", err)
	}

	// Terminal Next already waited for the workers: done must be closed.
	select {
	case <-res.done:
	default:
		t.Fatal("worker completion channel not closed after terminal Next")
	}

	// No goroutine leak: everything the query started has exited.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if runtime.NumGoroutine() <= goroutinesBefore+3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not drain: before=%d now=%d", goroutinesBefore, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Stats are complete after the terminal Next; matched rows may exceed the
	// delivered count because buffered rows were dropped.
	stats := res.Stats()
	if stats.RowsMatched < int64(delivered) {
		t.Fatalf("stats lost matched rows: matched %d < delivered %d", stats.RowsMatched, delivered)
	}
}

// TestQueryCursorCloseEarly: Close mid-iteration winds the workers down,
// releases their semaphore slots, is idempotent, and leaves Err nil.
func TestQueryCursorCloseEarly(t *testing.T) {
	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		config.MaxQueryConcurrency = 2
	})

	const files = 4
	const rowsPerFile = 300
	for f := 0; f < files; f++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": float64(f*rowsPerFile + i), "service": "closeme"}
		}
		ingestAndFlush(t, engine, rows)
	}

	ctx := context.Background()
	res, err := engine.Query(ctx, NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		if !res.Next() {
			t.Fatalf("expected a row on Next call %d", i)
		}
	}

	if err := res.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// Close waited for the workers to wind down.
	select {
	case <-res.done:
	default:
		t.Fatal("worker completion channel not closed after Close")
	}

	if err := res.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if res.Next() {
		t.Fatal("Next returned a row after Close")
	}
	if err := res.Err(); err != nil {
		t.Fatalf("Err after deliberate Close should be nil, got %v", err)
	}

	// The closed query's semaphore slots were released: a fresh query runs to
	// completion with MaxQueryConcurrency=2.
	verify, err := engine.Query(ctx, NewQuery().Build())
	if err != nil {
		t.Fatalf("follow-up Query failed: %v", err)
	}
	defer verify.Close()
	count := 0
	for verify.Next() {
		count++
	}
	if err := verify.Err(); err != nil {
		t.Fatalf("follow-up query error: %v", err)
	}
	if count != files*rowsPerFile {
		t.Fatalf("follow-up query expected %d rows, got %d", files*rowsPerFile, count)
	}
}

// TestQueryCursorSlowConsumerNoStarvation: with a small global concurrency
// budget, a query whose consumer never calls Next must not park semaphore
// slots — an unrelated query still runs to completion.
func TestQueryCursorSlowConsumerNoStarvation(t *testing.T) {
	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		config.MaxQueryConcurrency = 2
	})

	// More matching rows than the cursor buffer holds, so query A's workers
	// end up blocked on delivery, and more blocks than the semaphore has
	// slots.
	const files = 4
	const rowsPerFile = 300
	for f := 0; f < files; f++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": float64(f*rowsPerFile + i), "service": "stalled"}
		}
		ingestAndFlush(t, engine, rows)
	}

	ctx := context.Background()

	// Query A: started, then never iterated.
	stalled, err := engine.Query(ctx, NewQuery().Build())
	if err != nil {
		t.Fatalf("stalled Query failed: %v", err)
	}
	defer stalled.Close()

	// Wait until A's delivery buffer is full, which forces its workers onto
	// the blocking-send path where they must give up their semaphore slots.
	deadline := time.Now().Add(5 * time.Second)
	for len(stalled.rowChan) < queryRowBatchBuffer {
		if time.Now().After(deadline) {
			t.Fatalf("stalled query never filled its row buffer: %d/%d", len(stalled.rowChan), queryRowBatchBuffer)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Query B must complete while A stays stalled.
	type queryBResult struct {
		count int
		err   error
	}
	bDone := make(chan queryBResult, 1)
	go func() {
		res, err := engine.Query(ctx, NewQuery().Build())
		if err != nil {
			bDone <- queryBResult{err: err}
			return
		}
		defer res.Close()
		count := 0
		for res.Next() {
			count++
		}
		bDone <- queryBResult{count: count, err: res.Err()}
	}()

	select {
	case result := <-bDone:
		if result.err != nil {
			t.Fatalf("query B failed: %v", result.err)
		}
		if result.count != files*rowsPerFile {
			t.Fatalf("query B expected %d rows, got %d", files*rowsPerFile, result.count)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query B starved behind the stalled consumer's semaphore slots")
	}
}

// TestBlockStatsAccuracy: a bloom-skipped block reports zero scanned
// rows/bytes with BloomFilterSkipped set, a scanned block reports its actual
// counts, and the QueryStats aggregates match.
func TestBlockStatsAccuracy(t *testing.T) {
	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		// Two partitions in one flush produce one file with two blocks, so
		// the pruning happens at block level (a separate file per token
		// would be pruned at file level and never produce a block job).
		config.PartitionFunc = func(row map[string]any) string {
			partition, _ := row["partition"].(string)
			return partition
		}
		// Make a bloom false positive on the pruned block negligible.
		config.BloomFalsePositiveRate = 1e-6
	})

	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1.0, "partition": "a", "message": "alphaonly"},
		{"id": 2.0, "partition": "a", "message": "alphaonly"},
		{"id": 3.0, "partition": "b", "message": "betaonly"},
		{"id": 4.0, "partition": "b", "message": "betaonly"},
		{"id": 5.0, "partition": "b", "message": "betaonly"},
	})

	res, err := engine.Query(context.Background(), NewQuery().Token("alphaonly").Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	rows := 0
	for res.Next() {
		rows++
	}
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if rows != 2 {
		t.Fatalf("expected 2 matching rows, got %d", rows)
	}

	stats := res.Stats()
	if len(stats.BlockStats) != 2 {
		t.Fatalf("expected 2 block stats entries, got %d", len(stats.BlockStats))
	}

	var scanned, skipped *BlockStats
	for i := range stats.BlockStats {
		block := &stats.BlockStats[i]
		if block.BloomFilterSkipped {
			skipped = block
		} else {
			scanned = block
		}
	}
	if scanned == nil || skipped == nil {
		t.Fatalf("expected one scanned and one skipped block, got %+v", stats.BlockStats)
	}

	if skipped.RowsProcessed != 0 || skipped.BytesProcessed != 0 {
		t.Fatalf("skipped block must report zero scanned rows/bytes, got %+v", skipped)
	}
	if skipped.TotalRows != 3 || skipped.TotalBytes <= 0 {
		t.Fatalf("skipped block must keep its totals, got %+v", skipped)
	}

	if scanned.RowsProcessed != 2 || scanned.RowsProcessed != scanned.TotalRows {
		t.Fatalf("scanned block must report actual (full) row count, got %+v", scanned)
	}
	if scanned.BytesProcessed <= 0 {
		t.Fatalf("scanned block must report scanned bytes, got %+v", scanned)
	}

	if stats.BlocksProcessed != 1 || stats.BlocksSkipped != 1 {
		t.Fatalf("expected 1 processed / 1 skipped, got %d/%d", stats.BlocksProcessed, stats.BlocksSkipped)
	}
	if stats.RowsScanned != scanned.RowsProcessed {
		t.Fatalf("RowsScanned %d != scanned block rows %d", stats.RowsScanned, scanned.RowsProcessed)
	}
	if stats.BytesScanned != scanned.BytesProcessed {
		t.Fatalf("BytesScanned %d != scanned block bytes %d", stats.BytesScanned, scanned.BytesProcessed)
	}
	if stats.RowsMatched != 2 {
		t.Fatalf("expected 2 rows matched, got %d", stats.RowsMatched)
	}
}

// TestQueryRowMaterializationEquivalence: rows materialized from the gjson
// parse used for matching are identical to what encoding/json produces for
// the same bytes (numbers as float64, nested maps/arrays, bools, nulls).
func TestQueryRowMaterializationEquivalence(t *testing.T) {
	engine := newTestEngine(t, nil)

	row := map[string]any{
		"id": "materialize1",
		"nested": map[string]any{
			"ints":   []any{1, 2, 3000000},
			"floats": []any{1.5, -2.25, 1e20},
			"bool":   true,
			"null":   nil,
			"deep":   map[string]any{"s": "hello world", "n": 9007199254740993},
		},
		"arr":   []any{map[string]any{"k": "v"}, []any{1, "two", false, nil}},
		"num":   42,
		"empty": map[string]any{},
		"str":   "text with spaces",
	}
	ingestAndFlush(t, engine, []map[string]any{row})

	rowBytes, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("failed to marshal row: %v", err)
	}
	var expected map[string]any
	if err := json.Unmarshal(rowBytes, &expected); err != nil {
		t.Fatalf("failed to unmarshal row: %v", err)
	}

	res, err := engine.Query(context.Background(), NewQuery().Token("materialize1").Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	var got []map[string]any
	for res.Next() {
		got = append(got, res.Row())
	}
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 row, got %d", len(got))
	}

	if !reflect.DeepEqual(got[0], expected) {
		t.Fatalf("materialized row differs from encoding/json:\n got: %#v\nwant: %#v", got[0], expected)
	}
}

// TestQueryCursorEmptyResult: a query matching nothing completes immediately
// with a well-formed terminal state.
func TestQueryCursorEmptyResult(t *testing.T) {
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{{"id": 1.0, "service": "lonely"}})

	res, err := engine.Query(context.Background(), NewQuery().Token("nosuchtokenanywhere").Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	if res.Next() {
		t.Fatalf("expected no rows, got %v", res.Row())
	}
	if err := res.Err(); err != nil {
		t.Fatalf("expected nil Err, got %v", err)
	}
	stats := res.Stats()
	if stats.RowsMatched != 0 {
		t.Fatalf("expected 0 rows matched, got %d", stats.RowsMatched)
	}
}
