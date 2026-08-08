package bloomsearch

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the streaming MetaStore iterator contract: candidate files flow
// through Query's pipeline as the store yields them (no full-materialization
// barrier), mid-iteration errors surface from Results.Err alongside partial
// results, and early termination releases a suspended iterator.

var errMetaStoreIteration = errors.New("injected metastore iteration failure")

// buildIteratorTestFiles flushes one single-row file per id ({"id": id})
// through a temporary engine and returns each id's MaybeFile. The engine is
// stopped before returning, so callers get a quiet goroutine baseline.
func buildIteratorTestFiles(t *testing.T, dataStore *FileSystemDataStore, ids ...string) map[string]MaybeFile {
	t.Helper()

	metaStore := NewMemoryMetaStore()
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("failed to create fixture engine: %v", err)
	}
	engine.Start()

	files := make(map[string]MaybeFile, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		ingestAndFlush(t, engine, []map[string]any{{"id": id}})
		maybeFiles, err := collectMaybeFiles(context.Background(), metaStore.GetMaybeFilesForQuery(context.Background(), nil))
		if err != nil {
			t.Fatalf("failed to list files: %v", err)
		}
		for _, maybeFile := range maybeFiles {
			pointer := string(maybeFile.PointerBytes)
			if !seen[pointer] {
				seen[pointer] = true
				files[id] = maybeFile
			}
		}
	}
	if len(files) != len(ids) {
		t.Fatalf("expected %d files, got %d", len(ids), len(files))
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.Stop(stopCtx); err != nil {
		t.Fatalf("failed to stop fixture engine: %v", err)
	}
	return files
}

// newIteratorQueryEngine builds an engine over the given MetaStore without
// starting it: queries are independent of the ingest lifecycle.
func newIteratorQueryEngine(t *testing.T, metaStore MetaStore, dataStore DataStore) *BloomSearchEngine {
	t.Helper()

	config := DefaultBloomSearchEngineConfig()
	config.MaxQueryConcurrency = 4
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	return engine
}

// waitForGoroutineDrain polls until the goroutine count returns to within a
// small slack of before. The deadline is failure detection, not
// synchronization.
func waitForGoroutineDrain(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if runtime.NumGoroutine() <= before+3 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("query goroutines did not drain: before=%d now=%d", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// gatedMetaStore yields fileA, signals suspension, then blocks until the gate
// opens (or ctx is done) before yielding fileB. cleanupRan tracks the
// iterator's deferred cleanup.
type gatedMetaStore struct {
	fileA, fileB MaybeFile
	gate         chan struct{} // closed by the test to allow yielding fileB
	suspended    chan struct{} // closed when the iterator reaches the gate
	cleanupRan   chan struct{} // closed by the iterator's deferred cleanup
}

func newGatedMetaStore(fileA, fileB MaybeFile) *gatedMetaStore {
	return &gatedMetaStore{
		fileA:      fileA,
		fileB:      fileB,
		gate:       make(chan struct{}),
		suspended:  make(chan struct{}),
		cleanupRan: make(chan struct{}),
	}
}

func (s *gatedMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		defer close(s.cleanupRan)
		if !yield(s.fileA, nil) {
			return
		}
		close(s.suspended)
		select {
		case <-s.gate:
		case <-ctx.Done():
			return
		}
		yield(s.fileB, nil)
	}
}

func (s *gatedMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return nil
}

// erroringMetaStore yields fileA, then yields an error.
type erroringMetaStore struct {
	fileA       MaybeFile
	err         error
	updateCalls atomic.Int64
}

func (s *erroringMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		if !yield(s.fileA, nil) {
			return
		}
		yield(MaybeFile{}, s.err)
	}
}

func (s *erroringMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	s.updateCalls.Add(1)
	return nil
}

// ctxHonoringMetaStore yields its files, honoring ctx before each yield per
// the MetaStore contract (a canceled iteration returns without an error), and
// counts Update calls.
type ctxHonoringMetaStore struct {
	files       []MaybeFile
	updateCalls atomic.Int64
}

func (s *ctxHonoringMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		for _, file := range s.files {
			if ctx.Err() != nil {
				return
			}
			if !yield(file, nil) {
				return
			}
		}
	}
}

func (s *ctxHonoringMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	s.updateCalls.Add(1)
	return nil
}

// TestQueryEmptyMetaStoreIterator: a store yielding no files reaches a clean
// terminal state — no rows, nil Err, no block jobs recorded, and every query
// goroutine exited — covering the pipeline path that replaced the old
// totalJobs == 0 fast path.
func TestQueryEmptyMetaStoreIterator(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	engine := newIteratorQueryEngine(t, NewMemoryMetaStore(), dataStore)

	goroutinesBefore := runtime.NumGoroutine()

	res, err := engine.Query(context.Background(), NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	if res.Next() {
		t.Fatalf("expected no rows, got %v", res.Row())
	}
	if err := res.Err(); err != nil {
		t.Fatalf("expected clean completion, got %v", err)
	}
	stats := res.Stats()
	if stats.RowsMatched != 0 || len(stats.BlockStats) != 0 {
		t.Fatalf("expected empty stats, got %+v", stats)
	}

	waitForGoroutineDrain(t, goroutinesBefore)
}

// TestQueryStreamsFilesFromMetaStoreIterator proves the pipeline has no
// full-materialization barrier: file A's rows are delivered while the store
// iterator is still suspended before yielding file B.
func TestQueryStreamsFilesFromMetaStoreIterator(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	files := buildIteratorTestFiles(t, dataStore, "a", "b")

	store := newGatedMetaStore(files["a"], files["b"])
	engine := newIteratorQueryEngine(t, store, dataStore)

	res, err := engine.Query(context.Background(), NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	// File A's row arrives while the iterator has yielded only file A.
	if !res.Next() {
		t.Fatalf("expected file A's row, got terminal state (err %v)", res.Err())
	}
	if got := res.Row()["id"]; got != "a" {
		t.Fatalf("expected file A's row first, got %v", got)
	}
	<-store.suspended
	select {
	case <-store.cleanupRan:
		t.Fatal("iterator finished before the gate opened; candidate files were materialized upfront")
	default:
	}

	// Resume the iterator; file B's row follows.
	close(store.gate)
	if !res.Next() {
		t.Fatalf("expected file B's row, got terminal state (err %v)", res.Err())
	}
	if got := res.Row()["id"]; got != "b" {
		t.Fatalf("expected file B's row second, got %v", got)
	}

	if res.Next() {
		t.Fatalf("unexpected extra row: %v", res.Row())
	}
	if err := res.Err(); err != nil {
		t.Fatalf("expected clean completion, got %v", err)
	}
	<-store.cleanupRan
}

// TestQueryMetaStoreErrorMidIteration: a mid-iteration iterator error stops
// further pulls but keeps the partial results — file A's rows deliver, and
// the error surfaces from Results.Err after iteration.
func TestQueryMetaStoreErrorMidIteration(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	files := buildIteratorTestFiles(t, dataStore, "a")

	store := &erroringMetaStore{fileA: files["a"], err: errMetaStoreIteration}
	engine := newIteratorQueryEngine(t, store, dataStore)

	res, err := engine.Query(context.Background(), NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	var ids []any
	for res.Next() {
		ids = append(ids, res.Row()["id"])
	}
	if len(ids) != 1 || ids[0] != "a" {
		t.Fatalf("expected file A's row before the error, got %v", ids)
	}
	if err := res.Err(); !errors.Is(err, errMetaStoreIteration) {
		t.Fatalf("expected Err to wrap the metastore error, got %v", err)
	}
}

// TestQueryCloseReleasesSuspendedIterator: Close while the store iterator is
// suspended mid-iteration cancels the query context the iterator received,
// its deferred cleanup runs, and the query's goroutines exit.
func TestQueryCloseReleasesSuspendedIterator(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	files := buildIteratorTestFiles(t, dataStore, "a", "b")

	goroutinesBefore := runtime.NumGoroutine()

	store := newGatedMetaStore(files["a"], files["b"])
	engine := newIteratorQueryEngine(t, store, dataStore)

	res, err := engine.Query(context.Background(), NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if !res.Next() {
		t.Fatalf("expected file A's row, got terminal state (err %v)", res.Err())
	}
	<-store.suspended

	if err := res.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// The gate never opens: only the canceled query context can release the
	// iterator. Timeout is failure detection, not synchronization.
	select {
	case <-store.cleanupRan:
	case <-time.After(10 * time.Second):
		t.Fatal("iterator cleanup did not run after Close")
	}

	waitForGoroutineDrain(t, goroutinesBefore)

	if err := res.Err(); err != nil {
		t.Fatalf("Err after deliberate Close should be nil, got %v", err)
	}
}

// TestQueryCancelReleasesSuspendedIterator: canceling the Query context while
// the store iterator is suspended mid-iteration releases the iterator (its
// deferred cleanup runs), the query terminates with the context error, and
// the query's goroutines exit.
func TestQueryCancelReleasesSuspendedIterator(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	files := buildIteratorTestFiles(t, dataStore, "a", "b")

	goroutinesBefore := runtime.NumGoroutine()

	store := newGatedMetaStore(files["a"], files["b"])
	engine := newIteratorQueryEngine(t, store, dataStore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := engine.Query(ctx, NewQuery().Build())
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer res.Close()

	if !res.Next() {
		t.Fatalf("expected file A's row, got terminal state (err %v)", res.Err())
	}
	<-store.suspended

	cancel()

	if res.Next() {
		t.Fatalf("Next returned a row after cancellation: %v", res.Row())
	}
	if err := res.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Err to match context.Canceled, got %v", err)
	}

	select {
	case <-store.cleanupRan:
	case <-time.After(10 * time.Second):
		t.Fatal("iterator cleanup did not run after cancellation")
	}

	waitForGoroutineDrain(t, goroutinesBefore)
}

// TestMergeAbortsOnMetaStoreIteratorError: an error yielded by the metastore
// iterator aborts the merge with that error before anything is grouped,
// written, or committed.
func TestMergeAbortsOnMetaStoreIteratorError(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	store := &erroringMetaStore{
		fileA: MaybeFile{
			PointerBytes: []byte("phantom.dat"),
			Metadata:     FileMetadata{DataBlocks: []DataBlockMetadata{{PartitionID: "p", Rows: 1}}},
		},
		err: errMetaStoreIteration,
	}
	engine := newIteratorQueryEngine(t, store, dataStore)

	stats, err := engine.Merge(context.Background())
	if !errors.Is(err, errMetaStoreIteration) {
		t.Fatalf("expected Merge to return the iterator error, got %v", err)
	}
	if stats != nil {
		t.Fatalf("expected nil stats from an aborted merge, got %+v", stats)
	}
	if calls := store.updateCalls.Load(); calls != 0 {
		t.Fatalf("aborted merge must not commit; MetaStore.Update called %d times", calls)
	}
	if dats := dirEntriesWithSuffix(t, dataStore.rootDir, ".dat"); len(dats) != 0 {
		t.Fatalf("aborted merge left files behind: %v", dats)
	}
}

// TestMergeCanceledContextAborts: a ctx-honoring store's iterator returns
// early and errorless under a canceled ctx, which is indistinguishable from
// an exhausted store; Merge must still report the context error rather than
// zero-work success, committing and writing nothing.
func TestMergeCanceledContextAborts(t *testing.T) {
	dataStore := NewFileSystemDataStore(t.TempDir())
	store := &ctxHonoringMetaStore{files: []MaybeFile{{
		PointerBytes: []byte("phantom.dat"),
		Metadata:     FileMetadata{DataBlocks: []DataBlockMetadata{{PartitionID: "p", Rows: 1}}},
	}}}
	engine := newIteratorQueryEngine(t, store, dataStore)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := engine.Merge(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Merge to return the context error, got %v", err)
	}
	if stats != nil {
		t.Fatalf("expected nil stats from a canceled merge, got %+v", stats)
	}
	if calls := store.updateCalls.Load(); calls != 0 {
		t.Fatalf("canceled merge must not commit; MetaStore.Update called %d times", calls)
	}
	if dats := dirEntriesWithSuffix(t, dataStore.rootDir, ".dat"); len(dats) != 0 {
		t.Fatalf("canceled merge left files behind: %v", dats)
	}
}

// TestMemoryMetaStoreUpdateDuringPausedIteration: MemoryMetaStore must not
// hold its lock across yields — a concurrent Update completes while a
// consumer is paused mid-iteration.
func TestMemoryMetaStoreUpdateDuringPausedIteration(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryMetaStore()
	for i := 0; i < 2; i++ {
		write := WriteOperation{
			FileMetadata:     &FileMetadata{DataBlocks: []DataBlockMetadata{{PartitionID: "p", Rows: 1}}},
			FilePointerBytes: fmt.Appendf(nil, "file-%d", i),
		}
		if err := store.Update(ctx, []WriteOperation{write}, nil); err != nil {
			t.Fatalf("seed Update failed: %v", err)
		}
	}

	next, stop := iter.Pull2(store.GetMaybeFilesForQuery(ctx, nil))
	defer stop()
	if _, _, ok := next(); !ok {
		t.Fatal("expected the iterator to yield a first file")
	}

	// The iteration is paused after one file; Update must complete without
	// waiting for it. Timeout is failure detection, not synchronization.
	done := make(chan error, 1)
	go func() {
		write := WriteOperation{
			FileMetadata:     &FileMetadata{},
			FilePointerBytes: []byte("file-during-iteration"),
		}
		done <- store.Update(ctx, []WriteOperation{write}, nil)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Update blocked while a GetMaybeFilesForQuery iteration was paused")
	}
}
