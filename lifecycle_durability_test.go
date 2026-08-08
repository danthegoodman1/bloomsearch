package bloomsearch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// --- test doubles ---

// ctxCheckingStore delegates to a FileSystemDataStore and records a violation
// for every store call that arrives with an already-canceled context. It
// proves shutdown flushes run with a live context.
type ctxCheckingStore struct {
	base       *FileSystemDataStore
	violations atomic.Int64
}

func (s *ctxCheckingStore) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		s.violations.Add(1)
		return err
	}
	return nil
}

func (s *ctxCheckingStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	if err := s.check(ctx); err != nil {
		return nil, nil, err
	}
	return s.base.CreateFile(ctx)
}

func (s *ctxCheckingStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	return s.base.OpenFile(ctx, filePointerBytes)
}

func (s *ctxCheckingStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.base.TombstoneFile(ctx, filePointerBytes)
}

func (s *ctxCheckingStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	if err := s.check(ctx); err != nil {
		return func(yield func(MaybeFile, error) bool) {
			yield(MaybeFile{}, err)
		}
	}
	return s.base.GetMaybeFilesForQuery(ctx, query)
}

func (s *ctxCheckingStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	if err := s.check(ctx); err != nil {
		return err
	}
	return s.base.Update(ctx, writes, deletes)
}

// flushFaultStore delegates to a FileSystemDataStore and injects failures at
// CreateFile, mid-Write, Close, MetaStore.Update, or TombstoneFile. It also
// counts MetaStore.Update calls so merge tests can assert nothing was
// committed.
type flushFaultStore struct {
	base *FileSystemDataStore

	mu            sync.Mutex
	failCreate    bool
	failWrite     bool
	failClose     bool
	failUpdate    bool
	failTombstone bool
	updateCalls   int
}

func (s *flushFaultStore) setFaults(failCreate, failWrite, failClose bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failCreate, s.failWrite, s.failClose = failCreate, failWrite, failClose
}

func (s *flushFaultStore) setFailUpdate(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failUpdate = fail
}

func (s *flushFaultStore) setFailTombstone(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failTombstone = fail
}

func (s *flushFaultStore) updateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateCalls
}

func (s *flushFaultStore) resetUpdateCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls = 0
}

func (s *flushFaultStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	s.mu.Lock()
	failCreate, failWrite, failClose := s.failCreate, s.failWrite, s.failClose
	s.mu.Unlock()

	if failCreate {
		return nil, nil, errors.New("injected CreateFile failure")
	}
	writer, pointer, err := s.base.CreateFile(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &faultWriter{inner: writer, failWrite: failWrite, failClose: failClose}, pointer, nil
}

func (s *flushFaultStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	return s.base.OpenFile(ctx, filePointerBytes)
}

func (s *flushFaultStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	s.mu.Lock()
	fail := s.failTombstone
	s.mu.Unlock()
	if fail {
		return errors.New("injected TombstoneFile failure")
	}
	return s.base.TombstoneFile(ctx, filePointerBytes)
}

func (s *flushFaultStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return s.base.GetMaybeFilesForQuery(ctx, query)
}

func (s *flushFaultStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	s.mu.Lock()
	s.updateCalls++
	fail := s.failUpdate
	s.mu.Unlock()
	if fail {
		return errors.New("injected Update failure")
	}
	return s.base.Update(ctx, writes, deletes)
}

// faultWriter wraps a DataStore writer with injectable Write/Close failures.
// Abort delegates to the inner writer so store artifacts are still cleaned.
type faultWriter struct {
	inner     io.WriteCloser
	failWrite bool
	failClose bool
}

func (w *faultWriter) Write(p []byte) (int, error) {
	if w.failWrite {
		return 0, errors.New("injected Write failure")
	}
	return w.inner.Write(p)
}

func (w *faultWriter) Close() error {
	if w.failClose {
		return errors.New("injected Close failure")
	}
	return w.inner.Close()
}

func (w *faultWriter) Abort() error {
	if aborter, ok := w.inner.(interface{ Abort() error }); ok {
		return aborter.Abort()
	}
	return w.inner.Close()
}

// blockingCreateStore delegates to a FileSystemDataStore; once armed, the
// next CreateFile signals entry and blocks until released.
type blockingCreateStore struct {
	base *FileSystemDataStore

	mu            sync.Mutex
	armed         bool
	createEntered chan struct{}
	releaseCreate chan struct{}
}

func newBlockingCreateStore(base *FileSystemDataStore) *blockingCreateStore {
	return &blockingCreateStore{
		base:          base,
		createEntered: make(chan struct{}),
		releaseCreate: make(chan struct{}),
	}
}

func (s *blockingCreateStore) arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = true
}

func (s *blockingCreateStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	s.mu.Lock()
	block := s.armed
	s.armed = false
	s.mu.Unlock()

	if block {
		close(s.createEntered)
		<-s.releaseCreate
	}
	return s.base.CreateFile(ctx)
}

func (s *blockingCreateStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	return s.base.OpenFile(ctx, filePointerBytes)
}

func (s *blockingCreateStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	return s.base.TombstoneFile(ctx, filePointerBytes)
}

func (s *blockingCreateStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return s.base.GetMaybeFilesForQuery(ctx, query)
}

func (s *blockingCreateStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return s.base.Update(ctx, writes, deletes)
}

// compressionStrippingMetaStore delegates to a FileSystemDataStore but clears
// the Compression field on every returned data block, simulating metadata
// from files written before "" was normalized to CompressionNone.
type compressionStrippingMetaStore struct {
	base *FileSystemDataStore
}

func (s *compressionStrippingMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		for maybeFile, err := range s.base.GetMaybeFilesForQuery(ctx, query) {
			if err != nil {
				yield(MaybeFile{}, err)
				return
			}
			for j := range maybeFile.Metadata.DataBlocks {
				maybeFile.Metadata.DataBlocks[j].Compression = ""
			}
			if !yield(maybeFile, nil) {
				return
			}
		}
	}
}

func (s *compressionStrippingMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return s.base.Update(ctx, writes, deletes)
}

// --- helpers ---

// dirEntriesWithSuffix returns the names of directory entries ending in suffix.
func dirEntriesWithSuffix(t *testing.T, dir, suffix string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read dir %s: %v", dir, err)
	}
	var names []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), suffix) {
			names = append(names, entry.Name())
		}
	}
	return names
}

// waitDone receives one value from a done channel or fails the test.
func waitDone(t *testing.T, doneChan chan error, timeout time.Duration, what string) error {
	t.Helper()
	select {
	case err := <-doneChan:
		return err
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for done channel: %s", what)
		return nil
	}
}

// rowIDs collects the string "id" field of every row.
func rowIDs(rows []map[string]any) map[string]int {
	ids := make(map[string]int, len(rows))
	for _, row := range rows {
		if id, ok := row["id"].(string); ok {
			ids[id]++
		}
	}
	return ids
}

// --- 3A: Stop / Flush / done delivery ---

func TestStopUnderLoadLosesNoAckedRows(t *testing.T) {
	dir := t.TempDir()
	base := NewFileSystemDataStore(dir)
	store := &ctxCheckingStore{base: base}

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.MaxBufferedRows = 40
	config.RowDataCompression = CompressionNone
	config.IngestBufferSize = 8 // small so requests queue up in ingestChan

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()

	type acceptedBatch struct {
		ids      []string
		doneChan chan error
	}
	var mu sync.Mutex
	var accepted []acceptedBatch

	const workers = 8
	const rowsPerBatch = 5
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for batch := 0; ; batch++ {
				ids := make([]string, rowsPerBatch)
				rows := make([]map[string]any, rowsPerBatch)
				for i := range rows {
					ids[i] = fmt.Sprintf("g%d-b%d-r%d", g, batch, i)
					rows[i] = map[string]any{"id": ids[i], "service": "stopload"}
				}
				doneChan := make(chan error, 1)
				if err := engine.IngestRows(context.Background(), rows, doneChan); err != nil {
					if !errors.Is(err, ErrEngineStopped) {
						t.Errorf("IngestRows returned unexpected error: %v", err)
					}
					return
				}
				mu.Lock()
				accepted = append(accepted, acceptedBatch{ids: ids, doneChan: doneChan})
				mu.Unlock()
			}
		}(g)
	}

	// Let the hammer build up queued requests, then stop under load.
	time.Sleep(50 * time.Millisecond)
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := engine.Stop(stopCtx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	wg.Wait()

	if len(accepted) == 0 {
		t.Fatal("test setup invalid: no batches were accepted before Stop")
	}

	// Every accepted batch must resolve: durable ack or error, never a hang.
	ackedIDs := make(map[string]bool)
	for i, batch := range accepted {
		err := waitDone(t, batch.doneChan, 5*time.Second, fmt.Sprintf("accepted batch %d", i))
		if err == nil {
			for _, id := range batch.ids {
				ackedIDs[id] = true
			}
		}
	}

	if violations := store.violations.Load(); violations != 0 {
		t.Fatalf("%d store calls arrived with an already-canceled context", violations)
	}

	// Every acked row must be durable and appear exactly once.
	verifyEngine, err := NewBloomSearchEngine(config, base, base)
	if err != nil {
		t.Fatalf("failed to create verification engine: %v", err)
	}
	durable := rowIDs(collectQueryRows(t, verifyEngine, nil))
	for id := range ackedIDs {
		if durable[id] != 1 {
			t.Fatalf("acked row %s appears %d times in durable data, want exactly 1", id, durable[id])
		}
	}
}

func TestFlushWaitsForInFlightFlushes(t *testing.T) {
	dir := t.TempDir()
	engine, _ := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.MaxBufferedRows = 20
		config.RowDataCompression = CompressionNone
	})

	ctx := context.Background()
	const batches = 6
	const batchSize = 25 // above MaxBufferedRows so every batch triggers a flush

	for batch := 0; batch < batches; batch++ {
		rows := make([]map[string]any, batchSize)
		for i := range rows {
			rows[i] = map[string]any{
				"id":      fmt.Sprintf("b%d-r%d", batch, i),
				"service": "flushwait",
			}
		}
		if err := engine.IngestRows(ctx, rows, nil); err != nil {
			t.Fatalf("ingest of batch %d failed: %v", batch, err)
		}
	}

	// Flush goes through the same FIFO as the six flushes above, so its ack
	// cannot overtake them: everything must be durable when it returns.
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	rows := collectQueryRows(t, engine, nil)
	if len(rows) != batches*batchSize {
		t.Fatalf("expected %d rows visible after Flush, got %d", batches*batchSize, len(rows))
	}
}

// TestStopHonorsDeadlineWhenFlushWorkerWedged wedges the flush worker on an
// abandoned unbuffered done channel (documented backpressure), fills the
// flush and ingest queues behind it, and asserts Stop still returns once its
// deadline expires instead of hanging on the backed-up pipeline.
func TestStopHonorsDeadlineWhenFlushWorkerWedged(t *testing.T) {
	dir := t.TempDir()
	store := NewFileSystemDataStore(dir)

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.MaxBufferedRows = 1 // every batch triggers a flush
	config.IngestBufferSize = 1
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()

	ctx := context.Background()
	abandoned := make(chan error) // unbuffered, never received from
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "wedge"}}, abandoned); err != nil {
		t.Fatalf("wedge ingest failed: %v", err)
	}
	// Stack more batches behind the wedged delivery: one fills flushChan, the
	// rest back up the ingest actor and ingestChan.
	for i := 0; i < 3; i++ {
		go engine.IngestRows(ctx, []map[string]any{{"id": fmt.Sprintf("backlog%d", i)}}, nil)
	}
	time.Sleep(100 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	stopResult := make(chan error, 1)
	go func() { stopResult <- engine.Stop(stopCtx) }()

	select {
	case <-stopResult:
		// Stop returned — timeout error or graceful unwind, either is fine;
		// the point is it cannot hang past its deadline.
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung past its deadline with a wedged flush worker")
	}

	// A deadline-path Stop can return while the workers are still unwinding.
	// Make cleanup deterministic before TempDir removal: drain the abandoned
	// channel so any in-flight delivery completes, and wait for full worker
	// exit via a second Stop with a generous deadline (it waits on the
	// engine's WaitGroup).
	drainDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-abandoned:
			case <-drainDone:
				return
			}
		}
	}()
	defer close(drainDone)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	if err := engine.Stop(waitCtx); err != nil {
		t.Fatalf("workers did not exit after the deadline abort: %v", err)
	}
}

func TestIngestAfterStopReturnsError(t *testing.T) {
	dir := t.TempDir()
	store := NewFileSystemDataStore(dir)

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.Stop(stopCtx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if err := engine.IngestRows(context.Background(), []map[string]any{{"id": "late"}}, nil); !errors.Is(err, ErrEngineStopped) {
		t.Fatalf("expected ErrEngineStopped from IngestRows, got %v", err)
	}
	if err := engine.Flush(context.Background()); !errors.Is(err, ErrEngineStopped) {
		t.Fatalf("expected ErrEngineStopped from Flush, got %v", err)
	}
}

// --- 3B: flush error paths ---

func TestFlushErrorPathsAbortAndReport(t *testing.T) {
	cases := []struct {
		name string
		arm  func(store *flushFaultStore)
	}{
		{name: "create fails", arm: func(s *flushFaultStore) { s.setFaults(true, false, false) }},
		{name: "write fails", arm: func(s *flushFaultStore) { s.setFaults(false, true, false) }},
		{name: "close fails", arm: func(s *flushFaultStore) { s.setFaults(false, false, true) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			store := &flushFaultStore{base: NewFileSystemDataStore(dir)}

			config := DefaultBloomSearchEngineConfig()
			config.MaxBufferedTime = time.Hour
			config.RowDataCompression = CompressionNone

			engine, err := NewBloomSearchEngine(config, store, store)
			if err != nil {
				t.Fatalf("failed to create engine: %v", err)
			}
			engine.Start()
			t.Cleanup(func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				engine.Stop(stopCtx)
			})

			ctx := context.Background()
			tc.arm(store)

			// Two independently acked batches share the failing flush.
			doneChan1 := make(chan error, 1)
			doneChan2 := make(chan error, 1)
			if err := engine.IngestRows(ctx, []map[string]any{{"id": "f1"}}, doneChan1); err != nil {
				t.Fatalf("ingest 1 failed: %v", err)
			}
			if err := engine.IngestRows(ctx, []map[string]any{{"id": "f2"}}, doneChan2); err != nil {
				t.Fatalf("ingest 2 failed: %v", err)
			}

			if err := engine.Flush(ctx); err == nil {
				t.Fatal("expected Flush to report the injected failure")
			}
			if err := waitDone(t, doneChan1, 5*time.Second, "batch 1"); err == nil {
				t.Fatal("expected error on batch 1 done channel")
			}
			if err := waitDone(t, doneChan2, 5*time.Second, "batch 2"); err == nil {
				t.Fatal("expected error on batch 2 done channel")
			}

			// The aborted file left nothing behind: no visible ".dat" (not
			// even the 0-byte reservation) and no ".tmp".
			if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 0 {
				t.Fatalf("expected no .dat artifacts after aborted flush, got %v", dat)
			}
			if tmp := dirEntriesWithSuffix(t, dir, ".tmp"); len(tmp) != 0 {
				t.Fatalf("expected no .tmp artifacts after aborted flush, got %v", tmp)
			}

			// The engine stays usable after the failure.
			store.setFaults(false, false, false)
			doneChan3 := make(chan error, 1)
			if err := engine.IngestRows(ctx, []map[string]any{{"id": "f3"}}, doneChan3); err != nil {
				t.Fatalf("post-failure ingest failed: %v", err)
			}
			if err := engine.Flush(ctx); err != nil {
				t.Fatalf("post-failure Flush failed: %v", err)
			}
			if err := waitDone(t, doneChan3, 5*time.Second, "batch 3"); err != nil {
				t.Fatalf("post-failure batch reported error: %v", err)
			}
			ids := rowIDs(collectQueryRows(t, engine, nil))
			if ids["f3"] != 1 {
				t.Fatalf("expected post-failure row f3 durable exactly once, got %v", ids)
			}
		})
	}
}

// --- 3C: batch atomicity ---

func TestBatchAtomicityOnMarshalError(t *testing.T) {
	dir := t.TempDir()
	engine, _ := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.RowDataCompression = CompressionNone
	})

	ctx := context.Background()
	doneChan1 := make(chan error, 1)
	if err := engine.IngestRows(ctx, []map[string]any{
		{"id": "keep1"},
		{"id": "keep2"},
	}, doneChan1); err != nil {
		t.Fatalf("ingest of good batch failed: %v", err)
	}

	// One row of this batch cannot marshal: the whole batch is rejected and
	// the buffered state stays exactly as it was.
	doneChan2 := make(chan error, 1)
	if err := engine.IngestRows(ctx, []map[string]any{
		{"id": "poison-good"},
		{"id": "poison-bad", "bad": func() {}},
	}, doneChan2); err != nil {
		t.Fatalf("ingest of poisoned batch failed at enqueue: %v", err)
	}
	if err := waitDone(t, doneChan2, 5*time.Second, "poisoned batch"); err == nil {
		t.Fatal("expected marshal error on poisoned batch done channel")
	}

	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if err := waitDone(t, doneChan1, 5*time.Second, "good batch"); err != nil {
		t.Fatalf("good batch reported error: %v", err)
	}

	ids := rowIDs(collectQueryRows(t, engine, nil))
	if ids["keep1"] != 1 || ids["keep2"] != 1 {
		t.Fatalf("expected both good-batch rows durable exactly once, got %v", ids)
	}
	if ids["poison-good"] != 0 {
		t.Fatalf("row from rejected batch leaked into durable data: %v", ids)
	}
}

// --- 3D: merge commit safety ---

func TestMergeAbortsOnCloseFailure(t *testing.T) {
	dir := t.TempDir()
	store := &flushFaultStore{base: NewFileSystemDataStore(dir)}

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(stopCtx)
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := engine.IngestRows(ctx, []map[string]any{{"id": fmt.Sprintf("m%d", i)}}, nil); err != nil {
			t.Fatalf("ingest %d failed: %v", i, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush %d failed: %v", i, err)
		}
	}
	if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 2 {
		t.Fatalf("test setup invalid: expected 2 source files, got %v", dat)
	}

	store.setFaults(false, false, true)
	store.resetUpdateCount()

	if _, err := engine.Merge(ctx); err == nil {
		t.Fatal("expected Merge to fail on injected Close failure")
	}

	// No commit, sources intact, output invisible.
	if calls := store.updateCount(); calls != 0 {
		t.Fatalf("expected no MetaStore.Update after failed merge close, got %d calls", calls)
	}
	if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 2 {
		t.Fatalf("expected the 2 source files and no merge output, got %v", dat)
	}
	if tmp := dirEntriesWithSuffix(t, dir, ".tmp"); len(tmp) != 0 {
		t.Fatalf("expected no .tmp artifacts after aborted merge, got %v", tmp)
	}
	ids := rowIDs(collectQueryRows(t, engine, nil))
	if ids["m0"] != 1 || ids["m1"] != 1 {
		t.Fatalf("source rows must remain queryable after failed merge, got %v", ids)
	}

	// A later merge succeeds once the fault is gone.
	store.setFaults(false, false, false)
	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("post-failure merge failed: %v", err)
	}
	ids = rowIDs(collectQueryRows(t, engine, nil))
	if ids["m0"] != 1 || ids["m1"] != 1 {
		t.Fatalf("rows must survive the successful merge exactly once, got %v", ids)
	}
}

// TestFlushUpdateFailureTombstonesOrphan covers the flush-path
// MetaStore.Update failure: the file is fully written and published, so the
// engine must tombstone the never-referenced orphan and deliver the error to
// every done channel.
func TestFlushUpdateFailureTombstonesOrphan(t *testing.T) {
	dir := t.TempDir()
	store := &flushFaultStore{base: NewFileSystemDataStore(dir)}

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(stopCtx)
	})

	ctx := context.Background()
	store.setFailUpdate(true)

	doneChan1 := make(chan error, 1)
	doneChan2 := make(chan error, 1)
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "u1"}}, doneChan1); err != nil {
		t.Fatalf("ingest 1 failed: %v", err)
	}
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "u2"}}, doneChan2); err != nil {
		t.Fatalf("ingest 2 failed: %v", err)
	}
	if err := engine.Flush(ctx); err == nil {
		t.Fatal("expected Flush to report the injected Update failure")
	}
	if err := waitDone(t, doneChan1, 5*time.Second, "batch 1"); err == nil {
		t.Fatal("expected error on batch 1 done channel")
	}
	if err := waitDone(t, doneChan2, 5*time.Second, "batch 2"); err == nil {
		t.Fatal("expected error on batch 2 done channel")
	}

	// The published-but-unreferenced file was tombstoned: nothing remains.
	if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 0 {
		t.Fatalf("expected orphan tombstoned after Update failure, got %v", dat)
	}
	if tmp := dirEntriesWithSuffix(t, dir, ".tmp"); len(tmp) != 0 {
		t.Fatalf("expected no .tmp artifacts after Update failure, got %v", tmp)
	}

	// The engine stays usable after the failure.
	store.setFailUpdate(false)
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "u3"}}, nil); err != nil {
		t.Fatalf("post-failure ingest failed: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("post-failure Flush failed: %v", err)
	}
	ids := rowIDs(collectQueryRows(t, engine, nil))
	if ids["u3"] != 1 || ids["u1"] != 0 || ids["u2"] != 0 {
		t.Fatalf("expected only post-failure row u3 durable, got %v", ids)
	}
}

// TestMergeUpdateFailureTombstonesOutputsKeepsSources covers the merge-path
// MetaStore.Update failure: nothing committed, so the published merge output
// is tombstoned while the sources and the metastore stay untouched.
func TestMergeUpdateFailureTombstonesOutputsKeepsSources(t *testing.T) {
	dir := t.TempDir()
	store := &flushFaultStore{base: NewFileSystemDataStore(dir)}

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(stopCtx)
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := engine.IngestRows(ctx, []map[string]any{{"id": fmt.Sprintf("mu%d", i)}}, nil); err != nil {
			t.Fatalf("ingest %d failed: %v", i, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush %d failed: %v", i, err)
		}
	}

	sourcesBefore, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files before merge: %v", err)
	}
	sourcePointers := make(map[string]bool)
	for _, maybeFile := range sourcesBefore {
		sourcePointers[string(maybeFile.PointerBytes)] = true
	}
	if len(sourcePointers) != 2 {
		t.Fatalf("test setup invalid: expected 2 source files, got %d", len(sourcePointers))
	}

	store.setFailUpdate(true)
	if _, err := engine.Merge(ctx); err == nil {
		t.Fatal("expected Merge to fail on injected Update failure")
	}

	// Metastore unchanged: the same two source files, nothing else.
	filesAfter, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files after failed merge: %v", err)
	}
	if len(filesAfter) != 2 {
		t.Fatalf("expected metastore unchanged (2 files) after failed merge, got %d", len(filesAfter))
	}
	for _, maybeFile := range filesAfter {
		if !sourcePointers[string(maybeFile.PointerBytes)] {
			t.Fatalf("unexpected file in metastore after failed merge: %s", maybeFile.PointerBytes)
		}
	}
	// The published merge output was tombstoned and left no artifacts.
	if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 2 {
		t.Fatalf("expected only the 2 source files on disk, got %v", dat)
	}
	if tmp := dirEntriesWithSuffix(t, dir, ".tmp"); len(tmp) != 0 {
		t.Fatalf("expected no .tmp artifacts after failed merge, got %v", tmp)
	}
	ids := rowIDs(collectQueryRows(t, engine, nil))
	if ids["mu0"] != 1 || ids["mu1"] != 1 {
		t.Fatalf("source rows must remain queryable after failed merge, got %v", ids)
	}

	// A later merge succeeds once the fault is gone.
	store.setFailUpdate(false)
	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("post-failure merge failed: %v", err)
	}
	ids = rowIDs(collectQueryRows(t, engine, nil))
	if ids["mu0"] != 1 || ids["mu1"] != 1 {
		t.Fatalf("rows must survive the successful merge exactly once, got %v", ids)
	}
}

// TestMergePostCommitTombstoneFailure covers "merge committed, GC failed":
// the metastore references only the merged file, the stats come back
// alongside an error wrapping ErrPostCommitCleanup, and the unreferenced
// source files linger in the DataStore.
func TestMergePostCommitTombstoneFailure(t *testing.T) {
	dir := t.TempDir()
	dataStore := &flushFaultStore{base: NewFileSystemDataStore(dir)}
	metaStore := NewMemoryMetaStore()

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(stopCtx)
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := engine.IngestRows(ctx, []map[string]any{{"id": fmt.Sprintf("pc%d", i)}}, nil); err != nil {
			t.Fatalf("ingest %d failed: %v", i, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush %d failed: %v", i, err)
		}
	}

	sourcesBefore, err := collectMaybeFiles(ctx, metaStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files before merge: %v", err)
	}
	sourcePointers := make(map[string]bool)
	for _, maybeFile := range sourcesBefore {
		sourcePointers[string(maybeFile.PointerBytes)] = true
	}
	if len(sourcePointers) != 2 {
		t.Fatalf("test setup invalid: expected 2 source files, got %d", len(sourcePointers))
	}

	dataStore.setFailTombstone(true)
	stats, err := engine.Merge(ctx)
	if !errors.Is(err, ErrPostCommitCleanup) {
		t.Fatalf("expected error wrapping ErrPostCommitCleanup, got %v", err)
	}
	if stats == nil {
		t.Fatal("expected merge stats alongside ErrPostCommitCleanup")
	}
	if stats.FilesProcessed != 2 {
		t.Fatalf("expected stats for 2 processed files, got %d", stats.FilesProcessed)
	}

	// The merge committed: the metastore references exactly the merged file.
	filesAfter, err := collectMaybeFiles(ctx, metaStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files after merge: %v", err)
	}
	if len(filesAfter) != 1 {
		t.Fatalf("expected exactly the merged file in the metastore, got %d files", len(filesAfter))
	}
	if sourcePointers[string(filesAfter[0].PointerBytes)] {
		t.Fatalf("metastore still references a source file: %s", filesAfter[0].PointerBytes)
	}

	// The unreferenced sources linger in the DataStore (GC failed): merged
	// file plus both sources on disk.
	if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 3 {
		t.Fatalf("expected merged file plus 2 lingering sources on disk, got %v", dat)
	}

	// State is consistent: queries see every row exactly once.
	ids := rowIDs(collectQueryRows(t, engine, nil))
	if ids["pc0"] != 1 || ids["pc1"] != 1 || len(ids) != 2 {
		t.Fatalf("expected rows pc0 and pc1 exactly once after committed merge, got %v", ids)
	}
}

func TestConcurrentMergeSingleFlight(t *testing.T) {
	dir := t.TempDir()
	store := newBlockingCreateStore(NewFileSystemDataStore(dir))

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(stopCtx)
	})

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := engine.IngestRows(ctx, []map[string]any{{"id": fmt.Sprintf("c%d", i)}}, nil); err != nil {
			t.Fatalf("ingest %d failed: %v", i, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush %d failed: %v", i, err)
		}
	}

	// Hold the first merge inside its output CreateFile, then race a second.
	store.arm()
	firstResult := make(chan error, 1)
	go func() {
		_, err := engine.Merge(ctx)
		firstResult <- err
	}()

	select {
	case <-store.createEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first merge to reach CreateFile")
	}

	if _, err := engine.Merge(ctx); !errors.Is(err, ErrMergeInProgress) {
		t.Fatalf("expected ErrMergeInProgress from concurrent merge, got %v", err)
	}

	close(store.releaseCreate)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first merge failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first merge to finish")
	}

	// Post-state has every row exactly once.
	ids := rowIDs(collectQueryRows(t, engine, nil))
	if ids["c0"] != 1 || ids["c1"] != 1 || len(ids) != 2 {
		t.Fatalf("expected rows c0 and c1 exactly once after merge, got %v", ids)
	}
}

// --- 3E: merge read correctness ---

// mergeTestPartitionFunc partitions on the "p" field.
func mergeTestPartitionFunc(row map[string]any) string {
	if p, ok := row["p"].(string); ok {
		return p
	}
	return ""
}

func TestMergeSameFileBlocks(t *testing.T) {
	dir := t.TempDir()

	smallGroups := func(config *BloomSearchEngineConfig) {
		config.PartitionFunc = mergeTestPartitionFunc
		config.MaxRowGroupRows = 3
		config.RowDataCompression = CompressionNone
	}

	ctx := context.Background()
	engine1, store := newFileSystemStoreEngine(t, dir, smallGroups)

	ingestAndFlush := func(engine *BloomSearchEngine, rows []map[string]any, what string) {
		t.Helper()
		if err := engine.IngestRows(ctx, rows, nil); err != nil {
			t.Fatalf("ingest failed (%s): %v", what, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush failed (%s): %v", what, err)
		}
	}

	// Two files, each with a 2-row P block and a 1-row Q block.
	ingestAndFlush(engine1, []map[string]any{
		{"p": "P", "id": "p1"}, {"p": "P", "id": "p2"}, {"p": "Q", "id": "q1"},
	}, "file A")
	ingestAndFlush(engine1, []map[string]any{
		{"p": "P", "id": "p3"}, {"p": "P", "id": "p4"}, {"p": "Q", "id": "q2"},
	}, "file B")

	// With MaxRowGroupRows=3 the P blocks (2+2 rows) cannot merge but the Q
	// blocks (1+1) can, so the merge output carries TWO P blocks in one file.
	if _, err := engine1.Merge(ctx); err != nil {
		t.Fatalf("first merge failed: %v", err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	engine1.Stop(stopCtx)
	cancel()

	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files after first merge: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("expected 1 file after first merge, got %d", len(maybeFiles))
	}
	pBlocks := 0
	for _, block := range maybeFiles[0].Metadata.DataBlocks {
		if block.PartitionID == "P" {
			pBlocks++
		}
	}
	if pBlocks != 2 {
		t.Fatalf("fixture invalid: expected 2 same-partition blocks in one file, got %d", pBlocks)
	}

	// A third file with a matching block so the merged file can group again.
	engine3, _ := newFileSystemStoreEngine(t, dir, smallGroups)
	ingestAndFlush(engine3, []map[string]any{{"p": "P", "id": "p5"}}, "file C")
	stopCtx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	engine3.Stop(stopCtx3)
	cancel3()

	// Raised limits let all three P blocks — two of them from the SAME source
	// file — merge into one block, exercising per-reader file handles.
	engine2, _ := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.PartitionFunc = mergeTestPartitionFunc
		config.MaxRowGroupRows = 100
		config.RowDataCompression = CompressionNone
	})
	if _, err := engine2.Merge(ctx); err != nil {
		t.Fatalf("second merge failed: %v", err)
	}

	ids := rowIDs(collectQueryRows(t, engine2, nil))
	want := []string{"p1", "p2", "p3", "p4", "p5", "q1", "q2"}
	if len(ids) != len(want) {
		t.Fatalf("expected %d distinct rows after merge, got %v", len(want), ids)
	}
	for _, id := range want {
		if ids[id] != 1 {
			t.Fatalf("expected row %s exactly once after merge, got %d (%v)", id, ids[id], ids)
		}
	}
}

// TestMergeStampsRebuiltParams asserts that merged metadata describes the
// filters the merge actually built: rebuilt from the merged rows' measured
// entry counts at the merging engine's configured false positive rate, not
// the sources' parameters.
func TestMergeStampsRebuiltParams(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	oldParams := func(config *BloomSearchEngineConfig) {
		config.BloomFalsePositiveRate = 0.01
		config.MaxRowGroupRows = 50
		config.RowDataCompression = CompressionNone
	}
	newParams := func(config *BloomSearchEngineConfig) {
		config.BloomFalsePositiveRate = 0.02
		config.MaxRowGroupRows = 75
		config.RowDataCompression = CompressionNone
	}

	ingestAndFlush := func(engine *BloomSearchEngine, id string) {
		t.Helper()
		if err := engine.IngestRows(ctx, []map[string]any{{"id": id}}, nil); err != nil {
			t.Fatalf("ingest %s failed: %v", id, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush %s failed: %v", id, err)
		}
	}

	// Two files under the old bloom config.
	engineOld, store := newFileSystemStoreEngine(t, dir, oldParams)
	ingestAndFlush(engineOld, "old1")
	ingestAndFlush(engineOld, "old2")
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	engineOld.Stop(stopCtx)
	cancel()

	// Restart with a different bloom config and merge the old files.
	engineNew, _ := newFileSystemStoreEngine(t, dir, newParams)
	if _, err := engineNew.Merge(ctx); err != nil {
		t.Fatalf("merge of old-config files failed: %v", err)
	}

	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(maybeFiles))
	}
	merged := maybeFiles[0].Metadata
	if merged.BloomFalsePositiveRate != 0.02 {
		t.Fatalf("merged file metadata must carry the REBUILT filters' fpr (0.02), got %v",
			merged.BloomFalsePositiveRate)
	}
	// Both rows share the field "id" and have distinct tokens old1/old2.
	if merged.BloomEntryCounts.Fields != 1 || merged.BloomEntryCounts.Tokens != 2 || merged.BloomEntryCounts.FieldTokens != 2 {
		t.Fatalf("merged file metadata must carry measured entry counts (1 field, 2 tokens, 2 fieldtokens), got %+v",
			merged.BloomEntryCounts)
	}
	// The rebuilt file-level filters are sized from those measured counts.
	wantTokenFilter := bloom.NewWithEstimates(2, 0.02)
	if got := merged.BloomFilters.TokenBloomFilter.Cap(); got != wantTokenFilter.Cap() {
		t.Fatalf("merged file token filter must be sized from measured counts: want m=%d, got m=%d",
			wantTokenFilter.Cap(), got)
	}
	for _, block := range merged.DataBlocks {
		if block.BloomFalsePositiveRate != 0.02 {
			t.Fatalf("merged block metadata must carry the REBUILT filters' fpr (0.02), got %v",
				block.BloomFalsePositiveRate)
		}
		if block.BloomEntryCounts.Fields != 1 || block.BloomEntryCounts.Tokens != 2 || block.BloomEntryCounts.FieldTokens != 2 {
			t.Fatalf("merged block metadata must carry measured entry counts, got %+v", block.BloomEntryCounts)
		}
		if !block.HasRowDataHash {
			t.Fatalf("merged block must carry an explicit row data hash")
		}
	}

	// Files written under yet another config must merge with the rebuilt
	// file in a later pass: filter params impose no mergeability constraint.
	ingestAndFlush(engineNew, "new1")
	ingestAndFlush(engineNew, "new2")
	if _, err := engineNew.Merge(ctx); err != nil {
		t.Fatalf("merge with new-config files failed: %v", err)
	}

	ids := rowIDs(collectQueryRows(t, engineNew, nil))
	for _, id := range []string{"old1", "old2", "new1", "new2"} {
		if ids[id] != 1 {
			t.Fatalf("expected row %s exactly once after merges, got %d (%v)", id, ids[id], ids)
		}
	}
}

func TestMergeMinMaxKeySetIncompatible(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.PartitionFunc = mergeTestPartitionFunc
		config.MinMaxIndexes = []string{"ts"}
		config.RowDataCompression = CompressionNone
	})

	ingestAndFlush := func(rows []map[string]any, what string) {
		t.Helper()
		if err := engine.IngestRows(ctx, rows, nil); err != nil {
			t.Fatalf("ingest failed (%s): %v", what, err)
		}
		if err := engine.Flush(ctx); err != nil {
			t.Fatalf("flush failed (%s): %v", what, err)
		}
	}

	// File A: partition "pp" block WITH a ts minmax entry; file B: partition
	// "pp" block WITHOUT one. The "qq" blocks share the key set {ts}, so the
	// two files still form a merge group.
	ingestAndFlush([]map[string]any{
		{"p": "pp", "id": "a-with-ts", "ts": 10},
		{"p": "qq", "id": "aq", "ts": 20},
	}, "file A")
	ingestAndFlush([]map[string]any{
		{"p": "pp", "id": "b-no-ts-1"},
		{"p": "pp", "id": "b-no-ts-2"},
		{"p": "qq", "id": "bq", "ts": 30},
	}, "file B")

	tsQuery := NewQuery().MatchPrefilter(MinMax("ts", NumericBetween(0, 100))).Build()

	// Strict prefilter semantics before the merge: blocks without a ts
	// minmax entry are excluded.
	preIDs := rowIDs(collectQueryRows(t, engine, tsQuery))
	wantVisible := []string{"a-with-ts", "aq", "bq"}
	if len(preIDs) != len(wantVisible) {
		t.Fatalf("pre-merge strict prefilter expected rows %v, got %v", wantVisible, preIDs)
	}

	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// The pp blocks have differing minmax key sets so they are copied as-is,
	// never merged into one block.
	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files after merge: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("expected 1 file after merge, got %d", len(maybeFiles))
	}
	var ppWithTs, ppWithoutTs, qqBlocks int
	for _, block := range maybeFiles[0].Metadata.DataBlocks {
		switch block.PartitionID {
		case "pp":
			if _, ok := block.MinMaxIndexes["ts"]; ok {
				ppWithTs++
			} else {
				ppWithoutTs++
			}
		case "qq":
			qqBlocks++
		}
	}
	if ppWithTs != 1 || ppWithoutTs != 1 || qqBlocks != 1 {
		t.Fatalf("expected pp blocks copied as-is (1 with ts, 1 without) and 1 merged qq block, got with=%d without=%d qq=%d",
			ppWithTs, ppWithoutTs, qqBlocks)
	}

	// Strict prefilter visibility is unchanged by the merge.
	postIDs := rowIDs(collectQueryRows(t, engine, tsQuery))
	if len(postIDs) != len(wantVisible) {
		t.Fatalf("post-merge strict prefilter expected rows %v, got %v", wantVisible, postIDs)
	}
	for _, id := range wantVisible {
		if postIDs[id] != 1 {
			t.Fatalf("expected row %s visible exactly once post-merge, got %d (%v)", id, postIDs[id], postIDs)
		}
	}
	if postIDs["b-no-ts-1"] != 0 || postIDs["b-no-ts-2"] != 0 {
		t.Fatalf("rows from the ts-less block leaked into a strict ts prefilter: %v", postIDs)
	}
}

// --- 3F: lifecycle hardening ---

func TestDoubleStartStopSafe(t *testing.T) {
	dir := t.TempDir()
	store := NewFileSystemDataStore(dir)

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	engine.Start()
	engine.Start() // second Start is a no-op, no duplicate workers

	ctx := context.Background()
	doneChan := make(chan error, 1)
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "ds1"}}, doneChan); err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if err := waitDone(t, doneChan, 5*time.Second, "double-start batch"); err != nil {
		t.Fatalf("batch reported error: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.Stop(stopCtx); err != nil {
		t.Fatalf("first Stop failed: %v", err)
	}
	if err := engine.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}

	// Start after Stop is a no-op: the engine stays stopped.
	engine.Start()
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "late"}}, nil); !errors.Is(err, ErrEngineStopped) {
		t.Fatalf("expected ErrEngineStopped after Start-after-Stop, got %v", err)
	}
}

func TestEmptyIngestAcksImmediately(t *testing.T) {
	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.RowDataCompression = CompressionNone
	})

	ctx := context.Background()
	doneChan := make(chan error, 1)
	if err := engine.IngestRows(ctx, []map[string]any{}, doneChan); err != nil {
		t.Fatalf("empty ingest failed: %v", err)
	}
	if err := waitDone(t, doneChan, 2*time.Second, "empty batch ack"); err != nil {
		t.Fatalf("empty batch reported error: %v", err)
	}
	if err := engine.IngestRows(ctx, nil, nil); err != nil {
		t.Fatalf("nil-rows ingest failed: %v", err)
	}

	// Nothing was buffered, so a forced flush writes no file at all.
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}
	if dat := dirEntriesWithSuffix(t, dir, ".dat"); len(dat) != 0 {
		t.Fatalf("expected no files after empty ingests, got %v", dat)
	}

	// A real row afterwards flushes normally and no 0-row blocks exist.
	if err := engine.IngestRows(ctx, []map[string]any{{"id": "real"}}, nil); err != nil {
		t.Fatalf("real ingest failed: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("second flush failed: %v", err)
	}
	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	for _, maybeFile := range maybeFiles {
		for _, block := range maybeFile.Metadata.DataBlocks {
			if block.Rows == 0 {
				t.Fatalf("found a 0-row block in %s", maybeFile.PointerBytes)
			}
		}
	}
}

// --- 3G: config validation and "" compression ---

func TestConfigValidationTable(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(config *BloomSearchEngineConfig)
	}{
		{"nil tokenizer", func(c *BloomSearchEngineConfig) { c.Tokenizer = nil }},
		{"zero MaxRowGroupRows", func(c *BloomSearchEngineConfig) { c.MaxRowGroupRows = 0 }},
		{"negative MaxRowGroupRows", func(c *BloomSearchEngineConfig) { c.MaxRowGroupRows = -1 }},
		{"zero MaxRowGroupBytes", func(c *BloomSearchEngineConfig) { c.MaxRowGroupBytes = 0 }},
		{"negative MaxRowGroupBytes", func(c *BloomSearchEngineConfig) { c.MaxRowGroupBytes = -10 }},
		{"zero MaxFileSize", func(c *BloomSearchEngineConfig) { c.MaxFileSize = 0 }},
		{"zero MaxBufferedRows", func(c *BloomSearchEngineConfig) { c.MaxBufferedRows = 0 }},
		{"zero MaxBufferedBytes", func(c *BloomSearchEngineConfig) { c.MaxBufferedBytes = 0 }},
		{"zero MaxBufferedTime", func(c *BloomSearchEngineConfig) { c.MaxBufferedTime = 0 }},
		{"zero IngestBufferSize", func(c *BloomSearchEngineConfig) { c.IngestBufferSize = 0 }},
		{"zero BloomFalsePositiveRate", func(c *BloomSearchEngineConfig) { c.BloomFalsePositiveRate = 0 }},
		{"BloomFalsePositiveRate of 1", func(c *BloomSearchEngineConfig) { c.BloomFalsePositiveRate = 1 }},
		{"zero MaxQueryConcurrency", func(c *BloomSearchEngineConfig) { c.MaxQueryConcurrency = 0 }},
		{"MaxFilesToMergePerOperation below 2", func(c *BloomSearchEngineConfig) { c.MaxFilesToMergePerOperation = 1 }},
		{"unknown compression", func(c *BloomSearchEngineConfig) { c.RowDataCompression = "gzip" }},
		{"zstd level too low", func(c *BloomSearchEngineConfig) {
			c.RowDataCompression = CompressionZstd
			c.ZstdCompressionLevel = 0
		}},
		{"zstd level too high", func(c *BloomSearchEngineConfig) {
			c.RowDataCompression = CompressionZstd
			c.ZstdCompressionLevel = 23
		}},
	}

	store := NewFileSystemDataStore(t.TempDir())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultBloomSearchEngineConfig()
			tc.mutate(&config)
			if _, err := NewBloomSearchEngine(config, store, store); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("expected ErrInvalidConfig, got %v", err)
			}
		})
	}

	t.Run("empty compression normalized to none", func(t *testing.T) {
		config := DefaultBloomSearchEngineConfig()
		config.RowDataCompression = ""
		engine, err := NewBloomSearchEngine(config, store, store)
		if err != nil {
			t.Fatalf("empty compression must be valid, got %v", err)
		}
		if engine.config.RowDataCompression != CompressionNone {
			t.Fatalf("expected \"\" normalized to CompressionNone, got %q", engine.config.RowDataCompression)
		}
	})

	t.Run("zstd level ignored for snappy", func(t *testing.T) {
		config := DefaultBloomSearchEngineConfig()
		config.RowDataCompression = CompressionSnappy
		config.ZstdCompressionLevel = 0
		if _, err := NewBloomSearchEngine(config, store, store); err != nil {
			t.Fatalf("zstd level must not be validated for snappy, got %v", err)
		}
	})
}

func TestEmptyCompressionReadable(t *testing.T) {
	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.RowDataCompression = CompressionNone
	})

	ctx := context.Background()
	if err := engine.IngestRows(ctx, []map[string]any{
		{"id": "e1", "service": "legacy"},
		{"id": "e2", "service": "legacy"},
	}, nil); err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// Query through a metastore whose block metadata reports Compression ""
	// (as files written before normalization do): rows must still read as
	// uncompressed instead of erroring "unsupported compression type".
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	legacyEngine, err := NewBloomSearchEngine(config, &compressionStrippingMetaStore{base: store}, store)
	if err != nil {
		t.Fatalf("failed to create legacy-read engine: %v", err)
	}

	ids := rowIDs(collectQueryRows(t, legacyEngine, nil))
	if ids["e1"] != 1 || ids["e2"] != 1 {
		t.Fatalf("expected both rows readable with \"\" compression metadata, got %v", ids)
	}
}

// --- FS store artifact cleanup ---

func TestTombstoneRemovesAllArtifacts(t *testing.T) {
	ctx := context.Background()

	t.Run("mid-write tombstone removes reservation and tmp", func(t *testing.T) {
		dir := t.TempDir()
		store := NewFileSystemDataStore(dir)

		writer, pointer, err := store.CreateFile(ctx)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		if _, err := writer.Write([]byte("partial write")); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Both the 0-byte ".dat" reservation and the ".tmp" exist right now.
		if len(dirEntriesWithSuffix(t, dir, ".dat")) != 1 || len(dirEntriesWithSuffix(t, dir, ".tmp")) != 1 {
			t.Fatal("fixture invalid: expected one reservation and one temp file mid-write")
		}

		if err := store.TombstoneFile(ctx, pointer); err != nil {
			t.Fatalf("TombstoneFile failed: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("failed to read dir: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("expected empty directory after tombstone, got %v", entries)
		}

		// Release the file handle; abort after tombstone stays clean.
		if aborter, ok := writer.(interface{ Abort() error }); ok {
			aborter.Abort()
		}
	})

	t.Run("abort removes reservation and tmp", func(t *testing.T) {
		dir := t.TempDir()
		store := NewFileSystemDataStore(dir)

		writer, _, err := store.CreateFile(ctx)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		if _, err := writer.Write([]byte("aborted write")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		aborter, ok := writer.(interface{ Abort() error })
		if !ok {
			t.Fatal("FileSystemDataStore writer must implement Abort")
		}
		if err := aborter.Abort(); err != nil {
			t.Fatalf("Abort failed: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("failed to read dir: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("expected empty directory after abort, got %v", entries)
		}
	})

	t.Run("published file tombstone removes dat", func(t *testing.T) {
		dir := t.TempDir()
		store := NewFileSystemDataStore(dir)

		writer, pointer, err := store.CreateFile(ctx)
		if err != nil {
			t.Fatalf("CreateFile failed: %v", err)
		}
		if _, err := writer.Write([]byte("published file")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		if err := store.TombstoneFile(ctx, pointer); err != nil {
			t.Fatalf("TombstoneFile failed: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("failed to read dir: %v", err)
		}
		if len(entries) != 0 {
			t.Fatalf("expected empty directory after tombstone of published file, got %v", entries)
		}
	})
}
