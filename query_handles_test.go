package bloomsearch

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the query read path's per-query handle pool: a candidate file is
// opened for its block filter pass and its block scans reuse that handle, so
// DataStore opens scale with candidate files rather than with data blocks.

// instrumentedDataStore wraps a DataStore and watches every handle the engine
// opens through it: how many were opened and closed, whether any handle was
// closed twice, and whether two goroutines ever used one at the same time
// (which the DataStore contract forbids). readFault, when set, fails reads.
type instrumentedDataStore struct {
	inner DataStore

	opens         atomic.Int64
	closes        atomic.Int64
	doubleCloses  atomic.Int64
	concurrentUse atomic.Int64

	// readFault is consulted before each Read with the handle's file pointer
	// and the offset the read starts at; a non-nil result fails the read.
	readFault func(pointer []byte, offset int64) error
}

func (s *instrumentedDataStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	return s.inner.CreateFile(ctx)
}

func (s *instrumentedDataStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	return s.inner.TombstoneFile(ctx, filePointerBytes)
}

func (s *instrumentedDataStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	inner, err := s.inner.OpenFile(ctx, filePointerBytes)
	if err != nil {
		return nil, err
	}
	s.opens.Add(1)
	return &instrumentedHandle{store: s, inner: inner, pointer: filePointerBytes}, nil
}

type instrumentedHandle struct {
	store   *instrumentedDataStore
	inner   io.ReadSeekCloser
	pointer []byte

	inUse  atomic.Bool
	closed atomic.Bool
	pos    atomic.Int64
}

// enter and exit bracket every use of the handle. A failed claim means another
// goroutine is inside: the handle is being shared, which no pooled handle ever
// may be.
func (h *instrumentedHandle) enter() {
	if !h.inUse.CompareAndSwap(false, true) {
		h.store.concurrentUse.Add(1)
	}
	// Widen the window a sharing bug would have to hit.
	runtime.Gosched()
}

func (h *instrumentedHandle) exit() {
	h.inUse.Store(false)
}

func (h *instrumentedHandle) Read(p []byte) (int, error) {
	h.enter()
	defer h.exit()

	offset := h.pos.Load()
	if h.store.readFault != nil {
		if err := h.store.readFault(h.pointer, offset); err != nil {
			return 0, err
		}
	}
	n, err := h.inner.Read(p)
	if n > 0 {
		h.pos.Store(offset + int64(n))
	}
	return n, err
}

func (h *instrumentedHandle) Seek(offset int64, whence int) (int64, error) {
	h.enter()
	defer h.exit()

	pos, err := h.inner.Seek(offset, whence)
	if err == nil {
		h.pos.Store(pos)
	}
	return pos, err
}

func (h *instrumentedHandle) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		h.store.doubleCloses.Add(1)
		return nil
	}
	h.store.closes.Add(1)
	return h.inner.Close()
}

// check fails the test on any contract violation the store observed.
func (s *instrumentedDataStore) check(t *testing.T) {
	t.Helper()
	if n := s.concurrentUse.Load(); n != 0 {
		t.Errorf("%d concurrent uses of a single handle (handles are exclusively checked out)", n)
	}
	if n := s.doubleCloses.Load(); n != 0 {
		t.Errorf("%d handles closed more than once", n)
	}
}

const (
	blockTestShardField  = "shard"
	blockTestMarkerToken = "needlemarkertoken"
)

// buildMultiBlockFiles writes files x blocksPerFile data blocks into dir, one
// block per shard per flush, with rowsPerBlock rows in each. Every file's shard
// 0 block — and only that block — carries blockTestMarkerToken, so a query for
// the token passes every file's file-level filter and survives exactly one
// block filter per file: the pruning-dominated shape a real deployment has.
//
// The block layout is verified against the MetaStore before returning, so a
// test that depends on it fails here rather than misreporting later.
func buildMultiBlockFiles(t *testing.T, dir string, files, blocksPerFile, rowsPerBlock int) *FileSystemDataStore {
	t.Helper()

	store := NewFileSystemDataStore(dir)
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.MaxBufferedRows = blocksPerFile * rowsPerBlock * 2
	config.BloomFalsePositiveRate = 1e-9 // block-level false positives must not decide these tests
	config.PartitionFunc = func(row map[string]any) string {
		shard, _ := row[blockTestShardField].(string)
		return shard
	}
	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("failed to create build engine: %v", err)
	}
	engine.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := engine.Stop(ctx); err != nil {
			t.Fatalf("failed to stop build engine: %v", err)
		}
	}()

	for f := 0; f < files; f++ {
		rows := make([]map[string]any, 0, blocksPerFile*rowsPerBlock)
		for shard := 0; shard < blocksPerFile; shard++ {
			for i := 0; i < rowsPerBlock; i++ {
				row := map[string]any{
					"id":                fmt.Sprintf("f%d-s%d-r%d", f, shard, i),
					blockTestShardField: fmt.Sprintf("%d", shard),
					"message":           fmt.Sprintf("filler row %d", i),
				}
				if shard == 0 {
					row["message"] = blockTestMarkerToken
				}
				rows = append(rows, row)
			}
		}
		ingestAndFlush(t, engine, rows)
	}

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list built files: %v", err)
	}
	if len(maybeFiles) != files {
		t.Fatalf("built %d files, want %d", len(maybeFiles), files)
	}
	for _, file := range maybeFiles {
		if len(file.Metadata.DataBlocks) != blocksPerFile {
			t.Fatalf("built a file with %d data blocks, want %d", len(file.Metadata.DataBlocks), blocksPerFile)
		}
	}
	return store
}

// queryEngineOver builds a query-only engine reading dir through dataStore,
// with fsStore serving metadata so metadata reads are not instrumented.
func queryEngineOver(t *testing.T, fsStore *FileSystemDataStore, dataStore DataStore, concurrency int) *BloomSearchEngine {
	t.Helper()

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.MaxQueryConcurrency = concurrency
	engine, err := NewBloomSearchEngine(config, fsStore, dataStore)
	if err != nil {
		t.Fatalf("failed to create query engine: %v", err)
	}
	// Queries need no ingest workers, so the engine is never started: the read
	// path touches only the MetaStore and DataStore.
	return engine
}

// drainQuery runs a query to completion and returns the rows it delivered.
func drainQuery(t *testing.T, engine *BloomSearchEngine, query *Query) ([]map[string]any, *Results) {
	t.Helper()

	res, err := engine.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	var rows []map[string]any
	for res.Next() {
		rows = append(rows, res.Row())
	}
	return rows, res
}

// TestQueryOpensPerFileNotPerBlock: a query over multi-block files opens one
// handle per candidate file, not one per data block, and the count is
// unchanged when the same files hold twice as many blocks.
func TestQueryOpensPerFileNotPerBlock(t *testing.T) {
	const files = 3
	const rowsPerBlock = 4

	// The needle query survives exactly one block filter per file, so exactly
	// one block scan follows the file's filter pass and reuses its handle:
	// one open per file, whatever the concurrency budget.
	for _, blocksPerFile := range []int{4, 8} {
		for _, concurrency := range []int{1, 4, 1000} {
			name := fmt.Sprintf("blocks=%d/concurrency=%d", blocksPerFile, concurrency)
			t.Run(name, func(t *testing.T) {
				fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
				store := &instrumentedDataStore{inner: fsStore}
				engine := queryEngineOver(t, fsStore, store, concurrency)

				rows, res := drainQuery(t, engine, NewQuery().Token(blockTestMarkerToken).Build())
				defer res.Close()
				if err := res.Err(); err != nil {
					t.Fatalf("query error: %v", err)
				}
				if len(rows) != files*rowsPerBlock {
					t.Fatalf("delivered %d rows, want %d", len(rows), files*rowsPerBlock)
				}

				blocks := files * blocksPerFile
				if opens := store.opens.Load(); opens != files {
					t.Fatalf("opened %d handles for %d files / %d blocks, want %d", opens, files, blocks, files)
				}
				stats := res.Stats()
				if len(stats.BlockStats) != blocks {
					t.Fatalf("evaluated %d blocks, want %d", len(stats.BlockStats), blocks)
				}
				store.check(t)
			})
		}
	}

	// A query where every block survives its filters scans blocks concurrently,
	// so a file can have several handles out at once — but only as many as the
	// concurrency budget allows, because a handle is only ever checked out by a
	// worker holding a query-semaphore slot. Opens therefore stay bounded by
	// files x concurrency however many blocks the files hold, and land at
	// exactly one per file when only one reader may run at a time.
	for _, concurrency := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("all-blocks-scanned/concurrency=%d", concurrency), func(t *testing.T) {
			// More surviving blocks per file than the block-job channel buffers,
			// so the dispatch blocks and the pipeline has to make progress with
			// a file worker parked on a send.
			const blocksPerFile = queryJobBuffer + 4
			fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
			store := &instrumentedDataStore{inner: fsStore}
			engine := queryEngineOver(t, fsStore, store, concurrency)

			// Field("shard") is present in every block, so nothing is pruned
			// and every block is scanned.
			rows, res := drainQuery(t, engine, NewQuery().Field(blockTestShardField).Build())
			defer res.Close()
			if err := res.Err(); err != nil {
				t.Fatalf("query error: %v", err)
			}
			if want := files * blocksPerFile * rowsPerBlock; len(rows) != want {
				t.Fatalf("delivered %d rows, want %d", len(rows), want)
			}

			blocks := files * blocksPerFile
			opens := store.opens.Load()
			if opens < files {
				t.Fatalf("opened %d handles for %d files, want at least one per file", opens, files)
			}
			if concurrency == 1 && opens != files {
				t.Fatalf("opened %d handles with a serial budget, want %d (one per file)", opens, files)
			}
			if limit := int64(files * concurrency); opens > limit {
				t.Fatalf("opened %d handles for %d files at concurrency %d, want at most %d", opens, files, concurrency, limit)
			}
			// The bound above is what makes the point: it is far below one open
			// per block.
			if opens >= int64(blocks) {
				t.Fatalf("opened %d handles for %d blocks: handles are not being reused", opens, blocks)
			}
			store.check(t)
		})
	}
}

// TestQueryHandleExclusivity: no pooled handle is ever read or seeked by two
// goroutines at once, with both the filter passes and the block scans running
// at full concurrency over multiple multi-block files. Meaningful under -race.
func TestQueryHandleExclusivity(t *testing.T) {
	const files = 4
	const blocksPerFile = 6
	const rowsPerBlock = 8

	fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
	store := &instrumentedDataStore{inner: fsStore}
	engine := queryEngineOver(t, fsStore, store, 16)

	// Every block survives, so file workers and block workers contend for the
	// same files' handles throughout.
	rows, res := drainQuery(t, engine, NewQuery().Field(blockTestShardField).Build())
	defer res.Close()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if want := files * blocksPerFile * rowsPerBlock; len(rows) != want {
		t.Fatalf("delivered %d rows, want %d", len(rows), want)
	}
	if store.opens.Load() == 0 {
		t.Fatal("no handles were opened (test proves nothing)")
	}
	store.check(t)
}

// TestQueryClosesEveryHandle: every handle a query opens is closed exactly
// once — on clean completion, after Close mid-iteration, and after the Query
// context is canceled. The query's teardown closes the pool before the cursor
// reports its terminal state, so the counts are settled once Next has returned
// false (or Close has returned).
func TestQueryClosesEveryHandle(t *testing.T) {
	const files = 3
	const blocksPerFile = 4
	const rowsPerBlock = 40

	newQuery := func(t *testing.T) (*instrumentedDataStore, *BloomSearchEngine) {
		fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
		store := &instrumentedDataStore{inner: fsStore}
		return store, queryEngineOver(t, fsStore, store, 4)
	}

	assertBalanced := func(t *testing.T, store *instrumentedDataStore) {
		t.Helper()
		opens, closes := store.opens.Load(), store.closes.Load()
		if opens == 0 {
			t.Fatal("no handles were opened (test proves nothing)")
		}
		if closes != opens {
			t.Fatalf("opened %d handles, closed %d", opens, closes)
		}
		store.check(t)
	}

	t.Run("clean completion", func(t *testing.T) {
		store, engine := newQuery(t)
		rows, res := drainQuery(t, engine, NewQuery().Field(blockTestShardField).Build())
		if err := res.Err(); err != nil {
			t.Fatalf("query error: %v", err)
		}
		if want := files * blocksPerFile * rowsPerBlock; len(rows) != want {
			t.Fatalf("delivered %d rows, want %d", len(rows), want)
		}
		assertBalanced(t, store)
		res.Close()
		assertBalanced(t, store)
	})

	t.Run("close mid-iteration", func(t *testing.T) {
		store, engine := newQuery(t)
		res, err := engine.Query(context.Background(), NewQuery().Field(blockTestShardField).Build())
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if !res.Next() {
			t.Fatalf("expected at least one row before Close: %v", res.Err())
		}
		if err := res.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		assertBalanced(t, store)
	})

	t.Run("context cancellation", func(t *testing.T) {
		store, engine := newQuery(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		res, err := engine.Query(ctx, NewQuery().Field(blockTestShardField).Build())
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer res.Close()
		if !res.Next() {
			t.Fatalf("expected at least one row before cancellation: %v", res.Err())
		}
		cancel()
		// The terminal Next waits for the pipeline to wind down, which is when
		// the pool closes.
		for res.Next() {
		}
		if err := res.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected a canceled query, got %v", err)
		}
		assertBalanced(t, store)
	})
}

// descendingBlockMetaStore yields the wrapped store's files with each file's
// data blocks in descending offset order, and keeps the slices it yielded so a
// test can assert the engine ordered its own copy instead of sorting the
// store's.
type descendingBlockMetaStore struct {
	inner MetaStore

	mu     sync.Mutex
	yields [][]DataBlockMetadata
}

func (s *descendingBlockMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		for file, err := range s.inner.GetMaybeFilesForQuery(ctx, query) {
			if err != nil {
				yield(MaybeFile{}, err)
				return
			}
			descending := make([]DataBlockMetadata, len(file.Metadata.DataBlocks))
			for i, block := range file.Metadata.DataBlocks {
				descending[len(descending)-1-i] = block
			}
			slices.SortFunc(descending, func(a, b DataBlockMetadata) int { return cmp.Compare(b.Offset, a.Offset) })
			file.Metadata.DataBlocks = descending

			s.mu.Lock()
			s.yields = append(s.yields, descending)
			s.mu.Unlock()

			if !yield(file, nil) {
				return
			}
		}
	}
}

func (s *descendingBlockMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return s.inner.Update(ctx, writes, deletes)
}

// TestQueryFilterPassOrdersBlocksItself: a MetaStore that yields data blocks out
// of offset order still gets the right block scanned — the filter pass orders
// the blocks for its forward read pass, and it orders a copy, leaving the
// store's slice untouched.
func TestQueryFilterPassOrdersBlocksItself(t *testing.T) {
	const files = 2
	const blocksPerFile = 5
	const rowsPerBlock = 3

	fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
	metaStore := &descendingBlockMetaStore{inner: fsStore}
	store := &instrumentedDataStore{inner: fsStore}

	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	engine, err := NewBloomSearchEngine(config, metaStore, store)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	rows, res := drainQuery(t, engine, NewQuery().Token(blockTestMarkerToken).Build())
	defer res.Close()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if want := files * rowsPerBlock; len(rows) != want {
		t.Fatalf("delivered %d rows, want %d", len(rows), want)
	}

	// The marker block is the one holding the rows, whatever order the blocks
	// arrived in, and the rest were pruned.
	stats := res.Stats()
	if stats.BlocksProcessed != files || stats.BlocksSkipped != files*(blocksPerFile-1) {
		t.Fatalf("aggregates: %d processed / %d skipped, want %d / %d",
			stats.BlocksProcessed, stats.BlocksSkipped, files, files*(blocksPerFile-1))
	}
	if opens := store.opens.Load(); opens != files {
		t.Fatalf("opened %d handles, want %d (one per file)", opens, files)
	}

	metaStore.mu.Lock()
	defer metaStore.mu.Unlock()
	if len(metaStore.yields) != files {
		t.Fatalf("store yielded %d files, want %d", len(metaStore.yields), files)
	}
	for i, blocks := range metaStore.yields {
		if !slices.IsSortedFunc(blocks, func(a, b DataBlockMetadata) int { return cmp.Compare(b.Offset, a.Offset) }) {
			t.Fatalf("file %d: the engine reordered the MetaStore's own block slice", i)
		}
	}
	store.check(t)
}

// errInjectedFilterFault is the sentinel the filter-read fault injects.
var errInjectedFilterFault = errors.New("injected filter read fault")

// TestQueryFilterReadFailureIsolatesFile: when one file's block filter section
// cannot be read, that file is abandoned with a single recorded error while the
// other files' rows are delivered in full, and the abandoned file's blocks
// still each contribute one BlockStats entry.
func TestQueryFilterReadFailureIsolatesFile(t *testing.T) {
	const files = 3
	const blocksPerFile = 4
	const rowsPerBlock = 5

	dir := t.TempDir()
	fsStore := buildMultiBlockFiles(t, dir, files, blocksPerFile, rowsPerBlock)

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, fsStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	poisoned := maybeFiles[0]

	// Fail only reads that start at a block offset, which is where a filter
	// section begins; row data starts past the section, so scans are untouched.
	filterOffsets := make(map[int64]bool, len(poisoned.Metadata.DataBlocks))
	for _, block := range poisoned.Metadata.DataBlocks {
		filterOffsets[int64(block.Offset)] = true
	}
	store := &instrumentedDataStore{
		inner: fsStore,
		readFault: func(pointer []byte, offset int64) error {
			if string(pointer) == string(poisoned.PointerBytes) && filterOffsets[offset] {
				return errInjectedFilterFault
			}
			return nil
		},
	}
	engine := queryEngineOver(t, fsStore, store, 4)

	// A bloom-conditioned query, so the filter sections are read at all. The
	// marker block is one block per file.
	rows, res := drainQuery(t, engine, NewQuery().Token(blockTestMarkerToken).Build())
	defer res.Close()

	// Every healthy file still delivered its matching block in full.
	if want := (files - 1) * rowsPerBlock; len(rows) != want {
		t.Fatalf("delivered %d rows from the healthy files, want %d", len(rows), want)
	}

	err = res.Err()
	if !errors.Is(err, errInjectedFilterFault) {
		t.Fatalf("expected Err to wrap the injected fault, got %v", err)
	}
	// One error for the file, not one per block: the failure is the file's.
	if got := strings.Count(err.Error(), errInjectedFilterFault.Error()); got != 1 {
		t.Fatalf("expected 1 recorded filter fault, got %d: %v", got, err)
	}

	stats := res.Stats()
	if len(stats.BlockStats) != files*blocksPerFile {
		t.Fatalf("recorded %d block stats, want %d (one per candidate block)", len(stats.BlockStats), files*blocksPerFile)
	}
	for _, block := range stats.BlockStats {
		if string(block.FilePointer) != string(poisoned.PointerBytes) {
			continue
		}
		if block.BloomFilterSkipped {
			t.Fatalf("unreadable file's block reported as bloom-pruned: %+v", block)
		}
		if block.RowsProcessed != 0 || block.BytesProcessed != 0 {
			t.Fatalf("unreadable file's block reported scanned rows: %+v", block)
		}
		if block.TotalRows != int64(rowsPerBlock) {
			t.Fatalf("unreadable file's block lost its totals: %+v", block)
		}
	}
	store.check(t)
}

// TestQueryBlockStatsParityUnderFilterPruning: the exact set of BlockStats a
// multi-block query produces — one entry per candidate block, carrying the
// block's metadata totals, with BloomFilterSkipped set on exactly the blocks
// the filters pruned. This is the regression net for block pruning happening in
// the file stage rather than in the block scan.
func TestQueryBlockStatsParityUnderFilterPruning(t *testing.T) {
	const files = 2
	const blocksPerFile = 4
	const rowsPerBlock = 6

	fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
	store := &instrumentedDataStore{inner: fsStore}
	engine := queryEngineOver(t, fsStore, store, 8)

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, fsStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}

	// The expected outcome per candidate block, straight from the metadata: the
	// marker token lives in shard 0's block of each file, so that block is
	// scanned and every other block is pruned by its filters.
	type blockKey struct {
		file   string
		offset int
	}
	wantTotals := make(map[blockKey]int64)
	wantScanned := make(map[blockKey]bool)
	for _, file := range maybeFiles {
		for _, block := range file.Metadata.DataBlocks {
			key := blockKey{file: string(file.PointerBytes), offset: block.Offset}
			wantTotals[key] = int64(block.Rows)
			if block.PartitionID == "0" {
				wantScanned[key] = true
			}
		}
	}
	if len(wantTotals) != files*blocksPerFile {
		t.Fatalf("expected %d candidate blocks in metadata, got %d", files*blocksPerFile, len(wantTotals))
	}
	if len(wantScanned) != files {
		t.Fatalf("expected %d marker-carrying blocks in metadata, got %d", files, len(wantScanned))
	}

	rows, res := drainQuery(t, engine, NewQuery().Token(blockTestMarkerToken).Build())
	defer res.Close()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if want := files * rowsPerBlock; len(rows) != want {
		t.Fatalf("delivered %d rows, want %d", len(rows), want)
	}

	stats := res.Stats()
	seen := make(map[blockKey]int)
	for _, block := range stats.BlockStats {
		key := blockKey{file: string(block.FilePointer), offset: block.BlockOffset}
		seen[key]++
		total, ok := wantTotals[key]
		if !ok {
			t.Fatalf("stats for an unknown block: %+v", block)
		}
		if block.TotalRows != total {
			t.Fatalf("block %+v reported TotalRows %d, want %d", key, block.TotalRows, total)
		}
		if block.TotalBytes <= 0 {
			t.Fatalf("block %+v reported no total bytes: %+v", key, block)
		}
		if block.Duration <= 0 {
			t.Fatalf("block %+v reported no duration: %+v", key, block)
		}
		if wantScanned[key] {
			if block.BloomFilterSkipped {
				t.Fatalf("block %+v holding the marker was pruned", key)
			}
			if block.RowsProcessed != total || block.BytesProcessed <= 0 {
				t.Fatalf("scanned block %+v reported %d/%d rows/bytes", key, block.RowsProcessed, block.BytesProcessed)
			}
			continue
		}
		if !block.BloomFilterSkipped {
			t.Fatalf("block %+v without the marker was not pruned: %+v", key, block)
		}
		if block.RowsProcessed != 0 || block.BytesProcessed != 0 {
			t.Fatalf("pruned block %+v reported scanned rows/bytes: %+v", key, block)
		}
	}

	if len(seen) != len(wantTotals) {
		t.Fatalf("stats covered %d distinct blocks, want %d", len(seen), len(wantTotals))
	}
	for key, count := range seen {
		if count != 1 {
			t.Fatalf("block %+v produced %d stats entries, want exactly 1", key, count)
		}
	}
	if stats.BlocksProcessed != files || stats.BlocksSkipped != files*(blocksPerFile-1) {
		t.Fatalf("aggregates: %d processed / %d skipped, want %d / %d",
			stats.BlocksProcessed, stats.BlocksSkipped, files, files*(blocksPerFile-1))
	}
	if stats.RowsScanned != int64(files*rowsPerBlock) {
		t.Fatalf("RowsScanned %d, want %d", stats.RowsScanned, files*rowsPerBlock)
	}
	store.check(t)
}
