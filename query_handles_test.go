package bloomsearch

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the query read path's per-query handle pool: a candidate file is
// opened once to read its block filter region and its block scans reuse that
// handle, so DataStore opens scale with candidate files rather than with data
// blocks.

// instrumentedDataStore wraps a DataStore and watches every handle the engine
// opens through it: how many were opened and closed, whether any handle was
// closed twice, and whether two goroutines ever used one at the same time
// (which the DataStore contract forbids). It also records every read's byte
// range per file, which is how tests assert where the read path goes and how
// many requests it takes to get there. readFault, when set, fails reads.
type instrumentedDataStore struct {
	inner DataStore

	opens         atomic.Int64
	closes        atomic.Int64
	reads         atomic.Int64
	doubleCloses  atomic.Int64
	concurrentUse atomic.Int64

	rangesMu sync.Mutex
	ranges   map[string][]readRange

	// readFault is consulted before each Read with the handle's file pointer
	// and the offset the read starts at; a non-nil result fails the read.
	readFault func(pointer []byte, offset int64) error
}

// readRange is one read request against a file: where it started and how many
// bytes it drew. A request is one Seek followed by an io.ReadFull — what an
// object store would charge as a range GET — so successive Read calls draining
// one ReadFull are folded into a single range (see instrumentedHandle.accumulate)
// rather than counted separately.
type readRange struct {
	offset int64
	length int
}

func (s *instrumentedDataStore) record(pointer []byte, offset int64, length int) {
	s.rangesMu.Lock()
	defer s.rangesMu.Unlock()
	if s.ranges == nil {
		s.ranges = make(map[string][]readRange)
	}
	s.ranges[string(pointer)] = append(s.ranges[string(pointer)], readRange{offset: offset, length: length})
}

// readsFor returns the reads recorded against one file, in no particular order
// across concurrent readers.
func (s *instrumentedDataStore) readsFor(pointer []byte) []readRange {
	s.rangesMu.Lock()
	defer s.rangesMu.Unlock()
	return slices.Clone(s.ranges[string(pointer)])
}

// resetReads forgets every recorded read, so a second query over the same store
// is measured on its own.
func (s *instrumentedDataStore) resetReads() {
	s.rangesMu.Lock()
	defer s.rangesMu.Unlock()
	s.ranges = nil
	s.reads.Store(0)
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

	// The read request currently being drained, closed out by the next Seek or by
	// Close. Counts are settled once the query's handle pool has closed every
	// handle, which happens before the cursor reports its terminal state.
	requestMu    sync.Mutex
	requestStart int64
	requestLen   int
	requestOpen  bool
}

// accumulate folds one Read into the request it belongs to: a Read starting where
// the previous one ended is the same io.ReadFull still being drained, which the
// OS is free to split, not a second request.
func (h *instrumentedHandle) accumulate(offset int64, n int) {
	h.requestMu.Lock()
	defer h.requestMu.Unlock()

	if h.requestOpen && h.requestStart+int64(h.requestLen) == offset {
		h.requestLen += n
		return
	}
	h.flushLocked()
	h.requestOpen, h.requestStart, h.requestLen = true, offset, n
}

func (h *instrumentedHandle) flushLocked() {
	if !h.requestOpen {
		return
	}
	h.store.record(h.pointer, h.requestStart, h.requestLen)
	h.store.reads.Add(1)
	h.requestOpen = false
}

func (h *instrumentedHandle) flush() {
	h.requestMu.Lock()
	defer h.requestMu.Unlock()
	h.flushLocked()
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
		h.accumulate(offset, n)
		h.pos.Store(offset + int64(n))
	}
	return n, err
}

func (h *instrumentedHandle) Seek(offset int64, whence int) (int64, error) {
	h.enter()
	defer h.exit()

	pos, err := h.inner.Seek(offset, whence)
	if err == nil {
		// A seek ends whatever request was being drained.
		h.flush()
		h.pos.Store(pos)
	}
	return pos, err
}

func (h *instrumentedHandle) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		h.store.doubleCloses.Add(1)
		return nil
	}
	h.flush()
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
	// one block scan follows the file's filter region read and reuses its handle:
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

// TestQueryFilterReadsPerFileNotPerBlock: a bloom-conditioned query consults
// every block of a candidate file with a single read, because the file's block
// filter sections are contiguous on disk and these files' regions fit inside one
// blockFilterChunkTarget chunk. The read count per file — and the query's total —
// must therefore not grow when the same files hold 16x the blocks, which is the
// whole point of the block filter region. Regions too large for one chunk are
// covered by TestQueryFilterRegionReadsAreChunked.
func TestQueryFilterReadsPerFileNotPerBlock(t *testing.T) {
	const files = 3
	const rowsPerBlock = 4

	blockCounts := []int{2, 8, 32}
	totalReads := make(map[int]int64, len(blockCounts))

	for _, blocksPerFile := range blockCounts {
		t.Run(fmt.Sprintf("blocks=%d", blocksPerFile), func(t *testing.T) {
			fsStore := buildMultiBlockFiles(t, t.TempDir(), files, blocksPerFile, rowsPerBlock)
			store := &instrumentedDataStore{inner: fsStore}
			engine := queryEngineOver(t, fsStore, store, 4)

			ctx := context.Background()
			maybeFiles, err := collectMaybeFiles(ctx, fsStore.GetMaybeFilesForQuery(ctx, nil))
			if err != nil {
				t.Fatalf("failed to list files: %v", err)
			}

			rows, res := drainQuery(t, engine, NewQuery().Token(blockTestMarkerToken).Build())
			defer res.Close()
			if err := res.Err(); err != nil {
				t.Fatalf("query error: %v", err)
			}
			if len(rows) != files*rowsPerBlock {
				t.Fatalf("delivered %d rows, want %d", len(rows), files*rowsPerBlock)
			}

			for _, file := range maybeFiles {
				if filterReads := regionReads(t, store, file); len(filterReads) != 1 {
					t.Fatalf("%d reads touched the block filter region of a %d-block file, want exactly 1",
						len(filterReads), len(file.Metadata.DataBlocks))
				}
			}

			if opens := store.opens.Load(); opens != files {
				t.Fatalf("opened %d handles for %d files, want %d", opens, files, files)
			}
			totalReads[blocksPerFile] = store.reads.Load()
			store.check(t)
		})
	}

	// One region read plus one row data read per file, whatever the block count.
	for _, blocksPerFile := range blockCounts {
		if got, want := totalReads[blocksPerFile], totalReads[blockCounts[0]]; got != want {
			t.Fatalf("a %d-block-per-file query issued %d reads, but a %d-block-per-file one issued %d: reads scale with blocks",
				blocksPerFile, got, blockCounts[0], want)
		}
	}
	if want := int64(2 * files); totalReads[blockCounts[0]] != want {
		t.Fatalf("query issued %d reads over %d files, want %d (one filter region read and one row data read each)",
			totalReads[blockCounts[0]], files, want)
	}
}

// regionReads returns the read requests the store recorded against a file that
// touched its block filter region.
func regionReads(t *testing.T, store *instrumentedDataStore, file MaybeFile) []readRange {
	t.Helper()

	if file.Metadata.BlockFilterRegionSize <= 0 {
		t.Fatalf("file has no block filter region: %+v", file.Metadata)
	}
	regionStart := int64(file.Metadata.BlockFilterRegionOffset)
	regionEnd := regionStart + int64(file.Metadata.BlockFilterRegionSize)

	var reads []readRange
	for _, read := range store.readsFor(file.PointerBytes) {
		if read.offset < regionEnd && read.offset+int64(read.length) > regionStart {
			reads = append(reads, read)
		}
	}
	return reads
}

const (
	// largeSectionCapacity sizes a hand-rolled block's filters so its section runs
	// about 1.5MiB: two fit in a blockFilterChunkTarget chunk and three do not.
	// The tests below assert that packing, so a drift in bloom sizing fails loudly
	// instead of quietly changing what they measure.
	largeSectionCapacity = 440_000
	// oversizedSectionCapacity sizes a single section larger than a whole chunk.
	oversizedSectionCapacity = 1_300_000
)

// buildLargeFilterRegionFile writes one file of blocks blocks whose filter
// sections are each sized for capacity entries — far more than the single row a
// block holds — so the file's block filter region spans several chunks without a
// corpus large enough to earn filters that big. Block i is alone in partition "i"
// and holds {"id": "p<i>", "part": "<i>"}, so a partition prefilter selects an
// exact set of blocks.
func buildLargeFilterRegionFile(t *testing.T, dir string, blocks int, capacity uint) *FileSystemDataStore {
	t.Helper()

	raw := make([]rawTestBlock, blocks)
	for i := range raw {
		partition := fmt.Sprintf("%d", i)
		raw[i] = rawTestBlock{
			partitionID:    partition,
			filterCapacity: capacity,
			rows:           []map[string]any{{"id": fmt.Sprintf("p%d", i), "part": partition}},
		}
	}
	writeRawTestFile(t, filepath.Join(dir, "largeregion.dat"), raw, true, nil)
	return NewFileSystemDataStore(dir)
}

// uniformSectionSize returns the size every block's filter section has, failing if
// they differ: the chunk-count expectations below are closed forms over equal,
// contiguous sections.
func uniformSectionSize(t *testing.T, metadata FileMetadata) int {
	t.Helper()

	size := metadata.DataBlocks[0].BloomFilterSize
	for i, block := range metadata.DataBlocks {
		if block.BloomFilterSize != size {
			t.Fatalf("block %d filter section is %d bytes but block 0 is %d: the expectations assume uniform sections",
				i, block.BloomFilterSize, size)
		}
	}
	return size
}

// singleFile returns the one file a fixture wrote.
func singleFile(t *testing.T, store MetaStore) MaybeFile {
	t.Helper()

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("expected 1 file, got %d", len(maybeFiles))
	}
	return maybeFiles[0]
}

// TestQueryFilterRegionReadsAreChunked: a block filter region too large to read
// in one go is read in chunks of at most blockFilterChunkTarget bytes — several
// sections per chunk, and never a chunk's worth of memory more than that. The cap
// is what keeps a file worker's transient memory bounded no matter how many
// blocks a file holds; batching within it is what keeps the request count far
// below one per block.
func TestQueryFilterRegionReadsAreChunked(t *testing.T) {
	const blocks = 6

	fsStore := buildLargeFilterRegionFile(t, t.TempDir(), blocks, largeSectionCapacity)
	file := singleFile(t, fsStore)
	sectionSize := uniformSectionSize(t, file.Metadata)

	// The fixture only measures anything if the region needs several chunks.
	perChunk := blockFilterChunkTarget / sectionSize
	if perChunk != 2 {
		t.Fatalf("%d-byte sections pack %d per %d-byte chunk, want 2 (retune largeSectionCapacity)",
			sectionSize, perChunk, blockFilterChunkTarget)
	}
	wantReads := (blocks + perChunk - 1) / perChunk

	store := &instrumentedDataStore{inner: fsStore}
	engine := queryEngineOver(t, fsStore, store, 4)

	// Field("part") is in every block, so every block is consulted and survives.
	rows, res := drainQuery(t, engine, NewQuery().Field("part").Build())
	defer res.Close()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != blocks {
		t.Fatalf("delivered %d rows, want %d", len(rows), blocks)
	}

	reads := regionReads(t, store, file)
	if len(reads) != wantReads {
		t.Fatalf("%d reads covered a %d-byte region of %d sections, want %d (%d sections per %d-byte chunk)",
			len(reads), file.Metadata.BlockFilterRegionSize, blocks, wantReads, perChunk, blockFilterChunkTarget)
	}
	for _, read := range reads {
		if read.length > blockFilterChunkTarget {
			t.Fatalf("a region read drew %d bytes, above the %d-byte chunk cap", read.length, blockFilterChunkTarget)
		}
	}
	if len(reads) >= blocks {
		t.Fatalf("%d reads for %d sections: chunks are not batching sections", len(reads), blocks)
	}
	store.check(t)
}

// TestQueryFilterRegionReadsSkipSparseGaps: when a prefilter leaves only a few
// candidate blocks scattered across a large region, the reads cover their sections
// and not the gaps between them. Each chunk starts at the section of the block
// being evaluated, so a gap wider than the cap is never paid for — which is why
// sparse coverage costs fewer reads than dense coverage of the same region.
func TestQueryFilterRegionReadsSkipSparseGaps(t *testing.T) {
	const blocks = 6

	fsStore := buildLargeFilterRegionFile(t, t.TempDir(), blocks, largeSectionCapacity)
	file := singleFile(t, fsStore)
	sectionSize := uniformSectionSize(t, file.Metadata)

	// Two candidates at opposite ends of the region, further apart than a chunk.
	sparseQuery := NewQuery().Field("part").
		MatchPrefilter(Partition(PartitionIn("0", fmt.Sprintf("%d", blocks-1)))).
		Build()

	store := &instrumentedDataStore{inner: fsStore}
	engine := queryEngineOver(t, fsStore, store, 4)
	rows, res := drainQuery(t, engine, sparseQuery)
	defer res.Close()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("delivered %d rows, want 2", len(rows))
	}

	sparse := regionReads(t, store, file)
	if len(sparse) != 2 {
		t.Fatalf("%d region reads for 2 sparse candidates, want 2 (one per candidate section)", len(sparse))
	}
	for _, read := range sparse {
		if read.length != sectionSize {
			t.Fatalf("a region read drew %d bytes for a %d-byte section: the gap between the sparse candidates was read too",
				read.length, sectionSize)
		}
	}
	store.check(t)

	// Dense coverage of the same region takes more reads, which is the comparison
	// that makes the point.
	denseStore := &instrumentedDataStore{inner: fsStore}
	denseEngine := queryEngineOver(t, fsStore, denseStore, 4)
	_, denseRes := drainQuery(t, denseEngine, NewQuery().Field("part").Build())
	defer denseRes.Close()
	if err := denseRes.Err(); err != nil {
		t.Fatalf("dense query error: %v", err)
	}
	if dense := regionReads(t, denseStore, file); len(dense) <= len(sparse) {
		t.Fatalf("dense coverage took %d region reads and sparse took %d: sparse candidates are not saving requests",
			len(dense), len(sparse))
	}
	denseStore.check(t)
}

// TestQueryOversizedFilterSectionIsRead: a single filter section larger than the
// chunk cap is read on its own, in full. The cap bounds how much slack a read may
// carry, never whether a block can be consulted — a block whose filters do not fit
// a chunk must still be evaluated rather than failed.
func TestQueryOversizedFilterSectionIsRead(t *testing.T) {
	dir := t.TempDir()
	writeRawTestFile(t, filepath.Join(dir, "oversized.dat"), []rawTestBlock{
		{partitionID: "big", filterCapacity: oversizedSectionCapacity,
			rows: []map[string]any{{"id": "bigrow", "part": "big"}}},
		{partitionID: "small", rows: []map[string]any{{"id": "smallrow", "part": "small"}}},
	}, true, nil)

	fsStore := NewFileSystemDataStore(dir)
	file := singleFile(t, fsStore)

	var big DataBlockMetadata
	for _, block := range file.Metadata.DataBlocks {
		if block.PartitionID == "big" {
			big = block
		}
	}
	if big.BloomFilterSize <= blockFilterChunkTarget {
		t.Fatalf("the oversized block's section is %d bytes, not above the %d-byte cap (retune oversizedSectionCapacity)",
			big.BloomFilterSize, blockFilterChunkTarget)
	}

	store := &instrumentedDataStore{inner: fsStore}
	engine := queryEngineOver(t, fsStore, store, 4)

	rows, res := drainQuery(t, engine, NewQuery().Token("bigrow").Build())
	defer res.Close()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
	if ids := rowIDs(rows); ids["bigrow"] != 1 || len(ids) != 1 {
		t.Fatalf("row behind an oversized filter section not found exactly once: %v", ids)
	}

	oversized := 0
	for _, read := range regionReads(t, store, file) {
		if read.offset != int64(big.BloomFilterOffset) {
			continue
		}
		oversized++
		if read.length != big.BloomFilterSize {
			t.Fatalf("the oversized section was read as %d bytes, want its full %d", read.length, big.BloomFilterSize)
		}
	}
	if oversized != 1 {
		t.Fatalf("%d reads started at the oversized section, want exactly 1", oversized)
	}
	store.check(t)
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
			slices.SortFunc(descending, func(a, b DataBlockMetadata) int { return cmp.Compare(b.RowDataOffset, a.RowDataOffset) })
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
// of row data order still gets the right block scanned — the file worker orders
// the blocks so its row data reads move forward through the file, and it orders a
// copy, leaving the store's slice untouched.
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
		if !slices.IsSortedFunc(blocks, func(a, b DataBlockMetadata) int { return cmp.Compare(b.RowDataOffset, a.RowDataOffset) }) {
			t.Fatalf("file %d: the engine reordered the MetaStore's own block slice", i)
		}
	}
	store.check(t)
}

// errInjectedFilterFault is the sentinel the filter-read fault injects.
var errInjectedFilterFault = errors.New("injected filter read fault")

// TestQueryFilterReadFailureIsolatesFile: when one file's block filter region
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

	// Fail only the read of the poisoned file's block filter region, which starts
	// at the region offset when every block is a candidate. Row data lives before
	// the region, so the scans of the other files' blocks are untouched.
	regionOffset := int64(poisoned.Metadata.BlockFilterRegionOffset)
	store := &instrumentedDataStore{
		inner: fsStore,
		readFault: func(pointer []byte, offset int64) error {
			if string(pointer) == string(poisoned.PointerBytes) && offset == regionOffset {
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
			key := blockKey{file: string(file.PointerBytes), offset: block.RowDataOffset}
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
