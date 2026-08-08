package bloomsearch

// Query execution: file/block bloom evaluation and the block scan workers
// behind the Results cursor (query_results.go).

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// dataBlockJob is a scan job for one data block whose filters did not
// disqualify it (see evaluateBlockFilters). filterDuration is the time the file
// stage spent obtaining and evaluating this block's filter section, carried
// along so the block's BlockStats.Duration still accounts for the whole block.
type dataBlockJob struct {
	filePointer    []byte
	blockMetadata  DataBlockMetadata
	filterDuration time.Duration
}

// fileFilterJob is one candidate file's block filter evaluation: the file
// passed the prefilter and the file-level bloom test, and its blocks' filter
// sections have yet to be read. The file's block filter region bounds come
// along so the filter pass can reject block metadata pointing outside it
// instead of reading (or allocating for) an arbitrary span.
type fileFilterJob struct {
	filePointer        []byte
	filterRegionOffset int
	filterRegionSize   int
	blocks             []DataBlockMetadata
}

// blockScanCandidate is one block that survived filter evaluation, as an index
// into the evaluated block slice. The survivors are carried as indexes rather
// than as copied metadata so a file worker's own state stays a small multiple of
// the file's block count, on top of the block metadata the MetaStore yielded.
type blockScanCandidate struct {
	index          int
	filterDuration time.Duration
}

// BlockStats describes one candidate data block of a query. BlockOffset
// identifies the block within its file by its row data offset. RowsProcessed and
// BytesProcessed report what the scan actually read — rows and uncompressed
// row bytes (length prefix included) — so a bloom-skipped block reports zero.
// TotalRows and TotalBytes are the block's metadata totals (TotalBytes is the
// block's full on-disk footprint, its filter section included).
//
// Duration is the time the block cost the query, which is where its work
// happened: its share of the file's block filter region read plus its own
// filter evaluation for a block pruned by its filters, and that plus the row
// data read and scan for a block that was scanned. The region read is shared —
// one read covers every candidate block of the file — so it is amortized evenly
// across them rather than charged to whichever block was evaluated first.
// Duration excludes time the block spent queued between the two stages.
type BlockStats struct {
	FilePointer        []byte
	BlockOffset        int
	RowsProcessed      int64
	BytesProcessed     int64
	TotalRows          int64
	TotalBytes         int64
	Duration           time.Duration
	BloomFilterSkipped bool
}

// evaluateBloomFilters tests if bloom filters match the bloom query
func (b *BloomSearchEngine) evaluateBloomFilters(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	bloomQuery *BloomQuery,
) bool {
	if bloomQuery == nil || bloomQuery.Expression == nil {
		return true // No bloom filtering needed, since it's only used to DISQUALIFY files
	}

	return b.evaluateBloomExpression(fieldFilter, tokenFilter, fieldTokenFilter, bloomQuery.Expression)
}

// evaluateBloomExpression tests if bloom filters match a bloom expression tree
func (b *BloomSearchEngine) evaluateBloomExpression(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	expression *BloomExpression,
) bool {
	if expression == nil {
		return true
	}

	switch expression.ExpressionType {
	case BloomExpressionCondition:
		if expression.Condition == nil {
			return true
		}
		return b.evaluateBloomCondition(fieldFilter, tokenFilter, fieldTokenFilter, expression.Condition)
	case BloomExpressionOr:
		if len(expression.Children) == 0 {
			return false
		}
		for i := range expression.Children {
			if b.evaluateBloomExpression(fieldFilter, tokenFilter, fieldTokenFilter, &expression.Children[i]) {
				return true
			}
		}
		return false
	case BloomExpressionAnd:
		for i := range expression.Children {
			if !b.evaluateBloomExpression(fieldFilter, tokenFilter, fieldTokenFilter, &expression.Children[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// evaluateBloomCondition tests if bloom filters match a single bloom condition
func (b *BloomSearchEngine) evaluateBloomCondition(
	fieldFilter *bloom.BloomFilter,
	tokenFilter *bloom.BloomFilter,
	fieldTokenFilter *bloom.BloomFilter,
	condition *BloomCondition,
) (result bool) {
	switch condition.Type {
	case BloomField:
		// If the filter is nil (e.g., not present in metadata), we cannot disqualify
		if fieldFilter == nil {
			b.logger.Warn("field bloom filter missing; cannot disqualify", "condition", condition.Field)
			return true
		}
		result = fieldFilter.TestString(condition.Field)
	case BloomToken:
		if tokenFilter == nil {
			b.logger.Warn("token bloom filter missing; cannot disqualify", "condition", condition.Token)
			return true
		}
		result = tokenFilter.TestString(condition.Token)
	case BloomFieldToken:
		if fieldTokenFilter == nil {
			b.logger.Warn("field:token bloom filter missing; cannot disqualify", "field", condition.Field, "token", condition.Token)
			return true
		}
		key := makeFieldTokenKey(condition.Field, condition.Token)
		result = fieldTokenFilter.TestString(key)
	default:
		result = false // We don't know what this is, so it's invalid
	}
	return
}

const (
	// queryJobBuffer is the block-job channel's buffer. It only smooths the
	// hand-off between the file workers and the block workers.
	queryJobBuffer = 16

	// queryFileJobBuffer is the file-job channel's buffer. It stays small
	// because each file job carries a whole file's block metadata: the
	// candidate files in flight anywhere in a query are bounded by the worker
	// counts plus these constants, never by the candidate count.
	queryFileJobBuffer = 4
)

// Query starts a query and returns a Results cursor streaming the matching
// rows. Only pre-iteration setup (regex compilation) can fail fast with
// (nil, err); everything else — the MetaStore iteration, prefilter
// enforcement, file-level bloom pruning, and block scans — runs behind the
// cursor, which the engine fully owns (see Results for the iteration, error,
// cancellation, and stats contracts).
//
// Candidate files stream from the MetaStore iterator through a bounded,
// three-stage pipeline, so per-query memory scales with the in-flight window
// (worker counts plus constant channel buffers), not with the candidate-file
// count:
//
//   - the file stage pulls one candidate at a time, enforces the prefilter, and
//     applies the in-memory file-level bloom test;
//   - a file worker evaluates that file's data block filters, reading every
//     candidate block's section in a single read of the file's block filter
//     region (see evaluateBlockFilters), and dispatches only the blocks that
//     survive;
//   - block workers scan those blocks concurrently, reusing the file's pooled
//     handles rather than opening one per block (see fileHandlePool).
//
// A MetaStore error mid-iteration stops further pulls but lets already-
// dispatched work finish and deliver its rows; the error surfaces from
// Results.Err joined with any block errors — the same partial-results
// philosophy as block errors.
//
// Queries are independent of the ingest lifecycle: a stopped engine still
// serves queries, because reads touch only the MetaStore and DataStore.
func (b *BloomSearchEngine) Query(ctx context.Context, query *Query) (*Results, error) {
	if query == nil {
		query = NewQuery().Build()
	}

	rowBloomQuery := query.Bloom
	if rowBloomQuery == nil {
		rowBloomQuery = &BloomQuery{}
	}

	compiledRegexQuery, err := compileRegexQuery(query.Regex)
	if err != nil {
		return nil, fmt.Errorf("failed to compile regex query: %w", err)
	}

	// Row verification is compiled once per query: pre-split paths, verbatim
	// target tokens, and a single-walk evaluator (see compiledRowMatcher).
	rowMatcher := compileRowMatcher(rowBloomQuery, compiledRegexQuery, ".", b.config.Tokenizer)

	pruneBloomQuery := AndBloomQueries(rowBloomQuery, RegexFieldGuardBloomQuery(query.Regex))

	// A query without bloom conditions cannot be disqualified by any filter:
	// skip filter evaluation entirely, and downstream, skip reading the block
	// filter sections (see evaluateBlockFilters).
	hasBloomConditions := pruneBloomQuery != nil && pruneBloomQuery.Expression != nil

	r := newResults(ctx)

	// One handle pool per query: each candidate file is opened once for its
	// block filter region read, and its block scans borrow that handle back
	// instead of opening their own. The pool is closed on teardown, after every
	// worker has exited.
	handles := newFileHandlePool(b.dataStore)

	fileJobs := make(chan fileFilterJob, queryFileJobBuffer)
	blockJobs := make(chan dataBlockJob, queryJobBuffer)

	// Workers are spawned on demand, one per job sent, until
	// MaxQueryConcurrency of each kind are running: the job total is unknown up
	// front (files stream from the MetaStore), and spawning against demand
	// yields the same min(jobs, MaxQueryConcurrency) worker count upfront
	// sizing would — a fully pruned query spawns no block workers. Each worker
	// also watches the query context so cancellation winds workers down
	// promptly even while the file stage is still draining the store iterator.
	var fileWorkers, blockWorkers sync.WaitGroup

	blockWorker := func() {
		defer blockWorkers.Done()

		slot := querySlot{sem: b.querySemaphore, ctx: r.ctx}
		defer slot.release()
		var scratch *rowMatchScratch

		// runJob releases the file reference its dispatcher retained on every
		// path — scanned, failed, or abandoned at cancellation — and reports
		// whether the worker should keep taking jobs.
		runJob := func(job dataBlockJob) bool {
			defer handles.release(job.filePointer)
			if !slot.acquire() {
				return false
			}
			defer slot.release()
			if scratch == nil {
				scratch = newRowMatchScratch(rowMatcher)
			}
			b.processDataBlock(r, &slot, handles, job, rowMatcher, scratch)
			return true
		}

		for {
			select {
			case job, ok := <-blockJobs:
				if !ok {
					return
				}
				if !runJob(job) {
					return
				}
			case <-r.ctx.Done():
				return
			}
		}
	}

	// Block workers are spawned by whichever file worker dispatched the job, so
	// their running count is shared and moves under CAS.
	var blockWorkersSpawned atomic.Int64
	spawnBlockWorker := func() {
		limit := int64(b.config.MaxQueryConcurrency)
		for {
			running := blockWorkersSpawned.Load()
			if running >= limit {
				return
			}
			if blockWorkersSpawned.CompareAndSwap(running, running+1) {
				break
			}
		}
		blockWorkers.Add(1)
		go blockWorker()
	}

	fileWorker := func() {
		defer fileWorkers.Done()

		slot := querySlot{sem: b.querySemaphore, ctx: r.ctx}
		defer slot.release()
		// One survivor buffer per worker, reused across files.
		var survivors []blockScanCandidate

		for {
			select {
			case job, ok := <-fileJobs:
				if !ok {
					return
				}

				// Blocks are evaluated and dispatched in ascending row data
				// order, so a file's row data reads move forward through the
				// file; the dispatch below indexes the same ordering.
				blocks := blocksByAscendingRowDataOffset(job.blocks)

				// One reference spans the filter pass and the dispatch that
				// follows, so the file's handles cannot be closed in between.
				handles.retain(job.filePointer)
				survivors = b.evaluateBlockFilters(r, &slot, handles, job, blocks, pruneBloomQuery, survivors[:0])
				// The filter pass is the I/O this worker holds a semaphore slot
				// for; dispatch can block on the block workers, and a slot held
				// while blocked would park it for every other query (and, at a
				// small MaxQueryConcurrency, keep the block workers from ever
				// acquiring one).
				slot.release()

				dispatched := true
				for _, survivor := range survivors {
					blockJob := dataBlockJob{
						filePointer:    job.filePointer,
						blockMetadata:  blocks[survivor.index],
						filterDuration: survivor.filterDuration,
					}
					handles.retain(job.filePointer)
					if err := sendWithContext(r.ctx, blockJobs, blockJob); err != nil {
						handles.release(job.filePointer)
						dispatched = false
						break
					}
					spawnBlockWorker()
				}
				handles.release(job.filePointer)
				if !dispatched {
					return
				}
			case <-r.ctx.Done():
				return
			}
		}
	}

	// File stage: pull one candidate at a time from the MetaStore iterator and
	// hand the survivors to the file workers. Exiting the range loop — on
	// error, cancellation, or a blocked job send — runs the store iterator's
	// deferred cleanup. File-level bloom tests are pure in-memory checks, so
	// this single goroutine runs them inline without taking query-semaphore
	// slots; the semaphore keeps bounding the I/O stages.
	//
	// The file stage holds a token in fileWorkers and is the only spawner of
	// file workers, so that counter cannot reach zero — letting fileWorkers.Wait
	// return — before it exits; spawns therefore never race the Wait, and the
	// zero-worker query (empty iterator, everything pruned) completes when the
	// file stage alone finishes.
	fileWorkers.Add(1)
	go func() {
		defer fileWorkers.Done()
		defer close(fileJobs)

		workersSpawned := 0
		for maybeFile, err := range b.metaStore.GetMaybeFilesForQuery(r.ctx, query.Prefilter) {
			if err != nil {
				// Stop pulling; blocks already dispatched still finish, and
				// the error surfaces from Results.Err.
				r.recordQueryError(fmt.Errorf("MetaStore iteration failed: %w", err))
				return
			}
			if r.ctx.Err() != nil {
				return
			}

			// The engine enforces strict prefilter semantics itself:
			// MetaStore-side prefiltering is only an optimization, so
			// re-filter whatever the store yielded. FilterDataBlocks
			// allocates a new slice when it filters, and MaybeFile is a
			// value, so the store's own metadata is never mutated. Files
			// left with no matching blocks are dropped.
			maybeFile.Metadata.DataBlocks = FilterDataBlocks(maybeFile.Metadata.DataBlocks, query.Prefilter)
			if len(maybeFile.Metadata.DataBlocks) == 0 {
				continue
			}

			if hasBloomConditions && !b.evaluateBloomFilters(
				maybeFile.Metadata.BloomFilters.FieldBloomFilter,
				maybeFile.Metadata.BloomFilters.TokenBloomFilter,
				maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter,
				pruneBloomQuery,
			) {
				continue
			}

			// Once a file has passed (or skipped) the file-level test, its
			// filters have served their purpose: release them before block
			// jobs are built so per-query memory stops scaling with
			// candidate-file filter size.
			maybeFile.Metadata.BloomFilters = BloomFilters{}

			if b.queryFilePruneHook != nil {
				b.queryFilePruneHook(maybeFile)
			}

			job := fileFilterJob{
				filePointer:        maybeFile.PointerBytes,
				filterRegionOffset: maybeFile.Metadata.BlockFilterRegionOffset,
				filterRegionSize:   maybeFile.Metadata.BlockFilterRegionSize,
				blocks:             maybeFile.Metadata.DataBlocks,
			}
			if err := sendWithContext(r.ctx, fileJobs, job); err != nil {
				return
			}
			if workersSpawned < b.config.MaxQueryConcurrency {
				workersSpawned++
				fileWorkers.Add(1)
				go fileWorker()
			}
		}
	}()

	// Teardown order: file workers are the only spawners of block workers, so
	// waiting for them first makes blockWorkers.Wait race-free, and the pool
	// closes only once no reader can hold or ask for a handle.
	go func() {
		fileWorkers.Wait()
		close(blockJobs)
		blockWorkers.Wait()
		handles.closeAll()
		r.markWorkersDone()
	}()

	return r, nil
}

// blocksByAscendingRowDataOffset returns the blocks ordered by row data offset,
// which is the order the file worker evaluates and dispatches them in, so a
// file's row data reads move forward through the file instead of jumping back.
// Metadata almost always arrives in that order (blocks are written in order), so
// the common case only verifies it; when a store yields them out of order the
// sort runs on a copy, because the slice belongs to the MetaStore's yielded
// metadata.
func blocksByAscendingRowDataOffset(blocks []DataBlockMetadata) []DataBlockMetadata {
	byOffset := func(a, b DataBlockMetadata) int { return cmp.Compare(a.RowDataOffset, b.RowDataOffset) }
	if slices.IsSortedFunc(blocks, byOffset) {
		return blocks
	}
	sorted := slices.Clone(blocks)
	slices.SortFunc(sorted, byOffset)
	return sorted
}

// evaluateBlockFilters evaluates one candidate file's data block filters,
// appending the blocks that survived to dst as indexes into blocks.
//
// The file's filter sections are contiguous on disk, so the pass costs one open
// and a read per chunk of the region rather than a read per block: a
// blockFilterCursor reads up to blockFilterChunkTarget bytes at a time into a
// pooled buffer and decodes each block's filters from it, so a file whose whole
// region fits the cap costs a single read. The handle is held for the pass and
// returned to the pool before the survivors are dispatched, so the file's first
// block scan borrows it back; the chunk buffer is released when the pass ends.
//
// Pruned blocks are accounted for here: each records its BlockStats with
// BloomFilterSkipped set, zero rows and bytes processed, and a Duration
// covering its share of the open and of the chunk read that served it, plus its
// own evaluation — all the block cost. Surviving blocks carry that duration to
// their scan job so their own BlockStats.Duration still covers the whole block.
//
// Failures keep the query going, and the blocks already evaluated keep their
// outcome:
//
//   - A section that does not parse is that block's problem alone: the block
//     records an error and the pass continues with the rest of the region.
//   - A file that cannot be opened, or whose chunk read fails, or whose metadata
//     does not describe a readable region, makes the whole file unreadable: the
//     handle is dropped, the file records one error, and every block still
//     records the stats entry it owes.
func (b *BloomSearchEngine) evaluateBlockFilters(
	r *Results,
	slot *querySlot,
	handles *fileHandlePool,
	job fileFilterJob,
	blocks []DataBlockMetadata,
	pruneBloomQuery *BloomQuery,
	dst []blockScanCandidate,
) []blockScanCandidate {
	// A query without bloom conditions cannot be disqualified by any filter: no
	// section is read at all and every candidate block goes on to be scanned.
	if pruneBloomQuery == nil || pruneBloomQuery.Expression == nil {
		for i := range blocks {
			dst = append(dst, blockScanCandidate{index: i})
		}
		return dst
	}

	if !slot.acquire() {
		return dst
	}

	// fail records a filter-evaluation failure unless the query has terminated:
	// after cancellation or Close, the terminal state already tells the story
	// and errors provoked by the teardown itself are noise.
	fail := func(err error) {
		if r.ctx.Err() != nil {
			return
		}
		r.recordBlockError(err)
	}

	if r.ctx.Err() != nil {
		// Cancellation is neither an error nor a block outcome: the blocks
		// record nothing, exactly like blocks still queued for a scan when the
		// query terminates.
		return dst
	}

	regionStart, regionEnd, hasSections, err := planBlockFilterReads(blocks, job.filterRegionOffset, job.filterRegionSize)
	if err != nil {
		fail(fmt.Errorf("unusable block filter region metadata: %w", err))
		recordUnreadBlocks(r, job.filePointer, blocks, 0)
		return dst
	}
	if !hasSections {
		// Nothing to read, so the file is never even opened: absent filters
		// disqualify nothing and every candidate block goes on to be scanned.
		for i := range blocks {
			dst = append(dst, blockScanCandidate{index: i})
		}
		return dst
	}

	openStart := time.Now()
	handle, err := handles.acquire(r.ctx, job.filePointer)
	openDuration := time.Since(openStart)
	if err != nil {
		fail(fmt.Errorf("failed to open file: %w", err))
		recordUnreadBlocks(r, job.filePointer, blocks, openDuration)
		return dst
	}
	// A handle whose read failed has an unknown stream position and must not be
	// lent to the file's next reader; a healthy one goes back for the block scans.
	handleHealthy := true
	defer func() {
		if handleHealthy {
			handles.put(job.filePointer, handle)
		} else {
			handles.discard(handle)
		}
	}()

	cursor := blockFilterCursor{file: handle, blocks: blocks, regionStart: regionStart, regionEnd: regionEnd}
	defer cursor.release()

	// The open serves the whole pass, so every block carries an even share of it.
	// hasSections guarantees at least one block.
	openShare := openDuration / time.Duration(len(blocks))

	for i := range blocks {
		if r.ctx.Err() != nil {
			return dst
		}

		block := blocks[i]
		blockStart := time.Now()

		filters, readShare, readFailed, err := cursor.filtersFor(i)
		if err != nil {
			fail(fmt.Errorf("failed to read data block bloom filters: %w", err))
			if readFailed {
				handleHealthy = false
				recordUnreadBlocks(r, job.filePointer, blocks[i:], openShare+readShare+time.Since(blockStart))
				return dst
			}
			recordUnreadBlocks(r, job.filePointer, blocks[i:i+1], openShare+readShare+time.Since(blockStart))
			continue
		}

		survived := b.evaluateBloomFilters(
			filters.FieldBloomFilter,
			filters.TokenBloomFilter,
			filters.FieldTokenBloomFilter,
			pruneBloomQuery,
		)
		// The block's cost: its share of the open and of the chunk read that
		// served it, plus reading its filters out of that chunk and testing them.
		duration := openShare + readShare + time.Since(blockStart)

		if survived {
			dst = append(dst, blockScanCandidate{index: i, filterDuration: duration})
			continue
		}

		r.recordBlockStats(BlockStats{
			FilePointer:        job.filePointer,
			BlockOffset:        block.RowDataOffset,
			TotalRows:          int64(block.Rows),
			TotalBytes:         int64(block.OnDiskSize()),
			Duration:           duration,
			BloomFilterSkipped: true,
		})
	}

	return dst
}

// recordUnreadBlocks gives blocks a failure kept the query from evaluating the
// stats entry every block owes: nothing scanned, nothing pruned, and the
// block's full metadata totals — the same shape a block whose scan failed
// records. The first block carries the failed operation's duration; the ones
// behind it cost nothing.
func recordUnreadBlocks(r *Results, filePointer []byte, blocks []DataBlockMetadata, firstDuration time.Duration) {
	for i, block := range blocks {
		duration := time.Duration(0)
		if i == 0 {
			duration = firstDuration
		}
		r.recordBlockStats(BlockStats{
			FilePointer: filePointer,
			BlockOffset: block.RowDataOffset,
			TotalRows:   int64(block.Rows),
			TotalBytes:  int64(block.OnDiskSize()),
			Duration:    duration,
		})
	}
}

// processDataBlock scans one data block for the query behind r, delivering
// matched rows to the cursor and recording the block's stats losslessly. The
// block's filters were already evaluated by the file stage
// (evaluateBlockFilters), so reaching here means the block must be scanned.
//
// A failure in this block records its error on the cursor and returns: the
// query continues with other blocks, because partial results are valuable for
// search, and the error surfaces from Results.Err once iteration finishes.
// Cancellation (of the Query context, or via Results.Close) is not a block
// error; it just stops the scan.
func (b *BloomSearchEngine) processDataBlock(
	r *Results,
	slot *querySlot,
	handles *fileHandlePool,
	job dataBlockJob,
	rowMatcher *compiledRowMatcher,
	scratch *rowMatchScratch,
) {
	blockStartTime := time.Now()
	var rowsScanned, bytesScanned int64

	// Record stats on every exit path. RowsProcessed/BytesProcessed report what
	// was actually scanned (zero when the scan failed before reading rows);
	// TotalRows/TotalBytes remain the block's full counts. Duration includes the
	// block's filter evaluation and its share of the file's region read, which
	// the file stage performed.
	defer func() {
		r.recordBlockStats(BlockStats{
			FilePointer:    job.filePointer,
			BlockOffset:    job.blockMetadata.RowDataOffset,
			RowsProcessed:  rowsScanned,
			BytesProcessed: bytesScanned,
			TotalRows:      int64(job.blockMetadata.Rows),
			TotalBytes:     int64(job.blockMetadata.OnDiskSize()),
			Duration:       job.filterDuration + time.Since(blockStartTime),
		})
	}()

	ctx := r.ctx

	// fail records a block error unless the query has terminated: after
	// cancellation or Close, the terminal state (Results.Err) already tells
	// the story, and errors provoked by the teardown itself are noise.
	fail := func(err error) {
		if ctx.Err() != nil {
			return
		}
		r.recordBlockError(err)
	}

	file, err := handles.acquire(ctx, job.filePointer)
	if err != nil {
		fail(fmt.Errorf("failed to open file: %w", err))
		return
	}

	// Verify before emit: the row data section is read fully into memory and
	// CRC-checked, then decompressed with its output bounded by the block's
	// UncompressedSize, before any row is scanned — a corrupt block errors
	// cleanly instead of streaming corrupt rows to the caller, and a corrupt
	// length prefix is rejected by the scanner instead of driving a giant
	// allocation.
	rowData, releaseRowData, err := readPooledBlockRowData(file, &job.blockMetadata)
	if err != nil {
		// This read is the only thing the scan needs the handle for, and a
		// failure may have left it mid-stream: close it rather than lend it to
		// the file's next reader.
		handles.discard(file)
		fail(fmt.Errorf("failed to read block row data: %w", err))
		return
	}
	// The rest of the scan runs on the in-memory row data, so the handle goes
	// back now instead of at the end of the scan: the file's other blocks can
	// reuse it, and no handle is ever held while a worker waits on a slow
	// consumer.
	handles.put(job.filePointer, file)
	// The block buffer returns to the pool when this scan exits: by then every
	// row view has been dropped (matching parses transient views; matched rows
	// are materialized as independent copies before batching).
	defer releaseRowData()

	// Matched rows are delivered in batches; the deferred flush covers every
	// scan exit (end of data, block error, cancellation) so rows matched
	// before a mid-block failure still reach the cursor.
	batcher := rowBatcher{results: r, slot: slot}
	defer batcher.flush()

	scanner := NewBlockRowScanner(rowData)
	for {
		// Cancellation is terminal for the query; stop scanning promptly.
		if ctx.Err() != nil {
			return
		}

		rowBytes, ok, err := scanner.Next()
		if err != nil {
			fail(fmt.Errorf("failed to read row: %w", err))
			return
		}
		if !ok {
			break // End of data
		}

		rowsScanned++
		bytesScanned += int64(LengthPrefixSize) + int64(len(rowBytes))

		// Matching parses a zero-copy view of the row (rowBytes is a subslice
		// of the block buffer, immutable for the whole scan); a matched row is
		// materialized from an independent copy so delivered maps never alias
		// the block buffer (see materializeRow).
		if !rowMatcher.matchRowBytes(rowBytes, scratch) {
			continue
		}

		row, err := materializeRow(rowBytes)
		if err != nil {
			fail(err)
			return
		}

		if err := batcher.add(row); err != nil {
			return
		}
	}
}
