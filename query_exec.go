package bloomsearch

// Query execution: file/block bloom evaluation and the block scan workers
// behind the Results cursor (query_results.go).

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// dataBlockJob represents a job to process a data block
type dataBlockJob struct {
	filePointer   []byte
	blockMetadata DataBlockMetadata
}

// BlockStats describes one block job of a query. RowsProcessed and
// BytesProcessed report what the scan actually read — rows and uncompressed
// row bytes (length prefix included) — so a bloom-skipped block reports zero.
// TotalRows and TotalBytes are the block's metadata totals (TotalBytes is the
// on-disk block size, bloom filters included).
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

// queryJobBuffer is the block-job channel's buffer. It only smooths the
// hand-off between the file stage and the block workers; the total number of
// candidate files buffered anywhere in a query is bounded by the worker count
// plus this constant, never by the candidate count.
const queryJobBuffer = 16

// Query starts a query and returns a Results cursor streaming the matching
// rows. Only pre-iteration setup (regex compilation) can fail fast with
// (nil, err); everything else — the MetaStore iteration, prefilter
// enforcement, file-level bloom pruning, and block scans — runs behind the
// cursor, which the engine fully owns (see Results for the iteration, error,
// cancellation, and stats contracts).
//
// Candidate files stream from the MetaStore iterator through a bounded
// pipeline: a file stage pulls one file at a time, prunes it, and fans its
// data blocks out to the block workers, so per-query memory scales with the
// in-flight window (worker count plus constant channel buffers), not with
// the candidate-file count. A MetaStore error mid-iteration stops further
// pulls but lets already-dispatched block jobs finish and deliver their
// rows; the error surfaces from Results.Err joined with any block errors —
// the same partial-results philosophy as block errors.
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
	// filter sections (see processDataBlock).
	hasBloomConditions := pruneBloomQuery != nil && pruneBloomQuery.Expression != nil

	r := newResults(ctx)

	jobs := make(chan dataBlockJob, queryJobBuffer)

	// Block workers are spawned on demand, one per job sent, until
	// MaxQueryConcurrency are running: the job total is unknown up front
	// (files stream from the MetaStore), and spawning against demand yields
	// the same min(jobs, MaxQueryConcurrency) worker count the old upfront
	// sizing produced — a fully pruned query spawns none. Each worker also
	// watches the query context so cancellation winds workers down promptly
	// even while the file stage is still draining the store iterator.
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()

		slot := querySlot{sem: b.querySemaphore, ctx: r.ctx}
		defer slot.release()
		var scratch *rowMatchScratch
		for {
			select {
			case job, ok := <-jobs:
				if !ok {
					return
				}
				if !slot.acquire() {
					return
				}
				if scratch == nil {
					scratch = newRowMatchScratch(rowMatcher)
				}
				b.processDataBlock(r, &slot, job, pruneBloomQuery, rowMatcher, scratch)
				slot.release()
			case <-r.ctx.Done():
				return
			}
		}
	}

	// File stage: pull one candidate at a time from the MetaStore iterator and
	// fan its blocks out to the workers. Exiting the range loop — on error,
	// cancellation, or a blocked job send — runs the store iterator's deferred
	// cleanup. File-level bloom tests are pure in-memory checks, so this
	// single goroutine runs them inline without taking query-semaphore slots;
	// the semaphore keeps bounding block scans exactly as before.
	//
	// The file stage holds its own WaitGroup token and is the only spawner,
	// so the counter cannot reach zero — letting wg.Wait return — before it
	// exits; worker spawns therefore never race wg.Wait, and the zero-worker
	// query (empty iterator, everything pruned) completes when the file stage
	// alone finishes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(jobs)

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

			for _, blockMetadata := range maybeFile.Metadata.DataBlocks {
				job := dataBlockJob{
					filePointer:   maybeFile.PointerBytes,
					blockMetadata: blockMetadata,
				}
				if err := sendWithContext(r.ctx, jobs, job); err != nil {
					return
				}
				if workersSpawned < b.config.MaxQueryConcurrency {
					workersSpawned++
					wg.Add(1)
					go worker()
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		r.markWorkersDone()
	}()

	return r, nil
}

// processDataBlock scans one data block for the query behind r, delivering
// matched rows to the cursor and recording the block's stats losslessly.
//
// A failure in this block records its error on the cursor and returns: the
// query continues with other blocks, because partial results are valuable for
// search, and the error surfaces from Results.Err once iteration finishes.
// Cancellation (of the Query context, or via Results.Close) is not a block
// error; it just stops the scan.
func (b *BloomSearchEngine) processDataBlock(
	r *Results,
	slot *querySlot,
	job dataBlockJob,
	pruneBloomQuery *BloomQuery,
	rowMatcher *compiledRowMatcher,
	scratch *rowMatchScratch,
) {
	blockStartTime := time.Now()
	var bloomFilterSkipped bool
	var rowsScanned, bytesScanned int64

	// Record stats on every exit path. RowsProcessed/BytesProcessed report
	// what was actually scanned (zero for a bloom-skipped block); TotalRows/
	// TotalBytes remain the block's full counts.
	defer func() {
		r.recordBlockStats(BlockStats{
			FilePointer:        job.filePointer,
			BlockOffset:        job.blockMetadata.Offset,
			RowsProcessed:      rowsScanned,
			BytesProcessed:     bytesScanned,
			TotalRows:          int64(job.blockMetadata.Rows),
			TotalBytes:         int64(job.blockMetadata.Size),
			Duration:           time.Since(blockStartTime),
			BloomFilterSkipped: bloomFilterSkipped,
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

	file, err := b.dataStore.OpenFile(ctx, job.filePointer)
	if err != nil {
		fail(fmt.Errorf("failed to open file: %w", err))
		return
	}
	defer file.Close()

	// The block filter section is only read when the query has bloom
	// conditions; a condition-free query jumps straight to the row data.
	if pruneBloomQuery != nil && pruneBloomQuery.Expression != nil {
		blockBloomFilters, err := ReadDataBlockBloomFilters(file, job.blockMetadata)
		if err != nil {
			fail(fmt.Errorf("failed to read data block bloom filters: %w", err))
			return
		}

		if !b.evaluateBloomFilters(
			blockBloomFilters.FieldBloomFilter,
			blockBloomFilters.TokenBloomFilter,
			blockBloomFilters.FieldTokenBloomFilter,
			pruneBloomQuery,
		) {
			bloomFilterSkipped = true
			return
		}
	}

	// Verify before emit: the row data section is read fully into memory and
	// CRC-checked, then decompressed with its output bounded by the block's
	// UncompressedSize, before any row is scanned — a corrupt block errors
	// cleanly instead of streaming corrupt rows to the caller, and a corrupt
	// length prefix is rejected by the scanner instead of driving a giant
	// allocation.
	rowData, releaseRowData, err := readPooledBlockRowData(file, &job.blockMetadata)
	if err != nil {
		fail(fmt.Errorf("failed to read block row data: %w", err))
		return
	}
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
