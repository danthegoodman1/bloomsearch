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

// Query starts a query and returns a Results cursor streaming the matching
// rows. Everything that can fail fast — regex compilation, the MetaStore
// lookup, prefilter enforcement, and file-level bloom pruning — runs
// synchronously and returns (nil, err) without starting anything. On success
// the engine owns every channel and goroutine behind the cursor; see Results
// for the iteration, error, cancellation, and stats contracts.
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

	maybeFiles, err := b.metaStore.GetMaybeFilesForQuery(ctx, query.Prefilter)
	if err != nil {
		return nil, err
	}

	// The engine enforces strict prefilter semantics itself: MetaStore-side
	// prefiltering is only an optimization, so re-filter whatever the store
	// returned. FilterDataBlocks allocates a new slice, and MaybeFile is a
	// value, so the store's own metadata is never mutated. Files left with no
	// matching blocks are dropped.
	prefilteredFiles := make([]MaybeFile, 0, len(maybeFiles))
	for _, maybeFile := range maybeFiles {
		maybeFile.Metadata.DataBlocks = FilterDataBlocks(maybeFile.Metadata.DataBlocks, query.Prefilter)
		if len(maybeFile.Metadata.DataBlocks) == 0 {
			continue
		}
		prefilteredFiles = append(prefilteredFiles, maybeFile)
	}
	maybeFiles = prefilteredFiles

	// A query without bloom conditions cannot be disqualified by any filter:
	// skip filter evaluation entirely, and downstream, skip reading the block
	// filter sections (see processDataBlock).
	hasBloomConditions := pruneBloomQuery != nil && pruneBloomQuery.Expression != nil

	// Test file-level bloom filters, using concurrency only above a threshold
	const concurrencyThreshold = 20

	// Once a file has passed (or skipped) the file-level test, its filters
	// have served their purpose: release them before block jobs are built so
	// per-query memory stops scaling with candidate-file size.
	var matchingFiles []MaybeFile
	if !hasBloomConditions || len(maybeFiles) < concurrencyThreshold {
		// Sequential evaluation for small numbers of files
		matchingFiles = make([]MaybeFile, 0, len(maybeFiles))
		for _, maybeFile := range maybeFiles {
			if !hasBloomConditions || b.evaluateBloomFilters(
				maybeFile.Metadata.BloomFilters.FieldBloomFilter,
				maybeFile.Metadata.BloomFilters.TokenBloomFilter,
				maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter,
				pruneBloomQuery,
			) {
				maybeFile.Metadata.BloomFilters = BloomFilters{}
				matchingFiles = append(matchingFiles, maybeFile)
			}
		}
	} else {
		// Concurrent evaluation for larger numbers of files
		var fileWg sync.WaitGroup
		matchingFilesChan := make(chan MaybeFile, len(maybeFiles))

		for _, maybeFile := range maybeFiles {
			fileWg.Add(1)
			go func(maybeFile MaybeFile) {
				defer fileWg.Done()

				if err := sendWithContext(ctx, b.querySemaphore, struct{}{}); err != nil {
					return
				}
				defer func() { <-b.querySemaphore }()

				if b.evaluateBloomFilters(
					maybeFile.Metadata.BloomFilters.FieldBloomFilter,
					maybeFile.Metadata.BloomFilters.TokenBloomFilter,
					maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter,
					pruneBloomQuery,
				) {
					maybeFile.Metadata.BloomFilters = BloomFilters{}
					sendWithContext(ctx, matchingFilesChan, maybeFile)
				}
			}(maybeFile)
		}

		go func() {
			fileWg.Wait()
			close(matchingFilesChan) // close to tell the range below to stop
		}()

		for matchingFile := range matchingFilesChan {
			matchingFiles = append(matchingFiles, matchingFile)
		}
	}

	if b.queryFilePruneHook != nil {
		b.queryFilePruneHook(matchingFiles)
	}

	// Cancellation during setup surfaces as a query error rather than
	// silently proceeding with whichever files happened to be evaluated.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	totalJobs := 0
	for _, matchingFile := range matchingFiles {
		totalJobs += len(matchingFile.Metadata.DataBlocks)
	}

	r := newResults(ctx)

	if totalJobs == 0 {
		r.markWorkersDone()
		return r, nil
	}

	workerCount := min(b.config.MaxQueryConcurrency, totalJobs)
	jobs := make(chan dataBlockJob, workerCount)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			slot := querySlot{sem: b.querySemaphore, ctx: r.ctx}
			defer slot.release()
			scratch := newRowMatchScratch(rowMatcher)
			for job := range jobs {
				if !slot.acquire() {
					return
				}
				b.processDataBlock(r, &slot, job, pruneBloomQuery, rowMatcher, scratch)
				slot.release()
			}
		}()
	}

	go func() {
		defer close(jobs)

		for _, matchingFile := range matchingFiles {
			for _, blockMetadata := range matchingFile.Metadata.DataBlocks {
				job := dataBlockJob{
					filePointer:   matchingFile.PointerBytes,
					blockMetadata: blockMetadata,
				}
				if err := sendWithContext(r.ctx, jobs, job); err != nil {
					return
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
