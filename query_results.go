package bloomsearch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Matched rows travel from block workers to the Results cursor in worker-local
// batches: per-row sends on a shared channel made the channel lock the scan
// bottleneck once row verification itself became cheap. queryRowBatchSize rows
// are accumulated per worker before one channel send; queryRowBatchBuffer
// batches may sit in the channel, bounding buffered-but-undelivered rows at
// queryRowBatchBuffer×queryRowBatchSize (256, the same bound the previous
// per-row buffer provided) so a stalled consumer cannot accumulate unbounded
// rows. The tradeoff is first-row latency: a matched row waits until its batch
// fills or its block's scan ends, so delivery lags by at most one block scan.
const (
	queryRowBatchSize   = 64
	queryRowBatchBuffer = 4
)

// QueryStats summarizes the work a query performed. Obtain it from
// Results.Stats; it is complete once Next has returned false.
type QueryStats struct {
	// BlocksProcessed counts blocks not pruned by bloom filters, including
	// blocks whose scan failed (possibly before reading any row data).
	BlocksProcessed int
	// BlocksSkipped counts blocks pruned by their bloom filters without any
	// row data being read.
	BlocksSkipped int
	// RowsScanned and BytesScanned count the rows and uncompressed row bytes
	// actually read across all blocks (bloom-skipped blocks contribute zero).
	RowsScanned  int64
	BytesScanned int64
	// RowsMatched counts matched rows delivered toward the cursor. On a clean
	// completion it equals the number of rows observed through Next; after
	// cancellation or Close the two may differ, because matched rows still
	// buffered (or batched in a worker) at termination are dropped.
	RowsMatched int64
	// Duration is the time from Query returning the cursor until the last
	// block worker finished, or the elapsed time so far while in flight.
	Duration time.Duration
	// BlockStats holds per-block detail, one element per block job that ran.
	// Collection is lossless: every block job contributes exactly one entry.
	BlockStats []BlockStats
}

// Results is an engine-owned cursor over a query's matching rows. The engine
// owns every channel and goroutine behind it; callers only iterate:
//
//	results, err := engine.Query(ctx, query)
//	if err != nil { return err }
//	defer results.Close()
//	for results.Next() {
//	    row := results.Row()
//	    // use row
//	}
//	if err := results.Err(); err != nil { return err }
//
// Next and Row must be used from a single goroutine; Close may be called
// concurrently with them. A Results is single-use: once iteration ends, call
// Query again for a fresh cursor.
type Results struct {
	// callerCtx is the context passed to Query; it distinguishes caller
	// cancellation (a terminal error) from a deliberate Close (not an error).
	callerCtx context.Context
	// ctx is the query's internal context: canceled when callerCtx is
	// canceled, when Close is called, or when iteration finishes. Every
	// internal blocking send selects on it, so nothing wedges.
	ctx    context.Context
	cancel context.CancelFunc

	rowChan chan []map[string]any // closed once all block workers finish
	done    chan struct{}         // closed once all block workers finish and stats are frozen

	rowsMatched atomic.Int64

	mu         sync.Mutex
	errs       []error // recorded block-scan and MetaStore-iteration failures
	blockStats []BlockStats
	duration   time.Duration
	finished   bool // all workers finished; duration frozen
	finalized  bool // terminal state decided; err immutable from here
	err        error

	start time.Time

	// Iteration state, owned by the goroutine calling Next/Row. pending holds
	// the batch currently being handed out row by row.
	current    map[string]any
	pending    []map[string]any
	pendingIdx int
	iterDone   bool

	closeOnce sync.Once
}

// newResults builds a cursor whose internal context is derived from the
// caller's ctx.
func newResults(ctx context.Context) *Results {
	internalCtx, cancel := context.WithCancel(ctx)
	return &Results{
		callerCtx: ctx,
		ctx:       internalCtx,
		cancel:    cancel,
		rowChan:   make(chan []map[string]any, queryRowBatchBuffer),
		done:      make(chan struct{}),
		start:     time.Now(),
	}
}

// Next blocks until the next matching row is available and reports whether it
// produced one; retrieve the row with Row. It returns false on clean
// completion (every block finished and every row was delivered), on
// cancellation of the Query context, or after Close. Once Next has returned
// false, Err reports the terminal state and Stats is complete; further Next
// calls keep returning false.
//
// Cancellation is terminal: after the Query context is canceled, Next returns
// false once the query pipeline — the file stage pulling from the MetaStore
// iterator and the block workers — has observed the cancellation and exited
// (bounded: see Close), rows already buffered but not yet delivered are
// dropped, and Err returns the context error.
func (r *Results) Next() bool {
	if r.iterDone {
		return false
	}

	// Observe cancellation and Close before considering buffered rows:
	// termination drops undelivered rows rather than draining them.
	select {
	case <-r.ctx.Done():
		return r.terminate()
	default:
	}

	if r.pendingIdx < len(r.pending) {
		r.current = r.pending[r.pendingIdx]
		r.pendingIdx++
		return true
	}

	select {
	case batch, ok := <-r.rowChan:
		if !ok {
			// Clean completion: all workers finished and every buffered row
			// has been delivered. Recorded errors (failed blocks, a failed
			// MetaStore iteration), if any, are the terminal state; the query
			// deliberately continued past them because partial results are
			// valuable for search.
			r.finish(r.joinedErrs())
			return false
		}
		// Workers only deliver non-empty batches.
		r.pending = batch
		r.pendingIdx = 1
		r.current = batch[0]
		return true
	case <-r.ctx.Done():
		return r.terminate()
	}
}

// Row returns the row produced by the most recent successful Next. It remains
// valid until the next call to Next. After Next returns false, Row returns
// nil.
func (r *Results) Row() map[string]any {
	return r.current
}

// Err returns the query's terminal state once Next has returned false (and
// nil before then):
//
//   - nil on clean completion;
//   - the recorded errors joined with errors.Join when parts of the query
//     failed — a block-scan failure continues with the other blocks, and a
//     MetaStore iteration failure stops pulling candidate files while
//     already-dispatched blocks finish — so errors are surfaced here without
//     discarding the partial results, and never silently dropped;
//   - the Query context's error (matching errors.Is) when the query was
//     canceled, so a canceled query is never mistaken for a complete one;
//   - after a deliberate Close, whatever terminal state existed before Close
//     (nil if none): Close itself is not an error state.
func (r *Results) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// Stats returns a snapshot of the work performed so far. After Next has
// returned false the snapshot is complete: every block job that ran has
// contributed its BlockStats and Duration is frozen.
func (r *Results) Stats() QueryStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := QueryStats{
		RowsMatched: r.rowsMatched.Load(),
		BlockStats:  append([]BlockStats(nil), r.blockStats...),
	}
	if r.finished {
		stats.Duration = r.duration
	} else {
		stats.Duration = time.Since(r.start)
	}
	for _, block := range r.blockStats {
		if block.BloomFilterSkipped {
			stats.BlocksSkipped++
		} else {
			stats.BlocksProcessed++
		}
		stats.RowsScanned += block.RowsProcessed
		stats.BytesScanned += block.BytesProcessed
	}
	return stats
}

// Close terminates the query early: it cancels the query's internal context,
// waits for the query pipeline — the file stage pulling from the MetaStore
// iterator and the block workers — to wind down (bounded: every internal
// send honors that context, global query-semaphore slots are released as the
// workers exit, and the MetaStore contract requires iterators to honor ctx),
// and freezes the terminal state. Close is idempotent, safe to call
// concurrently with Next, and always returns nil: Close is not an error
// state, so a subsequent Err returns whatever terminal state existed before
// Close (nil if none). A closed Results is not reusable.
func (r *Results) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		<-r.done

		err := r.joinedErrs()
		r.mu.Lock()
		if !r.finalized {
			r.finalized = true
			r.err = err
		}
		r.mu.Unlock()
	})
	return nil
}

// terminate handles a Next that observed cancellation or Close: stop the
// pipeline, wait for it to wind down (bounded — see Close), and freeze the
// terminal state.
func (r *Results) terminate() bool {
	r.cancel()
	<-r.done

	var err error
	if cerr := r.callerCtx.Err(); cerr != nil {
		err = fmt.Errorf("query canceled: %w", cerr)
	} else {
		// Close path: keep whatever recorded errors existed before Close.
		err = r.joinedErrs()
	}
	r.finish(err)
	return false
}

// finish ends iteration with err as the terminal state, unless a terminal
// state was already decided (first finalizer wins).
func (r *Results) finish(err error) {
	r.iterDone = true
	r.current = nil
	r.pending = nil
	r.pendingIdx = 0
	r.mu.Lock()
	if !r.finalized {
		r.finalized = true
		r.err = err
	}
	r.mu.Unlock()
	r.cancel()
}

func (r *Results) joinedErrs() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return errors.Join(r.errs...)
}

// deliver hands a non-empty batch of matched rows to the cursor, transferring
// ownership of the slice. The fast path is a non-blocking send into the batch
// buffer. When the buffer is full, the worker releases its global
// query-semaphore slot before blocking, so a stalled consumer parks only its
// own query's workers — other queries proceed — then re-acquires the slot
// before resuming the scan. Returns the context error when the query
// terminated instead of accepting the batch.
func (r *Results) deliver(slot *querySlot, batch []map[string]any) error {
	select {
	case r.rowChan <- batch:
		r.rowsMatched.Add(int64(len(batch)))
		return nil
	default:
	}

	slot.release()
	select {
	case r.rowChan <- batch:
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	r.rowsMatched.Add(int64(len(batch)))
	if !slot.acquire() {
		return r.ctx.Err()
	}
	return nil
}

// rowBatcher accumulates one block worker's matched rows and delivers them in
// queryRowBatchSize batches. Each batch slice is handed off to the cursor, so
// flush starts a fresh one. Callers must flush at the end of a scan (including
// error exits, so rows matched before a mid-block failure are still
// delivered).
type rowBatcher struct {
	results *Results
	slot    *querySlot
	batch   []map[string]any
}

func (b *rowBatcher) add(row map[string]any) error {
	if b.batch == nil {
		b.batch = make([]map[string]any, 0, queryRowBatchSize)
	}
	b.batch = append(b.batch, row)
	if len(b.batch) >= queryRowBatchSize {
		return b.flush()
	}
	return nil
}

func (b *rowBatcher) flush() error {
	if len(b.batch) == 0 {
		return nil
	}
	batch := b.batch
	b.batch = nil
	return b.results.deliver(b.slot, batch)
}

// recordBlockStats appends one block's stats. Collection is engine-internal
// and lossless: a slice append, not a lossy channel write.
func (r *Results) recordBlockStats(stats BlockStats) {
	r.mu.Lock()
	r.blockStats = append(r.blockStats, stats)
	r.mu.Unlock()
}

// recordQueryError records a failure the query survived — a MetaStore
// iterator yielding an error mid-iteration stops further pulls while
// already-dispatched block jobs finish and deliver their rows. The error
// surfaces from Err, joined with any others, once iteration finishes.
func (r *Results) recordQueryError(err error) {
	r.mu.Lock()
	r.errs = append(r.errs, err)
	r.mu.Unlock()
}

// recordBlockError records a block-processing failure. The query continues
// with other blocks; the error surfaces from Err once iteration finishes.
func (r *Results) recordBlockError(err error) {
	r.recordQueryError(err)
}

// markWorkersDone freezes Duration and releases everything waiting on the
// cursor once the query pipeline (file stage and block workers) has exited:
// the row channel closes (Next drains any buffered rows, then reports
// completion) and done closes (terminate and Close stop waiting).
func (r *Results) markWorkersDone() {
	r.mu.Lock()
	r.duration = time.Since(r.start)
	r.finished = true
	r.mu.Unlock()
	close(r.rowChan)
	close(r.done)
}

// querySlot tracks one worker's occupancy of the engine's global query
// semaphore. Workers must not hold a slot while blocked on a slow consumer
// (see Results.deliver), so occupancy toggles rather than nests.
type querySlot struct {
	sem  chan struct{}
	ctx  context.Context
	held bool
}

// acquire takes a semaphore slot, blocking until one is free or the query's
// context is canceled. It reports whether the slot is held.
func (s *querySlot) acquire() bool {
	if s.held {
		return true
	}
	select {
	case s.sem <- struct{}{}:
		s.held = true
		return true
	case <-s.ctx.Done():
		return false
	}
}

// release frees the held slot; releasing an unheld slot is a no-op.
func (s *querySlot) release() {
	if !s.held {
		return
	}
	<-s.sem
	s.held = false
}
