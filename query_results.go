package bloomsearch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// queryRowBuffer is the size of the engine-owned row buffer between block
// workers and the Results cursor: enough to smooth worker/consumer
// interleaving without letting a stalled consumer accumulate unbounded rows.
const queryRowBuffer = 256

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
	// RowsMatched counts rows that matched the query during scanning. On a
	// clean completion it equals the number of rows observed through Next;
	// after cancellation or Close it may exceed them, because matched rows
	// buffered for delivery are dropped at termination.
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

	rowChan chan map[string]any // closed once all block workers finish
	done    chan struct{}       // closed once all block workers finish and stats are frozen

	rowsMatched atomic.Int64

	mu         sync.Mutex
	blockErrs  []error
	blockStats []BlockStats
	duration   time.Duration
	finished   bool // all workers finished; duration frozen
	finalized  bool // terminal state decided; err immutable from here
	err        error

	start time.Time

	// Iteration state, owned by the goroutine calling Next/Row.
	current  map[string]any
	iterDone bool

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
		rowChan:   make(chan map[string]any, queryRowBuffer),
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
// false promptly (bounded by the workers observing cancellation), rows
// already buffered but not yet delivered are dropped, and Err returns the
// context error.
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

	select {
	case row, ok := <-r.rowChan:
		if !ok {
			// Clean completion: all workers finished and every buffered row
			// has been delivered. Block errors (if any) are the terminal
			// state; the query deliberately continued past them because
			// partial results are valuable for search.
			r.finish(r.joinedBlockErrs())
			return false
		}
		r.current = row
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
//   - the per-block errors joined with errors.Join when blocks failed — a
//     block failure records its error and the query continues with other
//     blocks, so errors are surfaced here without discarding the partial
//     results, and never silently dropped;
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
// waits for the block workers to wind down (bounded — every internal send
// honors that context, and global query-semaphore slots are released as the
// workers exit), and freezes the terminal state. Close is idempotent, safe to
// call concurrently with Next, and always returns nil: Close is not an error
// state, so a subsequent Err returns whatever terminal state existed before
// Close (nil if none). A closed Results is not reusable.
func (r *Results) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		<-r.done

		err := r.joinedBlockErrs()
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
// workers, wait for them to wind down (bounded — they honor the internal
// context), and freeze the terminal state.
func (r *Results) terminate() bool {
	r.cancel()
	<-r.done

	var err error
	if cerr := r.callerCtx.Err(); cerr != nil {
		err = fmt.Errorf("query canceled: %w", cerr)
	} else {
		// Close path: keep whatever block errors existed before Close.
		err = r.joinedBlockErrs()
	}
	r.finish(err)
	return false
}

// finish ends iteration with err as the terminal state, unless a terminal
// state was already decided (first finalizer wins).
func (r *Results) finish(err error) {
	r.iterDone = true
	r.current = nil
	r.mu.Lock()
	if !r.finalized {
		r.finalized = true
		r.err = err
	}
	r.mu.Unlock()
	r.cancel()
}

func (r *Results) joinedBlockErrs() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return errors.Join(r.blockErrs...)
}

// deliver hands a matched row to the cursor. The fast path is a non-blocking
// send into the row buffer. When the buffer is full, the worker releases its
// global query-semaphore slot before blocking, so a stalled consumer parks
// only its own query's workers — other queries proceed — then re-acquires the
// slot before resuming the scan. Returns the context error when the query
// terminated instead of accepting the row.
func (r *Results) deliver(slot *querySlot, row map[string]any) error {
	select {
	case r.rowChan <- row:
		r.rowsMatched.Add(1)
		return nil
	default:
	}

	slot.release()
	select {
	case r.rowChan <- row:
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	r.rowsMatched.Add(1)
	if !slot.acquire() {
		return r.ctx.Err()
	}
	return nil
}

// recordBlockStats appends one block's stats. Collection is engine-internal
// and lossless: a slice append, not a lossy channel write.
func (r *Results) recordBlockStats(stats BlockStats) {
	r.mu.Lock()
	r.blockStats = append(r.blockStats, stats)
	r.mu.Unlock()
}

// recordBlockError records a block-processing failure. The query continues
// with other blocks; the error surfaces from Err once iteration finishes.
func (r *Results) recordBlockError(err error) {
	r.mu.Lock()
	r.blockErrs = append(r.blockErrs, err)
	r.mu.Unlock()
}

// markWorkersDone freezes Duration and releases everything waiting on the
// cursor: the row channel closes (Next drains any buffered rows, then reports
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
