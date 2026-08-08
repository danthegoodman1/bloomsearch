package bloomsearch

// Engine type, configuration, and lifecycle (Start/Stop). The write path
// lives in ingest.go and flush.go, the read path in query_exec.go, and
// merging in merge.go.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrEngineStopped is returned by IngestRows and Flush once Stop has
	// begun: the engine no longer accepts work, so callers must not retry.
	ErrEngineStopped = errors.New("engine is stopped")

	// ErrMergeInProgress is returned by Merge when another Merge call on this
	// engine is still running. Merges are single-flight in-process because
	// concurrent merges would write every merged row into two output files.
	ErrMergeInProgress = errors.New("merge already in progress")

	// ErrPostCommitCleanup wraps tombstone failures that happen after a merge
	// has committed to the MetaStore. The merge itself succeeded and state is
	// consistent — the new file is referenced and the old files are not — but
	// the unreferenced source files could not be tombstoned and linger in the
	// DataStore. Callers receive the MergeStats alongside this error.
	ErrPostCommitCleanup = errors.New("merge committed but source cleanup failed")
)

type PartitionFunc func(row map[string]any) string

type BloomSearchEngine struct {
	config    BloomSearchEngineConfig
	metaStore MetaStore
	dataStore DataStore
	logger    *slog.Logger

	ingestChan chan *ingestRequest
	flushChan  chan flushRequest
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	ingestDone chan struct{}

	// flushCtx governs flush-path store calls and done-channel delivery. It is
	// derived from context.Background, not from b.ctx: once rows are accepted
	// into a flush, the flush must complete for durability, so engine
	// cancellation alone never aborts it. Stop arms flushCancel on its own
	// context (see Stop), which makes the shutdown deadline the only thing
	// that can abort in-flight flushes and their done-channel delivery.
	flushCtx    context.Context
	flushCancel context.CancelFunc

	// stateMu guards started/stopped. IngestRows and Flush take the read lock
	// around the stopped check and the ingestChan send; Stop takes the write
	// lock to set stopped before canceling b.ctx. Because a send only happens
	// under the read lock with stopped == false, no new request can land in
	// ingestChan after Stop holds the write lock — the ingest worker's
	// shutdown drain of ingestChan is therefore complete.
	stateMu sync.RWMutex
	started bool
	stopped bool

	// mergeMu makes Merge single-flight in-process (see ErrMergeInProgress).
	mergeMu sync.Mutex

	querySemaphore chan struct{}

	// queryFilePruneHook, when set (tests only), observes each file that
	// survives the file-level bloom test, after its filters are released and
	// before its block filter job is enqueued. It is called from the query's
	// file-stage goroutine, one file at a time.
	queryFilePruneHook func(MaybeFile)
}

type BloomSearchEngineConfig struct {
	Tokenizer     ValueTokenizerFunc
	PartitionFunc PartitionFunc

	// Logger receives the engine's diagnostics: flush and merge lifecycle at
	// Debug, unusual-but-recoverable conditions (missing bloom filters,
	// flushes abandoned at the Stop deadline) at Warn. nil discards
	// everything — the engine never writes to stdout or stderr on its own.
	Logger *slog.Logger

	MinMaxIndexes []string

	MaxRowGroupBytes int
	MaxRowGroupRows  int
	MaxFileSize      int

	MaxBufferedRows  int
	MaxBufferedBytes int
	MaxBufferedTime  time.Duration

	IngestBufferSize int

	// The maximum number of total data blocks that can be processed concurrently across all queries
	MaxQueryConcurrency int

	// Bloom filter false positive rate. Filter capacities need no
	// configuration: filters are sized at flush/merge time from the measured
	// distinct entry counts of the rows they cover.
	BloomFalsePositiveRate float64

	// Compression configuration
	RowDataCompression CompressionType

	// Compression level for zstd (1-22, higher = better compression, slower)
	// Ignored for snappy compression
	ZstdCompressionLevel int

	// Maximum number of files to merge together in a single merge operation
	MaxFilesToMergePerOperation int
}

func DefaultBloomSearchEngineConfig() BloomSearchEngineConfig {
	return BloomSearchEngineConfig{
		Tokenizer: BasicWhitespaceLowerTokenizer,

		MaxRowGroupBytes: 10 * 1024 * 1024,
		MaxRowGroupRows:  10000,
		MaxFileSize:      10 * 1024 * 1024 * 1024,

		MaxBufferedRows:  1000,
		MaxBufferedBytes: 1 * 1024 * 1024,
		MaxBufferedTime:  10 * time.Second, // this is designed for async writing

		IngestBufferSize: 1_000,

		MaxQueryConcurrency: 1_000,

		BloomFalsePositiveRate: 0.001,

		// Default to Snappy for fast decompression
		RowDataCompression:   CompressionSnappy,
		ZstdCompressionLevel: 3, // Balanced compression/speed for zstd

		MaxFilesToMergePerOperation: 10,
	}
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) (*BloomSearchEngine, error) {
	if config.Tokenizer == nil {
		return nil, fmt.Errorf("%w: tokenizer is required", ErrInvalidConfig)
	}

	if config.MaxRowGroupRows <= 0 {
		return nil, fmt.Errorf("%w: MaxRowGroupRows must be greater than 0", ErrInvalidConfig)
	}

	if config.MaxRowGroupBytes <= 0 {
		return nil, fmt.Errorf("%w: MaxRowGroupBytes must be greater than 0", ErrInvalidConfig)
	}

	if config.MaxFileSize <= 0 {
		return nil, fmt.Errorf("%w: MaxFileSize must be greater than 0", ErrInvalidConfig)
	}

	if config.MaxBufferedRows <= 0 {
		return nil, fmt.Errorf("%w: MaxBufferedRows must be greater than 0", ErrInvalidConfig)
	}

	if config.MaxBufferedBytes <= 0 {
		return nil, fmt.Errorf("%w: MaxBufferedBytes must be greater than 0", ErrInvalidConfig)
	}

	if config.MaxBufferedTime <= 0 {
		return nil, fmt.Errorf("%w: MaxBufferedTime must be greater than 0", ErrInvalidConfig)
	}

	if config.IngestBufferSize <= 0 {
		return nil, fmt.Errorf("%w: IngestBufferSize must be greater than 0", ErrInvalidConfig)
	}

	if config.BloomFalsePositiveRate <= 0 || config.BloomFalsePositiveRate >= 1 {
		return nil, fmt.Errorf("%w: BloomFalsePositiveRate must be between 0 and 1", ErrInvalidConfig)
	}

	if config.MaxQueryConcurrency <= 0 {
		return nil, fmt.Errorf("%w: MaxQueryConcurrency must be greater than 0", ErrInvalidConfig)
	}

	if config.MaxFilesToMergePerOperation < 2 {
		return nil, fmt.Errorf("%w: MaxFilesToMergePerOperation must be at least 2", ErrInvalidConfig)
	}

	// Normalize the empty compression value at construction so every block is
	// written with an explicit compression type. The read path independently
	// accepts "" as CompressionNone for files written before normalization
	// (see normalizeCompression).
	switch config.RowDataCompression {
	case "":
		config.RowDataCompression = CompressionNone
	case CompressionNone, CompressionSnappy:
	case CompressionZstd:
		if config.ZstdCompressionLevel < 1 || config.ZstdCompressionLevel > 22 {
			return nil, fmt.Errorf("%w: ZstdCompressionLevel must be between 1 and 22", ErrInvalidConfig)
		}
	default:
		return nil, fmt.Errorf("%w: unknown RowDataCompression %q", ErrInvalidConfig, config.RowDataCompression)
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	ctx, cancel := context.WithCancel(context.Background())
	flushCtx, flushCancel := context.WithCancel(context.Background())

	return &BloomSearchEngine{
		config:    config,
		metaStore: metaStore,
		dataStore: dataStore,
		logger:    logger,

		ingestChan:  make(chan *ingestRequest, config.IngestBufferSize),
		flushChan:   make(chan flushRequest, 1), // Buffered flush channel
		ctx:         ctx,
		cancel:      cancel,
		flushCtx:    flushCtx,
		flushCancel: flushCancel,

		querySemaphore: make(chan struct{}, config.MaxQueryConcurrency),
		ingestDone:     make(chan struct{}),
	}, nil
}

// normalizeCompression maps the empty compression value to CompressionNone.
// Files written before construction-time normalization carry "" in block
// metadata (the field marshals with omitempty) and their row data is
// uncompressed, so the read path treats the two identically.
func normalizeCompression(compression CompressionType) CompressionType {
	if compression == "" {
		return CompressionNone
	}
	return compression
}

// Start begins the ingestion and flush workers. Start is idempotent: extra
// calls while running are no-ops, and Start after Stop is a no-op (a stopped
// engine cannot be restarted; construct a new one).
func (b *BloomSearchEngine) Start() {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()

	if b.started || b.stopped {
		return
	}
	b.started = true

	b.wg.Add(2)
	go b.ingestWorker()
	go b.flushWorker()
}

// Stop gracefully shuts down the engine. Ingest requests accepted before Stop
// are drained and flushed: the ingest worker empties ingestChan, flushes any
// buffered rows, and the flush worker finishes every queued flush before
// exiting, so every accepted batch is either made durable or receives an
// error on its done channel. Shutdown flushes run against the DataStore and
// MetaStore with a live context; ctx expiring aborts them (and Stop returns
// the deadline error), which is the only way an in-flight flush is canceled.
//
// Pass a ctx with a deadline (or one that will be canceled): ctx expiry is
// the only abort path, so Stop(context.Background()) waits indefinitely if
// the pipeline is wedged behind the documented done-channel backpressure (an
// abandoned unbuffered doneChan stalls the flush worker, backing up the
// ingest worker and any blocked IngestRows callers Stop must wait for).
func (b *BloomSearchEngine) Stop(ctx context.Context) error {
	// Arm the deadline abort before anything that can block: when ctx
	// expires, in-flight flush store calls, done-channel delivery, and flush
	// enqueueing all abort, which also unwinds any IngestRows caller holding
	// the read lock on a full ingest buffer — so Stop can always honor its
	// deadline. The AfterFunc is dropped on a graceful finish, leaving
	// flushCtx live.
	stopAfter := context.AfterFunc(ctx, b.flushCancel)

	b.stateMu.Lock()
	b.stopped = true
	b.stateMu.Unlock()

	// Signal workers to stop
	b.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Workers finished gracefully
		stopAfter()
		return nil
	case <-ctx.Done():
		// Timeout occurred
		return fmt.Errorf("shutdown timeout exceeded: %w", ctx.Err())
	}
}
