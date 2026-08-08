package bloomsearch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/tidwall/gjson"
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

// dataBlockJob represents a job to process a data block
type dataBlockJob struct {
	filePointer   []byte
	blockMetadata DataBlockMetadata
}

// makeFieldTokenKey creates a key for field-token bloom filter entries
func makeFieldTokenKey(field, token string) string {
	return field + "::" + token
}

// compressionEncoders holds compression-related objects
type compressionEncoders struct {
	writer        io.Writer
	zstdEncoder   *zstd.Encoder
	snappyEncoder *snappy.Writer
}

// createCompressionWriter creates appropriate compression writer based on configuration
func (b *BloomSearchEngine) createCompressionWriter(dest io.Writer) (*compressionEncoders, error) {
	encoders := &compressionEncoders{}

	switch b.config.RowDataCompression {
	case CompressionZstd:
		var err error
		encoders.zstdEncoder, err = zstd.NewWriter(dest, zstd.WithEncoderLevel(zstd.EncoderLevel(b.config.ZstdCompressionLevel)))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
		}
		encoders.writer = encoders.zstdEncoder
	case CompressionSnappy:
		encoders.snappyEncoder = snappy.NewBufferedWriter(dest)
		encoders.writer = encoders.snappyEncoder
	default:
		encoders.writer = dest
	}

	return encoders, nil
}

// finalizeCompression closes any compression encoders and finalizes compression
func (e *compressionEncoders) finalizeCompression() error {
	if e.zstdEncoder != nil {
		if err := e.zstdEncoder.Close(); err != nil {
			return fmt.Errorf("failed to close zstd encoder: %w", err)
		}
	}
	if e.snappyEncoder != nil {
		if err := e.snappyEncoder.Close(); err != nil {
			return fmt.Errorf("failed to close snappy encoder: %w", err)
		}
	}
	return nil
}

// bloomEntrySets accumulates the distinct bloom entries — field paths,
// tokens, and field::token pairs — of a set of rows. Ingest and merge collect
// entries into these sets instead of inserting into filters directly, so that
// filters can be built right-sized from exact distinct counts once the rows
// are known (bloom hashing then happens on the flush/merge path, off the
// ingest actor).
type bloomEntrySets struct {
	fields      map[string]struct{}
	tokens      map[string]struct{}
	fieldTokens map[string]struct{}
}

func newBloomEntrySets() *bloomEntrySets {
	return &bloomEntrySets{
		fields:      make(map[string]struct{}),
		tokens:      make(map[string]struct{}),
		fieldTokens: make(map[string]struct{}),
	}
}

// indexRow walks a marshaled row through the shared walker and records every
// bloom entry it produces: every path (including intermediate object/array
// paths) as a field entry, leaf-value tokens as token entries, and
// exact-leaf-path::token pairs as field-token entries.
func (s *bloomEntrySets) indexRow(rowBytes []byte, tokenizer ValueTokenizerFunc) {
	forEachPathValue(gjson.ParseBytes(rowBytes), ".", func(path string, value gjson.Result, isLeaf bool) {
		s.fields[path] = struct{}{}
		if !isLeaf {
			return
		}
		text, ok := leafTokenInput(value)
		if !ok {
			return
		}
		for _, token := range tokenizer(text) {
			s.tokens[token] = struct{}{}
			s.fieldTokens[makeFieldTokenKey(path, token)] = struct{}{}
		}
	})
}

// unionInto adds every entry to dst.
func (s *bloomEntrySets) unionInto(dst *bloomEntrySets) {
	for entry := range s.fields {
		dst.fields[entry] = struct{}{}
	}
	for entry := range s.tokens {
		dst.tokens[entry] = struct{}{}
	}
	for entry := range s.fieldTokens {
		dst.fieldTokens[entry] = struct{}{}
	}
}

func (s *bloomEntrySets) counts() BloomEntryCounts {
	return BloomEntryCounts{
		Fields:      len(s.fields),
		Tokens:      len(s.tokens),
		FieldTokens: len(s.fieldTokens),
	}
}

// buildFilters builds bloom filters sized for exactly the accumulated entries
// at the given false positive rate.
func (s *bloomEntrySets) buildFilters(falsePositiveRate float64) BloomFilters {
	return BloomFilters{
		FieldBloomFilter:      buildSizedBloomFilter(s.fields, falsePositiveRate),
		TokenBloomFilter:      buildSizedBloomFilter(s.tokens, falsePositiveRate),
		FieldTokenBloomFilter: buildSizedBloomFilter(s.fieldTokens, falsePositiveRate),
	}
}

// buildSizedBloomFilter builds a filter sized for exactly the given entries.
// NewWithEstimates degenerates at n=0 (zero bits, NaN hash count), so an
// empty set is sized for one entry; with nothing inserted it still correctly
// tests negative.
func buildSizedBloomFilter(entries map[string]struct{}, falsePositiveRate float64) *bloom.BloomFilter {
	filter := bloom.NewWithEstimates(uint(max(len(entries), 1)), falsePositiveRate)
	for entry := range entries {
		filter.AddString(entry)
	}
	return filter
}

type PartitionFunc func(row map[string]any) string

type ingestRequest struct {
	rows       []map[string]any
	doneChan   chan error
	forceFlush bool // if true, this is a force flush request
}

func (r *ingestRequest) reset() {
	r.rows = nil
	r.doneChan = nil
	r.forceFlush = false
}

type flushRequest struct {
	partitionBuffers map[string]*partitionBuffer
	doneChans        []chan error
}

type BloomSearchEngine struct {
	config    BloomSearchEngineConfig
	metaStore MetaStore
	dataStore DataStore

	ingestChan  chan *ingestRequest
	flushChan   chan flushRequest
	requestPool *sync.Pool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	ingestDone  chan struct{}

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

	// queryFilePruneHook, when set (tests only), observes the matching files
	// after the file-level bloom test and filter release, before block jobs
	// are enqueued.
	queryFilePruneHook func([]MaybeFile)
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

type MergeStats struct {
	FilesProcessed     int64
	RowGroupsProcessed int64
	RowsProcessed      int64
	BytesProcessed     int64
	Duration           time.Duration
	RowsPerSecond      float64
	BytesPerSecond     float64
}

type BloomSearchEngineConfig struct {
	Tokenizer     ValueTokenizerFunc
	PartitionFunc PartitionFunc

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

type partitionBuffer struct {
	partitionID         string
	rowCount            int
	minMaxIndexes       map[string]MinMaxIndex
	buffer              bytes.Buffer
	entries             *bloomEntrySets
	compressionEncoders *compressionEncoders
	uncompressedSize    int
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

	ctx, cancel := context.WithCancel(context.Background())
	flushCtx, flushCancel := context.WithCancel(context.Background())

	return &BloomSearchEngine{
		config:    config,
		metaStore: metaStore,
		dataStore: dataStore,

		ingestChan: make(chan *ingestRequest, config.IngestBufferSize),
		flushChan:  make(chan flushRequest, 1), // Buffered flush channel
		requestPool: &sync.Pool{
			New: func() interface{} {
				return &ingestRequest{}
			},
		},
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

// IngestRows queues rows for ingestion by the actor. Returns
// ErrEngineStopped once Stop has begun; a nil return means the batch was
// accepted and will either be made durable or receive an error on doneChan.
//
// Done-channel delivery is blocking: provide a buffered channel or actively
// receive from it. An abandoned unbuffered doneChan stalls the flush worker
// (deliberate backpressure) until the engine's Stop deadline aborts delivery.
func (b *BloomSearchEngine) IngestRows(ctx context.Context, rows []map[string]any, doneChan chan error) error {
	b.stateMu.RLock()
	defer b.stateMu.RUnlock()

	if b.stopped {
		return ErrEngineStopped
	}

	req := b.requestPool.Get().(*ingestRequest)
	req.rows = rows
	req.doneChan = doneChan

	// Sending under the read lock means Stop cannot set stopped (and cancel
	// b.ctx) until this send lands, so the shutdown drain always sees it. The
	// ingest worker keeps consuming until b.ctx is canceled, so the send
	// cannot block Stop indefinitely.
	select {
	case b.ingestChan <- req:
		return nil
	case <-ctx.Done():
		req.reset()
		b.requestPool.Put(req)
		return ctx.Err()
	}
}

// Flush forces a flush of any buffered data and waits for it — and for every
// flush queued before it — to complete. With nothing buffered it still waits
// behind all in-flight flush work (all flushes go through one FIFO worker),
// so a nil return means every row ingested before Flush was called is
// durable. Returns ErrEngineStopped once Stop has begun.
func (b *BloomSearchEngine) Flush(ctx context.Context) error {
	b.stateMu.RLock()
	if b.stopped {
		b.stateMu.RUnlock()
		return ErrEngineStopped
	}

	req := b.requestPool.Get().(*ingestRequest)
	req.rows = nil
	req.forceFlush = true
	doneChan := make(chan error, 1)
	req.doneChan = doneChan

	select {
	case b.ingestChan <- req:
		b.stateMu.RUnlock()
		// Wait for flush to complete (once committed, let it finish)
		return <-doneChan
	case <-ctx.Done():
		b.stateMu.RUnlock()
		req.reset()
		b.requestPool.Put(req)
		return ctx.Err()
	}
}

func (b *BloomSearchEngine) ingestWorker() {
	defer func() {
		close(b.ingestDone)
		b.wg.Done()
	}()

	// Local state owned by the ingestion actor
	partitionBuffers := make(map[string]*partitionBuffer)
	doneChans := make([]chan error, 0)
	bufferedRowCount := 0
	bufferedBytes := 0
	var bufferStartTime time.Time

	// Create a ticker for periodic time-based flush checks
	ticker := time.NewTicker(100 * time.Millisecond) // Check every 100ms
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			fmt.Println("ingestWorker context done")
			// Stop set the stopped flag before canceling b.ctx, so ingestChan
			// can no longer receive new requests: draining until empty
			// processes every accepted batch. Requests are processed with the
			// flush context — b.ctx is already canceled and using it would
			// fail the shutdown flush and drop done-channel delivery.
			for {
				select {
				case req := <-b.ingestChan:
					b.processIngestRequest(
						b.flushCtx,
						req,
						partitionBuffers,
						&doneChans,
						&bufferedRowCount,
						&bufferedBytes,
						&bufferStartTime,
					)
				default:
					// Flush any remaining buffered data (and ack any
					// remaining waiters) before exiting.
					b.flushBufferedData(
						partitionBuffers,
						&doneChans,
						&bufferedRowCount,
						&bufferedBytes,
						&bufferStartTime,
					)
					return
				}
			}
		case req := <-b.ingestChan:
			// Process the batch of rows
			b.processIngestRequest(
				b.flushCtx,
				req,
				partitionBuffers,
				&doneChans,
				&bufferedRowCount,
				&bufferedBytes,
				&bufferStartTime,
			)
		case <-ticker.C:
			// Check for time-based flush
			if bufferedRowCount > 0 && !bufferStartTime.IsZero() && time.Since(bufferStartTime) >= b.config.MaxBufferedTime {
				b.flushBufferedData(
					partitionBuffers,
					&doneChans,
					&bufferedRowCount,
					&bufferedBytes,
					&bufferStartTime,
				)
			}
		}
	}
}

// flushBufferedData flushes the current buffered data and resets the buffer
// state. With no buffered partitions but pending done channels it still
// enqueues an ack-only flush request, so waiters are acked in FIFO order
// behind every flush already queued or in flight.
func (b *BloomSearchEngine) flushBufferedData(
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
) {
	if len(partitionBuffers) == 0 && len(*doneChans) == 0 {
		return
	}

	// Copy data before sending to flush worker
	partitionBuffersCopy := make(map[string]*partitionBuffer)
	for k, v := range partitionBuffers {
		partitionBuffersCopy[k] = v
	}
	doneChannsCopy := make([]chan error, len(*doneChans))
	copy(doneChannsCopy, *doneChans)

	b.triggerFlush(partitionBuffersCopy, doneChannsCopy)

	// Reset local state
	for k := range partitionBuffers {
		delete(partitionBuffers, k)
	}
	*doneChans = make([]chan error, 0)
	*bufferedRowCount = 0
	*bufferedBytes = 0
	*bufferStartTime = time.Time{}
}

func (b *BloomSearchEngine) processIngestRequest(
	ctx context.Context,
	req *ingestRequest,
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
) {
	// Process the request and return to pool at the end
	defer func() {
		req.reset()
		b.requestPool.Put(req)
	}()

	// If this is a force flush request, route it through the flush FIFO even
	// with nothing buffered: flushBufferedData enqueues an ack-only flush
	// request in that case, so the ack is ordered behind all in-flight flush
	// work and Flush never returns before earlier rows are durable.
	if req.forceFlush {
		*doneChans = append(*doneChans, req.doneChan)
		b.flushBufferedData(
			partitionBuffers,
			doneChans,
			bufferedRowCount,
			bufferedBytes,
			bufferStartTime,
		)
		return
	}

	// An empty batch has nothing to make durable: ack immediately and leave
	// the buffers untouched (no empty partition buffer, no 0-row block).
	if len(req.rows) == 0 {
		SendOptionalWithContext(ctx, req.doneChan, nil)
		return
	}

	// Group rows by partition ID
	partitionedRows := make(map[string][]map[string]any)
	if b.config.PartitionFunc != nil {
		for _, row := range req.rows {
			partitionID := b.config.PartitionFunc(row)
			partitionedRows[partitionID] = append(partitionedRows[partitionID], row)
		}
	} else {
		partitionedRows[""] = req.rows
	}

	// Serialize and validate every row before mutating any partition buffer or
	// bloom filter, so a mid-batch error rejects the whole batch and leaves
	// the buffered state exactly as it was. Indexing walks the marshaled JSON
	// bytes, the same canonical representation query-time row verification
	// walks (see forEachPathValue in tokenizer.go).
	partitionedRowBytes := make(map[string][][]byte, len(partitionedRows))
	for partitionID, rows := range partitionedRows {
		rowBytesList := make([][]byte, len(rows))
		for i, row := range rows {
			rowBytes, err := json.Marshal(row)
			if err != nil {
				SendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to serialize row: %w", err))
				return
			}

			// Check if row is too large for uint32 length prefix
			if len(rowBytes) > 0xFFFFFFFF {
				SendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("row too large: %d bytes exceeds maximum of %d bytes", len(rowBytes), 0xFFFFFFFF))
				return
			}

			rowBytesList[i] = rowBytes
		}
		partitionedRowBytes[partitionID] = rowBytesList
	}

	// Create partition buffers if they don't exist. The compression encoder is
	// created before the buffer is registered, and buffers created for this
	// batch are removed on failure — a half-constructed entry (nil encoder)
	// must never persist, or the next ingest for that partition panics.
	createdPartitions := make([]string, 0)
	for partitionID := range partitionedRows {
		if partitionBuffers[partitionID] == nil {
			newBuffer := &partitionBuffer{
				partitionID:      partitionID,
				minMaxIndexes:    make(map[string]MinMaxIndex),
				buffer:           bytes.Buffer{},
				entries:          newBloomEntrySets(),
				uncompressedSize: 0,
				rowCount:         0,
			}
			encoders, err := b.createCompressionWriter(&newBuffer.buffer)
			if err != nil {
				for _, createdID := range createdPartitions {
					delete(partitionBuffers, createdID)
				}
				SendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to create compression writer: %w", err))
				return
			}
			newBuffer.compressionEncoders = encoders
			partitionBuffers[partitionID] = newBuffer
			createdPartitions = append(createdPartitions, partitionID)
		}
	}

	// If we haven't saved the buffer start time yet, do it
	if bufferStartTime.IsZero() {
		*bufferStartTime = time.Now()
	}

	// Track if we should flush
	shouldFlush := false

	// Process each partition
	for partitionID, rows := range partitionedRows {
		partitionBuffer := partitionBuffers[partitionID]
		rowBytesList := partitionedRowBytes[partitionID]

		// Process each row
		for i, row := range rows {
			rowBytes := rowBytesList[i]

			// Record the row's bloom entries in the partition's dedup sets.
			// Filters are built from these sets at flush time, sized for the
			// exact distinct counts (see handleFlush).
			partitionBuffer.entries.indexRow(rowBytes, b.config.Tokenizer)

			// Check for minmax indexes, reading Go-native values from the
			// original row so numeric identity is preserved (conversions clamp
			// out-of-range values, see min_max.go)
			for _, index := range b.config.MinMaxIndexes {
				if value, ok := row[index]; ok {
					minVal, maxVal, isNumeric := ConvertToMinMaxInt64(value)
					if !isNumeric {
						continue
					}

					if existingIndex, exists := partitionBuffer.minMaxIndexes[index]; exists {
						partitionBuffer.minMaxIndexes[index] = UpdateMinMaxIndex(existingIndex, minVal, maxVal)
					} else {
						partitionBuffer.minMaxIndexes[index] = MinMaxIndex{
							Min: minVal,
							Max: maxVal,
						}
					}
				}
			}

			// Write length prefix (uint32) followed by row bytes. The
			// destination is an in-memory buffer, so failures here are
			// exceptional; they are still reported rather than swallowed.
			lengthBytes := make([]byte, LengthPrefixSize)
			binary.LittleEndian.PutUint32(lengthBytes, uint32(len(rowBytes)))
			if _, err := partitionBuffer.compressionEncoders.writer.Write(lengthBytes); err != nil {
				SendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to buffer row length: %w", err))
				return
			}
			if _, err := partitionBuffer.compressionEncoders.writer.Write(rowBytes); err != nil {
				SendOptionalWithContext(ctx, req.doneChan, fmt.Errorf("failed to buffer row data: %w", err))
				return
			}

			// Increment stats
			partitionBuffer.uncompressedSize += len(rowBytes) + LengthPrefixSize
			partitionBuffer.rowCount += 1

			// Use uncompressed size for flush decisions since compression may buffer data
			uncompressedRowSize := len(rowBytes) + LengthPrefixSize
			*bufferedBytes += uncompressedRowSize
			*bufferedRowCount += 1
		}

		// Check partition-level limits
		if !shouldFlush {
			partitionUncompressedBytes := partitionBuffer.uncompressedSize

			if partitionBuffer.rowCount >= b.config.MaxRowGroupRows {
				fmt.Printf("FLUSH TRIGGER: Partition '%s' hit max rows (%d >= %d)\n",
					partitionBuffer.partitionID, partitionBuffer.rowCount, b.config.MaxRowGroupRows)
				shouldFlush = true
			} else if partitionUncompressedBytes >= b.config.MaxRowGroupBytes {
				fmt.Printf("FLUSH TRIGGER: Partition '%s' hit max uncompressed bytes (%d >= %d)\n",
					partitionBuffer.partitionID, partitionUncompressedBytes, b.config.MaxRowGroupBytes)
				shouldFlush = true
			}
		}
	}

	// If we haven't decided to flush based on partition limits, check buffer-level limits
	if !shouldFlush {
		if *bufferedRowCount >= b.config.MaxBufferedRows {
			fmt.Printf("FLUSH TRIGGER: Buffer hit max rows (%d >= %d)\n",
				*bufferedRowCount, b.config.MaxBufferedRows)
			shouldFlush = true
		}

		if !shouldFlush && *bufferedBytes >= b.config.MaxBufferedBytes {
			fmt.Printf("FLUSH TRIGGER: Buffer hit max bytes (%d >= %d)\n",
				*bufferedBytes, b.config.MaxBufferedBytes)
			shouldFlush = true
		}

		if !shouldFlush && time.Since(*bufferStartTime) >= b.config.MaxBufferedTime {
			fmt.Printf("FLUSH TRIGGER: Buffer hit max time (%v >= %v)\n",
				time.Since(*bufferStartTime), b.config.MaxBufferedTime)
			shouldFlush = true
		}
	}

	// Store the doneChan
	*doneChans = append(*doneChans, req.doneChan)

	// Trigger flush if needed
	if shouldFlush {
		// Log flush details
		fmt.Printf("FLUSH STARTING: %d partitions, %d total rows, %d total bytes\n",
			len(partitionBuffers), *bufferedRowCount, *bufferedBytes)
		for partitionID, partition := range partitionBuffers {
			fmt.Printf("  Partition '%s': %d rows, %d bytes\n",
				partitionID, partition.rowCount, partition.buffer.Len())
		}
		b.flushBufferedData(
			partitionBuffers,
			doneChans,
			bufferedRowCount,
			bufferedBytes,
			bufferStartTime,
		)
	}
}

// triggerFlush enqueues a flush request for the flush worker. The send
// blocks: the ingest actor is the only producer and the flush worker the only
// consumer, so flushes execute strictly FIFO — Flush acks cannot overtake
// flushes queued before them — and flushing never runs inline on the ingest
// actor. A full channel simply applies backpressure to ingest. If the
// shutdown deadline expires while the flush worker is wedged, waiters are
// told (best effort) instead of being dropped silently.
func (b *BloomSearchEngine) triggerFlush(partitionBuffers map[string]*partitionBuffer, doneChans []chan error) {
	flushReq := flushRequest{
		partitionBuffers: partitionBuffers,
		doneChans:        doneChans,
	}

	select {
	case b.flushChan <- flushReq:
		// Successfully queued for flush
	case <-b.flushCtx.Done():
		// Shutdown deadline expired: the flush worker will not take this
		// request. Deliver the failure to ready waiters; flushCtx is already
		// canceled so blocked channels are given up immediately.
		SendToChannelsWithContext(b.flushCtx, doneChans, fmt.Errorf("flush abandoned: %w", b.flushCtx.Err()))
	}
}

func (b *BloomSearchEngine) flushWorker() {
	defer b.wg.Done()

	shuttingDown := false
	for {
		if !shuttingDown {
			select {
			case <-b.ctx.Done():
				shuttingDown = true
			case flushReq := <-b.flushChan:
				b.handleFlush(b.flushCtx, flushReq)
			}
			continue
		}

		select {
		case flushReq := <-b.flushChan:
			b.handleFlush(b.flushCtx, flushReq)
		case <-b.ingestDone:
			// The ingest worker has exited, so every flush request it will
			// ever produce is already in the channel; drain them all.
			for {
				select {
				case flushReq := <-b.flushChan:
					b.handleFlush(b.flushCtx, flushReq)
				default:
					fmt.Println("flushWorker context done")
					return
				}
			}
		}
	}
}

// abortFileWriter discards a partially written file so it can never become
// visible: writers implementing Abort discard without publishing (Close on a
// rename-on-close writer would publish a corrupt file); other writers are
// closed unless a Close was already attempted. The pointer is then
// tombstoned so the store forgets whatever was reserved for it. Cleanup is
// best effort — the caller's original error is what gets reported.
func (b *BloomSearchEngine) abortFileWriter(ctx context.Context, writer io.WriteCloser, filePointerBytes []byte, closeAttempted bool) {
	if aborter, ok := writer.(interface{ Abort() error }); ok {
		aborter.Abort()
	} else if !closeAttempted {
		writer.Close()
	}
	b.dataStore.TombstoneFile(ctx, filePointerBytes)
}

// handleFlush writes one file from a flush request and acks its done
// channels. A request without partition buffers is ack-only: it exists purely
// to order a Flush ack behind earlier flush work. On any failure after
// CreateFile the writer is aborted and the pointer tombstoned before the
// error is delivered, so no partial file stays visible or leaks a handle.
func (b *BloomSearchEngine) handleFlush(ctx context.Context, flushReq flushRequest) {
	// Once the shutdown deadline has aborted flush work, queued requests must
	// not start any store work: a ctx-ignoring store would happily keep
	// creating files after Stop already returned. Report the abandonment to
	// every waiter instead (best effort — ctx is already canceled, so only
	// ready channels receive it).
	if err := ctx.Err(); err != nil {
		SendToChannelsWithContext(ctx, flushReq.doneChans, fmt.Errorf("flush abandoned: %w", err))
		return
	}

	if len(flushReq.partitionBuffers) == 0 {
		SendToChannelsWithContext(ctx, flushReq.doneChans, nil)
		return
	}

	fileMetadata := FileMetadata{
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		DataBlocks:             make([]DataBlockMetadata, 0),
	}

	// Stream write to data store
	writer, filePointerBytes, err := b.dataStore.CreateFile(ctx)
	if err != nil {
		fmt.Println("failed to create file: %w", err)
		// Write error to all done channels
		SendToChannelsWithContext(ctx, flushReq.doneChans, fmt.Errorf("failed to create file: %w", err))
		return
	}

	// fail aborts the partial file and reports err to every waiter.
	fail := func(err error, closeAttempted bool) {
		b.abortFileWriter(ctx, writer, filePointerBytes, closeAttempted)
		SendToChannelsWithContext(ctx, flushReq.doneChans, err)
	}

	currentOffset := 0

	// The file-level filters are built from the union of the blocks' entry
	// sets, so they too are sized for exact distinct counts.
	fileEntries := newBloomEntrySets()

	// For each partition buffer, write the data block to the data store
	for _, partitionBuffer := range flushReq.partitionBuffers {
		// Finalize compression encoders before writing
		var compressedData []byte
		if err := partitionBuffer.compressionEncoders.finalizeCompression(); err != nil {
			fail(fmt.Errorf("failed to finalize compression: %w", err), false)
			return
		}
		compressedData = partitionBuffer.buffer.Bytes()

		// Build the block's filters right-sized from the measured distinct
		// entry counts and write them as the block's filter section.
		blockFilters := partitionBuffer.entries.buildFilters(b.config.BloomFalsePositiveRate)
		filterSection, err := encodeFilterSection(&blockFilters)
		if err != nil {
			fail(fmt.Errorf("failed to encode bloom filters: %w", err), false)
			return
		}
		if _, err := writer.Write(filterSection); err != nil {
			fail(fmt.Errorf("failed to write bloom filters: %w", err), false)
			return
		}

		// Calculate hash of compressed row data (CRC32C)
		rowDataHash := crc32.Checksum(compressedData, crc32cTable)

		// Write the row data buffer
		if _, err := writer.Write(compressedData); err != nil {
			fail(fmt.Errorf("failed to write data block: %w", err), false)
			return
		}

		partitionBuffer.entries.unionInto(fileEntries)

		dataBlockSize := len(filterSection) + len(compressedData)

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:            partitionBuffer.partitionID,
			Rows:                   partitionBuffer.rowCount,
			Offset:                 currentOffset,
			Size:                   dataBlockSize,
			BloomFiltersSize:       len(filterSection),
			MinMaxIndexes:          partitionBuffer.minMaxIndexes,
			Compression:            b.config.RowDataCompression,
			UncompressedSize:       partitionBuffer.uncompressedSize,
			RowDataHash:            rowDataHash,
			HasRowDataHash:         true,
			BloomEntryCounts:       partitionBuffer.entries.counts(),
			BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		})

		currentOffset += dataBlockSize
	}

	fileMetadata.BloomFilters = fileEntries.buildFilters(b.config.BloomFalsePositiveRate)
	fileMetadata.BloomEntryCounts = fileEntries.counts()

	// Write final metadata to data store and footer
	if err := b.writeFileMetadataAndFooter(writer, &fileMetadata); err != nil {
		fail(fmt.Errorf("failed to write file metadata and footer: %w", err), false)
		return
	}

	if err := writer.Close(); err != nil {
		fail(fmt.Errorf("failed to close file writer: %w", err), true)
		return
	}

	if err := b.metaStore.Update(ctx, []WriteOperation{
		{
			FileMetadata:     &fileMetadata,
			FilePointerBytes: filePointerBytes,
		},
	}, nil); err != nil {
		// The file is fully written and published but never became
		// referenced; tombstone the orphan. The writer is already closed, so
		// no abort.
		b.dataStore.TombstoneFile(ctx, filePointerBytes)
		SendToChannelsWithContext(ctx, flushReq.doneChans, fmt.Errorf("failed to store file metadata: %w", err))
		return
	}

	SendToChannelsWithContext(ctx, flushReq.doneChans, nil)
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
			fmt.Println("[WARNING] fieldFilter is nil, how did your file not have this filter?")
			return true
		}
		result = fieldFilter.TestString(condition.Field)
	case BloomToken:
		if tokenFilter == nil {
			fmt.Println("[WARNING] tokenFilter is nil, how did your file not have this filter?")
			return true
		}
		result = tokenFilter.TestString(condition.Token)
	case BloomFieldToken:
		if fieldTokenFilter == nil {
			fmt.Println("[WARNING] fieldTokenFilter is nil, how did your file not have this filter?")
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

	compiledRegexQuery, err := CompileRegexQuery(query.Regex)
	if err != nil {
		return nil, fmt.Errorf("failed to compile regex query: %w", err)
	}

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

				if err := SendWithContext(ctx, b.querySemaphore, struct{}{}); err != nil {
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
					SendWithContext(ctx, matchingFilesChan, maybeFile)
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
			for job := range jobs {
				if !slot.acquire() {
					return
				}
				b.processDataBlock(r, &slot, job, rowBloomQuery, pruneBloomQuery, compiledRegexQuery)
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
				if err := SendWithContext(r.ctx, jobs, job); err != nil {
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
	rowBloomQuery *BloomQuery,
	pruneBloomQuery *BloomQuery,
	regexQuery *compiledRegexQuery,
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
	rowData, err := readBlockRowData(file, &job.blockMetadata)
	if err != nil {
		fail(fmt.Errorf("failed to read block row data: %w", err))
		return
	}

	scanner := blockRowScanner{data: rowData}
	for {
		// Cancellation is terminal for the query; stop scanning promptly.
		if ctx.Err() != nil {
			return
		}

		rowBytes, ok, err := scanner.next()
		if err != nil {
			fail(fmt.Errorf("failed to read row: %w", err))
			return
		}
		if !ok {
			break // End of data
		}

		rowsScanned++
		bytesScanned += int64(LengthPrefixSize) + int64(len(rowBytes))

		rowValue := gjson.ParseBytes(rowBytes)
		if !TestGJSONForQuery(rowValue, rowBloomQuery, regexQuery, ".", b.config.Tokenizer) {
			continue
		}

		// Materialize the row from the same gjson parse used for matching —
		// one parse per row. gjson materializes JSON numbers as float64,
		// matching encoding/json.
		row, isObject := rowValue.Value().(map[string]any)
		if !isObject {
			fail(fmt.Errorf("row is not a JSON object"))
			return
		}

		if err := r.deliver(slot, row); err != nil {
			return
		}
	}
}

// Merge executes file merging to optimize storage and query performance.
// Merge is single-flight per engine: a call while another Merge is running
// returns ErrMergeInProgress (concurrent merges would duplicate every merged
// row). A return of non-nil stats WITH an error wrapping
// ErrPostCommitCleanup means the merge committed — state is consistent, only
// unreferenced source files linger in the DataStore.
func (b *BloomSearchEngine) Merge(ctx context.Context) (*MergeStats, error) {
	if !b.mergeMu.TryLock() {
		return nil, ErrMergeInProgress
	}
	defer b.mergeMu.Unlock()
	return b.merge(ctx)
}

// merge will evaluate and merge data files to optimize query performance.
func (b *BloomSearchEngine) merge(ctx context.Context) (*MergeStats, error) {
	mergeStartTime := time.Now()

	// Get all files for evaluation
	maybeFiles, err := b.metaStore.GetMaybeFilesForQuery(ctx, nil)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Found %d files for merge evaluation\n", len(maybeFiles))

	// Convert to merge candidates with file-level statistics
	var mergeCandidates []fileMergeCandidate
	for _, maybeFile := range maybeFiles {
		candidate := fileMergeCandidate{
			filePointer: maybeFile.PointerBytes,
			metadata:    maybeFile.Metadata,
			statistics:  b.calculateFileStatistics(maybeFile.Metadata),
		}
		mergeCandidates = append(mergeCandidates, candidate)
	}

	// Group files into merge groups (this handles sorting internally)
	mergeGroups := b.identifyFileMergeGroups(mergeCandidates)

	fmt.Println("\nFILE MERGE GROUPS:")
	mergeGroupCount := 0
	totalMergeFiles := 0

	for _, group := range mergeGroups {
		totalSize := 0
		totalRows := 0
		totalBlocks := 0
		allPartitions := make(map[string]bool)

		for _, candidate := range group {
			totalSize += candidate.statistics.totalSize
			totalRows += candidate.statistics.totalRows
			totalBlocks += candidate.statistics.blockCount
			for _, partitionID := range candidate.statistics.partitionIDs {
				allPartitions[partitionID] = true
			}
		}

		partitionList := make([]string, 0, len(allPartitions))
		for partitionID := range allPartitions {
			partitionList = append(partitionList, partitionID)
		}

		fmt.Printf("  Group %d: %d files, Partitions: %v, TotalSize: %d, TotalRows: %d, TotalBlocks: %d\n",
			mergeGroupCount, len(group), partitionList, totalSize, totalRows, totalBlocks)
		mergeGroupCount++
		totalMergeFiles += len(group)
	}

	// Show remaining single files that won't be merged
	singleFileCount := 0
	for _, candidate := range mergeCandidates {
		// Check if this file is already in a merge group
		inMergeGroup := false
		for _, group := range mergeGroups {
			for _, groupFile := range group {
				if string(candidate.filePointer) == string(groupFile.filePointer) {
					inMergeGroup = true
					break
				}
			}
			if inMergeGroup {
				break
			}
		}

		if !inMergeGroup {
			fmt.Printf("  Single file %d: Partitions: %v, TotalSize: %d, TotalRows: %d, TotalBlocks: %d\n",
				singleFileCount, candidate.statistics.partitionIDs, candidate.statistics.totalSize,
				candidate.statistics.totalRows, candidate.statistics.blockCount)
			singleFileCount++
		}
	}

	fmt.Printf("\nSUMMARY: %d merge groups (%d files), %d single files, %d total files\n",
		len(mergeGroups), totalMergeFiles, singleFileCount, len(mergeCandidates))

	// Calculate statistics for files that will be merged
	var totalFilesProcessed int64
	var totalRowGroupsProcessed int64
	var totalRowsProcessed int64
	var totalBytesProcessed int64

	for _, group := range mergeGroups {
		for _, candidate := range group {
			totalFilesProcessed++
			totalRowGroupsProcessed += int64(len(candidate.metadata.DataBlocks))
			for _, block := range candidate.metadata.DataBlocks {
				totalRowsProcessed += int64(block.Rows)
				totalBytesProcessed += int64(block.Size)
			}
		}
	}

	// Execute merges for each group
	var writeOps []WriteOperation
	var deleteOps []DeleteOperation

	for groupIndex, group := range mergeGroups {
		fmt.Printf("Merging group %d with %d files...\n", groupIndex, len(group))

		newFilePointer, newFileMetadata, err := b.executeMergeGroup(ctx, group)
		if err != nil {
			// Outputs from groups that already completed were published but
			// never referenced by the MetaStore; tombstone the orphans.
			// Sources are untouched — they are only tombstoned after a
			// successful MetaStore.Update.
			for _, writeOp := range writeOps {
				b.dataStore.TombstoneFile(ctx, writeOp.FilePointerBytes)
			}
			return nil, fmt.Errorf("failed to merge group %d: %w", groupIndex, err)
		}

		// Add new file to write operations
		writeOps = append(writeOps, WriteOperation{
			FileMetadata:     newFileMetadata,
			FilePointerBytes: newFilePointer,
		})

		// Add old files to delete operations
		for _, candidate := range group {
			deleteOps = append(deleteOps, DeleteOperation{
				FilePointerBytes: candidate.filePointer,
			})
		}

		fmt.Printf("Successfully merged group %d into new file\n", groupIndex)
	}

	// Update metastore: add new files and remove old ones
	var postCommitCleanupErr error
	if len(writeOps) > 0 {
		fmt.Printf("Updating metastore: adding %d new files, removing %d old files\n", len(writeOps), len(deleteOps))
		if err := b.metaStore.Update(ctx, writeOps, deleteOps); err != nil {
			// Nothing committed: the merge outputs were published but never
			// referenced, so tombstone them. Sources stay referenced and
			// untouched.
			for _, writeOp := range writeOps {
				b.dataStore.TombstoneFile(ctx, writeOp.FilePointerBytes)
			}
			return nil, fmt.Errorf("failed to update metastore after merge: %w", err)
		}
		fmt.Printf("Metastore update completed successfully\n")

		// The merge is committed from here on. Tombstone failures are
		// garbage-collection failures, not merge failures: report them via
		// ErrPostCommitCleanup alongside the stats.
		var tombstoneErrs []error
		for _, deleteOp := range deleteOps {
			if err := b.dataStore.TombstoneFile(ctx, deleteOp.FilePointerBytes); err != nil {
				tombstoneErrs = append(tombstoneErrs, err)
			}
		}
		if len(tombstoneErrs) > 0 {
			postCommitCleanupErr = fmt.Errorf("%w: %w", ErrPostCommitCleanup, errors.Join(tombstoneErrs...))
		}
	}

	// Calculate final statistics
	duration := time.Since(mergeStartTime)
	stats := &MergeStats{
		FilesProcessed:     totalFilesProcessed,
		RowGroupsProcessed: totalRowGroupsProcessed,
		RowsProcessed:      totalRowsProcessed,
		BytesProcessed:     totalBytesProcessed,
		Duration:           duration,
	}

	// Calculate rates
	if duration.Seconds() > 0 {
		stats.RowsPerSecond = float64(totalRowsProcessed) / duration.Seconds()
		stats.BytesPerSecond = float64(totalBytesProcessed) / duration.Seconds()
	}

	return stats, postCommitCleanupErr
}

// dataBlocksAreMergeable checks if two data blocks can be merged together.
// Bloom filter parameters impose no constraint: merged blocks rebuild their
// filters from row data, sized for the merged rows' measured entry counts.
func (b *BloomSearchEngine) dataBlocksAreMergeable(block1, block2 DataBlockMetadata) bool {
	// Must have the same partition ID
	if block1.PartitionID != block2.PartitionID {
		return false
	}

	// Blocks must index the same minmax key set. Merging a block that lacks a
	// key with one that has it would give the keyless block's rows the other
	// block's range — widening strict-prefilter visibility to rows whose
	// block never indexed the key. Such blocks are copied as-is instead.
	if len(block1.MinMaxIndexes) != len(block2.MinMaxIndexes) {
		return false
	}
	for key := range block1.MinMaxIndexes {
		if _, exists := block2.MinMaxIndexes[key]; !exists {
			return false
		}
	}

	// Check if merging would exceed size limits
	combinedRows := block1.Rows + block2.Rows
	combinedUncompressedSize := block1.UncompressedSize + block2.UncompressedSize

	if combinedRows > b.config.MaxRowGroupRows {
		return false
	}

	if combinedUncompressedSize > b.config.MaxRowGroupBytes {
		return false
	}

	return true
}

// mergeMinMaxIndexes merges minmax indexes from two data blocks
func (b *BloomSearchEngine) mergeMinMaxIndexes(indexes1, indexes2 map[string]MinMaxIndex) map[string]MinMaxIndex {
	merged := make(map[string]MinMaxIndex)

	// Copy all indexes from first block
	for key, index := range indexes1 {
		merged[key] = index
	}

	// Merge indexes from second block
	for key, index2 := range indexes2 {
		if index1, exists := merged[key]; exists {
			// Merge the indexes
			merged[key] = UpdateMinMaxIndex(index1, index2.Min, index2.Max)
		} else {
			// Add new index
			merged[key] = index2
		}
	}

	return merged
}

// fileMergeCandidate represents a file that could be merged
type fileMergeCandidate struct {
	filePointer []byte
	metadata    FileMetadata
	statistics  fileStatistics
}

// fileStatistics contains pre-calculated statistics about a file
type fileStatistics struct {
	partitionIDs []string
	totalSize    int
	totalRows    int
	blockCount   int
}

// calculateFileStatistics computes basic statistics for a file
func (b *BloomSearchEngine) calculateFileStatistics(metadata FileMetadata) fileStatistics {
	stats := fileStatistics{
		partitionIDs: make([]string, 0),
	}

	partitionSet := make(map[string]bool)

	for _, block := range metadata.DataBlocks {
		// Track unique partitions
		if !partitionSet[block.PartitionID] {
			partitionSet[block.PartitionID] = true
			stats.partitionIDs = append(stats.partitionIDs, block.PartitionID)
		}

		// Accumulate totals
		stats.totalSize += block.Size
		stats.totalRows += block.Rows
		stats.blockCount++
	}

	// Sort partition IDs for consistent ordering
	sort.Strings(stats.partitionIDs)

	return stats
}

// identifyFileMergeGroups groups files that should be merged together using
// smart row group merging. Bloom filter parameters play no role in grouping:
// merged blocks rebuild their filters from row data, so any two files with
// mergeable row groups are candidates (with measured filter sizing, files
// essentially never share filter parameters).
func (b *BloomSearchEngine) identifyFileMergeGroups(files []fileMergeCandidate) [][]fileMergeCandidate {
	if len(files) < 2 {
		return nil
	}

	// Sort files by potential for merging (smaller files first, then by partition locality)
	candidates := append([]fileMergeCandidate(nil), files...)
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]

		// Primary: Prefer files with smaller average block sizes (more opportunity for merging)
		aAvgBlockSize := a.statistics.totalSize / max(a.statistics.blockCount, 1)
		bAvgBlockSize := b.statistics.totalSize / max(b.statistics.blockCount, 1)

		if aAvgBlockSize != bAvgBlockSize {
			return aAvgBlockSize < bAvgBlockSize
		}

		// Secondary: Sort by total size (smaller first)
		return a.statistics.totalSize < b.statistics.totalSize
	})

	var mergeGroups [][]fileMergeCandidate
	totalFilesInGroups := 0

	// Track which files have already been assigned to a group
	fileAssigned := make(map[int]bool)

	// Greedy approach: try to group files that can benefit from row group merging
	for i, file := range candidates {
		if fileAssigned[i] {
			continue
		}

		if totalFilesInGroups >= b.config.MaxFilesToMergePerOperation {
			break
		}

		currentGroup := []fileMergeCandidate{file}
		currentGroupSize := file.statistics.totalSize
		fileAssigned[i] = true

		// Add compatible files to this group
		for j := i + 1; j < len(candidates); j++ {
			if fileAssigned[j] {
				continue
			}

			if totalFilesInGroups+len(currentGroup)+1 > b.config.MaxFilesToMergePerOperation {
				break
			}

			candidate := candidates[j]

			newSize := currentGroupSize + candidate.statistics.totalSize
			if newSize > b.config.MaxFileSize {
				continue
			}

			if b.hasCompatibleRowGroups(currentGroup, candidate) {
				currentGroup = append(currentGroup, candidate)
				currentGroupSize = newSize
				fileAssigned[j] = true
			}
		}

		if len(currentGroup) > 1 {
			mergeGroups = append(mergeGroups, currentGroup)
			totalFilesInGroups += len(currentGroup)
		}
	}

	return mergeGroups
}

// hasCompatibleRowGroups checks if a candidate file has row groups that can be merged with existing group
func (b *BloomSearchEngine) hasCompatibleRowGroups(currentGroup []fileMergeCandidate, candidate fileMergeCandidate) bool {
	// Check if candidate has row groups that can be merged with any in the current group
	for _, groupFile := range currentGroup {
		for _, candidateBlock := range candidate.metadata.DataBlocks {
			for _, groupBlock := range groupFile.metadata.DataBlocks {
				if b.dataBlocksAreMergeable(candidateBlock, groupBlock) {
					return true
				}
			}
		}
	}
	return false
}

// executeMergeGroup merges a group of files with smart row group merging.
//
// The output file's filters are rebuilt from measured entries rather than
// OR-merged from the sources: merged blocks contribute the entry sets
// collected while their rows stream through (see mergeDataBlocks), and
// raw-copied blocks — whose bytes are copied verbatim — are re-streamed
// purely for entry collection (see copyDataBlock). Rebuilding keeps the
// merged file's filters right-sized instead of unioning possibly saturated
// or differently-sized source filters.
func (b *BloomSearchEngine) executeMergeGroup(ctx context.Context, group []fileMergeCandidate) ([]byte, *FileMetadata, error) {
	// Create new file for writing
	writer, filePointerBytes, err := b.dataStore.CreateFile(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create merge file: %w", err)
	}

	// fail aborts the partial output (never publishing it) and tombstones its
	// pointer; the sources are untouched because they are only tombstoned
	// after a successful MetaStore.Update in merge().
	fail := func(err error, closeAttempted bool) ([]byte, *FileMetadata, error) {
		b.abortFileWriter(ctx, writer, filePointerBytes, closeAttempted)
		return nil, nil, err
	}

	var newDataBlocks []DataBlockMetadata
	currentOffset := 0

	// File-level entries accumulate across every block of the output file.
	fileEntries := newBloomEntrySets()

	// Collect all data blocks from all files with their file pointers. Block
	// reads open their own handle per block, so same-file blocks never
	// interleave reads on one seek position.
	var allBlocks []blockWithFile
	for _, candidate := range group {
		for _, blockMetadata := range candidate.metadata.DataBlocks {
			allBlocks = append(allBlocks, blockWithFile{
				block:       blockMetadata,
				filePointer: candidate.filePointer,
			})
		}
	}

	// Group blocks by partition for potential merging
	partitionBlocks := make(map[string][]int) // partition -> indices into allBlocks
	for i, block := range allBlocks {
		partitionBlocks[block.block.PartitionID] = append(partitionBlocks[block.block.PartitionID], i)
	}

	// Process each partition
	for partitionID, blockIndices := range partitionBlocks {
		err := b.processPartitionBlocks(ctx, writer, allBlocks, blockIndices, partitionID, &currentOffset, &newDataBlocks, fileEntries)
		if err != nil {
			return fail(fmt.Errorf("failed to process partition %s: %w", partitionID, err), false)
		}
	}

	// The metadata describes what was actually built: filters sized from the
	// measured entry counts at the engine's configured false positive rate.
	newFileMetadata := &FileMetadata{
		BloomFilters:           fileEntries.buildFilters(b.config.BloomFalsePositiveRate),
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		BloomEntryCounts:       fileEntries.counts(),
		DataBlocks:             newDataBlocks,
	}

	// Write file metadata and footer
	if err := b.writeFileMetadataAndFooter(writer, newFileMetadata); err != nil {
		return fail(fmt.Errorf("failed to write file metadata: %w", err), false)
	}

	// Close is the publish step (and for rename-on-close stores the only
	// point the file becomes visible): it must succeed before merge() may
	// commit the pointer to the MetaStore, or a failed finalize would delete
	// sole copies of the source data.
	if err := writer.Close(); err != nil {
		return fail(fmt.Errorf("failed to close merge file writer: %w", err), true)
	}

	return filePointerBytes, newFileMetadata, nil
}

// blockWithFile represents a data block with the pointer of its source file
type blockWithFile struct {
	block       DataBlockMetadata
	filePointer []byte
}

// processPartitionBlocks handles merging data blocks for a single partition
func (b *BloomSearchEngine) processPartitionBlocks(ctx context.Context, writer io.Writer, allBlocks []blockWithFile, blockIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata, fileEntries *bloomEntrySets) error {
	// Group mergeable blocks together
	var mergeGroups [][]int // groups of block indices that can be merged
	processed := make(map[int]bool)

	for _, blockIdx := range blockIndices {
		if processed[blockIdx] {
			continue
		}

		currentGroup := []int{blockIdx}
		currentRows := allBlocks[blockIdx].block.Rows
		currentSize := allBlocks[blockIdx].block.UncompressedSize
		processed[blockIdx] = true

		// Find blocks that can be merged with this one
		for _, otherIdx := range blockIndices {
			if processed[otherIdx] {
				continue
			}

			otherBlock := allBlocks[otherIdx].block
			if b.dataBlocksAreMergeable(allBlocks[blockIdx].block, otherBlock) {
				// Check if adding this block would exceed limits
				if currentRows+otherBlock.Rows <= b.config.MaxRowGroupRows &&
					currentSize+otherBlock.UncompressedSize <= b.config.MaxRowGroupBytes {
					currentGroup = append(currentGroup, otherIdx)
					currentRows += otherBlock.Rows
					currentSize += otherBlock.UncompressedSize
					processed[otherIdx] = true
				}
			}
		}

		mergeGroups = append(mergeGroups, currentGroup)
	}

	// Process each merge group
	for _, group := range mergeGroups {
		if len(group) == 1 {
			// Single block, just copy it
			blockIdx := group[0]
			err := b.copyDataBlock(ctx, writer, allBlocks[blockIdx], currentOffset, newDataBlocks, fileEntries)
			if err != nil {
				return fmt.Errorf("failed to copy data block: %w", err)
			}
		} else {
			// Multiple blocks, merge them
			err := b.mergeDataBlocks(ctx, writer, allBlocks, group, partitionID, currentOffset, newDataBlocks, fileEntries)
			if err != nil {
				return fmt.Errorf("failed to merge data blocks: %w", err)
			}
		}
	}

	return nil
}

// copyDataBlock copies a single data block — filter section and row data —
// to the output file verbatim, then re-streams the block's rows purely for
// entry collection so the merged file's rebuilt file-level filters cover the
// copied rows too. Both the filter section (its CRC, via parseFilterSection)
// and the row data (see decodeBlockRowData) are verified before anything is
// written: a corrupt source block fails the merge instead of propagating the
// corruption into the output file.
func (b *BloomSearchEngine) copyDataBlock(ctx context.Context, writer io.Writer, bwf blockWithFile, currentOffset *int, newDataBlocks *[]DataBlockMetadata, fileEntries *bloomEntrySets) error {
	block := bwf.block
	if block.BloomFiltersSize < 0 || block.BloomFiltersSize > block.Size {
		return fmt.Errorf("invalid bloom filter section size %d (block size %d)", block.BloomFiltersSize, block.Size)
	}

	file, err := b.dataStore.OpenFile(ctx, bwf.filePointer)
	if err != nil {
		return fmt.Errorf("failed to open source file for block copy: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(int64(block.Offset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to source block: %w", err)
	}
	raw := make([]byte, block.Size)
	if _, err := io.ReadFull(file, raw); err != nil {
		return fmt.Errorf("failed to read block data: %w", err)
	}

	if block.BloomFiltersSize > 0 {
		if _, err := parseFilterSection(raw[:block.BloomFiltersSize]); err != nil {
			return fmt.Errorf("failed to verify copied block filter section: %w", err)
		}
	}

	rowData, err := decodeBlockRowData(raw[block.BloomFiltersSize:], &block)
	if err != nil {
		return fmt.Errorf("failed to verify copied block row data: %w", err)
	}
	scanner := blockRowScanner{data: rowData}
	for {
		rowBytes, ok, err := scanner.next()
		if err != nil {
			return fmt.Errorf("error reading from data block: %w", err)
		}
		if !ok {
			break
		}
		fileEntries.indexRow(rowBytes, b.config.Tokenizer)
	}

	if _, err := writer.Write(raw); err != nil {
		return fmt.Errorf("failed to copy block data: %w", err)
	}

	// Create new block metadata with updated offset (everything else stays the same)
	newBlockMetadata := block // copy the struct
	newBlockMetadata.Offset = *currentOffset

	*newDataBlocks = append(*newDataBlocks, newBlockMetadata)
	*currentOffset += newBlockMetadata.Size

	return nil
}

// mergeDataBlocks merges multiple data blocks into a single data block,
// rebuilding the block's bloom filters from its rows: every row is fed
// through the shared walker/tokenizer into fresh entry sets, and exact-sized
// filters are built once the merged block is complete. Because the filter
// section precedes the row data on disk but is only known after every row has
// streamed through, the compressed row data is buffered in memory (bounded by
// the row-group size budget) until the block completes.
//
// Blocks are loaded one at a time, each through its own file handle, so
// blocks from the same source file never interleave reads on a shared seek
// position.
func (b *BloomSearchEngine) mergeDataBlocks(ctx context.Context, writer io.Writer, allBlocks []blockWithFile, groupIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata, fileEntries *bloomEntrySets) error {
	blockEntries := newBloomEntrySets()
	var mergedMinMaxIndexes map[string]MinMaxIndex

	var compressed bytes.Buffer
	rowDataHasher := crc32.New(crc32cTable)
	compressionEncoders, err := b.createCompressionWriter(io.MultiWriter(&compressed, rowDataHasher))
	if err != nil {
		return fmt.Errorf("failed to create compression writer: %w", err)
	}
	rowDataWriter := compressionEncoders.writer

	uncompressedSize := 0
	rowCount := 0
	var lengthBytes [LengthPrefixSize]byte

	for i, blockIdx := range groupIndices {
		bwf := allBlocks[blockIdx]

		if i == 0 {
			mergedMinMaxIndexes = bwf.block.MinMaxIndexes
		} else {
			mergedMinMaxIndexes = b.mergeMinMaxIndexes(mergedMinMaxIndexes, bwf.block.MinMaxIndexes)
		}

		rowData, err := b.loadBlockRowData(ctx, bwf.filePointer, bwf.block)
		if err != nil {
			return fmt.Errorf("failed to read data block rows: %w", err)
		}

		scanner := blockRowScanner{data: rowData}
		for {
			rowBytes, ok, scanErr := scanner.next()
			if scanErr != nil {
				return fmt.Errorf("error reading from data block: %w", scanErr)
			}
			if !ok {
				break
			}

			blockEntries.indexRow(rowBytes, b.config.Tokenizer)

			binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(rowBytes)))
			if _, err := rowDataWriter.Write(lengthBytes[:]); err != nil {
				return fmt.Errorf("failed to write row length: %w", err)
			}
			if _, err := rowDataWriter.Write(rowBytes); err != nil {
				return fmt.Errorf("failed to write row data: %w", err)
			}

			uncompressedSize += len(rowBytes) + LengthPrefixSize
			rowCount++
		}
	}

	// Finalize compression
	if err := compressionEncoders.finalizeCompression(); err != nil {
		return fmt.Errorf("failed to finalize compression: %w", err)
	}

	// Every row has streamed through: build the block's exact-sized filters
	// and write the block as [filter section][compressed row data].
	blockFilters := blockEntries.buildFilters(b.config.BloomFalsePositiveRate)
	filterSection, err := encodeFilterSection(&blockFilters)
	if err != nil {
		return fmt.Errorf("failed to encode bloom filters: %w", err)
	}
	if _, err := writer.Write(filterSection); err != nil {
		return fmt.Errorf("failed to write bloom filters: %w", err)
	}
	if _, err := writer.Write(compressed.Bytes()); err != nil {
		return fmt.Errorf("failed to write merged row data: %w", err)
	}

	blockEntries.unionInto(fileEntries)

	totalSize := len(filterSection) + compressed.Len()
	*newDataBlocks = append(*newDataBlocks, DataBlockMetadata{
		PartitionID:      partitionID,
		Rows:             rowCount,
		Offset:           *currentOffset,
		Size:             totalSize,
		BloomFiltersSize: len(filterSection),
		MinMaxIndexes:    mergedMinMaxIndexes,
		// Compression and bloom params both reflect what was just built with
		// the current config.
		Compression:            b.config.RowDataCompression,
		UncompressedSize:       uncompressedSize,
		RowDataHash:            rowDataHasher.Sum32(),
		HasRowDataHash:         true,
		BloomEntryCounts:       blockEntries.counts(),
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
	})
	*currentOffset += totalSize

	return nil
}

// loadBlockRowData opens a dedicated handle on the block's source file and
// returns the block's verified, decompressed row data (see readBlockRowData
// for the verification order and bounds). The handle is closed before
// returning — callers work from the in-memory copy.
func (b *BloomSearchEngine) loadBlockRowData(ctx context.Context, filePointer []byte, block DataBlockMetadata) ([]byte, error) {
	file, err := b.dataStore.OpenFile(ctx, filePointer)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file for row reader: %w", err)
	}
	defer file.Close()
	return readBlockRowData(file, &block)
}

// writeFileMetadataAndFooter completes a file with the v2 footer: the
// file-level filter section (binary, see encodeFilterSection), the metadata
// JSON — which carries no filter bytes, only the section's size — its CRC32C
// and length, the file version, and the magic bytes.
func (b *BloomSearchEngine) writeFileMetadataAndFooter(writer io.Writer, metadata *FileMetadata) error {
	// Write the file-level filter section
	filterSection, err := encodeFilterSection(&metadata.BloomFilters)
	if err != nil {
		return fmt.Errorf("failed to encode file bloom filters: %w", err)
	}
	if _, err := writer.Write(filterSection); err != nil {
		return fmt.Errorf("failed to write file bloom filters: %w", err)
	}

	// Write file metadata
	metadataBytes, err := json.Marshal(fileMetadataV2JSON{
		BloomFalsePositiveRate: metadata.BloomFalsePositiveRate,
		BloomEntryCounts:       metadata.BloomEntryCounts,
		FileFilterSectionSize:  len(filterSection),
		DataBlocks:             metadata.DataBlocks,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}
	if _, err := writer.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write file metadata: %w", err)
	}

	metadataHashBytes := make([]byte, HashSize)
	binary.LittleEndian.PutUint32(metadataHashBytes, crc32.Checksum(metadataBytes, crc32cTable))
	if _, err := writer.Write(metadataHashBytes); err != nil {
		return fmt.Errorf("failed to write file metadata hash: %w", err)
	}

	// Write metadata length
	metadataLengthBytes := make([]byte, LengthPrefixSize)
	binary.LittleEndian.PutUint32(metadataLengthBytes, uint32(len(metadataBytes)))
	if _, err := writer.Write(metadataLengthBytes); err != nil {
		return fmt.Errorf("failed to write file metadata length: %w", err)
	}

	// Write version
	versionBytes := make([]byte, VersionPrefixSize)
	binary.LittleEndian.PutUint32(versionBytes, FileVersion)
	if _, err := writer.Write(versionBytes); err != nil {
		return fmt.Errorf("failed to write file version: %w", err)
	}

	// Write magic bytes
	if _, err := writer.Write([]byte(MagicBytes)); err != nil {
		return fmt.Errorf("failed to write magic bytes: %w", err)
	}

	return nil
}
