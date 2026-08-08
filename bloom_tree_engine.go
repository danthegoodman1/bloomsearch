package bloomsearch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
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

// writeBloomFiltersWithHash serializes bloom filters and writes them with their hash to a writer
func (b *BloomSearchEngine) writeBloomFiltersWithHash(writer io.Writer, bloomFilters *BloomFilters) ([]byte, []byte, int, error) {
	// Serialize bloom filters and get hash
	bloomFiltersBytes, bloomFiltersHashBytes := bloomFilters.Bytes()

	// Write bloom filters
	if _, err := writer.Write(bloomFiltersBytes); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to write bloom filters: %w", err)
	}

	// Write bloom filters hash
	if _, err := writer.Write(bloomFiltersHashBytes); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to write bloom filters hash: %w", err)
	}

	// Return bloom filter bytes, hash bytes, and total size written
	return bloomFiltersBytes, bloomFiltersHashBytes, len(bloomFiltersBytes) + len(bloomFiltersHashBytes), nil
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
	fileBloomFilters BloomFilters
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

	// Bloom filter parameters
	FileBloomExpectedItems uint
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
	partitionID           string
	rowCount              int
	minMaxIndexes         map[string]MinMaxIndex
	buffer                bytes.Buffer
	fieldBloomFilter      *bloom.BloomFilter
	tokenBloomFilter      *bloom.BloomFilter
	fieldTokenBloomFilter *bloom.BloomFilter
	compressionEncoders   *compressionEncoders
	uncompressedSize      int
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

		FileBloomExpectedItems: 100_000,
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

	if config.FileBloomExpectedItems == 0 {
		return nil, fmt.Errorf("%w: BloomExpectedItems must be greater than 0", ErrInvalidConfig)
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

func (b *BloomSearchEngine) newFileLevelBloomFilters() (*bloom.BloomFilter, *bloom.BloomFilter, *bloom.BloomFilter) {
	return bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate),
		bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate),
		bloom.NewWithEstimates(b.config.FileBloomExpectedItems, b.config.BloomFalsePositiveRate)
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
	fileFieldBloomFilter, fileTokenBloomFilter, fileFieldTokenBloomFilter := b.newFileLevelBloomFilters()

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
						&fileFieldBloomFilter,
						&fileTokenBloomFilter,
						&fileFieldTokenBloomFilter,
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
						&fileFieldBloomFilter,
						&fileTokenBloomFilter,
						&fileFieldTokenBloomFilter,
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
				&fileFieldBloomFilter,
				&fileTokenBloomFilter,
				&fileFieldTokenBloomFilter,
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
					&fileFieldBloomFilter,
					&fileTokenBloomFilter,
					&fileFieldTokenBloomFilter,
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
	fileFieldBloomFilter **bloom.BloomFilter,
	fileTokenBloomFilter **bloom.BloomFilter,
	fileFieldTokenBloomFilter **bloom.BloomFilter,
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

	b.triggerFlush(
		partitionBuffersCopy,
		doneChannsCopy,
		BloomFilters{
			FieldBloomFilter:      *fileFieldBloomFilter,
			TokenBloomFilter:      *fileTokenBloomFilter,
			FieldTokenBloomFilter: *fileFieldTokenBloomFilter,
		},
	)

	// Reset local state
	for k := range partitionBuffers {
		delete(partitionBuffers, k)
	}
	*doneChans = make([]chan error, 0)
	*bufferedRowCount = 0
	*bufferedBytes = 0
	*bufferStartTime = time.Time{}
	*fileFieldBloomFilter, *fileTokenBloomFilter, *fileFieldTokenBloomFilter = b.newFileLevelBloomFilters()
}

func (b *BloomSearchEngine) processIngestRequest(
	ctx context.Context,
	req *ingestRequest,
	partitionBuffers map[string]*partitionBuffer,
	doneChans *[]chan error,
	bufferedRowCount *int,
	bufferedBytes *int,
	bufferStartTime *time.Time,
	fileFieldBloomFilter **bloom.BloomFilter,
	fileTokenBloomFilter **bloom.BloomFilter,
	fileFieldTokenBloomFilter **bloom.BloomFilter,
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
			fileFieldBloomFilter,
			fileTokenBloomFilter,
			fileFieldTokenBloomFilter,
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
				partitionID:           partitionID,
				minMaxIndexes:         make(map[string]MinMaxIndex),
				buffer:                bytes.Buffer{},
				fieldBloomFilter:      bloom.NewWithEstimates(uint(b.config.MaxRowGroupRows), b.config.BloomFalsePositiveRate),
				tokenBloomFilter:      bloom.NewWithEstimates(uint(b.config.MaxRowGroupRows), b.config.BloomFalsePositiveRate),
				fieldTokenBloomFilter: bloom.NewWithEstimates(uint(b.config.MaxRowGroupRows), b.config.BloomFalsePositiveRate),
				uncompressedSize:      0,
				rowCount:              0,
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

			// Add info to bloom filters: every path (including intermediate
			// object/array paths) to the field filters, leaf-value tokens to the
			// token filters, and exact-leaf-path::token pairs to the field-token
			// filters
			forEachPathValue(gjson.ParseBytes(rowBytes), ".", func(path string, value gjson.Result, isLeaf bool) {
				partitionBuffer.fieldBloomFilter.AddString(path)
				(*fileFieldBloomFilter).AddString(path)
				if !isLeaf {
					return
				}
				text, ok := leafTokenInput(value)
				if !ok {
					return
				}
				for _, token := range b.config.Tokenizer(text) {
					fieldTokenKey := makeFieldTokenKey(path, token)
					partitionBuffer.tokenBloomFilter.AddString(token)
					partitionBuffer.fieldTokenBloomFilter.AddString(fieldTokenKey)
					(*fileTokenBloomFilter).AddString(token)
					(*fileFieldTokenBloomFilter).AddString(fieldTokenKey)
				}
			})

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
			fileFieldBloomFilter,
			fileTokenBloomFilter,
			fileFieldTokenBloomFilter,
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
func (b *BloomSearchEngine) triggerFlush(partitionBuffers map[string]*partitionBuffer, doneChans []chan error, fileBloomFilters BloomFilters) {
	flushReq := flushRequest{
		partitionBuffers: partitionBuffers,
		doneChans:        doneChans,
		fileBloomFilters: fileBloomFilters,
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
		BloomFilters:           flushReq.fileBloomFilters,
		BloomExpectedItems:     b.config.FileBloomExpectedItems,
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

	// For each partition buffer, write the data block to the data store
	for _, partitionBuffer := range flushReq.partitionBuffers {
		// Finalize compression encoders before writing
		var compressedData []byte
		if err := partitionBuffer.compressionEncoders.finalizeCompression(); err != nil {
			fail(fmt.Errorf("failed to finalize compression: %w", err), false)
			return
		}
		compressedData = partitionBuffer.buffer.Bytes()
		// Create data block bloom filters struct
		dataBlockBloomFilters := &BloomFilters{
			FieldBloomFilter:      partitionBuffer.fieldBloomFilter,
			TokenBloomFilter:      partitionBuffer.tokenBloomFilter,
			FieldTokenBloomFilter: partitionBuffer.fieldTokenBloomFilter,
		}

		// Write bloom filters and hash
		_, _, bloomFiltersSize, err := b.writeBloomFiltersWithHash(writer, dataBlockBloomFilters)
		if err != nil {
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

		// No file-level hash currently needed

		dataBlockSize := bloomFiltersSize + len(compressedData)

		fileMetadata.DataBlocks = append(fileMetadata.DataBlocks, DataBlockMetadata{
			PartitionID:            partitionBuffer.partitionID,
			Rows:                   partitionBuffer.rowCount,
			Offset:                 currentOffset,
			Size:                   dataBlockSize,
			BloomFiltersSize:       bloomFiltersSize,
			MinMaxIndexes:          partitionBuffer.minMaxIndexes,
			Compression:            b.config.RowDataCompression,
			UncompressedSize:       partitionBuffer.uncompressedSize,
			RowDataHash:            rowDataHash,
			BloomExpectedItems:     uint(b.config.MaxRowGroupRows),
			BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		})

		currentOffset += dataBlockSize
	}

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

	// Test file-level bloom filters, using concurrency only above a threshold
	const concurrencyThreshold = 20

	var matchingFiles []MaybeFile
	if len(maybeFiles) < concurrencyThreshold {
		// Sequential evaluation for small numbers of files
		matchingFiles = make([]MaybeFile, 0, len(maybeFiles))
		for _, maybeFile := range maybeFiles {
			if b.evaluateBloomFilters(
				maybeFile.Metadata.BloomFilters.FieldBloomFilter,
				maybeFile.Metadata.BloomFilters.TokenBloomFilter,
				maybeFile.Metadata.BloomFilters.FieldTokenBloomFilter,
				pruneBloomQuery,
			) {
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

	rowDataOffset := int64(job.blockMetadata.Offset + job.blockMetadata.BloomFiltersSize)
	if _, err := file.Seek(rowDataOffset, 0); err != nil {
		fail(fmt.Errorf("failed to seek to row data: %w", err))
		return
	}

	// Calculate compressed row data size (no trailing hash now)
	compressedRowDataSize := job.blockMetadata.Size - job.blockMetadata.BloomFiltersSize

	// Create a limited reader for the compressed row data
	limitedReader := io.LimitReader(file, int64(compressedRowDataSize))

	// Create hash-calculating reader to verify hash while streaming
	hashReader := newHashCalculatingReader(limitedReader, int64(compressedRowDataSize))

	// Create appropriate decompression reader based on compression type
	var rowDataReader io.Reader
	switch normalizeCompression(job.blockMetadata.Compression) {
	case CompressionNone:
		rowDataReader = hashReader
	case CompressionSnappy:
		rowDataReader = snappy.NewReader(hashReader)
	case CompressionZstd:
		decoder, err := zstd.NewReader(hashReader)
		if err != nil {
			fail(fmt.Errorf("failed to create zstd decoder: %w", err))
			return
		}
		rowDataReader = decoder
		defer decoder.Close()
	default:
		fail(fmt.Errorf("unsupported compression type: %s", job.blockMetadata.Compression))
		return
	}

	// Now read individual rows from the decompressed stream
	var lengthBytes [LengthPrefixSize]byte
	rowBuf := make([]byte, 0)
	for {
		// Cancellation is terminal for the query; stop scanning promptly.
		if ctx.Err() != nil {
			return
		}

		n, err := io.ReadFull(rowDataReader, lengthBytes[:])
		if err == io.EOF {
			break // End of data
		}
		if err != nil || n != LengthPrefixSize {
			fail(fmt.Errorf("failed to read row length: %w", err))
			return
		}

		rowLength := binary.LittleEndian.Uint32(lengthBytes[:])

		if cap(rowBuf) < int(rowLength) {
			rowBuf = make([]byte, int(rowLength))
		} else {
			rowBuf = rowBuf[:int(rowLength)]
		}

		n, err = io.ReadFull(rowDataReader, rowBuf)
		if err != nil || n != int(rowLength) {
			fail(fmt.Errorf("failed to read row data: %w", err))
			return
		}

		rowsScanned++
		bytesScanned += int64(LengthPrefixSize) + int64(rowLength)

		rowValue := gjson.ParseBytes(rowBuf)
		if !TestGJSONForQuery(rowValue, rowBloomQuery, regexQuery, ".", b.config.Tokenizer) {
			continue
		}

		// Materialize the row from the same gjson parse used for matching —
		// one parse per row. gjson materializes JSON numbers as float64,
		// matching encoding/json, and ParseBytes copied rowBuf, so the map
		// does not alias the reused scan buffer.
		row, ok := rowValue.Value().(map[string]any)
		if !ok {
			fail(fmt.Errorf("row is not a JSON object"))
			return
		}

		if err := r.deliver(slot, row); err != nil {
			return
		}
	}

	// Verify hash after all data has been read
	if job.blockMetadata.RowDataHash != 0 {
		computedHash := hashReader.Sum32()
		if computedHash != job.blockMetadata.RowDataHash {
			fail(fmt.Errorf("row data hash mismatch: expected %x, got %x", job.blockMetadata.RowDataHash, computedHash))
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

// dataBlocksAreMergeable checks if two data blocks can be merged together
func (b *BloomSearchEngine) dataBlocksAreMergeable(block1, block2 DataBlockMetadata) bool {
	// Must have the same partition ID
	if block1.PartitionID != block2.PartitionID {
		return false
	}

	if block1.BloomExpectedItems != block2.BloomExpectedItems ||
		block1.BloomFalsePositiveRate != block2.BloomFalsePositiveRate {
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

// mergeBloomFiltersStruct merges two bloom filters by performing Merge operation
func (b *BloomSearchEngine) mergeBloomFiltersStruct(filters1, filters2 *BloomFilters) (*BloomFilters, error) {
	// Copy the first filter
	merged := &BloomFilters{
		FieldBloomFilter:      filters1.FieldBloomFilter.Copy(),
		TokenBloomFilter:      filters1.TokenBloomFilter.Copy(),
		FieldTokenBloomFilter: filters1.FieldTokenBloomFilter.Copy(),
	}

	// Merge the second filter into the copy
	// This will fail if the filters have incompatible parameters (different m, k values)
	if err := merged.FieldBloomFilter.Merge(filters2.FieldBloomFilter); err != nil {
		return nil, fmt.Errorf("failed to merge bloom filters: %w", err)
	}
	if err := merged.TokenBloomFilter.Merge(filters2.TokenBloomFilter); err != nil {
		return nil, fmt.Errorf("failed to merge bloom filters: %w", err)
	}
	if err := merged.FieldTokenBloomFilter.Merge(filters2.FieldTokenBloomFilter); err != nil {
		return nil, fmt.Errorf("failed to merge bloom filters: %w", err)
	}

	return merged, nil
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

// identifyFileMergeGroups groups files that should be merged together using smart row group merging
func (b *BloomSearchEngine) identifyFileMergeGroups(files []fileMergeCandidate) [][]fileMergeCandidate {
	if len(files) == 0 {
		return nil
	}

	// Group files by bloom filter parameters
	type bloomFilterParams struct {
		expectedItems     uint
		falsePositiveRate float64
	}

	parameterGroups := make(map[bloomFilterParams][]fileMergeCandidate)
	for _, file := range files {
		params := bloomFilterParams{
			expectedItems:     file.metadata.BloomExpectedItems,
			falsePositiveRate: file.metadata.BloomFalsePositiveRate,
		}
		parameterGroups[params] = append(parameterGroups[params], file)
	}

	var mergeGroups [][]fileMergeCandidate
	totalFilesInGroups := 0

	for _, compatibleFiles := range parameterGroups {
		if len(compatibleFiles) < 2 {
			continue
		}

		// Sort files by potential for merging (smaller files first, then by partition locality)
		sort.Slice(compatibleFiles, func(i, j int) bool {
			a, b := compatibleFiles[i], compatibleFiles[j]

			// Primary: Prefer files with smaller average block sizes (more opportunity for merging)
			aAvgBlockSize := a.statistics.totalSize / max(a.statistics.blockCount, 1)
			bAvgBlockSize := b.statistics.totalSize / max(b.statistics.blockCount, 1)

			if aAvgBlockSize != bAvgBlockSize {
				return aAvgBlockSize < bAvgBlockSize
			}

			// Secondary: Sort by total size (smaller first)
			return a.statistics.totalSize < b.statistics.totalSize
		})

		// Track which files have already been assigned to a group
		fileAssigned := make(map[int]bool)

		// Greedy approach: try to group files that can benefit from row group merging
		for i, file := range compatibleFiles {
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
			for j := i + 1; j < len(compatibleFiles); j++ {
				if fileAssigned[j] {
					continue
				}

				if totalFilesInGroups+len(currentGroup)+1 > b.config.MaxFilesToMergePerOperation {
					break
				}

				candidate := compatibleFiles[j]

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

		if totalFilesInGroups >= b.config.MaxFilesToMergePerOperation {
			break
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

// executeMergeGroup merges a group of files with smart row group merging
func (b *BloomSearchEngine) executeMergeGroup(ctx context.Context, group []fileMergeCandidate) ([]byte, *FileMetadata, error) {
	// Initialize new file-level bloom filters by merging from source files
	newFileFieldBloomFilter := bloom.NewWithEstimates(group[0].metadata.BloomExpectedItems, group[0].metadata.BloomFalsePositiveRate)
	newFileTokenBloomFilter := bloom.NewWithEstimates(group[0].metadata.BloomExpectedItems, group[0].metadata.BloomFalsePositiveRate)
	newFileFieldTokenBloomFilter := bloom.NewWithEstimates(group[0].metadata.BloomExpectedItems, group[0].metadata.BloomFalsePositiveRate)

	// Merge file-level bloom filters from all source files
	for _, candidate := range group {
		if err := newFileFieldBloomFilter.Merge(candidate.metadata.BloomFilters.FieldBloomFilter); err != nil {
			return nil, nil, fmt.Errorf("failed to merge file field bloom filter: %w", err)
		}
		if err := newFileTokenBloomFilter.Merge(candidate.metadata.BloomFilters.TokenBloomFilter); err != nil {
			return nil, nil, fmt.Errorf("failed to merge file token bloom filter: %w", err)
		}
		if err := newFileFieldTokenBloomFilter.Merge(candidate.metadata.BloomFilters.FieldTokenBloomFilter); err != nil {
			return nil, nil, fmt.Errorf("failed to merge file field-token bloom filter: %w", err)
		}
	}

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

	// Collect all data blocks from all files with their file references.
	// These shared handles serve only whole-block copies; row-level merge
	// readers open their own handle per block (see mergeDataBlocks) so
	// same-file blocks never interleave reads on one seek position.
	var allBlocks []blockWithFile
	openFiles := make(map[string]io.ReadSeekCloser)

	// Open all files and collect blocks
	for _, candidate := range group {
		fileKey := string(candidate.filePointer)
		if openFiles[fileKey] == nil {
			sourceFile, err := b.dataStore.OpenFile(ctx, candidate.filePointer)
			if err != nil {
				return fail(fmt.Errorf("failed to open source file for merge: %w", err), false)
			}
			openFiles[fileKey] = sourceFile
			defer sourceFile.Close()
		}

		for _, blockMetadata := range candidate.metadata.DataBlocks {
			allBlocks = append(allBlocks, blockWithFile{
				block:       blockMetadata,
				file:        openFiles[fileKey],
				filePointer: candidate.filePointer,
				processed:   false,
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
		err := b.processPartitionBlocks(ctx, writer, allBlocks, blockIndices, partitionID, &currentOffset, &newDataBlocks)
		if err != nil {
			return fail(fmt.Errorf("failed to process partition %s: %w", partitionID, err), false)
		}
	}

	// Create new file metadata. The bloom params describe the source group's
	// filters (the merged filters were built from them above) — stamping the
	// current config here would make the metadata lie about the filters' m/k
	// whenever the config changed since the sources were written, permanently
	// breaking future merges of this file.
	newFileMetadata := &FileMetadata{
		BloomFilters: BloomFilters{
			FieldBloomFilter:      newFileFieldBloomFilter,
			TokenBloomFilter:      newFileTokenBloomFilter,
			FieldTokenBloomFilter: newFileFieldTokenBloomFilter,
		},
		BloomExpectedItems:     group[0].metadata.BloomExpectedItems,
		BloomFalsePositiveRate: group[0].metadata.BloomFalsePositiveRate,
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

// blockWithFile represents a data block with its associated file handle
type blockWithFile struct {
	block       DataBlockMetadata
	file        io.ReadSeeker
	filePointer []byte
	processed   bool
}

// processPartitionBlocks handles merging data blocks for a single partition
func (b *BloomSearchEngine) processPartitionBlocks(ctx context.Context, writer io.Writer, allBlocks []blockWithFile, blockIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata) error {
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
			err := b.copyDataBlock(writer, allBlocks[blockIdx], currentOffset, newDataBlocks)
			if err != nil {
				return fmt.Errorf("failed to copy data block: %w", err)
			}
		} else {
			// Multiple blocks, merge them
			err := b.mergeDataBlocks(ctx, writer, allBlocks, group, partitionID, currentOffset, newDataBlocks)
			if err != nil {
				return fmt.Errorf("failed to merge data blocks: %w", err)
			}
		}
	}

	return nil
}

// copyDataBlock copies a single data block to the output file
func (b *BloomSearchEngine) copyDataBlock(writer io.Writer, blockWithFile blockWithFile, currentOffset *int, newDataBlocks *[]DataBlockMetadata) error {
	// Seek to the start of the source block
	if _, err := blockWithFile.file.Seek(int64(blockWithFile.block.Offset), 0); err != nil {
		return fmt.Errorf("failed to seek to source block: %w", err)
	}

	// Stream copy the entire block (bloom filters + hash + row data) as raw bytes
	copied, err := io.CopyN(writer, blockWithFile.file, int64(blockWithFile.block.Size))
	if err != nil {
		return fmt.Errorf("failed to copy block data: %w", err)
	}
	if copied != int64(blockWithFile.block.Size) {
		return fmt.Errorf("incomplete copy: expected %d bytes, copied %d bytes", blockWithFile.block.Size, copied)
	}

	// Create new block metadata with updated offset (everything else stays the same)
	newBlockMetadata := blockWithFile.block // copy the struct
	newBlockMetadata.Offset = *currentOffset

	*newDataBlocks = append(*newDataBlocks, newBlockMetadata)
	*currentOffset += newBlockMetadata.Size

	return nil
}

// mergeDataBlocks merges multiple data blocks into a single optimized data block using streaming
func (b *BloomSearchEngine) mergeDataBlocks(ctx context.Context, writer io.Writer, allBlocks []blockWithFile, groupIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata) error {
	// Create streaming readers for each block. Every reader opens its own
	// file handle: blocks from the same source file sharing one seeking
	// handle would clobber each other's positions as the round-robin merge
	// interleaves reads.
	var readers []*dataBlockRowReader
	defer func() {
		for _, reader := range readers {
			reader.Close()
		}
	}()

	var mergedBloomFilters *BloomFilters
	var mergedMinMaxIndexes map[string]MinMaxIndex

	for i, blockIdx := range groupIndices {
		blockWithFile := allBlocks[blockIdx]

		// Create streaming reader for this block
		reader, err := b.newDataBlockRowReader(ctx, blockWithFile.filePointer, blockWithFile.block)
		if err != nil {
			return fmt.Errorf("failed to create row reader for data block: %w", err)
		}
		readers = append(readers, reader)

		// Merge bloom filters
		if i == 0 {
			mergedBloomFilters = reader.bloomFilters
			mergedMinMaxIndexes = blockWithFile.block.MinMaxIndexes
		} else {
			mergedBloomFilters, err = b.mergeBloomFiltersStruct(mergedBloomFilters, reader.bloomFilters)
			if err != nil {
				return fmt.Errorf("failed to merge bloom filters: %w", err)
			}
			mergedMinMaxIndexes = b.mergeMinMaxIndexes(mergedMinMaxIndexes, blockWithFile.block.MinMaxIndexes)
		}
	}

	// Stream merge the data blocks. The merged block's bloom params come from
	// the source blocks (dataBlocksAreMergeable requires them to be
	// identical) so the metadata describes the filters that were actually
	// merged, not the current config.
	sourceBlock := allBlocks[groupIndices[0]].block
	newBlockMetadata, err := b.streamMergeDataBlocks(writer, readers, partitionID, mergedBloomFilters, mergedMinMaxIndexes, *currentOffset, sourceBlock.BloomExpectedItems, sourceBlock.BloomFalsePositiveRate)
	if err != nil {
		return fmt.Errorf("failed to stream merge data blocks: %w", err)
	}

	*newDataBlocks = append(*newDataBlocks, *newBlockMetadata)
	*currentOffset += newBlockMetadata.Size

	return nil
}

// streamMergeDataBlocks performs streaming merge of multiple data block readers
func (b *BloomSearchEngine) streamMergeDataBlocks(writer io.Writer, readers []*dataBlockRowReader, partitionID string, bloomFilters *BloomFilters, minMaxIndexes map[string]MinMaxIndex, offset int, bloomExpectedItems uint, bloomFalsePositiveRate float64) (*DataBlockMetadata, error) {
	// Serialize and write bloom filters
	_, _, bloomFiltersSize, err := b.writeBloomFiltersWithHash(writer, bloomFilters)
	if err != nil {
		return nil, fmt.Errorf("failed to write bloom filters: %w", err)
	}

	// Stream compressed row data directly to the output writer while tracking bytes and checksum.
	rowDataCounter := &countingWriter{writer: writer}
	rowDataHasher := crc32.New(crc32cTable)
	rowDataDest := io.MultiWriter(rowDataCounter, rowDataHasher)

	uncompressedSize := 0
	rowCount := 0

	compressionEncoders, err := b.createCompressionWriter(rowDataDest)
	if err != nil {
		return nil, fmt.Errorf("failed to create compression writer: %w", err)
	}
	rowDataWriter := compressionEncoders.writer

	// Stream merge all readers (simple round-robin for now, could be more sophisticated)
	for {
		hasData := false

		// Check each reader and write any available rows
		for _, reader := range readers {
			if reader.hasMore && reader.err == nil {
				rowBytes := reader.getCurrentRow()
				if rowBytes != nil {
					hasData = true

					if len(rowBytes) > 0xFFFFFFFF {
						return nil, fmt.Errorf("row too large: %d bytes exceeds maximum", len(rowBytes))
					}

					// Write length prefix and row data
					lengthBytes := make([]byte, LengthPrefixSize)
					binary.LittleEndian.PutUint32(lengthBytes, uint32(len(rowBytes)))

					if _, err := rowDataWriter.Write(lengthBytes); err != nil {
						return nil, fmt.Errorf("failed to write row length: %w", err)
					}
					if _, err := rowDataWriter.Write(rowBytes); err != nil {
						return nil, fmt.Errorf("failed to write row data: %w", err)
					}

					uncompressedSize += len(rowBytes) + LengthPrefixSize
					rowCount++
				}
			} else {
			}

			// Check for reader errors
			if reader.err != nil {
				return nil, fmt.Errorf("error reading from data block: %w", reader.err)
			}
		}

		// If no readers have data, we're done
		if !hasData {
			break
		}
	}

	// Finalize compression
	if err := compressionEncoders.finalizeCompression(); err != nil {
		return nil, fmt.Errorf("failed to finalize compression: %w", err)
	}

	rowDataHash := rowDataHasher.Sum32()
	totalSize := bloomFiltersSize + rowDataCounter.count

	return &DataBlockMetadata{
		PartitionID:      partitionID,
		Rows:             rowCount,
		Offset:           offset,
		Size:             totalSize,
		BloomFiltersSize: bloomFiltersSize,
		MinMaxIndexes:    minMaxIndexes,
		// Compression reflects how the merged row data was just written
		// (current config); the bloom params reflect the source filters.
		Compression:            b.config.RowDataCompression,
		UncompressedSize:       uncompressedSize,
		RowDataHash:            rowDataHash,
		BloomExpectedItems:     bloomExpectedItems,
		BloomFalsePositiveRate: bloomFalsePositiveRate,
	}, nil
}

// countingWriter tracks streamed compressed row bytes for block metadata sizing.
type countingWriter struct {
	writer io.Writer
	count  int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.count += n
	return n, err
}

// dataBlockRowReader provides streaming access to rows from a data block.
// Each reader owns its file handle, so multiple readers over blocks of the
// same source file never disturb each other's positions; Close releases it.
type dataBlockRowReader struct {
	file          io.ReadSeekCloser
	rowDataReader io.Reader
	bloomFilters  *BloomFilters
	hasMore       bool
	currentRow    []byte
	err           error
	hashReader    *hashCalculatingReader
	expectedHash  uint32
	hashVerified  bool
	zstdDecoder   *zstd.Decoder
}

// newDataBlockRowReader creates a streaming reader for a data block, opening
// a dedicated file handle for it. Callers must Close the reader.
func (b *BloomSearchEngine) newDataBlockRowReader(ctx context.Context, filePointer []byte, blockMetadata DataBlockMetadata) (*dataBlockRowReader, error) {
	file, err := b.dataStore.OpenFile(ctx, filePointer)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file for row reader: %w", err)
	}

	// Read bloom filters first
	blockBloomFilters, err := ReadDataBlockBloomFilters(file, blockMetadata)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read bloom filters: %w", err)
	}

	// Seek to row data
	rowDataOffset := int64(blockMetadata.Offset + blockMetadata.BloomFiltersSize)
	if _, err := file.Seek(rowDataOffset, 0); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to row data: %w", err)
	}

	// Create streaming reader for compressed data with hash calculation
	compressedRowDataSize := blockMetadata.Size - blockMetadata.BloomFiltersSize
	limitedReader := io.LimitReader(file, int64(compressedRowDataSize))

	// Create hash-calculating reader to verify hash while streaming
	hashReader := newHashCalculatingReader(limitedReader, int64(compressedRowDataSize))

	// Create appropriate decompression reader based on compression type
	var rowDataReader io.Reader
	var zstdDec *zstd.Decoder
	switch normalizeCompression(blockMetadata.Compression) {
	case CompressionNone:
		rowDataReader = hashReader
	case CompressionSnappy:
		rowDataReader = snappy.NewReader(hashReader)
	case CompressionZstd:
		// Use streaming zstd decompression
		decoder, err := zstd.NewReader(hashReader)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		rowDataReader = decoder
		zstdDec = decoder
	default:
		file.Close()
		return nil, fmt.Errorf("unsupported compression type: %s", blockMetadata.Compression)
	}

	// Hash will be verified when EOF is reached (or no verification needed if no hash)
	hashAlreadyVerified := blockMetadata.RowDataHash == 0

	reader := &dataBlockRowReader{
		file:          file,
		rowDataReader: rowDataReader,
		bloomFilters:  blockBloomFilters,
		hasMore:       true,
		hashReader:    hashReader,
		expectedHash:  blockMetadata.RowDataHash,
		hashVerified:  hashAlreadyVerified,
	}
	if zstdDec != nil {
		reader.zstdDecoder = zstdDec
	}

	// Read the first row to initialize
	reader.next()

	return reader, nil
}

// Close releases the reader's decoder and file handle (idempotent).
func (r *dataBlockRowReader) Close() {
	r.closeDecoder()
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

// next reads the next row from the stream
func (r *dataBlockRowReader) next() {
	if !r.hasMore || r.err != nil {
		return
	}

	var lengthBytes [LengthPrefixSize]byte
	n, err := io.ReadFull(r.rowDataReader, lengthBytes[:])
	if err == io.EOF {
		r.hasMore = false
		// Verify hash if we haven't already and we have a hash to verify
		if !r.hashVerified && r.expectedHash != 0 && r.hashReader != nil {
			computedHash := r.hashReader.Sum32()
			if computedHash != r.expectedHash {
				r.err = fmt.Errorf("row data hash mismatch: expected %x, got %x", r.expectedHash, computedHash)
				r.closeDecoder()
				return
			}
			r.hashVerified = true
		}
		r.closeDecoder()
		return
	}
	if err != nil || n != LengthPrefixSize {
		r.err = fmt.Errorf("failed to read row length: %w", err)
		r.hasMore = false
		r.closeDecoder()
		return
	}

	rowLength := binary.LittleEndian.Uint32(lengthBytes[:])
	if cap(r.currentRow) < int(rowLength) {
		r.currentRow = make([]byte, int(rowLength))
	} else {
		r.currentRow = r.currentRow[:int(rowLength)]
	}
	n, err = io.ReadFull(r.rowDataReader, r.currentRow)
	if err != nil || n != int(rowLength) {
		r.err = fmt.Errorf("failed to read row data: %w", err)
		r.hasMore = false
		r.closeDecoder()
		return
	}
}

// closeDecoder closes the zstd decoder if present (idempotent)
func (r *dataBlockRowReader) closeDecoder() {
	if r.zstdDecoder != nil {
		r.zstdDecoder.Close()
		r.zstdDecoder = nil
	}
}

// getCurrentRow returns the current row and advances to the next
func (r *dataBlockRowReader) getCurrentRow() []byte {
	if !r.hasMore || r.err != nil {
		return nil
	}

	// Copy to preserve contents across advance when buffers are reused
	out := make([]byte, len(r.currentRow))
	copy(out, r.currentRow)
	r.next() // advance to next row
	return out
}

// writeFileMetadataAndFooter writes the file metadata and footer to complete the file
func (b *BloomSearchEngine) writeFileMetadataAndFooter(writer io.Writer, metadata *FileMetadata) error {
	// Write file metadata
	metadataBytes, metadataHashBytes := metadata.Bytes()
	if _, err := writer.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write file metadata: %w", err)
	}
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

// hashCalculatingReader wraps an io.Reader and calculates checksum as data is read
type hashCalculatingReader struct {
	reader    io.Reader
	hasher    hash.Hash32
	totalRead int64
	limit     int64
}

func newHashCalculatingReader(reader io.Reader, limit int64) *hashCalculatingReader {
	return &hashCalculatingReader{
		reader: reader,
		hasher: crc32.New(crc32cTable),
		limit:  limit,
	}
}

func (h *hashCalculatingReader) Read(p []byte) (n int, err error) {
	if h.totalRead >= h.limit {
		return 0, io.EOF
	}

	// Don't read more than our limit
	if int64(len(p)) > h.limit-h.totalRead {
		p = p[:h.limit-h.totalRead]
	}

	n, err = h.reader.Read(p)
	if n > 0 {
		h.hasher.Write(p[:n])
		h.totalRead += int64(n)
	}
	return n, err
}

func (h *hashCalculatingReader) Sum64() uint64 {
	// Maintain compatibility with callers expecting Sum64; derive from 32-bit checksum
	return uint64(h.hasher.Sum32())
}

func (h *hashCalculatingReader) Sum32() uint32 {
	return h.hasher.Sum32()
}
