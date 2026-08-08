package bloomsearch

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmarks for the ingest, query, and merge hot paths.
// Run: go test -bench=. -benchmem -run='^$'

var benchLevels = []string{"debug", "info", "warn", "error"}
var benchServices = []string{"auth", "payment", "search", "gateway", "billing"}
var benchWords = []string{
	"connection", "timeout", "retry", "database", "request", "processed",
	"failed", "succeeded", "cache", "miss", "upstream", "latency", "shard",
}

// benchRows returns n deterministic, realistic log-shaped rows.
func benchRows(n int) []map[string]any {
	rng := rand.New(rand.NewSource(42))
	rows := make([]map[string]any, n)
	for i := range rows {
		msg := ""
		for w := 0; w < 8; w++ {
			if w > 0 {
				msg += " "
			}
			msg += benchWords[rng.Intn(len(benchWords))]
		}
		rows[i] = map[string]any{
			"timestamp": int64(1700000000 + i),
			"level":     benchLevels[rng.Intn(len(benchLevels))],
			"service":   benchServices[rng.Intn(len(benchServices))],
			"message":   msg,
			"user_id":   rng.Intn(100000),
			"nested": map[string]any{
				"region": fmt.Sprintf("region-%d", rng.Intn(8)),
				"az":     fmt.Sprintf("az-%d", rng.Intn(3)),
			},
			"tags": []any{benchWords[rng.Intn(len(benchWords))], benchWords[rng.Intn(len(benchWords))]},
		}
	}
	return rows
}

func benchEngine(b *testing.B, dir string, mutate func(*BloomSearchEngineConfig)) *BloomSearchEngine {
	b.Helper()
	ds := NewFileSystemDataStore(dir)
	cfg := DefaultBloomSearchEngineConfig()
	cfg.MaxBufferedTime = time.Hour
	if mutate != nil {
		mutate(&cfg)
	}
	engine, err := NewBloomSearchEngine(cfg, ds, ds)
	if err != nil {
		b.Fatal(err)
	}
	engine.Start()
	return engine
}

// BenchmarkIngest measures end-to-end ingest throughput (tokenize, bloom
// insert, marshal, compress, flush) through the public API. b.N is rows.
func BenchmarkIngest(b *testing.B) {
	engine := benchEngine(b, b.TempDir(), func(cfg *BloomSearchEngineConfig) {
		cfg.MaxBufferedRows = 50_000
		cfg.MaxBufferedBytes = 512 << 20
	})
	defer engine.Stop(context.Background())

	const batchSize = 100
	pool := benchRows(1000)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		start := i % (len(pool) - batchSize)
		if err := engine.IngestRows(ctx, pool[start:start+batchSize], nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := engine.Flush(ctx); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}

// buildBenchDataset writes files x rowsPerFile rows into dir and returns after
// all files are durable. Each Flush call produces one file.
func buildBenchDataset(b *testing.B, dir string, files, rowsPerFile int) {
	b.Helper()
	engine := benchEngine(b, dir, func(cfg *BloomSearchEngineConfig) {
		cfg.MaxBufferedRows = rowsPerFile * 2
		cfg.MaxBufferedBytes = 512 << 20
	})
	defer engine.Stop(context.Background())

	rows := benchRows(files * rowsPerFile)
	ctx := context.Background()
	for f := 0; f < files; f++ {
		if err := engine.IngestRows(ctx, rows[f*rowsPerFile:(f+1)*rowsPerFile], nil); err != nil {
			b.Fatal(err)
		}
		if err := engine.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func benchQuery(b *testing.B, q *Query, wantResults bool) {
	dir := b.TempDir()
	buildBenchDataset(b, dir, 10, 2000)
	engine := benchEngine(b, dir, nil)
	defer engine.Stop(context.Background())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		res, err := engine.Query(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		for res.Next() {
			total++
		}
		if err := res.Err(); err != nil {
			b.Fatal(err)
		}
		res.Close()
	}
	b.StopTimer()
	if wantResults && total == 0 {
		b.Fatal("expected results, got none")
	}
	if !wantResults && total != 0 {
		b.Fatalf("expected no results, got %d", total)
	}
}

// BenchmarkQueryFieldTokenHit scans 20k rows for a field:token matching ~20%.
func BenchmarkQueryFieldTokenHit(b *testing.B) {
	benchQuery(b, NewQuery().FieldToken("service", "payment").Build(), true)
}

// BenchmarkQueryTokenMiss queries a token that exists nowhere; measures bloom
// pruning effectiveness plus filter read overhead.
func BenchmarkQueryTokenMiss(b *testing.B) {
	benchQuery(b, NewQuery().Token("zzznotfoundtoken").Build(), false)
}

// BenchmarkQueryRegex applies a regex final filter over a bloom-narrowed scan.
func BenchmarkQueryRegex(b *testing.B) {
	benchQuery(b, NewQuery().FieldToken("level", "error").FieldRegex("message", "timeout|cache").Build(), true)
}

// requestCountingStore wraps a DataStore to model object-storage access
// patterns. It charges a fixed latency per request — one for OpenFile, one per
// Read on an open handle, each standing in for a range GET — and counts them.
//
// Request counts are the concurrency-independent measure of access-pattern
// cost, and on object storage they are what the bill and the rate limit track.
// The injected latency makes that cost visible in wall-clock time at a bounded
// query concurrency, which is how real deployments run: at the 1000-way default
// a fan-out wide enough to hide latency entirely would hide the difference too.
type requestCountingStore struct {
	inner   DataStore
	latency time.Duration
	opens   atomic.Int64
	reads   atomic.Int64
}

func (s *requestCountingStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	return s.inner.CreateFile(ctx)
}

func (s *requestCountingStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	return s.inner.TombstoneFile(ctx, filePointerBytes)
}

func (s *requestCountingStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	s.opens.Add(1)
	if s.latency > 0 {
		time.Sleep(s.latency)
	}
	reader, err := s.inner.OpenFile(ctx, filePointerBytes)
	if err != nil {
		return nil, err
	}
	return &countingReader{store: s, inner: reader}, nil
}

type countingReader struct {
	store *requestCountingStore
	inner io.ReadSeekCloser
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.store.reads.Add(1)
	if r.store.latency > 0 {
		time.Sleep(r.store.latency)
	}
	return r.inner.Read(p)
}

func (r *countingReader) Seek(offset int64, whence int) (int64, error) {
	return r.inner.Seek(offset, whence)
}

func (r *countingReader) Close() error { return r.inner.Close() }

const (
	blockyFiles       = 8
	blockyRowsPerFile = 4000
	blockyPartitions  = 16 // one data block per partition per flush
	blockyMarkerToken = "zzmarkertoken"
	blockyMarkerShard = "7"
)

// buildBlockyDataset writes files x rowsPerFile rows partitioned into
// blockyPartitions shards, so each flush produces one data block per shard and
// every file holds many blocks — the shape of a real deployment. (A single
// partition yields exactly one block per file, since the row-group limits
// trigger a whole-file flush, so partitions and merges are what make files
// multi-block.)
//
// Exactly one row per file carries blockyMarkerToken, in shard
// blockyMarkerShard. A query for that token therefore passes every file's
// file-level filter and is rejected by all but one block filter per file: the
// pruning-dominated access pattern, where a query's cost is block filter reads
// rather than row data.
func buildBlockyDataset(b *testing.B, dir string, files, rowsPerFile int) {
	b.Helper()
	engine := benchEngine(b, dir, func(cfg *BloomSearchEngineConfig) {
		cfg.MaxBufferedRows = rowsPerFile * 2
		cfg.MaxBufferedBytes = 512 << 20
		cfg.PartitionFunc = func(row map[string]any) string {
			shard, _ := row["shard"].(string)
			return shard
		}
	})
	defer engine.Stop(context.Background())

	rows := benchRows(files * rowsPerFile)
	for i, row := range rows {
		row["shard"] = fmt.Sprintf("%d", i%blockyPartitions)
	}

	ctx := context.Background()
	for f := 0; f < files; f++ {
		fileRows := rows[f*rowsPerFile : (f+1)*rowsPerFile]
		marked := false
		for _, row := range fileRows {
			if row["shard"] == blockyMarkerShard {
				row["message"] = blockyMarkerToken
				marked = true
				break
			}
		}
		if !marked {
			b.Fatal("no row in the marker shard to mark")
		}
		if err := engine.IngestRows(ctx, fileRows, nil); err != nil {
			b.Fatal(err)
		}
		if err := engine.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// benchManyBlockQuery runs q against a many-blocks-per-file dataset, reporting
// DataStore requests per query alongside the timing. latency models
// object-storage per-request cost; concurrency bounds the fan-out.
func benchManyBlockQuery(b *testing.B, q *Query, latency time.Duration, concurrency int, wantResults bool) {
	benchManyBlockQueryFiles(b, q, blockyFiles, latency, concurrency, wantResults)
}

func benchManyBlockQueryFiles(b *testing.B, q *Query, files int, latency time.Duration, concurrency int, wantResults bool) {
	dir := b.TempDir()
	buildBlockyDataset(b, dir, files, blockyRowsPerFile)

	fs := NewFileSystemDataStore(dir)
	store := &requestCountingStore{inner: fs, latency: latency}
	cfg := DefaultBloomSearchEngineConfig()
	cfg.MaxBufferedTime = time.Hour
	cfg.MaxQueryConcurrency = concurrency
	engine, err := NewBloomSearchEngine(cfg, fs, store)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Stop(context.Background())
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		res, err := engine.Query(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		for res.Next() {
			total++
		}
		if err := res.Err(); err != nil {
			b.Fatal(err)
		}
		res.Close()
	}
	b.StopTimer()
	if wantResults && total == 0 {
		b.Fatal("expected results, got none")
	}
	if !wantResults && total != 0 {
		b.Fatalf("expected no results, got %d", total)
	}
	b.ReportMetric(float64(store.opens.Load())/float64(b.N), "opens/op")
	b.ReportMetric(float64(store.reads.Load())/float64(b.N), "reads/op")
}

// BenchmarkManyBlocksNeedleLocal isolates the CPU and syscall cost of consulting
// every block's filters to find one row per file, with no injected latency.
func BenchmarkManyBlocksNeedleLocal(b *testing.B) {
	benchManyBlockQuery(b, NewQuery().Token(blockyMarkerToken).Build(), 0, 1000, true)
}

// BenchmarkManyBlocksNeedleRemote is the pruning-dominated object-storage case:
// every block's filters are consulted, one block per file survives.
func BenchmarkManyBlocksNeedleRemote(b *testing.B) {
	benchManyBlockQuery(b, NewQuery().Token(blockyMarkerToken).Build(), 250*time.Microsecond, 16, true)
}

// BenchmarkManyBlocksBroadRemote scans row data from every block: the case where
// filter traffic is amortized against real scanning.
func BenchmarkManyBlocksBroadRemote(b *testing.B) {
	benchManyBlockQuery(b, NewQuery().FieldToken("service", "payment").Build(), 250*time.Microsecond, 16, true)
}

// BenchmarkManyBlocksNeedleFanout is the deployment shape: candidate files
// outnumber query concurrency (32 files, 512 blocks, 8-way), so request latency
// lands on the critical path instead of being hidden by spare fan-out. The
// benchmarks above cannot show a request-count reduction because they run more
// workers than they have files.
func BenchmarkManyBlocksNeedleFanout(b *testing.B) {
	benchManyBlockQueryFiles(b, NewQuery().Token(blockyMarkerToken).Build(), 32, 250*time.Microsecond, 8, true)
}

// BenchmarkMerge merges 6 small files into one. Dataset rebuild is untimed.
func BenchmarkMerge(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		buildBenchDataset(b, dir, 6, 500)
		engine := benchEngine(b, dir, nil)
		b.StartTimer()

		if _, err := engine.Merge(ctx); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		engine.Stop(context.Background())
		b.StartTimer()
	}
}
