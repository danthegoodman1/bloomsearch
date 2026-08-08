package bloomsearch

import (
	"context"
	"fmt"
	"math/rand"
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
