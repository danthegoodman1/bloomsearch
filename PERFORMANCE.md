# Performance

Measured with the committed benchmark suite (`bench_test.go`):

```
go test -bench=. -benchmem -run='^$'
```

## Results (August 2026)

Apple M3 Max (darwin/arm64), Go 1.26, 2-3 sequential runs per side (the
deltas of 2.4×-262× dwarf the <1% run-to-run variance). "v1 baseline" is the
pre-overhaul engine at the commit that recorded it (`2393763`, JSON bloom
encoding, filters sized from configured guesses); "current" is the v2 engine
(measured filter sizing, binary format, compiled row matcher, pooled codecs),
both run on the same machine on the same day.

| Benchmark               | v1 baseline        | current            | time        |
| ----------------------- | ------------------ | ------------------ | ----------- |
| `Ingest` (per row)      | 8.1µs, 104 allocs  | 3.3µs, 21 allocs   | −59% (2.4×) |
| `QueryFieldTokenHit`    | 79.9ms, 264k allocs| 3.5ms, 68.6k allocs| −96% (23×)  |
| `QueryTokenMiss`        | 69.9ms, 689 allocs | 0.27ms, 516 allocs | −99.6% (262×) |
| `QueryRegex`            | 79.9ms, 269k allocs| 4.1ms, 64.6k allocs| −95% (19×)  |
| `Merge`                 | 54.2ms, 10.3k allocs| 29.3ms, 12.8k allocs| −46% (1.8×) |

Benchmark shapes: queries scan a 10-file × 2,000-row dataset
(`QueryFieldTokenHit` matches ~20% of rows, `QueryTokenMiss` matches nothing,
`QueryRegex` is a bloom-narrowed regex final filter); `Merge` combines 6
files × 500 rows; `Ingest` measures end-to-end batches of 100 through the
public API (~300k rows/s single-actor).

The `QueryTokenMiss` delta is the hierarchical index working: file-level bloom
filters disqualify every file, so the query reads no block data at all. In the
v1 baseline the same query scanned all 20,000 rows because saturated filters
pruned nothing. Merge allocations rose vs the baseline because merge now
verifies every source block (CRC + decompression bounds) and rebuilds filters
from row data rather than OR-ing bits.

**Storage**: the v2 binary filter encoding plus measured sizing cut the
benchmark dataset's files from 882KB to 120KB each (−86.3%) versus v1's
JSON+base64 filters sized from configured guesses.

## Methodology

Comparisons are made with `benchstat` over interleaved runs of two compiled
test binaries (before/after), `-count=3` or more per side, on an otherwise
idle machine, so machine drift lands on both sides. Per-phase benchstat
reports (with p-values) are recorded in the git history of this file and in
PLAN.md's status ledgers.

## History

Results older than the August 2026 numbers above — including the 2023-era
load-test transcripts that previously filled this file — predate the v2 file
format, the measured filter sizing, and the cursor query API, and describe a
design that no longer exists. See this file's git history for them.
