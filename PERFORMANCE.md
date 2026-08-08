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

## Block filter access (August 2026)

The benchmarks above put one data block in each file, so they cannot show what
it costs to consult a file's block filters. Files hold many blocks whenever
partitioning is used or after a merge — merge combines blocks only while they
stay under `MaxRowGroupRows`/`MaxRowGroupBytes`, so a 10GB merged file
necessarily holds hundreds of them. The `ManyBlocks*` benchmarks cover that
shape: a partitioned dataset (one block per partition per flush) queried for a
marker token present in one row per file, so every file passes its file-level
filter and all but one block per file is pruned. A wrapping DataStore counts
opens and reads and can charge latency per request.

Filter sections used to sit immediately before their block's row data, costing
one open and one read per block. They now live in one contiguous region per
file, read in chunks of up to 4MiB, and handles are pooled per file.

| Benchmark (per query)           | before               | after               | time         |
| ------------------------------- | -------------------- | ------------------- | ------------ |
| `ManyBlocksNeedleLocal`         | 3.39ms, 128 opens, 136 reads | 1.67ms, 8 opens, 16 reads | −51% (2.0×) |
| `ManyBlocksNeedleRemote`        | 5.68ms, 128 opens, 136 reads | 2.12ms, 8 opens, 16 reads | −63% (2.7×) |
| `ManyBlocksBroadRemote`         | 11.77ms, 128 opens, 256 reads | 8.17ms, 47 opens, 136 reads | −31% (1.4×) |
| `ManyBlocksNeedleFanout`        | 40.89ms, 512 opens, 544 reads | 6.47ms, 32 opens, 64 reads | −84% (6.3×) |
| `ManyBlocksNeedleS3Latency`     | 1416ms, 256 opens, 272 reads | 150ms, 16 opens, 32 reads | −89% (9.4×) |

Request counts are the concurrency-independent result, and on object storage
they are what the bill and the rate limit track. Wall-clock gains depend on
how much of the latency spare fan-out was already hiding: `NeedleRemote` runs
16 workers over 8 files, so most of its request cost was overlapped and only
the local work shows. `Fanout` (32 files, 8-way) and `S3Latency` (20ms per
request, in the range of an S3 GET's time to first byte) are the deployment
shape — files outnumber workers — and there the reduction converts directly.
`BroadRemote` reads every block's row data, so filter access is a small share
of its work and 1.4× is the ceiling for that shape.

Extrapolating to a needle query with a 24h prefilter over 100TB in 10GB merged
files (~30 candidate files, ~1,000 blocks each): DataStore requests drop from
roughly 60,000 to under 100. Bytes moved do not change — the filters still have
to be read — so a cold query becomes limited by filter bytes rather than by
round trips, and a warm filter cache leaves only the surviving block scans.
Two costs this does not address: the block metadata a MetaStore ships per
query (which this change grew by ~42 bytes per block), and the file-level
filter bytes, which the benchmarks do not model at all because both shipped
MetaStores are local.

## Methodology

Comparisons are made with `benchstat` over interleaved runs of two compiled
test binaries (before/after), `-count=3` or more per side, on an otherwise
idle machine, so machine drift lands on both sides. Per-phase benchstat
reports (with p-values) are recorded in the git history of this file and in
PLAN.md's status ledgers.

## History

Results older than the August 2026 numbers above — including the 2023-era
load-test transcripts that previously filled this file — predate the current
file format, the measured filter sizing, and the cursor query API, and
describe a design that no longer exists. See this file's git history for them.

The "v1 baseline"/"current" table measures the engine before and after the
correctness and performance overhaul, when block filter sections still sat
inline before each block's row data. The block filter access numbers were
measured separately, against the commit that introduced their benchmarks.
