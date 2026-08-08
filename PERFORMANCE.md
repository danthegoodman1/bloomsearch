# Performance

Measured with the committed benchmark suite (`bench_test.go`):

```
go test -bench=. -benchmem -run='^$' -count=6
```

Apple M3 Max (darwin/arm64), Go 1.26, `-count=6`, idle machine. Ranges are
benchstat's run-to-run spread.

## Ingest and query

| Benchmark              | time            | allocs | notes                        |
| ---------------------- | --------------- | ------ | ---------------------------- |
| `Ingest` (per row)     | 3.32µs ± 1%     | 21     | 302k rows/s, single actor    |
| `QueryFieldTokenHit`   | 3.08ms ± 3%     | 68.6k  | matches ~20% of 20,000 rows  |
| `QueryTokenMiss`       | 307µs ± 1%      | 531    | file filters disqualify everything |
| `QueryRegex`           | 3.54ms ± 1%     | 64.7k  | bloom-narrowed regex filter  |
| `Merge`                | 22.3ms ± 3%     | 12.8k  | 6 files × 500 rows into one  |

Queries run against 10 files × 2,000 rows. `Ingest` measures end-to-end
batches of 100 through the public API, including tokenization, bloom
insertion, marshaling, compression, and flush.

`QueryTokenMiss` is the hierarchical index at work: file-level bloom filters
disqualify every file, so the query opens no data blocks at all. `Merge`
verifies every source block (CRC and decompression bounds) and rebuilds
filters from row data.

## Block filter access

Files hold many data blocks whenever partitioning is used or after a merge —
merge combines blocks only while they stay under `MaxRowGroupRows` and
`MaxRowGroupBytes`, so a 10GB merged file necessarily holds hundreds. The
`ManyBlocks*` benchmarks cover that shape: a partitioned dataset (one block per
partition per flush) queried for a marker token present in one row per file, so
every file passes its file-level filter and all but one block per file is
pruned. A wrapping DataStore counts requests and can charge latency per
request.

| Benchmark                   | time         | opens | reads | shape                          |
| --------------------------- | ------------ | ----- | ----- | ------------------------------ |
| `ManyBlocksNeedleLocal`     | 1.67ms ± 0%  | 8     | 16    | 8 files × 16 blocks, no latency |
| `ManyBlocksNeedleRemote`    | 2.11ms ± 3%  | 8     | 16    | 250µs/request, 16-way          |
| `ManyBlocksBroadRemote`     | 8.22ms ± 2%  | 47    | 136   | every block's row data read     |
| `ManyBlocksNeedleFanout`    | 6.48ms ± 2%  | 32    | 64    | 32 files, 8-way                 |
| `ManyBlocksNeedleS3Latency` | 150ms ± 1%   | 16    | 32    | 20ms/request, 16 files, 8-way   |

Requests, not wall-clock time, are the concurrency-independent measure of
access-pattern cost, and on object storage they are what the bill and the rate
limit track. A query costs one open per candidate file plus a few reads: block
filter sections live in one contiguous region per file, read in chunks of up to
4MiB, with each chunk starting at the section being consulted so gaps left by
prefiltered blocks are skipped. Row data is then read only for blocks that
survive their filters.

Wall-clock sensitivity to request latency depends on whether spare fan-out
hides it. `NeedleRemote` runs 16 workers over 8 files, so most of its request
cost overlaps. `Fanout` and `S3Latency` run more files than workers — the
deployment shape — so their request cost lands on the critical path. 250µs is
in the range of a local NVMe read; 20ms is in the range of an S3 GET's time to
first byte.

A needle query with a 24h prefilter over 100TB stored as 10GB merged files
(~30 candidate files, ~1,000 blocks each) issues roughly 30 opens and ~200
chunk reads. Bytes moved are unchanged by chunking — the filters still have to
be read — so such a query is bounded by filter bytes rather than round trips,
and a warm filter cache leaves only the surviving block scans.

Two costs these benchmarks do not measure: the block metadata a MetaStore
ships per query (~290 bytes per candidate block), and file-level filter bytes,
since both shipped MetaStores are local and effectively free. A remote
MetaStore makes both significant.
