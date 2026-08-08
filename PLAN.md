# Development Plan

## Overarching Goal

Make BloomSearch's stated guarantees true under adversarial inputs and real (context-honoring, latency-having) storage backends, then make the hierarchical index actually prune at scale. The guarantees kept: no false negatives, immutable self-contained files, streaming results, async ingest with reliable done-channel acks, strict prefilter semantics — with one strengthening: guarantees are enforced by the engine itself, not delegated to store implementations. The public API is not sacred (pre-1.0): the channel-triple `Query` signature and the on-disk bloom encoding both change.

Non-goals: distributed query processing, `CoordinatedMetaStore`, and TTLs remain roadmap items (the docs stop claiming them as shipped); no new storage backends.

## Implementation Principles

- Ingest indexing and query-time row verification must walk one canonical representation — the marshaled JSON bytes — through one shared implementation. Every confirmed false negative traces to these two paths disagreeing.
- The engine owns its guarantees. MetaStore-side prefiltering is an optimization; the engine re-applies prefilters to whatever a store returns.
- Commit points gate on durability: `writer.Close()` success precedes any `MetaStore.Update`; aborted writes tombstone their partial files.
- On-disk format changes bump the file version; readers keep v1 compatibility.
- Metadata describes what was actually built (bloom params from source filters, not current config).
- Libraries do not write to stdout; logging goes through an injectable `*slog.Logger`.
- Every confirmed bug lands with a named regression test in the same change.

## Testing Strategy

- `go test -race ./...` clean is a standing gate from Phase 1 onward (the suite currently fails it via leaked test goroutines).
- Property/fuzz test for the core invariant: for generated rows (all int magnitudes, floats, dotted/metachar keys, nested maps, arrays, nulls, structs), every Field/Token/FieldToken/FieldRegex query derivable from a row returns that row.
- Fault-injection store doubles: erroring/failing-`Close` writer, context-honoring DataStore/MetaStore, abandoned done channels, concurrent flush+query.
- Benchmark baseline (ingest rows/s, scan rows/s per core, allocs/row, bytes on disk) recorded before Phase 5 and re-run after Phases 5–6; PERFORMANCE.md updated from real runs.

## Phase 1: Canonical tokenization — restore "no false negatives"

Goal:
Any row that matches a query is always returned, regardless of value types or field-name characters.

Scope:
- Rewrite ingest indexing to walk the just-marshaled JSON bytes with the same gjson-based walker used by query-time verification; delete the reflection walk (`UniqueFields`/`collectPathsAndValues`, tokenizer.go:29-83). Fixes: int ≥ 1e6 / float64-lossy ints (`%v` exponent notation), structs, `time.Time`, `[]byte`, and any type whose `%v` differs from its JSON encoding.
- Replace `gjson.Result.Get(component)` path lookups with literal-key matching in both walkers (tokenizer.go:122,216) so `*`, `?`, `\` in field names neither wildcard-match other rows (wrong-row leak) nor become unfindable.
- One field-path policy for keys containing the delimiter (`{"a.b":1}` vs `{"a":{"b":1}}`): escape, path-array, or documented rejection — applied identically at ingest and verification.
- One leaf/non-leaf policy: either index intermediate paths in field blooms or make row-level `Field`/`FieldToken`/regex-guard semantics leaf-only — bloom pruning and row verification must agree (today `Field("user")` on `{"user":{"name":...}}` is bloom-pruned but row-matches).
- Regex final filter matches the raw field text (`v.Str`/`v.Raw`), not `fmt.Sprintf("%v", v.Value())` (tokenizer.go:374).
- Consistent policy for `null` / empty-composite values (indexed vs verified) in both walkers.
- MinMax conversions clamp instead of wrap: `uint64 > MaxInt64`, float overflow, NaN (min_max.go:22-24,59-71).

Out of scope:
- Performance tuning of the new walker (Phase 6); on-disk format changes (Phase 5).

Completion gate:
Property test passes: for generated rows covering every divergence class above, every derivable query returns the row through a real flush/query cycle. All named regression tests pass.

Testing plan:
- Named regression tests: large-int token, dotted key, metachar key (both leak and miss directions), non-leaf Field and FieldRegex guard, struct/time/[]byte values, null field, uint64 minmax overflow.
- Property/fuzz test wired into `go test`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Ingest indexes from marshaled JSON via shared walker; reflection walk deleted | `forEachPathValue`/`leafTokenInput` in tokenizer.go; ingest walks marshaled bytes in `processIngestRequest`; `UniqueFields` deleted. Tests: `TestFieldTokenLargeInt`, `TestValueTypeEncodings`. |
| Complete | Work | 1B: Literal-key walking replaces gjson path Get in both walkers | ForEach-only traversal, zero `Result.Get` path lookups. Test: `TestFieldMetacharKeys` (leak + miss directions). |
| Complete | Work | 1C: Delimiter-in-key policy decided and applied both sides | Delimiter-split prefixes emitted as field paths via single-sourced `emitKeyPrefixPaths`; empty paths nonexistent for all condition types. Test: `TestFieldWithDelimiterInKey` (incl. regex-guard symmetry, reviewer's repro). |
| Complete | Work | 1D: Leaf/non-leaf field semantics unified across blooms, row check, regex guard | Intermediate paths in field bloom; exact-path FieldToken (documented on builders). Test: `TestFieldNonLeafPath`. |
| Complete | Work | 1E: Regex matches raw field text | `leafTokenInput` (.Str/.Raw) feeds regex; no `%v`. Test: `TestRegexNumericField`. |
| Complete | Work | 1F: Null/empty-value policy consistent | Null/empty containers = field existence only, both sides. Test: `TestValueTypeEncodings` null case. |
| Complete | Work | 1G: MinMax conversions clamp (uint64/float/NaN) | min_max.go clamps before conversion, NaN skipped; `EvaluateMinMaxCondition` saturation at int64 extremes. Test: `TestMinMaxOverflowClamp` (7 boundary cases + e2e). |
| Complete | Test | Property test: every derivable query returns its row | `TestPropertyNoFalseNegatives`: ~2700 derived queries incl. independent delimiter-split-prefix derivation (non-circular). |
| Complete | Gate | All Phase 1 regressions + property test green under `-race` | `go test -race -count=1 ./...` pass (19.4s, coordinator-verified). Reviewer approved after one blocker round (regex-guard prefix divergence, fixed in shared walker). Benchmarks vs baseline: ingest 33% faster/−68% allocs; queries within tolerance. |

## Phase 2: Engine-enforced prefilters and honest reference stores

Goal:
Strict prefilter semantics hold regardless of MetaStore implementation, and the shipped stores are safe to use as documented.

Scope:
- `Query` applies `FilterDataBlocks(file.Metadata.DataBlocks, query.Prefilter)` to every returned `MaybeFile` before enqueueing jobs (bloom_tree_engine.go:1040-1054); MetaStore filtering becomes an optimization, documented as such (meta_store.go:9-17).
- `MemoryMetaStore`: add `sync.RWMutex` (flushWorker/inline-flush/merge/query all touch it concurrently today — map-race panic); reconcile the `NewSimpleMetaStore`/`MemoryMetaStore`/`simple_meta_store.go` naming.
- `FileSystemDataStore`: unreadable files are skipped and logged, not query-fatal (comment already claims this, testing_file_system_store.go:152-157); in-progress files never visible to scans (write under a non-`.dat` temp name, rename on successful Close); rename file/type to drop the "testing" signal since README presents it as the reference implementation.
- Export the prefilter/bloom/regex expression trees (exported fields or accessor/visitor API) so third-party MetaStores can actually translate them (all condition content is unexported today, query.go:59-78,470-497); give them lossless JSON round-trips (current tags serialize to empty objects — a remote MetaStore would silently get an always-true prefilter).

Out of scope:
- New MetaStore implementations; changing `MaybeFile`'s shape beyond what enforcement requires.

Completion gate:
A partition-prefiltered query through `MemoryMetaStore` returns only matching-partition rows; queries run concurrently with continuous flushes against `FileSystemDataStore` never fail on partial files.

Testing plan:
- Prefilter-through-MemoryMetaStore regression (currently returns rows from all partitions).
- Concurrent flush+query loop test; unreadable-garbage-file-in-dir test.
- Expression-tree JSON round-trip test; example SQL-translation exercising the exported tree.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: Engine-side FilterDataBlocks on every MaybeFile | Enforcement in `Query` before file-level bloom eval; red-before-green proven via prefilter-ignoring wrapper store. Test: `TestPrefilterEnforcedByEngine` (partition + minmax, both stores). |
| Complete | Work | 2B: MemoryMetaStore mutex + naming reconciliation | `sync.RWMutex` + store-side FilterDataBlocks; renamed to memory_meta_store.go / `NewMemoryMetaStore`. Test: `TestMemoryMetaStoreConcurrentAccess` under `-race`. |
| Complete | Work | 2C: FileSystemDataStore skips unreadable files; temp-name + rename-on-close; de-"testing" rename | Reservation-first CreateFile (O_EXCL final-path reservation, Sync→Close→Rename), unreadable/vanished files skipped; renamed to file_system_store.go. Tests: `TestFileSystemStoreSkipsUnreadableFiles`, `TestFileSystemStoreConcurrentFlushQuery` (deterministic, 10/10), `TestFileSystemStoreCreateFileNeverClobbersExisting` (red-check verified). Measured fsync cost ~3.9ms/flushed file, amortized invisible. |
| Complete | Work | 2D: Expression trees exported with lossless JSON round-trip | Exported fields/constants on all three trees; byte-identical re-marshal + semantic-equality tests incl. must-be-false cases. Tests: `TestQueryExpressionJSONRoundTrip`, `TestPrefilterExpressionSQLTranslation`. |
| Complete | Gate | Prefilters hold with every shipped store, concurrently | `go test -race -count=1 ./...` ok (30.3s, coordinator-verified); reviewer approved after one blocker round (final-path clobber, fixed via reservation). Discovered Flush-ordering bug recorded in Phase 3A. Benchmarks neutral vs Phase 1. |

## Phase 3: Lifecycle, durability, and merge safety

Goal:
No acknowledged row is ever lost, no waiter ever hangs, and merge cannot commit a bad file or duplicate rows.

Scope:
- Stop path: drain `ingestChan` before the final flush (queued requests are silently dropped today, bloom_tree_engine.go:367-383 — acked rows lost, doneChan waiters and `Flush()` hang forever); run the shutdown flush with `Stop`'s context, not the canceled `b.ctx` (bloom_tree_engine.go:735,810); done-channel delivery must not race ctx cancellation or skip remaining channels on first error (chan_helpers.go:37-55).
- `Flush()` ordering (found by Phase 2's concurrent flush/query test): an empty-buffer force flush acks immediately with no ordering against flush requests still queued or in flight on the flush worker — `Flush()` can return before previously ingested rows are durable. `Flush` must not ack until all earlier flush work has completed.
- Flush error paths: close the writer, `TombstoneFile` the partial output, and deliver the error (every error return after `CreateFile` currently leaks both, bloom_tree_engine.go:749-818).
- Batch atomicity: marshal/validate all rows before mutating buffers or blooms so a mid-batch error doesn't half-persist the batch (bloom_tree_engine.go:586-597); fix the nil-compression-encoder poisoned-partition panic (bloom_tree_engine.go:513-531).
- Merge commit safety: check `writer.Close()` before `MetaStore.Update` (ignored via defer today, bloom_tree_engine.go:1630 — can delete sole copies after a failed S3 finalize); tombstone the merge output on any pre-commit failure; in-process single-flight guard on `Merge` (concurrent merges commit duplicate rows today); distinguish "merge committed, GC failed" from "merge failed" in the return.
- Merge read correctness: per-reader file handles or section readers (blocks from the same source file share one seeking handle today, interleaving reads, bloom_tree_engine.go:1637-1659); stamp merged file/block metadata with source-filter bloom params, not current config (bloom_tree_engine.go:1682-1683,1914-1915); make minmax key-set compatibility part of `dataBlocksAreMergeable` or document the widening drift.
- Lifecycle hardening: idempotent/guarded `Start` (double Start panics at Stop via double-close of `ingestDone`); empty-row-slice ingest acks immediately and creates no empty partition buffer/0-row block; `triggerFlush` overflow blocks on the channel or spawns a bounded goroutine instead of flushing inline on the ingest actor.
- Config validation for every knob at construction: `MaxRowGroupRows/Bytes > 0` (negative wraps `uint()` today), `MaxBufferedTime > 0`, `ZstdCompressionLevel` range, `RowDataCompression` normalized (`""` → `CompressionNone`); read path accepts `""` for already-written files (currently every query on such blocks errors).

Out of scope:
- Cross-process merge coordination (roadmap); crash-recovery GC sweep of orphans beyond tombstoning at failure time.

Completion gate:
Under load, `Stop` loses zero acknowledged rows and every done channel fires exactly once; fault-injection merges (failing Close, mid-write errors) never mutate the metastore; concurrent `Merge` calls produce no duplicates.

Testing plan:
- Stop-under-load test with requests queued in `ingestChan` and a context-honoring fake store.
- Fault-injection flush/merge tests (failing writes, failing Close, failing Update) asserting tombstones and no metastore mutation.
- Concurrent-Merge duplication regression; same-file two-block merge regression (after a limits increase); double-Start, empty-ingest, abandoned-doneChan tests.
- Config validation table test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: Stop drains ingestChan; shutdown flush uses live ctx; reliable done delivery; Flush acks only after all earlier flush work completes | Accepting-flag (`stateMu`+`stopped`) closes the enqueue race; engine-owned `flushCtx` + Stop `AfterFunc` deadline abort (stronger than packet spec); `SendToChannelsWithContext` attempts every channel; single-flusher FIFO with blocking `triggerFlush`. Tests: `TestStopUnderLoadLosesNoAckedRows` (ctx-checking store), `TestFlushWaitsForInFlightFlushes` (count=10), `TestIngestAfterStopReturnsError`, `TestStopHonorsDeadlineWhenFlushWorkerWedged`. |
| Complete | Work | 3B: Flush error paths close writer + tombstone + deliver error | `abortFileWriter` + optional `Abort()` interface; FS store Abort + dir fsync + TombstoneFile removes .dat/reservation/.tmp. Tests: `TestFlushErrorPathsAbortAndReport`, `TestFlushUpdateFailureTombstonesOrphan`, `TestTombstoneRemovesAllArtifacts`. |
| Complete | Work | 3C: Batch atomicity + nil-encoder poisoned-partition fix | All rows marshaled before any buffer/bloom mutation (same bytes feed walker and buffer — Phase 1 invariant reviewer-verified); encoder created before buffer registration. Test: `TestBatchAtomicityOnMarshalError`. |
| Complete | Work | 3D: Merge gates commit on Close; tombstones aborted output; single-flight; GC-failure distinct | Close checked before Update; `ErrMergeInProgress` TryLock; `ErrPostCommitCleanup` sentinel. Tests: `TestMergeAbortsOnCloseFailure`, `TestConcurrentMergeSingleFlight`, `TestMergeUpdateFailureTombstonesOutputsKeepsSources`, `TestMergePostCommitTombstoneFailure`. |
| Complete | Work | 3E: Per-reader handles in merge; source-param metadata stamping; minmax key-set rule | Per-reader `OpenFile`; file params from group[0], block params from sources; equal-key-set mergeability. Red-checked vs HEAD (unexpected EOF / wrong params / widened visibility). Tests: `TestMergeSameFileBlocks`, `TestMergeStampsSourceParams`, `TestMergeMinMaxKeySetIncompatible`. |
| Complete | Work | 3F: Start guard; empty-ingest ack; flush overflow off the ingest actor | Idempotent Start; empty ingest acks with no 0-row blocks; inline-flush path deleted (FIFO). Tests: `TestDoubleStartStopSafe`, `TestEmptyIngestAcksImmediately`. |
| Complete | Work | 3G: Full config validation; `""` compression normalized and readable | Every knob validated (`ErrInvalidConfig`); `""` normalized at construction, `normalizeCompression` on both read paths. Tests: `TestConfigValidationTable`, `TestEmptyCompressionReadable`. |
| Complete | Gate | Zero acked-row loss, exactly-once done delivery, no bad merge commits | `go test -race -count=1 ./...` ok (42.7s, coordinator-verified); 9-test concurrency set at `-race -count=3` ×5 loops + reviewer ×2, zero flakes. Two review rounds (blocker: flaky wedged-Stop test → engine-side post-deadline flush abandonment). Benchmarks: ingest neutral, merge +6-11% (dir fsync + per-reader handles, accepted durability cost). |

## Phase 4: Query API reshape

Goal:
A query API whose misuse is hard: unambiguous completion, deterministic error delivery, no caller-owned channel hazards.

Scope:
- Replace `Query(ctx, q, resultChan, errorChan, statsChan)` with an engine-owned cursor (`Results` with `Next/Row/Err/Stats`, or `iter.Seq2`). Kills in one move: double-close panic on channel reuse, closed-`resultChan`-but-errors ambiguity, never-closed `errorChan`/`statsChan` (the repo's own tests leak goroutines on these — `-race` fails today), and the undocumented must-drain-both-channels deadlock.
- Canceled queries are distinguishable from complete ones (ctx cancellation surfaces as a terminal error; today dropped files yield silent partial results, bloom_tree_engine.go:978-1007). Note from Phase 3: `SendWithContext` now try-sends before honoring cancellation, so a canceled query whose consumer keeps draining keeps receiving rows — Phase 4 defines cancellation delivery semantics deliberately.
- Decide and document worker semantics on block error: stop-query vs skip-block (doc comment currently contradicts the code).
- Workers release the global `querySemaphore` while blocked sending to a slow consumer, or the semaphore becomes per-query with a global cap (today one slow consumer parks global slots and starves unrelated queries, bloom_tree_engine.go:1029-1036,1193).
- `BlockStats` reports actual scanned rows/bytes (skipped blocks currently report full counts, inflating PERFORMANCE.md); stats delivery contract defined (lossy or not) and stats always terminated.
- Matched rows built from the already-parsed gjson value instead of a second `json.Unmarshal` (bloom_tree_engine.go:1187-1191).

Out of scope:
- Keeping a channel-based variant unless a concrete consumer needs it.

Completion gate:
No caller-visible channel ownership remains; full test suite passes `-race` (leaked-goroutine failures gone); README examples compile.

Testing plan:
- Port all engine tests to the new API; add slow-consumer test proving unrelated queries proceed.
- Cancellation test asserting a terminal error, not silent partials.
- Doc-example compile test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: Cursor/iterator Query API; engine owns all channels | `Query(ctx, q) (*Results, error)` in query_results.go (Next/Row/Err/Stats/Close); old signature deleted; all tests ported assertion-for-assertion (reviewer-verified zero weakening). Test: `TestQueryCursorBasic`. |
| Complete | Work | 4B: Cancellation → terminal error; error semantics decided + documented | Skip-and-report per block (`errors.Join`); ctx cancel terminal with buffered rows dropped; setup-phase cancel returns `(nil, err)`. Tests: `TestQueryCursorCancellation`, `TestQueryCursorBlockErrorContinues`, `TestQueryCursorCloseEarly`. |
| Complete | Work | 4C: Semaphore not held while blocked on consumer | try-send/release/blocking-send/re-acquire in `deliver`; leak-free on every exit path (reviewer-enumerated). Red-checked vs HEAD starvation. Test: `TestQueryCursorSlowConsumerNoStarvation`. |
| Complete | Work | 4D: Accurate BlockStats; defined delivery contract | Actual scanned rows/bytes (0 when skipped); lossless mutex-guarded collection; `QueryStats` aggregates. Test: `TestBlockStatsAccuracy`. |
| Complete | Work | 4E: Single-parse row materialization | `rowValue.Value()` map (checked assertion); second `json.Unmarshal` deleted. FieldTokenHit allocs −31% (p=0.029, reviewer-reproduced), Regex −24%. Test: `TestQueryRowMaterializationEquivalence` (incl. 2^53+1, 1e20). |
| Complete | Gate | Suite `-race` clean; docs compile | `go test -race -count=1 ./...` ok (46.2s, coordinator-verified); concurrency set `-race -count=3` zero flakes; README Quick Start + Query-path prose ported to cursor (compiling `Example` in example_test.go); one review round (blocker was the stale README). |

## Phase 5: Bloom effectiveness and file format v2

Goal:
The hierarchical index actually prunes at scale, filters are cheap to read, and corrupt data is rejected before any row reaches a caller.

Scope:
- Size filters from measured entry counts, not row counts: filters receive fields + tokens + field:token pairs per row (10–50× rows), so defaults saturate to ~100% FPR — the PERFORMANCE.md benchmark pruned 0 of 100 blocks. Separate `BlockBloomExpectedItems`/`FileBloomExpectedItems` (per-entry) knobs, validated against buffer limits; record actual insert counts in block metadata for observability.
- Binary bloom encoding (`bloom.WriteTo/ReadFrom`) for block filters and the footer, replacing JSON+base64 (+33% size; filter JSON was ~43% of file bytes and ~90% of a skipped block's query time in the benchmark). File version 2; v1 remains readable.
- Skip the block-filter read entirely when the prune query has no bloom conditions (read unconditionally today, bloom_tree_engine.go:1104-1118); drop `Metadata.BloomFilters` from `MaybeFile`s after the file-level test so per-query memory stops scaling with candidate-file count (README claims it doesn't; it does).
- Verify-before-emit: read the compressed block fully (bounded by block Size), CRC-check, then decompress and scan from memory — corrupt rows can currently stream to callers before the trailing hash check; bound per-row length and decompressor output by `UncompressedSize` (a corrupt 4-byte prefix can force a ~4 GiB allocation today); replace the `RowDataHash == 0` "no hash" sentinel with an explicit presence flag in v2 metadata.
- Merge: rebuild filters from row data when source params differ or fill ratio exceeds a threshold — implementing the README's claimed behavior (today mismatched-param files can never merge, and OR-merging saturated filters preserves saturation). If de-scoped, the README claim is removed instead.

Out of scope:
- Alternative filter structures (split-block, ribbon) — only if rebuild-at-merge proves insufficient.

Completion gate:
On a selective-query benchmark at realistic scale, file/block pruning skips >0 blocks (vs 0 today) and measured FPR tracks the configured rate; v1 files remain queryable; corrupt-block tests error before emitting any row.

Testing plan:
- v1/v2 cross-version read tests; corruption tests (flipped bit in filters, row data, length prefix).
- FPR measurement test at defaults; benchmark before/after for file size, filter-read time, per-query memory.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 5A: Entry-count-based filter sizing + observability counts | `bloomEntrySets` at ingest, exact-sized filters at flush; `FileBloomExpectedItems` knob and `BloomExpectedItems` metadata deleted (filters self-describe m/k); `BloomEntryCounts` recorded. Tests: `TestMeasuredFilterSizing`, `TestFalsePositiveRateWithinBudget` (measured ~0.011 vs 0.01 configured). |
| Complete | Work | 5B: Binary filter encoding; format v2 with v1 read compat | Presence-flagged binary sections with CRC; metadata JSON filter-free; version dispatch (content-based for block sections — reviewer-verified unambiguous); FILE_FORMAT.md rewritten and hex-dump-verified. Tests: `TestV1FilesRemainReadable`, `TestV2FormatRoundTrip`, `TestV1FilterSectionInsideV2Container`. Storage: −86.3% (882KB→120KB/file on bench dataset). |
| Complete | Work | 5C: Conditional filter reads; filters dropped post-file-test | Block filter reads skipped when no bloom conditions; file filters nil'd before job enqueue. Tests: `TestNoFilterReadWhenNoBloomConditions` (byte-range tracker + positive control), `TestFileFiltersReleasedAfterFileTest`. |
| Complete | Work | 5D: Verify-before-emit; bounded allocations; explicit hash-presence flag | `readBlockRowData`/`blockRowScanner`: CRC before decompression, exact `UncompressedSize` buffer, bounded row prefixes; `HasRowDataHash` (v1 0-sentinel translated); unified block reader (Phase 7C half done early). Tests: corruption subtests in `TestV2FormatRoundTrip`, `TestOversizeRowLengthRejected`. |
| Complete | Work | 5E: Merge-time filter rebuild | Mergeability drops bloom-param rules; merged blocks rebuild filters from re-streamed rows; raw-copied blocks verified (filter section + row data) and re-streamed for entry collection so file-level filters stay strong. Tests: `TestMergeRebuildsFilters`, `TestMergeAbortsOnCorruptSourceBlock` (3 subtests, red-checked), `TestMergeStampsRebuiltParams`. |
| Complete | Gate | Selective benchmark shows real pruning; v1 compat; no corrupt row emitted | Reviewer-reproduced (interleaved benchstat): TokenMiss 75.9→0.28ms (−99.6%, file-level pruning works), Hit 86.3→7.2ms (−91.7%), Regex −88.7%, Ingest −36% time (+47% allocs, honest tradeoff), Merge −60% (+722% allocs → Phase 6). `go test -race` ok (7.6s, coordinator-verified). One review round, approve-with-should-fixes all landed. |

## Phase 6: Hot-path performance

Goal:
Materially higher ingest and scan throughput via allocation elimination, measured against the Phase 5 baseline.

Scope:
- Ingest (single-threaded actor caps throughput): the Phase 1 gjson-walk indexer tuned for zero reflection and minimal allocation — reusable path buffer, one `field::token` key build per token (built twice today), stack-array length prefix, scratch maps reused across rows; drop the `sync.Pool` of 3-word `ingestRequest` structs (bookkeeping without benefit).
- Scan: compile the query once per query — pre-split field paths, pre-lowered target tokens, zero-alloc case-insensitive token matcher (replacing per-value `ToLower`+`Fields` allocation), `v.Str` fast path for strings; avoid the per-row heap copy in `gjson.ParseBytes` (dedicated buffer per row or unsafe view); evaluate multi-condition expressions in fewer walks.
- Codec churn: pool zstd/snappy encoders/decoders with `Reset`, encoder/decoder concurrency 1 (each block job currently constructs multi-goroutine codec state; 1000 concurrent jobs → allocation storms).
- Merge: write-then-advance in `getCurrentRow` to eliminate the per-row copy (bloom_tree_engine.go:2060-2070); reuse the length-prefix buffer; index blocks by (partition, params) to replace O(n²) grouping; drop the O(candidates × groups) print loop.

Out of scope:
- Parallelizing the ingest actor; changing the row encoding (still length-prefixed JSON).

Completion gate:
Recorded before/after benchmarks show ≥2× single-core scan throughput and ≥50% allocs/row reduction on both ingest and scan paths (targets confirmed against the Phase 5 baseline); no correctness test regresses.

Testing plan:
- `go test -bench` suite covering ingest, scan (hit and miss paths), merge; `benchstat` before/after artifacts committed to PERFORMANCE.md.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: Zero-reflection, low-alloc ingest indexing | Missing: implementation + ingest benchmark delta. |
| Incomplete | Work | 6B: Compiled per-query matcher; zero-alloc token compare; no per-row copy | Missing: implementation + scan benchmark delta. |
| Incomplete | Work | 6C: Pooled codecs with single-threaded settings | Missing: implementation + zstd-path benchmark delta. |
| Incomplete | Work | 6D: Merge copy elimination + near-linear grouping | Missing: implementation + merge benchmark delta. |
| Incomplete | Gate | Benchmarks hit targets; PERFORMANCE.md regenerated from real runs | Missing: benchstat artifacts. |

## Phase 7: Surface hygiene and documentation truth

Goal:
The public API is intentional, the docs describe the code that exists, and the library is silent by default.

Scope:
- Injectable `*slog.Logger` (no-op default); delete all ~25 `fmt.Printf/Println` sites, including the per-block nil-filter warnings and O(files) merge prints.
- API surface prune: unexport/delete `TryWriteToChannels` and unused channel helpers, `FormatRate`/`FormatBytesPerSecond` (test-only), string-based `TestJSONFor*` duplicates, `hashCalculatingReader.Sum64`, write-only `MaybeFile.Size`; move `NullDataStore`/`NullMetaStore` to test files; remove the redundant internal limit in `hashCalculatingReader`.
- File-format API home: move footer write (`writeFileMetadataAndFooter`) and footer read (currently private to the FS store) plus the block row reader into `file_format.go` as public, reusable functions — external stores currently must reimplement footer parsing; unify `processDataBlock` and `dataBlockRowReader` on one block-reading implementation.
- Split `bloom_tree_engine.go` (2146 lines) into ingest/flush/query/merge files; reconcile the `bloom_tree_engine.go` filename with the `BloomSearchEngine` type.
- Docs truth pass: README Quick Start compiles (missing `err` return, missing `statsChan` arg today); TTLs, merge-time rebuild (unless Phase 5E shipped it), `CoordinatedMetaStore`, and distributed queries marked roadmap; document done-ack durability semantics (`Close` + `Update`, fsync policy is the DataStore's), leaf-path field semantics from Phase 1D, block-granularity minmax semantics, and engine context lifecycle (query-after-Stop behavior).

Out of scope:
- Multi-package restructuring; renaming the module.

Completion gate:
Zero stdout writes from library code; README/PERFORMANCE claims each verifiable against code; examples compile in a doc test.

Testing plan:
- Compile-checked example (`Example*` funcs) mirroring the README.
- Grep gate for `fmt.Print` in non-test files.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: slog injection; stdout eliminated | Missing: implementation + grep gate. |
| Incomplete | Work | 7B: API surface pruned | Missing: diff + `go vet`/apidiff note. |
| Incomplete | Work | 7C: Public footer/block reader in file_format.go; single block-read implementation | Missing: implementation + external-store usage example. |
| Incomplete | Work | 7D: Engine file split | Missing: mechanical refactor, suite green. |
| Incomplete | Work | 7E: README/PERFORMANCE truth pass + Example funcs | Missing: doc diff + compiling examples. |
| Incomplete | Gate | Docs verifiable; examples compile; no stdout | Missing: doc test + grep gate run. |
