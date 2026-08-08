#  BloomSearch <!-- omit in toc -->

**Keyword search engine with hierarchical bloom filters for massive datasets**

BloomSearch provides low memory usage and low cold-start searches through pluggable storage interfaces.

- **Memory efficient**: Bloom filters are sized from the measured distinct entries of the rows they cover, so the configured false positive rate holds at any data volume; queries release file-level filters as soon as they are tested
- **Pluggable storage**: DataStore and MetaStore interfaces for any backend (can be same or separate)
- **Fast filtering**: Hierarchical pruning via partitions, minmax indexes, and bloom filters
- **Flexible queries**: Search by `field`, `token`, or `field:token` with AND/OR combinators, plus a regex final filter
- **Disaggregated storage and compute**: Decoupled resources allow for asymmetric scaling
- **Silent by default**: the engine never writes to stdout/stderr; diagnostics go to an injectable `slog.Logger` (discarded unless configured)

Perfect for logs, JSON documents, and high-cardinality keyword search.



## Quick start

```bash
go get github.com/danthegoodman1/bloomsearch
```

```go
// The filesystem store implements both the MetaStore and the DataStore.
store := NewFileSystemDataStore("./data")

// Create engine with default config
engine, err := NewBloomSearchEngine(DefaultBloomSearchEngineConfig(), store, store)
if err != nil {
    log.Fatal(err)
}
engine.Start()

// Ingest is asynchronous: pass nil to fire-and-forget, or provide a
// buffered `chan error` that receives nil once the rows are durable
// (or the error that prevented it).
doneChan := make(chan error, 1)
err = engine.IngestRows(ctx, []map[string]any{{
    "level":   "error",
    "message": "database connection failed",
    "service": "auth",
}}, doneChan)
if err != nil {
    log.Fatal(err) // rows were not accepted: engine stopped, or ctx canceled on a full buffer
}

// Force a flush and wait for it (and everything queued before it).
if err := engine.Flush(ctx); err != nil {
    log.Fatal(err)
}
if err := <-doneChan; err != nil {
    log.Fatal(err)
}

// Stream the matching rows through the engine-owned cursor
results, err := engine.Query(
    ctx,
    // Query for rows where `.level: "error"`
    NewQuery().FieldToken("level", "error").Build(),
)
if err != nil {
    log.Fatal(err)
}
defer results.Close() // safe to call at any point; ends the query early

for results.Next() {
    // Process matching row
    fmt.Printf("Found row: %+v\n", results.Row())
}
// Err is nil on clean completion; failed blocks report joined errors here
// (the query continues past them), and a canceled query reports its
// context error
if err := results.Err(); err != nil {
    log.Fatal(err)
}

// Stop with a deadline: ctx expiry is the only way a wedged shutdown aborts.
stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := engine.Stop(stopCtx); err != nil {
    log.Fatal(err)
}
```

The compile- and output-checked version of this walkthrough lives in [`example_test.go`](./example_test.go). See the other tests for complete working examples, including partitioning and minmax index filtering.

- [Quick start](#quick-start)
- [Concepts](#concepts)
  - [Bloom filters](#bloom-filters)
  - [Search types](#search-types)
  - [Search semantics](#search-semantics)
  - [Data files](#data-files)
    - [Partitions](#partitions)
    - [MinMax Indexes](#minmax-indexes)
  - [Merging](#merging)
    - [Coordinated Merges (roadmap)](#coordinated-merges-roadmap)
    - [TTLs (roadmap)](#ttls-roadmap)
  - [DataStore](#datastore)
  - [MetaStore](#metastore)
  - [Write path](#write-path)
  - [Query path](#query-path)
    - [Distributed Query Processing (roadmap)](#distributed-query-processing-roadmap)
- [Performance](#performance)
- [Contributing](#contributing)
  - [AI Code](#ai-code)

## Concepts

### Bloom filters

[Bloom filters](https://en.wikipedia.org/wiki/Bloom_filter) are a probabilistic data structure for testing set membership. They guarantee no false negatives but allow tunable false positives.

BloomSearch sizes every filter at flush/merge time from the measured number of distinct entries it will hold (fields, tokens, and field:token pairs), so filter size tracks the actual content of each block and file, and the configured false positive rate (`BloomFalsePositiveRate`) holds by construction. The measured counts are recorded in the file metadata (`BloomEntryCounts`) for observability.

### Search types

BloomSearch supports three types of searches against JSON documents:

Given example log records:
```json
{"level": "error", "service": "auth", "message": "login failed", "user_id": 123}
{"level": "info", "service": "payment", "message": "payment processed", "amount": 50.00}
{"level": "error", "service": "payment", "message": "database timeout", "retry_count": 3}
```

**Field search** - Find records containing a specific field path:
```go
// Find all records with "retry_count" field
query := NewQuery().Field("retry_count").Build()
```

**Token search** - Find records containing a value anywhere:
```go
// Find all records containing "error" in any field
query := NewQuery().Token("error").Build()
```

**Field:token search** - Find records with a specific value in a specific field:
```go
// Find all records where `.service: "payment"`
query := NewQuery().FieldToken("service", "payment").Build()
```

**Field regex search (final scan stage)** - Apply regex to the full value of a specific field path:
```go
// Find rows where `.message` contains timeout-like text
query := NewQuery().
    FieldRegex("message", "(?i)timeout|connection reset").
    Build()
```

Regex filters support boolean composition and are always evaluated as a final-stage AND filter after normal bloom matching:
```go
// Bloom narrowing first, then regex final filter:
// finalMatch = bloomMatch AND regexMatch
query := NewQuery().
    Field("service").
    MatchRegex(
        RegexAnd(
            RegexOr(
                FieldRegex("service", "^payment$"),
                FieldRegex("service", "^auth$"),
            ),
            FieldRegex("message", "timeout"),
        ),
    ).
    Build()
```

Regex evaluation targets full field value strings (not tokenizer tokens), and matches leaves at or beneath the condition's field path.

**Complex combinations**:
```go
// (field AND token) OR fieldtoken
query := NewQuery().
    Match(
        Or(
            // And(Field, Token) means "field exists" AND "token exists anywhere" (can be different fields).
            And(
                Field("retry_count"),
                Token("error"),
            ),
            // FieldToken means the token must be present in this specific field.
            FieldToken("service", "payment"),
        ),
    ).
    Build()

// (service OR level) AND token:error
query := NewQuery().
    Match(
        And(
            Or(
                Field("service"),
                Field("level"),
            ),
            Token("error"),
        ),
    ).
    Build()
```

`Match(...)` takes a boolean expression tree built from `And(...)` and `Or(...)`.
Simple chained calls like `.Field(...).Token(...)` still default to implicit `AND`.

Queries can be combined with AND/OR operators and filtered by [partitions](#partitions) and [minmax indexes](#minmax-indexes).

### Search semantics

Indexing and row verification walk one canonical representation of each row — its marshaled JSON bytes — through one shared walker, which is what makes "no false negatives" hold. The rules that fall out of that walk:

- **Field paths** are object keys joined with the delimiter (`.`). Keys are always treated literally: `*`, `?`, and `\` in field names are ordinary characters, never wildcards.
- **`Field` matches intermediate paths too**: `Field("user")` matches `{"user": {"name": "x"}}`, because container paths are indexed as field-existence entries.
- **`FieldToken` is exact-path**: `FieldToken("user", "john")` does not match `{"user": {"name": "john"}}` — use `FieldToken("user.name", "john")` or `Token("john")`.
- **Keys containing the delimiter collide with the equivalent nested path**: `{"a.b": 1}` and `{"a": {"b": 1}}` both produce the path `a.b` (and both emit the prefix path `a`), so they are indistinguishable to `Field`/`FieldToken`/`FieldRegex`. This is accepted and intentional.
- **Array elements contribute under the array's own path**; indices are ignored: `FieldToken("tags", "admin")` matches `{"tags": ["admin", "user"]}`.
- **An empty field path matches nothing**, for every condition type.
- **Leaf values are canonicalized before tokenizing**: strings use their decoded text, numbers use their raw JSON literal (`9007199254740993` stays exact — no float64 round-trip), booleans become `"true"`/`"false"`, and `null` contributes field existence but no tokens. The tokenizer always receives a string.
- **The default tokenizer** (`BasicWhitespaceLowerTokenizer`) splits on whitespace and lowercases, so `Token("error")` matches `"Error occurred"`. A custom `ValueTokenizerFunc` replaces it for both indexing and verification.

### Data files

Data files are designed for single-pass writing with row groups, similar to Parquet. They include minmax indexes for quick pruning and support partitions like ClickHouse.

Files are self-contained and immutable. Bloom filters are stored in a compact binary encoding, sized from the measured distinct entries of each block and of the whole file.

See [FILE_FORMAT.md](./FILE_FORMAT.md) for details, including the public helpers (`ReadFileMetadata`, `WriteFileFooter`, `ReadDataBlockBloomFilters`, `ReadDataBlockRowData`) that external store implementations can use instead of reimplementing the framing.

#### Partitions

Partitions enable eager pruning before bloom filter tests. Each data block belongs to one partition:

```
                 File Metadata
                      │
        ┌─────────────┼─────────────┐
        │             │             │
     [202301]      [202302]     [202303]
     Jan 2023      Feb 2023     Mar 2023
       logs          logs         logs
```

They can be specified with a `PartitionFunc`:

```go
// Partition by year-month from timestamp
func TimePartition(row map[string]any) string {
    if ts, ok := row["timestamp"].(int64); ok {
        return time.Unix(ts/1000, 0).Format("200601") // YYYYMM
    }
    return ""
}

config.PartitionFunc = TimePartition
```

Partitions are optional at ingest. With strict prefilter semantics, queries with partition conditions exclude files/blocks without partition IDs.

#### MinMax Indexes

Track minimum and maximum values for numeric fields, enabling range-based pruning:

```go
config.MinMaxIndexes = []string{"timestamp", "response_time"}

// Query with range filter and bloom conditions
query := NewQuery().
    MatchPrefilter(
        PrefilterAnd(
            MinMax("timestamp", NumericBetween(start, end)),
            MinMax("response_time", NumericLessThan(1000)),
        ),
    ).
    FieldToken("level", "error").
    Build()
```

Use `MatchPrefilter(...)` with `PrefilterAnd(...)` / `PrefilterOr(...)` for prefilter logic.

MinMax indexes are optional at ingest. With strict prefilter semantics, queries with MinMax conditions exclude files/blocks without matching minmax metadata.

Prefilters prune at **block granularity**: they decide which data blocks are scanned, not which rows are returned. Rows inside a surviving block are only tested against the bloom and regex conditions, so a block whose range overlaps the query can still return rows outside the minmax condition's range. Pair a minmax prefilter with row-level conditions (or filter client-side) when exact range semantics matter.

### Merging

Merging files reduces metadata operations (file opens, bloom filter tests) and improves query performance.

Bloom filter parameters play no role in merge eligibility: merged blocks rebuild their filters from the row data, sized from the measured distinct entries of the merged rows. Two blocks are mergeable if they share the same partition ID and the same set of minmax index keys, and combined they stay within the row group limits (`MaxRowGroupRows`, `MaxRowGroupBytes`); files group together when they contain at least one mergeable block pair and their combined size stays under `MaxFileSize`.

The merge rewrites data block by block, with memory bounded per block rather than per file. Blocks that merge are decompressed and their rows re-streamed: every row feeds the rebuilt filters, and the combined rows are recompressed with the engine's current compression config (so differing compression settings are supported and consolidated). Because a block's filter section precedes its row data on disk but is only known after every row has streamed through, the merged block's compressed row data is buffered in memory until the block completes — bounded by `MaxRowGroupBytes`. Blocks with no merge partner are copied verbatim (after their filter-section CRC and row-data hash are verified), then re-streamed once for entry collection so the output file's rebuilt file-level filters cover them too. A corrupt source block fails the merge instead of propagating into the output.

Commit is gated on durability: the output file's `Close` must succeed before the MetaStore atomically adds the new file and removes the old ones, and only after that commit are the source files tombstoned. A merge that fails partway tombstones its own partial output and leaves every source untouched. `Merge` is single-flight per engine (`ErrMergeInProgress`), and a committed merge whose source cleanup failed reports `ErrPostCommitCleanup` alongside the stats — state is consistent, only unreferenced files linger.

v1-format files remain mergeable: their blocks are rewritten into v2 form when they combine, and copied verbatim otherwise.

#### Coordinated Merges (roadmap)

**Not implemented** — tracked in [this issue](https://github.com/danthegoodman1/bloomsearch/issues/19). Merges today are single-flight within one engine process; multiple concurrent writers need external coordination. A `CoordinatedMetaStore` could expose lease methods, enabling multiple writers and background merge processes to work together safely.

#### TTLs (roadmap)

**Not implemented.** The design intent is for TTLs to reuse the merging mechanism to drop expired data (conditions based on partition ID, minmax indexes, or row group age, applied during merge), but no TTL configuration exists today.

### DataStore

Pluggable interface for file storage:

```go
type DataStore interface {
    CreateFile(ctx context.Context) (io.WriteCloser, []byte, error)
    OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error)
    TombstoneFile(ctx context.Context, filePointerBytes []byte) error
}
```

The `filePointerBytes` abstracts storage location (file path, S3 bucket/key, etc.) and is stored in the MetaStore for later retrieval. Enables storage backends like filesystem, S3, GCS, etc.

A successful `Close` on the returned writer must durably publish the file — the engine acknowledges ingested rows and commits pointers only after `Close` returns nil. The writer may optionally implement `Abort() error` to discard a failed partial write without publishing it. `TombstoneFile` marks a file (and any artifacts derived from its pointer) as unreferenced; implementations decide when physical garbage collection happens.

The shipped `FileSystemDataStore` writes under a temp name and publishes with sync + rename + directory fsync on `Close`, so in-progress files are never visible to queries.

### MetaStore

Handles file metadata storage and query pre-filtering:

```go
type MetaStore interface {
    GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error]
    Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error
}
```

Can be the same as DataStore (e.g., `FileSystemDataStore`) or separate for performance.

`GetMaybeFilesForQuery` returns a standard-library iterator, so candidate files stream to the engine one at a time instead of arriving as one slice — per-query memory scales with the in-flight window, not the candidate count. Errors flow through the yielded second value: a store that fails yields one `(MaybeFile{}, err)` and returns, and the engine stops pulling at the first error, surfacing it from `Results.Err` alongside any block errors. The engine may also stop early (query canceled or cursor closed), so stores must release resources via defers inside the iterator closure, and blocking waits inside the iterator must honor `ctx` so a terminated query winds down promptly. Yields block on query backpressure, so stores must not hold exclusive locks across yields — snapshot under a read lock or page through stable views, then yield. Yielding transfers ownership: the engine retains the yielded file pointer and data-block metadata for the life of the query, so stores must not reuse or mutate those buffers, slices, or maps after yielding.

Store-side prefiltering is an optimization, not a correctness requirement: the engine re-applies the query's prefilter to every yielded file's data blocks, so a store may ignore the query entirely and yield everything. Advanced implementations using databases should still pre-filter partition IDs and minmax indexes to avoid shipping metadata the engine will discard. The prefilter/bloom/regex expression trees are exported with lossless JSON round-trips so external MetaStores can translate them (e.g., to SQL).

### Write path

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│1. Ingest    │ ──►│2. Buffer        │ ──►│3. Flush      │
│   Rows      │    │   • Partitions  │    │   • Create   │
│             │    │   • Bloom       │    │     file     │
│             │    │   • MinMax      │    │   • Stream   │
└─────────────┘    └─────────────────┘    │     blocks   │
                                          └──────┬───────┘
                                                 │
                                          ┌──────▼───────┐
                                          │4. Finalize   │
                                          │   • Metadata │
                                          │   • Update   │
                                          │     stores   │
                                          └──────────────┘
```

Configurable flush triggers: row count, byte size, or time-based.

Buffering runs on a single ingest actor (no lock contention), and flushes are handed to a single dedicated flush worker through a FIFO queue. Ingestion continues while a flush writes, but the pipeline is bounded: if flushes fall behind, the queue applies backpressure to the ingest actor rather than buffering without limit.

**Durability contract**: a batch's done channel receives nil only after the file's writer `Close` succeeded (for the filesystem store: data fsync, rename to the final name, directory fsync) *and* the MetaStore `Update` committed the pointer. `Flush(ctx)` waits behind every flush queued before it, so a nil return means every previously ingested row is durable.

**Done channels are delivered to with a blocking send**: provide a buffered channel or actively receive from it. An abandoned unbuffered done channel deliberately stalls the flush worker (backpressure) until the engine's Stop deadline aborts delivery.

**Stop drains before exiting**: requests accepted before `Stop` are flushed, and every done channel is answered. Pass `Stop` a context with a deadline — ctx expiry is the only abort path for a wedged pipeline, and `Stop(context.Background())` can wait forever.

### Query path

Query flow for `field`, `token`, `field:token`, and final-stage `field regex` combinations:

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│1. Build     │ ──►│2. Pre-filter    │ ──►│3. Bloom Test │
│   Query     │    │   (MetaStore +  │    │ (file-level) │
│             │    │    engine)      │    │              │
└─────────────┘    └─────────────────┘    └──────┬───────┘
                                                 │
                                                 ▼
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│6. Row Bloom │ ◄──│5. Bloom Test    │ ◄──│4. Stream     │
│   Match     │    │   (block-level) │    │   Blocks     │
│             │    │                 │    │              │
└──────┬──────┘    └─────────────────┘    └──────────────┘
       │
       ▼
┌─────────────┐
│7. Regex     │
│   Final     │
│   Filter    │
└─────────────┘
```

```go
// Example query combining prefiltering with bloom search
query := NewQuery().
    MatchPrefilter(
        PrefilterOr(
            Partition(PartitionEquals("202301")),
            MinMax("timestamp", NumericBetween(start, end)),
        ),
    ).
    Field("user_id").Token("error").
    Build()

results, err := engine.Query(ctx, query)
```

Candidate files stream from the MetaStore iterator through a bounded pipeline: a file stage pulls one file at a time, enforces strict prefilter semantics itself (re-filtering whatever the MetaStore yields), tests the file-level bloom filters, and releases them immediately after, so per-query memory does not scale with the candidate-file count or filter size. Each surviving data block becomes a job for a bounded worker pool — up to `MaxQueryConcurrency` blocks across *all* queries process concurrently — and a query without bloom conditions skips reading block filter sections entirely.

When regex filters are present, the engine compiles patterns once per query and derives a field-existence bloom guard for earlier file/block pruning. Row verification is compiled once per query into a single-walk matcher.

Block scans verify before emitting: the block's row data is read fully, CRC-checked, and decompressed within metadata-declared bounds before any row is matched, so a corrupt block yields a clean block error and no rows.

`BloomSearchEngine.Query` returns an engine-owned `Results` cursor. Block workers stream matched rows into the cursor in bounded batches, so the caller receives rows through `Next`/`Row` as blocks complete, and arbitrarily large result sets never accumulate in memory.

When `Next` returns `false`, no block workers remain: `Err` reports the terminal state (nil on clean completion, the joined block errors when some blocks failed — the query continues past failed blocks because partial results are valuable for search — or the context error when the query was canceled), and `Stats` is complete. Call `Close` to terminate a query early.

Queries are independent of the ingest lifecycle: a stopped engine still serves queries, because reads touch only the MetaStore and DataStore.

#### Distributed Query Processing (roadmap)

**Not implemented** — tracked in [this issue](https://github.com/danthegoodman1/bloomsearch/issues/14). The design sketch:

Query processing naturally decomposes into independent row group tasks that can be distributed across multiple nodes. Since results are streamed back asynchronously without ordering guarantees, this creates a perfectly parallelizable workload.

```
┌──────────┐     ┌──────────────┐     ┌───────────┐     ┌─────────────┐     ┌─────────────┐
│1. Build  │ ──► │2. Pre-filter │ ──► │3. Scatter │ ──► │4. Peers     │ ──► │5. Stream    │
│   Query  │     │   MetaStore  │     │   Work to │ ──► │   Process   │ ──► │   Results   │
│          │     │              │     │    Peers  │ ──► │  Row Groups │ ──► │   Back to   │
└──────────┘     └──────────────┘     └───────────┘     └─────────────┘     │ Coordinator │
                                                                            └─────────────┘
```

1. **Build Query** - Coordinator constructs the query with bloom conditions and prefilters
2. **Pre-filter MetaStore** - Coordinator identifies candidate files using partition and MinMax indexes where possible
3. **Scatter Work to Peers** - Coordinator distributes row group processing tasks across available peers
4. **Peers Process Row Groups** - Each peer performs bloom filter tests and row scanning independently
5. **Stream Results Back to Coordinator** - Peers stream matching rows directly to the coordinator via unique query IDs

Peer discovery would use a gossip protocol for fault tolerance, while work assignment prioritizes peers with available capacity.

## Performance

See [`PERFORMANCE.md`](/PERFORMANCE.md)

## Contributing

Do not submit random PRs, they will be closed.

For feature requests and bugs, create an Issue.

For questions, create a Discussion.

### AI Code

More as a disclaimer, this codebase was heavily contributed by Claude 4 Sonnet using Cursor.

I normally use Goland (which I miss many features from that fill massive gaps in the go linter/compiler, like telling me what's needed to implement an interface).

All code has been carefully reviewed, and tests have been written, to ensure validity and that it is of the quality that I would write myself.

The common pattern I used is:

1. Define a clear spec (with a todo list)
2. Have it build that
3. Have it write tests to my spec that check edge cases and verify robustness
4. Have it simplify the code and find consolidation and code reusability opportunities - an example can be seen in [this commit](https://github.com/danthegoodman1/bloomsearch/commit/f498dc6445974e9a4296fb5c27c8fa05c8dbe60e)
