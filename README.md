#  <img src="https://media.tenor.com/oarbV8g0O0gAAAAi/smiling-flower-kids%27-choice-awards.gif" width="36px" /> BloomSearch <img src="https://media.tenor.com/oarbV8g0O0gAAAAi/smiling-flower-kids%27-choice-awards.gif" width="36px" /> <!-- omit in toc -->

**Keyword search engine with hierarchical bloom filters for massive datasets**

BloomSearch provides extremely low memory usage and low cold-start searches through pluggable storage interfaces.

- **Memory efficient**: Bloom filters have constant size regardless of data volume
- **Pluggable storage**: DataStore and MetaStore interfaces for any backend (can be same or separate)
- **Fast filtering**: Hierarchical pruning via partitions, minmax indexes, and bloom filters
- **Flexible queries**: Search by `field`, `token`, or `field:token` with AND/OR combinators
- **Disaggregated storage and compute**: Unbound ingest and query throughput

Perfect for logs, JSON documents, and high-cardinality keyword search.



## Quick start

```bash
go get github.com/danthegoodman1/bloomsearch
```

```go
// Initialize stores
dataStore := NewFileSystemDataStore("./data")
metaStore := dataStore // FileSystemDataStore also implements MetaStore

// Create engine with default config
engine := NewBloomSearchEngine(DefaultBloomSearchEngineConfig(), metaStore, dataStore)
engine.Start()

// Insert data asynchronously (no wait for flush)
engine.IngestRows(ctx, []map[string]any{{
    "level": "error",
    "message": "database connection failed",
    "service": "auth",
}}, nil)

// Provide a `chan error` to wait for flush
doneChan := make(chan error)
engine.IngestRows(ctx, []map[string]any{{
    "level": "info",
    "message": "login successful",
    "service": "auth",
}}, doneChan)
if err := <-doneChan; err != nil {
    log.Fatal(err)
}

// Collect the resulting rows that match
resultChan := make(chan map[string]any, 100)
// If any of the workers error, they report it here
errorChan := make(chan error, 10)

err := engine.Query(
    ctx,
    // Query for rows where `.level: "error"`
    NewQueryWithGroupCombinator(CombinatorAND).Field("level").Token("error").Build(),
    resultChan,
    errorChan,
)
if err != nil {
    log.Fatal(err)
}

// Process results
for {
    select {
    case <-ctx.Done():
        return
    case row, activeWorkers := <-resultChan:
        if !activeWorkers {
            return
        }
        // Process matching row
        fmt.Printf("Found row: %+v\n", row)
    case err := <-errorChan:
        log.Printf("Query error: %v", err)
        // Continue processing other results, or cancel context
    }
}
```

See tests for complete working examples, including partitioning and minmax index filtering.

- [Quick start](#quick-start)
- [Concepts](#concepts)
  - [Bloom filters](#bloom-filters)
  - [Search types](#search-types)
  - [Data files](#data-files)
    - [Partitions](#partitions)
    - [MinMax Indexes](#minmax-indexes)
  - [Merging](#merging)
    - [Coordinated Merges (issue)](#coordinated-merges-issue)
    - [TTLs](#ttls)
  - [DataStore](#datastore)
  - [MetaStore](#metastore)
  - [Write path](#write-path)
  - [Query path](#query-path)
    - [Distributed Query Processing (issue)](#distributed-query-processing-issue)
- [Performance](#performance)
- [Contributing](#contributing)

## Concepts

### Bloom filters

[Bloom filters](https://en.wikipedia.org/wiki/Bloom_filter) are a probabilistic data structure for testing set membership. They guarantee no false negatives but allow tunable false positives. Constant size regardless of data volume with extremely fast lookups and minimal memory usage.

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
query := NewQueryWithGroupCombinator(CombinatorAND).Field("retry_count").Build()
```

**Token search** - Find records containing a value anywhere:
```go
// Find all records containing "error" in any field
query := NewQueryWithGroupCombinator(CombinatorAND).Token("error").Build()
```

**Field:token search** - Find records with a specific value in a specific field:
```go
// Find all records where `.service: "payment"`
query := NewQueryWithGroupCombinator(CombinatorAND).FieldToken("service", "payment").Build()
```

**Complex combinations**:
```go
// (field AND token) AND fieldtoken (groups combined with AND)
query := NewQueryWithGroupCombinator(CombinatorAND).
    Field("retry_count").Token("error").        // group1: AND within group
    And().FieldToken("service", "payment").     // group2
    Build()

// (field AND token) OR fieldtoken (groups combined with OR)
query := NewQueryWithGroupCombinator(CombinatorOR).
    And().Field("retry_count").Token("error").  // group1: AND within group
    And().FieldToken("service", "payment").     // group2
    Build()
```

Queries can be combined with AND/OR operators and filtered by [partitions](#partitions) and [minmax indexes](#minmax-indexes).

### Data files

Data files are designed for single-pass writing with row groups, similar to Parquet. They include minmax filters for quick pruning and support partitions like ClickHouse.

Files are self-contained and immutable. Bloom filter storage overhead is amortized as row groups grow while filters remain constant size.

See [FILE_FORMAT.md](./FILE_FORMAT.md) for details.

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

Partitions are optional. When querying with partition conditions, files without partition IDs are always included to avoid missing data.

#### MinMax Indexes

Track minimum and maximum values for numeric fields, enabling range-based pruning:

```go
config.MinMaxIndexes = []string{"timestamp", "response_time"}

// Query with range filter and bloom conditions
query := NewQueryWithGroupCombinator(CombinatorAND).
    AddMinMaxCondition("timestamp", NumericBetween(start, end)).
    AddMinMaxCondition("response_time", NumericLessThan(1000)).
    WithMinMaxFieldCombinator(CombinatorAND).
    FieldToken("level", "error").
    Build()
```

Within each field, conditions are OR-ed. Across fields, use `CombinatorAND` (default) or `CombinatorOR`.

MinMax indexes are optional. When querying with range conditions, files without minmax indexes are always included to avoid missing data.

### Merging

Merging files reduces metadata operations (file opens, bloom filter tests) and improves query performance.

Bloom filters of the same size can be trivially merged by OR-ing their bits. If bloom filter parameters change, the system rebuilds filters from raw data during merge.

Two files are considered mergeable if they have the same file-level bloom filter parameters, and combined they are still under the max file size threshold.

Once two files have been decided to merge, the algorithm then considers whether row groups within the files can be merged. They are considered mergeable if they share the same partition ID, have the same bloom filter parameters and combined are under the max row group size parameters (number of rows is ignored here since this matters less than when memory-buffering).

Then, all merging is done via streaming to keep memory usage low.

First, the new file is created in the DataStore. Then, row groups are merged together by decompressing and rewriting them (this means different compression settings are supported and consolidated) and bloom filters merged. Row groups that are not merged are simply copied in as-is without decompressing. Row group metadata is merged (minmax indexes, number of rows, etc.) and added to the running file metadata. The final file metadata is created by merging all file-level bloom filters and writing it out to the new file. Finally, the MetaStore receives an update to atomically create the new file, and delete all the old files.

#### Coordinated Merges ([issue](https://github.com/danthegoodman1/bloomsearch/issues/19))

Multiple concurrent writers need coordination to avoid conflicts. A `CoordinatedMetaStore` can expose lease methods, enabling multiple writers and background merge processes to work together safely.

#### TTLs

TTL uses the same merging mechanism to drop expired data. Configure TTL conditions based on partition ID, minmax indexes, or row group age. Expired row groups and files are dropped during merge.

TTLs are optional.

### DataStore

Pluggable interface for file storage with two methods:

```go
type DataStore interface {
    CreateFile(ctx context.Context) (io.WriteCloser, []byte, error)
    OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error)
}
```

The `filePointerBytes` abstracts storage location (file path, S3 bucket/key, etc.) and is stored in the MetaStore for later retrieval. Enables storage backends like filesystem, S3, GCS, etc.

### MetaStore

Handles file metadata storage and query pre-filtering:

```go
type MetaStore interface {
    GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error)
    Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error
}
```

Can be the same as DataStore (e.g., `FileSystemDataStore`) or separate for performance.

Advanced implementations using databases can pre-filter partition IDs and minmax indexes, reducing bloom filter tests.

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

Buffering is done in a single thread to remove lock content, and at flush time spawns off a dedicated goroutine for writing the buffers. This means
that flushing has no impact on ingestion performance.

### Query path

Query flow for `field`, `token`, or `field:token` combinations:

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│1. Build     │ ──►│2. Pre-filter    │ ──►│3. Bloom Test │
│   Query     │    │   (MetaStore)   │    │ (file-level) │
│             │    │                 │    │              │
└─────────────┘    └─────────────────┘    └──────┬───────┘
                                                 │
                                                 ▼
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│6. Row       │ ◄──│5. Bloom Test    │ ◄──│4. Stream     │
│   Scan      │    │   (block-level) │    │   Blocks     │
│             │    │                 │    │              │
└─────────────┘    └─────────────────┘    └──────────────┘
```

```go
// Example query combining prefiltering with bloom search
query := NewQueryWithGroupCombinator(CombinatorAND).
    AddPartitionCondition(PartitionEquals("202301")).
    AddMinMaxCondition("timestamp", NumericBetween(start, end)).
    Field("user_id").Token("error").
    Build()

maybeFiles, err := metaStore.GetMaybeFilesForQuery(ctx, query.Prefilter)
```

Query processing is done highly-concurrently: A goroutine is spawned for every file (if the result is over 20 files), and for every row group. This allows it to maximimze multi-core machines.

Memory usage scales with concurrent file reads, not dataset size.

This flow is a bit simplified, see `BloomSearchEngine.Query` for more detail.

As you notice, `BloomSearchEngine.Query` takes in a `resultChan` and `errorChan`. This is because each row group
processor reads the row group one row at a time, allowing to stream matches back to the caller.

This enables processing of arbitrarily large results as well.

When the `resultChan` closes, there are no more active row group processors, and the caller can exit.

#### Distributed Query Processing ([issue](https://github.com/danthegoodman1/bloomsearch/issues/14))

Query processing naturally decomposes into independent row group tasks that can be distributed across multiple nodes. Since results are streamed back asynchronously without ordering guarantees, this creates a perfectly parallelizable workload.

Distributed query processing extends the existing path like this:

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

Peer discovery uses gossip protocol for fault tolerance, while work assignment prioritizes peers with available capacity. Each peer maintains its own connection to the coordinator for result streaming, enabling horizontal scaling without central bottlenecks.

## Performance

See [`PERFORMANCE.md`](/PERFORMANCE.md)

## Contributing

Do not submit random PRs, they will be closed.

For feature requests and bugs, create an Issue.

For questions, create a Discussion.
