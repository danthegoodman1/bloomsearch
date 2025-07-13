#  <img src="https://media.tenor.com/_xAGKS8S3zEAAAAi/flower-sam-smith.gif" width="36px" /> BloomSearch <img src="https://media.tenor.com/_xAGKS8S3zEAAAAi/flower-sam-smith.gif" width="36px" /> <!-- omit in toc -->

**Keyword search engine with hierarchical bloom filters for massive datasets**

BloomSearch provides extremely low memory usage and low cold-start searches through pluggable storage interfaces.

- **Memory efficient**: Bloom filters have constant size regardless of data volume
- **Pluggable storage**: DataStore and MetaStore interfaces for any backend (can be same or separate)
- **Fast filtering**: Hierarchical pruning via partitions, minmax indexes, and bloom filters
- **Flexible queries**: Search by `field`, `token`, or `field:token` with AND/OR combinators

Perfect for logs, JSON documents, and high-cardinality keyword search.

- [Usage](#usage)
  - [Quick start](#quick-start)
- [Concepts](#concepts)
  - [Bloom filters](#bloom-filters)
  - [Search types](#search-types)
  - [Data files](#data-files)
    - [Partitions](#partitions)
    - [MinMax Indexes](#minmax-indexes)
  - [Merging](#merging)
    - [Coordinated Merges](#coordinated-merges)
    - [TTLs](#ttls)
  - [DataStore](#datastore)
  - [MetaStore](#metastore)
  - [Write path](#write-path)
  - [Query path](#query-path)
- [Contributing](#contributing)

## Usage

### Quick start

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

// Query for rows where "level"="error"
matchingRows := engine.Query(NewQueryWithGroupCombinator(CombinatorAND).Field("level").Token("error").Build())
```

See tests for complete working examples, including partitioning and minmax index filtering.

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
// Find all records where service is "payment"
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

#### Coordinated Merges

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
    GetMaybeFilesForQuery(ctx context.Context, query *QueryCondition) ([]MaybeFile, error)
    WriteFileMetadata(ctx context.Context, fileMetadata *FileMetadata, filePointerBytes []byte) error
}
```

Can be the same as DataStore (e.g., `FileSystemDataStore`) or separate for performance.

Advanced implementations using databases can pre-filter partition IDs and minmax indexes, reducing bloom filter tests.

### Write path

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Ingest    │ ──►│   Buffer        │ ──►│   Flush      │
│   Rows      │    │   • Partitions  │    │   • Create   │
│             │    │   • Bloom       │    │     file     │
│             │    │   • MinMax      │    │   • Stream   │
└─────────────┘    └─────────────────┘    │     blocks   │
                                          └──────┬───────┘
                                                 │
                                          ┌──────▼───────┐
                                          │   Finalize   │
                                          │   • Metadata │
                                          │   • Update   │
                                          │     stores   │
                                          └──────────────┘
```

Configurable flush triggers: row count, byte size, or time-based.

### Query path

Query flow for `field`, `token`, or `field:token` combinations:

```
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Build     │ ──►│   Pre-filter    │ ──►│ Bloom Test   │
│   Query     │    │  (MetaStore)    │    │ (file-level) │
│             │    │                 │    │              │
└─────────────┘    └─────────────────┘    └──────┬───────┘
                                                 │
                                                 ▼
┌─────────────┐    ┌─────────────────┐    ┌──────────────┐
│   Row       │ ◄──│   Bloom Test    │ ◄──│   Stream     │
│   Scan      │    │  (block-level)  │    │   Blocks     │
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

Memory usage scales with concurrent file reads, not dataset size.

## Contributing

Do not submit random PRs, they will be closed.

For feature requests and bugs, create an Issue.

For questions, create a Discussion.
