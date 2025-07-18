# File Format

The bloomsearch file format is designed for efficient, single-pass writing of unordered row data and selective reading for rapid filtering and reduced I/O during reads.

As the number of rows within a row group increases, the bloom filter stays at a constant size, meaning the storage overhead for indexing is amortized.

## Overall Structure

```
[Data Block 1]
[Data Block 2]
...
[Data Block N]
[File Metadata]
[uint64: File Metadata xxhash]
[uint32: File Metadata Length]
[uint32: File Version]
[8 bytes: magic bytes "BLOMSRCH"]
```

## File Metadata

Currently, the file metadata is just a JSON-encoded struct. This is intended to be greatly optimized in the future.

The file metadata contains:
- File-level bloomfilter for rapid checks of the file
- Per-block metadata (partition IDs, minmax indexes, offsets, sizes, row counts)

To query whether something exists in a file, generally you'd want to follow:
1. If a partition is specified, does it exist?
2. If a minmax index is specified, does any data block satisfy it
3. Consult the file-level bloom filter
4. If the file-level bloom filter says maybe, load and consult the per-block bloom filters from the data blocks

Dedicated metadata stores can store the partition IDs and minmax index values externally such that they can filter down the list of potential files at query time. The bloom filters are stored in the data blocks themselves to avoid memory bloat in the metadata store.

## Data Block Structure

```
[Data Block Bloom Filters] (JSON serialized)
[uint64: Data Block Bloom Filters xxhash]
[Compressed Row Data]
```

### Row Data Section
The row data section contains length-prefixed rows that may be compressed:

```
[uint32: row 1 length]
[row 1 bytes]
[uint32: row 2 length] 
[row 2 bytes]
...
```

### Compression Support
Row data can be compressed using:
- **None**: Raw uncompressed data
- **Snappy**: Fast compression/decompression for query performance
- **Zstd**: Higher compression ratios for storage efficiency

The compression type and uncompressed size are stored in the data block metadata for efficient decompression.

### Bloom Filters
The bloom filters are stored at the beginning of each data block for efficient access during query processing. They include:
- Field bloom filter (for field paths)
- Token bloom filter (for tokenized values)
- Field+Token bloom filter (for field:token combinations)

The bloom filters are JSON-serialized and have their own integrity hash.

### Integrity Verification
- **Bloom filters hash**: Stored immediately after bloom filters
- **Row data hash**: Stored in metadata (DataBlockMetadata.RowDataHash) for integrity verification without requiring additional I/O
