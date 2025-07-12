# File Format

The bloomsearch file format is designed for efficient, single-pass writing of unordered row data and selective reading for rapid filtering and reduced I/O during reads.

## Overall Structure

```
[Data Block 1]
[Data Block 2]
...
[Data Block N]
[File Metadata]
[uint32: File Metadata Length]
[uint64: File Metadata xxhash]
[8 bytes: magic bytes "BLOMSRCH"]
```

## File Metadata

Currently, the file metadata is just a JSON-encoded struct. This is intended to be greatly optimized in the future.

The file metadata contains:
- File-level bloomfilter for rapid checks of the file
- Per-block bloomfilter
- Per-block minxmax indexes
- Per-block partition IDs

To query whether something exists in a file, generally you'd want to follow:
1. If a partition is specified, does it exist?
2. If a minxmax index is specified, does any data block satisfy it
3. Consult the file-level bloom filter
4. If the file-level bloom filter says maybe, consult the per-block bloom filters

Dedicated metadata stores can store the partition IDs and mixmax index values externally such that they can filter down the list of potential files at query time.

## Data Block Structure

```
[N row bytes]
[row bytes]
...
[uint64: Data Block xxhash]
```
