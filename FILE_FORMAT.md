# File Format

The bloomsearch file format is designed for efficient, single-pass writing of unordered row data and selective reading for rapid filtering and reduced I/O during reads.

Bloom filters are sized at write time from the measured number of distinct entries they will hold (fields, tokens, and field:token pairs), so indexing overhead tracks the actual content instead of a configured guess, and the false positive rate holds at the configured target.

All of a file's block filter sections live in one contiguous **block filter region** behind the row data, so a query consults many blocks' filters per request instead of one per block. On object storage a file whose region fits one read costs a single GET instead of one per candidate block.

Files are written as **version 3**. Earlier versions stored each block's filter section immediately before that block's row data; they are rejected on read rather than translated, since misreading one would decode filters out of row data bytes.

## Overall Structure

```
[Block 0 Row Data]
[Block 1 Row Data]
...
[Block N-1 Row Data]
[Block Filter Region]             block 0 filter section, block 1 filter section, ...
[File-Level Bloom Filter Section]
[File Metadata (JSON)]
[uint32: File Metadata CRC32C]
[uint32: File Metadata Length]
[uint32: File Version (3)]
[8 bytes: magic bytes "BLOMSRCH"]
```

Readers locate the footer from the end of the file: magic bytes, version, metadata length, metadata CRC32C, then the metadata JSON. `ReadFileMetadata` takes three reads to do it — the fixed footer tail, the metadata JSON, and the file-level filter section — and validates that the region, every block's row data, and every block's filter section lie inside the file before returning metadata a caller will seek by.

The writer streams row data as it goes and buffers the blocks' filter sections in memory until the file closes, which is what the layout costs. Filter sections run roughly 1–5% of file size, so a flushed file (bounded by `MaxBufferedBytes`/`MaxBufferedRows` and the row group limits) buffers a negligible amount, while a merge output (bounded by `MaxFileSize`, 10GB by default) can buffer hundreds of MB. Nothing spills; lower `MaxFileSize` if that is too much.

## Public API

External DataStore/MetaStore implementations and tooling can parse and produce this format without reimplementing the framing:

- `ReadFileMetadata(r io.ReadSeeker) (*FileMetadata, int64, error)` — locates, verifies, and decodes the footer, returning the metadata with the file-level filters decoded, plus the file size. This is what `FileSystemDataStore` uses to serve `GetMaybeFilesForQuery`.
- `WriteFileFooter(w io.Writer, metadata *FileMetadata) error` — writes the footer (file-level filter section, metadata JSON, CRC32C, length, version, magic bytes) after the block filter region. The metadata must describe what was actually written: readers seek by `BlockFilterRegionOffset`/`BlockFilterRegionSize` and the per-block offsets.
- `ReadDataBlockBloomFilters(r io.ReadSeeker, block DataBlockMetadata) (*BloomFilters, error)` — reads and verifies one block's filter section out of the region. The query path instead reads every candidate block's section in one request.
- `ReadDataBlockRowData(r io.ReadSeeker, block *DataBlockMetadata) ([]byte, error)` — reads, verifies (CRC before decompression, output bounded by `UncompressedSize`), and decompresses one block's row data.
- `NewBlockRowScanner(rowData []byte) *BlockRowScanner` — iterates the length-prefixed rows of a decoded row data section.
- `DataBlockMetadata.OnDiskSize()` — the block's full on-disk footprint: its row data plus its filter section.

## Bloom Filter Sections

Block-level and file-level bloom filters share one binary framing:

```
[uint8: presence flags]           bit0 = field filter, bit1 = token filter,
                                  bit2 = field:token filter
for each present filter, in field/token/fieldtoken order:
  [uint32 LE: filter length]
  [filter bytes]                  bits-and-blooms binary encoding (m, k, bitset)
[uint32 LE: CRC32C]               over all preceding section bytes
```

An absent filter (presence bit clear) can never disqualify anything at query time — readers fail open.

Each section carries its own CRC32C, so corruption inside the block filter region fails one block rather than the whole file: the query records that block's error and keeps evaluating the rest of the region it already read.

## Block Filter Region

The region holds the blocks' filter sections back to back, in the order the blocks' row data was written. `FileMetadata.BlockFilterRegionOffset`/`BlockFilterRegionSize` locate it, and each block's `BloomFilterOffset`/`BloomFilterSize` locate that block's section inside it (absolute file offsets, so a block's metadata is enough to read its filters on its own).

A reader that wants every block's filters can issue one read of the whole region. The query path instead reads it in chunks of at most 4MiB, each chunk covering as many consecutive sections as fit:

- A region up to 4MiB — which is every file a flush produces, and most merge outputs — is one read.
- A larger region costs `ceil(region / 4MiB)` reads. A 27MB region (roughly a 10GB merged file of ~1000 blocks) is 7 reads, not 1000.
- The cap bounds a reader's transient memory, not just its request count: one file worker holds at most one chunk at a time, so peak query memory is 4MiB per in-flight worker rather than the whole region per worker. Without it, a prefilter-free bloom query at a high query concurrency could hold many GB of filter bytes at once.
- Each chunk starts at the section of the block being consulted, so a gap wider than the cap — blocks a partition or minmax prefilter dropped — is skipped rather than read. A narrow gap is read and discarded when the section after it still fits inside the chunk's cap window, because one request that carries some slack beats two that do not; otherwise the next chunk starts after the gap.
- A single section larger than the cap is read on its own. The cap limits slack, never whether a block can be consulted.

## File Metadata

The file metadata is a JSON-encoded struct containing:

- Per-block metadata (partition IDs, minmax indexes, row data offset/size, filter section offset/size, row counts, row data hash, measured bloom entry counts)
- `BlockFilterRegionOffset` and `BlockFilterRegionSize`: the block filter region's location
- `FileFilterSectionSize`: the size of the file-level bloom filter section that immediately precedes the metadata (0 = no file-level filters)
- `BloomFalsePositiveRate` and `BloomEntryCounts`: the false positive rate the filters were built at and the measured distinct entry counts they were sized from

The file-level filter bytes themselves live in the separate binary section, so MetaStores that hold file metadata externally are not forced to carry filter payloads inside the JSON, and readers can decode the metadata without touching filter bytes. A MetaStore that persists file metadata must persist the region fields with it: the query path bounds its region read by them.

To query whether something exists in a file, generally you'd want to follow:
1. If a partition is specified, does it exist?
2. If a minmax index is specified, does any data block satisfy it
3. Consult the file-level bloom filters
4. If the file-level bloom filters say maybe, read the block filter region once and consult the surviving blocks' filter sections

Queries without bloom conditions skip steps 3 and 4 entirely and read only row data — the block filter region is never touched, and the file is never opened for it.

## Data Block Structure

A block is two extents: its row data at the front of the file, and its filter section inside the block filter region.

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

### Integrity Verification
- **Bloom filter sections**: each carries a trailing CRC32C, verified before decoding.
- **File metadata CRC32C**: verified before the metadata JSON is parsed.
- **Layout framing**: the metadata's region and per-block extents are bounds-checked against the file before any of them is used to seek or slice, so a truncated file or an oversized recorded size produces an error rather than an out-of-bounds read or an absurd allocation.
- **Row data hash**: `DataBlockMetadata.RowDataHash` is the CRC32C of the block's compressed row data, valid when `HasRowDataHash` is true (0 is a legitimate checksum value). Readers verify it — and bound decompression by `UncompressedSize` — **before** scanning any row, so a corrupt block produces a clean error and emits no rows, and a corrupt length prefix cannot force an oversized allocation.
