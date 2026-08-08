# File Format

The bloomsearch file format is designed for efficient, single-pass writing of unordered row data and selective reading for rapid filtering and reduced I/O during reads.

Bloom filters are sized at write time from the measured number of distinct entries they will hold (fields, tokens, and field:token pairs), so indexing overhead tracks the actual content instead of a configured guess, and the false positive rate holds at the configured target.

Files are written as **version 2**. Version 1 files (bloom filters JSON-encoded inside the metadata and block sections) remain fully readable and mergeable; merging rewrites their blocks into v2 form when blocks combine, and copies them verbatim otherwise.

## Overall Structure (v2)

```
[Data Block 1]
[Data Block 2]
...
[Data Block N]
[File-Level Bloom Filter Section]
[File Metadata (JSON)]
[uint32: File Metadata CRC32C]
[uint32: File Metadata Length]
[uint32: File Version (2)]
[8 bytes: magic bytes "BLOMSRCH"]
```

Readers locate the footer from the end of the file: magic bytes, version, metadata length, metadata CRC32C, then the metadata JSON. The version field selects the decoder — v1 metadata embeds the file-level filters as JSON; v2 metadata carries no filter bytes.

## Public API

External DataStore/MetaStore implementations and tooling can parse and produce this format without reimplementing the framing:

- `ReadFileMetadata(r io.ReadSeeker) (*FileMetadata, int64, error)` — locates, verifies, and decodes the footer (v1 and v2), returning the metadata with the file-level filters decoded, plus the file size. This is what `FileSystemDataStore` uses to serve `GetMaybeFilesForQuery`.
- `WriteFileFooter(w io.Writer, metadata *FileMetadata) error` — writes the v2 footer (filter section, metadata JSON, CRC32C, length, version, magic bytes) after the last data block.
- `ReadDataBlockBloomFilters(r io.ReadSeeker, block DataBlockMetadata) (*BloomFilters, error)` — reads and verifies one block's filter section.
- `ReadDataBlockRowData(r io.ReadSeeker, block *DataBlockMetadata) ([]byte, error)` — reads, verifies (CRC before decompression, output bounded by `UncompressedSize`), and decompresses one block's row data.
- `NewBlockRowScanner(rowData []byte) *BlockRowScanner` — iterates the length-prefixed rows of a decoded row data section.
- `FileMetadataFromBytesWithHash(metadata, hash []byte) (*FileMetadata, error)` — verifies and decodes a standalone v1 metadata payload.

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

Readers verify the trailing CRC32C first, then dispatch on the first payload byte: `{` marks a v1 JSON-encoded section, a byte ≤ 0x07 marks the v2 presence flags. Sections are self-identifying so that a v2 file may carry v1 sections in blocks that a merge copied verbatim from a v1 file.

## File Metadata

The file metadata is a JSON-encoded struct containing:

- Per-block metadata (partition IDs, minmax indexes, offsets, sizes, row counts, row data hash, measured bloom entry counts)
- `FileFilterSectionSize`: the size of the file-level bloom filter section that immediately precedes the metadata (0 = no file-level filters)
- `BloomFalsePositiveRate` and `BloomEntryCounts`: the false positive rate the filters were built at and the measured distinct entry counts they were sized from

The file-level filter bytes themselves live in the separate binary section, so MetaStores that hold file metadata externally are not forced to carry filter payloads inside the JSON, and readers can decode the metadata without touching filter bytes.

To query whether something exists in a file, generally you'd want to follow:
1. If a partition is specified, does it exist?
2. If a minmax index is specified, does any data block satisfy it
3. Consult the file-level bloom filters
4. If the file-level bloom filters say maybe, load and consult the per-block bloom filter sections from the data blocks

Queries without bloom conditions skip steps 3 and 4 entirely and read only row data.

## Data Block Structure

```
[Bloom Filter Section]    (see framing above; size = BloomFiltersSize)
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

### Integrity Verification
- **Bloom filter sections**: each carries a trailing CRC32C, verified before decoding.
- **File metadata CRC32C**: verified before the metadata JSON is parsed.
- **Row data hash**: `DataBlockMetadata.RowDataHash` is the CRC32C of the block's compressed row data, valid when `HasRowDataHash` is true (v1 files used a 0 sentinel for "no hash"; 0 is a legitimate checksum value in v2). Readers verify it — and bound decompression by `UncompressedSize` — **before** scanning any row, so a corrupt block produces a clean error and emits no rows, and a corrupt length prefix cannot force an oversized allocation.

## Version 1 (read compatibility)

v1 files differ from v2 in two ways, both handled transparently on read:

- Block filter sections are JSON-encoded (`[filters JSON][uint32 CRC32C]`).
- The file metadata JSON embeds the file-level bloom filters (JSON+base64) directly, along with the configured expected-item counts v1 filters were sized from (today's reader ignores those counts — v1 filters self-describe their parameters); there is no separate file-level filter section.
