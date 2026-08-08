package bloomsearch

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"hash/crc32"

	"github.com/bits-and-blooms/bloom/v3"
)

var (
	ErrInvalidHash = errors.New("invalid hash")
)

// File format constants. Files are always written as FileVersion (v2: binary
// bloom filter sections, filter-free metadata JSON); v1 files (JSON-encoded
// filters embedded in the metadata and block sections) remain fully readable.
const (
	FileVersionV1 = uint32(1)
	FileVersionV2 = uint32(2)
	FileVersion   = FileVersionV2

	LengthPrefixSize  = 4
	VersionPrefixSize = 4
	HashSize          = 4
)

// v2 filter section presence flags: one bit per filter, in on-disk order.
const (
	filterSectionFlagField      = byte(1 << 0)
	filterSectionFlagToken      = byte(1 << 1)
	filterSectionFlagFieldToken = byte(1 << 2)
	filterSectionFlagsAll       = filterSectionFlagField | filterSectionFlagToken | filterSectionFlagFieldToken
)

const MagicBytes = "BLOMSRCH"

// CRC32C table used for checksums (Castagnoli)
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// BloomEntryCounts records the exact number of distinct entries inserted into
// each bloom filter, measured while the filter's source rows were collected.
// Filters are sized from these counts (via bloom.NewWithEstimates), so they
// double as the filters' expected-item parameters. Files read from the v1
// format carry zero counts: v1 filters were sized from configured guesses,
// not measurements.
type BloomEntryCounts struct {
	Fields      int
	Tokens      int
	FieldTokens int
}

type FileMetadata struct {
	// BloomFilters are the file-level filters. They live here in memory
	// regardless of on-disk format: v1 stored them JSON-encoded inside the
	// metadata itself, v2 stores them in a binary filter section that readers
	// decode into this field. Any filter may be nil (absent filters cannot
	// disqualify anything at query time).
	BloomFilters           BloomFilters
	BloomFalsePositiveRate float64
	BloomEntryCounts       BloomEntryCounts `json:",omitzero"`

	DataBlocks []DataBlockMetadata
}

// fileMetadataV2JSON is the JSON payload written into a v2 footer: the
// FileMetadata fields minus the bloom filters (which live in the preceding
// binary filter section, located via FileFilterSectionSize).
type fileMetadataV2JSON struct {
	BloomFalsePositiveRate float64
	BloomEntryCounts       BloomEntryCounts `json:",omitzero"`

	// FileFilterSectionSize is the size in bytes of the file-level filter
	// section that immediately precedes the metadata JSON. Zero means the
	// file has no file-level filters (they then cannot disqualify the file).
	FileFilterSectionSize int

	DataBlocks []DataBlockMetadata
}

// FileMetadataFromBytesWithHash verifies and decodes a v1 metadata payload
// (JSON with the file-level bloom filters embedded). v1 block metadata used
// RowDataHash == 0 as a "no hash written" sentinel, so it is translated into
// the explicit HasRowDataHash flag here.
func FileMetadataFromBytesWithHash(bytes []byte, expectedHashBytes []byte) (*FileMetadata, error) {
	// Calculate CRC32C of the provided bytes
	actualHash := crc32.Checksum(bytes, crc32cTable)

	// Convert expected hash bytes to uint32
	expectedHash := binary.LittleEndian.Uint32(expectedHashBytes)

	// Verify hash matches
	if actualHash != expectedHash {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidHash, expectedHash, actualHash)
	}

	// Unmarshal the JSON bytes into FileMetadata
	var metadata FileMetadata
	err := json.Unmarshal(bytes, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	for i := range metadata.DataBlocks {
		metadata.DataBlocks[i].HasRowDataHash = metadata.DataBlocks[i].RowDataHash != 0
	}

	return &metadata, nil
}

// fileMetadataV2FromBytesWithHash verifies and decodes a v2 metadata payload
// (JSON without filter bytes). The caller resolves the file filter section
// via FileFilterSectionSize.
func fileMetadataV2FromBytesWithHash(payload []byte, expectedHashBytes []byte) (*fileMetadataV2JSON, error) {
	actualHash := crc32.Checksum(payload, crc32cTable)
	expectedHash := binary.LittleEndian.Uint32(expectedHashBytes)
	if actualHash != expectedHash {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidHash, expectedHash, actualHash)
	}

	var metadata fileMetadataV2JSON
	if err := json.Unmarshal(payload, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	return &metadata, nil
}

// BloomFilters contains the bloom filters for a data block or a whole file.
// Any filter may be nil: an absent filter cannot disqualify anything.
type BloomFilters struct {
	FieldBloomFilter      *bloom.BloomFilter
	TokenBloomFilter      *bloom.BloomFilter
	FieldTokenBloomFilter *bloom.BloomFilter
}

// encodeFilterSection serializes bloom filters into the v2 binary filter
// section:
//
//	[uint8: presence flags]
//	for each present filter, in field/token/fieldtoken order:
//	  [uint32 LE: filter length][filter bytes (bloom binary encoding)]
//	[uint32 LE: CRC32C of all preceding section bytes]
//
// The same framing is used for per-block sections and the file-level section
// in the footer.
func encodeFilterSection(filters *BloomFilters) ([]byte, error) {
	var flags byte
	present := make([]*bloom.BloomFilter, 0, 3)
	if filters != nil {
		if filters.FieldBloomFilter != nil {
			flags |= filterSectionFlagField
			present = append(present, filters.FieldBloomFilter)
		}
		if filters.TokenBloomFilter != nil {
			flags |= filterSectionFlagToken
			present = append(present, filters.TokenBloomFilter)
		}
		if filters.FieldTokenBloomFilter != nil {
			flags |= filterSectionFlagFieldToken
			present = append(present, filters.FieldTokenBloomFilter)
		}
	}

	var buf bytes.Buffer
	buf.WriteByte(flags)

	var filterBuf bytes.Buffer
	var lengthBytes [LengthPrefixSize]byte
	for _, filter := range present {
		filterBuf.Reset()
		if _, err := filter.WriteTo(&filterBuf); err != nil {
			return nil, fmt.Errorf("failed to serialize bloom filter: %w", err)
		}
		if int64(filterBuf.Len()) > 0xFFFFFFFF {
			return nil, fmt.Errorf("bloom filter too large: %d bytes exceeds uint32 length prefix", filterBuf.Len())
		}
		binary.LittleEndian.PutUint32(lengthBytes[:], uint32(filterBuf.Len()))
		buf.Write(lengthBytes[:])
		buf.Write(filterBuf.Bytes())
	}

	crc := crc32.Checksum(buf.Bytes(), crc32cTable)
	var crcBytes [HashSize]byte
	binary.LittleEndian.PutUint32(crcBytes[:], crc)
	buf.Write(crcBytes[:])

	return buf.Bytes(), nil
}

// parseFilterSection verifies a filter section's trailing CRC32C and decodes
// the filters. Both encodings are self-identifying once the CRC has passed:
// v1 sections are bloom-filter JSON (first byte '{'), v2 sections start with
// a presence-flag byte (<= 0x07). Dispatching on content rather than a file
// version lets a v2 file carry v1 sections in blocks that a merge raw-copied
// from a v1 file.
func parseFilterSection(section []byte) (*BloomFilters, error) {
	if len(section) < HashSize+1 {
		return nil, fmt.Errorf("bloom filter section too small: %d bytes", len(section))
	}

	payload := section[:len(section)-HashSize]
	expectedHash := binary.LittleEndian.Uint32(section[len(section)-HashSize:])
	actualHash := crc32.Checksum(payload, crc32cTable)
	if actualHash != expectedHash {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidHash, expectedHash, actualHash)
	}

	if payload[0] == '{' {
		var filters BloomFilters
		if err := json.Unmarshal(payload, &filters); err != nil {
			return nil, fmt.Errorf("failed to unmarshal bloom filters: %w", err)
		}
		return &filters, nil
	}

	flags := payload[0]
	if flags&^filterSectionFlagsAll != 0 {
		return nil, fmt.Errorf("unrecognized bloom filter section (leading byte %#x)", flags)
	}

	rest := payload[1:]
	next := func() (*bloom.BloomFilter, error) {
		if len(rest) < LengthPrefixSize {
			return nil, fmt.Errorf("truncated bloom filter length prefix (%d bytes remaining)", len(rest))
		}
		length := binary.LittleEndian.Uint32(rest[:LengthPrefixSize])
		rest = rest[LengthPrefixSize:]
		if uint64(length) > uint64(len(rest)) {
			return nil, fmt.Errorf("bloom filter length %d exceeds section remainder %d", length, len(rest))
		}
		filter := &bloom.BloomFilter{}
		if _, err := filter.ReadFrom(bytes.NewReader(rest[:length])); err != nil {
			return nil, fmt.Errorf("failed to decode bloom filter: %w", err)
		}
		rest = rest[length:]
		return filter, nil
	}

	filters := &BloomFilters{}
	var err error
	if flags&filterSectionFlagField != 0 {
		if filters.FieldBloomFilter, err = next(); err != nil {
			return nil, err
		}
	}
	if flags&filterSectionFlagToken != 0 {
		if filters.TokenBloomFilter, err = next(); err != nil {
			return nil, err
		}
	}
	if flags&filterSectionFlagFieldToken != 0 {
		if filters.FieldTokenBloomFilter, err = next(); err != nil {
			return nil, err
		}
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("bloom filter section has %d trailing bytes", len(rest))
	}
	return filters, nil
}

// ReadDataBlockBloomFilters reads and verifies the bloom filter section at
// the start of a data block. A block without a filter section
// (BloomFiltersSize == 0) yields empty BloomFilters, which cannot disqualify
// anything.
func ReadDataBlockBloomFilters(file io.ReadSeeker, blockMetadata DataBlockMetadata) (*BloomFilters, error) {
	if blockMetadata.BloomFiltersSize < 0 || blockMetadata.BloomFiltersSize > blockMetadata.Size {
		return nil, fmt.Errorf("invalid bloom filter section size %d (block size %d)", blockMetadata.BloomFiltersSize, blockMetadata.Size)
	}
	if blockMetadata.BloomFiltersSize == 0 {
		return &BloomFilters{}, nil
	}

	if _, err := file.Seek(int64(blockMetadata.Offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to block offset: %w", err)
	}

	section := make([]byte, blockMetadata.BloomFiltersSize)
	if _, err := io.ReadFull(file, section); err != nil {
		return nil, fmt.Errorf("failed to read bloom filters: %w", err)
	}

	return parseFilterSection(section)
}

// CompressionType represents the compression algorithm used for row data
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionSnappy CompressionType = "snappy"
	CompressionZstd   CompressionType = "zstd"
)

type DataBlockMetadata struct {
	// Absolute file offset (includes bloom filters at the beginning)
	Offset int

	// Size includes the bloom filters, their hash, and row data (no trailing hash)
	Size int
	Rows int

	// Size of the bloom filter section (filters + trailing CRC32C)
	BloomFiltersSize int

	MinMaxIndexes map[string]MinMaxIndex `json:",omitempty"`
	PartitionID   string                 `json:",omitempty"`

	// Compression algorithm used for the row data in this block
	Compression CompressionType `json:",omitempty"`

	// Uncompressed size of row data (for decompression buffer allocation)
	UncompressedSize int `json:",omitempty"`

	// Hash of the compressed row data (CRC32C), valid only when
	// HasRowDataHash is true — 0 is a legitimate checksum value. v1 files
	// used 0 as a "no hash" sentinel; the v1 reader translates that into
	// HasRowDataHash=false.
	RowDataHash    uint32 `json:",omitempty"`
	HasRowDataHash bool   `json:",omitempty"`

	// BloomEntryCounts are the measured distinct entry counts this block's
	// filters were built and sized from (zero for v1 files).
	BloomEntryCounts BloomEntryCounts `json:",omitzero"`

	BloomFalsePositiveRate float64
}

// decodeBlockRowData verifies and decompresses a block's row data section,
// already read into memory. The CRC check (when the block carries a hash)
// runs before any decompression or row parsing, so corrupt data is rejected
// before a single row can be emitted. Decompression output is bounded by the
// block's UncompressedSize — the stream must decode to exactly that many
// bytes — so a corrupt stream cannot force unbounded allocation.
func decodeBlockRowData(compressed []byte, block *DataBlockMetadata) ([]byte, error) {
	return decodeBlockRowDataInto(nil, compressed, block)
}

// decodeBlockRowDataInto is decodeBlockRowData decoding into dst when dst has
// capacity for the block's UncompressedSize (a fresh buffer is allocated
// otherwise; dst's length is ignored). With CompressionNone the returned slice
// is compressed itself, never dst.
func decodeBlockRowDataInto(dst []byte, compressed []byte, block *DataBlockMetadata) ([]byte, error) {
	if block.HasRowDataHash {
		actualHash := crc32.Checksum(compressed, crc32cTable)
		if actualHash != block.RowDataHash {
			return nil, fmt.Errorf("row data hash mismatch: expected %x, got %x", block.RowDataHash, actualHash)
		}
	}

	var decompressor io.Reader
	switch normalizeCompression(block.Compression) {
	case CompressionNone:
		return compressed, nil
	case CompressionSnappy:
		reader := getPooledSnappyReader(bytes.NewReader(compressed))
		defer putPooledSnappyReader(reader)
		decompressor = reader
	case CompressionZstd:
		decoder, err := getPooledZstdDecoder(bytes.NewReader(compressed))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		defer putPooledZstdDecoder(decoder)
		decompressor = decoder
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", block.Compression)
	}

	if block.UncompressedSize < 0 {
		return nil, fmt.Errorf("invalid uncompressed size %d", block.UncompressedSize)
	}
	var rowData []byte
	if cap(dst) >= block.UncompressedSize {
		rowData = dst[:block.UncompressedSize]
	} else {
		rowData = make([]byte, block.UncompressedSize)
	}
	if _, err := io.ReadFull(decompressor, rowData); err != nil {
		return nil, fmt.Errorf("row data shorter than metadata UncompressedSize %d: %w", block.UncompressedSize, err)
	}
	var probe [1]byte
	switch _, err := io.ReadFull(decompressor, probe[:]); err {
	case io.EOF:
		return rowData, nil
	case nil:
		return nil, fmt.Errorf("row data longer than metadata UncompressedSize %d", block.UncompressedSize)
	default:
		return nil, fmt.Errorf("failed to verify row data ends at UncompressedSize %d: %w", block.UncompressedSize, err)
	}
}

// readBlockRowData reads a block's compressed row data fully (bounded by the
// block's metadata Size) and returns the verified, decompressed row bytes;
// see decodeBlockRowData for the verification order and bounds. The returned
// buffer is plainly allocated and safe to retain (the merge path retains
// views into it via custom-tokenizer output; see bloomEntrySets.indexRow).
func readBlockRowData(file io.ReadSeeker, block *DataBlockMetadata) ([]byte, error) {
	compressedSize := block.Size - block.BloomFiltersSize
	if block.BloomFiltersSize < 0 || compressedSize < 0 {
		return nil, fmt.Errorf("invalid block sizes (size %d, bloom filter section %d)", block.Size, block.BloomFiltersSize)
	}

	if _, err := file.Seek(int64(block.Offset+block.BloomFiltersSize), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to row data: %w", err)
	}
	compressed := make([]byte, compressedSize)
	if _, err := io.ReadFull(file, compressed); err != nil {
		return nil, fmt.Errorf("failed to read row data: %w", err)
	}

	return decodeBlockRowData(compressed, block)
}

// readPooledBlockRowData is readBlockRowData with both the compressed and
// decompressed buffers drawn from the scan buffer pool. The caller must call
// release exactly once, only after no view into the returned buffer can be
// dereferenced again: the buffer will be handed to another block scan and
// overwritten. The query scan path qualifies — row matching parses transient
// views and delivered rows are materialized from independent copies — which is
// exactly what TestMatchedRowNoAliasing guards. The merge path must keep using
// readBlockRowData: its entry-set indexing can retain custom-tokenizer output
// aliasing the buffer.
func readPooledBlockRowData(file io.ReadSeeker, block *DataBlockMetadata) (rowData []byte, release func(), err error) {
	compressedSize := block.Size - block.BloomFiltersSize
	if block.BloomFiltersSize < 0 || compressedSize < 0 {
		return nil, nil, fmt.Errorf("invalid block sizes (size %d, bloom filter section %d)", block.Size, block.BloomFiltersSize)
	}
	if block.UncompressedSize < 0 {
		return nil, nil, fmt.Errorf("invalid uncompressed size %d", block.UncompressedSize)
	}

	if _, err := file.Seek(int64(block.Offset+block.BloomFiltersSize), io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("failed to seek to row data: %w", err)
	}
	compressed := getScanBuffer(compressedSize)
	if _, err := io.ReadFull(file, compressed); err != nil {
		putScanBuffer(compressed)
		return nil, nil, fmt.Errorf("failed to read row data: %w", err)
	}

	if normalizeCompression(block.Compression) == CompressionNone {
		// The decoded data is the compressed buffer itself (after the CRC
		// check); it is released as the row data.
		rowData, err := decodeBlockRowData(compressed, block)
		if err != nil {
			putScanBuffer(compressed)
			return nil, nil, err
		}
		return rowData, func() { putScanBuffer(compressed) }, nil
	}

	dst := getScanBuffer(block.UncompressedSize)
	rowData, err = decodeBlockRowDataInto(dst, compressed, block)
	// The decompressors copy into rowData and their pooled state is Reset
	// inside decode, so the compressed buffer is reusable as soon as decode
	// returns.
	putScanBuffer(compressed)
	if err != nil {
		putScanBuffer(dst)
		return nil, nil, err
	}
	return rowData, func() { putScanBuffer(rowData) }, nil
}

// blockRowScanner iterates the length-prefixed rows of a decoded row data
// section. Every row is a subslice of the section — no per-row allocation —
// and row lengths are validated against the remaining data, so a corrupt
// length prefix produces an error instead of an oversized read.
type blockRowScanner struct {
	data []byte
	pos  int
}

// next returns the next row's bytes. ok is false once the section is
// exhausted; a malformed length prefix or a row length exceeding the
// remaining data returns an error.
func (s *blockRowScanner) next() (row []byte, ok bool, err error) {
	if s.pos == len(s.data) {
		return nil, false, nil
	}
	if len(s.data)-s.pos < LengthPrefixSize {
		return nil, false, fmt.Errorf("truncated row length prefix: %d bytes remaining", len(s.data)-s.pos)
	}
	rowLength := binary.LittleEndian.Uint32(s.data[s.pos:])
	s.pos += LengthPrefixSize
	if uint64(rowLength) > uint64(len(s.data)-s.pos) {
		return nil, false, fmt.Errorf("row length %d exceeds remaining row data %d", rowLength, len(s.data)-s.pos)
	}
	row = s.data[s.pos : s.pos+int(rowLength)]
	s.pos += int(rowLength)
	return row, true, nil
}
