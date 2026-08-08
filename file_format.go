package bloomsearch

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"hash/crc32"

	"github.com/bits-and-blooms/bloom/v3"
)

var (
	ErrInvalidHash = errors.New("invalid hash")
)

// File format constants. FileVersion is the only version this package writes
// or reads: earlier versions stored each block's filter section immediately
// before that block's row data, which cost the query path one request per
// block, and are rejected outright rather than translated.
const (
	FileVersion = uint32(3)

	LengthPrefixSize  = 4
	VersionPrefixSize = 4
	HashSize          = 4
)

// Filter section presence flags: one bit per filter, in on-disk order.
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
// double as the filters' expected-item parameters.
type BloomEntryCounts struct {
	Fields      int
	Tokens      int
	FieldTokens int
}

type FileMetadata struct {
	// BloomFilters are the file-level filters, stored on disk in a binary
	// filter section immediately before the metadata JSON and decoded into
	// this field on read. Any filter may be nil (absent filters cannot
	// disqualify anything at query time).
	BloomFilters           BloomFilters
	BloomFalsePositiveRate float64
	BloomEntryCounts       BloomEntryCounts `json:",omitzero"`

	// BlockFilterRegionOffset and BlockFilterRegionSize locate the file's
	// block filter region: the contiguous run, written after the last block's
	// row data, holding every data block's filter section. A reader consults
	// many blocks' filters per request against this span (see blockFilterCursor
	// for how the query path reads it).
	BlockFilterRegionOffset int
	BlockFilterRegionSize   int

	DataBlocks []DataBlockMetadata
}

// fileMetadataJSON is the JSON payload written into the footer: the
// FileMetadata fields minus the bloom filters, which live in the preceding
// binary filter section (located via FileFilterSectionSize).
type fileMetadataJSON struct {
	BloomFalsePositiveRate float64
	BloomEntryCounts       BloomEntryCounts `json:",omitzero"`

	BlockFilterRegionOffset int
	BlockFilterRegionSize   int

	// FileFilterSectionSize is the size in bytes of the file-level filter
	// section that immediately precedes the metadata JSON. Zero means the
	// file has no file-level filters (they then cannot disqualify the file).
	FileFilterSectionSize int

	DataBlocks []DataBlockMetadata
}

// WriteFileFooter completes a bloom file by writing the footer to w: the
// file-level filter section (binary, see encodeFilterSection), the metadata
// JSON — which carries no filter bytes, only the section's size — its CRC32C
// and length, the file version, and the magic bytes. The engine calls it after
// the block filter region. It covers the footer only: the row data blocks and
// the block filter region are written by the engine, so producing whole files
// outside the engine still requires it — and requires metadata whose
// BlockFilterRegionOffset/Size and per-block offsets describe what was
// actually written, since readers seek by them (see FileMetadata.validate).
func WriteFileFooter(w io.Writer, metadata *FileMetadata) error {
	// Write the file-level filter section
	filterSection, err := encodeFilterSection(&metadata.BloomFilters)
	if err != nil {
		return fmt.Errorf("failed to encode file bloom filters: %w", err)
	}
	if _, err := w.Write(filterSection); err != nil {
		return fmt.Errorf("failed to write file bloom filters: %w", err)
	}

	// Write file metadata
	metadataBytes, err := json.Marshal(fileMetadataJSON{
		BloomFalsePositiveRate:  metadata.BloomFalsePositiveRate,
		BloomEntryCounts:        metadata.BloomEntryCounts,
		BlockFilterRegionOffset: metadata.BlockFilterRegionOffset,
		BlockFilterRegionSize:   metadata.BlockFilterRegionSize,
		FileFilterSectionSize:   len(filterSection),
		DataBlocks:              metadata.DataBlocks,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}
	if _, err := w.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write file metadata: %w", err)
	}

	metadataHashBytes := make([]byte, HashSize)
	binary.LittleEndian.PutUint32(metadataHashBytes, crc32.Checksum(metadataBytes, crc32cTable))
	if _, err := w.Write(metadataHashBytes); err != nil {
		return fmt.Errorf("failed to write file metadata hash: %w", err)
	}

	// Write metadata length
	metadataLengthBytes := make([]byte, LengthPrefixSize)
	binary.LittleEndian.PutUint32(metadataLengthBytes, uint32(len(metadataBytes)))
	if _, err := w.Write(metadataLengthBytes); err != nil {
		return fmt.Errorf("failed to write file metadata length: %w", err)
	}

	// Write version
	versionBytes := make([]byte, VersionPrefixSize)
	binary.LittleEndian.PutUint32(versionBytes, FileVersion)
	if _, err := w.Write(versionBytes); err != nil {
		return fmt.Errorf("failed to write file version: %w", err)
	}

	// Write magic bytes
	if _, err := w.Write([]byte(MagicBytes)); err != nil {
		return fmt.Errorf("failed to write magic bytes: %w", err)
	}

	return nil
}

// ReadFileMetadata locates, verifies, and decodes a bloom file's footer,
// returning the file's metadata (with the file-level bloom filters decoded
// into FileMetadata.BloomFilters) and the file's total size in bytes, so
// external DataStore/MetaStore implementations can parse bloom files without
// reimplementing the footer framing. It reads three ranges: the fixed footer
// tail, the metadata JSON, and the file-level filter section. r's seek
// position on return is unspecified.
//
// The returned metadata is self-consistent: the block filter region, every
// block's row data, and every block's filter section have been checked to lie
// within the file (see FileMetadata.validate), so a reader can seek by them.
func ReadFileMetadata(r io.ReadSeeker) (*FileMetadata, int64, error) {
	fileSize, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to determine file size: %w", err)
	}

	// The fixed-size footer tail, read in one shot:
	// [metadata hash][metadata length][version][magic bytes]
	footer := make([]byte, HashSize+LengthPrefixSize+VersionPrefixSize+len(MagicBytes))
	if fileSize < int64(len(footer)) {
		return nil, 0, fmt.Errorf("file (%d bytes) is too small to be a valid bloom file", fileSize)
	}
	if err := readFullAt(r, footer, fileSize-int64(len(footer))); err != nil {
		return nil, 0, fmt.Errorf("failed to read file footer: %w", err)
	}

	if string(footer[len(footer)-len(MagicBytes):]) != MagicBytes {
		return nil, 0, errors.New("invalid magic bytes")
	}
	version := binary.LittleEndian.Uint32(footer[HashSize+LengthPrefixSize:])
	metadataLength := binary.LittleEndian.Uint32(footer[HashSize:])
	metadataHashBytes := footer[:HashSize]

	metadataOffset := fileSize - int64(len(footer)) - int64(metadataLength)
	if metadataOffset < 0 {
		return nil, 0, fmt.Errorf("metadata length %d exceeds file size %d", metadataLength, fileSize)
	}
	metadataBytes := make([]byte, metadataLength)
	if err := readFullAt(r, metadataBytes, metadataOffset); err != nil {
		return nil, 0, fmt.Errorf("failed to read metadata: %w", err)
	}

	if version != FileVersion {
		return nil, 0, fmt.Errorf("unsupported file version %d (this build reads version %d)", version, FileVersion)
	}

	payload, err := fileMetadataFromBytesWithHash(metadataBytes, metadataHashBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse metadata: %w", err)
	}

	metadata := &FileMetadata{
		BloomFalsePositiveRate:  payload.BloomFalsePositiveRate,
		BloomEntryCounts:        payload.BloomEntryCounts,
		BlockFilterRegionOffset: payload.BlockFilterRegionOffset,
		BlockFilterRegionSize:   payload.BlockFilterRegionSize,
		DataBlocks:              payload.DataBlocks,
	}

	if payload.FileFilterSectionSize < 0 || int64(payload.FileFilterSectionSize) > metadataOffset {
		return nil, 0, fmt.Errorf("invalid file filter section size %d", payload.FileFilterSectionSize)
	}
	// Everything the metadata describes has to fit before the file-level
	// filter section.
	if err := metadata.validate(metadataOffset - int64(payload.FileFilterSectionSize)); err != nil {
		return nil, 0, err
	}

	if payload.FileFilterSectionSize > 0 {
		filterSection := make([]byte, payload.FileFilterSectionSize)
		if err := readFullAt(r, filterSection, metadataOffset-int64(payload.FileFilterSectionSize)); err != nil {
			return nil, 0, fmt.Errorf("failed to read file bloom filters: %w", err)
		}
		filters, err := parseFilterSection(filterSection)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to parse file bloom filters: %w", err)
		}
		metadata.BloomFilters = *filters
	}

	return metadata, fileSize, nil
}

// validate checks that the file's block filter region, every block's row data,
// and every block's filter section lie inside the file's data area — the
// dataLimit bytes preceding the file-level filter section. The metadata's own
// CRC has passed by the time this runs, so a failure means the file does not
// describe itself: readers must not seek or slice by it, since a region that
// runs past the data area (a truncated file, an oversized recorded size) or a
// block whose filters fall outside the region would otherwise drive an
// out-of-bounds read or an absurd allocation.
//
// Sizes are compared by subtraction rather than by adding offset and size, so
// values large enough to overflow cannot slip through the bounds check.
func (m *FileMetadata) validate(dataLimit int64) error {
	if m.BlockFilterRegionOffset < 0 || m.BlockFilterRegionSize < 0 {
		return fmt.Errorf("invalid block filter region (offset %d, size %d)",
			m.BlockFilterRegionOffset, m.BlockFilterRegionSize)
	}
	regionOffset := int64(m.BlockFilterRegionOffset)
	if dataLimit < 0 || regionOffset > dataLimit || int64(m.BlockFilterRegionSize) > dataLimit-regionOffset {
		return fmt.Errorf("block filter region (offset %d, size %d) does not fit in the file's %d-byte data area",
			m.BlockFilterRegionOffset, m.BlockFilterRegionSize, dataLimit)
	}
	regionEnd := regionOffset + int64(m.BlockFilterRegionSize)

	for i := range m.DataBlocks {
		block := &m.DataBlocks[i]
		if block.RowDataOffset < 0 || block.RowDataSize < 0 {
			return fmt.Errorf("block %d: invalid row data location (offset %d, size %d)",
				i, block.RowDataOffset, block.RowDataSize)
		}
		// Row data precedes the region, so the region's start is its limit.
		if int64(block.RowDataOffset) > regionOffset || int64(block.RowDataSize) > regionOffset-int64(block.RowDataOffset) {
			return fmt.Errorf("block %d: row data (offset %d, size %d) runs past the block filter region at %d",
				i, block.RowDataOffset, block.RowDataSize, m.BlockFilterRegionOffset)
		}
		if err := block.validateFilterSection(regionOffset, regionEnd); err != nil {
			return fmt.Errorf("block %d: %w", i, err)
		}
	}
	return nil
}

// validateFilterSection checks that a block's filter section lies within the
// block filter region spanning [regionOffset, regionEnd). A block without a
// section (BloomFilterSize == 0) is always in bounds; its offset is not read.
func (b *DataBlockMetadata) validateFilterSection(regionOffset, regionEnd int64) error {
	if b.BloomFilterSize < 0 {
		return fmt.Errorf("invalid bloom filter section size %d", b.BloomFilterSize)
	}
	if b.BloomFilterSize == 0 {
		return nil
	}
	offset := int64(b.BloomFilterOffset)
	if offset < regionOffset || offset > regionEnd || int64(b.BloomFilterSize) > regionEnd-offset {
		return fmt.Errorf("bloom filter section (offset %d, size %d) is outside the block filter region [%d, %d)",
			b.BloomFilterOffset, b.BloomFilterSize, regionOffset, regionEnd)
	}
	return nil
}

// readFullAt seeks to off and fills buf.
func readFullAt(r io.ReadSeeker, buf []byte, off int64) error {
	if _, err := r.Seek(off, io.SeekStart); err != nil {
		return err
	}
	_, err := io.ReadFull(r, buf)
	return err
}

// fileMetadataFromBytesWithHash verifies a metadata payload's CRC32C and
// decodes it. The caller resolves the file filter section via
// FileFilterSectionSize.
func fileMetadataFromBytesWithHash(payload []byte, expectedHashBytes []byte) (*fileMetadataJSON, error) {
	actualHash := crc32.Checksum(payload, crc32cTable)
	expectedHash := binary.LittleEndian.Uint32(expectedHashBytes)
	if actualHash != expectedHash {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidHash, expectedHash, actualHash)
	}

	var metadata fileMetadataJSON
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

// encodeFilterSection serializes bloom filters into the binary filter section:
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
// the filters. Everything it retains is copied out of section (each filter
// decodes into its own words), so the caller may recycle the bytes as soon as
// it returns — which is what lets the query path parse straight out of a
// pooled block filter region buffer.
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

// ReadDataBlockBloomFilters reads and verifies one data block's filter section
// out of its file's block filter region. A block without a filter section
// (BloomFilterSize == 0) yields empty BloomFilters, which cannot disqualify
// anything.
//
// This is the one-block-at-a-time entry point, for external readers and
// tooling. The query path instead reads many blocks' sections per request (see
// blockFilterCursor), which is the whole point of keeping the sections
// contiguous.
func ReadDataBlockBloomFilters(file io.ReadSeeker, blockMetadata DataBlockMetadata) (*BloomFilters, error) {
	if blockMetadata.BloomFilterSize < 0 || blockMetadata.BloomFilterOffset < 0 {
		return nil, fmt.Errorf("invalid bloom filter section location (offset %d, size %d)",
			blockMetadata.BloomFilterOffset, blockMetadata.BloomFilterSize)
	}
	if blockMetadata.BloomFilterSize == 0 {
		return &BloomFilters{}, nil
	}

	// The section bytes are transient: parseFilterSection copies out what it
	// retains, so the buffer goes straight back to the pool.
	section := getScanBuffer(blockMetadata.BloomFilterSize)
	defer putScanBuffer(section)
	if err := readFullAt(file, section, int64(blockMetadata.BloomFilterOffset)); err != nil {
		return nil, fmt.Errorf("failed to read bloom filters: %w", err)
	}

	return parseFilterSection(section)
}

// blockFilterChunkTarget caps how many bytes of a file's block filter region one
// read may cover. It sets two things at once: the transient memory a file worker
// holds while evaluating filters, and how many requests consulting a whole file's
// filters takes.
//
// A file's region runs roughly 1-5% of file size, which for a 10GB merged file of
// ~1000 blocks is tens of MB — reading that whole span in one request would make
// peak query memory the region size times the file workers in flight (up to
// MaxQueryConcurrency), so a prefilter-free bloom query over a large corpus could
// reach many GB of transient buffers. Chunking makes it 4MiB per in-flight file
// worker instead, at the cost of ceil(region/4MiB) requests: a 27MB region costs
// 7 requests rather than 1, still nothing like the 1000 the pre-region layout
// charged. 4MiB is also well under the MaxRowGroupBytes-sized buffer a block scan
// already holds, so the filter stage is not the query's dominant allocation.
const blockFilterChunkTarget = 4 << 20

// blockFilterCursor reads a file's block filter sections in chunks of at most
// blockFilterChunkTarget bytes and decodes each block's filters out of the chunk
// it landed in. One chunk covers as many consecutive sections as the cap allows,
// so a file whose region fits the cap costs a single request.
//
// Chunks start at the section of the block being evaluated, so a gap between
// sparse candidates — blocks a prefilter dropped — is skipped rather than read,
// as long as it is wider than the cap. A section larger than the cap is read on
// its own: the cap bounds how much slack a read may carry, never whether a block
// can be consulted at all.
//
// Chunk growth walks forward from the block being evaluated, which is optimal when
// sections are laid out in the order the blocks are evaluated (the order this
// package writes them, since a block's filters and its row data are appended
// together). Sections ordered some other way still decode correctly, just across
// more chunks.
type blockFilterCursor struct {
	file   io.ReadSeeker
	blocks []DataBlockMetadata

	// regionStart and regionEnd bound the file's block filter region; every
	// section is checked against them before it is read or sliced.
	regionStart int64
	regionEnd   int64

	// buf holds the chunk currently in hand, drawn from the scan buffer pool,
	// and chunkStart is the file offset buf[0] came from.
	buf        []byte
	chunkStart int64

	// chunkShare is each covered block's even share of the current chunk's read
	// duration: one read serves several blocks, so no block owns it alone.
	chunkShare time.Duration
}

// planBlockFilterReads validates that every block's filter section lies inside
// the file's block filter region, returning the region's bounds and whether any
// block has a section to read. Validating up front makes metadata that does not
// describe its file one file-level failure rather than a surprise partway through
// the pass, and it happens before anything is allocated or read.
func planBlockFilterReads(blocks []DataBlockMetadata, regionOffset, regionSize int) (regionStart, regionEnd int64, hasSections bool, err error) {
	if regionOffset < 0 || regionSize < 0 {
		return 0, 0, false, fmt.Errorf("invalid block filter region (offset %d, size %d)", regionOffset, regionSize)
	}
	regionStart = int64(regionOffset)
	regionEnd = regionStart + int64(regionSize)
	if regionEnd < regionStart {
		return 0, 0, false, fmt.Errorf("block filter region (offset %d, size %d) overflows", regionOffset, regionSize)
	}

	for i := range blocks {
		block := &blocks[i]
		if err := block.validateFilterSection(regionStart, regionEnd); err != nil {
			return 0, 0, false, fmt.Errorf("block at row data offset %d: %w", block.RowDataOffset, err)
		}
		if block.BloomFilterSize > 0 {
			hasSections = true
		}
	}
	return regionStart, regionEnd, hasSections, nil
}

// release returns the chunk buffer to the scan buffer pool. It must be called
// only once no filter section decoded from the cursor is still in use;
// parseFilterSection copies what it retains, so that is as soon as the last call
// to filtersFor has returned.
func (c *blockFilterCursor) release() {
	putScanBuffer(c.buf)
	c.buf = nil
}

// filtersFor decodes block i's filters, reading a chunk of the region first when
// the section is not in the chunk already in hand. A block without a filter
// section yields empty BloomFilters, which cannot disqualify anything.
//
// share is the block's amortized cost of the chunk read that served it — the same
// share for every block that chunk covered, and zero for a block that needed no
// read at all. readFailed reports that the chunk read failed, which leaves the
// handle's position unknown and the rest of the file unreadable on it; any other
// error belongs to this block alone.
func (c *blockFilterCursor) filtersFor(i int) (filters *BloomFilters, share time.Duration, readFailed bool, err error) {
	block := &c.blocks[i]
	if err := block.validateFilterSection(c.regionStart, c.regionEnd); err != nil {
		return nil, 0, false, err
	}
	if block.BloomFilterSize == 0 {
		return &BloomFilters{}, 0, false, nil
	}

	section, ok := c.heldSection(block)
	if !ok {
		if err := c.readChunkFrom(i); err != nil {
			return nil, 0, true, err
		}
		// The chunk just read covers block i by construction; a miss here would
		// mean reading filters from the wrong bytes, which could turn into a
		// false negative, so it is an error rather than a guess.
		if section, ok = c.heldSection(block); !ok {
			return nil, 0, false, fmt.Errorf("bloom filter section (offset %d, size %d) is outside the %d bytes read at %d",
				block.BloomFilterOffset, block.BloomFilterSize, len(c.buf), c.chunkStart)
		}
	}

	filters, err = parseFilterSection(section)
	return filters, c.chunkShare, false, err
}

// heldSection returns the block's filter section out of the chunk in hand, if
// that chunk covers it in full.
func (c *blockFilterCursor) heldSection(block *DataBlockMetadata) ([]byte, bool) {
	if c.buf == nil {
		return nil, false
	}
	offset := int64(block.BloomFilterOffset) - c.chunkStart
	if offset < 0 || offset > int64(len(c.buf)) || int64(block.BloomFilterSize) > int64(len(c.buf))-offset {
		return nil, false
	}
	return c.buf[offset : offset+int64(block.BloomFilterSize)], true
}

// readChunkFrom reads a chunk starting at block i's filter section, extended over
// the sections that follow while they stay within blockFilterChunkTarget of the
// chunk's start. The read replaces whatever chunk was in hand.
func (c *blockFilterCursor) readChunkFrom(i int) error {
	start := int64(c.blocks[i].BloomFilterOffset)
	end := start + int64(c.blocks[i].BloomFilterSize)

	// covered counts the sections this read serves, which is what its cost is
	// divided across.
	covered := 1
	for j := i + 1; j < len(c.blocks); j++ {
		next := &c.blocks[j]
		if next.BloomFilterSize == 0 {
			continue
		}
		if next.validateFilterSection(c.regionStart, c.regionEnd) != nil {
			// filtersFor will report it when the pass reaches this block.
			break
		}
		nextStart := int64(next.BloomFilterOffset)
		nextEnd := nextStart + int64(next.BloomFilterSize)
		if nextStart < start || nextEnd-start > blockFilterChunkTarget {
			// Behind the chunk, or past the cap: this block gets its own chunk,
			// which is what skips a gap wider than the cap instead of reading it.
			break
		}
		if nextEnd > end {
			end = nextEnd
		}
		covered++
	}

	// The new buffer is taken before the old one is released, so they cannot be
	// the same buffer.
	buf := getScanBuffer(int(end - start))
	readStart := time.Now()
	if err := readFullAt(c.file, buf, start); err != nil {
		putScanBuffer(buf)
		return fmt.Errorf("failed to read block filter region: %w", err)
	}
	readDuration := time.Since(readStart)

	putScanBuffer(c.buf)
	c.buf = buf
	c.chunkStart = start
	c.chunkShare = readDuration / time.Duration(covered)
	return nil
}

// blockFilterRegionWriter buffers data blocks' filter sections while their row
// data streams to a file, so that every section can be written as one
// contiguous block filter region once the last block's row data is out. That
// layout is what lets a query read a whole file's block filters in one request.
//
// The cost is memory: a file's filter sections are held until the file is
// closed. Filters run roughly 1-5% of file size (they are sized from measured
// distinct entry counts, so the ratio tracks content cardinality rather than a
// configured guess), so tens of MB for a 1GB file. Nothing spills; the bound is
// whatever bounds the file:
//
//   - A flushed file holds one flush's buffers, so MaxBufferedBytes and
//     MaxBufferedRows (and MaxRowGroupBytes/MaxRowGroupRows, which force a flush
//     as soon as one partition reaches them) cap it. At the defaults a flushed
//     file is megabytes and this buffer is negligible.
//   - A merged file's sources are grouped under MaxFileSize, so that is the cap
//     there. At the default 10GB MaxFileSize a merge can buffer hundreds of MB of
//     filter sections; a deployment that cannot afford it should lower
//     MaxFileSize.
type blockFilterRegionWriter struct {
	buf bytes.Buffer
}

// add appends a block's filter section, returning the offset to record in the
// block's metadata — relative to the start of the region, which is not known
// until the row data is complete — and the section's size. finish rebases those
// offsets to absolute file offsets.
func (r *blockFilterRegionWriter) add(section []byte) (relativeOffset, size int) {
	relativeOffset = r.buf.Len()
	r.buf.Write(section)
	return relativeOffset, len(section)
}

// finish writes the buffered region to w — which must be positioned at
// regionOffset, immediately after the last block's row data — and rebases every
// block's BloomFilterOffset from region-relative (as returned by add) to
// absolute. It returns the region's size, for FileMetadata.BlockFilterRegionSize.
//
// blocks must be exactly the blocks whose sections were added, since each one's
// offset is rebased once.
func (r *blockFilterRegionWriter) finish(w io.Writer, regionOffset int, blocks []DataBlockMetadata) (int, error) {
	if _, err := w.Write(r.buf.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write block filter region: %w", err)
	}
	for i := range blocks {
		blocks[i].BloomFilterOffset += regionOffset
	}
	return r.buf.Len(), nil
}

// CompressionType represents the compression algorithm used for row data
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionSnappy CompressionType = "snappy"
	CompressionZstd   CompressionType = "zstd"
)

type DataBlockMetadata struct {
	// RowDataOffset is the absolute file offset of the block's compressed row
	// data, and RowDataSize its length in bytes (no trailing hash). Row data
	// blocks occupy the front of the file, one after another.
	RowDataOffset int
	RowDataSize   int

	Rows int

	// BloomFilterOffset is the absolute file offset of the block's filter
	// section (filters + trailing CRC32C) inside the file's block filter
	// region, and BloomFilterSize its length. BloomFilterSize == 0 means the
	// block has no filter section, so nothing about it can be disqualified.
	BloomFilterOffset int
	BloomFilterSize   int

	MinMaxIndexes map[string]MinMaxIndex `json:",omitempty"`
	PartitionID   string                 `json:",omitempty"`

	// Compression algorithm used for the row data in this block
	Compression CompressionType `json:",omitempty"`

	// Uncompressed size of row data (for decompression buffer allocation)
	UncompressedSize int `json:",omitempty"`

	// Hash of the compressed row data (CRC32C), valid only when
	// HasRowDataHash is true — 0 is a legitimate checksum value.
	RowDataHash    uint32 `json:",omitempty"`
	HasRowDataHash bool   `json:",omitempty"`

	// BloomEntryCounts are the measured distinct entry counts this block's
	// filters were built and sized from.
	BloomEntryCounts BloomEntryCounts `json:",omitzero"`

	BloomFalsePositiveRate float64
}

// OnDiskSize is the block's full on-disk footprint: its row data plus the
// filter section it owns in the file's block filter region. The two live in
// different parts of the file, so this is a sum rather than one extent.
func (b *DataBlockMetadata) OnDiskSize() int {
	return b.RowDataSize + b.BloomFilterSize
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

// ReadDataBlockRowData reads a block's compressed row data fully (bounded by
// the block's metadata RowDataSize) and returns the verified, decompressed row
// bytes — length-prefixed rows, iterated with a BlockRowScanner; see
// decodeBlockRowData for the verification order and bounds. The returned
// buffer is plainly allocated and safe to retain (the merge path retains
// views into it via custom-tokenizer output; see bloomEntrySets.indexRow).
func ReadDataBlockRowData(file io.ReadSeeker, block *DataBlockMetadata) ([]byte, error) {
	if block.RowDataOffset < 0 || block.RowDataSize < 0 {
		return nil, fmt.Errorf("invalid row data location (offset %d, size %d)", block.RowDataOffset, block.RowDataSize)
	}

	compressed := make([]byte, block.RowDataSize)
	if err := readFullAt(file, compressed, int64(block.RowDataOffset)); err != nil {
		return nil, fmt.Errorf("failed to read row data: %w", err)
	}

	return decodeBlockRowData(compressed, block)
}

// readPooledBlockRowData is ReadDataBlockRowData with both the compressed and
// decompressed buffers drawn from the scan buffer pool. The caller must call
// release exactly once, only after no view into the returned buffer can be
// dereferenced again: the buffer will be handed to another block scan and
// overwritten. The query scan path qualifies — row matching parses transient
// views and delivered rows are materialized from independent copies — which is
// exactly what TestMatchedRowNoAliasing guards. The merge path must keep using
// ReadDataBlockRowData: its entry-set indexing can retain custom-tokenizer
// output aliasing the buffer.
func readPooledBlockRowData(file io.ReadSeeker, block *DataBlockMetadata) (rowData []byte, release func(), err error) {
	if block.RowDataOffset < 0 || block.RowDataSize < 0 {
		return nil, nil, fmt.Errorf("invalid row data location (offset %d, size %d)", block.RowDataOffset, block.RowDataSize)
	}
	if block.UncompressedSize < 0 {
		return nil, nil, fmt.Errorf("invalid uncompressed size %d", block.UncompressedSize)
	}

	compressed := getScanBuffer(block.RowDataSize)
	if err := readFullAt(file, compressed, int64(block.RowDataOffset)); err != nil {
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

// BlockRowScanner iterates the length-prefixed rows of a decoded row data
// section (as returned by ReadDataBlockRowData). Every row is a subslice of
// the section — no per-row allocation — and row lengths are validated against
// the remaining data, so a corrupt length prefix produces an error instead of
// an oversized read.
type BlockRowScanner struct {
	data []byte
	pos  int
}

// NewBlockRowScanner returns a scanner over a decoded row data section.
func NewBlockRowScanner(rowData []byte) *BlockRowScanner {
	return &BlockRowScanner{data: rowData}
}

// Next returns the next row's bytes (a subslice of the section, valid as long
// as the section is). ok is false once the section is exhausted; a malformed
// length prefix or a row length exceeding the remaining data returns an error.
func (s *BlockRowScanner) Next() (row []byte, ok bool, err error) {
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
