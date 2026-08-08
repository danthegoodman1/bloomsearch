package bloomsearch

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// --- measured filter sizing ---

// TestMeasuredFilterSizing asserts filters are sized from the measured
// distinct entry counts of the flushed rows — not from row counts or a
// configured guess — and that those counts are recorded in block and file
// metadata.
func TestMeasuredFilterSizing(t *testing.T) {
	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.RowDataCompression = CompressionNone
	})

	// 100 rows, but only 2 distinct fields, 101 distinct tokens (row0..row99
	// plus the shared "red"), and 101 distinct field::token pairs.
	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{"id": fmt.Sprintf("row%d", i), "color": "red"}
	}
	ingestAndFlush(t, engine, rows)

	maybeFiles, err := collectMaybeFiles(context.Background(), store.GetMaybeFilesForQuery(context.Background(), nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(maybeFiles) != 1 || len(maybeFiles[0].Metadata.DataBlocks) != 1 {
		t.Fatalf("expected 1 file with 1 block, got %+v", maybeFiles)
	}

	wantCounts := BloomEntryCounts{Fields: 2, Tokens: 101, FieldTokens: 101}
	metadata := maybeFiles[0].Metadata
	block := metadata.DataBlocks[0]
	if block.BloomEntryCounts != wantCounts {
		t.Fatalf("block entry counts: want %+v, got %+v", wantCounts, block.BloomEntryCounts)
	}
	if metadata.BloomEntryCounts != wantCounts {
		t.Fatalf("file entry counts: want %+v, got %+v", wantCounts, metadata.BloomEntryCounts)
	}

	fpr := DefaultBloomSearchEngineConfig().BloomFalsePositiveRate

	// The file-level filters must be sized for the measured counts.
	assertSizedFor := func(name string, filter *bloom.BloomFilter, count int) {
		t.Helper()
		want := bloom.NewWithEstimates(uint(count), fpr)
		if filter.Cap() != want.Cap() || filter.K() != want.K() {
			t.Fatalf("%s: want m=%d k=%d (sized for %d entries), got m=%d k=%d",
				name, want.Cap(), want.K(), count, filter.Cap(), filter.K())
		}
	}
	assertSizedFor("file field filter", metadata.BloomFilters.FieldBloomFilter, wantCounts.Fields)
	assertSizedFor("file token filter", metadata.BloomFilters.TokenBloomFilter, wantCounts.Tokens)
	assertSizedFor("file fieldtoken filter", metadata.BloomFilters.FieldTokenBloomFilter, wantCounts.FieldTokens)

	// So must the block filters read from the data block itself.
	file, err := store.OpenFile(context.Background(), maybeFiles[0].PointerBytes)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	blockFilters, err := ReadDataBlockBloomFilters(file, block)
	if err != nil {
		t.Fatalf("failed to read block filters: %v", err)
	}
	assertSizedFor("block field filter", blockFilters.FieldBloomFilter, wantCounts.Fields)
	assertSizedFor("block token filter", blockFilters.TokenBloomFilter, wantCounts.Tokens)
	assertSizedFor("block fieldtoken filter", blockFilters.FieldTokenBloomFilter, wantCounts.FieldTokens)

	// Row-count-derived sizing (the old scheme) would be visibly different.
	rowSized := bloom.NewWithEstimates(uint(len(rows)), fpr)
	if blockFilters.FieldBloomFilter.Cap() == rowSized.Cap() {
		t.Fatalf("block field filter looks row-count sized (m=%d)", rowSized.Cap())
	}
}

// TestFalsePositiveRateWithinBudget pushes ~50k distinct tokens through one
// block and probes 10k absent tokens: the measured false positive rate must
// stay within 3x the configured rate (documented tolerance for the
// probabilistic bound).
func TestFalsePositiveRateWithinBudget(t *testing.T) {
	const (
		numRows       = 5000
		tokensPerRow  = 10
		probeCount    = 10000
		configuredFPR = 0.01
		tolerance     = 3.0
	)

	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.BloomFalsePositiveRate = configuredFPR
		config.MaxBufferedRows = numRows * 2
		config.MaxBufferedBytes = 512 << 20
		config.RowDataCompression = CompressionNone
	})

	rows := make([]map[string]any, numRows)
	token := 0
	for i := range rows {
		var msg bytes.Buffer
		for w := 0; w < tokensPerRow; w++ {
			if w > 0 {
				msg.WriteByte(' ')
			}
			fmt.Fprintf(&msg, "tok%d", token)
			token++
		}
		rows[i] = map[string]any{"m": msg.String()}
	}
	ingestAndFlush(t, engine, rows)

	maybeFiles, err := collectMaybeFiles(context.Background(), store.GetMaybeFilesForQuery(context.Background(), nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(maybeFiles) != 1 || len(maybeFiles[0].Metadata.DataBlocks) != 1 {
		t.Fatalf("expected 1 file with 1 block, got %d files", len(maybeFiles))
	}
	block := maybeFiles[0].Metadata.DataBlocks[0]
	if got := block.BloomEntryCounts.Tokens; got != numRows*tokensPerRow {
		t.Fatalf("expected %d distinct tokens, got %d", numRows*tokensPerRow, got)
	}

	file, err := store.OpenFile(context.Background(), maybeFiles[0].PointerBytes)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	blockFilters, err := ReadDataBlockBloomFilters(file, block)
	if err != nil {
		t.Fatalf("failed to read block filters: %v", err)
	}

	falsePositives := 0
	for i := 0; i < probeCount; i++ {
		if blockFilters.TokenBloomFilter.TestString(fmt.Sprintf("absent%d", i)) {
			falsePositives++
		}
	}
	measured := float64(falsePositives) / float64(probeCount)
	if measured > configuredFPR*tolerance {
		t.Fatalf("measured FPR %.4f exceeds %.1fx configured %.4f", measured, tolerance, configuredFPR)
	}
	t.Logf("measured FPR %.4f (configured %.4f, %d/%d probes)", measured, configuredFPR, falsePositives, probeCount)
}

// --- on-disk layout, round trip, and corruption ---

// rawTestBlock is one data block of a hand-rolled file (see writeRawTestFile).
// filterCapacity, when non-zero, sizes the block's filters for that many entries
// instead of for the rows it actually holds, which is how a test gets filter
// sections of a chosen size without a corpus large enough to earn them.
type rawTestBlock struct {
	partitionID    string
	rows           []map[string]any
	filterCapacity uint
}

// writeRawTestFile hand-rolls a file in the current format: uncompressed row
// data blocks of length-prefixed JSON rows, then the block filter region, then
// the footer through the exported WriteFileFooter. It lets a test produce
// framing the engine never writes — a block with no row data hash, metadata
// that lies about the layout — and doubles as coverage that an external writer
// can build a readable file from the exported footer writer.
//
// mutate, when non-nil, sees the metadata just before the footer is written.
func writeRawTestFile(t *testing.T, path string, blocks []rawTestBlock, includeRowHash bool, mutate func(*FileMetadata)) {
	t.Helper()

	const fpr = 0.01

	var out bytes.Buffer
	var filterRegion blockFilterRegionWriter
	fileEntries := newBloomEntrySets()
	metadata := FileMetadata{BloomFalsePositiveRate: fpr}

	for _, block := range blocks {
		entries := newBloomEntrySets()
		var rowData bytes.Buffer
		var lengthBytes [LengthPrefixSize]byte
		for _, row := range block.rows {
			rowBytes, err := json.Marshal(row)
			if err != nil {
				t.Fatalf("failed to marshal row: %v", err)
			}
			entries.indexRow(rowBytes, BasicWhitespaceLowerTokenizer)
			binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(rowBytes)))
			rowData.Write(lengthBytes[:])
			rowData.Write(rowBytes)
		}
		entries.unionInto(fileEntries)

		filters := entries.buildFilters(fpr)
		if block.filterCapacity > 0 {
			// Same entries, filters sized for a capacity the rows never justify.
			filters = BloomFilters{
				FieldBloomFilter:      bloom.NewWithEstimates(block.filterCapacity, fpr),
				TokenBloomFilter:      bloom.NewWithEstimates(block.filterCapacity, fpr),
				FieldTokenBloomFilter: bloom.NewWithEstimates(block.filterCapacity, fpr),
			}
			for entry := range entries.fields {
				filters.FieldBloomFilter.AddString(entry)
			}
			for entry := range entries.tokens {
				filters.TokenBloomFilter.AddString(entry)
			}
			for entry := range entries.fieldTokens {
				filters.FieldTokenBloomFilter.AddString(entry)
			}
		}
		section, err := encodeFilterSection(&filters)
		if err != nil {
			t.Fatalf("failed to encode filter section: %v", err)
		}
		filterOffset, filterSize := filterRegion.add(section)

		rowDataOffset := out.Len()
		out.Write(rowData.Bytes())

		metadata.DataBlocks = append(metadata.DataBlocks, DataBlockMetadata{
			PartitionID:            block.partitionID,
			Rows:                   len(block.rows),
			RowDataOffset:          rowDataOffset,
			RowDataSize:            rowData.Len(),
			BloomFilterOffset:      filterOffset,
			BloomFilterSize:        filterSize,
			Compression:            CompressionNone,
			UncompressedSize:       rowData.Len(),
			RowDataHash:            crc32.Checksum(rowData.Bytes(), crc32cTable),
			HasRowDataHash:         includeRowHash,
			BloomEntryCounts:       entries.counts(),
			BloomFalsePositiveRate: fpr,
		})
	}

	regionOffset := out.Len()
	regionSize, err := filterRegion.finish(&out, regionOffset, metadata.DataBlocks)
	if err != nil {
		t.Fatalf("failed to write block filter region: %v", err)
	}
	metadata.BlockFilterRegionOffset = regionOffset
	metadata.BlockFilterRegionSize = regionSize
	metadata.BloomFilters = fileEntries.buildFilters(fpr)
	metadata.BloomEntryCounts = fileEntries.counts()

	if mutate != nil {
		mutate(&metadata)
	}
	if err := WriteFileFooter(&out, &metadata); err != nil {
		t.Fatalf("failed to write footer: %v", err)
	}
	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
}

// TestBlockFilterRegionLayout asserts the on-disk layout a multi-block file is
// written in: every block's row data first, contiguous from offset 0, then one
// contiguous block filter region holding every block's filter section, then the
// file-level filter section and footer. This is the layout that lets a query
// read a whole file's block filters in one request; the per-file read count is
// asserted in TestQueryFilterReadsPerFileNotPerBlock.
func TestBlockFilterRegionLayout(t *testing.T) {
	const blocks = 5

	dir := t.TempDir()
	store := buildMultiBlockFiles(t, dir, 1, blocks, 3)

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil || len(maybeFiles) != 1 {
		t.Fatalf("expected 1 file, got %d (err %v)", len(maybeFiles), err)
	}
	metadata := maybeFiles[0].Metadata
	if len(metadata.DataBlocks) != blocks {
		t.Fatalf("expected %d blocks, got %d", blocks, len(metadata.DataBlocks))
	}

	// Row data occupies the front of the file, one block after another.
	rowData := slices.Clone(metadata.DataBlocks)
	slices.SortFunc(rowData, func(a, b DataBlockMetadata) int { return cmp.Compare(a.RowDataOffset, b.RowDataOffset) })
	expectedOffset := 0
	for i, block := range rowData {
		if block.RowDataOffset != expectedOffset {
			t.Fatalf("block %d row data starts at %d, want %d (row data must be contiguous from 0)", i, block.RowDataOffset, expectedOffset)
		}
		if block.RowDataSize <= 0 {
			t.Fatalf("block %d has no row data: %+v", i, block)
		}
		expectedOffset += block.RowDataSize
	}

	// The region begins where the row data ends and holds every block's filter
	// section, back to back, with nothing left over.
	if metadata.BlockFilterRegionOffset != expectedOffset {
		t.Fatalf("block filter region starts at %d, want %d (immediately after the row data)",
			metadata.BlockFilterRegionOffset, expectedOffset)
	}
	filters := slices.Clone(metadata.DataBlocks)
	slices.SortFunc(filters, func(a, b DataBlockMetadata) int { return cmp.Compare(a.BloomFilterOffset, b.BloomFilterOffset) })
	expectedOffset = metadata.BlockFilterRegionOffset
	for i, block := range filters {
		if block.BloomFilterOffset != expectedOffset {
			t.Fatalf("block %d filter section starts at %d, want %d (sections must be contiguous)", i, block.BloomFilterOffset, expectedOffset)
		}
		if block.BloomFilterSize <= 0 {
			t.Fatalf("block %d has no filter section: %+v", i, block)
		}
		expectedOffset += block.BloomFilterSize
	}
	if got := metadata.BlockFilterRegionOffset + metadata.BlockFilterRegionSize; got != expectedOffset {
		t.Fatalf("block filter region ends at %d, but its sections end at %d", got, expectedOffset)
	}

	// The region and the footer account for the whole file: the file-level
	// filter section starts where the region ends.
	raw, err := os.ReadFile(string(maybeFiles[0].PointerBytes))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	fileFilterOffset, _ := footerOffsets(t, raw)
	if int64(expectedOffset) != fileFilterOffset {
		t.Fatalf("block filter region ends at %d but the file-level filter section starts at %d", expectedOffset, fileFilterOffset)
	}

	// Every block's filters are readable one at a time from those offsets too.
	file, err := store.OpenFile(ctx, maybeFiles[0].PointerBytes)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()
	for i, block := range metadata.DataBlocks {
		blockFilters, err := ReadDataBlockBloomFilters(file, block)
		if err != nil {
			t.Fatalf("block %d filters unreadable: %v", i, err)
		}
		if blockFilters.TokenBloomFilter == nil {
			t.Fatalf("block %d has no token filter", i)
		}
	}
}

// footerOffsets returns the offsets of a file's file-level filter section and
// its metadata JSON, parsed from the fixed footer tail.
func footerOffsets(t *testing.T, raw []byte) (fileFilterOffset, metadataOffset int64) {
	t.Helper()

	fileSize := int64(len(raw))
	tail := int64(len(MagicBytes) + VersionPrefixSize + LengthPrefixSize + HashSize)
	metadataLength := int64(binary.LittleEndian.Uint32(raw[fileSize-tail+int64(HashSize):]))
	metadataOffset = fileSize - tail - metadataLength

	var payload fileMetadataJSON
	if err := json.Unmarshal(raw[metadataOffset:metadataOffset+metadataLength], &payload); err != nil {
		t.Fatalf("failed to parse metadata JSON: %v", err)
	}
	return metadataOffset - int64(payload.FileFilterSectionSize), metadataOffset
}

// TestFileFormatRoundTrip writes a file, reads its metadata and filters back
// exactly, and then corrupts every section in turn: each corruption must
// produce a clean error — no panic — and a corrupt block must emit zero rows
// (verify-before-emit).
func TestFileFormatRoundTrip(t *testing.T) {
	dir := t.TempDir()
	metaStore := NewMemoryMetaStore()
	dataStore := NewFileSystemDataStore(dir)
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	config.RowDataCompression = CompressionNone
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	})

	rows := []map[string]any{
		{"id": "alpha", "service": "auth"},
		{"id": "beta", "service": "payment"},
		{"id": "gamma", "service": "search"},
	}
	ingestAndFlush(t, engine, rows)

	// The metadata handed to the MetaStore at flush is the source of truth;
	// what the file reader decodes must match it exactly.
	ctx := context.Background()
	written, err := collectMaybeFiles(ctx, metaStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil || len(written) != 1 {
		t.Fatalf("expected 1 file in metastore, got %d (err %v)", len(written), err)
	}
	datPath := string(written[0].PointerBytes)

	readBack, err := dataStore.readFileMetadata(datPath)
	if err != nil {
		t.Fatalf("failed to read metadata back: %v", err)
	}

	source := written[0].Metadata
	// Compare canonical JSON: an empty MinMaxIndexes map marshals away
	// (omitempty) and reads back nil, which is semantically identical.
	sourceBlocksJSON, _ := json.Marshal(source.DataBlocks)
	readBlocksJSON, _ := json.Marshal(readBack.DataBlocks)
	if !bytes.Equal(sourceBlocksJSON, readBlocksJSON) {
		t.Fatalf("data blocks round trip mismatch:\nwrote %s\nread  %s", sourceBlocksJSON, readBlocksJSON)
	}
	if readBack.BloomFalsePositiveRate != source.BloomFalsePositiveRate || readBack.BloomEntryCounts != source.BloomEntryCounts {
		t.Fatalf("metadata scalars round trip mismatch: wrote (%v, %+v), read (%v, %+v)",
			source.BloomFalsePositiveRate, source.BloomEntryCounts, readBack.BloomFalsePositiveRate, readBack.BloomEntryCounts)
	}
	if readBack.BlockFilterRegionOffset != source.BlockFilterRegionOffset || readBack.BlockFilterRegionSize != source.BlockFilterRegionSize {
		t.Fatalf("block filter region round trip mismatch: wrote (%d, %d), read (%d, %d)",
			source.BlockFilterRegionOffset, source.BlockFilterRegionSize, readBack.BlockFilterRegionOffset, readBack.BlockFilterRegionSize)
	}
	if !readBack.BloomFilters.FieldBloomFilter.Equal(source.BloomFilters.FieldBloomFilter) ||
		!readBack.BloomFilters.TokenBloomFilter.Equal(source.BloomFilters.TokenBloomFilter) ||
		!readBack.BloomFilters.FieldTokenBloomFilter.Equal(source.BloomFilters.FieldTokenBloomFilter) {
		t.Fatalf("file-level bloom filters did not round trip bit-exactly")
	}

	// Block filters decode and behave.
	file, err := dataStore.OpenFile(ctx, written[0].PointerBytes)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	block := source.DataBlocks[0]
	blockFilters, err := ReadDataBlockBloomFilters(file, block)
	file.Close()
	if err != nil {
		t.Fatalf("failed to read block filters: %v", err)
	}
	if !blockFilters.TokenBloomFilter.TestString("alpha") {
		t.Fatalf("block token filter lost an inserted token")
	}

	pristine, err := os.ReadFile(datPath)
	if err != nil {
		t.Fatalf("failed to read pristine file: %v", err)
	}
	fileFilterOffset, metadataOffset := footerOffsets(t, pristine)

	// corruptAt returns a directory holding a copy of the file with one byte
	// flipped at the given offset.
	corruptAt := func(t *testing.T, offset int64) (string, string) {
		t.Helper()
		corruptDir := t.TempDir()
		mutated := append([]byte(nil), pristine...)
		mutated[offset] ^= 0xFF
		corruptPath := filepath.Join(corruptDir, "corrupt.dat")
		if err := os.WriteFile(corruptPath, mutated, 0o600); err != nil {
			t.Fatalf("failed to write corrupt file: %v", err)
		}
		return corruptDir, corruptPath
	}

	// queryCorrupt runs a query over the corrupted file and returns the rows
	// delivered and the cursor's terminal error.
	queryCorrupt := func(t *testing.T, corruptDir string, query *Query) ([]map[string]any, error) {
		t.Helper()
		corruptEngine, _ := newFileSystemStoreEngine(t, corruptDir, func(config *BloomSearchEngineConfig) {
			config.RowDataCompression = CompressionNone
		})
		res, err := corruptEngine.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("query setup failed (want block-level error): %v", err)
		}
		defer res.Close()
		var got []map[string]any
		for res.Next() {
			got = append(got, res.Row())
		}
		return got, res.Err()
	}

	t.Run("corrupt file filter section", func(t *testing.T) {
		_, corruptPath := corruptAt(t, fileFilterOffset)
		if _, err := dataStore.readFileMetadata(corruptPath); err == nil {
			t.Fatalf("expected error reading metadata with corrupt file filter section")
		}
	})

	t.Run("corrupt metadata JSON", func(t *testing.T) {
		_, corruptPath := corruptAt(t, metadataOffset)
		if _, err := dataStore.readFileMetadata(corruptPath); err == nil {
			t.Fatalf("expected error reading corrupt metadata")
		}
	})

	t.Run("corrupt block filter section in the region", func(t *testing.T) {
		corruptDir, _ := corruptAt(t, int64(block.BloomFilterOffset)+1)
		rows, err := queryCorrupt(t, corruptDir, NewQuery().Token("alpha").Build())
		if err == nil {
			t.Fatalf("expected block error from corrupt filter section")
		}
		if len(rows) != 0 {
			t.Fatalf("corrupt block emitted %d rows", len(rows))
		}
	})

	t.Run("corrupt row data", func(t *testing.T) {
		// Flip a byte in the middle of the row data: the CRC check must
		// reject the block before any row is emitted, even though earlier
		// rows in the block are intact.
		corruptDir, _ := corruptAt(t, int64(block.RowDataOffset+block.RowDataSize/2))
		rows, err := queryCorrupt(t, corruptDir, nil)
		if err == nil {
			t.Fatalf("expected block error from corrupt row data")
		}
		if len(rows) != 0 {
			t.Fatalf("corrupt block emitted %d rows before hash check", len(rows))
		}
	})

	t.Run("corrupt row length prefix", func(t *testing.T) {
		// The first 4 row data bytes are the first row's length prefix.
		corruptDir, _ := corruptAt(t, int64(block.RowDataOffset))
		rows, err := queryCorrupt(t, corruptDir, nil)
		if err == nil {
			t.Fatalf("expected block error from corrupt length prefix")
		}
		if len(rows) != 0 {
			t.Fatalf("corrupt block emitted %d rows", len(rows))
		}
	})
}

// TestUnsupportedFileVersionRejected asserts a file whose footer carries any
// other version is rejected outright rather than parsed as if the layout
// matched: earlier versions stored block filters immediately before their row
// data, so misreading one would read filters from row data bytes.
func TestUnsupportedFileVersionRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wrongversion.dat")
	writeRawTestFile(t, path, []rawTestBlock{{rows: []map[string]any{{"id": "r1"}}}}, true, nil)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	versionOffset := len(raw) - len(MagicBytes) - VersionPrefixSize
	for _, version := range []uint32{1, 2, FileVersion + 1} {
		binary.LittleEndian.PutUint32(raw[versionOffset:], version)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatalf("failed to write file: %v", err)
		}
		if _, err := NewFileSystemDataStore(dir).readFileMetadata(path); err == nil {
			t.Fatalf("version %d was accepted", version)
		} else if !strings.Contains(err.Error(), "unsupported file version") {
			t.Fatalf("version %d: want an unsupported-version error, got %v", version, err)
		}
	}
}

// TestBlockFilterRegionFramingRejected asserts metadata whose recorded framing
// does not describe the file is rejected when the footer is read, before any
// reader can seek or slice by it. The metadata's own CRC covers these values, so
// they cannot be flipped in place — each case is written by a writer that
// records a lie.
func TestBlockFilterRegionFramingRejected(t *testing.T) {
	blocks := []rawTestBlock{
		{rows: []map[string]any{{"id": "r1"}}},
		{rows: []map[string]any{{"id": "r2"}}},
	}

	cases := []struct {
		name   string
		mutate func(*FileMetadata)
		want   string
	}{
		{
			name:   "region larger than the file",
			mutate: func(m *FileMetadata) { m.BlockFilterRegionSize += 1 << 20 },
			want:   "does not fit in the file",
		},
		{
			name:   "region offset past the file",
			mutate: func(m *FileMetadata) { m.BlockFilterRegionOffset += 1 << 20 },
			want:   "does not fit in the file",
		},
		{
			name:   "negative region size",
			mutate: func(m *FileMetadata) { m.BlockFilterRegionSize = -1 },
			want:   "invalid block filter region",
		},
		{
			name: "region truncated below its sections",
			mutate: func(m *FileMetadata) {
				m.BlockFilterRegionSize -= m.DataBlocks[1].BloomFilterSize
			},
			want: "outside the block filter region",
		},
		{
			name: "block filter section before the region",
			mutate: func(m *FileMetadata) {
				m.DataBlocks[0].BloomFilterOffset = m.BlockFilterRegionOffset - 1
			},
			want: "outside the block filter region",
		},
		{
			name:   "negative block filter size",
			mutate: func(m *FileMetadata) { m.DataBlocks[0].BloomFilterSize = -1 },
			want:   "invalid bloom filter section size",
		},
		{
			name: "row data overlapping the region",
			mutate: func(m *FileMetadata) {
				m.DataBlocks[0].RowDataSize = m.BlockFilterRegionOffset + 1
			},
			want: "runs past the block filter region",
		},
		{
			name:   "negative row data offset",
			mutate: func(m *FileMetadata) { m.DataBlocks[0].RowDataOffset = -1 },
			want:   "invalid row data location",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "framing.dat")
			writeRawTestFile(t, path, blocks, true, tc.mutate)

			_, err := NewFileSystemDataStore(dir).readFileMetadata(path)
			if err == nil {
				t.Fatalf("expected an error reading a file with %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}

// blockMutatingMetaStore yields the wrapped store's files with mutate applied to
// each file's metadata, so a test can serve block metadata that does not match
// the file on disk — what a MetaStore holding its own copy could do, and what
// the footer reader's validation cannot catch.
type blockMutatingMetaStore struct {
	inner  MetaStore
	mutate func(*FileMetadata)
}

func (s *blockMutatingMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		for file, err := range s.inner.GetMaybeFilesForQuery(ctx, query) {
			if err != nil {
				yield(MaybeFile{}, err)
				return
			}
			file.Metadata.DataBlocks = slices.Clone(file.Metadata.DataBlocks)
			s.mutate(&file.Metadata)
			if !yield(file, nil) {
				return
			}
		}
	}
}

func (s *blockMutatingMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return s.inner.Update(ctx, writes, deletes)
}

// TestQueryRejectsOutOfBoundsFilterMetadata: a MetaStore serving block metadata
// whose filter sections fall outside the file's block filter region — or outside
// the file entirely — makes the query fail cleanly with no rows and no panic,
// rather than reading filters from whatever bytes those offsets land on. Filters
// read from the wrong bytes could produce false negatives, so the read must
// refuse rather than guess.
func TestQueryRejectsOutOfBoundsFilterMetadata(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*FileMetadata)
	}{
		{"filter section past the region", func(m *FileMetadata) {
			m.DataBlocks[0].BloomFilterOffset = m.BlockFilterRegionOffset + m.BlockFilterRegionSize
		}},
		{"filter section before the region", func(m *FileMetadata) {
			m.DataBlocks[0].BloomFilterOffset = 0
		}},
		{"absurd filter section size", func(m *FileMetadata) {
			m.DataBlocks[0].BloomFilterSize = 1 << 40
		}},
		{"region past the end of the file", func(m *FileMetadata) {
			m.BlockFilterRegionOffset += 1 << 30
			m.BlockFilterRegionSize = 1 << 20
			for i := range m.DataBlocks {
				m.DataBlocks[i].BloomFilterOffset += 1 << 30
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fsStore := buildMultiBlockFiles(t, dir, 1, 3, 4)
			metaStore := &blockMutatingMetaStore{inner: fsStore, mutate: tc.mutate}

			config := DefaultBloomSearchEngineConfig()
			config.MaxBufferedTime = time.Hour
			engine, err := NewBloomSearchEngine(config, metaStore, fsStore)
			if err != nil {
				t.Fatalf("failed to create engine: %v", err)
			}

			res, err := engine.Query(context.Background(), NewQuery().Token(blockTestMarkerToken).Build())
			if err != nil {
				t.Fatalf("query setup failed: %v", err)
			}
			defer res.Close()
			rows := 0
			for res.Next() {
				rows++
			}
			if res.Err() == nil {
				t.Fatalf("expected an error from unusable filter metadata")
			}
			if rows != 0 {
				t.Fatalf("delivered %d rows from a file whose filter metadata is unusable", rows)
			}
		})
	}
}

// TestOversizeRowLengthRejected corrupts a row length prefix in a block that
// carries no row data hash, so no CRC can catch it first: the scanner must
// reject the oversized length with a clean error instead of attempting a giant
// allocation.
func TestOversizeRowLengthRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nohash.dat")
	writeRawTestFile(t, path, []rawTestBlock{{rows: []map[string]any{
		{"id": "r1"},
		{"id": "r2"},
	}}}, false /* includeRowHash */, nil)

	// Locate the row data start from the (valid) metadata and stamp an
	// absurd length into the first row's prefix.
	store := NewFileSystemDataStore(dir)
	metadata, err := store.readFileMetadata(path)
	if err != nil {
		t.Fatalf("failed to read metadata: %v", err)
	}
	block := metadata.DataBlocks[0]
	if block.HasRowDataHash {
		t.Fatalf("test requires a block with no row data hash")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	binary.LittleEndian.PutUint32(raw[block.RowDataOffset:], 0xFFFFFF00)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("failed to write corrupted file: %v", err)
	}

	engine, _ := newFileSystemStoreEngine(t, dir, nil)
	res, err := engine.Query(context.Background(), nil)
	if err != nil {
		t.Fatalf("query setup failed: %v", err)
	}
	defer res.Close()
	rowCount := 0
	for res.Next() {
		rowCount++
	}
	err = res.Err()
	if err == nil {
		t.Fatalf("expected block error for oversized row length")
	}
	if !strings.Contains(err.Error(), "exceeds remaining row data") {
		t.Fatalf("expected oversize-rejection error, got: %v", err)
	}
	if rowCount != 0 {
		t.Fatalf("corrupt block emitted %d rows", rowCount)
	}
}

// --- conditional filter reads and file filter release ---

// TestNoFilterReadWhenNoBloomConditions asserts that a query without bloom
// conditions never reads into the block filter region: scans go straight to the
// row data, and the file is never even opened for a filter pass. A
// bloom-conditioned control query proves the tracker sees filter reads when they
// do happen.
func TestNoFilterReadWhenNoBloomConditions(t *testing.T) {
	dir := t.TempDir()
	seedEngine, fsStore := newFileSystemStoreEngine(t, dir, nil)
	ingestAndFlush(t, seedEngine, []map[string]any{
		{"id": "one", "service": "auth"},
		{"id": "two", "service": "payment"},
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	seedEngine.Stop(stopCtx)
	cancel()

	tracking := &instrumentedDataStore{inner: fsStore}
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	engine, err := NewBloomSearchEngine(config, fsStore, tracking)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	})

	ctx := context.Background()
	maybeFiles, err := collectMaybeFiles(ctx, fsStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil || len(maybeFiles) != 1 {
		t.Fatalf("expected 1 file, got %d (err %v)", len(maybeFiles), err)
	}
	file := maybeFiles[0]
	regionStart := int64(file.Metadata.BlockFilterRegionOffset)
	regionEnd := regionStart + int64(file.Metadata.BlockFilterRegionSize)
	if file.Metadata.BlockFilterRegionSize <= 0 {
		t.Fatalf("file has no block filter region: %+v", file.Metadata)
	}

	readsIntoRegion := func() int {
		count := 0
		for _, read := range tracking.readsFor(file.PointerBytes) {
			if read.offset < regionEnd && read.offset+int64(read.length) > regionStart {
				count++
			}
		}
		return count
	}

	// Match-all query: no bloom conditions, no regex.
	rows := collectQueryRows(t, engine, nil)
	if len(rows) != 2 {
		t.Fatalf("match-all returned %d rows, want 2", len(rows))
	}
	if n := readsIntoRegion(); n != 0 {
		t.Fatalf("%d reads touched the block filter region for a query with no bloom conditions", n)
	}

	// Control: a bloom-conditioned query must read the region.
	tracking.resetReads()
	_ = collectQueryRows(t, engine, NewQuery().Token("one").Build())
	if n := readsIntoRegion(); n != 1 {
		t.Fatalf("control query with bloom conditions issued %d region reads, want 1", n)
	}
}

// TestFileFiltersReleasedAfterFileTest asserts that once the file-level bloom
// test has run, the engine drops its references to the file-level filters
// before block jobs are enqueued, so per-query memory stops scaling with
// candidate-file size.
func TestFileFiltersReleasedAfterFileTest(t *testing.T) {
	dir := t.TempDir()
	metaStore := NewMemoryMetaStore()
	dataStore := NewFileSystemDataStore(dir)
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	engine, err := NewBloomSearchEngine(config, metaStore, dataStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	})

	ingestAndFlush(t, engine, []map[string]any{{"id": "one", "service": "auth"}})
	ingestAndFlush(t, engine, []map[string]any{{"id": "two", "service": "payment"}})

	// The hook runs on the query's file-stage goroutine. Reading observed
	// after the cursor drains needs no lock: Next returning false
	// happens-after the file stage exited, and with it every hook call.
	var observed []MaybeFile
	engine.queryFilePruneHook = func(file MaybeFile) {
		observed = append(observed, file)
	}

	// Token matches both files, so both pass the file-level test.
	rows := collectQueryRows(t, engine, NewQuery().Field("service").Build())
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if len(observed) != 2 {
		t.Fatalf("expected 2 matching files at the prune hook, got %d", len(observed))
	}
	for i, file := range observed {
		filters := file.Metadata.BloomFilters
		if filters.FieldBloomFilter != nil || filters.TokenBloomFilter != nil || filters.FieldTokenBloomFilter != nil {
			t.Fatalf("file %d retained file-level bloom filters after the file test", i)
		}
	}

	// The MetaStore's own copies are untouched by the release.
	maybeFiles, err := collectMaybeFiles(context.Background(), metaStore.GetMaybeFilesForQuery(context.Background(), nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	for _, file := range maybeFiles {
		if file.Metadata.BloomFilters.TokenBloomFilter == nil {
			t.Fatalf("release leaked into the MetaStore's copy")
		}
	}
}

// --- merge-time rebuild ---

// TestMergeRebuildsFilters merges blocks whose filters have different sizes
// (measured sizing makes that the norm) and asserts the merged block's
// filters are rebuilt right-sized from the union of entries, every row stays
// findable, and the merged file-level filter is present and prunes misses.
func TestMergeRebuildsFilters(t *testing.T) {
	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.RowDataCompression = CompressionNone
	})
	ctx := context.Background()

	makeRows := func(prefix string, n int) []map[string]any {
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{"id": fmt.Sprintf("%s%d", prefix, i)}
		}
		return rows
	}
	smallRows := makeRows("a", 20)
	largeRows := makeRows("b", 200)
	ingestAndFlush(t, engine, smallRows)
	ingestAndFlush(t, engine, largeRows)

	// Sanity: measured sizing gave the two files' blocks different filter
	// parameters, the case that could never merge before the rebuild path.
	before, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil || len(before) != 2 {
		t.Fatalf("expected 2 files before merge, got %d (err %v)", len(before), err)
	}
	if before[0].Metadata.BloomFilters.TokenBloomFilter.Cap() == before[1].Metadata.BloomFilters.TokenBloomFilter.Cap() {
		t.Fatalf("expected differently-sized source filters, both have m=%d", before[0].Metadata.BloomFilters.TokenBloomFilter.Cap())
	}

	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	after, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("expected 1 file after merge, got %d", len(after))
	}
	merged := after[0].Metadata
	if len(merged.DataBlocks) != 1 {
		t.Fatalf("expected 1 merged block, got %d", len(merged.DataBlocks))
	}

	// The merged block's filters are sized from the union's measured counts.
	unionTokens := len(smallRows) + len(largeRows)
	wantCounts := BloomEntryCounts{Fields: 1, Tokens: unionTokens, FieldTokens: unionTokens}
	block := merged.DataBlocks[0]
	if block.BloomEntryCounts != wantCounts {
		t.Fatalf("merged block entry counts: want %+v, got %+v", wantCounts, block.BloomEntryCounts)
	}
	fpr := DefaultBloomSearchEngineConfig().BloomFalsePositiveRate
	wantFilter := bloom.NewWithEstimates(uint(unionTokens), fpr)
	file, err := store.OpenFile(ctx, after[0].PointerBytes)
	if err != nil {
		t.Fatalf("failed to open merged file: %v", err)
	}
	blockFilters, err := ReadDataBlockBloomFilters(file, block)
	file.Close()
	if err != nil {
		t.Fatalf("failed to read merged block filters: %v", err)
	}
	if blockFilters.TokenBloomFilter.Cap() != wantFilter.Cap() || blockFilters.TokenBloomFilter.K() != wantFilter.K() {
		t.Fatalf("merged block token filter: want m=%d k=%d, got m=%d k=%d",
			wantFilter.Cap(), wantFilter.K(), blockFilters.TokenBloomFilter.Cap(), blockFilters.TokenBloomFilter.K())
	}

	// Every source row is still found through its own token query.
	for _, rows := range [][]map[string]any{smallRows, largeRows} {
		for _, row := range rows {
			id := row["id"].(string)
			ids := rowIDs(collectQueryRows(t, engine, NewQuery().Token(id).Build()))
			if ids[id] != 1 {
				t.Fatalf("row %s not found exactly once after merge: %v", id, ids)
			}
		}
	}

	// The rebuilt file-level filter is present and effective: find a probe
	// token it rejects (retry a few in case of a false positive), then prove
	// the engine prunes the whole file for it — no blocks processed at all.
	if merged.BloomFilters.TokenBloomFilter == nil {
		t.Fatalf("merged file has no file-level token filter")
	}
	if merged.BloomEntryCounts != wantCounts {
		t.Fatalf("merged file entry counts: want %+v, got %+v", wantCounts, merged.BloomEntryCounts)
	}
	probe := ""
	for i := 0; i < 5; i++ {
		candidate := fmt.Sprintf("zzzabsent%d", i)
		if !merged.BloomFilters.TokenBloomFilter.TestString(candidate) {
			probe = candidate
			break
		}
	}
	if probe == "" {
		t.Fatalf("file-level filter claims to contain 5 distinct absent tokens; it is not effective")
	}
	res, err := engine.Query(ctx, NewQuery().Token(probe).Build())
	if err != nil {
		t.Fatalf("miss query failed: %v", err)
	}
	for res.Next() {
		t.Fatalf("miss query returned a row")
	}
	if err := res.Err(); err != nil {
		t.Fatalf("miss query error: %v", err)
	}
	stats := res.Stats()
	res.Close()
	if stats.BlocksProcessed != 0 || stats.BlocksSkipped != 0 {
		t.Fatalf("file-level filter did not prune the file: %d processed, %d skipped blocks",
			stats.BlocksProcessed, stats.BlocksSkipped)
	}
}

// TestMergeRebuildsBlockFilterRegion merges files whose blocks mostly cannot
// combine — each partition is alone, so most blocks are copied verbatim — and
// asserts the output is a well-formed multi-block file: its blocks' filter
// sections were rebuilt into one contiguous region behind the row data, the
// copied sections still decode, and every row is still findable through a
// bloom-conditioned query.
func TestMergeRebuildsBlockFilterRegion(t *testing.T) {
	const partitions = 6

	dir := t.TempDir()
	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.RowDataCompression = CompressionNone
		config.PartitionFunc = func(row map[string]any) string {
			part, _ := row["part"].(string)
			return part
		}
	})
	ctx := context.Background()

	// Two files: partition "p0" exists in both (its blocks merge), every other
	// partition exists in one file only (its block is copied verbatim).
	var wantIDs []string
	for f := 0; f < 2; f++ {
		var rows []map[string]any
		for p := 0; p < partitions; p++ {
			part := fmt.Sprintf("p%d", p)
			if p != 0 && p%2 != f {
				continue
			}
			id := fmt.Sprintf("f%d-%s", f, part)
			rows = append(rows, map[string]any{"id": id, "part": part})
			wantIDs = append(wantIDs, id)
		}
		ingestAndFlush(t, engine, rows)
	}

	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	after, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil || len(after) != 1 {
		t.Fatalf("expected 1 merged file, got %d (err %v)", len(after), err)
	}
	merged := after[0]
	metadata := merged.Metadata
	if len(metadata.DataBlocks) != partitions {
		t.Fatalf("expected %d blocks in the merged file (one per partition), got %d", partitions, len(metadata.DataBlocks))
	}

	// The output's row data is contiguous from 0 and its filter sections are
	// contiguous inside the recorded region.
	rowDataEnd := 0
	for i := range metadata.DataBlocks {
		rowDataEnd += metadata.DataBlocks[i].RowDataSize
	}
	if metadata.BlockFilterRegionOffset != rowDataEnd {
		t.Fatalf("merged region starts at %d, want %d (right after %d bytes of row data)",
			metadata.BlockFilterRegionOffset, rowDataEnd, rowDataEnd)
	}
	regionEnd := metadata.BlockFilterRegionOffset + metadata.BlockFilterRegionSize
	sectionBytes := 0
	for i, block := range metadata.DataBlocks {
		if block.BloomFilterOffset < metadata.BlockFilterRegionOffset || block.BloomFilterOffset+block.BloomFilterSize > regionEnd {
			t.Fatalf("merged block %d filter section [%d,%d) is outside the region [%d,%d)",
				i, block.BloomFilterOffset, block.BloomFilterOffset+block.BloomFilterSize,
				metadata.BlockFilterRegionOffset, regionEnd)
		}
		sectionBytes += block.BloomFilterSize
	}
	if sectionBytes != metadata.BlockFilterRegionSize {
		t.Fatalf("merged blocks' sections total %d bytes but the region is %d", sectionBytes, metadata.BlockFilterRegionSize)
	}

	// Every block's rebuilt or copied section decodes and holds its rows.
	file, err := store.OpenFile(ctx, merged.PointerBytes)
	if err != nil {
		t.Fatalf("failed to open merged file: %v", err)
	}
	defer file.Close()
	for i, block := range metadata.DataBlocks {
		filters, err := ReadDataBlockBloomFilters(file, block)
		if err != nil {
			t.Fatalf("merged block %d (partition %q) filters unreadable: %v", i, block.PartitionID, err)
		}
		if !filters.FieldTokenBloomFilter.TestString(makeFieldTokenKey("part", block.PartitionID)) {
			t.Fatalf("merged block %d lost its partition field:token entry", i)
		}
	}

	// Bloom-conditioned queries still find every row exactly once.
	for _, id := range wantIDs {
		if ids := rowIDs(collectQueryRows(t, engine, NewQuery().Token(id).Build())); ids[id] != 1 {
			t.Fatalf("row %s not found exactly once after merge: %v", id, ids)
		}
	}
}

// TestMergeAbortsOnCorruptSourceBlock corrupts a source block before a merge
// — the verbatim-copy path (row data and filter section) and the rebuild path —
// and asserts the merge aborts cleanly: both source files stay intact and
// referenced, and no output artifacts are left behind. The copy path verifies
// the source's filter section before its bytes reach the output's filter
// region, so a corrupt section can never be copied through.
func TestMergeAbortsOnCorruptSourceBlock(t *testing.T) {
	cases := []struct {
		name string
		// partition of the block to corrupt: "q" is alone in its partition
		// (copied verbatim), "p" exists in both files (rebuilt).
		partition string
		// corrupt inside the filter section instead of the row data
		filterSection bool
		// ids that must remain queryable from the untouched blocks
		intactIDs []string
	}{
		{"copied block row data", "q", false, []string{"p1", "p2"}},
		{"copied block filter section", "q", true, []string{"p1", "p2"}},
		{"rebuilt block row data", "p", false, []string{"p1", "q1"}},
	}

	partitionFunc := func(row map[string]any) string {
		part, _ := row["part"].(string)
		return part
	}
	configure := func(config *BloomSearchEngineConfig) {
		config.PartitionFunc = partitionFunc
		config.RowDataCompression = CompressionNone
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seed, store := newFileSystemStoreEngine(t, dir, configure)
			// file1: p+q blocks; file2: p block only. The p blocks merge, the
			// q block is copied verbatim.
			ingestAndFlush(t, seed, []map[string]any{
				{"id": "p1", "part": "p"},
				{"id": "q1", "part": "q"},
			})
			ingestAndFlush(t, seed, []map[string]any{{"id": "p2", "part": "p"}})
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			seed.Stop(stopCtx)
			cancel()

			ctx := context.Background()
			maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
			if err != nil || len(maybeFiles) != 2 {
				t.Fatalf("expected 2 source files, got %d (err %v)", len(maybeFiles), err)
			}

			// Locate the target block. For "q" it lives in the two-block
			// file; for "p" corrupt the single-block file's copy.
			var targetPath string
			var targetBlock DataBlockMetadata
			for _, maybeFile := range maybeFiles {
				for _, block := range maybeFile.Metadata.DataBlocks {
					if block.PartitionID != tc.partition {
						continue
					}
					if tc.partition == "p" && len(maybeFile.Metadata.DataBlocks) != 1 {
						continue
					}
					targetPath = string(maybeFile.PointerBytes)
					targetBlock = block
				}
			}
			if targetPath == "" {
				t.Fatalf("target block not found")
			}

			corruptOffset := targetBlock.RowDataOffset // first row data byte
			if tc.filterSection {
				corruptOffset = targetBlock.BloomFilterOffset + 1 // inside the filter section
			}
			raw, err := os.ReadFile(targetPath)
			if err != nil {
				t.Fatalf("failed to read source file: %v", err)
			}
			raw[corruptOffset] ^= 0xFF
			if err := os.WriteFile(targetPath, raw, 0o600); err != nil {
				t.Fatalf("failed to write corrupted file: %v", err)
			}

			listDir := func() (dats, others []string) {
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatalf("failed to list dir: %v", err)
				}
				for _, entry := range entries {
					if filepath.Ext(entry.Name()) == ".dat" {
						dats = append(dats, entry.Name())
					} else {
						others = append(others, entry.Name())
					}
				}
				return dats, others
			}
			datsBefore, _ := listDir()

			engine, _ := newFileSystemStoreEngine(t, dir, configure)
			if _, err := engine.Merge(ctx); err == nil {
				t.Fatalf("expected merge to abort on corrupt source block")
			}

			// Sources intact and still referenced; the aborted output left no
			// artifacts (no published .dat, no .tmp, no reservation).
			datsAfter, others := listDir()
			if !slicesEqualUnordered(datsBefore, datsAfter) {
				t.Fatalf("source files changed across failed merge: before %v, after %v", datsBefore, datsAfter)
			}
			if len(others) != 0 {
				t.Fatalf("failed merge left artifacts behind: %v", others)
			}
			after, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
			if err != nil || len(after) != 2 {
				t.Fatalf("expected 2 source files after failed merge, got %d (err %v)", len(after), err)
			}

			// Rows in untouched blocks stay queryable. Each query is scoped
			// to its row's partition so it never touches the corrupt block.
			for _, id := range tc.intactIDs {
				query := NewQuery().Token(id).
					MatchPrefilter(Partition(PartitionEquals(id[:1]))).
					Build()
				if ids := rowIDs(collectQueryRows(t, engine, query)); ids[id] != 1 {
					t.Fatalf("row %s not queryable after failed merge: %v", id, ids)
				}
			}
		})
	}
}

func slicesEqualUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
}
