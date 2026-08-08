package bloomsearch

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

// --- 5A: measured filter sizing ---

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

// --- 5B: v1 compatibility and v2 round trip ---

// v1FileMetadataJSON and v1DataBlockMetadataJSON replicate the metadata shape
// the v1 (pre-Phase-5) writer produced, bloom filters embedded and all.
type v1FileMetadataJSON struct {
	BloomFilters           BloomFilters
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64
	DataBlocks             []v1DataBlockMetadataJSON
}

type v1DataBlockMetadataJSON struct {
	Offset                 int
	Size                   int
	Rows                   int
	BloomFiltersSize       int
	MinMaxIndexes          map[string]MinMaxIndex `json:",omitempty"`
	PartitionID            string                 `json:",omitempty"`
	Compression            CompressionType        `json:",omitempty"`
	UncompressedSize       int                    `json:",omitempty"`
	RowDataHash            uint32                 `json:",omitempty"`
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64
}

// v1TestBlock is one data block of a hand-rolled v1 file.
type v1TestBlock struct {
	partitionID string
	rows        []map[string]any
}

// writeV1TestFile hand-rolls a v1-format file exactly as the pre-Phase-5
// writer did: uncompressed data blocks of length-prefixed JSON rows behind
// JSON+CRC bloom filter sections, and a footer whose metadata JSON embeds
// the file-level filters. includeRowHash=false replicates v1's 0-sentinel
// "no hash" blocks.
func writeV1TestFile(t *testing.T, path string, blocks []v1TestBlock, includeRowHash bool) {
	t.Helper()

	const (
		expectedItems = uint(1000)
		fpr           = 0.01
	)

	newFilters := func() BloomFilters {
		return BloomFilters{
			FieldBloomFilter:      bloom.NewWithEstimates(expectedItems, fpr),
			TokenBloomFilter:      bloom.NewWithEstimates(expectedItems, fpr),
			FieldTokenBloomFilter: bloom.NewWithEstimates(expectedItems, fpr),
		}
	}
	fileFilters := newFilters()

	var out bytes.Buffer
	var crcBytes [HashSize]byte
	var blockMetadatas []v1DataBlockMetadataJSON

	for _, block := range blocks {
		blockFilters := newFilters()

		var rowData bytes.Buffer
		for _, row := range block.rows {
			rowBytes, err := json.Marshal(row)
			if err != nil {
				t.Fatalf("failed to marshal row: %v", err)
			}
			entries := newBloomEntrySets()
			entries.indexRow(rowBytes, BasicWhitespaceLowerTokenizer)
			for _, filters := range []BloomFilters{blockFilters, fileFilters} {
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

			var lengthBytes [LengthPrefixSize]byte
			binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(rowBytes)))
			rowData.Write(lengthBytes[:])
			rowData.Write(rowBytes)
		}

		// v1 block filter section: JSON + CRC32C.
		blockFilterJSON, err := json.Marshal(&blockFilters)
		if err != nil {
			t.Fatalf("failed to marshal block filters: %v", err)
		}
		offset := out.Len()
		out.Write(blockFilterJSON)
		binary.LittleEndian.PutUint32(crcBytes[:], crc32.Checksum(blockFilterJSON, crc32cTable))
		out.Write(crcBytes[:])
		bloomFiltersSize := len(blockFilterJSON) + HashSize
		out.Write(rowData.Bytes())

		rowDataHash := uint32(0)
		if includeRowHash {
			rowDataHash = crc32.Checksum(rowData.Bytes(), crc32cTable)
		}

		blockMetadatas = append(blockMetadatas, v1DataBlockMetadataJSON{
			Offset:                 offset,
			Size:                   bloomFiltersSize + rowData.Len(),
			Rows:                   len(block.rows),
			BloomFiltersSize:       bloomFiltersSize,
			PartitionID:            block.partitionID,
			Compression:            CompressionNone,
			UncompressedSize:       rowData.Len(),
			RowDataHash:            rowDataHash,
			BloomExpectedItems:     expectedItems,
			BloomFalsePositiveRate: fpr,
		})
	}

	metadata := v1FileMetadataJSON{
		BloomFilters:           fileFilters,
		BloomExpectedItems:     expectedItems,
		BloomFalsePositiveRate: fpr,
		DataBlocks:             blockMetadatas,
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("failed to marshal v1 metadata: %v", err)
	}
	out.Write(metadataJSON)
	binary.LittleEndian.PutUint32(crcBytes[:], crc32.Checksum(metadataJSON, crc32cTable))
	out.Write(crcBytes[:])

	var lengthBytes [LengthPrefixSize]byte
	binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(metadataJSON)))
	out.Write(lengthBytes[:])

	var versionBytes [VersionPrefixSize]byte
	binary.LittleEndian.PutUint32(versionBytes[:], FileVersionV1)
	out.Write(versionBytes[:])
	out.WriteString(MagicBytes)

	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatalf("failed to write v1 file: %v", err)
	}
}

// TestV1FilesRemainReadable proves v1 files stay fully queryable by the v2
// engine and mergeable with v2 files (through the rebuild path).
func TestV1FilesRemainReadable(t *testing.T) {
	dir := t.TempDir()
	writeV1TestFile(t, filepath.Join(dir, "v1file.dat"), []v1TestBlock{{rows: []map[string]any{
		{"id": "v1row1", "service": "auth"},
		{"id": "v1row2", "service": "payment"},
	}}}, true)

	engine, store := newFileSystemStoreEngine(t, dir, nil)
	ctx := context.Background()

	// Queries against the v1 file, through every filter level.
	if ids := rowIDs(collectQueryRows(t, engine, nil)); ids["v1row1"] != 1 || ids["v1row2"] != 1 {
		t.Fatalf("match-all over v1 file: got %v", ids)
	}
	if ids := rowIDs(collectQueryRows(t, engine, NewQuery().Token("v1row1").Build())); ids["v1row1"] != 1 || len(ids) != 1 {
		t.Fatalf("token query over v1 file: got %v", ids)
	}
	if ids := rowIDs(collectQueryRows(t, engine, NewQuery().FieldToken("service", "auth").Build())); ids["v1row1"] != 1 || len(ids) != 1 {
		t.Fatalf("fieldtoken query over v1 file: got %v", ids)
	}
	if rows := collectQueryRows(t, engine, NewQuery().Token("zzzabsenttoken").Build()); len(rows) != 0 {
		t.Fatalf("miss query over v1 file returned %d rows", len(rows))
	}

	// Write a v2 file alongside and merge: the v1 blocks go through the
	// rebuild path (their rows re-streamed, filters rebuilt right-sized).
	ingestAndFlush(t, engine, []map[string]any{
		{"id": "v2row1", "service": "search"},
		{"id": "v2row2", "service": "billing"},
	})
	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("merge of v1+v2 files failed: %v", err)
	}

	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		t.Fatalf("failed to list files: %v", err)
	}
	if len(maybeFiles) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(maybeFiles))
	}

	ids := rowIDs(collectQueryRows(t, engine, nil))
	for _, id := range []string{"v1row1", "v1row2", "v2row1", "v2row2"} {
		if ids[id] != 1 {
			t.Fatalf("expected row %s exactly once after merge, got %d (%v)", id, ids[id], ids)
		}
	}
	if ids := rowIDs(collectQueryRows(t, engine, NewQuery().Token("v1row2").Build())); ids["v1row2"] != 1 || len(ids) != 1 {
		t.Fatalf("token query over merged file: got %v", ids)
	}
}

// TestV2FormatRoundTrip writes a v2 file, reads its metadata and filters back
// exactly, and then corrupts every section in turn: each corruption must
// produce a clean error — no panic — and a corrupt block must emit zero rows
// (verify-before-emit).
func TestV2FormatRoundTrip(t *testing.T) {
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
	fileSize := int64(len(pristine))
	metadataLength := int64(binary.LittleEndian.Uint32(pristine[fileSize-8-4-4 : fileSize-8-4]))
	metadataOffset := fileSize - 8 - 4 - 4 - int64(HashSize) - metadataLength

	var v2meta fileMetadataV2JSON
	if err := json.Unmarshal(pristine[metadataOffset:metadataOffset+metadataLength], &v2meta); err != nil {
		t.Fatalf("failed to parse v2 metadata JSON: %v", err)
	}
	fileFilterOffset := metadataOffset - int64(v2meta.FileFilterSectionSize)

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

	t.Run("corrupt block filter section", func(t *testing.T) {
		corruptDir, _ := corruptAt(t, int64(block.Offset)+1)
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
		corruptDir, _ := corruptAt(t, int64(block.Offset+block.BloomFiltersSize)+int64(block.Size-block.BloomFiltersSize)/2)
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
		corruptDir, _ := corruptAt(t, int64(block.Offset+block.BloomFiltersSize))
		rows, err := queryCorrupt(t, corruptDir, nil)
		if err == nil {
			t.Fatalf("expected block error from corrupt length prefix")
		}
		if len(rows) != 0 {
			t.Fatalf("corrupt block emitted %d rows", len(rows))
		}
	})
}

// TestOversizeRowLengthRejected corrupts a row length prefix in a block that
// carries no row data hash (the v1 fail-open case, where no CRC can catch it
// first): the scanner must reject the oversized length with a clean error
// instead of attempting a giant allocation.
func TestOversizeRowLengthRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v1nohash.dat")
	writeV1TestFile(t, path, []v1TestBlock{{rows: []map[string]any{
		{"id": "r1"},
		{"id": "r2"},
	}}}, false /* includeRowHash: v1 no-hash sentinel */)

	// Locate the row data start from the (valid) metadata and stamp an
	// absurd length into the first row's prefix.
	store := NewFileSystemDataStore(dir)
	metadata, err := store.readFileMetadata(path)
	if err != nil {
		t.Fatalf("failed to read v1 metadata: %v", err)
	}
	block := metadata.DataBlocks[0]
	if block.HasRowDataHash {
		t.Fatalf("test requires a hashless block (v1 sentinel)")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	binary.LittleEndian.PutUint32(raw[block.Offset+block.BloomFiltersSize:], 0xFFFFFF00)
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

// --- 5C: conditional filter reads and file filter release ---

// readRange records one Read against a tracked file handle.
type readRange struct {
	offset int64
	length int
}

// readTrackingStore wraps FileSystemDataStore, recording the byte ranges of
// every Read on handles opened through OpenFile.
type readTrackingStore struct {
	*FileSystemDataStore
	mu    sync.Mutex
	reads []readRange
}

func (s *readTrackingStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	file, err := s.FileSystemDataStore.OpenFile(ctx, filePointerBytes)
	if err != nil {
		return nil, err
	}
	return &readTrackingFile{file: file, store: s}, nil
}

func (s *readTrackingStore) record(offset int64, length int) {
	s.mu.Lock()
	s.reads = append(s.reads, readRange{offset: offset, length: length})
	s.mu.Unlock()
}

func (s *readTrackingStore) snapshot() []readRange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]readRange(nil), s.reads...)
}

type readTrackingFile struct {
	file  io.ReadSeekCloser
	store *readTrackingStore
	pos   int64
}

func (f *readTrackingFile) Read(p []byte) (int, error) {
	n, err := f.file.Read(p)
	if n > 0 {
		f.store.record(f.pos, n)
		f.pos += int64(n)
	}
	return n, err
}

func (f *readTrackingFile) Seek(offset int64, whence int) (int64, error) {
	pos, err := f.file.Seek(offset, whence)
	if err == nil {
		f.pos = pos
	}
	return pos, err
}

func (f *readTrackingFile) Close() error {
	return f.file.Close()
}

// TestNoFilterReadWhenNoBloomConditions asserts that a query without bloom
// conditions never reads a block's filter section: scans jump straight to the
// row data. A bloom-conditioned control query proves the tracker sees filter
// reads when they do happen.
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

	tracking := &readTrackingStore{FileSystemDataStore: fsStore}
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
	blocks := maybeFiles[0].Metadata.DataBlocks

	intersectsFilterSection := func(reads []readRange) bool {
		for _, read := range reads {
			for _, block := range blocks {
				sectionStart := int64(block.Offset)
				sectionEnd := int64(block.Offset + block.BloomFiltersSize)
				if read.offset < sectionEnd && read.offset+int64(read.length) > sectionStart {
					return true
				}
			}
		}
		return false
	}

	// Match-all query: no bloom conditions, no regex.
	rows := collectQueryRows(t, engine, nil)
	if len(rows) != 2 {
		t.Fatalf("match-all returned %d rows, want 2", len(rows))
	}
	if intersectsFilterSection(tracking.snapshot()) {
		t.Fatalf("block filter section was read for a query with no bloom conditions")
	}

	// Control: a bloom-conditioned query must read the filter section.
	tracking.mu.Lock()
	tracking.reads = nil
	tracking.mu.Unlock()
	_ = collectQueryRows(t, engine, NewQuery().Token("one").Build())
	if !intersectsFilterSection(tracking.snapshot()) {
		t.Fatalf("control query with bloom conditions did not read the filter section (tracker broken?)")
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

// --- 5E: merge-time rebuild ---

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

// TestV1FilterSectionInsideV2Container exercises the case that motivates
// content-based filter section dispatch: a v1 block that cannot merge (alone
// in its partition) is raw-copied — v1 JSON filter section and all — into a
// v2 output file, and bloom-conditioned queries against it still work.
func TestV1FilterSectionInsideV2Container(t *testing.T) {
	dir := t.TempDir()
	// v1 file: a "p" block that will merge with the v2 file's "p" block, and
	// a "q" block alone in its partition, which the merge raw-copies.
	writeV1TestFile(t, filepath.Join(dir, "v1file.dat"), []v1TestBlock{
		{partitionID: "p", rows: []map[string]any{{"id": "v1p1", "part": "p"}}},
		{partitionID: "q", rows: []map[string]any{{"id": "v1q1", "part": "q"}}},
	}, true)

	engine, store := newFileSystemStoreEngine(t, dir, func(config *BloomSearchEngineConfig) {
		config.PartitionFunc = func(row map[string]any) string {
			part, _ := row["part"].(string)
			return part
		}
	})
	ingestAndFlush(t, engine, []map[string]any{{"id": "v2p1", "part": "p"}})

	ctx := context.Background()
	if _, err := engine.Merge(ctx); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	maybeFiles, err := collectMaybeFiles(ctx, store.GetMaybeFilesForQuery(ctx, nil))
	if err != nil || len(maybeFiles) != 1 {
		t.Fatalf("expected 1 merged file, got %d (err %v)", len(maybeFiles), err)
	}
	merged := maybeFiles[0]

	var qBlock *DataBlockMetadata
	for i := range merged.Metadata.DataBlocks {
		if merged.Metadata.DataBlocks[i].PartitionID == "q" {
			qBlock = &merged.Metadata.DataBlocks[i]
		}
	}
	if qBlock == nil {
		t.Fatalf("no q block in merged file: %+v", merged.Metadata.DataBlocks)
	}

	// The q block was raw-copied: its filter section inside the v2 container
	// is still v1 JSON (leading '{'), and the shared reader parses it.
	raw, err := os.ReadFile(string(merged.PointerBytes))
	if err != nil {
		t.Fatalf("failed to read merged file: %v", err)
	}
	if raw[qBlock.Offset] != '{' {
		t.Fatalf("expected raw-copied v1 JSON filter section (leading '{'), got %#x", raw[qBlock.Offset])
	}
	file, err := store.OpenFile(ctx, merged.PointerBytes)
	if err != nil {
		t.Fatalf("failed to open merged file: %v", err)
	}
	qFilters, err := ReadDataBlockBloomFilters(file, *qBlock)
	file.Close()
	if err != nil {
		t.Fatalf("failed to parse v1 filter section inside v2 file: %v", err)
	}
	if !qFilters.TokenBloomFilter.TestString("v1q1") {
		t.Fatalf("copied v1 block filter lost its token")
	}

	// End-to-end bloom-conditioned query against the copied block.
	if ids := rowIDs(collectQueryRows(t, engine, NewQuery().Token("v1q1").Build())); ids["v1q1"] != 1 || len(ids) != 1 {
		t.Fatalf("token query against copied v1 block: got %v", ids)
	}
	// The rebuilt p block still finds both its rows.
	for _, id := range []string{"v1p1", "v2p1"} {
		if ids := rowIDs(collectQueryRows(t, engine, NewQuery().Token(id).Build())); ids[id] != 1 {
			t.Fatalf("row %s not found after merge: %v", id, ids)
		}
	}
}

// TestMergeAbortsOnCorruptSourceBlock corrupts a source block before a merge
// — the raw-copy path (row data and filter section) and the rebuild path —
// and asserts the merge aborts cleanly: both source files stay intact and
// referenced, and no output artifacts are left behind.
func TestMergeAbortsOnCorruptSourceBlock(t *testing.T) {
	cases := []struct {
		name string
		// partition of the block to corrupt: "q" is alone in its partition
		// (raw-copied), "p" exists in both files (rebuilt).
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
			// q block is raw-copied.
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

			corruptOffset := targetBlock.Offset + targetBlock.BloomFiltersSize // first row data byte
			if tc.filterSection {
				corruptOffset = targetBlock.Offset + 1 // inside the filter section
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
