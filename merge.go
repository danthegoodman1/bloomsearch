package bloomsearch

// File merging: candidate grouping, block merge/copy, and the commit
// protocol against the MetaStore.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"sort"
	"time"
)

type MergeStats struct {
	FilesProcessed     int64
	RowGroupsProcessed int64
	RowsProcessed      int64
	BytesProcessed     int64
	Duration           time.Duration
	RowsPerSecond      float64
	BytesPerSecond     float64
}

// Merge executes file merging to optimize storage and query performance.
// Merge is single-flight per engine: a call while another Merge is running
// returns ErrMergeInProgress (concurrent merges would duplicate every merged
// row). A return of non-nil stats WITH an error wrapping
// ErrPostCommitCleanup means the merge committed — state is consistent, only
// unreferenced source files linger in the DataStore.
func (b *BloomSearchEngine) Merge(ctx context.Context) (*MergeStats, error) {
	if !b.mergeMu.TryLock() {
		return nil, ErrMergeInProgress
	}
	defer b.mergeMu.Unlock()
	return b.merge(ctx)
}

// merge will evaluate and merge data files to optimize query performance.
func (b *BloomSearchEngine) merge(ctx context.Context) (*MergeStats, error) {
	mergeStartTime := time.Now()

	// Get all files for evaluation. Merge grouping genuinely needs the full
	// candidate view at once, so the iterator is collected; any yielded error
	// — or ctx termination mid-iteration — aborts the merge before anything
	// is written.
	maybeFiles, err := collectMaybeFiles(ctx, b.metaStore.GetMaybeFilesForQuery(ctx, nil))
	if err != nil {
		return nil, err
	}

	b.logger.Debug("merge evaluating files", "files", len(maybeFiles))

	// Convert to merge candidates with file-level statistics
	var mergeCandidates []fileMergeCandidate
	for _, maybeFile := range maybeFiles {
		candidate := fileMergeCandidate{
			filePointer: maybeFile.PointerBytes,
			metadata:    maybeFile.Metadata,
			statistics:  b.calculateFileStatistics(maybeFile.Metadata),
		}
		mergeCandidates = append(mergeCandidates, candidate)
	}

	// Group files into merge groups (this handles sorting internally)
	mergeGroups := b.identifyFileMergeGroups(mergeCandidates)

	// The per-group summary walk exists only to feed the logger, so it is
	// skipped entirely unless Debug is enabled.
	if b.logger.Enabled(ctx, slog.LevelDebug) {
		totalMergeFiles := 0
		for groupIndex, group := range mergeGroups {
			totalSize := 0
			totalRows := 0
			totalBlocks := 0
			allPartitions := make(map[string]bool)

			for _, candidate := range group {
				totalSize += candidate.statistics.totalSize
				totalRows += candidate.statistics.totalRows
				totalBlocks += candidate.statistics.blockCount
				for _, partitionID := range candidate.statistics.partitionIDs {
					allPartitions[partitionID] = true
				}
			}

			partitionList := make([]string, 0, len(allPartitions))
			for partitionID := range allPartitions {
				partitionList = append(partitionList, partitionID)
			}

			b.logger.Debug("merge group identified",
				"group", groupIndex, "files", len(group), "partitions", partitionList,
				"totalSize", totalSize, "totalRows", totalRows, "totalBlocks", totalBlocks)
			totalMergeFiles += len(group)
		}

		b.logger.Debug("merge groups identified",
			"groups", len(mergeGroups), "filesInGroups", totalMergeFiles, "candidateFiles", len(mergeCandidates))
	}

	// Calculate statistics for files that will be merged
	var totalFilesProcessed int64
	var totalRowGroupsProcessed int64
	var totalRowsProcessed int64
	var totalBytesProcessed int64

	for _, group := range mergeGroups {
		for _, candidate := range group {
			totalFilesProcessed++
			totalRowGroupsProcessed += int64(len(candidate.metadata.DataBlocks))
			for _, block := range candidate.metadata.DataBlocks {
				totalRowsProcessed += int64(block.Rows)
				totalBytesProcessed += int64(block.Size)
			}
		}
	}

	// Execute merges for each group
	var writeOps []WriteOperation
	var deleteOps []DeleteOperation

	for groupIndex, group := range mergeGroups {
		b.logger.Debug("merging group", "group", groupIndex, "files", len(group))

		newFilePointer, newFileMetadata, err := b.executeMergeGroup(ctx, group)
		if err != nil {
			// Outputs from groups that already completed were published but
			// never referenced by the MetaStore; tombstone the orphans.
			// Sources are untouched — they are only tombstoned after a
			// successful MetaStore.Update.
			for _, writeOp := range writeOps {
				b.dataStore.TombstoneFile(ctx, writeOp.FilePointerBytes)
			}
			return nil, fmt.Errorf("failed to merge group %d: %w", groupIndex, err)
		}

		// Add new file to write operations
		writeOps = append(writeOps, WriteOperation{
			FileMetadata:     newFileMetadata,
			FilePointerBytes: newFilePointer,
		})

		// Add old files to delete operations
		for _, candidate := range group {
			deleteOps = append(deleteOps, DeleteOperation{
				FilePointerBytes: candidate.filePointer,
			})
		}

		b.logger.Debug("merged group into new file", "group", groupIndex)
	}

	// Update metastore: add new files and remove old ones
	var postCommitCleanupErr error
	if len(writeOps) > 0 {
		b.logger.Debug("merge updating metastore", "newFiles", len(writeOps), "removedFiles", len(deleteOps))
		if err := b.metaStore.Update(ctx, writeOps, deleteOps); err != nil {
			// Nothing committed: the merge outputs were published but never
			// referenced, so tombstone them. Sources stay referenced and
			// untouched.
			for _, writeOp := range writeOps {
				b.dataStore.TombstoneFile(ctx, writeOp.FilePointerBytes)
			}
			return nil, fmt.Errorf("failed to update metastore after merge: %w", err)
		}
		b.logger.Debug("merge metastore update committed")

		// The merge is committed from here on. Tombstone failures are
		// garbage-collection failures, not merge failures: report them via
		// ErrPostCommitCleanup alongside the stats.
		var tombstoneErrs []error
		for _, deleteOp := range deleteOps {
			if err := b.dataStore.TombstoneFile(ctx, deleteOp.FilePointerBytes); err != nil {
				tombstoneErrs = append(tombstoneErrs, err)
			}
		}
		if len(tombstoneErrs) > 0 {
			postCommitCleanupErr = fmt.Errorf("%w: %w", ErrPostCommitCleanup, errors.Join(tombstoneErrs...))
		}
	}

	// Calculate final statistics
	duration := time.Since(mergeStartTime)
	stats := &MergeStats{
		FilesProcessed:     totalFilesProcessed,
		RowGroupsProcessed: totalRowGroupsProcessed,
		RowsProcessed:      totalRowsProcessed,
		BytesProcessed:     totalBytesProcessed,
		Duration:           duration,
	}

	// Calculate rates
	if duration.Seconds() > 0 {
		stats.RowsPerSecond = float64(totalRowsProcessed) / duration.Seconds()
		stats.BytesPerSecond = float64(totalBytesProcessed) / duration.Seconds()
	}

	return stats, postCommitCleanupErr
}

// blockMergeKey returns an injective key over the properties two blocks must
// share to be mergeable: the partition ID and the minmax key set. Blocks with
// different keys never merge — merging a block that lacks a minmax key with
// one that has it would give the keyless block's rows the other block's range,
// widening strict-prefilter visibility to rows whose block never indexed the
// key; such blocks are copied as-is instead. Bloom filter parameters impose no
// constraint: merged blocks rebuild their filters from row data. Components
// are length-prefixed so distinct (partition, key set) tuples cannot collide.
// Two blocks with equal keys are mergeable iff blocksWithinMergeLimits.
func blockMergeKey(block *DataBlockMetadata) string {
	keys := make([]string, 0, len(block.MinMaxIndexes))
	for key := range block.MinMaxIndexes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	buf := make([]byte, 0, len(block.PartitionID)+16)
	buf = binary.AppendUvarint(buf, uint64(len(block.PartitionID)))
	buf = append(buf, block.PartitionID...)
	for _, key := range keys {
		buf = binary.AppendUvarint(buf, uint64(len(key)))
		buf = append(buf, key...)
	}
	return string(buf)
}

// blockMergeShape is the size profile of a block for pairwise merge-limit
// checks within a blockMergeKey bucket.
type blockMergeShape struct {
	rows             int
	uncompressedSize int
}

// blocksWithinMergeLimits reports whether merging two blocks (which must
// already share a blockMergeKey) stays within the row-group limits.
func (b *BloomSearchEngine) blocksWithinMergeLimits(shape1, shape2 blockMergeShape) bool {
	return shape1.rows+shape2.rows <= b.config.MaxRowGroupRows &&
		shape1.uncompressedSize+shape2.uncompressedSize <= b.config.MaxRowGroupBytes
}

// mergeMinMaxIndexes merges minmax indexes from two data blocks
func (b *BloomSearchEngine) mergeMinMaxIndexes(indexes1, indexes2 map[string]MinMaxIndex) map[string]MinMaxIndex {
	merged := make(map[string]MinMaxIndex)

	// Copy all indexes from first block
	for key, index := range indexes1 {
		merged[key] = index
	}

	// Merge indexes from second block
	for key, index2 := range indexes2 {
		if index1, exists := merged[key]; exists {
			// Merge the indexes
			merged[key] = UpdateMinMaxIndex(index1, index2.Min, index2.Max)
		} else {
			// Add new index
			merged[key] = index2
		}
	}

	return merged
}

// fileMergeCandidate represents a file that could be merged
type fileMergeCandidate struct {
	filePointer []byte
	metadata    FileMetadata
	statistics  fileStatistics
}

// fileStatistics contains pre-calculated statistics about a file
type fileStatistics struct {
	partitionIDs []string
	totalSize    int
	totalRows    int
	blockCount   int
}

// calculateFileStatistics computes basic statistics for a file
func (b *BloomSearchEngine) calculateFileStatistics(metadata FileMetadata) fileStatistics {
	stats := fileStatistics{
		partitionIDs: make([]string, 0),
	}

	partitionSet := make(map[string]bool)

	for _, block := range metadata.DataBlocks {
		// Track unique partitions
		if !partitionSet[block.PartitionID] {
			partitionSet[block.PartitionID] = true
			stats.partitionIDs = append(stats.partitionIDs, block.PartitionID)
		}

		// Accumulate totals
		stats.totalSize += block.Size
		stats.totalRows += block.Rows
		stats.blockCount++
	}

	// Sort partition IDs for consistent ordering
	sort.Strings(stats.partitionIDs)

	return stats
}

// identifyFileMergeGroups groups files that should be merged together using
// smart row group merging. Bloom filter parameters play no role in grouping:
// merged blocks rebuild their filters from row data, so any two files with
// mergeable row groups are candidates (with measured filter sizing, files
// essentially never share filter parameters).
//
// Compatibility is indexed by blockMergeKey (partition + minmax key set): a
// candidate joins a group when one of its blocks shares a key with a group
// block and the pair stays within merge limits. Only same-key blocks are ever
// compared, so grouping cost no longer scales with cross-partition block
// pairs.
func (b *BloomSearchEngine) identifyFileMergeGroups(files []fileMergeCandidate) [][]fileMergeCandidate {
	if len(files) < 2 {
		return nil
	}

	// Sort files by potential for merging (smaller files first, then by partition locality)
	candidates := append([]fileMergeCandidate(nil), files...)
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]

		// Primary: Prefer files with smaller average block sizes (more opportunity for merging)
		aAvgBlockSize := a.statistics.totalSize / max(a.statistics.blockCount, 1)
		bAvgBlockSize := b.statistics.totalSize / max(b.statistics.blockCount, 1)

		if aAvgBlockSize != bAvgBlockSize {
			return aAvgBlockSize < bAvgBlockSize
		}

		// Secondary: Sort by total size (smaller first)
		return a.statistics.totalSize < b.statistics.totalSize
	})

	// Precompute every candidate's blocks indexed by merge key.
	candidateBlocks := make([]map[string][]blockMergeShape, len(candidates))
	for i := range candidates {
		blocks := candidates[i].metadata.DataBlocks
		index := make(map[string][]blockMergeShape, len(blocks))
		for j := range blocks {
			key := blockMergeKey(&blocks[j])
			index[key] = append(index[key], blockMergeShape{blocks[j].Rows, blocks[j].UncompressedSize})
		}
		candidateBlocks[i] = index
	}

	var mergeGroups [][]fileMergeCandidate
	totalFilesInGroups := 0

	// Track which files have already been assigned to a group
	fileAssigned := make([]bool, len(candidates))

	// Greedy approach: try to group files that can benefit from row group merging
	for i, file := range candidates {
		if fileAssigned[i] {
			continue
		}

		if totalFilesInGroups >= b.config.MaxFilesToMergePerOperation {
			break
		}

		currentGroup := []fileMergeCandidate{file}
		currentGroupSize := file.statistics.totalSize
		fileAssigned[i] = true

		// The group's blocks, indexed by merge key, growing as files join.
		groupBlocks := make(map[string][]blockMergeShape, len(candidateBlocks[i]))
		for key, shapes := range candidateBlocks[i] {
			groupBlocks[key] = append(groupBlocks[key], shapes...)
		}

		// Add compatible files to this group
		for j := i + 1; j < len(candidates); j++ {
			if fileAssigned[j] {
				continue
			}

			if totalFilesInGroups+len(currentGroup)+1 > b.config.MaxFilesToMergePerOperation {
				break
			}

			candidate := candidates[j]

			newSize := currentGroupSize + candidate.statistics.totalSize
			if newSize > b.config.MaxFileSize {
				continue
			}

			if b.hasMergeableBlockPair(groupBlocks, candidateBlocks[j]) {
				currentGroup = append(currentGroup, candidate)
				currentGroupSize = newSize
				fileAssigned[j] = true
				for key, shapes := range candidateBlocks[j] {
					groupBlocks[key] = append(groupBlocks[key], shapes...)
				}
			}
		}

		if len(currentGroup) > 1 {
			mergeGroups = append(mergeGroups, currentGroup)
			totalFilesInGroups += len(currentGroup)
		}
	}

	return mergeGroups
}

// hasMergeableBlockPair reports whether any candidate block shares a merge key
// with a group block within merge limits.
func (b *BloomSearchEngine) hasMergeableBlockPair(groupBlocks, candBlocks map[string][]blockMergeShape) bool {
	for key, candShapes := range candBlocks {
		groupShapes, ok := groupBlocks[key]
		if !ok {
			continue
		}
		for _, cand := range candShapes {
			for _, group := range groupShapes {
				if b.blocksWithinMergeLimits(cand, group) {
					return true
				}
			}
		}
	}
	return false
}

// executeMergeGroup merges a group of files with smart row group merging.
//
// The output file's filters are rebuilt from measured entries rather than
// OR-merged from the sources: merged blocks contribute the entry sets
// collected while their rows stream through (see mergeDataBlocks), and
// raw-copied blocks — whose bytes are copied verbatim — are re-streamed
// purely for entry collection (see copyDataBlock). Rebuilding keeps the
// merged file's filters right-sized instead of unioning possibly saturated
// or differently-sized source filters.
func (b *BloomSearchEngine) executeMergeGroup(ctx context.Context, group []fileMergeCandidate) ([]byte, *FileMetadata, error) {
	// Create new file for writing
	writer, filePointerBytes, err := b.dataStore.CreateFile(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create merge file: %w", err)
	}

	// fail aborts the partial output (never publishing it) and tombstones its
	// pointer; the sources are untouched because they are only tombstoned
	// after a successful MetaStore.Update in merge().
	fail := func(err error, closeAttempted bool) ([]byte, *FileMetadata, error) {
		b.abortFileWriter(ctx, writer, filePointerBytes, closeAttempted)
		return nil, nil, err
	}

	var newDataBlocks []DataBlockMetadata
	currentOffset := 0

	// File-level entries accumulate across every block of the output file.
	fileEntries := newBloomEntrySets()

	// Collect all data blocks from all files with their file pointers. Block
	// reads open their own handle per block, so same-file blocks never
	// interleave reads on one seek position.
	var allBlocks []blockWithFile
	for _, candidate := range group {
		for _, blockMetadata := range candidate.metadata.DataBlocks {
			allBlocks = append(allBlocks, blockWithFile{
				block:       blockMetadata,
				filePointer: candidate.filePointer,
			})
		}
	}

	// Group blocks by partition for potential merging
	partitionBlocks := make(map[string][]int) // partition -> indices into allBlocks
	for i, block := range allBlocks {
		partitionBlocks[block.block.PartitionID] = append(partitionBlocks[block.block.PartitionID], i)
	}

	// Process each partition
	for partitionID, blockIndices := range partitionBlocks {
		err := b.processPartitionBlocks(ctx, writer, allBlocks, blockIndices, partitionID, &currentOffset, &newDataBlocks, fileEntries)
		if err != nil {
			return fail(fmt.Errorf("failed to process partition %s: %w", partitionID, err), false)
		}
	}

	// The metadata describes what was actually built: filters sized from the
	// measured entry counts at the engine's configured false positive rate.
	newFileMetadata := &FileMetadata{
		BloomFilters:           fileEntries.buildFilters(b.config.BloomFalsePositiveRate),
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
		BloomEntryCounts:       fileEntries.counts(),
		DataBlocks:             newDataBlocks,
	}

	// Write file metadata and footer
	if err := WriteFileFooter(writer, newFileMetadata); err != nil {
		return fail(fmt.Errorf("failed to write file metadata: %w", err), false)
	}

	// Close is the publish step (and for rename-on-close stores the only
	// point the file becomes visible): it must succeed before merge() may
	// commit the pointer to the MetaStore, or a failed finalize would delete
	// sole copies of the source data.
	if err := writer.Close(); err != nil {
		return fail(fmt.Errorf("failed to close merge file writer: %w", err), true)
	}

	return filePointerBytes, newFileMetadata, nil
}

// blockWithFile represents a data block with the pointer of its source file
type blockWithFile struct {
	block       DataBlockMetadata
	filePointer []byte
}

// processPartitionBlocks handles merging data blocks for a single partition
func (b *BloomSearchEngine) processPartitionBlocks(ctx context.Context, writer io.Writer, allBlocks []blockWithFile, blockIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata, fileEntries *bloomEntrySets) error {
	// Bucket blocks by merge key (within one partition, that is the minmax
	// key set); blocks in different buckets can never merge, so grouping only
	// compares same-key blocks.
	buckets := make(map[string][]int)
	var bucketOrder []string
	for _, blockIdx := range blockIndices {
		key := blockMergeKey(&allBlocks[blockIdx].block)
		if _, ok := buckets[key]; !ok {
			bucketOrder = append(bucketOrder, key)
		}
		buckets[key] = append(buckets[key], blockIdx)
	}

	// Greedy grouping within each bucket: a seed block collects the following
	// blocks that pair with it and cumulatively fit within row-group limits.
	var mergeGroups [][]int // groups of block indices that can be merged
	for _, key := range bucketOrder {
		bucket := buckets[key]
		used := make([]bool, len(bucket))
		for s := range bucket {
			if used[s] {
				continue
			}
			used[s] = true
			seed := &allBlocks[bucket[s]].block
			seedShape := blockMergeShape{seed.Rows, seed.UncompressedSize}
			currentGroup := []int{bucket[s]}
			currentRows := seed.Rows
			currentSize := seed.UncompressedSize

			for o := s + 1; o < len(bucket); o++ {
				if used[o] {
					continue
				}
				otherBlock := &allBlocks[bucket[o]].block
				if !b.blocksWithinMergeLimits(seedShape, blockMergeShape{otherBlock.Rows, otherBlock.UncompressedSize}) {
					continue
				}
				// Check if adding this block would exceed limits
				if currentRows+otherBlock.Rows <= b.config.MaxRowGroupRows &&
					currentSize+otherBlock.UncompressedSize <= b.config.MaxRowGroupBytes {
					currentGroup = append(currentGroup, bucket[o])
					currentRows += otherBlock.Rows
					currentSize += otherBlock.UncompressedSize
					used[o] = true
				}
			}

			mergeGroups = append(mergeGroups, currentGroup)
		}
	}

	// Process each merge group
	for _, group := range mergeGroups {
		if len(group) == 1 {
			// Single block, just copy it
			blockIdx := group[0]
			err := b.copyDataBlock(ctx, writer, allBlocks[blockIdx], currentOffset, newDataBlocks, fileEntries)
			if err != nil {
				return fmt.Errorf("failed to copy data block: %w", err)
			}
		} else {
			// Multiple blocks, merge them
			err := b.mergeDataBlocks(ctx, writer, allBlocks, group, partitionID, currentOffset, newDataBlocks, fileEntries)
			if err != nil {
				return fmt.Errorf("failed to merge data blocks: %w", err)
			}
		}
	}

	return nil
}

// copyDataBlock copies a single data block — filter section and row data —
// to the output file verbatim, then re-streams the block's rows purely for
// entry collection so the merged file's rebuilt file-level filters cover the
// copied rows too. Both the filter section (its CRC, via parseFilterSection)
// and the row data (see decodeBlockRowData) are verified before anything is
// written: a corrupt source block fails the merge instead of propagating the
// corruption into the output file.
func (b *BloomSearchEngine) copyDataBlock(ctx context.Context, writer io.Writer, bwf blockWithFile, currentOffset *int, newDataBlocks *[]DataBlockMetadata, fileEntries *bloomEntrySets) error {
	block := bwf.block
	if block.BloomFiltersSize < 0 || block.BloomFiltersSize > block.Size {
		return fmt.Errorf("invalid bloom filter section size %d (block size %d)", block.BloomFiltersSize, block.Size)
	}

	file, err := b.dataStore.OpenFile(ctx, bwf.filePointer)
	if err != nil {
		return fmt.Errorf("failed to open source file for block copy: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(int64(block.Offset), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to source block: %w", err)
	}
	raw := make([]byte, block.Size)
	if _, err := io.ReadFull(file, raw); err != nil {
		return fmt.Errorf("failed to read block data: %w", err)
	}

	if block.BloomFiltersSize > 0 {
		if _, err := parseFilterSection(raw[:block.BloomFiltersSize]); err != nil {
			return fmt.Errorf("failed to verify copied block filter section: %w", err)
		}
	}

	rowData, err := decodeBlockRowData(raw[block.BloomFiltersSize:], &block)
	if err != nil {
		return fmt.Errorf("failed to verify copied block row data: %w", err)
	}
	scanner := NewBlockRowScanner(rowData)
	for {
		rowBytes, ok, err := scanner.Next()
		if err != nil {
			return fmt.Errorf("error reading from data block: %w", err)
		}
		if !ok {
			break
		}
		fileEntries.indexRow(rowBytes, b.config.Tokenizer)
	}

	if _, err := writer.Write(raw); err != nil {
		return fmt.Errorf("failed to copy block data: %w", err)
	}

	// Create new block metadata with updated offset (everything else stays the same)
	newBlockMetadata := block // copy the struct
	newBlockMetadata.Offset = *currentOffset

	*newDataBlocks = append(*newDataBlocks, newBlockMetadata)
	*currentOffset += newBlockMetadata.Size

	return nil
}

// mergeDataBlocks merges multiple data blocks into a single data block,
// rebuilding the block's bloom filters from its rows: every row is fed
// through the shared walker/tokenizer into fresh entry sets, and exact-sized
// filters are built once the merged block is complete. Because the filter
// section precedes the row data on disk but is only known after every row has
// streamed through, the compressed row data is buffered in memory (bounded by
// the row-group size budget) until the block completes.
//
// Blocks are loaded one at a time, each through its own file handle, so
// blocks from the same source file never interleave reads on a shared seek
// position.
func (b *BloomSearchEngine) mergeDataBlocks(ctx context.Context, writer io.Writer, allBlocks []blockWithFile, groupIndices []int, partitionID string, currentOffset *int, newDataBlocks *[]DataBlockMetadata, fileEntries *bloomEntrySets) error {
	blockEntries := newBloomEntrySets()
	var mergedMinMaxIndexes map[string]MinMaxIndex

	var compressed bytes.Buffer
	rowDataHasher := crc32.New(crc32cTable)
	compressionEncoders, err := b.createCompressionWriter(io.MultiWriter(&compressed, rowDataHasher))
	if err != nil {
		return fmt.Errorf("failed to create compression writer: %w", err)
	}
	rowDataWriter := compressionEncoders.writer

	uncompressedSize := 0
	rowCount := 0
	var lengthBytes [LengthPrefixSize]byte

	for i, blockIdx := range groupIndices {
		bwf := allBlocks[blockIdx]

		if i == 0 {
			mergedMinMaxIndexes = bwf.block.MinMaxIndexes
		} else {
			mergedMinMaxIndexes = b.mergeMinMaxIndexes(mergedMinMaxIndexes, bwf.block.MinMaxIndexes)
		}

		rowData, err := b.loadBlockRowData(ctx, bwf.filePointer, bwf.block)
		if err != nil {
			return fmt.Errorf("failed to read data block rows: %w", err)
		}

		scanner := NewBlockRowScanner(rowData)
		for {
			rowBytes, ok, scanErr := scanner.Next()
			if scanErr != nil {
				return fmt.Errorf("error reading from data block: %w", scanErr)
			}
			if !ok {
				break
			}

			blockEntries.indexRow(rowBytes, b.config.Tokenizer)

			binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(rowBytes)))
			if _, err := rowDataWriter.Write(lengthBytes[:]); err != nil {
				return fmt.Errorf("failed to write row length: %w", err)
			}
			if _, err := rowDataWriter.Write(rowBytes); err != nil {
				return fmt.Errorf("failed to write row data: %w", err)
			}

			uncompressedSize += len(rowBytes) + LengthPrefixSize
			rowCount++
		}
	}

	// Finalize compression and recycle the encoders (the compressed bytes
	// live in the buffer).
	if err := compressionEncoders.finalizeCompression(); err != nil {
		return fmt.Errorf("failed to finalize compression: %w", err)
	}
	compressionEncoders.release()

	// Every row has streamed through: build the block's exact-sized filters
	// and write the block as [filter section][compressed row data].
	blockFilters := blockEntries.buildFilters(b.config.BloomFalsePositiveRate)
	filterSection, err := encodeFilterSection(&blockFilters)
	if err != nil {
		return fmt.Errorf("failed to encode bloom filters: %w", err)
	}
	if _, err := writer.Write(filterSection); err != nil {
		return fmt.Errorf("failed to write bloom filters: %w", err)
	}
	if _, err := writer.Write(compressed.Bytes()); err != nil {
		return fmt.Errorf("failed to write merged row data: %w", err)
	}

	blockEntries.unionInto(fileEntries)

	totalSize := len(filterSection) + compressed.Len()
	*newDataBlocks = append(*newDataBlocks, DataBlockMetadata{
		PartitionID:      partitionID,
		Rows:             rowCount,
		Offset:           *currentOffset,
		Size:             totalSize,
		BloomFiltersSize: len(filterSection),
		MinMaxIndexes:    mergedMinMaxIndexes,
		// Compression and bloom params both reflect what was just built with
		// the current config.
		Compression:            b.config.RowDataCompression,
		UncompressedSize:       uncompressedSize,
		RowDataHash:            rowDataHasher.Sum32(),
		HasRowDataHash:         true,
		BloomEntryCounts:       blockEntries.counts(),
		BloomFalsePositiveRate: b.config.BloomFalsePositiveRate,
	})
	*currentOffset += totalSize

	return nil
}

// loadBlockRowData opens a dedicated handle on the block's source file and
// returns the block's verified, decompressed row data (see
// ReadDataBlockRowData for the verification order and bounds). The handle is
// closed before returning — callers work from the in-memory copy.
func (b *BloomSearchEngine) loadBlockRowData(ctx context.Context, filePointer []byte, block DataBlockMetadata) ([]byte, error) {
	file, err := b.dataStore.OpenFile(ctx, filePointer)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file for row reader: %w", err)
	}
	defer file.Close()
	return ReadDataBlockRowData(file, &block)
}
