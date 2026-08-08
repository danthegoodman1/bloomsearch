package bloomsearch

import (
	"context"
	"fmt"
	"sync"
)

// MemoryMetaStore is an in-memory map-based MetaStore. It is safe for
// concurrent use: the engine calls Update from flush paths while query
// goroutines call GetMaybeFilesForQuery.
type MemoryMetaStore struct {
	mu    sync.RWMutex
	files map[string]FileMetadata
}

func NewMemoryMetaStore() *MemoryMetaStore {
	return &MemoryMetaStore{
		files: make(map[string]FileMetadata),
	}
}

// Update implements the MetaStore interface
func (s *MemoryMetaStore) Update(ctx context.Context, writeOps []WriteOperation, deleteOps []DeleteOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, op := range writeOps {
		if op.FileMetadata != nil {
			s.files[string(op.FilePointerBytes)] = *op.FileMetadata
		}
	}

	for _, op := range deleteOps {
		delete(s.files, string(op.FilePointerBytes))
	}

	return nil
}

// GetMaybeFilesForQuery implements the MetaStore interface. As a store-side
// optimization (the engine re-applies the prefilter regardless), data blocks
// that cannot match the prefilter are dropped and files left with no matching
// blocks are omitted.
func (s *MemoryMetaStore) GetMaybeFilesForQuery(ctx context.Context, prefilter *QueryPrefilter) ([]MaybeFile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []MaybeFile

	for pointer, metadata := range s.files {
		// metadata is a copy of the map value, so reassigning its DataBlocks
		// (FilterDataBlocks allocates when it filters) leaves the stored
		// metadata untouched.
		metadata.DataBlocks = FilterDataBlocks(metadata.DataBlocks, prefilter)
		if prefilter != nil && len(metadata.DataBlocks) == 0 {
			continue
		}
		result = append(result, MaybeFile{
			PointerBytes: []byte(pointer),
			Metadata:     metadata,
		})
	}

	return result, nil
}

// PrintFiles prints all files in the metastore for debugging
func (s *MemoryMetaStore) PrintFiles() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fmt.Println("Files in metastore:")
	for pointer, metadata := range s.files {
		fmt.Printf("  File: %s\n", pointer)
		fmt.Printf("    DataBlocks: %d\n", len(metadata.DataBlocks))
		for i, block := range metadata.DataBlocks {
			fmt.Printf("      Block %d: Partition=%s, Size=%d, Rows=%d",
				i, block.PartitionID, block.Size, block.Rows)
			if len(block.MinMaxIndexes) > 0 {
				fmt.Printf(", MinMax=%v", block.MinMaxIndexes)
			}
			fmt.Println()
		}
	}
}
