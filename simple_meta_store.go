package bloomsearch

import (
	"context"
	"fmt"
)

// SimpleMetaStore is a simple map-based implementation for testing
type SimpleMetaStore struct {
	files map[string]FileMetadata
}

func NewSimpleMetaStore() *SimpleMetaStore {
	return &SimpleMetaStore{
		files: make(map[string]FileMetadata),
	}
}

// Update implements the MetaStore interface
func (s *SimpleMetaStore) Update(ctx context.Context, writeOps []WriteOperation, deleteOps []DeleteOperation) error {
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

// GetMaybeFilesForQuery implements the MetaStore interface
func (s *SimpleMetaStore) GetMaybeFilesForQuery(ctx context.Context, prefilter *QueryPrefilter) ([]MaybeFile, error) {
	var result []MaybeFile

	for pointer, metadata := range s.files {
		result = append(result, MaybeFile{
			PointerBytes: []byte(pointer),
			Metadata:     metadata,
		})
	}

	return result, nil
}

// PrintFiles prints all files in the metastore for debugging
func (s *SimpleMetaStore) PrintFiles() {
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
