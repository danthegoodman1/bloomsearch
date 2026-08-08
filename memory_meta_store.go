package bloomsearch

import (
	"context"
	"iter"
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
//
// Consumers do slow work between yields, so the candidate set is snapshotted
// under the read lock and yielded after it is released — a concurrent Update
// never waits on a paused iteration. The O(candidates) snapshot is
// deliberate, not an optimization target: the MetaStore contract forbids
// holding locks across yields, and this store keeps everything in memory
// anyway.
func (s *MemoryMetaStore) GetMaybeFilesForQuery(ctx context.Context, prefilter *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		s.mu.RLock()
		snapshot := make([]MaybeFile, 0, len(s.files))
		for pointer, metadata := range s.files {
			// metadata is a copy of the map value, so reassigning its
			// DataBlocks (FilterDataBlocks allocates when it filters) leaves
			// the stored metadata untouched.
			metadata.DataBlocks = FilterDataBlocks(metadata.DataBlocks, prefilter)
			if prefilter != nil && len(metadata.DataBlocks) == 0 {
				continue
			}
			snapshot = append(snapshot, MaybeFile{
				PointerBytes: []byte(pointer),
				Metadata:     metadata,
			})
		}
		s.mu.RUnlock()

		for _, file := range snapshot {
			if !yield(file, nil) {
				return
			}
		}
	}
}
