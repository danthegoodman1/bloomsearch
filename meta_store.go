package bloomsearch

import "context"

// MetaStore is a generic interface for a metadata store that can be used to store and retrieve file and data block metadata.
//
// FilePointer is a pointer to a file in the metadata store, depending on the implementation of the MetaStore and DataStore.
type MetaStore[FilePointer any] interface {
	// TODO: define query conditions, return pointers to row groups that the DataStore can read
	GetRowGroupsForQuery(ctx context.Context, queryConditions map[string]any) ([]MaybeFile[FilePointer], error)

	// WriteFileMetadata writes the file metadata to the store
	WriteFileMetadata(ctx context.Context, fileMetadata *FileMetadata) error
}

type MaybeFile[FilePointer any] struct {
	Pointer    FilePointer
	Metadata   FileMetadata
	DataBlocks []DataBlockMetadata
}
