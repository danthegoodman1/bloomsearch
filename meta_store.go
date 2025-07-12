package bloomsearch

import "context"

// MetaStore is a generic interface for a metadata store that can be used to store and retrieve file and data block metadata.
//
// FilePointer is a pointer to a file in the metadata store, depending on the implementation of the MetaStore and DataStore.
type MetaStore[FilePointer any] interface {
	// GetMaybeFilesForQuery returns pointers to files that may contain rows of interest based on the query conditions.
	// The returned files have already been pre-filtered based on partition IDs and MinMaxIndex conditions,
	// but their bloom filters have not been tested yet.
	GetMaybeFilesForQuery(ctx context.Context, query *QueryCondition) ([]MaybeFile[FilePointer], error)

	// WriteFileMetadata writes the file metadata to the store.
	//
	// This is called after the file has been written to the DataStore.
	WriteFileMetadata(ctx context.Context, fileMetadata *FileMetadata) error
}

// MaybeFile is a pointer to a file that may contain rows of interest based on pre-filtering conditions (partition IDs, minmax indexes). They have not had their bloom filters tested yet.
type MaybeFile[FilePointer any] struct {
	Pointer  FilePointer
	Metadata FileMetadata
	// DataBlocks that match the query conditions within this file
	MatchingDataBlocks []DataBlockMetadata
}
