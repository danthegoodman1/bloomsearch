package bloomsearch

import "context"

// MetaStore is a generic interface for a metadata store that can be used to store and retrieve file and data block metadata.
//
// FilePointer is a pointer to a file in the metadata store, depending on the implementation of the MetaStore and DataStore.
type MetaStore interface {
	// GetMaybeFilesForQuery returns pointers to files that may contain rows of interest based on the query conditions.
	// The returned files have already been pre-filtered based on partition IDs and MinMaxIndex conditions,
	// but their bloom filters have not been tested yet.
	//
	// If the query specifies partition ID or MinMax index conditions, but the file does not have them,
	// the file must be included in the result set, as it may have rows of interest.
	//
	// The MaybeFile.Metadata.DataBlocks may choose to be a filtered list instead of the full list of data blocks
	// if the query conditions are able to guarantee that some data blocks will not match the query conditions.
	GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error)

	// Update atomically performes a set of operations on the MetaStore.
	Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error
}

type WriteOperation struct {
	FileMetadata     *FileMetadata
	FilePointerBytes []byte
}

type DeleteOperation struct {
	FilePointerBytes []byte
}

// MaybeFile is a pointer to a file that may contain rows of interest based on pre-filtering conditions (partition IDs, minmax indexes). They have not had their bloom filters tested yet.
type MaybeFile struct {
	// The file pointer is serialized to bytes and passed to the DataStore to open the file for reading.
	PointerBytes []byte
	// The FileMetadata.DataBlocks may choose to be a filtered list instead of the full list of data blocks
	Metadata FileMetadata
	// The size of the file in bytes
	Size int
}

// TESTING

type NullMetaStore struct{}

func (n *NullMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error) {
	return nil, nil
}

func (n *NullMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return nil
}
