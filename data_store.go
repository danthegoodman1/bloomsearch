package bloomsearch

import (
	"context"
	"io"
)

// DataStore is used to read and write the file from storage.
//
// filePointerBytes is the serialized file pointer that is passed to the DataStore to open the file for reading, and stored within the MetaStore.
// For example, for an S3DataStore, this might be a serialzed JSON object of the bucket and file key.
type DataStore interface {
	// CreateFile creates a file for single-pass writing, returning the handle for writing and the file pointer bytes.
	CreateFile(ctx context.Context) (io.WriteCloser, []byte, error)

	// OpenFile opens a file for reading.
	OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error)
}

// TESTING
type NullDataStore struct{}

type NullDataStoreFilePointer struct {
	ID string
}

func (n *NullDataStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	return nil, nil
}

func (n *NullDataStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	return nil, nil, nil
}

func init() {
	var _ DataStore = &NullDataStore{}
}
