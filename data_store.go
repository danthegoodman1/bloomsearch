package bloomsearch

import (
	"context"
	"io"
)

type DataStore interface {
	// CreateFile creates a file for single-pass writing.
	CreateFile(ctx context.Context, filePointerBytes []byte) (*io.WriteCloser, error)

	// OpenFile opens a file for reading.
	OpenFile(ctx context.Context, filePointerBytes []byte) (*io.ReadSeekCloser, error)
}

// TESTING
type NullDataStore struct{}

type NullDataStoreFilePointer struct {
	ID string
}

func (n *NullDataStore) OpenFile(ctx context.Context, filePointerBytes []byte) (*io.ReadSeekCloser, error) {
	return nil, nil
}

func (n *NullDataStore) CreateFile(ctx context.Context, filePointerBytes []byte) (*io.WriteCloser, error) {
	return nil, nil
}

func init() {
	var _ DataStore = &NullDataStore{}
}
