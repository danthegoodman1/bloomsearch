package bloomsearch

import "context"

type MetaStore interface {
	// TODO: define query conditions, return pointers to row groups that the DataStore can read
	GetRowGroupsForQuery(ctx context.Context, queryConditions map[string]any) ([]*DataBlockMetadata, error)

	// WriteFileMetadata writes the file metadata to the store
	WriteFileMetadata(ctx context.Context, fileMetadata *FileMetadata) error
}
