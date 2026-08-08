package bloomsearch

import "context"

// NullMetaStore is a no-op MetaStore for tests that exercise the write path
// without querying: Update discards writes and GetMaybeFilesForQuery returns
// nothing.
type NullMetaStore struct{}

var _ MetaStore = &NullMetaStore{}

func (n *NullMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error) {
	return nil, nil
}

func (n *NullMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return nil
}
