package bloomsearch

import (
	"context"
	"iter"
)

// NullMetaStore is a no-op MetaStore for tests that exercise the write path
// without querying: Update discards writes and GetMaybeFilesForQuery yields
// nothing.
type NullMetaStore struct{}

var _ MetaStore = &NullMetaStore{}

func (n *NullMetaStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {}
}

func (n *NullMetaStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	return nil
}
