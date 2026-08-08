package bloomsearch

import (
	"context"
	"io"
)

// DataStore is used to read and write the file from storage.
//
// filePointerBytes is the serialized file pointer that is passed to the DataStore to open the file for reading, and stored within the MetaStore.
// For example, for an S3DataStore, this might be a serialized JSON object of the bucket and file key.
type DataStore interface {
	// CreateFile creates a file for single-pass writing, returning the handle
	// for writing and the file pointer bytes. The writer is used from one
	// goroutine at a time. A successful Close must durably publish the file —
	// the engine acknowledges ingested rows and commits pointers to the
	// MetaStore only after Close returns nil.
	//
	// The writer may additionally implement `Abort() error`: when a write
	// fails partway, the engine calls Abort (instead of Close) to discard the
	// partial file without ever publishing it. Writers without Abort are
	// closed and then tombstoned.
	CreateFile(ctx context.Context) (io.WriteCloser, []byte, error)

	// OpenFile opens a file for reading. A handle is used by one goroutine at a
	// time and serves many seeks and reads: the query path reuses handles
	// across a file's filter and row data reads, and opens separate handles for
	// concurrent reads of the same file.
	OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error)

	// TombstoneFile marks a file as no longer referenced by metadata, along
	// with every artifact the pointer's write cycle can leave behind (e.g.
	// temp names or reservations derived from the pointer). The engine calls
	// it for aborted writes, for files whose MetaStore commit failed, and for
	// merged-away source files after the merge commits. Implementations
	// decide when physical garbage collection occurs.
	TombstoneFile(ctx context.Context, filePointerBytes []byte) error
}
