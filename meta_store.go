package bloomsearch

import "context"

// MetaStore is a generic interface for a metadata store that can be used to store and retrieve file and data block metadata.
//
// FilePointer is a pointer to a file in the metadata store, depending on the implementation of the MetaStore and DataStore.
type MetaStore interface {
	// GetMaybeFilesForQuery returns pointers to files that may contain rows of interest based on the query conditions.
	// The returned files' bloom filters have not been tested yet.
	//
	// Store-side prefiltering is an optimization, not a correctness
	// requirement: the engine re-applies the prefilter to every returned
	// file's data blocks (see FilterDataBlocks), so a store may ignore the
	// query entirely and return everything. Stores that can prune cheaply
	// should still do so — dropping files whose data blocks cannot match, and
	// optionally returning MaybeFile.Metadata.DataBlocks as a filtered subset
	// — to avoid shipping metadata the engine will discard.
	//
	// Strict prefilter semantics (enforced by the engine, mirrored by stores
	// that prefilter): if query conditions reference partition ID or MinMax
	// indexes, data blocks missing that metadata are excluded.
	GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error)

	// Update atomically performs a set of operations on the MetaStore. The
	// engine only calls Update after the corresponding DataStore writes have
	// been durably published (writer.Close succeeded), so a committed pointer
	// always references a complete file.
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
}
