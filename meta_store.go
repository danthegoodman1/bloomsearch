package bloomsearch

import (
	"context"
	"iter"
)

// MetaStore is a generic interface for a metadata store that can be used to store and retrieve file and data block metadata.
//
// FilePointer is a pointer to a file in the metadata store, depending on the implementation of the MetaStore and DataStore.
type MetaStore interface {
	// GetMaybeFilesForQuery returns an iterator over pointers to files that
	// may contain rows of interest based on the query conditions. The yielded
	// files' bloom filters have not been tested yet.
	//
	// Iterator contract:
	//
	//   - Errors flow through the yielded error value: a store that fails
	//     yields one (MaybeFile{}, err) and returns. The engine stops pulling
	//     at the first non-nil error and surfaces it from Results.Err.
	//   - The consumer may stop early (exit its range loop), so the store must
	//     release resources — locks, cursors, connections — via defers inside
	//     the iterator closure; they run whether iteration completes or stops.
	//   - Yields block for as long as the consumer takes to process the file
	//     (the engine applies backpressure), so the store must not hold
	//     exclusive locks across yields: snapshot under a read lock or page
	//     through stable views, then yield.
	//   - Blocking waits inside the iterator must honor ctx: an
	//     early-terminated query (Results.Close, context cancellation) cancels
	//     ctx and waits for the iterator to return before the cursor's
	//     terminal state is frozen.
	//   - The engine retains yielded data past the yield's return:
	//     PointerBytes, DataBlocks, and their MinMaxIndexes maps live for the
	//     rest of the query (file pointers surface in BlockStats from the
	//     cursor's Stats). Yielding transfers ownership — stores must not
	//     reuse or mutate those buffers, slices, or maps after yielding, so
	//     no scratch-buffer reuse across yields.
	//
	// Store-side prefiltering is an optimization, not a correctness
	// requirement: the engine re-applies the prefilter to every yielded
	// file's data blocks (see FilterDataBlocks), so a store may ignore the
	// query entirely and yield everything. Stores that can prune cheaply
	// should still do so — dropping files whose data blocks cannot match, and
	// optionally yielding MaybeFile.Metadata.DataBlocks as a filtered subset
	// — to avoid shipping metadata the engine will discard.
	//
	// Strict prefilter semantics (enforced by the engine, mirrored by stores
	// that prefilter): if query conditions reference partition ID or MinMax
	// indexes, data blocks missing that metadata are excluded.
	GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error]

	// Update atomically performs a set of operations on the MetaStore. The
	// engine only calls Update after the corresponding DataStore writes have
	// been durably published (writer.Close succeeded), so a committed pointer
	// always references a complete file.
	Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error
}

// collectMaybeFiles drains a GetMaybeFilesForQuery iterator into a slice,
// aborting with the first yielded error. Under the iterator contract a
// returning iterator means either the candidates are exhausted or ctx
// terminated the iteration — indistinguishable from the iterator alone — so
// collecting consumers must consult ctx to tell them apart: a canceled
// collection reports ctx.Err() rather than posing as an empty store. The
// helper exists for consumers that need the full candidate view at once
// (merge grouping does); the query path streams from the iterator instead.
func collectMaybeFiles(ctx context.Context, seq iter.Seq2[MaybeFile, error]) ([]MaybeFile, error) {
	var files []MaybeFile
	for file, err := range seq {
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return files, nil
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
