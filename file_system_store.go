package bloomsearch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
)

type FileSystemDataStore struct {
	rootDir string

	// drawFileName returns a candidate base name (without extension) for
	// CreateFile; overridden in tests to force reservation collisions.
	drawFileName func() string
}

func NewFileSystemDataStore(rootDir string) *FileSystemDataStore {
	// Make dir if not exists
	if _, err := os.Stat(rootDir); os.IsNotExist(err) {
		os.MkdirAll(rootDir, 0755)
	}

	return &FileSystemDataStore{
		rootDir:      rootDir,
		drawFileName: defaultFileNameDraw,
	}
}

func defaultFileNameDraw() string {
	return fmt.Sprintf("bloom-%d", rand.Uint64())
}

func (fs *FileSystemDataStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	filePath := string(filePointerBytes)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// maxCreateFileAttempts bounds CreateFile's name-draw retry loop, matching
// os.CreateTemp's collision retry limit.
const maxCreateFileAttempts = 10000

// CreateFile reserves a final ".dat" path and writes to a sibling ".tmp" name
// that directory scans ignore; the returned writer's Close syncs the file and
// renames it over the reservation, so in-progress writes are never visible to
// GetMaybeFilesForQuery. The returned file pointer is the final ".dat" path —
// that is what callers store in the MetaStore. The writer also implements
// Abort, which discards the write: it removes the ".tmp" file and the paired
// 0-byte reservation. If Close fails and nothing aborts, the ".tmp" file and
// the empty reservation are left in place, both invisible to queries;
// TombstoneFile on the pointer removes them.
//
// The final path is claimed with an exclusive 0-byte create before the ".tmp"
// is opened: os.Rename replaces its destination, so without the reservation a
// colliding name draw would let Close silently overwrite a committed file.
// Scans skip the reservation because it is too small to be a valid bloom
// file.
func (fs *FileSystemDataStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	draw := fs.drawFileName
	if draw == nil {
		draw = defaultFileNameDraw
	}

	for attempt := 0; attempt < maxCreateFileAttempts; attempt++ {
		base := draw()
		finalPath := filepath.Join(fs.rootDir, base+".dat")
		tempPath := filepath.Join(fs.rootDir, base+".tmp")

		reservation, err := os.OpenFile(finalPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			if os.IsExist(err) {
				// Name taken by a committed or in-progress file; redraw.
				continue
			}
			return nil, nil, err
		}
		if err := reservation.Close(); err != nil {
			os.Remove(finalPath)
			return nil, nil, err
		}

		file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			// Release the reservation: this attempt owns no ".tmp" to ever
			// rename over it.
			os.Remove(finalPath)
			if os.IsExist(err) {
				// Orphaned ".tmp" from an aborted write; redraw.
				continue
			}
			return nil, nil, err
		}

		writer := &renameOnCloseFile{
			file:      file,
			tempPath:  tempPath,
			finalPath: finalPath,
		}
		return writer, []byte(finalPath), nil
	}

	return nil, nil, fmt.Errorf("failed to draw an unused file name in %s after %d attempts", fs.rootDir, maxCreateFileAttempts)
}

// renameOnCloseFile publishes a written file at its final path only on a
// successful Close (sync, close, rename, directory fsync — in that order),
// replacing the 0-byte reservation CreateFile placed at finalPath. Abort
// discards the write instead: it closes the handle and removes both the
// ".tmp" file and the reservation, so nothing is ever published.
//
// A Close failure before the rename leaves the ".tmp" file and the
// reservation in place, both invisible to queries. A failure of the
// directory fsync happens after the rename: a complete data file sits at
// finalPath, transiently visible to directory scans but never referenced by
// a metastore (Close reported failure, so no pointer is committed), until
// Abort or TombstoneFile removes it.
//
// The writer is single-goroutine, matching the DataStore writer contract.
type renameOnCloseFile struct {
	file      *os.File
	tempPath  string
	finalPath string
	published bool
}

func (f *renameOnCloseFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f *renameOnCloseFile) Close() error {
	if err := f.file.Sync(); err != nil {
		f.file.Close()
		return err
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.tempPath, f.finalPath); err != nil {
		return err
	}
	// fsync the directory so the rename itself survives power loss: once an
	// external metastore commits the pointer, the publish must be durable.
	if err := syncDir(filepath.Dir(f.finalPath)); err != nil {
		return err
	}
	f.published = true
	return nil
}

// Abort discards the write without ever publishing it: the ".tmp" file and
// the 0-byte reservation at the final path are both removed. Abort after a
// successful Close is a no-op — the published file is only removed by
// TombstoneFile.
func (f *renameOnCloseFile) Abort() error {
	if f.published {
		return nil
	}
	// The handle may already be closed by a failed Close; that error carries
	// no information here.
	f.file.Close()
	var errs []error
	if err := os.Remove(f.tempPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if err := os.Remove(f.finalPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// syncDir fsyncs a directory so metadata operations in it (renames, removes)
// are durable.
func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := handle.Sync()
	closeErr := handle.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// TombstoneFile removes every artifact the pointer's write cycle can leave
// behind: the published ".dat" (or the 0-byte reservation if the write never
// completed) and the derived ".tmp" from an aborted or failed write.
func (fs *FileSystemDataStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	finalPath := string(filePointerBytes)

	var errs []error
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if strings.HasSuffix(finalPath, ".dat") {
		tempPath := strings.TrimSuffix(finalPath, ".dat") + ".tmp"
		if err := os.Remove(tempPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// readFileMetadata opens filePath and reads its footer through the public
// ReadFileMetadata parser.
func (fs *FileSystemDataStore) readFileMetadata(filePath string) (*FileMetadata, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	metadata, _, err := ReadFileMetadata(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata from %s: %w", filePath, err)
	}
	return metadata, nil
}

// GetMaybeFilesForQuery streams each candidate file as the directory scan
// proceeds: one file's metadata (footer decode, filters included) is in
// memory per yield rather than the whole directory's.
func (fs *FileSystemDataStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) iter.Seq2[MaybeFile, error] {
	return func(yield func(MaybeFile, error) bool) {
		files, err := os.ReadDir(fs.rootDir)
		if err != nil {
			yield(MaybeFile{}, err)
			return
		}

		for _, file := range files {
			// Honor ctx on every entry, not just at yields: the skip paths
			// (non-.dat entries, unreadable footers, fully pruned files)
			// never yield, and a terminated query must not wait for the
			// remaining scan.
			if ctx.Err() != nil {
				return
			}

			// Skip directories and non-bloom files
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".dat") {
				continue
			}

			filePath := filepath.Join(fs.rootDir, file.Name())

			// Read file metadata from bloom file. Unreadable or invalid files
			// (partial writes, foreign files dropped in the directory) are
			// skipped rather than failing the whole query.
			fileMetadata, err := fs.readFileMetadata(filePath)
			if err != nil {
				continue
			}

			// Filter data blocks based on query conditions
			fileMetadata.DataBlocks = FilterDataBlocks(fileMetadata.DataBlocks, query)

			// Only yield files that have matching data blocks (or all files if no query conditions)
			if query != nil && len(fileMetadata.DataBlocks) == 0 {
				continue
			}
			if !yield(MaybeFile{
				PointerBytes: []byte(filePath),
				Metadata:     *fileMetadata,
			}, nil) {
				return
			}
		}
	}
}

func (fs *FileSystemDataStore) Update(ctx context.Context, writes []WriteOperation, deletes []DeleteOperation) error {
	// writes are no-op, it's stored in the files
	for _, delete := range deletes {
		os.Remove(string(delete.FilePointerBytes))
	}
	return nil
}

func init() {
	var _ DataStore = &FileSystemDataStore{}
	var _ MetaStore = &FileSystemDataStore{}
}
