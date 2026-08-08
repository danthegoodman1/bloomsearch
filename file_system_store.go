package bloomsearch

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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

type FileSystemDataStoreFilePointer struct {
	ID string
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
// that is what callers store in the MetaStore. If Close fails, the ".tmp"
// file and the empty reservation are left in place, both invisible to
// queries.
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
// successful Close (sync, close, rename — in that order), replacing the
// 0-byte reservation CreateFile placed at finalPath. Any failure leaves the
// ".tmp" file and the reservation in place.
//
// TombstoneFile only removes the final path, so an aborted write orphans its
// ".tmp": Phase 3B's abort cleanup must track tempPath to remove it.
type renameOnCloseFile struct {
	file      *os.File
	tempPath  string
	finalPath string
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
	return os.Rename(f.tempPath, f.finalPath)
}

func (fs *FileSystemDataStore) TombstoneFile(ctx context.Context, filePointerBytes []byte) error {
	if err := os.Remove(string(filePointerBytes)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// readFileMetadata reads the file metadata from a bloom file, returning it
// with the file size.
func (fs *FileSystemDataStore) readFileMetadata(filePath string) (*FileMetadata, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}
	fileSize := stat.Size()

	// Check if file is large enough to contain the footer
	// Footer: [8 bytes magic] + [4 bytes version] + [4 bytes metadata length] + [HashSize bytes metadata hash]
	minFooterSize := int64(8 + 4 + 4 + HashSize)
	if fileSize < minFooterSize {
		return nil, 0, fmt.Errorf("file %s is too small to be a valid bloom file", filePath)
	}

	// Read magic bytes from the end
	magicBytes := make([]byte, 8)
	_, err = file.ReadAt(magicBytes, fileSize-8)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read magic bytes from %s: %w", filePath, err)
	}

	// Verify magic bytes
	if string(magicBytes) != MagicBytes {
		return nil, 0, fmt.Errorf("invalid magic bytes in file %s", filePath)
	}

	// Read file version
	versionBytes := make([]byte, 4)
	_, err = file.ReadAt(versionBytes, fileSize-8-4)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read version from %s: %w", filePath, err)
	}
	version := binary.LittleEndian.Uint32(versionBytes)

	// Verify version
	if version != FileVersion {
		return nil, 0, fmt.Errorf("unsupported file version %d in file %s", version, filePath)
	}

	// Read metadata length
	metadataLengthBytes := make([]byte, 4)
	_, err = file.ReadAt(metadataLengthBytes, fileSize-8-4-4)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read metadata length from %s: %w", filePath, err)
	}
	metadataLength := binary.LittleEndian.Uint32(metadataLengthBytes)

	// Read metadata hash
	metadataHashBytes := make([]byte, HashSize)
	_, err = file.ReadAt(metadataHashBytes, fileSize-8-4-4-int64(HashSize))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read metadata hash from %s: %w", filePath, err)
	}

	// Read metadata
	metadataBytes := make([]byte, metadataLength)
	metadataOffset := fileSize - 8 - 4 - 4 - int64(HashSize) - int64(metadataLength)
	_, err = file.ReadAt(metadataBytes, metadataOffset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read metadata from %s: %w", filePath, err)
	}

	// Parse and verify metadata
	metadata, err := FileMetadataFromBytesWithHash(metadataBytes, metadataHashBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse metadata from %s: %w", filePath, err)
	}

	return metadata, fileSize, nil
}

func (fs *FileSystemDataStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryPrefilter) ([]MaybeFile, error) {
	files, err := os.ReadDir(fs.rootDir)
	if err != nil {
		return nil, err
	}

	maybeFiles := make([]MaybeFile, 0, len(files))
	for _, file := range files {
		// Skip directories and non-bloom files
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".dat") {
			continue
		}

		filePath := filepath.Join(fs.rootDir, file.Name())

		// Read file metadata from bloom file. Unreadable or invalid files
		// (partial writes, foreign files dropped in the directory) are
		// skipped rather than failing the whole query.
		// TODO(Phase 7): report skipped files through the injectable
		// structured logger once it lands.
		fileMetadata, fileSize, err := fs.readFileMetadata(filePath)
		if err != nil {
			continue
		}

		// Filter data blocks based on query conditions
		fileMetadata.DataBlocks = FilterDataBlocks(fileMetadata.DataBlocks, query)

		// Only include files that have matching data blocks (or all files if no query conditions)
		if query == nil || len(fileMetadata.DataBlocks) > 0 {
			maybeFiles = append(maybeFiles, MaybeFile{
				PointerBytes: []byte(filePath),
				Metadata:     *fileMetadata,
				Size:         int(fileSize),
			})
		}
	}

	return maybeFiles, nil
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
