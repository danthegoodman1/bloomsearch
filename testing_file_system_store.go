package bloomsearch

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type FileSystemDataStore struct {
	rootDir string
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
		rootDir: rootDir,
	}
}

func (fs *FileSystemDataStore) OpenFile(ctx context.Context, filePointerBytes []byte) (io.ReadSeekCloser, error) {
	filePath := string(filePointerBytes)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (fs *FileSystemDataStore) CreateFile(ctx context.Context) (io.WriteCloser, []byte, error) {
	file, err := os.CreateTemp(fs.rootDir, "bloom-*.dat")
	if err != nil {
		return nil, nil, err
	}

	filePath := file.Name()
	return file, []byte(filePath), nil
}

// readFileMetadata reads the file metadata from a bloom file
func (fs *FileSystemDataStore) readFileMetadata(filePath string) (*FileMetadata, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}
	fileSize := stat.Size()

	// Check if file is large enough to contain the footer
	// Footer: [8 bytes magic] + [4 bytes version] + [4 bytes metadata length] + [8 bytes metadata hash]
	minFooterSize := int64(8 + 4 + 4 + 8)
	if fileSize < minFooterSize {
		return nil, fmt.Errorf("file %s is too small to be a valid bloom file", filePath)
	}

	// Read magic bytes from the end
	magicBytes := make([]byte, 8)
	_, err = file.ReadAt(magicBytes, fileSize-8)
	if err != nil {
		return nil, fmt.Errorf("failed to read magic bytes from %s: %w", filePath, err)
	}

	// Verify magic bytes
	if string(magicBytes) != MagicBytes {
		return nil, fmt.Errorf("invalid magic bytes in file %s", filePath)
	}

	// Read file version
	versionBytes := make([]byte, 4)
	_, err = file.ReadAt(versionBytes, fileSize-8-4)
	if err != nil {
		return nil, fmt.Errorf("failed to read version from %s: %w", filePath, err)
	}
	version := binary.LittleEndian.Uint32(versionBytes)

	// Verify version
	if version != FileVersion {
		return nil, fmt.Errorf("unsupported file version %d in file %s", version, filePath)
	}

	// Read metadata length
	metadataLengthBytes := make([]byte, 4)
	_, err = file.ReadAt(metadataLengthBytes, fileSize-8-4-4)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata length from %s: %w", filePath, err)
	}
	metadataLength := binary.LittleEndian.Uint32(metadataLengthBytes)

	// Read metadata hash
	metadataHashBytes := make([]byte, 8)
	_, err = file.ReadAt(metadataHashBytes, fileSize-8-4-4-8)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata hash from %s: %w", filePath, err)
	}

	// Read metadata
	metadataBytes := make([]byte, metadataLength)
	metadataOffset := fileSize - 8 - 4 - 4 - 8 - int64(metadataLength)
	_, err = file.ReadAt(metadataBytes, metadataOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata from %s: %w", filePath, err)
	}

	// Parse and verify metadata
	metadata, err := FileMetadataFromBytesWithHash(metadataBytes, metadataHashBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata from %s: %w", filePath, err)
	}

	return metadata, nil
}

func (fs *FileSystemDataStore) GetMaybeFilesForQuery(ctx context.Context, query *QueryCondition) ([]MaybeFile, error) {
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

		// Read file metadata from bloom file
		fileMetadata, err := fs.readFileMetadata(filePath)
		if err != nil {
			// Skip files with read errors (they might not be valid bloom files)
			continue
		}

		// Filter data blocks based on query conditions
		fileMetadata.DataBlocks = FilterDataBlocks(fileMetadata.DataBlocks, query)

		// Only include files that have matching data blocks (or all files if no query conditions)
		if query == nil || len(fileMetadata.DataBlocks) > 0 {
			maybeFiles = append(maybeFiles, MaybeFile{
				PointerBytes: []byte(filePath),
				Metadata:     *fileMetadata,
			})
		}
	}

	return maybeFiles, nil
}

func (fs *FileSystemDataStore) WriteFileMetadata(ctx context.Context, fileMetadata *FileMetadata, filePointerBytes []byte) error {
	// no-op, it's stored in the files
	return nil
}

func init() {
	var _ DataStore = &FileSystemDataStore{}
	var _ MetaStore = &FileSystemDataStore{}
}
