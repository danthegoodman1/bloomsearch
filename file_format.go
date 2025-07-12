package bloomsearch

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash"
)

var (
	ErrInvalidHash = errors.New("invalid hash")
)

type FileMetadata struct {
	bloomFilter *bloom.BloomFilter
}

// need to proxy with another struct to keep the properties of FileMetadata private but allow json marshalling
type fileMetadataJSON struct {
	BloomFilter *bloom.BloomFilter
}

func NewFileMetadata(bloomFilter *bloom.BloomFilter) *FileMetadata {
	return &FileMetadata{
		bloomFilter: bloomFilter,
	}
}

// Returns the file metadata as a byte slice and the xxhash of the file metadata
func (f *FileMetadata) Bytes() ([]byte, []byte) {
	jsonBytes, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	xxhashValue := xxhash.Sum64(jsonBytes)
	xxhashBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(xxhashBytes, xxhashValue)
	return jsonBytes, xxhashBytes
}

func FileMetadataFromBytesWithHash(bytes []byte, expectedHashBytes []byte) (*FileMetadata, error) {
	// Calculate xxhash of the provided bytes
	actualHash := xxhash.Sum64(bytes)

	// Convert expected hash bytes to uint64
	expectedHash := binary.LittleEndian.Uint64(expectedHashBytes)

	// Verify hash matches
	if actualHash != expectedHash {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidHash, expectedHash, actualHash)
	}

	// Unmarshal the JSON bytes into FileMetadata
	var metadata fileMetadataJSON
	err := json.Unmarshal(bytes, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &FileMetadata{
		bloomFilter: metadata.BloomFilter,
	}, nil
}

func (f *FileMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(fileMetadataJSON{
		BloomFilter: f.bloomFilter,
	})
}

type DataBlockMetadata struct {
	bloomFilter *bloom.BloomFilter

	offset uint64
	// size includes the uint64 xxhash at the end of the byte slice
	size uint64
}

// TODO: stream write datablock and build hash
