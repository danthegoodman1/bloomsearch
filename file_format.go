package bloomsearch

/**
This package should probably make lowerJSON versions of structs to protect internal fields, but that's for a later optimization.
*/

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

// File format constants
const (
	FileVersion = uint32(1)
	MagicBytes  = "BLOMSRCH"
)

type FileMetadata struct {
	FieldBloomFilter      *bloom.BloomFilter // must exist
	TokenBloomFilter      *bloom.BloomFilter // must exist
	FieldTokenBloomFilter *bloom.BloomFilter // must exist

	DataBlocks []DataBlockMetadata
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
	var metadata FileMetadata
	err := json.Unmarshal(bytes, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

type DataBlockMetadata struct {
	// Absolute file offset
	Offset int

	// Size includes the uint64 xxhash at the end of the byte slice
	Size int
	Rows int

	BloomFilter *bloom.BloomFilter // must exist

	MinMaxIndexes map[string]MinMaxIndex `json:",omitempty"`
	PartitionID   string                 `json:",omitempty"`
}
