package bloomsearch

/**
This package should probably make lowerJSON versions of structs to protect internal fields, but that's for a later optimization.
*/

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

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

	LengthPrefixSize  = 4
	VersionPrefixSize = 4
	HashSize          = 8
)

type FileMetadata struct {
	BloomFilters           BloomFilters
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64

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

// BloomFilters contains the bloom filters for a data block
// This struct is serialized and stored at the beginning of each data block
type BloomFilters struct {
	FieldBloomFilter      *bloom.BloomFilter
	TokenBloomFilter      *bloom.BloomFilter
	FieldTokenBloomFilter *bloom.BloomFilter
}

// Returns the data block bloom filters as a byte slice and the xxhash of the bloom filters
func (d *BloomFilters) Bytes() ([]byte, []byte) {
	jsonBytes, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	xxhashValue := xxhash.Sum64(jsonBytes)
	xxhashBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(xxhashBytes, xxhashValue)
	return jsonBytes, xxhashBytes
}

func DataBlockBloomFiltersFromBytesWithHash(bytes []byte, expectedHashBytes []byte) (*BloomFilters, error) {
	// Calculate xxhash of the provided bytes
	actualHash := xxhash.Sum64(bytes)

	// Convert expected hash bytes to uint64
	expectedHash := binary.LittleEndian.Uint64(expectedHashBytes)

	// Verify hash matches
	if actualHash != expectedHash {
		return nil, fmt.Errorf("%w: expected %x, got %x", ErrInvalidHash, expectedHash, actualHash)
	}

	// Unmarshal the JSON bytes into DataBlockBloomFilters
	var bloomFilters BloomFilters
	err := json.Unmarshal(bytes, &bloomFilters)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal bloom filters: %w", err)
	}

	return &bloomFilters, nil
}

// ReadDataBlockBloomFilters reads bloom filters from a data block given a file reader and block metadata
func ReadDataBlockBloomFilters(file io.ReadSeeker, blockMetadata DataBlockMetadata) (*BloomFilters, error) {
	// Seek to the beginning of the data block
	_, err := file.Seek(int64(blockMetadata.Offset), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to block offset: %w", err)
	}

	// Read bloom filters bytes (excluding the hash)
	bloomFiltersBytes := make([]byte, blockMetadata.BloomFiltersSize-8)
	_, err = file.Read(bloomFiltersBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filters: %w", err)
	}

	// Read bloom filters hash
	bloomFiltersHashBytes := make([]byte, 8)
	_, err = file.Read(bloomFiltersHashBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read bloom filters hash: %w", err)
	}

	// Verify and parse bloom filters
	return DataBlockBloomFiltersFromBytesWithHash(bloomFiltersBytes, bloomFiltersHashBytes)
}

// CompressionType represents the compression algorithm used for row data
type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionSnappy CompressionType = "snappy"
	CompressionZstd   CompressionType = "zstd"
)

type DataBlockMetadata struct {
	// Absolute file offset (includes bloom filters at the beginning)
	Offset int

	// Size includes the bloom filters, their hash, and row data (no trailing hash)
	Size int
	Rows int

	// Size of the bloom filters section (bloom filters + hash)
	BloomFiltersSize int

	MinMaxIndexes map[string]MinMaxIndex `json:",omitempty"`
	PartitionID   string                 `json:",omitempty"`

	// Compression algorithm used for the row data in this block
	Compression CompressionType `json:",omitempty"`

	// Uncompressed size of row data (for decompression buffer allocation)
	UncompressedSize int `json:",omitempty"`

	// Hash of the compressed row data (for integrity verification)
	RowDataHash uint64 `json:",omitempty"`

	BloomExpectedItems     uint
	BloomFalsePositiveRate float64
}
