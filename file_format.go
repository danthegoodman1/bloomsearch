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

	"hash/crc32"

	"github.com/bits-and-blooms/bloom/v3"
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
	HashSize          = 4
)

// CRC32C table used for checksums (Castagnoli)
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

type FileMetadata struct {
	BloomFilters           BloomFilters
	BloomExpectedItems     uint
	BloomFalsePositiveRate float64

	DataBlocks []DataBlockMetadata
}

// Returns the file metadata as a byte slice and the CRC32C of the file metadata
func (f *FileMetadata) Bytes() ([]byte, []byte) {
	jsonBytes, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	crc := crc32.Checksum(jsonBytes, crc32cTable)
	crcBytes := make([]byte, HashSize)
	binary.LittleEndian.PutUint32(crcBytes, crc)
	return jsonBytes, crcBytes
}

func FileMetadataFromBytesWithHash(bytes []byte, expectedHashBytes []byte) (*FileMetadata, error) {
	// Calculate CRC32C of the provided bytes
	actualHash := crc32.Checksum(bytes, crc32cTable)

	// Convert expected hash bytes to uint32
	expectedHash := binary.LittleEndian.Uint32(expectedHashBytes)

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

// Returns the data block bloom filters as a byte slice and the CRC32C of the bloom filters
func (d *BloomFilters) Bytes() ([]byte, []byte) {
	jsonBytes, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	crc := crc32.Checksum(jsonBytes, crc32cTable)
	crcBytes := make([]byte, HashSize)
	binary.LittleEndian.PutUint32(crcBytes, crc)
	return jsonBytes, crcBytes
}

func DataBlockBloomFiltersFromBytesWithHash(bytes []byte, expectedHashBytes []byte) (*BloomFilters, error) {
	// Calculate CRC32C of the provided bytes
	actualHash := crc32.Checksum(bytes, crc32cTable)

	// Convert expected hash bytes to uint32
	expectedHash := binary.LittleEndian.Uint32(expectedHashBytes)

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
	bloomFiltersBytes := make([]byte, blockMetadata.BloomFiltersSize-HashSize)
	if _, err = io.ReadFull(file, bloomFiltersBytes); err != nil {
		return nil, fmt.Errorf("failed to read bloom filters: %w", err)
	}

	// Read bloom filters hash
	bloomFiltersHashBytes := make([]byte, HashSize)
	if _, err = io.ReadFull(file, bloomFiltersHashBytes); err != nil {
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
	RowDataHash uint32 `json:",omitempty"`

	BloomExpectedItems     uint
	BloomFalsePositiveRate float64
}
