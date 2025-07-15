package bloomsearch

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
)

func TestMergeWithRealEngine(t *testing.T) {
	// Create a simple metastore
	metaStore := NewSimpleMetaStore()

	// Create some synthetic file metadata with different data blocks
	bloom1 := bloom.NewWithEstimates(1000, 0.01)
	bloom2 := bloom.NewWithEstimates(1000, 0.01)
	bloom3 := bloom.NewWithEstimates(1000, 0.01)

	ctx := context.Background()

	// Prepare synthetic file metadata
	file1Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "partition_b",
				Offset:      0,
				Size:        1000,
				Rows:        100,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 1000, Max: 2000},
					"user_id":   {Min: 100, Max: 200},
				},
			},
			{
				PartitionID: "partition_a",
				Offset:      1000,
				Size:        500,
				Rows:        50,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 500, Max: 1000},
				},
			},
		},
	}

	file2Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "partition_a",
				Offset:      0,
				Size:        800,
				Rows:        80,
				// No MinMax indexes - should be sorted after ones with indexes
			},
			{
				PartitionID: "partition_b",
				Offset:      800,
				Size:        1200,
				Rows:        120,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 1500, Max: 2500},
					"user_id":   {Min: 150, Max: 250},
				},
			},
		},
	}

	file3Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "partition_a",
				Offset:      0,
				Size:        600,
				Rows:        60,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 200, Max: 800},
					"user_id":   {Min: 50, Max: 150},
				},
			},
			{
				PartitionID: "partition_b",
				Offset:      600,
				Size:        300,
				Rows:        30,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 2000, Max: 3000},
				},
			},
		},
	}

	// File with no partition ID (empty string)
	file4Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "", // No partition
				Offset:      0,
				Size:        400,
				Rows:        40,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 100, Max: 300},
					"user_id":   {Min: 10, Max: 50},
				},
			},
			{
				PartitionID: "", // No partition
				Offset:      400,
				Size:        200,
				Rows:        20,
				// No MinMax indexes
			},
		},
	}

	// File with unique partition IDs
	file5Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "partition_unique_x",
				Offset:      0,
				Size:        750,
				Rows:        75,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 800, Max: 1200},
				},
			},
			{
				PartitionID: "partition_unique_y",
				Offset:      750,
				Size:        350,
				Rows:        35,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 1800, Max: 2200},
					"user_id":   {Min: 200, Max: 300},
				},
			},
		},
	}

	// File with more partition_a blocks to test complex sorting within partition
	file6Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "partition_a",
				Offset:      0,
				Size:        450,
				Rows:        45,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 200, Max: 800}, // Same timestamp range as file3 block 0
					"user_id":   {Min: 25, Max: 75},   // Different user_id range
				},
			},
			{
				PartitionID: "partition_a",
				Offset:      450,
				Size:        650,
				Rows:        65,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 150, Max: 400}, // Earlier timestamp
					"user_id":   {Min: 80, Max: 120},
				},
			},
			{
				PartitionID: "partition_a",
				Offset:      1100,
				Size:        900,
				Rows:        90,
				// No MinMax indexes - should be sorted after ones with indexes
			},
		},
	}

	// File with partition_b blocks with overlapping ranges
	file7Metadata := FileMetadata{
		FieldBloomFilter:      bloom1,
		TokenBloomFilter:      bloom2,
		FieldTokenBloomFilter: bloom3,
		DataBlocks: []DataBlockMetadata{
			{
				PartitionID: "partition_b",
				Offset:      0,
				Size:        800,
				Rows:        80,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 1000, Max: 2000}, // Same as file1 block 0
					"user_id":   {Min: 90, Max: 190},    // Overlapping but different range
				},
			},
			{
				PartitionID: "partition_b",
				Offset:      800,
				Size:        600,
				Rows:        60,
				MinMaxIndexes: map[string]MinMaxIndex{
					"timestamp": {Min: 1000, Max: 2000}, // Same as above
					"user_id":   {Min: 110, Max: 210},   // Different user_id range
				},
			},
		},
	}

	// Prepare WriteOperations array
	writeOps := []WriteOperation{
		{
			FileMetadata:     &file1Metadata,
			FilePointerBytes: []byte("file1"),
		},
		{
			FileMetadata:     &file2Metadata,
			FilePointerBytes: []byte("file2"),
		},
		{
			FileMetadata:     &file3Metadata,
			FilePointerBytes: []byte("file3"),
		},
		{
			FileMetadata:     &file4Metadata,
			FilePointerBytes: []byte("file4"),
		},
		{
			FileMetadata:     &file5Metadata,
			FilePointerBytes: []byte("file5"),
		},
		{
			FileMetadata:     &file6Metadata,
			FilePointerBytes: []byte("file6"),
		},
		{
			FileMetadata:     &file7Metadata,
			FilePointerBytes: []byte("file7"),
		},
	}

	// Shuffle the files to make the test more realistic
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(writeOps), func(i, j int) {
		writeOps[i], writeOps[j] = writeOps[j], writeOps[i]
	})

	fmt.Println("=== SHUFFLED FILE ORDER ===")
	for i, op := range writeOps {
		fmt.Printf("  [%d] File: %s, DataBlocks: %d\n", i, string(op.FilePointerBytes), len(op.FileMetadata.DataBlocks))
	}

	err := metaStore.Update(ctx, writeOps, nil)
	if err != nil {
		t.Fatalf("Failed to update metastore: %v", err)
	}

	// Create the engine with MinMax indexes configured
	config := DefaultBloomSearchEngineConfig()
	config.MinMaxIndexes = []string{"timestamp", "user_id"}
	config.MaxFileSize = 2000 // Set a smaller file size limit (will be used at file level, not merge group level)

	engine, err := NewBloomSearchEngine(config, metaStore, &NullDataStore{})
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	fmt.Println("=== METASTORE CONTENTS ===")
	metaStore.PrintFiles()

	fmt.Println("\n=== MERGE SORTING TEST ===")
	err = engine.merge(ctx)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
}
