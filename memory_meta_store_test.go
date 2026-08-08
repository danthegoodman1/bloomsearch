package bloomsearch

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ignorePrefilterMetaStore wraps MemoryMetaStore but discards the prefilter,
// modeling a naive MetaStore that returns everything. The engine must still
// return only prefilter-matching rows.
type ignorePrefilterMetaStore struct {
	*MemoryMetaStore
}

func (s *ignorePrefilterMetaStore) GetMaybeFilesForQuery(ctx context.Context, prefilter *QueryPrefilter) ([]MaybeFile, error) {
	return s.MemoryMetaStore.GetMaybeFilesForQuery(ctx, nil)
}

// TestPrefilterEnforcedByEngine is the regression for the engine trusting
// MetaStore-side prefiltering: with a store that returns every data block, a
// partition- or minmax-prefiltered query used to return rows from every
// partition.
func TestPrefilterEnforcedByEngine(t *testing.T) {
	stores := []struct {
		name string
		make func() MetaStore
	}{
		{"MemoryMetaStore", func() MetaStore { return NewMemoryMetaStore() }},
		{"PrefilterIgnoringMetaStore", func() MetaStore { return &ignorePrefilterMetaStore{NewMemoryMetaStore()} }},
	}

	for _, store := range stores {
		t.Run(store.name, func(t *testing.T) {
			dataStore := NewFileSystemDataStore(t.TempDir())

			config := DefaultBloomSearchEngineConfig()
			config.MaxBufferedTime = time.Hour
			config.PartitionFunc = func(row map[string]any) string {
				partition, _ := row["partition"].(string)
				return partition
			}
			config.MinMaxIndexes = []string{"value"}

			engine, err := NewBloomSearchEngine(config, store.make(), dataStore)
			if err != nil {
				t.Fatalf("failed to create engine: %v", err)
			}
			engine.Start()
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				engine.Stop(ctx)
			}()

			ctx := context.Background()
			rows := []map[string]any{
				{"id": "a1", "partition": "a", "value": 1.0},
				{"id": "a2", "partition": "a", "value": 2.0},
				{"id": "a3", "partition": "a", "value": 3.0},
				{"id": "b1", "partition": "b", "value": 100.0},
				{"id": "b2", "partition": "b", "value": 101.0},
				{"id": "b3", "partition": "b", "value": 102.0},
			}
			if err := engine.IngestRows(ctx, rows, nil); err != nil {
				t.Fatalf("failed to ingest rows: %v", err)
			}
			if err := engine.Flush(ctx); err != nil {
				t.Fatalf("failed to flush: %v", err)
			}

			runPrefilterQuery := func(t *testing.T, expression PrefilterExpression, wantIDs map[string]bool) {
				t.Helper()
				query := NewQuery().MatchPrefilter(expression).Build()
				resultChan := make(chan map[string]any, len(rows)*2)
				errorChan := make(chan error, 16)
				if err := engine.Query(ctx, query, resultChan, errorChan, nil); err != nil {
					t.Fatalf("query failed: %v", err)
				}

				gotIDs := make(map[string]bool)
				for row := range resultChan {
					id, _ := row["id"].(string)
					gotIDs[id] = true
				}
				select {
				case err := <-errorChan:
					t.Fatalf("query worker error: %v", err)
				default:
				}

				if len(gotIDs) != len(wantIDs) {
					t.Fatalf("expected ids %v, got %v", wantIDs, gotIDs)
				}
				for id := range wantIDs {
					if !gotIDs[id] {
						t.Fatalf("expected ids %v, got %v", wantIDs, gotIDs)
					}
				}
			}

			partitionA := map[string]bool{"a1": true, "a2": true, "a3": true}

			t.Run("partition prefilter", func(t *testing.T) {
				runPrefilterQuery(t, Partition(PartitionEquals("a")), partitionA)
			})

			t.Run("minmax prefilter", func(t *testing.T) {
				runPrefilterQuery(t, MinMax("value", NumericLessThanEqual(50)), partitionA)
			})
		})
	}
}

// TestMemoryMetaStoreConcurrentAccess exercises concurrent Update and
// GetMaybeFilesForQuery calls; it must pass under -race.
func TestMemoryMetaStoreConcurrentAccess(t *testing.T) {
	store := NewMemoryMetaStore()
	prefilter := NewQueryPrefilter()
	expression := Partition(PartitionEquals("a"))
	prefilter.Expression = &expression

	const (
		writers    = 4
		readers    = 4
		iterations = 200
	)

	ctx := context.Background()
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				pointer := []byte(fmt.Sprintf("file-%d-%d", w, i))
				metadata := &FileMetadata{
					DataBlocks: []DataBlockMetadata{
						{PartitionID: "a", Rows: 1},
						{PartitionID: "b", Rows: 1},
					},
				}
				if err := store.Update(ctx, []WriteOperation{{FileMetadata: metadata, FilePointerBytes: pointer}}, nil); err != nil {
					t.Errorf("update failed: %v", err)
					return
				}
				if i%2 == 0 {
					if err := store.Update(ctx, nil, []DeleteOperation{{FilePointerBytes: pointer}}); err != nil {
						t.Errorf("delete failed: %v", err)
						return
					}
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				maybeFiles, err := store.GetMaybeFilesForQuery(ctx, prefilter)
				if err != nil {
					t.Errorf("query failed: %v", err)
					return
				}
				for _, maybeFile := range maybeFiles {
					for _, block := range maybeFile.Metadata.DataBlocks {
						if block.PartitionID != "a" {
							t.Errorf("prefilter leaked block from partition %q", block.PartitionID)
							return
						}
					}
				}
			}
		}()
	}

	wg.Wait()
}
