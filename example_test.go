package bloomsearch_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	bloomsearch "github.com/danthegoodman1/bloomsearch"
)

// Example mirrors the README quick start: ingest rows, flush them to durable
// storage, then stream the matching rows back through the query cursor. The
// engine is silent by default (diagnostics go to the discard logger unless
// BloomSearchEngineConfig.Logger is set), so the output below is exactly what
// the example itself prints.
func Example() {
	dir, err := os.MkdirTemp("", "bloomsearch-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// The filesystem store implements both the MetaStore and the DataStore.
	store := bloomsearch.NewFileSystemDataStore(dir)
	engine, err := bloomsearch.NewBloomSearchEngine(bloomsearch.DefaultBloomSearchEngineConfig(), store, store)
	if err != nil {
		log.Fatal(err)
	}
	engine.Start()

	ctx := context.Background()

	// Ingest is asynchronous: the done channel receives nil once the rows
	// are durable, or the error that prevented it.
	doneChan := make(chan error, 1)
	rows := []map[string]any{
		{"id": 1, "service": "auth", "message": "login timeout for user"},
		{"id": 2, "service": "payment", "message": "charge succeeded"},
	}
	if err := engine.IngestRows(ctx, rows, doneChan); err != nil {
		log.Fatal(err)
	}

	// Force a flush and wait for the ingest ack.
	if err := engine.Flush(ctx); err != nil {
		log.Fatal(err)
	}
	if err := <-doneChan; err != nil {
		log.Fatal(err)
	}

	// Query through the engine-owned cursor.
	query := bloomsearch.NewQuery().FieldToken("service", "auth").Build()
	results, err := engine.Query(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	defer results.Close()

	for results.Next() {
		row := results.Row()
		fmt.Println(row["service"], row["message"])
	}
	// Err is nil on clean completion; block errors are joined here, and a
	// canceled query reports its context error.
	if err := results.Err(); err != nil {
		log.Fatal(err)
	}

	stats := results.Stats()
	fmt.Printf("scanned %d rows across %d blocks\n", stats.RowsScanned, stats.BlocksProcessed)

	// Stop with a deadline: ctx expiry is the only way a wedged shutdown
	// aborts.
	stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := engine.Stop(stopCtx); err != nil {
		log.Fatal(err)
	}

	// Output:
	// auth login timeout for user
	// scanned 2 rows across 1 blocks
}
