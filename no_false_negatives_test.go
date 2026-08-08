package bloomsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// Phase 1 regression tests: ingest indexing and query-time row verification
// walk one canonical representation (the marshaled JSON bytes) through one
// shared walker, so any row matching a query is always returned.

// newTestEngine creates and starts an engine backed by a temp directory,
// stopped automatically at test cleanup.
func newTestEngine(t *testing.T, mutate func(*BloomSearchEngineConfig)) *BloomSearchEngine {
	t.Helper()

	store := NewFileSystemDataStore(t.TempDir())
	config := DefaultBloomSearchEngineConfig()
	config.MaxBufferedTime = time.Hour
	if mutate != nil {
		mutate(&config)
	}

	engine, err := NewBloomSearchEngine(config, store, store)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	engine.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(ctx)
	})
	return engine
}

// ingestAndFlush ingests rows and forces a flush, failing the test on any error.
func ingestAndFlush(t *testing.T, engine *BloomSearchEngine, rows []map[string]any) {
	t.Helper()

	ctx := context.Background()
	doneChan := make(chan error, 1)
	if err := engine.IngestRows(ctx, rows, doneChan); err != nil {
		t.Fatalf("Failed to ingest rows: %v", err)
	}
	if err := engine.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	select {
	case err := <-doneChan:
		if err != nil {
			t.Fatalf("Ingest failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Ingest did not complete within timeout")
	}
}

// queryRows runs a query and returns all matching rows.
func queryRows(t *testing.T, engine *BloomSearchEngine, query *Query) []map[string]any {
	t.Helper()

	resultChan := make(chan map[string]any, 1024)
	errorChan := make(chan error, 16)
	if err := engine.Query(context.Background(), query, resultChan, errorChan, nil); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	var results []map[string]any
	for row := range resultChan {
		results = append(results, row)
	}
	select {
	case err := <-errorChan:
		if err != nil {
			t.Fatalf("Query returned error: %v", err)
		}
	default:
	}
	return results
}

// resultIDs collects the "id" fields of the results (unmarshaled as float64).
func resultIDs(results []map[string]any) map[float64]bool {
	ids := make(map[float64]bool)
	for _, row := range results {
		if id, ok := row["id"].(float64); ok {
			ids[id] = true
		}
	}
	return ids
}

// TestFieldTokenLargeInt: ints ≥ 1e6 and float64-lossy int64s must be findable
// by their exact decimal string. The reflection-based ingest indexed "1234567"
// while verification tokenized the gjson float64 via %v into "1.234567e+06",
// so FieldToken("user_id", "1234567") returned 0 rows.
func TestFieldTokenLargeInt(t *testing.T) {
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "user_id": 1234567},
		{"id": 2, "big": int64(9007199254740993)}, // 2^53+1, not exactly representable as float64
	})

	results := queryRows(t, engine, NewQuery().FieldToken("user_id", "1234567").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldToken(user_id, 1234567): expected exactly row 1, got %v", results)
	}

	results = queryRows(t, engine, NewQuery().FieldToken("big", "9007199254740993").Build())
	if len(results) != 1 || !resultIDs(results)[2] {
		t.Fatalf("FieldToken(big, 9007199254740993): expected exactly row 2, got %v", results)
	}

	results = queryRows(t, engine, NewQuery().Token("1234567").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("Token(1234567): expected exactly row 1, got %v", results)
	}
}

// TestFieldWithDelimiterInKey: a key containing the path delimiter must be
// findable via its literal name. Verification used to split "a.b" and walk
// nested objects, never seeing the flat key. A dotted key intentionally
// behaves like the equivalent nested path: both match Field("a.b"), and via
// delimiter-split prefix paths both match Field("a") and pass the
// FieldRegex("a", ...) bloom field guard.
func TestFieldWithDelimiterInKey(t *testing.T) {
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "a.b": "hello"},
		{"id": 2, "a": map[string]any{"b": "world"}},
		{"id": 3, "user.name": "x"},
		{"id": 4, ".a": "xyz"},
	})

	results := queryRows(t, engine, NewQuery().Field("a.b").Build())
	ids := resultIDs(results)
	if len(results) != 2 || !ids[1] || !ids[2] {
		t.Fatalf("Field(a.b): expected rows 1 and 2 (dotted key and nested path collide), got %v", results)
	}

	results = queryRows(t, engine, NewQuery().FieldToken("a.b", "hello").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldToken(a.b, hello): expected exactly row 1, got %v", results)
	}

	results = queryRows(t, engine, NewQuery().FieldToken("a.b", "world").Build())
	if len(results) != 1 || !resultIDs(results)[2] {
		t.Fatalf("FieldToken(a.b, world): expected exactly row 2, got %v", results)
	}

	// Delimiter-split prefixes of a flat dotted key are field-existence paths,
	// matching the nested form's intermediate container path
	results = queryRows(t, engine, NewQuery().Field("a").Build())
	ids = resultIDs(results)
	if len(results) != 2 || !ids[1] || !ids[2] {
		t.Fatalf("Field(a): expected rows 1 and 2 (flat prefix and nested container), got %v", results)
	}

	// Regex on a prefix of a flat dotted key: the bloom field guard must not
	// prune what the row-level regex matcher matches
	results = queryRows(t, engine, NewQuery().FieldRegex("user", "^x$").Build())
	if len(results) != 1 || !resultIDs(results)[3] {
		t.Fatalf("FieldRegex(user, ^x$) on flat key user.name: expected exactly row 3, got %v", results)
	}

	results = queryRows(t, engine, NewQuery().FieldRegex("a", "^hello$").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldRegex(a, ^hello$): expected exactly row 1, got %v", results)
	}

	results = queryRows(t, engine, NewQuery().FieldRegex("a", "^world$").Build())
	if len(results) != 1 || !resultIDs(results)[2] {
		t.Fatalf("FieldRegex(a, ^world$): expected exactly row 2, got %v", results)
	}

	// Empty field paths are nonexistent for every condition type: a
	// leading-delimiter key's "" prefix is skipped by the walker, so the
	// row-level regex matcher must reject it too, keeping both sides
	// consistent (both say no match) instead of bloom-guard-dependent
	emptyFieldQuery := NewQuery().FieldRegex("", "^xyz$").Build()
	results = queryRows(t, engine, emptyFieldQuery)
	if len(results) != 0 {
		t.Fatalf("FieldRegex(\"\", ^xyz$): expected no rows (empty field path is nonexistent), got %v", results)
	}
	compiledEmptyField, err := CompileRegexQuery(emptyFieldQuery.Regex)
	if err != nil {
		t.Fatalf("Failed to compile empty-field regex query: %v", err)
	}
	rowJSON := gjson.Parse(`{"id":4,".a":"xyz"}`)
	if TestGJSONForQuery(rowJSON, nil, compiledEmptyField, ".", BasicWhitespaceLowerTokenizer) {
		t.Fatal("TestGJSONForQuery with FieldRegex(\"\", ^xyz$): expected false to match the bloom guard's empty-path skip")
	}
}

// TestFieldMetacharKeys: field names are literal, never gjson path syntax.
// Verification used gjson Get, so Field("a*") wildcard-matched rows that only
// had the key "ab" (wrong rows in results) and keys containing "\" were
// unfindable.
func TestFieldMetacharKeys(t *testing.T) {
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "ab": "x"},
		{"id": 2, "a*": 1},
		{"id": 3, `back\slash`: "v", "q?x": "y"},
	})

	// Wrong-row leak direction: "a*" must not pattern-match the key "ab"
	results := queryRows(t, engine, NewQuery().Field("a*").Build())
	if len(results) != 1 || !resultIDs(results)[2] {
		t.Fatalf("Field(a*): expected exactly row 2 (must not wildcard-match key 'ab'), got %v", results)
	}

	// Miss direction: metachar keys must be findable
	results = queryRows(t, engine, NewQuery().FieldToken("a*", "1").Build())
	if len(results) != 1 || !resultIDs(results)[2] {
		t.Fatalf("FieldToken(a*, 1): expected exactly row 2, got %v", results)
	}

	results = queryRows(t, engine, NewQuery().Field(`back\slash`).Build())
	if len(results) != 1 || !resultIDs(results)[3] {
		t.Fatalf(`Field(back\slash): expected exactly row 3, got %v`, results)
	}

	results = queryRows(t, engine, NewQuery().FieldToken("q?x", "y").Build())
	if len(results) != 1 || !resultIDs(results)[3] {
		t.Fatalf("FieldToken(q?x, y): expected exactly row 3, got %v", results)
	}
}

// TestFieldNonLeafPath: intermediate object paths are indexed, so Field and
// the FieldRegex bloom guard on a non-leaf path are not bloom-pruned.
func TestFieldNonLeafPath(t *testing.T) {
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "user": map[string]any{"name": "x"}},
		{"id": 2, "other": "y"},
	})

	results := queryRows(t, engine, NewQuery().Field("user").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("Field(user): expected exactly row 1, got %v", results)
	}

	// Regex matches values at or beneath the target path; its bloom field
	// guard must not prune the non-leaf path either
	results = queryRows(t, engine, NewQuery().FieldRegex("user", "^x$").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldRegex(user, ^x$): expected exactly row 1, got %v", results)
	}
}

// TestValueTypeEncodings: values index as their JSON encodings, not their Go
// fmt %v representations, so structs, time.Time, and []byte are findable via
// what actually appears in the stored row. null values contribute field
// existence but no tokens.
func TestValueTypeEncodings(t *testing.T) {
	type testPoint struct {
		X int    `json:"x"`
		Y string `json:"y"`
	}

	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "p": testPoint{X: 7, Y: "hi"}},
		{"id": 2, "ts": time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)}, // "2020-01-02T03:04:05Z"
		{"id": 3, "data": []byte("hi")},                              // base64 "aGk="
		{"id": 4, "n": nil},
	})

	results := queryRows(t, engine, NewQuery().FieldToken("p.x", "7").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldToken(p.x, 7): expected exactly row 1, got %v", results)
	}
	results = queryRows(t, engine, NewQuery().FieldToken("p.y", "hi").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldToken(p.y, hi): expected exactly row 1, got %v", results)
	}

	// Tokenizer lowercases the RFC 3339 encoding
	results = queryRows(t, engine, NewQuery().FieldToken("ts", "2020-01-02t03:04:05z").Build())
	if len(results) != 1 || !resultIDs(results)[2] {
		t.Fatalf("FieldToken(ts, 2020-01-02t03:04:05z): expected exactly row 2, got %v", results)
	}

	// []byte marshals as base64, lowercased by the tokenizer
	results = queryRows(t, engine, NewQuery().FieldToken("data", "agk=").Build())
	if len(results) != 1 || !resultIDs(results)[3] {
		t.Fatalf("FieldToken(data, agk=): expected exactly row 3, got %v", results)
	}

	// null: field existence, no tokens
	results = queryRows(t, engine, NewQuery().Field("n").Build())
	if len(results) != 1 || !resultIDs(results)[4] {
		t.Fatalf("Field(n): expected exactly row 4, got %v", results)
	}
	results = queryRows(t, engine, NewQuery().FieldToken("n", "null").Build())
	if len(results) != 0 {
		t.Fatalf("FieldToken(n, null): expected no rows (null has no tokens), got %v", results)
	}
}

// TestRegexNumericField: regex matches the raw JSON literal of numbers, not a
// float64 %v round-trip (which rendered 1500000 as "1.5e+06").
func TestRegexNumericField(t *testing.T) {
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "latency": 1500000},
		{"id": 2, "latency": 42},
	})

	results := queryRows(t, engine, NewQuery().FieldRegex("latency", "^1500000$").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldRegex(latency, ^1500000$): expected exactly row 1, got %v", results)
	}
}

// TestMinMaxOverflowClamp: out-of-range numeric values clamp instead of
// wrapping. uint64 max used to convert to -1, so NumericGreaterThan(0)
// excluded the block under strict prefilter semantics.
func TestMinMaxOverflowClamp(t *testing.T) {
	// Unit-level conversion checks
	minVal, maxVal, ok := ConvertToMinMaxInt64(uint64(math.MaxUint64))
	if !ok || minVal != math.MaxInt64 || maxVal != math.MaxInt64 {
		t.Fatalf("ConvertToMinMaxInt64(uint64 max): expected clamp to MaxInt64, got (%d, %d, %v)", minVal, maxVal, ok)
	}
	minVal, maxVal, ok = ConvertToMinMaxInt64(1e19)
	if !ok || minVal != math.MaxInt64 || maxVal != math.MaxInt64 {
		t.Fatalf("ConvertToMinMaxInt64(1e19): expected clamp to MaxInt64, got (%d, %d, %v)", minVal, maxVal, ok)
	}
	minVal, maxVal, ok = ConvertToMinMaxInt64(-1e19)
	if !ok || minVal != math.MinInt64 || maxVal != math.MinInt64 {
		t.Fatalf("ConvertToMinMaxInt64(-1e19): expected clamp to MinInt64, got (%d, %d, %v)", minVal, maxVal, ok)
	}
	if _, _, ok := ConvertToMinMaxInt64(math.NaN()); ok {
		t.Fatal("ConvertToMinMaxInt64(NaN): expected ok=false (value not indexed)")
	}
	if _, ok := ConvertToInt64(math.NaN()); ok {
		t.Fatal("ConvertToInt64(NaN): expected ok=false")
	}

	// Saturated bounds: a clamped extreme may understate the true range, so
	// strict comparisons at the exact boundary must include, not exclude
	saturatedHigh := MinMaxIndex{Min: math.MaxInt64, Max: math.MaxInt64}
	if !EvaluateMinMaxCondition(saturatedHigh, NumericGreaterThan(math.MaxInt64)) {
		t.Fatal("saturated max with > MaxInt64: expected inclusion (true max may exceed MaxInt64)")
	}
	if !EvaluateMinMaxCondition(saturatedHigh, NumericNotEquals(math.MaxInt64)) {
		t.Fatal("saturated max with != MaxInt64: expected inclusion (true values may differ)")
	}
	saturatedLow := MinMaxIndex{Min: math.MinInt64, Max: math.MinInt64}
	if !EvaluateMinMaxCondition(saturatedLow, NumericLessThan(math.MinInt64)) {
		t.Fatal("saturated min with < MinInt64: expected inclusion (true min may be below MinInt64)")
	}
	if !EvaluateMinMaxCondition(saturatedLow, NumericNotEquals(math.MinInt64)) {
		t.Fatal("saturated min with != MinInt64: expected inclusion (true values may differ)")
	}
	if !EvaluateMinMaxCondition(MinMaxIndex{Min: math.MinInt64, Max: math.MaxInt64}, NumericNotBetween(math.MinInt64, math.MaxInt64)) {
		t.Fatal("saturated bounds with NOT BETWEEN full range: expected inclusion (true values may lie outside)")
	}
	// Unsaturated bounds keep strict exclusion
	if EvaluateMinMaxCondition(MinMaxIndex{Min: 10, Max: 20}, NumericGreaterThan(20)) {
		t.Fatal("unsaturated max with > max: expected exclusion")
	}
	if EvaluateMinMaxCondition(MinMaxIndex{Min: 15, Max: 15}, NumericNotEquals(15)) {
		t.Fatal("unsaturated single-point range with != that point: expected exclusion")
	}

	// End-to-end: the block containing uint64 max must survive > 0 and
	// > MaxInt64 prefilters (the true value exceeds both thresholds)
	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		config.MinMaxIndexes = []string{"value"}
	})
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "value": uint64(math.MaxUint64)},
	})

	query := NewQuery().MatchPrefilter(MinMax("value", NumericGreaterThan(0))).Build()
	results := queryRows(t, engine, query)
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("MinMax(value > 0) with uint64 max: expected exactly row 1, got %v", results)
	}

	query = NewQuery().MatchPrefilter(MinMax("value", NumericGreaterThan(math.MaxInt64))).Build()
	results = queryRows(t, engine, query)
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("MinMax(value > MaxInt64) with uint64 max: expected exactly row 1 (saturated bound), got %v", results)
	}
}

// --- Property test ---

var propertyKeys = []string{
	"plain", "a.b", "a*", "q?x", `back\slash`, "héllo", "日本語",
	"with space", "UPPER", "num0", "nested", "arr",
}

var propertyWords = []string{
	"hello", "World", "MIXED case words", "user@example.com",
	"日本語のテキスト", "emoji 😊 text", "tab\tsep", "<html>&amp;", "",
	"multi word string value", "0123", "true",
}

func randomPropertyValue(rng *rand.Rand, depth int) any {
	choice := rng.Intn(12)
	if depth >= 2 && choice >= 10 {
		choice = rng.Intn(10)
	}
	switch choice {
	case 0:
		return rng.Intn(2000) - 1000
	case 1:
		// Ints across all magnitudes, including float64-lossy ones
		return []any{
			1234567,
			int64(1) << (20 + rng.Intn(43)),
			int64(9007199254740993), // 2^53+1
			int64(math.MaxInt64),
			-int64(1) << (20 + rng.Intn(43)),
		}[rng.Intn(5)]
	case 2:
		return uint64(math.MaxUint64) - uint64(rng.Intn(1000))
	case 3:
		return rng.NormFloat64() * math.Pow(10, float64(rng.Intn(12)))
	case 4:
		return []any{1.5, -0.001, 1e21, 1500000.0, 0.0}[rng.Intn(5)]
	case 5, 6:
		return propertyWords[rng.Intn(len(propertyWords))]
	case 7:
		return rng.Intn(2) == 0
	case 8, 9:
		return nil
	case 10:
		nested := make(map[string]any)
		for i, n := 0, 1+rng.Intn(3); i < n; i++ {
			nested[propertyKeys[rng.Intn(len(propertyKeys))]] = randomPropertyValue(rng, depth+1)
		}
		return nested
	default:
		arr := make([]any, 1+rng.Intn(3))
		for i := range arr {
			arr[i] = randomPropertyValue(rng, depth+1)
		}
		return arr
	}
}

func randomPropertyRow(rng *rand.Rand) map[string]any {
	row := make(map[string]any)
	for i, n := 0, 3+rng.Intn(4); i < n; i++ {
		row[propertyKeys[rng.Intn(len(propertyKeys))]] = randomPropertyValue(rng, 0)
	}
	return row
}

// TestPropertyNoFalseNegatives generates random rows covering every known
// ingest/verify divergence class (all int magnitudes, floats, dotted,
// metachar, and unicode keys, nested maps, arrays, nulls), derives
// Field/Token/FieldToken/FieldRegex queries from the shared walker's own
// enumeration of each row, and asserts every derived query returns the row
// through a real engine flush+query cycle.
func TestPropertyNoFalseNegatives(t *testing.T) {
	const numRows = 100
	rng := rand.New(rand.NewSource(7))

	rows := make([]map[string]any, numRows)
	for i := range rows {
		row := randomPropertyRow(rng)
		row["row_id"] = fmt.Sprintf("rowid%d", i)
		rows[i] = row
	}

	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		config.MaxBufferedRows = numRows * 2
		config.MaxBufferedBytes = 512 << 20
		// Small filters keep the ~2000 queries below fast: every query re-reads
		// the file-level filters from the footer, and the default 100k-item
		// sizing makes that the dominant cost. A higher false positive rate
		// cannot cause false negatives, which is all this test asserts.
		config.FileBloomExpectedItems = 2_000
		config.MaxRowGroupRows = 2_000
		config.BloomFalsePositiveRate = 0.01
	})
	ingestAndFlush(t, engine, rows)

	containsRowID := func(results []map[string]any, rowID string) bool {
		for _, row := range results {
			if row["row_id"] == rowID {
				return true
			}
		}
		return false
	}

	type derivedQuery struct {
		description string
		query       *Query
	}

	// delimiterSplitPrefixes returns every proper prefix of path cut at a
	// delimiter occurrence, computed independently of the walker so the
	// prefix-path rule is checked from the outside rather than against the
	// walker's own output.
	delimiterSplitPrefixes := func(path, delimiter string) []string {
		var prefixes []string
		for splitAt := 0; ; {
			idx := strings.Index(path[splitAt:], delimiter)
			if idx == -1 {
				return prefixes
			}
			splitAt += idx
			if splitAt > 0 {
				prefixes = append(prefixes, path[:splitAt])
			}
			splitAt += len(delimiter)
		}
	}

	for i, row := range rows {
		rowBytes, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("Failed to marshal row %d: %v", i, err)
		}
		rowID := row["row_id"].(string)

		// Derive queries from the shared walker's own enumeration of the row
		var queries []derivedQuery
		var leaves []rowLeaf
		fieldCount, tokenCount, fieldTokenCount, regexCount := 0, 0, 0, 0
		seenPaths := make(map[string]bool)
		forEachPathValue(gjson.ParseBytes(rowBytes), ".", func(path string, value gjson.Result, isLeaf bool) {
			if !seenPaths[path] && fieldCount < 8 {
				seenPaths[path] = true
				fieldCount++
				queries = append(queries, derivedQuery{
					description: fmt.Sprintf("Field(%q)", path),
					query:       NewQuery().Field(path).Build(),
				})
			}
			if !isLeaf {
				return
			}
			text, ok := leafTokenInput(value)
			if !ok {
				return
			}
			leaves = append(leaves, rowLeaf{path: path, text: text})
			if regexCount < 3 {
				regexCount++
				pattern := "^" + regexp.QuoteMeta(text) + "$"
				queries = append(queries, derivedQuery{
					description: fmt.Sprintf("FieldRegex(%q, %q)", path, pattern),
					query:       NewQuery().FieldRegex(path, pattern).Build(),
				})
			}
			for _, token := range BasicWhitespaceLowerTokenizer(text) {
				if tokenCount < 4 {
					tokenCount++
					queries = append(queries, derivedQuery{
						description: fmt.Sprintf("Token(%q)", token),
						query:       NewQuery().Token(token).Build(),
					})
				}
				if fieldTokenCount < 4 {
					fieldTokenCount++
					queries = append(queries, derivedQuery{
						description: fmt.Sprintf("FieldToken(%q, %q)", path, token),
						query:       NewQuery().FieldToken(path, token).Build(),
					})
				}
			}
		})

		// Delimiter-split prefixes of every leaf path must be field-existence
		// paths, whether the delimiter came from nesting or from a flat dotted
		// key, and FieldRegex must match leaves beneath them. Prefixes are
		// computed independently of the walker (see delimiterSplitPrefixes) so
		// this cannot be circular.
		prefixFieldCount, prefixRegexCount := 0, 0
		for _, leaf := range leaves {
			for _, prefix := range delimiterSplitPrefixes(leaf.path, ".") {
				if !seenPaths[prefix] && prefixFieldCount < 6 {
					seenPaths[prefix] = true
					prefixFieldCount++
					queries = append(queries, derivedQuery{
						description: fmt.Sprintf("Field(%q) [prefix of %q]", prefix, leaf.path),
						query:       NewQuery().Field(prefix).Build(),
					})
				}
				if prefixRegexCount < 3 {
					prefixRegexCount++
					pattern := "^" + regexp.QuoteMeta(leaf.text) + "$"
					queries = append(queries, derivedQuery{
						description: fmt.Sprintf("FieldRegex(%q, %q) [prefix of %q]", prefix, pattern, leaf.path),
						query:       NewQuery().FieldRegex(prefix, pattern).Build(),
					})
				}
			}
		}

		for _, derived := range queries {
			results := queryRows(t, engine, derived.query)
			if !containsRowID(results, rowID) {
				t.Fatalf("false negative: row %d (%s) not returned for derived query %s; row: %s",
					i, rowID, derived.description, rowBytes)
			}
		}
	}
}
