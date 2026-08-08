package bloomsearch

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// TestCompiledMatcherEquivalence differentially tests the compiled per-query
// matcher (the engine's scan path) against the set-based reference
// (buildRowMatchSets via TestGJSONForQuery): every (row, query) pair must get
// the identical verdict from both, under the fast-path tokenizer and a custom
// one. Rows come from the property-test corpus plus handcrafted case-folding
// and structure edge cases; queries are derived from the rows (so both hit and
// miss pairs occur in bulk) plus fixed expression-tree edge cases.
//
// Carve-out: identical verdicts hold only outside the FieldToken joined-key
// collision class — the compiled matcher compares (path, token) pairs while
// the reference matches the joined "path::token" string, so paths or tokens
// containing "::" combined with multi-path FieldToken queries can legitimately
// diverge (compiled strictly stricter). This corpus therefore contains no "::"
// in keys or tokens; see TestFieldTokenJoinedKeyCollisionPinned before adding
// any.
func TestCompiledMatcherEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(11))

	var rows []map[string]any
	for i := 0; i < 50; i++ {
		rows = append(rows, randomPropertyRow(rng))
	}
	rows = append(rows,
		// Unicode case folding where lowering changes byte length: Kelvin sign
		// (U+212A → 'k') and dotted capital I (U+0130 → 'i').
		map[string]any{"fold": "\u212AELVIN \u0130stanbul MIXED case ÉÀ"},
		// Varied whitespace kinds (ASCII spaces and the non-ASCII NBSP, which
		// strings.Fields also splits on) and a float64-precision-sensitive
		// number.
		map[string]any{"ws": "tab\tsep\nnl\u00a0nbsp end", "num": 1500000},
		// Delimiter-heavy keys, leading delimiter, empty-string values.
		map[string]any{"a.b.c": "deep", ".lead": "x", "empty": "", "a": map[string]any{"b": map[string]any{"c": "nested"}}},
		// Non-object-friendly shapes: arrays of arrays, nulls, bools.
		map[string]any{"arr": []any{[]any{"inner", 1}, nil, true, map[string]any{"k": "v"}}},
	)

	rowBytesList := make([][]byte, len(rows))
	for i, row := range rows {
		rowBytes, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("failed to marshal row %d: %v", i, err)
		}
		rowBytesList[i] = rowBytes
	}

	type namedQuery struct {
		description string
		query       *Query
	}
	var queries []namedQuery
	add := func(description string, query *Query) {
		queries = append(queries, namedQuery{description, query})
	}

	// Derive queries from each row's reference enumeration; evaluating every
	// query against every row also yields plentiful negative pairs.
	for i, rowBytes := range rowBytesList {
		fieldCount, tokenCount, fieldTokenCount, regexCount := 0, 0, 0, 0
		var anyPath, anyToken string
		forEachPathValue(gjson.ParseBytes(rowBytes), ".", func(path string, value gjson.Result, isLeaf bool) {
			if fieldCount < 3 {
				fieldCount++
				anyPath = path
				add(fmt.Sprintf("row%d Field(%q)", i, path), NewQuery().Field(path).Build())
			}
			if !isLeaf {
				return
			}
			text, ok := leafTokenInput(value)
			if !ok {
				return
			}
			if regexCount < 2 {
				regexCount++
				pattern := "^" + regexp.QuoteMeta(text) + "$"
				add(fmt.Sprintf("row%d FieldRegex(%q, %q)", i, path, pattern),
					NewQuery().FieldRegex(path, pattern).Build())
			}
			for _, token := range BasicWhitespaceLowerTokenizer(text) {
				if tokenCount < 3 {
					tokenCount++
					anyToken = token
					add(fmt.Sprintf("row%d Token(%q)", i, token), NewQuery().Token(token).Build())
					add(fmt.Sprintf("row%d Token(upper %q)", i, token),
						NewQuery().Token(strings.ToUpper(token)).Build())
				}
				if fieldTokenCount < 3 {
					fieldTokenCount++
					add(fmt.Sprintf("row%d FieldToken(%q, %q)", i, path, token),
						NewQuery().FieldToken(path, token).Build())
				}
			}
		})
		// Composites mixing hit and miss legs, plus bloom+regex conjunction.
		if anyPath != "" && anyToken != "" {
			add(fmt.Sprintf("row%d And(Field,Token)", i),
				NewQuery().Match(And(Field(anyPath), Token(anyToken))).Build())
			add(fmt.Sprintf("row%d Or(missing,Token)", i),
				NewQuery().Match(Or(Field("no.such.path.anywhere"), Token(anyToken))).Build())
			add(fmt.Sprintf("row%d And(Token,missingToken)", i),
				NewQuery().Match(And(Token(anyToken), Token("zz-no-such-token"))).Build())
			add(fmt.Sprintf("row%d bloom+regex", i),
				NewQuery().Field(anyPath).FieldRegex(anyPath, ".").Build())
			add(fmt.Sprintf("row%d bloom+missing regex", i),
				NewQuery().Token(anyToken).FieldRegex(anyPath, "\\Azzz-never-matches\\z").Build())
		}
	}

	// Fixed expression-tree edge cases.
	add("match-all", NewQuery().Build())
	add("empty Or", NewQuery().Match(Or()).Build())
	add("empty And", NewQuery().Match(And()).Build())
	add("empty regex Or", NewQuery().MatchRegex(RegexOr()).Build())
	add("Field empty path", NewQuery().Field("").Build())
	add("Token empty", NewQuery().Token("").Build())
	add("FieldToken empty", NewQuery().FieldToken("", "").Build())
	add("FieldRegex empty field", NewQuery().FieldRegex("", ".*").Build())
	add("FieldRegex missing field", NewQuery().FieldRegex("no.such.path", ".*").Build())
	add("unknown bloom condition type", &Query{
		Bloom: &BloomQuery{Expression: &BloomExpression{
			ExpressionType: BloomExpressionCondition,
			Condition:      &BloomCondition{Type: "BOGUS", Field: "a"},
		}},
	})
	add("nested Or(And(...),FieldToken)", NewQuery().Match(
		Or(And(Field("a"), Token("deep")), FieldToken("a.b.c", "deep")),
	).Build())

	customTokenizer := func(value string) []string { return strings.Split(value, " ") }
	tokenizers := []struct {
		name string
		fn   ValueTokenizerFunc
	}{
		{"BasicWhitespaceLowerTokenizer", BasicWhitespaceLowerTokenizer},
		{"customSpaceSplit", customTokenizer},
	}

	for _, tok := range tokenizers {
		for _, nq := range queries {
			compiledRegex, err := CompileRegexQuery(nq.query.Regex)
			if err != nil {
				t.Fatalf("[%s] %s: failed to compile regex query: %v", tok.name, nq.description, err)
			}
			matcher := compileRowMatcher(nq.query.Bloom, compiledRegex, ".", tok.fn)
			scratch := newRowMatchScratch(matcher)

			for i, rowBytes := range rowBytesList {
				want := TestGJSONForQuery(gjson.ParseBytes(rowBytes), nq.query.Bloom, compiledRegex, ".", tok.fn)
				got := matcher.matchRowBytes(rowBytes, scratch)
				if got != want {
					t.Fatalf("[%s] verdict mismatch for %s on row %d:\ncompiled=%v reference=%v\nrow: %s",
						tok.name, nq.description, i, got, want, rowBytes)
				}
			}
		}
	}
}

// TestFieldTokenJoinedKeyCollisionPinned pins the one deliberate verdict
// divergence between the compiled matcher and the set-based reference: the
// reference stores FieldToken evidence as the joined "path::token" string, so
// when a query targets multiple field paths, a path or token that itself
// contains "::" can collide with a different (path, token) pair and the
// reference accepts a row the exact-path semantics reject. The compiled
// matcher compares the (path, token) pair directly — strictly stricter, and
// only in this collision class — which removes that false-positive without
// ever losing a true match. The engine's end-to-end verdict must be the
// compiled one.
//
// TestCompiledMatcherEquivalence relies on this class being absent from its
// corpus: its keys/tokens contain no "::", and any corpus change introducing
// "::" keys or tokens combined with multi-path FieldToken queries must extend
// that test's carve-out rather than expecting identical verdicts here.
func TestFieldTokenJoinedKeyCollisionPinned(t *testing.T) {
	// Row with a leaf at path "a::b" whose token is "c". The reference indexes
	// it (once "a::b" is a targeted FieldToken path) under the joined key
	// "a::b::c" — exactly the key FieldToken("a", "b::c") looks up.
	rowBytes := []byte(`{"a::b":"c"}`)
	collision := NewQuery().Match(Or(
		FieldToken("a", "b::c"),      // no leaf at path "a": must not match
		FieldToken("a::b", "zzzzzz"), // targets path "a::b" so the reference tokenizes that leaf
	)).Build()

	// The reference accepts via the joined-key collision.
	if !TestGJSONForQuery(gjson.ParseBytes(rowBytes), collision.Bloom, nil, ".", BasicWhitespaceLowerTokenizer) {
		t.Fatal("reference no longer exhibits the joined-key collision; update the carve-out documentation if this is intentional")
	}
	// The compiled matcher compares (path, token) pairs and rejects.
	matcher := compileRowMatcher(collision.Bloom, nil, ".", BasicWhitespaceLowerTokenizer)
	if matcher.matchRowBytes(rowBytes, newRowMatchScratch(matcher)) {
		t.Fatal("compiled matcher accepted the joined-key collision; exact-path FieldToken semantics regressed")
	}

	// End to end, the engine's verdict is the compiled one.
	engine := newTestEngine(t, nil)
	ingestAndFlush(t, engine, []map[string]any{{"id": 1, "a::b": "c"}})

	results := queryRows(t, engine, collision)
	if len(results) != 0 {
		t.Fatalf("collision query: expected no rows (exact-path FieldToken semantics), got %v", results)
	}

	// Control: the genuine (path, token) pair still matches, proving the
	// rejection above is the collision, not the "::" key itself.
	results = queryRows(t, engine, NewQuery().FieldToken("a::b", "c").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldToken(a::b, c): expected exactly row 1, got %v", results)
	}
}

// TestMatchedRowNoAliasing guards the unsafe-view design of the scan path:
// matching parses a zero-copy view of the block buffer, but delivered maps
// must be materialized from an independent copy. Rows are scanned and
// materialized exactly as processDataBlock does (blockRowScanner →
// matchRowBytes → materializeRow), then the entire block buffer is overwritten
// (simulating release/reuse) and the delivered maps must be unchanged.
func TestMatchedRowNoAliasing(t *testing.T) {
	rows := []map[string]any{
		{"id": "r1", "msg": "keepme alpha", "nested": map[string]any{"s": "inner string", "n": 42}},
		{"id": "r2", "msg": "dropme"},
		{"id": "r3", "msg": "keepme beta", "arr": []any{"one", 2, nil, map[string]any{"k": "v"}}},
	}

	var blockBuf []byte
	var lengthBytes [LengthPrefixSize]byte
	var expected []map[string]any
	for _, row := range rows {
		rowBytes, err := json.Marshal(row)
		if err != nil {
			t.Fatalf("failed to marshal row: %v", err)
		}
		binary.LittleEndian.PutUint32(lengthBytes[:], uint32(len(rowBytes)))
		blockBuf = append(blockBuf, lengthBytes[:]...)
		blockBuf = append(blockBuf, rowBytes...)

		if strings.Contains(row["msg"].(string), "keepme") {
			var want map[string]any
			if err := json.Unmarshal(rowBytes, &want); err != nil {
				t.Fatalf("failed to unmarshal row: %v", err)
			}
			expected = append(expected, want)
		}
	}

	query := NewQuery().Token("keepme").Build()
	matcher := compileRowMatcher(query.Bloom, nil, ".", BasicWhitespaceLowerTokenizer)
	scratch := newRowMatchScratch(matcher)

	var delivered []map[string]any
	scanner := blockRowScanner{data: blockBuf}
	for {
		rowBytes, ok, err := scanner.next()
		if err != nil {
			t.Fatalf("scanner error: %v", err)
		}
		if !ok {
			break
		}
		if !matcher.matchRowBytes(rowBytes, scratch) {
			continue
		}
		row, err := materializeRow(rowBytes)
		if err != nil {
			t.Fatalf("materializeRow failed: %v", err)
		}
		delivered = append(delivered, row)
	}

	if len(delivered) != len(expected) {
		t.Fatalf("expected %d delivered rows, got %d", len(expected), len(delivered))
	}

	// Simulate the block buffer being released and its memory reused.
	for i := range blockBuf {
		blockBuf[i] = 0xFF
	}

	for i := range expected {
		if !reflect.DeepEqual(delivered[i], expected[i]) {
			t.Fatalf("delivered row %d changed after block buffer overwrite:\n got: %#v\nwant: %#v",
				i, delivered[i], expected[i])
		}
	}
}

// TestCustomTokenizerStillWorks exercises the non-fast-path tokenizer end to
// end: a tokenizer that splits on '|' and preserves case must drive both
// ingest indexing and query-time verification, with its output compared
// verbatim (no lowercasing, no whitespace splitting).
func TestCustomTokenizerStillWorks(t *testing.T) {
	pipeTokenizer := func(value string) []string { return strings.Split(value, "|") }
	if isBasicWhitespaceLowerTokenizer(pipeTokenizer) {
		t.Fatal("custom tokenizer misidentified as the built-in fast-path tokenizer")
	}

	engine := newTestEngine(t, func(config *BloomSearchEngineConfig) {
		config.Tokenizer = pipeTokenizer
	})
	ingestAndFlush(t, engine, []map[string]any{
		{"id": 1, "tags": "Alpha|Beta gamma"},
		{"id": 2, "tags": "other"},
	})

	// Exact-case token from the custom tokenizer's output.
	results := queryRows(t, engine, NewQuery().Token("Alpha").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("Token(Alpha): expected exactly row 1, got %v", results)
	}

	// Lowercased target must miss: the custom tokenizer does not fold case.
	results = queryRows(t, engine, NewQuery().Token("alpha").Build())
	if len(results) != 0 {
		t.Fatalf("Token(alpha): expected no rows with case-preserving tokenizer, got %v", results)
	}

	// A token containing whitespace proves tokens are split on '|' only.
	results = queryRows(t, engine, NewQuery().FieldToken("tags", "Beta gamma").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldToken(tags, Beta gamma): expected exactly row 1, got %v", results)
	}
	results = queryRows(t, engine, NewQuery().Token("Beta").Build())
	if len(results) != 0 {
		t.Fatalf("Token(Beta): expected no rows (tokens split on '|', not whitespace), got %v", results)
	}

	// Regex conditions are tokenizer-independent and must still work.
	results = queryRows(t, engine, NewQuery().FieldRegex("tags", "Beta").Build())
	if len(results) != 1 || !resultIDs(results)[1] {
		t.Fatalf("FieldRegex(tags, Beta): expected exactly row 1, got %v", results)
	}
}
