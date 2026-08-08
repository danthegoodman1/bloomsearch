package bloomsearch

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

// The functions in this file define the single canonical representation of a
// row for indexing and matching: the row's marshaled JSON bytes, enumerated by
// forEachPathValue. Ingest indexing (bloom filter population) and query-time
// row verification both walk rows through this one implementation, which makes
// disagreement between them — the source of every false negative — impossible
// by construction.
//
// Field path semantics:
//   - Paths are object keys joined with the delimiter. Keys are always treated
//     literally; gjson path syntax (`*`, `?`, `\`, `.`-traversal) is never
//     applied to them.
//   - Array elements contribute under the array's own path; indices are ignored.
//   - Intermediate object/array paths are field-existence entries, so
//     Field("user") matches {"user": {"name": "x"}}.
//   - FieldToken is exact-path: FieldToken("user", "john") does not match
//     {"user": {"name": "john"}} (use FieldToken("user.name", "john") or
//     Token("john")).
//   - A key containing the delimiter collides with the equivalent nested path:
//     {"a.b": 1} and {"a": {"b": 1}} both produce the path "a.b" and both match
//     Field("a.b"). To keep that symmetry complete, every delimiter-split
//     prefix of every path is also a field-existence path — the flat form
//     emits "a" just as the nested form does via its intermediate container —
//     so Field("a") and the FieldRegex("a", ...) bloom guard treat both forms
//     identically. This is accepted and intentional.
//   - null values contribute field existence but no tokens.

// forEachPathValue enumerates every (path, value) pair in a gjson-parsed JSON
// document, using ForEach traversal only so keys are always literal. emit is
// called with isLeaf=true for primitive values (string, number, bool, null)
// and isLeaf=false for object/array container paths and delimiter-split prefix
// paths (see emitKeyPrefixPaths). The root itself has no path and is not
// emitted; keys that produce an empty path (a top-level "" key) are skipped
// identically at ingest and verification, and every condition type treats an
// empty field path as nonexistent (Field/FieldToken find no entry, regex
// matching rejects it explicitly). Emitted paths may repeat within a row;
// consumers are set/bloom inserts, which are idempotent.
func forEachPathValue(value gjson.Result, delimiter string, emit func(path string, value gjson.Result, isLeaf bool)) {
	walkPathValues(value, "", delimiter, emit)
}

func walkPathValues(value gjson.Result, path, delimiter string, emit func(path string, value gjson.Result, isLeaf bool)) {
	if value.IsObject() {
		if path != "" {
			emit(path, value, false)
		}
		value.ForEach(func(key, child gjson.Result) bool {
			keyStr := key.String()
			childPath := keyStr
			if path != "" {
				childPath = path + delimiter + keyStr
			}
			emitKeyPrefixPaths(path, keyStr, delimiter, emit)
			walkPathValues(child, childPath, delimiter, emit)
			return true
		})
	} else if value.IsArray() {
		if path != "" {
			emit(path, value, false)
		}
		value.ForEach(func(_, child gjson.Result) bool {
			// Array elements contribute under the array's own path
			walkPathValues(child, path, delimiter, emit)
			return true
		})
	} else if path != "" {
		emit(path, value, true)
	}
}

// emitKeyPrefixPaths emits every delimiter-split prefix of a key as a
// field-existence path. A key containing the delimiter collides with the
// equivalent nested form ({"a.b": 1} and {"a": {"b": 1}} produce the same
// paths), and the nested form emits "a" as an intermediate container path —
// so the flat form must produce the same field-existence entries, or
// Field("a") and the FieldRegex("a", ...) bloom guard would prune rows the
// row-level matchers match. Prefixes contributed by ancestor keys are emitted
// by their own levels of the walk, so only the key's internal delimiter
// positions are split here.
func emitKeyPrefixPaths(parentPath, key, delimiter string, emit func(path string, value gjson.Result, isLeaf bool)) {
	if delimiter == "" {
		return
	}
	splitAt := 0
	for {
		idx := strings.Index(key[splitAt:], delimiter)
		if idx == -1 {
			return
		}
		splitAt += idx
		prefixPath := key[:splitAt]
		if parentPath != "" {
			prefixPath = parentPath + delimiter + prefixPath
		}
		if prefixPath != "" {
			emit(prefixPath, gjson.Result{}, false)
		}
		splitAt += len(delimiter)
	}
}

// leafTokenInput returns the canonical text of a primitive leaf that tokenizers
// and regex filters operate on: the decoded string for strings, the raw JSON
// literal for numbers (e.g. "9007199254740993" exactly as marshaled, never a
// float64 round-trip), and "true"/"false" for booleans. null has no token text
// (field existence only), reported via ok=false.
func leafTokenInput(value gjson.Result) (text string, ok bool) {
	switch value.Type {
	case gjson.String:
		return value.Str, true
	case gjson.Number:
		return value.Raw, true
	case gjson.True:
		return "true", true
	case gjson.False:
		return "false", true
	default:
		return "", false
	}
}

// ValueTokenizerFunc tokenizes the canonical text of a field value (see
// leafTokenInput) into a list of tokens.
type ValueTokenizerFunc func(value string) []string

// BasicWhitespaceLowerTokenizer tokenizes values by splitting on whitespace and
// converting to lowercase.
func BasicWhitespaceLowerTokenizer(value string) []string {
	return strings.Fields(strings.ToLower(value))
}

var _ ValueTokenizerFunc = BasicWhitespaceLowerTokenizer

// rowQueryNeeds lists the per-row sets a query's conditions read, so
// buildRowMatchSets only builds those. Each flag corresponds one-to-one to a
// condition type: Field reads the paths set, Token the token set, FieldToken
// the field:token set (only for its target paths), and regex conditions the
// raw leaf list. A set no condition reads is never consulted, so skipping it
// cannot change an outcome. The row is fully walked either way.
type rowQueryNeeds struct {
	// fields indicates a Field condition tests the path set
	fields bool
	// tokens indicates a Token condition tests the row-wide token set
	tokens bool
	// fieldTokenPaths holds the exact leaf paths targeted by FieldToken
	// conditions; only leaves at these paths need tokenizing for the
	// field:token set
	fieldTokenPaths map[string]struct{}
	// leaves indicates regex conditions need the raw leaf values
	leaves bool
}

func bloomExpressionNeeds(expression *BloomExpression, needs *rowQueryNeeds) {
	if expression == nil {
		return
	}
	if expression.condition != nil {
		switch expression.condition.Type {
		case BloomField:
			needs.fields = true
		case BloomToken:
			needs.tokens = true
		case BloomFieldToken:
			if needs.fieldTokenPaths == nil {
				needs.fieldTokenPaths = make(map[string]struct{})
			}
			needs.fieldTokenPaths[expression.condition.Field] = struct{}{}
		}
	}
	for i := range expression.children {
		bloomExpressionNeeds(&expression.children[i], needs)
	}
}

// rowLeaf is one primitive leaf of a row: its exact path and canonical text.
type rowLeaf struct {
	path string
	text string
}

// rowMatchSets is a row's enumeration under the shared walker, in the same
// shape the bloom filters were populated from at ingest: every path (including
// intermediates), tokens from primitive leaves, and exact-leaf-path::token
// pairs. Expressions are evaluated against these sets.
type rowMatchSets struct {
	paths       map[string]struct{}
	tokens      map[string]struct{}
	fieldTokens map[string]struct{}
	leaves      []rowLeaf
}

// buildRowMatchSets enumerates a parsed row once via the shared walker.
// tokenizer may be nil when needs requests no token sets.
func buildRowMatchSets(value gjson.Result, delimiter string, tokenizer ValueTokenizerFunc, needs rowQueryNeeds) *rowMatchSets {
	sets := &rowMatchSets{}
	if needs.fields {
		sets.paths = make(map[string]struct{})
	}
	if needs.tokens {
		sets.tokens = make(map[string]struct{})
	}
	if needs.fieldTokenPaths != nil {
		sets.fieldTokens = make(map[string]struct{})
	}

	forEachPathValue(value, delimiter, func(path string, v gjson.Result, isLeaf bool) {
		if needs.fields {
			sets.paths[path] = struct{}{}
		}
		if !isLeaf {
			return
		}
		text, ok := leafTokenInput(v)
		if !ok {
			return
		}
		if needs.leaves {
			sets.leaves = append(sets.leaves, rowLeaf{path: path, text: text})
		}
		_, wantFieldTokens := needs.fieldTokenPaths[path]
		if !needs.tokens && !wantFieldTokens {
			return
		}
		for _, token := range tokenizer(text) {
			if needs.tokens {
				sets.tokens[token] = struct{}{}
			}
			if wantFieldTokens {
				sets.fieldTokens[makeFieldTokenKey(path, token)] = struct{}{}
			}
		}
	})

	return sets
}

func (s *rowMatchSets) matchesBloomCondition(condition *BloomCondition) bool {
	switch condition.Type {
	case BloomField:
		_, ok := s.paths[condition.Field]
		return ok
	case BloomToken:
		_, ok := s.tokens[condition.Token]
		return ok
	case BloomFieldToken:
		_, ok := s.fieldTokens[makeFieldTokenKey(condition.Field, condition.Token)]
		return ok
	default:
		return false
	}
}

func (s *rowMatchSets) matchesBloomExpression(expression *BloomExpression) bool {
	if expression == nil {
		return true
	}

	switch expression.expressionType {
	case bloomExpressionCondition:
		if expression.condition == nil {
			return true
		}
		return s.matchesBloomCondition(expression.condition)
	case bloomExpressionOr:
		if len(expression.children) == 0 {
			return false
		}
		for i := range expression.children {
			if s.matchesBloomExpression(&expression.children[i]) {
				return true
			}
		}
		return false
	case bloomExpressionAnd:
		for i := range expression.children {
			if !s.matchesBloomExpression(&expression.children[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// matchesRegexCondition tests the pattern against the canonical text of every
// leaf at or beneath the condition's field path (decoded string for strings,
// raw literal for numbers/bools; null values are never regex-matched). An
// empty field path matches nothing, mirroring the walker's empty-path skip —
// otherwise the prefix rule would match leaves under leading-delimiter keys
// that the bloom field guard (which never sees an empty path) prunes.
func (s *rowMatchSets) matchesRegexCondition(condition *compiledRegexCondition, delimiter string) bool {
	if condition == nil {
		return true
	}
	if condition.field == "" {
		return false
	}
	prefix := condition.field + delimiter
	for i := range s.leaves {
		leaf := &s.leaves[i]
		if leaf.path != condition.field && !strings.HasPrefix(leaf.path, prefix) {
			continue
		}
		if condition.pattern.MatchString(leaf.text) {
			return true
		}
	}
	return false
}

func (s *rowMatchSets) matchesRegexExpression(expression *compiledRegexExpression, delimiter string) bool {
	if expression == nil {
		return true
	}

	switch expression.expressionType {
	case regexExpressionCondition:
		if expression.condition == nil {
			return true
		}
		return s.matchesRegexCondition(expression.condition, delimiter)
	case regexExpressionOr:
		if len(expression.children) == 0 {
			return false
		}
		for i := range expression.children {
			if s.matchesRegexExpression(&expression.children[i], delimiter) {
				return true
			}
		}
		return false
	case regexExpressionAnd:
		for i := range expression.children {
			if !s.matchesRegexExpression(&expression.children[i], delimiter) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// TestJSONForField tests if a field path exists in JSON (including
// intermediate object/array paths).
func TestJSONForField(jsonStr, fieldPath, delimiter string) bool {
	return TestGJSONForField(gjson.Parse(jsonStr), fieldPath, delimiter)
}

// TestJSONForToken tests if any primitive leaf value tokenizes to the token.
func TestJSONForToken(jsonStr, token string, tokenizer ValueTokenizerFunc) bool {
	return TestGJSONForToken(gjson.Parse(jsonStr), token, tokenizer)
}

// TestJSONForFieldToken tests if a primitive leaf at exactly the field path
// tokenizes to the token.
func TestJSONForFieldToken(jsonStr, fieldPath, delimiter, token string, tokenizer ValueTokenizerFunc) bool {
	return TestGJSONForFieldToken(gjson.Parse(jsonStr), fieldPath, delimiter, token, tokenizer)
}

// TestGJSONForField is TestJSONForField over a pre-parsed value.
func TestGJSONForField(value gjson.Result, fieldPath, delimiter string) bool {
	sets := buildRowMatchSets(value, delimiter, nil, rowQueryNeeds{fields: true})
	_, ok := sets.paths[fieldPath]
	return ok
}

// TestGJSONForToken is TestJSONForToken over a pre-parsed value.
func TestGJSONForToken(value gjson.Result, token string, tokenizer ValueTokenizerFunc) bool {
	sets := buildRowMatchSets(value, ".", tokenizer, rowQueryNeeds{tokens: true})
	_, ok := sets.tokens[token]
	return ok
}

// TestGJSONForFieldToken is TestJSONForFieldToken over a pre-parsed value.
func TestGJSONForFieldToken(value gjson.Result, fieldPath, delimiter, token string, tokenizer ValueTokenizerFunc) bool {
	sets := buildRowMatchSets(value, delimiter, tokenizer, rowQueryNeeds{
		fieldTokenPaths: map[string]struct{}{fieldPath: {}},
	})
	_, ok := sets.fieldTokens[makeFieldTokenKey(fieldPath, token)]
	return ok
}

// TestJSONForBloomCondition tests a JSON document against a single bloom condition.
func TestJSONForBloomCondition(jsonBytes []byte, condition *BloomCondition, delimiter string, tokenizer ValueTokenizerFunc) bool {
	if condition == nil {
		return true
	}
	expression := &BloomExpression{
		expressionType: bloomExpressionCondition,
		condition:      condition,
	}
	return TestJSONForBloomQuery(jsonBytes, &BloomQuery{Expression: expression}, delimiter, tokenizer)
}

// TestJSONForBloomQuery tests a JSON document against a bloom query.
func TestJSONForBloomQuery(jsonBytes []byte, bloomQuery *BloomQuery, delimiter string, tokenizer ValueTokenizerFunc) bool {
	return TestGJSONForQuery(gjson.ParseBytes(jsonBytes), bloomQuery, nil, delimiter, tokenizer)
}

type compiledRegexCondition struct {
	field   string
	pattern *regexp.Regexp
}

type compiledRegexExpression struct {
	expressionType RegexExpressionType
	condition      *compiledRegexCondition
	children       []compiledRegexExpression
}

type compiledRegexQuery struct {
	expression *compiledRegexExpression
}

func CompileRegexQuery(regexQuery *RegexQuery) (*compiledRegexQuery, error) {
	if regexQuery == nil || regexQuery.Expression == nil {
		return nil, nil
	}

	expression, err := compileRegexExpression(regexQuery.Expression)
	if err != nil {
		return nil, err
	}
	return &compiledRegexQuery{expression: expression}, nil
}

func compileRegexExpression(expression *RegexExpression) (*compiledRegexExpression, error) {
	if expression == nil {
		return nil, nil
	}

	switch expression.expressionType {
	case regexExpressionCondition:
		if expression.condition == nil {
			return nil, nil
		}
		compiledPattern, err := regexp.Compile(expression.condition.Pattern)
		if err != nil {
			return nil, err
		}
		return &compiledRegexExpression{
			expressionType: regexExpressionCondition,
			condition: &compiledRegexCondition{
				field:   expression.condition.Field,
				pattern: compiledPattern,
			},
		}, nil
	case regexExpressionAnd, regexExpressionOr:
		children := make([]compiledRegexExpression, 0, len(expression.children))
		for i := range expression.children {
			child, err := compileRegexExpression(&expression.children[i])
			if err != nil {
				return nil, err
			}
			if child != nil {
				children = append(children, *child)
			}
		}
		return &compiledRegexExpression{
			expressionType: expression.expressionType,
			children:       children,
		}, nil
	default:
		return nil, fmt.Errorf("unknown regex expression type: %s", expression.expressionType)
	}
}

// TestGJSONForQuery tests a parsed row against a query's bloom and regex
// expressions. The row is enumerated once through the shared walker into
// per-row match sets, then each expression tree is evaluated against them.
func TestGJSONForQuery(value gjson.Result, bloomQuery *BloomQuery, regexQuery *compiledRegexQuery, delimiter string, tokenizer ValueTokenizerFunc) bool {
	var bloomExpression *BloomExpression
	if bloomQuery != nil {
		bloomExpression = bloomQuery.Expression
	}
	var regexExpression *compiledRegexExpression
	if regexQuery != nil {
		regexExpression = regexQuery.expression
	}
	if bloomExpression == nil && regexExpression == nil {
		return true
	}

	var needs rowQueryNeeds
	bloomExpressionNeeds(bloomExpression, &needs)
	needs.leaves = regexExpression != nil

	sets := buildRowMatchSets(value, delimiter, tokenizer, needs)
	if bloomExpression != nil && !sets.matchesBloomExpression(bloomExpression) {
		return false
	}
	if regexExpression != nil && !sets.matchesRegexExpression(regexExpression, delimiter) {
		return false
	}
	return true
}
