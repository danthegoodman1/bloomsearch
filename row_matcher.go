package bloomsearch

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/tidwall/gjson"
)

// This file is the allocation-free row-verification path: a per-query compiled
// matcher evaluated in a single walk per row. It must produce exactly the
// verdicts of the set-based reference in tokenizer.go (buildRowMatchSets +
// matchesBloomExpression/matchesRegexExpression over forEachPathValue's
// enumeration); TestCompiledMatcherEquivalence asserts that differentially and
// TestPropertyNoFalseNegatives exercises it end-to-end through the engine.

// unsafeString returns a string view of b without copying. Safe only while b
// is never mutated for the lifetime of every reference to the returned string
// (including substrings gjson hands out); the GC tracks b's backing array
// through the string header, so lifetime itself needs no care beyond that.
func unsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// isBasicWhitespaceLowerTokenizer reports whether fn is this package's
// BasicWhitespaceLowerTokenizer, by code-pointer comparison. This gates the
// zero-alloc token fast path (forEachWord + appendFoldedWord), which
// reproduces strings.Fields(strings.ToLower(v)) exactly; any other tokenizer
// keeps the allocate-and-compare path using its own output. A false positive
// would require a distinct function sharing the code pointer, which the
// toolchain only produces for behaviorally identical functions.
func isBasicWhitespaceLowerTokenizer(fn ValueTokenizerFunc) bool {
	return fn != nil &&
		reflect.ValueOf(fn).Pointer() == reflect.ValueOf(ValueTokenizerFunc(BasicWhitespaceLowerTokenizer)).Pointer()
}

// pathWalker enumerates a parsed row exactly like forEachPathValue but passes
// each path as a transient view into a reusable buffer instead of building a
// string per level, and supports early exit (emit returning false unwinds the
// whole walk). The two implementations must stay emission-for-emission
// identical — same paths (buffer content ≡ the delimiter-joined path string),
// same isLeaf flags, same empty-path skips, same delimiter-split key prefix
// paths — since ingest indexing and query verification walk rows through this
// one and the reference/property tests derive expectations from the other.
// Path views are valid only during the emit call.
type pathWalker struct {
	buf []byte
}

// walk enumerates value. It returns false if emit stopped the walk early.
func (w *pathWalker) walk(value gjson.Result, delimiter string, emit func(path []byte, value gjson.Result, isLeaf bool) bool) bool {
	w.buf = w.buf[:0]
	return w.walkValue(value, delimiter, emit)
}

func (w *pathWalker) walkValue(value gjson.Result, delimiter string, emit func(path []byte, value gjson.Result, isLeaf bool) bool) bool {
	cont := true
	if value.IsObject() {
		if len(w.buf) != 0 && !emit(w.buf, value, false) {
			return false
		}
		value.ForEach(func(key, child gjson.Result) bool {
			keyStr := key.String()
			if !w.emitKeyPrefixPaths(keyStr, delimiter, emit) {
				cont = false
				return false
			}
			prevLen := len(w.buf)
			if prevLen != 0 {
				w.buf = append(w.buf, delimiter...)
			}
			w.buf = append(w.buf, keyStr...)
			if !w.walkValue(child, delimiter, emit) {
				cont = false
			}
			w.buf = w.buf[:prevLen]
			return cont
		})
		return cont
	}
	if value.IsArray() {
		if len(w.buf) != 0 && !emit(w.buf, value, false) {
			return false
		}
		value.ForEach(func(_, child gjson.Result) bool {
			// Array elements contribute under the array's own path
			if !w.walkValue(child, delimiter, emit) {
				cont = false
			}
			return cont
		})
		return cont
	}
	if len(w.buf) != 0 {
		return emit(w.buf, value, true)
	}
	return true
}

// emitKeyPrefixPaths is emitKeyPrefixPaths (tokenizer.go) over the buffer:
// every delimiter-split prefix of key becomes a field-existence emission,
// skipping prefixes that would produce an empty path. The buffer is restored
// to the parent path before returning.
func (w *pathWalker) emitKeyPrefixPaths(key, delimiter string, emit func(path []byte, value gjson.Result, isLeaf bool) bool) bool {
	if delimiter == "" || !strings.Contains(key, delimiter) {
		return true
	}
	parentLen := len(w.buf)
	splitAt := 0
	for {
		idx := strings.Index(key[splitAt:], delimiter)
		if idx == -1 {
			w.buf = w.buf[:parentLen]
			return true
		}
		splitAt += idx
		w.buf = w.buf[:parentLen]
		if parentLen != 0 {
			w.buf = append(w.buf, delimiter...)
		}
		w.buf = append(w.buf, key[:splitAt]...)
		if len(w.buf) != 0 {
			if !emit(w.buf, gjson.Result{}, false) {
				w.buf = w.buf[:parentLen]
				return false
			}
		}
		splitAt += len(delimiter)
	}
}

// forEachWord calls fn for each whitespace-separated word of text, with word
// boundaries identical to strings.Fields (runs of unicode.IsSpace runes;
// unicode.ToLower never changes a rune's IsSpace classification, so splitting
// before or after lowering yields the same words). Words are substrings of
// text. fn returning false stops the scan.
func forEachWord(text string, fn func(word string) bool) {
	i := 0
	for i < len(text) {
		// Skip leading space
		if c := text[i]; c < utf8.RuneSelf {
			if asciiSpace(c) {
				i++
				continue
			}
		} else {
			r, size := utf8.DecodeRuneInString(text[i:])
			if unicode.IsSpace(r) {
				i += size
				continue
			}
		}
		start := i
		for i < len(text) {
			if c := text[i]; c < utf8.RuneSelf {
				if asciiSpace(c) {
					break
				}
				i++
			} else {
				r, size := utf8.DecodeRuneInString(text[i:])
				if unicode.IsSpace(r) {
					break
				}
				i += size
			}
		}
		if !fn(text[start:i]) {
			return
		}
	}
}

func asciiSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

// appendFoldedWord appends strings.ToLower(word) to dst without the
// intermediate string: per-rune unicode.ToLower with the same ASCII fast path
// and the same invalid-UTF-8 handling (each invalid byte becomes
// utf8.RuneError, exactly as strings.Map re-encodes it).
func appendFoldedWord(dst []byte, word string) []byte {
	for i := 0; i < len(word); {
		if c := word[i]; c < utf8.RuneSelf {
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			dst = append(dst, c)
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(word[i:])
		i += size
		dst = utf8.AppendRune(dst, unicode.ToLower(r))
	}
	return dst
}

// rowConditionKind discriminates the leaf condition kinds a compiled matcher
// evaluates during the walk.
type rowConditionKind uint8

const (
	rowCondField rowConditionKind = iota
	rowCondToken
	rowCondFieldToken
	rowCondRegex
)

// rowCondition is one leaf condition of a compiled matcher, with its targets
// pre-split/pre-materialized so per-row evaluation compares without building
// keys or lowering targets.
type rowCondition struct {
	kind rowConditionKind

	// field is the exact target path for rowCondField/rowCondFieldToken and
	// the root path for rowCondRegex (never empty for rowCondRegex; an
	// empty-field regex condition compiles to a constant-false node).
	field string
	// fieldWithDelim is field+delimiter, precomputed for the regex
	// at-or-beneath prefix test.
	fieldWithDelim string
	// token is the target token for rowCondToken/rowCondFieldToken, compared
	// verbatim against tokenizer output (the reference semantics: targets are
	// never normalized).
	token   string
	pattern *regexp.Regexp
}

// matcherNodeKind discriminates compiled expression tree nodes.
type matcherNodeKind uint8

const (
	matcherNodeTrue matcherNodeKind = iota
	matcherNodeFalse
	matcherNodeCond
	matcherNodeAnd
	matcherNodeOr
)

// matcherNode is one node of the compiled expression tree, mirroring the
// reference evaluators' semantics exactly: nil expressions and nil conditions
// are constant true, empty Or (and unknown expression/condition types) are
// constant false, And over its children is true when all children are (so an
// empty And is true).
type matcherNode struct {
	kind     matcherNodeKind
	cond     int // index into compiledRowMatcher.conditions for matcherNodeCond
	children []matcherNode
}

func evalMatcherNode(n *matcherNode, sat []bool) bool {
	switch n.kind {
	case matcherNodeTrue:
		return true
	case matcherNodeCond:
		return sat[n.cond]
	case matcherNodeAnd:
		for i := range n.children {
			if !evalMatcherNode(&n.children[i], sat) {
				return false
			}
		}
		return true
	case matcherNodeOr:
		for i := range n.children {
			if evalMatcherNode(&n.children[i], sat) {
				return true
			}
		}
		return false
	default: // matcherNodeFalse
		return false
	}
}

// compiledRowMatcher is a query's row-verification program, compiled once per
// Query and immutable afterwards (safe for concurrent use; per-scan mutable
// state lives in rowMatchScratch). A row matches when the root expression —
// the AND of the query's bloom and regex trees — is true over the condition
// verdicts produced by one shared-walker enumeration of the row.
//
// Equivalence to the set-based reference (tokenizer.go): every condition's
// satisfaction predicate quantifies over the same enumeration —
//
//   - Field: some emitted path (containers, leaves, and delimiter-split key
//     prefixes alike) equals the target;
//   - Token: some word of some leaf's canonical text (leafTokenInput) folds to
//     the target (fold ≡ strings.ToLower; words ≡ strings.Fields), or for a
//     custom tokenizer, some tokenizer output equals the target;
//   - FieldToken: a leaf at exactly the target path yields the target token.
//     The (path, token) pair is compared directly rather than through the
//     joined "path::token" key, which is strictly stricter only when a path or
//     token itself contains "::" (a joined-key collision the reference would
//     accept); direct comparison is the documented exact-path semantics and
//     cannot lose a truly matching row;
//   - Regex: the pattern matches the canonical text of a leaf at or beneath
//     the target path.
//
// Conditions only flip false→true during the walk and the tree has no
// negation, so evaluation is monotone: the walk exits early once the root is
// true, and conditions still undecided at the end are false — exactly the
// set-membership outcome. Regex patterns run lazily after the walk, and only
// if satisfying every regex condition could still change the verdict,
// mirroring the reference's bloom-then-regex evaluation order.
type compiledRowMatcher struct {
	conditions []rowCondition
	root       matcherNode
	delimiter  string
	tokenizer  ValueTokenizerFunc
	fastTokens bool

	// matchesAll: the root is true with no conditions satisfied (e.g. no
	// bloom/regex expressions); every row matches without walking.
	matchesAll bool
	// neverMatches: the root is false even with every condition satisfied
	// (e.g. an empty Or); no row can match.
	neverMatches bool

	// Condition indices by kind, so the walk skips whole categories cheaply.
	fieldConds    []int // rowCondField: tested against every emitted path
	tokenishConds []int // rowCondToken + rowCondFieldToken: tested at leaves
	regexConds    []int // rowCondRegex: leaf texts collected, patterns run lazily
}

// rowMatchScratch is the per-scan mutable state of a compiled matcher. Not
// safe for concurrent use; each query worker owns one.
type rowMatchScratch struct {
	sat          []bool
	walker       pathWalker
	tokenTargets []int
	foldBuf      []byte
	// regexTexts[i] collects the candidate leaf texts for matcher.regexConds[i]
	// during the walk. Texts are views into the row being matched and are
	// discarded when match returns.
	regexTexts [][]string
}

func newRowMatchScratch(m *compiledRowMatcher) *rowMatchScratch {
	return &rowMatchScratch{
		sat:        make([]bool, len(m.conditions)),
		regexTexts: make([][]string, len(m.regexConds)),
	}
}

// compileRowMatcher compiles the row-level bloom and regex expressions of a
// query. regexQuery must already be pattern-compiled (CompileRegexQuery).
func compileRowMatcher(bloomQuery *BloomQuery, regexQuery *compiledRegexQuery, delimiter string, tokenizer ValueTokenizerFunc) *compiledRowMatcher {
	m := &compiledRowMatcher{
		delimiter:  delimiter,
		tokenizer:  tokenizer,
		fastTokens: isBasicWhitespaceLowerTokenizer(tokenizer),
	}

	bloomRoot := matcherNode{kind: matcherNodeTrue}
	if bloomQuery != nil {
		bloomRoot = m.compileBloomExpression(bloomQuery.Expression)
	}
	regexRoot := matcherNode{kind: matcherNodeTrue}
	if regexQuery != nil {
		regexRoot = m.compileRegexExpression(regexQuery.expression)
	}
	m.root = matcherNode{kind: matcherNodeAnd, children: []matcherNode{bloomRoot, regexRoot}}

	for i := range m.conditions {
		switch m.conditions[i].kind {
		case rowCondField:
			m.fieldConds = append(m.fieldConds, i)
		case rowCondToken, rowCondFieldToken:
			m.tokenishConds = append(m.tokenishConds, i)
		case rowCondRegex:
			m.regexConds = append(m.regexConds, i)
		}
	}

	sat := make([]bool, len(m.conditions))
	m.matchesAll = evalMatcherNode(&m.root, sat)
	for i := range sat {
		sat[i] = true
	}
	m.neverMatches = !evalMatcherNode(&m.root, sat)

	return m
}

func (m *compiledRowMatcher) addCondition(c rowCondition) matcherNode {
	m.conditions = append(m.conditions, c)
	return matcherNode{kind: matcherNodeCond, cond: len(m.conditions) - 1}
}

// compileBloomExpression mirrors matchesBloomExpression/matchesBloomCondition
// node for node (see matcherNode for the constant cases).
func (m *compiledRowMatcher) compileBloomExpression(e *BloomExpression) matcherNode {
	if e == nil {
		return matcherNode{kind: matcherNodeTrue}
	}
	switch e.ExpressionType {
	case BloomExpressionCondition:
		if e.Condition == nil {
			return matcherNode{kind: matcherNodeTrue}
		}
		switch e.Condition.Type {
		case BloomField:
			return m.addCondition(rowCondition{kind: rowCondField, field: e.Condition.Field})
		case BloomToken:
			return m.addCondition(rowCondition{kind: rowCondToken, token: e.Condition.Token})
		case BloomFieldToken:
			return m.addCondition(rowCondition{kind: rowCondFieldToken, field: e.Condition.Field, token: e.Condition.Token})
		default:
			return matcherNode{kind: matcherNodeFalse}
		}
	case BloomExpressionOr:
		if len(e.Children) == 0 {
			return matcherNode{kind: matcherNodeFalse}
		}
		children := make([]matcherNode, len(e.Children))
		for i := range e.Children {
			children[i] = m.compileBloomExpression(&e.Children[i])
		}
		return matcherNode{kind: matcherNodeOr, children: children}
	case BloomExpressionAnd:
		children := make([]matcherNode, len(e.Children))
		for i := range e.Children {
			children[i] = m.compileBloomExpression(&e.Children[i])
		}
		return matcherNode{kind: matcherNodeAnd, children: children}
	default:
		return matcherNode{kind: matcherNodeFalse}
	}
}

// compileRegexExpression mirrors matchesRegexExpression/matchesRegexCondition,
// including the empty-field rule: an empty field path is nonexistent, so the
// condition compiles to constant false (matching the walker's empty-path skip).
func (m *compiledRowMatcher) compileRegexExpression(e *compiledRegexExpression) matcherNode {
	if e == nil {
		return matcherNode{kind: matcherNodeTrue}
	}
	switch e.expressionType {
	case RegexExpressionCondition:
		if e.condition == nil {
			return matcherNode{kind: matcherNodeTrue}
		}
		if e.condition.field == "" {
			return matcherNode{kind: matcherNodeFalse}
		}
		return m.addCondition(rowCondition{
			kind:           rowCondRegex,
			field:          e.condition.field,
			fieldWithDelim: e.condition.field + m.delimiter,
			pattern:        e.condition.pattern,
		})
	case RegexExpressionOr:
		if len(e.children) == 0 {
			return matcherNode{kind: matcherNodeFalse}
		}
		children := make([]matcherNode, len(e.children))
		for i := range e.children {
			children[i] = m.compileRegexExpression(&e.children[i])
		}
		return matcherNode{kind: matcherNodeOr, children: children}
	case RegexExpressionAnd:
		children := make([]matcherNode, len(e.children))
		for i := range e.children {
			children[i] = m.compileRegexExpression(&e.children[i])
		}
		return matcherNode{kind: matcherNodeAnd, children: children}
	default:
		return matcherNode{kind: matcherNodeFalse}
	}
}

// matchRowBytes reports whether the row matches the query. The row is parsed
// through an unsafe string view of rowBytes — zero copies for rows that do not
// match. Safety: rowBytes is immutable for the duration of the call (a
// subslice of the block's decompressed buffer, or a caller-owned row), and no
// reference into the view survives the call: condition evaluation compares
// bytes and sets booleans, and the collected regex leaf texts are dropped when
// the call returns. Matched rows must be delivered via materializeRow, never
// from this parse.
func (m *compiledRowMatcher) matchRowBytes(rowBytes []byte, scratch *rowMatchScratch) bool {
	if m.matchesAll {
		return true
	}
	if m.neverMatches {
		return false
	}
	return m.match(gjson.Parse(unsafeString(rowBytes)), scratch)
}

func (m *compiledRowMatcher) match(value gjson.Result, scratch *rowMatchScratch) bool {
	sat := scratch.sat
	for i := range sat {
		sat[i] = false
	}
	for i := range scratch.regexTexts {
		scratch.regexTexts[i] = scratch.regexTexts[i][:0]
	}

	matched := false
	scratch.walker.walk(value, m.delimiter, func(path []byte, v gjson.Result, isLeaf bool) bool {
		flipped := false

		// Field conditions test every emitted path: containers, leaves, and
		// delimiter-split key prefixes alike.
		for _, i := range m.fieldConds {
			if !sat[i] && string(path) == m.conditions[i].field {
				sat[i] = true
				flipped = true
			}
		}

		if isLeaf {
			if text, ok := leafTokenInput(v); ok {
				if len(m.tokenishConds) > 0 {
					flipped = m.matchLeafTokens(path, text, scratch) || flipped
				}
				// Collect regex-candidate texts (leaves at or beneath the
				// condition's path) for the lazy phase after the walk.
				for ri, ci := range m.regexConds {
					c := &m.conditions[ci]
					if string(path) == c.field || hasStringPrefix(path, c.fieldWithDelim) {
						scratch.regexTexts[ri] = append(scratch.regexTexts[ri], text)
					}
				}
			}
		}

		if flipped && evalMatcherNode(&m.root, sat) {
			// Conditions only flip false→true, so the verdict is final.
			matched = true
			return false
		}
		return true
	})
	if matched {
		return true
	}
	if len(m.regexConds) == 0 {
		return false
	}

	// Lazy regex phase: run patterns only if satisfying every regex condition
	// could still make the root true (the reference likewise skips regex
	// evaluation when the bloom expression already failed).
	for _, ci := range m.regexConds {
		sat[ci] = true
	}
	possible := evalMatcherNode(&m.root, sat)
	for _, ci := range m.regexConds {
		sat[ci] = false
	}
	if !possible {
		return false
	}
	for ri, ci := range m.regexConds {
		c := &m.conditions[ci]
		for _, text := range scratch.regexTexts[ri] {
			if c.pattern.MatchString(text) {
				sat[ci] = true
				break
			}
		}
		if sat[ci] && evalMatcherNode(&m.root, sat) {
			return true
		}
	}
	return evalMatcherNode(&m.root, sat)
}

// matchLeafTokens tests one leaf's tokens against the unsatisfied Token
// conditions and the unsatisfied FieldToken conditions whose exact path this
// leaf sits at. Reports whether any condition flipped.
func (m *compiledRowMatcher) matchLeafTokens(path []byte, text string, scratch *rowMatchScratch) bool {
	sat := scratch.sat
	targets := scratch.tokenTargets[:0]
	for _, i := range m.tokenishConds {
		if sat[i] {
			continue
		}
		c := &m.conditions[i]
		if c.kind == rowCondToken || string(path) == c.field {
			targets = append(targets, i)
		}
	}
	scratch.tokenTargets = targets
	if len(targets) == 0 {
		return false
	}

	flipped := false
	if m.fastTokens {
		forEachWord(text, func(word string) bool {
			scratch.foldBuf = appendFoldedWord(scratch.foldBuf[:0], word)
			remaining := false
			for _, i := range targets {
				if sat[i] {
					continue
				}
				if string(scratch.foldBuf) == m.conditions[i].token {
					sat[i] = true
					flipped = true
				} else {
					remaining = true
				}
			}
			return remaining
		})
		return flipped
	}

	for _, token := range m.tokenizer(text) {
		for _, i := range targets {
			if !sat[i] && token == m.conditions[i].token {
				sat[i] = true
				flipped = true
			}
		}
	}
	return flipped
}

func hasStringPrefix(b []byte, prefix string) bool {
	return len(b) >= len(prefix) && string(b[:len(prefix)]) == prefix
}

// materializeRow builds the delivered row map from rowBytes. The
// string(rowBytes) conversion makes one independent copy, and everything gjson
// materializes references that copy — so delivered rows never alias the
// scanned block buffer, which is released (and must be assumed reusable) after
// the scan. gjson materializes JSON numbers as float64, matching
// encoding/json.
func materializeRow(rowBytes []byte) (map[string]any, error) {
	row, ok := gjson.Parse(string(rowBytes)).Value().(map[string]any)
	if !ok {
		return nil, fmt.Errorf("row is not a JSON object")
	}
	return row, nil
}
