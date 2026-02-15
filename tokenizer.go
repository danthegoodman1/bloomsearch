package bloomsearch

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

// FieldValues represents a field path and its associated values
type FieldValues struct {
	Path   string
	Values []string
}

// UniqueFields extracts all unique field paths from a nested map structure using the specified delimiter,
// returning tuples of (field_name, []values) where values are deduplicated per field.
// Arrays are traversed but indices are ignored, so duplicate paths from array elements are deduplicated.
//
// Example:
//
//	{"user": {"name": "John", "tags": [{"type": "user"}, {"role": "admin"}]}}
//
// Returns:
//
//	[{Path: "user.name", Values: ["John"]}, {Path: "user.tags.type", Values: ["user"]}, {Path: "user.tags.role", Values: ["admin"]}] (with delimiter ".")
func UniqueFields(row map[string]any, delimiter string) []FieldValues {
	pathValues := make(map[string]map[string]bool)
	collectPathsAndValues(row, "", pathValues, delimiter)

	// Convert map to slice
	result := make([]FieldValues, 0, len(pathValues))
	for path, valueSet := range pathValues {
		values := make([]string, 0, len(valueSet))
		for value := range valueSet {
			values = append(values, value)
		}
		result = append(result, FieldValues{Path: path, Values: values})
	}
	return result
}

func collectPathsAndValues(obj any, prefix string, pathValues map[string]map[string]bool, delimiter string) {
	if obj == nil {
		return
	}

	// Use reflection to handle any type generically
	val := reflect.ValueOf(obj)
	typ := val.Type()

	switch typ.Kind() {
	case reflect.Map:
		// Handle any map type generically
		for _, key := range val.MapKeys() {
			keyStr := fmt.Sprintf("%v", key.Interface())
			value := val.MapIndex(key).Interface()

			newPath := keyStr
			if prefix != "" {
				newPath = prefix + delimiter + keyStr
			}
			collectPathsAndValues(value, newPath, pathValues, delimiter)
		}
	case reflect.Slice, reflect.Array:
		// Handle any slice/array type generically
		for i := 0; i < val.Len(); i++ {
			item := val.Index(i).Interface()
			collectPathsAndValues(item, prefix, pathValues, delimiter)
		}
	default:
		// Primitive type - add the path and value if we have a prefix
		if prefix != "" {
			if pathValues[prefix] == nil {
				pathValues[prefix] = make(map[string]bool)
			}
			valueStr := fmt.Sprintf("%v", obj)
			pathValues[prefix][valueStr] = true
		}
	}
}

// ValueTokenizerFunc is a function that tokenizes a field value into a list of tokens
type ValueTokenizerFunc func(value any) []string

// BasicWhitespaceLowerTokenizer tokenizes values by splitting on whitespace and converting to lowercase
func BasicWhitespaceLowerTokenizer(value any) []string {
	// If the value is not a string, return the recursive call of its string representation
	if str, ok := value.(string); ok {
		// Split on whitespace and filter out empty strings
		tokens := strings.Fields(strings.ToLower(str))
		return tokens
	}

	// Convert value to string and recursively call BasicTokenizer
	return BasicWhitespaceLowerTokenizer(fmt.Sprintf("%v", value))
}

func init() {
	// Test that BasicTokenizer implements ValueTokenizerFunc
	var _ ValueTokenizerFunc = BasicWhitespaceLowerTokenizer
}

// TestJSONForField tests if a field path exists in JSON, handling arrays by walking them
func TestJSONForField(jsonStr, fieldPath, delimiter string) bool {
	pathComponents := strings.Split(fieldPath, delimiter)
	return walkJSONForField(gjson.Parse(jsonStr), pathComponents, 0)
}

// walkJSONForField recursively walks JSON structure to find a field path
func walkJSONForField(value gjson.Result, pathComponents []string, depth int) bool {
	if depth >= len(pathComponents) {
		return true // We've found the complete path
	}

	currentComponent := pathComponents[depth]

	if value.IsObject() {
		// Check if this object has the current path component
		if childValue := value.Get(currentComponent); childValue.Exists() {
			return walkJSONForField(childValue, pathComponents, depth+1)
		}
		return false
	} else if value.IsArray() {
		// For arrays, check each element to see if any contains the path
		found := false
		value.ForEach(func(key, val gjson.Result) bool {
			if walkJSONForField(val, pathComponents, depth) {
				found = true
				return false // Stop iteration
			}
			return true // Continue iteration
		})
		return found
	}

	return false
}

// TestJSONForToken tests if a token exists in any field value, handling arrays by walking them
func TestJSONForToken(jsonStr, token string, tokenizer ValueTokenizerFunc) bool {
	return walkJSONForValue(gjson.Parse(jsonStr), func(value gjson.Result) bool {
		tokens := tokenizer(value.Value())
		for _, t := range tokens {
			if t == token {
				return true
			}
		}
		return false
	})
}

// TestJSONForFieldToken tests if a specific field contains a specific token, handling arrays by walking them
func TestJSONForFieldToken(jsonStr, fieldPath, delimiter, token string, tokenizer ValueTokenizerFunc) bool {
	pathComponents := strings.Split(fieldPath, delimiter)
	return walkJSONForFieldValue(gjson.Parse(jsonStr), pathComponents, 0, func(value gjson.Result) bool {
		tokens := tokenizer(value.Value())
		for _, t := range tokens {
			if t == token {
				return true
			}
		}
		return false
	})
}

// walkJSONForValue recursively walks JSON structure and tests each primitive value
func walkJSONForValue(value gjson.Result, testFunc func(gjson.Result) bool) bool {
	switch value.Type {
	case gjson.String, gjson.Number, gjson.True, gjson.False:
		// Test this primitive value
		return testFunc(value)
	case gjson.JSON:
		if value.IsObject() {
			// Walk all values in the object
			found := false
			value.ForEach(func(key, val gjson.Result) bool {
				if walkJSONForValue(val, testFunc) {
					found = true
					return false // Stop iteration
				}
				return true // Continue iteration
			})
			return found
		} else if value.IsArray() {
			// Walk all elements in the array
			found := false
			value.ForEach(func(key, val gjson.Result) bool {
				if walkJSONForValue(val, testFunc) {
					found = true
					return false // Stop iteration
				}
				return true // Continue iteration
			})
			return found
		}
		return false
	default:
		return false
	}
}

// walkJSONForFieldValue recursively walks JSON structure to find a field path and test its values
func walkJSONForFieldValue(value gjson.Result, pathComponents []string, depth int, testFunc func(gjson.Result) bool) bool {
	if depth >= len(pathComponents) {
		// We've reached the target field, now test its values
		return walkJSONForValue(value, testFunc)
	}

	currentComponent := pathComponents[depth]

	if value.IsObject() {
		// Check if this object has the current path component
		if childValue := value.Get(currentComponent); childValue.Exists() {
			return walkJSONForFieldValue(childValue, pathComponents, depth+1, testFunc)
		}
		return false
	} else if value.IsArray() {
		// For arrays, check each element to see if any contains the path
		found := false
		value.ForEach(func(key, val gjson.Result) bool {
			if walkJSONForFieldValue(val, pathComponents, depth, testFunc) {
				found = true
				return false // Stop iteration
			}
			return true // Continue iteration
		})
		return found
	}

	return false
}

// TestJSONForBloomCondition tests a JSON string against a bloom condition using gjson
func TestJSONForBloomCondition(jsonBytes []byte, condition *BloomCondition, delimiter string, tokenizer ValueTokenizerFunc) bool {
	// Parse once to avoid repeated string allocation and parsing
	return testGJSONForBloomCondition(gjson.ParseBytes(jsonBytes), condition, delimiter, tokenizer)
}

// testGJSONForBloomCondition evaluates a single condition against a parsed gjson value
func testGJSONForBloomCondition(value gjson.Result, condition *BloomCondition, delimiter string, tokenizer ValueTokenizerFunc) bool {
	switch condition.Type {
	case BloomField:
		return TestGJSONForField(value, condition.Field, delimiter)
	case BloomToken:
		return TestGJSONForToken(value, condition.Token, tokenizer)
	case BloomFieldToken:
		return TestGJSONForFieldToken(value, condition.Field, delimiter, condition.Token, tokenizer)
	default:
		return false
	}
}

// testGJSONForBloomExpression evaluates a bloom expression with short-circuit logic against a parsed value
func testGJSONForBloomExpression(value gjson.Result, expression *BloomExpression, delimiter string, tokenizer ValueTokenizerFunc) bool {
	if expression == nil {
		return true
	}

	switch expression.expressionType {
	case bloomExpressionCondition:
		if expression.condition == nil {
			return true
		}
		return testGJSONForBloomCondition(value, expression.condition, delimiter, tokenizer)
	case bloomExpressionOr:
		if len(expression.children) == 0 {
			return false
		}
		for i := range expression.children {
			if testGJSONForBloomExpression(value, &expression.children[i], delimiter, tokenizer) {
				return true
			}
		}
		return false
	case bloomExpressionAnd:
		for i := range expression.children {
			if !testGJSONForBloomExpression(value, &expression.children[i], delimiter, tokenizer) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// TestJSONForBloomQuery tests a JSON string against a bloom query using gjson
func TestJSONForBloomQuery(jsonBytes []byte, bloomQuery *BloomQuery, delimiter string, tokenizer ValueTokenizerFunc) bool {
	if bloomQuery == nil || bloomQuery.Expression == nil {
		return true
	}

	value := gjson.ParseBytes(jsonBytes)
	return testGJSONForBloomExpression(value, bloomQuery.Expression, delimiter, tokenizer)
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

func testGJSONForRegexCondition(value gjson.Result, condition *compiledRegexCondition, delimiter string) bool {
	if condition == nil {
		return true
	}
	pathComponents := strings.Split(condition.field, delimiter)
	return walkJSONForFieldValue(value, pathComponents, 0, func(v gjson.Result) bool {
		return condition.pattern.MatchString(fmt.Sprintf("%v", v.Value()))
	})
}

func testGJSONForRegexExpression(value gjson.Result, expression *compiledRegexExpression, delimiter string) bool {
	if expression == nil {
		return true
	}

	switch expression.expressionType {
	case regexExpressionCondition:
		if expression.condition == nil {
			return true
		}
		return testGJSONForRegexCondition(value, expression.condition, delimiter)
	case regexExpressionOr:
		if len(expression.children) == 0 {
			return false
		}
		for i := range expression.children {
			if testGJSONForRegexExpression(value, &expression.children[i], delimiter) {
				return true
			}
		}
		return false
	case regexExpressionAnd:
		for i := range expression.children {
			if !testGJSONForRegexExpression(value, &expression.children[i], delimiter) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func TestGJSONForQuery(value gjson.Result, bloomQuery *BloomQuery, regexQuery *compiledRegexQuery, delimiter string, tokenizer ValueTokenizerFunc) bool {
	if bloomQuery != nil && bloomQuery.Expression != nil {
		if !testGJSONForBloomExpression(value, bloomQuery.Expression, delimiter, tokenizer) {
			return false
		}
	}

	if regexQuery != nil && regexQuery.expression != nil {
		if !testGJSONForRegexExpression(value, regexQuery.expression, delimiter) {
			return false
		}
	}

	return true
}

// Byte-based helpers that operate on a pre-parsed gjson.Result
func TestGJSONForField(value gjson.Result, fieldPath, delimiter string) bool {
	pathComponents := strings.Split(fieldPath, delimiter)
	return walkJSONForField(value, pathComponents, 0)
}

func TestGJSONForToken(value gjson.Result, token string, tokenizer ValueTokenizerFunc) bool {
	return walkJSONForValue(value, func(v gjson.Result) bool {
		tokens := tokenizer(v.Value())
		for _, t := range tokens {
			if t == token {
				return true
			}
		}
		return false
	})
}

func TestGJSONForFieldToken(value gjson.Result, fieldPath, delimiter, token string, tokenizer ValueTokenizerFunc) bool {
	pathComponents := strings.Split(fieldPath, delimiter)
	return walkJSONForFieldValue(value, pathComponents, 0, func(v gjson.Result) bool {
		tokens := tokenizer(v.Value())
		for _, t := range tokens {
			if t == token {
				return true
			}
		}
		return false
	})
}
