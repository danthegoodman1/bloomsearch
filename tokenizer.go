package bloomsearch

import (
	"fmt"
	"reflect"
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
//	{"user": {"name": "John", "tags": [{"type": "admin"}, {"role": "user"}]}}
//
// Returns:
//
//	[{Path: "user.name", Values: ["John"]}, {Path: "user.tags.type", Values: ["admin"]}, {Path: "user.tags.role", Values: ["user"]}] (with delimiter ".")
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
	jsonStr := string(jsonBytes)

	switch condition.Type {
	case BloomField:
		return TestJSONForField(jsonStr, condition.Field, delimiter)
	case BloomToken:
		return TestJSONForToken(jsonStr, condition.Token, tokenizer)
	case BloomFieldToken:
		return TestJSONForFieldToken(jsonStr, condition.Field, delimiter, condition.Token, tokenizer)
	default:
		return false
	}
}

// TestJSONForBloomGroup tests a JSON string against a bloom group using gjson
func TestJSONForBloomGroup(jsonBytes []byte, group *BloomGroup, delimiter string, tokenizer ValueTokenizerFunc) bool {
	if len(group.Conditions) == 0 {
		return true // Empty group matches everything
	}

	// Evaluate each condition in the group
	conditionResults := make([]bool, len(group.Conditions))
	for i, condition := range group.Conditions {
		conditionResults[i] = TestJSONForBloomCondition(jsonBytes, &condition, delimiter, tokenizer)
	}

	// Combine condition results based on group combinator
	if group.Combinator == CombinatorOR {
		// Any condition can match
		for _, result := range conditionResults {
			if result {
				return true
			}
		}
		return false
	}

	// Default AND behavior: all conditions must match
	for _, result := range conditionResults {
		if !result {
			return false
		}
	}
	return true
}

// TestJSONForBloomQuery tests a JSON string against a bloom query using gjson
func TestJSONForBloomQuery(jsonBytes []byte, bloomQuery *BloomQuery, delimiter string, tokenizer ValueTokenizerFunc) bool {
	if bloomQuery == nil || len(bloomQuery.Groups) == 0 {
		return true // No bloom filtering needed
	}

	// Evaluate each group
	groupResults := make([]bool, len(bloomQuery.Groups))
	for i, group := range bloomQuery.Groups {
		groupResults[i] = TestJSONForBloomGroup(jsonBytes, &group, delimiter, tokenizer)
	}

	// Combine group results based on query combinator
	if bloomQuery.Combinator == CombinatorOR {
		// Any group can match
		for _, result := range groupResults {
			if result {
				return true
			}
		}
		return false
	}

	// Default AND behavior: all groups must match
	for _, result := range groupResults {
		if !result {
			return false
		}
	}
	return true
}
