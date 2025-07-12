package bloomsearch

import (
	"fmt"
	"strings"
	"unicode"
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
//	[{Field: "user.name", Values: ["John"]}, {Field: "user.tags.type", Values: ["admin"]}, {Field: "user.tags.role", Values: ["user"]}] (with delimiter ".")
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
	switch v := obj.(type) {
	case map[string]any:
		// Handle map type
		for key, value := range v {
			newPath := key
			if prefix != "" {
				newPath = prefix + delimiter + key
			}
			collectPathsAndValues(value, newPath, pathValues, delimiter)
		}
	case []any:
		// Handle slice of any
		for _, item := range v {
			collectPathsAndValues(item, prefix, pathValues, delimiter)
		}
	default:
		// Primitive type - add the path and value if we have a prefix
		if prefix != "" {
			if pathValues[prefix] == nil {
				pathValues[prefix] = make(map[string]bool)
			}
			valueStr := fmt.Sprintf("%v", v)
			pathValues[prefix][valueStr] = true
		}
	}
}

// ValueTokenizerFunc is a function that tokenizes a field value into a list of tokens
type ValueTokenizerFunc func(value any) []string

// BasicWhitespaceTokenizer tokenizes values by keeping alphanumeric characters, dashes, underscores, and emojis, splitting on spaces
func BasicWhitespaceTokenizer(value any) []string {
	// If the value is not a string, return the recursive call of its string representation
	if str, ok := value.(string); ok {
		// Remove non-alphanumerical characters (keeping dashes, underscores, and emojis)
		var result strings.Builder
		for _, r := range str {
			if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' || unicode.IsSymbol(r) {
				result.WriteRune(r)
			} else {
				result.WriteRune(' ')
			}
		}

		// Split on spaces and filter out empty strings
		tokens := strings.Fields(result.String())
		return tokens
	}

	// Convert value to string and recursively call BasicTokenizer
	return BasicWhitespaceTokenizer(fmt.Sprintf("%v", value))
}

func init() {
	// Test that BasicTokenizer implements ValueTokenizerFunc
	var _ ValueTokenizerFunc = BasicWhitespaceTokenizer
}
