package bloomsearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUniqueFields(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected []FieldValues
	}{
		{
			name: "basic nested structure",
			input: map[string]any{
				"user": map[string]any{
					"name": "John",
					"age":  30,
				},
			},
			expected: []FieldValues{
				{Path: "user.name", Values: []string{"John"}},
				{Path: "user.age", Values: []string{"30"}},
			},
		},
		{
			name: "array with different keys",
			input: map[string]any{
				"items": []any{
					map[string]any{"type": "admin"},
					map[string]any{"role": "user"},
				},
			},
			expected: []FieldValues{
				{Path: "items.type", Values: []string{"admin"}},
				{Path: "items.role", Values: []string{"user"}},
			},
		},
		{
			name: "array with same keys, different values",
			input: map[string]any{
				"tags": []any{
					map[string]any{"name": "red"},
					map[string]any{"name": "blue"},
				},
			},
			expected: []FieldValues{
				{Path: "tags.name", Values: []string{"red", "blue"}},
			},
		},
		{
			name: "array with duplicate values",
			input: map[string]any{
				"colors": []any{
					map[string]any{"value": "red"},
					map[string]any{"value": "red"},
					map[string]any{"value": "blue"},
				},
			},
			expected: []FieldValues{
				{Path: "colors.value", Values: []string{"red", "blue"}},
			},
		},
		{
			name:     "empty object",
			input:    map[string]any{},
			expected: []FieldValues{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := UniqueFields(tt.input, ".")

			// Convert to maps for easier comparison since order doesn't matter
			resultMap := make(map[string][]string)
			for _, fv := range result {
				resultMap[fv.Path] = fv.Values
			}

			expectedMap := make(map[string][]string)
			for _, fv := range tt.expected {
				expectedMap[fv.Path] = fv.Values
			}

			assert.Equal(t, len(expectedMap), len(resultMap), "Number of paths should match")

			for path, expectedValues := range expectedMap {
				assert.Contains(t, resultMap, path, "Path should exist in result")
				assert.ElementsMatch(t, expectedValues, resultMap[path], "Values should match for path %s", path)
			}
		})
	}
}
