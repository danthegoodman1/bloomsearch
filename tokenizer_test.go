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

func TestBasicWhitespaceTokenizer(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected []string
	}{
		{
			name:     "basic alphanumeric string",
			input:    "hello world 123",
			expected: []string{"hello", "world", "123"},
		},
		{
			name:     "string with special characters",
			input:    "hello@world.com!test",
			expected: []string{"hello", "world", "com", "test"},
		},
		{
			name:     "string with dashes and underscores",
			input:    "hello-world_test",
			expected: []string{"hello-world_test"},
		},
		{
			name:     "string with emojis",
			input:    "hello 😊 world 🎉",
			expected: []string{"hello", "😊", "world", "🎉"},
		},
		{
			name:     "mixed alphanumeric with punctuation",
			input:    "user@domain.com, password123!",
			expected: []string{"user", "domain", "com", "password123"},
		},
		{
			name:     "number input",
			input:    42,
			expected: []string{"42"},
		},
		{
			name:     "boolean input",
			input:    true,
			expected: []string{"true"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "string with multiple spaces",
			input:    "hello   world",
			expected: []string{"hello", "world"},
		},
		{
			name:     "string with only special characters",
			input:    "!@#$%^&*()",
			expected: []string{"$", "^"},
		},
		{
			name:     "string with tabs and newlines",
			input:    "hello\tworld\ntest",
			expected: []string{"hello", "world", "test"},
		},
		{
			name:     "complex mixed content",
			input:    "user-name_123@example.com (active)",
			expected: []string{"user-name_123", "example", "com", "active"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BasicWhitespaceTokenizer(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
