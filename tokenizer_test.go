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
			expected: []string{"hello@world.com!test"},
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
			expected: []string{"user@domain.com,", "password123!"},
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
			expected: []string{"!@#$%^&*()"},
		},
		{
			name:     "string with tabs and newlines",
			input:    "hello\tworld\ntest",
			expected: []string{"hello", "world", "test"},
		},
		{
			name:     "complex mixed content",
			input:    "user-name_123@example.com (active)",
			expected: []string{"user-name_123@example.com", "(active)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BasicWhitespaceLowerTokenizer(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestJSONMatching(t *testing.T) {
	t.Run("Field", func(t *testing.T) {
		// Basic fields
		jsonStr := `{"user": {"name": "John", "age": 30}}`
		assert.True(t, TestJSONForField(jsonStr, "user.name", "."))
		assert.True(t, TestJSONForField(jsonStr, "user.age", "."))
		assert.True(t, TestJSONForField(jsonStr, "user", "."))
		assert.False(t, TestJSONForField(jsonStr, "user.email", "."))
		assert.False(t, TestJSONForField(jsonStr, "nothere", "."))

		// Fields in arrays (information loss scenario)
		jsonStr2 := `{"items": [{"name": "Item1", "price": 10}, {"name": "Item2", "price": 20}]}`
		assert.True(t, TestJSONForField(jsonStr2, "items.name", "."))
		assert.True(t, TestJSONForField(jsonStr2, "items.price", "."))
		assert.False(t, TestJSONForField(jsonStr2, "items.category", "."))

		// Nested arrays
		jsonStr3 := `{"orders": [{"items": [{"name": "A"}, {"name": "B"}]}, {"items": [{"name": "C"}]}]}`
		assert.True(t, TestJSONForField(jsonStr3, "orders.items.name", "."))
	})

	t.Run("Token", func(t *testing.T) {
		// Basic tokens
		jsonStr := `{"user": {"name": "John Doe", "age": 30}}`
		assert.True(t, TestJSONForToken(jsonStr, "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForToken(jsonStr, "doe", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForToken(jsonStr, "30", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForToken(jsonStr, "jane", BasicWhitespaceLowerTokenizer))

		// Tokens in arrays (information loss scenario)
		jsonStr = `{"items": [{"name": "Item1"}, {"name": "Item2"}, {"name": "Item3"}]}`
		assert.True(t, TestJSONForToken(jsonStr, "item1", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForToken(jsonStr, "item2", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForToken(jsonStr, "item3", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForToken(jsonStr, "item4", BasicWhitespaceLowerTokenizer))
	})

	t.Run("FieldToken", func(t *testing.T) {
		// Basic field+token
		jsonStr := `{"user": {"name": "John Doe", "role": "admin"}}`
		assert.True(t, TestJSONForFieldToken(jsonStr, "user.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "user.name", ".", "doe", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "user.role", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "user.name", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "user.role", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "user.email", ".", "test", BasicWhitespaceLowerTokenizer))

		// Arrays with field+token
		jsonStr = `{"users": [{"name": "John"}, {"name": "Jane"}], "tags": ["admin", "user"]}`
		assert.True(t, TestJSONForFieldToken(jsonStr, "users.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "users.name", ".", "jane", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "tags", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "users.name", ".", "bob", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "users.name", ".", "alice", BasicWhitespaceLowerTokenizer))

		// Deeply nested arrays
		jsonStr = `{"groups": [{"users": [{"name": "John"}, {"name": "Jane"}]}, {"users": [{"name": "Bob"}]}]}`
		assert.True(t, TestJSONForFieldToken(jsonStr, "groups.users.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "groups.users.name", ".", "jane", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "groups.users.name", ".", "bob", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "groups.users.name", ".", "alice", BasicWhitespaceLowerTokenizer))
	})

	t.Run("InformationLoss", func(t *testing.T) {
		// Core information loss test: same field path with different values across array elements
		jsonStr := `{"items": [{"name": "Item1", "category": "electronics"}, {"name": "Item2", "category": "books"}]}`

		// Should find ANY value that exists in the specific field path
		assert.True(t, TestJSONForFieldToken(jsonStr, "items.name", ".", "item1", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "items.name", ".", "item2", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "items.category", ".", "electronics", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "items.category", ".", "books", BasicWhitespaceLowerTokenizer))

		// Should not find values that don't exist in that field path
		assert.False(t, TestJSONForFieldToken(jsonStr, "items.name", ".", "item3", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "items.category", ".", "furniture", BasicWhitespaceLowerTokenizer))

		// Key test: we've "lost" the connection between Item1 and electronics
		// But we should still find both values independently
		assert.True(t, TestJSONForFieldToken(jsonStr, "items.name", ".", "item1", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "items.category", ".", "books", BasicWhitespaceLowerTokenizer))

		// Test duplicate values across array elements (like bloom filter deduplication)
		jsonStr = `{"tags": [{"type": "admin"}, {"type": "user"}, {"type": "admin"}]}`
		assert.True(t, TestJSONForFieldToken(jsonStr, "tags.type", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "tags.type", ".", "user", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "tags.type", ".", "guest", BasicWhitespaceLowerTokenizer))

		// Mixed data types
		jsonStr = `{"records": [{"id": 1, "active": true}, {"id": 2, "active": false}]}`
		assert.True(t, TestJSONForFieldToken(jsonStr, "records.id", ".", "1", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "records.id", ".", "2", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "records.active", ".", "true", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "records.active", ".", "false", BasicWhitespaceLowerTokenizer))

		// UniqueFields example - should not find cross-contamination
		jsonStr = `{"user": {"name": "John", "tags": [{"type": "admin"}, {"role": "user"}]}}`
		assert.True(t, TestJSONForFieldToken(jsonStr, "user.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "user.tags.type", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.True(t, TestJSONForFieldToken(jsonStr, "user.tags.role", ".", "user", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "user.tags.type", ".", "user", BasicWhitespaceLowerTokenizer))
		assert.False(t, TestJSONForFieldToken(jsonStr, "user.tags.role", ".", "admin", BasicWhitespaceLowerTokenizer))
	})
}
