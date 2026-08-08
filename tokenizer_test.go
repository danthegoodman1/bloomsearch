package bloomsearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

func TestBasicWhitespaceTokenizer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
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
			input:    "42",
			expected: []string{"42"},
		},
		{
			name:     "boolean input",
			input:    "true",
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
		assert.True(t, testJSONForField(jsonStr, "user.name", "."))
		assert.True(t, testJSONForField(jsonStr, "user.age", "."))
		assert.True(t, testJSONForField(jsonStr, "user", "."))
		assert.False(t, testJSONForField(jsonStr, "user.email", "."))
		assert.False(t, testJSONForField(jsonStr, "nothere", "."))

		// Fields in arrays (information loss scenario)
		jsonStr2 := `{"items": [{"name": "Item1", "price": 10}, {"name": "Item2", "price": 20}]}`
		assert.True(t, testJSONForField(jsonStr2, "items.name", "."))
		assert.True(t, testJSONForField(jsonStr2, "items.price", "."))
		assert.False(t, testJSONForField(jsonStr2, "items.category", "."))

		// Nested arrays
		jsonStr3 := `{"orders": [{"items": [{"name": "A"}, {"name": "B"}]}, {"items": [{"name": "C"}]}]}`
		assert.True(t, testJSONForField(jsonStr3, "orders.items.name", "."))
	})

	t.Run("Token", func(t *testing.T) {
		// Basic tokens
		jsonStr := `{"user": {"name": "John Doe", "age": 30}}`
		assert.True(t, testJSONForToken(jsonStr, "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForToken(jsonStr, "doe", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForToken(jsonStr, "30", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForToken(jsonStr, "jane", BasicWhitespaceLowerTokenizer))

		// Tokens in arrays (information loss scenario)
		jsonStr = `{"items": [{"name": "Item1"}, {"name": "Item2"}, {"name": "Item3"}]}`
		assert.True(t, testJSONForToken(jsonStr, "item1", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForToken(jsonStr, "item2", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForToken(jsonStr, "item3", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForToken(jsonStr, "item4", BasicWhitespaceLowerTokenizer))
	})

	t.Run("FieldToken", func(t *testing.T) {
		// Basic field+token
		jsonStr := `{"user": {"name": "John Doe", "role": "admin"}}`
		assert.True(t, testJSONForFieldToken(jsonStr, "user.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "user.name", ".", "doe", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "user.role", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "user.name", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "user.role", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "user.email", ".", "test", BasicWhitespaceLowerTokenizer))

		// Arrays with field+token
		jsonStr = `{"users": [{"name": "John"}, {"name": "Jane"}], "tags": ["admin", "user"]}`
		assert.True(t, testJSONForFieldToken(jsonStr, "users.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "users.name", ".", "jane", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "tags", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "users.name", ".", "bob", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "users.name", ".", "alice", BasicWhitespaceLowerTokenizer))

		// Deeply nested arrays
		jsonStr = `{"groups": [{"users": [{"name": "John"}, {"name": "Jane"}]}, {"users": [{"name": "Bob"}]}]}`
		assert.True(t, testJSONForFieldToken(jsonStr, "groups.users.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "groups.users.name", ".", "jane", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "groups.users.name", ".", "bob", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "groups.users.name", ".", "alice", BasicWhitespaceLowerTokenizer))
	})

	t.Run("InformationLoss", func(t *testing.T) {
		// Core information loss test: same field path with different values across array elements
		jsonStr := `{"items": [{"name": "Item1", "category": "electronics"}, {"name": "Item2", "category": "books"}]}`

		// Should find ANY value that exists in the specific field path
		assert.True(t, testJSONForFieldToken(jsonStr, "items.name", ".", "item1", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "items.name", ".", "item2", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "items.category", ".", "electronics", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "items.category", ".", "books", BasicWhitespaceLowerTokenizer))

		// Should not find values that don't exist in that field path
		assert.False(t, testJSONForFieldToken(jsonStr, "items.name", ".", "item3", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "items.category", ".", "furniture", BasicWhitespaceLowerTokenizer))

		// Key test: we've "lost" the connection between Item1 and electronics
		// But we should still find both values independently
		assert.True(t, testJSONForFieldToken(jsonStr, "items.name", ".", "item1", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "items.category", ".", "books", BasicWhitespaceLowerTokenizer))

		// Test duplicate values across array elements (like bloom filter deduplication)
		jsonStr = `{"tags": [{"type": "admin"}, {"type": "user"}, {"type": "admin"}]}`
		assert.True(t, testJSONForFieldToken(jsonStr, "tags.type", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "tags.type", ".", "user", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "tags.type", ".", "guest", BasicWhitespaceLowerTokenizer))

		// Mixed data types
		jsonStr = `{"records": [{"id": 1, "active": true}, {"id": 2, "active": false}]}`
		assert.True(t, testJSONForFieldToken(jsonStr, "records.id", ".", "1", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "records.id", ".", "2", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "records.active", ".", "true", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "records.active", ".", "false", BasicWhitespaceLowerTokenizer))

		// Sibling fields inside array elements stay path-scoped - a token under
		// one key must not match a FieldToken query for a sibling key
		jsonStr = `{"user": {"name": "John", "tags": [{"type": "admin"}, {"role": "user"}]}}`
		assert.True(t, testJSONForFieldToken(jsonStr, "user.name", ".", "john", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "user.tags.type", ".", "admin", BasicWhitespaceLowerTokenizer))
		assert.True(t, testJSONForFieldToken(jsonStr, "user.tags.role", ".", "user", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "user.tags.type", ".", "user", BasicWhitespaceLowerTokenizer))
		assert.False(t, testJSONForFieldToken(jsonStr, "user.tags.role", ".", "admin", BasicWhitespaceLowerTokenizer))
	})
}

func TestRegexQueryMatching(t *testing.T) {
	t.Run("FieldRegexMatchesNestedValuesAndNonStringPrimitives", func(t *testing.T) {
		query := NewQuery().
			MatchRegex(
				RegexAnd(
					FieldRegex("users.name", "(?i)^jo"),
					RegexOr(
						FieldRegex("users.active", "^true$"),
						FieldRegex("users.id", "^2$"),
					),
				),
			).
			Build()

		compiledRegexQuery, err := compileRegexQuery(query.Regex)
		assert.NoError(t, err)

		matchingJSON := `{"users":[{"id":1,"name":"John","active":true},{"id":2,"name":"Jane","active":false}]}`
		nonMatchingJSON := `{"users":[{"id":3,"name":"Alice","active":false}]}`

		assert.True(t, testGJSONForQuery(gjson.Parse(matchingJSON), nil, compiledRegexQuery, ".", BasicWhitespaceLowerTokenizer))
		assert.False(t, testGJSONForQuery(gjson.Parse(nonMatchingJSON), nil, compiledRegexQuery, ".", BasicWhitespaceLowerTokenizer))
	})

	t.Run("InvalidRegexFailsCompile", func(t *testing.T) {
		query := NewQuery().
			FieldRegex("message", "[unterminated(").
			Build()

		compiledRegexQuery, err := compileRegexQuery(query.Regex)
		assert.Error(t, err)
		assert.Nil(t, compiledRegexQuery)
	})
}
