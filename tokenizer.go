package bloomsearch

// UniquePaths extracts all unique field paths from a nested map structure using the specified delimiter.
// Arrays are traversed but indices are ignored, so duplicate paths from array elements are deduplicated.
//
// Example:
//
//	{"user": {"name": "John", "tags": [{"type": "admin"}, {"role": "user"}]}}
//
// Returns:
//
//	["user.name", "user.tags.type", "user.tags.role"] (with delimiter ".")
func UniquePaths(row map[string]any, delimiter string) []string {
	pathSet := make(map[string]bool)
	collectPaths(row, "", pathSet, delimiter)

	// Convert set to slice
	paths := make([]string, 0, len(pathSet))
	for path := range pathSet {
		paths = append(paths, path)
	}
	return paths
}

func collectPaths(obj any, prefix string, pathSet map[string]bool, delimiter string) {
	switch v := obj.(type) {
	case map[string]any:
		// Handle map type
		for key, value := range v {
			newPath := key
			if prefix != "" {
				newPath = prefix + delimiter + key
			}
			collectPaths(value, newPath, pathSet, delimiter)
		}
	case []any:
		// Handle slice of any
		for _, item := range v {
			collectPaths(item, prefix, pathSet, delimiter)
		}
	default:
		// Primitive type - add the path if we have a prefix
		if prefix != "" {
			pathSet[prefix] = true
		}
	}
}

// ValueTokenizerFunc is a function that tokenizes a field value into a list of tokens
type ValueTokenizerFunc func(value any) []string
