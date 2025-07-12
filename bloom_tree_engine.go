package bloomsearch

type PartitionFunc func(row map[string]any) string

type BloomSearchEngine struct {
}

func NewBloomSearchEngine() *BloomSearchEngine {
	return &BloomSearchEngine{}
}
