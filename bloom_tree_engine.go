package bloomsearch

type PartitionFunc func(row map[string]any) string

type BloomSearchEngine struct {
	config BloomSearchEngineConfig
}

type BloomSearchEngineConfig struct {
	Tokenizer     ValueTokenizerFunc
	PartitionFunc PartitionFunc
}

func NewBloomSearchEngine(config BloomSearchEngineConfig) *BloomSearchEngine {
	return &BloomSearchEngine{
		config: config,
	}
}
