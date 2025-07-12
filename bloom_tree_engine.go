package bloomsearch

type PartitionFunc func(row map[string]any) string

type BloomSearchEngine struct {
	config    BloomSearchEngineConfig
	metaStore MetaStore
	dataStore DataStore
}

type BloomSearchEngineConfig struct {
	Tokenizer     ValueTokenizerFunc
	PartitionFunc PartitionFunc
}

func NewBloomSearchEngine(config BloomSearchEngineConfig, metaStore MetaStore, dataStore DataStore) *BloomSearchEngine {
	return &BloomSearchEngine{
		config:    config,
		metaStore: metaStore,
		dataStore: dataStore,
	}
}
