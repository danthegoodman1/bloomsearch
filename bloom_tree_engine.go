package bloomtree

import "github.com/bits-and-blooms/bloom/v3"

type BloomTreeEngine struct {
	bloomFilterFactory func() *bloom.BloomFilter
}

func NewBloomTree() {

}
