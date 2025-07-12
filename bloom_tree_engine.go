package bloomsearch

import "github.com/bits-and-blooms/bloom/v3"

type bloomsearchEngine struct {
	bloomFilterFactory func() *bloom.BloomFilter
}

func Newbloomsearch() {

}
