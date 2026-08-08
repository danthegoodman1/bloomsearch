package bloomsearch

import (
	"io"
	"math/bits"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// Pooled compression codecs. Constructing zstd/snappy codec state is expensive
// (multi-goroutine workers, window and block buffers), and the flush, query,
// and merge paths each used to construct it per block. Codecs here are built
// with single-threaded settings — concurrency 1 spawns no goroutines, so an
// idle pooled codec is just buffers — and recycled with Reset.

// zstdEncoderPools holds one pool per compression level (levels are part of
// encoder construction and cannot be Reset).
var zstdEncoderPools sync.Map // int (level) -> *sync.Pool

func zstdEncoderPoolFor(level int) *sync.Pool {
	if p, ok := zstdEncoderPools.Load(level); ok {
		return p.(*sync.Pool)
	}
	p, _ := zstdEncoderPools.LoadOrStore(level, &sync.Pool{})
	return p.(*sync.Pool)
}

func getPooledZstdEncoder(dest io.Writer, level int) (*zstd.Encoder, error) {
	if enc, ok := zstdEncoderPoolFor(level).Get().(*zstd.Encoder); ok {
		enc.Reset(dest)
		return enc, nil
	}
	return zstd.NewWriter(dest,
		zstd.WithEncoderLevel(zstd.EncoderLevel(level)),
		zstd.WithEncoderConcurrency(1))
}

// putPooledZstdEncoder recycles an encoder whose stream was successfully
// Closed. Encoders in an uncertain state (a failed Close, an abandoned
// partition buffer) must be dropped for GC instead.
func putPooledZstdEncoder(enc *zstd.Encoder, level int) {
	enc.Reset(nil) // drop the destination writer reference while pooled
	zstdEncoderPoolFor(level).Put(enc)
}

var zstdDecoderPool sync.Pool

func getPooledZstdDecoder(src io.Reader) (*zstd.Decoder, error) {
	if dec, ok := zstdDecoderPool.Get().(*zstd.Decoder); ok {
		if err := dec.Reset(src); err == nil {
			return dec, nil
		}
		// A decoder that cannot Reset (closed) is dropped; fall through.
	}
	return zstd.NewReader(src, zstd.WithDecoderConcurrency(1))
}

func putPooledZstdDecoder(dec *zstd.Decoder) {
	dec.Reset(nil) // drop the source reader reference while pooled
	zstdDecoderPool.Put(dec)
}

var snappyWriterPool sync.Pool

func getPooledSnappyWriter(dest io.Writer) *snappy.Writer {
	if w, ok := snappyWriterPool.Get().(*snappy.Writer); ok {
		w.Reset(dest)
		return w
	}
	// Same options as snappy.NewBufferedWriter (snappy.Writer is an alias of
	// s2.Writer) plus concurrency 1: compression runs inline on Write instead
	// of on per-writer goroutines, so pooled writers hold no goroutines.
	return s2.NewWriter(dest,
		s2.WriterSnappyCompat(),
		s2.WriterBetterCompression(),
		s2.WriterConcurrency(1))
}

// putPooledSnappyWriter recycles a writer whose stream was successfully
// Closed; writers in an uncertain state must be dropped for GC.
func putPooledSnappyWriter(w *snappy.Writer) {
	w.Reset(nil) // drop the destination writer reference while pooled
	snappyWriterPool.Put(w)
}

var snappyReaderPool sync.Pool

func getPooledSnappyReader(src io.Reader) *snappy.Reader {
	if r, ok := snappyReaderPool.Get().(*snappy.Reader); ok {
		r.Reset(src)
		return r
	}
	return snappy.NewReader(src)
}

func putPooledSnappyReader(r *snappy.Reader) {
	r.Reset(nil) // drop the source reader reference while pooled
	snappyReaderPool.Put(r)
}

// Scan buffer pool: block-sized byte buffers for the query scan path
// (compressed reads and decompressed row data). Without pooling, every block
// job allocates and discards buffers on the order of the block size, and that
// garbage — not scan compute — dominates hit-query wall time via GC. Buffers
// are pooled in power-of-two capacity classes; a Get from class k always has
// capacity ≥ the requested size because Puts file a buffer under the largest
// class its capacity covers. Callers own release timing: a buffer must only be
// put back once no view into it can be dereferenced again (see
// readPooledBlockRowData for the scan path's argument).
const (
	scanBufferMinShift = 10 // 1 KiB: smaller buffers are cheaper to allocate than to pool
	scanBufferMaxShift = 26 // 64 MiB: larger buffers would pin too much idle memory
)

var scanBufferPools [scanBufferMaxShift - scanBufferMinShift + 1]sync.Pool

func getScanBuffer(size int) []byte {
	if size <= 0 {
		return nil
	}
	shift := bits.Len(uint(size - 1)) // ceil(log2(size))
	if shift < scanBufferMinShift {
		shift = scanBufferMinShift
	}
	if shift > scanBufferMaxShift {
		return make([]byte, size)
	}
	if v := scanBufferPools[shift-scanBufferMinShift].Get(); v != nil {
		return v.([]byte)[:size]
	}
	return make([]byte, size, 1<<shift)
}

func putScanBuffer(buf []byte) {
	c := cap(buf)
	// Reject capacities outside the pooled range outright, so the documented
	// per-buffer pin bound (1<<scanBufferMaxShift) holds.
	if c < 1<<scanBufferMinShift || c > 1<<scanBufferMaxShift {
		return
	}
	shift := bits.Len(uint(c)) - 1 // floor(log2(cap))
	scanBufferPools[shift-scanBufferMinShift].Put(buf[:c])
}
