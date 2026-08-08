package bloomsearch

// Per-query DataStore handle pooling for the read path (query_exec.go).

import (
	"context"
	"errors"
	"io"
	"sync"
)

// errHandlePoolClosed is returned by acquire once the query owning the pool has
// torn it down. Every reader has exited by then, so it is a guard, not a path
// the query takes.
var errHandlePoolClosed = errors.New("query file handle pool is closed")

// fileHandlePool lends DataStore read handles to one query's readers. A query
// touches a candidate file repeatedly — once to evaluate its data blocks'
// filter sections, then once per surviving block to read row data — and a
// handle per touch means thousands of opens per file on object storage.
//
// Handles are per file and exclusively checked out: acquire takes a handle out
// of the file's idle set (opening one when the set is empty) and put returns
// it, so no two goroutines ever read from the same handle, which is what the
// DataStore contract requires. A reader whose handle failed a seek or read
// calls discard instead: a handle left mid-stream must not be lent to the
// file's next reader.
//
// Handle lifetime is reference counted per file. A reader retains the file
// before acquiring handles for it and releases it when done, and the file's
// idle handles are closed as soon as its last reference goes away — a file the
// query has finished with does not pin handles for the rest of the query, so
// open handles are bounded by the readers in flight (worker counts) rather
// than by the candidate files a long query streams through. closeAll, run once
// every query worker has exited, closes whatever is left.
type fileHandlePool struct {
	store DataStore

	mu     sync.Mutex
	files  map[string]*pooledFileHandles
	closed bool
}

// pooledFileHandles is one file's pool state: how many readers still need the
// file, and the handles none of them currently holds.
type pooledFileHandles struct {
	refs int
	idle []io.ReadSeekCloser
}

func newFileHandlePool(store DataStore) *fileHandlePool {
	return &fileHandlePool{store: store, files: make(map[string]*pooledFileHandles)}
}

// retain registers one reader's need for a file's handles. Every retain needs
// exactly one matching release, and handles acquired for the file must be
// handed back (put or discard) before that release.
func (p *fileHandlePool) retain(pointer []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	if entry := p.files[string(pointer)]; entry != nil {
		entry.refs++
		return
	}
	p.files[string(pointer)] = &pooledFileHandles{refs: 1}
}

// release drops one reader's need for a file. The last release closes the
// file's idle handles.
func (p *fileHandlePool) release(pointer []byte) {
	p.mu.Lock()
	entry := p.files[string(pointer)]
	if entry == nil {
		p.mu.Unlock()
		return
	}
	entry.refs--
	if entry.refs > 0 {
		p.mu.Unlock()
		return
	}
	idle := entry.idle
	delete(p.files, string(pointer))
	p.mu.Unlock()

	closeHandles(idle)
}

// acquire lends an idle handle for the file, opening one when none is idle.
// The caller has exclusive use of it until put or discard hands it back.
func (p *fileHandlePool) acquire(ctx context.Context, pointer []byte) (io.ReadSeekCloser, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errHandlePoolClosed
	}
	if entry := p.files[string(pointer)]; entry != nil && len(entry.idle) > 0 {
		last := len(entry.idle) - 1
		handle := entry.idle[last]
		entry.idle[last] = nil
		entry.idle = entry.idle[:last]
		p.mu.Unlock()
		return handle, nil
	}
	p.mu.Unlock()

	// Opening is I/O: never under the pool lock.
	return p.store.OpenFile(ctx, pointer)
}

// put returns a healthy handle for the file's other readers to reuse. A handle
// nobody can ask for anymore — the pool is closed, or the file's last
// reference is gone — is closed instead of stored.
func (p *fileHandlePool) put(pointer []byte, handle io.ReadSeekCloser) {
	p.mu.Lock()
	entry := p.files[string(pointer)]
	if p.closed || entry == nil || entry.refs == 0 {
		p.mu.Unlock()
		handle.Close()
		return
	}
	entry.idle = append(entry.idle, handle)
	p.mu.Unlock()
}

// discard closes a handle instead of returning it to the pool. Readers use it
// for handles whose seek or read failed: the handle's stream position is
// unknown, and one bad handle must not poison the file's later readers.
func (p *fileHandlePool) discard(handle io.ReadSeekCloser) {
	handle.Close()
}

// closeAll closes every idle handle and refuses further acquires. The query
// runs it after its file and block workers have exited, so each handle it
// opened is by then either closed by its reader or idle here — closed exactly
// once, either way.
func (p *fileHandlePool) closeAll() {
	p.mu.Lock()
	p.closed = true
	files := p.files
	p.files = nil
	p.mu.Unlock()

	for _, entry := range files {
		closeHandles(entry.idle)
	}
}

func closeHandles(handles []io.ReadSeekCloser) {
	for _, handle := range handles {
		handle.Close()
	}
}
