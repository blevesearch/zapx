//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type numericIndexCache struct {
	m     sync.RWMutex
	cache map[uint16]*numericCacheEntry

	closeCh  chan struct{}
	isClosed bool
}

var numericMonitorFreq = 1 * time.Second

func newNumericIndexCache() *numericIndexCache {
	return &numericIndexCache{
		cache:   make(map[uint16]*numericCacheEntry),
		closeCh: make(chan struct{}),
	}
}

func (nc *numericIndexCache) Clear() {
	nc.m.Lock()
	if nc.isClosed {
		nc.m.Unlock()
		return
	}
	nc.isClosed = true
	close(nc.closeCh)

	for _, entry := range nc.cache {
		entry.Close()
	}
	nc.cache = nil
	nc.m.Unlock()
}

// loadOrCreate returns the cached arrays for the field, parsing them out of mem
// on first use. The caller owns one reference on the returned entry and must
// Close it. A nil entry with a nil error means the cache has been closed.
func (nc *numericIndexCache) loadOrCreate(field uint16, mem []byte,
	r *FileReader) (*numericCacheEntry, error) {
	nc.m.RLock()
	if nc.isClosed {
		nc.m.RUnlock()
		return nil, nil
	}

	entry, ok := nc.cache[field]
	if ok {
		nc.m.RUnlock()
		return entry.load(), nil
	}
	nc.m.RUnlock()

	nc.m.Lock()
	defer nc.m.Unlock()
	if nc.isClosed {
		return nil, nil
	}

	entry, ok = nc.cache[field]
	if ok {
		return entry.load(), nil
	}

	return nc.createAndCacheLocked(field, mem, r)
}

func (nc *numericIndexCache) createAndCacheLocked(field uint16, mem []byte,
	r *FileReader) (*numericCacheEntry, error) {
	var pos uint64

	// Load the format version
	formatVersion, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if formatVersion != numericV2FormatVersion {
		return nil, fmt.Errorf("unsupported number_v2 format version %d",
			formatVersion)
	}

	// Load the number of entries
	numEntries, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if numEntries == 0 {
		return nil, fmt.Errorf("no numeric entries found")
	}

	// Load the values, sorted ascending
	values, valuesMem, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load the segment doc numbers parallel to the values
	docNums, docNumsMem, _, err := r.ReadUint32Array(mem[pos:])
	if err != nil {
		return nil, err
	}

	if len(values) != len(docNums) {
		return nil, fmt.Errorf("number_v2 arrays disagree in length: "+
			"%d values, %d doc numbers", len(values), len(docNums))
	}

	rv := &numericCacheEntry{
		values:    values,
		valuesMem: valuesMem,

		docNums:    docNums,
		docNumsMem: docNumsMem,

		numEntries: numEntries,

		tracker: &ewma{
			alpha:  0.4,
			sample: 1,
		},
		refs: 1,
	}

	nc.insertLOCKED(field, rv)

	return rv, nil
}

func (nc *numericIndexCache) insertLOCKED(field uint16, entry *numericCacheEntry) {
	if len(nc.cache) == 0 {
		go nc.monitor()
	}

	nc.cache[field] = entry
}

func (nc *numericIndexCache) monitor() {
	ticker := time.NewTicker(numericMonitorFreq)
	defer ticker.Stop()

	for {
		select {
		case <-nc.closeCh:
			return
		case <-ticker.C:
			exit := nc.cleanup()
			if exit {
				return
			}
		}
	}
}

// cleanup runs one eviction pass over the cache. For each entry it folds the
// hit count accumulated since the last pass into the entry's moving average
// (see ewma.add), then evicts entries that have no outstanding references and
// whose average traffic has decayed to at most (1 - alpha).
// Returns true when the cache is empty, signalling the monitor goroutine to exit.
func (nc *numericIndexCache) cleanup() bool {
	nc.m.Lock()

	for field, entry := range nc.cache {
		sample := atomic.LoadUint64(&entry.tracker.sample)
		entry.tracker.add(sample)

		refCount := atomic.LoadInt64(&entry.refs)

		if refCount <= 0 && entry.tracker.avg <= (1-entry.tracker.alpha) {
			atomic.StoreUint64(&entry.tracker.sample, 0)
			delete(nc.cache, field)
			entry.Close()
			continue
		}
		atomic.StoreUint64(&entry.tracker.sample, 0)
	}

	rv := len(nc.cache) == 0
	nc.m.Unlock()
	return rv
}

// numericCacheEntry is a cached entry for one field's number_v2 arrays. The
// *Mem slices are the byte buffers the value slices were decoded from; when
// ReadUint*Array returned a zero-copy view over the segment's mmap they keep
// that memory referenced for as long as the entry lives.
//
// Unlike the geo equivalent there is no per-load wrapper: the doc numbers are
// already in segment doc-number space, so there is no exclusion bitmap to
// translate and nothing about a load is caller-specific.
type numericCacheEntry struct {
	values    []uint64
	valuesMem []byte

	docNums    []uint32
	docNumsMem []byte

	numEntries uint64

	tracker *ewma
	refs    int64
}

func (nce *numericCacheEntry) Values() []uint64 {
	return nce.values
}

func (nce *numericCacheEntry) DocNums() []uint32 {
	return nce.docNums
}

func (nce *numericCacheEntry) NumEntries() uint64 {
	return nce.numEntries
}

func (nce *numericCacheEntry) Close() {
	nce.decRef()
}

func (nce *numericCacheEntry) load() *numericCacheEntry {
	nce.incHits()
	nce.incRef()
	return nce
}

func (nce *numericCacheEntry) incHits() {
	atomic.AddUint64(&nce.tracker.sample, 1)
}

func (nce *numericCacheEntry) incRef() {
	atomic.AddInt64(&nce.refs, 1)
}

func (nce *numericCacheEntry) decRef() {
	atomic.AddInt64(&nce.refs, -1)
}
