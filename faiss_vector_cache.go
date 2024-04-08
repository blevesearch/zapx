//  Copyright (c) 2024 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package zap

import (
	"sync"
	"sync/atomic"
	"time"

	faiss "github.com/blevesearch/go-faiss"
)

type vecIndexCache struct {
	closeCh chan struct{}
	m       sync.RWMutex
	cache   map[uint16]*cacheEntry
}

type ewma struct {
	alpha  float64
	avg    float64
	sample uint64
}

type cacheEntry struct {
	tracker *ewma
	refs    int64

	m     sync.RWMutex
	index *faiss.IndexImpl
}

func newVectorIndexCache() *vecIndexCache {
	return &vecIndexCache{
		cache:   make(map[uint16]*cacheEntry),
		closeCh: make(chan struct{}),
	}
}

func (vc *vecIndexCache) Clear() {
	vc.m.Lock()
	close(vc.closeCh)
	vc.cache = nil
	vc.m.Unlock()
}

func (vc *vecIndexCache) loadVectorIndex(fieldID uint16,
	indexBytes []byte) (vecIndex *faiss.IndexImpl, err error) {
	cachedIndex, present := vc.isIndexCached(fieldID)
	if present {
		vecIndex = cachedIndex
		vc.incHit(fieldID)
	} else {
		vecIndex, err = vc.createAndCacheVectorIndex(fieldID, indexBytes)
	}
	vc.addRef(fieldID)
	return vecIndex, err
}

func (vc *vecIndexCache) createAndCacheVectorIndex(fieldID uint16,
	indexBytes []byte) (*faiss.IndexImpl, error) {
	vc.m.Lock()
	defer vc.m.Unlock()

	// when there are multiple threads trying to build the index, guard redundant
	// index creation by doing a double check and return if already created and
	// cached.
	entry, present := vc.cache[fieldID]
	if present {
		entry.m.RLock()
		rv := entry.index
		entry.incHit()
		entry.m.RUnlock()
		return rv, nil
	}

	// if the cache doesn't have vector index, just construct it out of the
	// index bytes and update the cache under lock.
	vecIndex, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
	vc.updateLOCKED(fieldID, vecIndex)
	return vecIndex, err
}
func (vc *vecIndexCache) updateLOCKED(fieldIDPlus1 uint16, index *faiss.IndexImpl) {
	// the first time we've hit the cache, try to spawn a monitoring routine
	// which will reconcile the moving averages for all the fields being hit
	if len(vc.cache) == 0 {
		go vc.monitor()
	}

	_, ok := vc.cache[fieldIDPlus1]
	if !ok {
		//  initializing the alpha with 0.4 essentially means that we are favoring
		// 	the history a little bit more relative to the current sample value.
		// 	this makes the average to be kept above the threshold value for a
		// 	longer time and thereby the index to be resident in the cache
		// 	for longer time.
		vc.cache[fieldIDPlus1] = initCacheEntry(index, 0.4)
	}
}

func (vc *vecIndexCache) isIndexCached(fieldID uint16) (*faiss.IndexImpl, bool) {
	vc.m.RLock()
	entry, present := vc.cache[fieldID]
	vc.m.RUnlock()
	if entry == nil {
		return nil, false
	}

	entry.m.RLock()
	rv := entry.index
	entry.m.RUnlock()

	return rv, present && (rv != nil)
}

func (vc *vecIndexCache) incHit(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry := vc.cache[fieldIDPlus1]
	entry.incHit()
	vc.m.RUnlock()
}

func (vc *vecIndexCache) addRef(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry := vc.cache[fieldIDPlus1]
	entry.addRef()
	vc.m.RUnlock()
}

func (vc *vecIndexCache) decRef(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry := vc.cache[fieldIDPlus1]
	entry.decRef()
	vc.m.RUnlock()
}

func (vc *vecIndexCache) refresh() bool {
	vc.m.Lock()
	cache := vc.cache

	// for every field reconcile the average with the current sample values
	for fieldIDPlus1, entry := range cache {
		sample := atomic.LoadUint64(&entry.tracker.sample)
		entry.tracker.add(sample)

		refCount := atomic.LoadInt64(&entry.refs)
		// the comparison threshold as of now is (1 - a). mathematically it
		// means that there is only 1 query per second on average as per history.
		// and in the current second, there were no queries performed against
		// this index.
		if entry.tracker.avg <= (1-entry.tracker.alpha) && refCount <= 0 {
			atomic.StoreUint64(&entry.tracker.sample, 0)
			entry.closeIndex()
			delete(vc.cache, fieldIDPlus1)
			continue
		}
		atomic.StoreUint64(&entry.tracker.sample, 0)
	}

	rv := len(vc.cache) == 0
	vc.m.Unlock()
	return rv
}

func (vc *vecIndexCache) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-vc.closeCh:
			return
		case <-ticker.C:
			exit := vc.refresh()
			if exit {
				// no entries to be monitored, exit
				return
			}
		}
	}
}

func (e *ewma) add(val uint64) {
	if e.avg == 0.0 {
		e.avg = float64(val)
	} else {
		// the exponentially weighted moving average
		// X(t) = a.v + (1 - a).X(t-1)
		e.avg = e.alpha*float64(val) + (1-e.alpha)*e.avg
	}
}

func initCacheEntry(index *faiss.IndexImpl, alpha float64) *cacheEntry {
	vc := &cacheEntry{
		index:   index,
		tracker: &ewma{},
	}
	vc.tracker.alpha = alpha

	atomic.StoreUint64(&vc.tracker.sample, 1)
	return vc
}

func (vc *cacheEntry) incHit() {
	// every access to the cache entry is accumulated as part of a sample
	// which will be used to calculate the average in the next cycle of average
	// computation
	atomic.AddUint64(&vc.tracker.sample, 1)
}

func (vc *cacheEntry) addRef() {
	atomic.AddInt64(&vc.refs, 1)
}

func (vc *cacheEntry) decRef() {
	atomic.AddInt64(&vc.refs, -1)
}

func (vc *cacheEntry) closeIndex() {
	vc.m.Lock()
	vc.index.Close()
	vc.index = nil
	vc.m.Unlock()
}