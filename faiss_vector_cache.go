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

type vecCache struct {
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

	m     sync.RWMutex
	index *faiss.IndexImpl
}

func newVectorCache() *vecCache {
	return &vecCache{
		cache: make(map[uint16]*cacheEntry),
	}
}

func (vc *vecCache) Clear() {
	vc.m.Lock()
	close(vc.closeCh)
	vc.cache = nil
	vc.m.Unlock()

}

func (vc *vecCache) checkCacheForVecIndex(fieldID uint16,
	indexBytes []byte) (vecIndex *faiss.IndexImpl, err error) {
	cachedIndex, present := vc.isVecIndexCached(fieldID)
	if present {
		vecIndex = cachedIndex
		vc.addRef(fieldID)
	} else {
		// if the cache doesn't have vector index, just construct it out of the
		// index bytes and update the cache.
		vecIndex, err = faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
		vc.update(fieldID, vecIndex)
	}
	return vecIndex, err
}

func (vc *vecCache) isVecIndexCached(fieldID uint16) (*faiss.IndexImpl, bool) {
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

func (vc *vecCache) addRef(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry := vc.cache[fieldIDPlus1]
	vc.m.RUnlock()

	entry.addRef()
}

func (vc *vecCache) clearField(fieldIDPlus1 uint16) {
	vc.m.Lock()
	delete(vc.cache, fieldIDPlus1)
	vc.m.Unlock()
}

func (vc *vecCache) refreshEntries() (rv int) {
	vc.m.RLock()
	cache := vc.cache
	vc.m.RUnlock()

	defer func() {
		vc.m.RLock()
		rv = len(vc.cache)
		vc.m.RUnlock()
	}()

	// for every field reconcile the average with the current sample values
	for fieldIDPlus1, entry := range cache {
		sample := atomic.LoadUint64(&entry.tracker.sample)
		entry.tracker.add(sample)
		// the comparison threshold as of now is (1 - a). mathematically it
		// means that there is only 1 query per second on average as per history.
		// and in the current second, there were no queries performed against
		// this index.
		if entry.tracker.avg <= (1 - entry.tracker.alpha) {
			atomic.StoreUint64(&entry.tracker.sample, 0)
			entry.closeIndex()
			vc.clearField(fieldIDPlus1)
			continue
		}
		atomic.StoreUint64(&entry.tracker.sample, 0)
	}
	return
}

func (vc *vecCache) monitor() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-vc.closeCh:
			return
		case <-ticker.C:
			numEntries := vc.refreshEntries()
			if numEntries == 0 {
				// no entries to be monitored, exit
				return
			}
		}
	}
}

func (vc *vecCache) update(fieldIDPlus1 uint16, index *faiss.IndexImpl) {
	vc.m.Lock()

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
	vc.m.Unlock()
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

func (vc *cacheEntry) addRef() {
	// every access to the cache entry is accumulated as part of a sample
	// which will be used to calculate the average in the next cycle of average
	// computation
	atomic.AddUint64(&vc.tracker.sample, 1)
}

func (vc *cacheEntry) closeIndex() {
	vc.m.Lock()
	vc.index.Close()
	vc.index = nil
	vc.m.Unlock()
}
