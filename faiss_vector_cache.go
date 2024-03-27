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
	m     sync.RWMutex
	cache map[uint16]*cacheEntry
}

type ewma struct {
	alpha  float64
	avg    float64
	sample uint64
}

type cacheEntry struct {
	cacheMonitor *ewma
	closeCh      chan struct{}

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
	defer vc.m.Unlock()

	// close every cache monitor in the cache, thereby closing the index
	// cached as well.
	for _, c := range vc.cache {
		close(c.closeCh)
	}
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

	// the following cleanup is when the index is closed as part of cacheEntry.closeIndex()
	// but the corresponding field entry in the vecCache is not cleaned up
	// fixme: this looks ugly must be refactored in a better way.
	if rv == nil {
		vc.m.Lock()
		delete(vc.cache, fieldID)
		vc.m.Unlock()
	}

	return rv, present && (rv != nil)
}

func (vc *vecCache) addRef(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry := vc.cache[fieldIDPlus1]
	vc.m.RUnlock()

	entry.addRef()
}

func (vc *vecCache) update(fieldIDPlus1 uint16, index *faiss.IndexImpl) {
	vc.m.Lock()
	_, ok := vc.cache[fieldIDPlus1]
	if !ok {
		// initializing the alpha with 0.3 essentially means that we are favoring
		// the history a little bit more relative to the current sample value.
		// this makes the average to be kept above the threshold value for a
		// longer time and thereby the index to be resident in the cache
		// for longer time.
		// todo: alpha to be experimented with different values
		vc.cache[fieldIDPlus1] = initCacheEntry(index, 0.3)
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
		index:        index,
		closeCh:      make(chan struct{}),
		cacheMonitor: &ewma{},
	}
	vc.cacheMonitor.alpha = alpha
	go vc.monitor()

	// initing the sample to be 16 for now. more like a cold start to the monitor
	// with a large enough value so that we don't immediately evict the index
	atomic.StoreUint64(&vc.cacheMonitor.sample, 16)
	return vc
}

func (vc *cacheEntry) addRef() {
	// every access to the cache entry is accumulated as part of a sample
	// which will be used to calculate the average in the next cycle of average
	// computation
	atomic.AddUint64(&vc.cacheMonitor.sample, 1)
}

func (vc *cacheEntry) closeIndex() {
	vc.m.Lock()
	vc.index.Close()
	vc.index = nil
	vc.m.Unlock()
}

func (vc *cacheEntry) monitor() {
	// a timer to determing the frequency at which exponentially weighted
	// moving average is computed
	ticker := time.NewTicker(1 * time.Second)
	var sample uint64
	for {
		select {
		case <-vc.closeCh:
			vc.closeIndex()
			return
		case <-ticker.C:
			sample = atomic.LoadUint64(&vc.cacheMonitor.sample)
			vc.cacheMonitor.add(sample)
			// the comparison threshold as of now is (1 - a). mathematically it
			// means that there is only 1 query per second on average as per history.
			// and in the current second, there were no queries performed against
			// this index.
			// todo: threshold needs better experimentation. affects the time for
			// which the index is resident in the cache.
			if vc.cacheMonitor.avg <= (1 - vc.cacheMonitor.alpha) {
				atomic.StoreUint64(&vc.cacheMonitor.sample, 0)
				vc.closeIndex()
				return
			}
			atomic.StoreUint64(&vc.cacheMonitor.sample, 0)
		}
	}
}
