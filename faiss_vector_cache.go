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
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
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

	refs          int64
	index         *faiss.IndexImpl
	vecDocIDMap   map[int64]uint32
	docIDExcluded map[uint32]struct{}
	vecExcluded   []int64
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

func (vc *vecIndexCache) loadFromCache(fieldID uint16, mem []byte, except *roaring.Bitmap) (map[int64]uint32, []int64, *faiss.IndexImpl, error) {
	cachedEntry, present := vc.checkEntry(fieldID)

	var vecIndex *faiss.IndexImpl
	var vecDocIDMap map[int64]uint32
	var vecExcluded []int64
	var err error

	if present {
		vecIndex, vecDocIDMap, vecExcluded = cachedEntry.load(except)
	} else {
		vecIndex, vecDocIDMap, vecExcluded, err = vc.createAndCacheEntry(fieldID, mem, except)
	}

	vc.addRef(fieldID)
	return vecDocIDMap, vecExcluded, vecIndex, err
}

func (vc *vecIndexCache) createAndCacheEntry(fieldID uint16,
	mem []byte, except *roaring.Bitmap) (*faiss.IndexImpl, map[int64]uint32, []int64, error) {
	vc.m.Lock()
	defer vc.m.Unlock()

	// when there are multiple threads trying to build the index, guard redundant
	// index creation by doing a double check and return if already created and
	// cached.
	entry, present := vc.cache[fieldID]
	if present {
		vecIndex, vecDocIDMap, vecExcluded := entry.load(except)
		return vecIndex, vecDocIDMap, vecExcluded, nil
	}

	pos := 0
	numVecs, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	vecExcluded := make([]int64, 0)
	docIDExcluded := make(map[uint32]struct{})
	vecDocIDMap := make(map[int64]uint32)
	for i := 0; i < int(numVecs); i++ {
		vecID, n := binary.Varint(mem[pos : pos+binary.MaxVarintLen64])
		pos += n
		docID, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		pos += n

		docIDUint32 := uint32(docID)
		if except != nil && except.Contains(docIDUint32) {
			// populate the list of vector IDs to be ignored on search
			vecExcluded = append(vecExcluded, vecID)
			docIDExcluded[docIDUint32] = struct{}{}
			// also, skip adding entry to vecDocIDMap
			continue
		}
		vecDocIDMap[vecID] = docIDUint32
	}

	indexSize, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	vecIndex, err := faiss.ReadIndexFromBuffer(mem[pos:pos+int(indexSize)], faiss.IOFlagReadOnly)
	if err != nil {
		return nil, nil, nil, err
	}

	vc.updateLOCKED(fieldID, vecIndex, vecDocIDMap, docIDExcluded, vecExcluded)
	return vecIndex, vecDocIDMap, vecExcluded, nil
}
func (vc *vecIndexCache) updateLOCKED(fieldIDPlus1 uint16, index *faiss.IndexImpl, vecDocIDMap map[int64]uint32, docIDExcluded map[uint32]struct{}, vecExcluded []int64) {
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

func (vc *vecIndexCache) checkEntry(fieldID uint16) (*cacheEntry, bool) {
	vc.m.RLock()
	entry, present := vc.cache[fieldID]
	vc.m.RUnlock()
	if entry == nil {
		return nil, false
	}

	return entry, present && (entry != nil)
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

func (vc *vecIndexCache) update(fieldIDPlus1 uint16, index *faiss.IndexImpl, vecDocIDMap map[int64]uint32, docIDExcluded map[uint32]struct{}, vecExcluded []int64) {
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
		vc.cache[fieldIDPlus1] = initCacheEntry(vecIndex, vecDocIDMap, docIDExcluded, vecExcluded, 0.4)
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

func initCacheEntry(vecIndex *faiss.IndexImpl, vecDocIDMap map[int64]uint32, docIDExcluded map[uint32]struct{}, vecExcluded []int64, alpha float64) *cacheEntry {
	vc := &cacheEntry{
		index:         vecIndex,
		vecDocIDMap:   vecDocIDMap,
		docIDExcluded: docIDExcluded,
		vecExcluded:   vecExcluded,
		tracker:       &ewma{},
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
	vc.index.Close()
	vc.index = nil
	vc.docIDExcluded = nil
	vc.vecDocIDMap = nil
	vc.vecExcluded = nil
}

func (vc *cacheEntry) load(except *roaring.Bitmap) (*faiss.IndexImpl, map[int64]uint32, []int64) {

	vc.m.RLock()
	vecIndex := vc.index
	vecDocIDMap := vc.vecDocIDMap
	docIDExcluded := vc.docIDExcluded
	vecExcluded := vc.vecExcluded
	vc.m.RUnlock()

	if except != nil {
		newExcluded := false
		vc.m.Lock()
		it := except.Iterator()
		for it.HasNext() {
			docID := it.Next()
			if _, exists := docIDExcluded[docID]; !exists {
				docIDExcluded[docID] = struct{}{}
				newExcluded = true
			}
		}

		if newExcluded {
			for vecID, docID := range vecDocIDMap {
				if _, exists := docIDExcluded[docID]; exists {
					delete(vecDocIDMap, vecID)
					vecExcluded = append(vecExcluded, vecID)
				}
			}
			vc.vecDocIDMap = vecDocIDMap
			vc.docIDExcluded = docIDExcluded
			vc.vecExcluded = vecExcluded
		}
		vc.m.Unlock()
	}

	vc.incHit()
	return vecIndex, vecDocIDMap, vecExcluded
}
