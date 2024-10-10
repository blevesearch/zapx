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

package zap

import (
	"encoding/binary"
	"sync"
)

func newSynonymIndexCache() *synonymIndexCache {
	return &synonymIndexCache{
		cache:   make(map[uint16]*synonymCacheEntry),
		closeCh: make(chan struct{}),
	}
}

type synonymIndexCache struct {
	closeCh chan struct{}
	m       sync.RWMutex

	cache map[uint16]*synonymCacheEntry
}

func (sc *synonymIndexCache) Clear() {
	sc.m.Lock()
	sc.cache = nil
	sc.m.Unlock()
}

func (sc *synonymIndexCache) loadOrCreate(thesaurusID uint16, mem []byte) map[uint32][]byte {

	sc.m.RLock()

	entry, ok := sc.cache[thesaurusID]
	if ok {
		sc.m.RUnlock()
		return entry.load()
	}

	sc.m.RUnlock()

	sc.m.Lock()
	defer sc.m.Unlock()

	entry, ok = sc.cache[thesaurusID]
	if ok {
		return entry.load()
	}

	return sc.createAndCacheLOCKED(thesaurusID, mem)
}

func (sc *synonymIndexCache) load(thesaurusID uint16) (map[uint32][]byte, bool) {
	sc.m.RLock()
	defer sc.m.RUnlock()

	entry, ok := sc.cache[thesaurusID]
	if !ok {
		return nil, false
	}

	return entry.load(), true
}

func (sc *synonymIndexCache) createAndCacheLOCKED(thesaurusID uint16, mem []byte) map[uint32][]byte {
	synTermMap := make(map[uint32][]byte)
	pos := 0
	numSyns, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += n
	for i := 0; i < int(numSyns); i++ {
		synID, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		pos += n

		termLen, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		pos += n

		term := mem[pos : pos+int(termLen)]

		synTermMap[uint32(synID)] = term
	}
	sc.insertLOCKED(thesaurusID, synTermMap)
	return synTermMap
}

func (sc *synonymIndexCache) insertLOCKED(thesaurusID uint16, synTermMap map[uint32][]byte) {
	_, ok := sc.cache[thesaurusID]
	if !ok {
		// initializing the alpha with 0.4 essentially means that we are favoring
		// the history a little bit more relative to the current sample value.
		// this makes the average to be kept above the threshold value for a
		// longer time and thereby the index to be resident in the cache
		// for longer time.
		sc.cache[thesaurusID] = createSynonymCacheEntry(synTermMap)
	}
}

func createSynonymCacheEntry(synTermMap map[uint32][]byte) *synonymCacheEntry {
	ce := &synonymCacheEntry{
		synTermMap: synTermMap,
	}
	return ce
}

type synonymCacheEntry struct {
	synTermMap map[uint32][]byte
}

func (ce *synonymCacheEntry) load() map[uint32][]byte {
	return ce.synTermMap
}
