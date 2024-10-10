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
	"fmt"
	"sync"

	"github.com/blevesearch/vellum"
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

func (sc *synonymIndexCache) loadOrCreate(fieldID uint16, mem []byte) (*vellum.FST, map[uint32][]byte, error) {
	sc.m.RLock()
	entry, ok := sc.cache[fieldID]
	if ok {
		sc.m.RUnlock()
		return entry.load()
	}

	sc.m.RUnlock()

	sc.m.Lock()
	defer sc.m.Unlock()

	entry, ok = sc.cache[fieldID]
	if ok {
		return entry.load()
	}

	return sc.createAndCacheLOCKED(fieldID, mem)
}

func (sc *synonymIndexCache) createAndCacheLOCKED(fieldID uint16, mem []byte) (*vellum.FST, map[uint32][]byte, error) {
	var pos uint64
	// read the length of the vellum data
	vellumLen, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if vellumLen == 0 || read <= 0 {
		return nil, nil, fmt.Errorf("vellum length is 0")
	}
	fstBytes := mem[pos+uint64(read) : pos+uint64(read)+vellumLen]
	fst, err := vellum.Load(fstBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("vellum err: %v", err)
	}
	synTermMap := make(map[uint32][]byte)
	numSyns, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if numSyns == 0 {
		return nil, nil, fmt.Errorf("no synonyms found")
	}
	for i := 0; i < int(numSyns); i++ {
		synID, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		termLen, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		if termLen == 0 {
			return nil, nil, fmt.Errorf("term length is 0")
		}
		term := mem[pos : pos+uint64(termLen)]
		synTermMap[uint32(synID)] = term
	}
	sc.insertLOCKED(fieldID, fst, synTermMap)
	return fst, synTermMap, nil
}

func (sc *synonymIndexCache) insertLOCKED(fieldID uint16, fst *vellum.FST, synTermMap map[uint32][]byte) {
	_, ok := sc.cache[fieldID]
	if !ok {
		sc.cache[fieldID] = &synonymCacheEntry{
			fst:        fst,
			synTermMap: synTermMap,
		}
	}
}

type synonymCacheEntry struct {
	fst        *vellum.FST
	synTermMap map[uint32][]byte
}

func (ce *synonymCacheEntry) load() (*vellum.FST, map[uint32][]byte, error) {
	return ce.fst, ce.synTermMap, nil
}
