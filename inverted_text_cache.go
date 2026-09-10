//  Copyright (c) 2025 Couchbase, Inc.
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

func newInvertedIndexCache() *invertedIndexCache {
	return &invertedIndexCache{
		cache: make(map[uint16]*invertedCacheEntry),
	}
}

type invertedIndexCache struct {
	m sync.RWMutex

	cache map[uint16]*invertedCacheEntry
}

func (sc *invertedIndexCache) Clear() {
	sc.m.Lock()
	sc.cache = nil
	sc.m.Unlock()
}

// dictLocation names the two regions of a field's inverted index section that
// the cache materialises: the term dictionary and the field-norm column.
type dictLocation struct {
	dictOffset  uint64
	normsOffset uint64
	normsLen    uint64
	numDocs     uint64
}

// loadOrCreate loads the inverted index cache for the specified fieldID if it is already present,
// or creates it if not. The inverted index cache for a fieldID consists of:
// - A Vellum FST (Finite State Transducer) representing the TermDictionary.
// - The field's quantized field-norm column, which every term of the field reads.
// This function returns the loaded or newly created entries, and the number of bytes read from
// the provided memory slice, if the cache was created.
func (sc *invertedIndexCache) loadOrCreate(fieldID uint16, mem []byte, loc dictLocation,
	fr *FileReader) (*vellum.FST, *normsColumn, uint64, error) {
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

	return sc.createAndCacheLOCKED(fieldID, mem, loc, fr)
}

// createAndCacheLOCKED creates the inverted index cache for the specified fieldID and caches it.
func (sc *invertedIndexCache) createAndCacheLOCKED(fieldID uint16, mem []byte, loc dictLocation,
	fr *FileReader) (*vellum.FST, *normsColumn, uint64, error) {
	pos := loc.dictOffset
	vellumLen, read := binary.Uvarint(memAt(mem, pos, binary.MaxVarintLen64))
	if vellumLen == 0 || read <= 0 {
		return nil, nil, 0, fmt.Errorf("vellum length is 0")
	}
	pos += uint64(read)
	fstBytes, err := fr.process(mem[pos : pos+vellumLen])
	if err != nil {
		return nil, nil, 0, fmt.Errorf("error processing vellum bytes: %v", err)
	}
	fst, err := vellum.Load(fstBytes)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("vellum err: %v", err)
	}
	pos += vellumLen
	bytesRead := pos - loc.dictOffset

	// The norm column is decoded once per field rather than per term, which is
	// the point of it living here: a term lookup then costs a single byte load.
	norms := normsAbsent
	if loc.normsLen > 0 {
		normBytes, err := fr.process(mem[loc.normsOffset : loc.normsOffset+loc.normsLen])
		if err != nil {
			return nil, nil, 0, fmt.Errorf("error processing norms bytes: %v", err)
		}
		norms, err = decodeNormsColumn(normBytes, loc.numDocs)
		if err != nil {
			return nil, nil, 0, err
		}
		bytesRead += loc.normsLen
	}

	sc.insertLOCKED(fieldID, fst, norms)
	return fst, norms, bytesRead, nil
}

// insertLOCKED inserts the vellum FST and norm column into the cache for the specified fieldID.
func (sc *invertedIndexCache) insertLOCKED(fieldID uint16, fst *vellum.FST, norms *normsColumn) {
	_, ok := sc.cache[fieldID]
	if !ok {
		sc.cache[fieldID] = &invertedCacheEntry{
			fst:   fst,
			norms: norms,
		}
	}
}

// invertedCacheEntry is the per-field inverted index state stored in the
// invertedIndexCache: the vellum FST and the field-norm column.
type invertedCacheEntry struct {
	fst   *vellum.FST
	norms *normsColumn
}

func (ce *invertedCacheEntry) load() (*vellum.FST, *normsColumn, uint64, error) {
	return ce.fst, ce.norms, 0, nil
}
