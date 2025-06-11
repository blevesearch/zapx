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

func newNestedIndexCache() *nestedIndexCache {
	return &nestedIndexCache{
		cache: make(map[uint16]*nestedCacheEntry),
	}
}

type nestedIndexCache struct {
	m     sync.RWMutex
	cache map[uint16]*nestedCacheEntry
}

func (sc *nestedIndexCache) Clear() {
	sc.m.Lock()
	sc.cache = nil
	sc.m.Unlock()
}

func (sc *nestedIndexCache) loadOrCreate(fieldID uint16, arrayPos int, field string, mem []byte, pos uint64) (*vellum.FST, uint64, uint64, error) {
	sc.m.RLock()
	entry, ok := sc.cache[fieldID]
	if ok {
		sc.m.RUnlock()
		return entry.load(arrayPos, field)
	}
	sc.m.RUnlock()
	sc.m.Lock()
	defer sc.m.Unlock()
	entry, ok = sc.cache[fieldID]
	if ok {
		return entry.load(arrayPos, field)
	}
	err := sc.createAndCacheLOCKED(fieldID, mem, pos)
	if err != nil {
		return nil, 0, 0, err
	}
	entry = sc.cache[fieldID]
	if entry == nil {
		return nil, 0, 0, fmt.Errorf("nested index cache entry for fieldID %d is nil", fieldID)
	}
	return entry.load(arrayPos, field)
}

func (sc *nestedIndexCache) createAndCacheLOCKED(fieldID uint16, mem []byte, pos uint64) error {
	numArrayPositions, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if numArrayPositions == 0 || read <= 0 {
		return fmt.Errorf("numArrayPositions length is 0")
	}
	pos += uint64(read)
	arrayPosOffsets := make([]uint64, int(numArrayPositions))
	for arrayPos := 0; arrayPos < int(numArrayPositions); arrayPos++ {
		offset, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		if offset == 0 || read <= 0 {
			return fmt.Errorf("arrayPos offset length is 0")
		}
		pos += uint64(read)
		arrayPosOffsets[arrayPos] = offset
	}
	fieldOffsets := make([][]*nestedFieldLocs, int(numArrayPositions))
	fieldsMap := make([]map[string]uint16, int(numArrayPositions))
	for i := range fieldsMap {
		fieldsMap[i] = make(map[string]uint16)
	}
	for arrayPos, offset := range arrayPosOffsets {
		pos = offset
		numFields, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		if numFields == 0 || read <= 0 {
			return fmt.Errorf("numFields length is 0")
		}
		pos += uint64(read)
		for fieldID := 0; fieldID < int(numFields); fieldID++ {
			fieldNameLen, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
			if fieldNameLen == 0 || read <= 0 {
				return fmt.Errorf("fieldName length is 0")
			}
			pos += uint64(read)
			fieldName := mem[pos : pos+fieldNameLen]
			pos += fieldNameLen
			dvStartOffset, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
			if read <= 0 {
				return fmt.Errorf("dvStartOffset length is 0")
			}
			pos += uint64(read)
			dvEndOffset, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
			if read <= 0 {
				return fmt.Errorf("dvEndOffset length is 0")
			}
			pos += uint64(read)
			dictStartOffset, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
			if read <= 0 {
				return fmt.Errorf("dictStartOffset length is 0")
			}
			pos += uint64(read)
			fieldsMap[arrayPos][string(fieldName)] = uint16(fieldID)
			fieldOffsets[arrayPos] = append(fieldOffsets[arrayPos], &nestedFieldLocs{
				dvStart:   dvStartOffset,
				dvEnd:     dvEndOffset,
				dictStart: dictStartOffset,
			})
		}
	}
	sc.cache[fieldID] = &nestedCacheEntry{
		fieldsMap:         fieldsMap,
		fieldsLoc:         fieldOffsets,
		fieldFST:          make([]map[uint16]*vellum.FST, int(numArrayPositions)),
		numArrayPositions: int(numArrayPositions),
		mem:               mem,
	}
	return nil
}

type nestedFieldLocs struct {
	dvStart   uint64
	dvEnd     uint64
	dictStart uint64
}

type nestedCacheEntry struct {
	fieldsMap         []map[string]uint16
	fieldsLoc         [][]*nestedFieldLocs
	mem               []byte
	numArrayPositions int
	mu                sync.RWMutex
	fieldFST          []map[uint16]*vellum.FST
}

func (e *nestedCacheEntry) load(arrayPos int, field string) (*vellum.FST, uint64, uint64, error) {
	if arrayPos < 0 || arrayPos >= e.numArrayPositions {
		return nil, 0, 0, fmt.Errorf("array position %d out of bounds for numArrayPositions %d", arrayPos, e.numArrayPositions)
	}
	fieldID, ok := e.fieldsMap[arrayPos][field]
	if !ok {
		return nil, 0, 0, fmt.Errorf("field %q not found in array position %d", field, arrayPos)
	}
	if fieldID >= uint16(len(e.fieldsLoc[arrayPos])) {
		return nil, 0, 0, fmt.Errorf("fieldID %d out of bounds for array position %d", fieldID, arrayPos)
	}
	locs := e.fieldsLoc[arrayPos][fieldID]
	if locs == nil {
		return nil, 0, 0, fmt.Errorf("no field locations found for fieldID %d in array position %d", fieldID, arrayPos)
	}
	var fst *vellum.FST
	var dvStart = locs.dvStart
	var dvEnd = locs.dvEnd
	var err error
	e.mu.RLock()
	fstMap := e.fieldFST[arrayPos]
	if fstMap != nil {
		fst = fstMap[fieldID]
		if fst != nil {
			e.mu.RUnlock()
			return fst, dvStart, dvEnd, nil
		}
	}
	e.mu.RUnlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	fstMap = e.fieldFST[arrayPos]
	if fstMap != nil {
		fst = fstMap[fieldID]
		if fst != nil {
			return fst, dvStart, dvEnd, nil
		}
	}
	pos := locs.dictStart
	vellumLen, read := binary.Uvarint(e.mem[pos : pos+binary.MaxVarintLen64])
	if vellumLen == 0 || read <= 0 {
		return nil, 0, 0, fmt.Errorf("vellum length is 0")
	}
	pos += uint64(read)
	fstBytes := e.mem[pos : pos+vellumLen]
	fst, err = vellum.Load(fstBytes)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("vellum err: %v", err)
	}
	pos += vellumLen
	if e.fieldFST[arrayPos] == nil {
		e.fieldFST[arrayPos] = make(map[uint16]*vellum.FST, len(e.fieldsMap[arrayPos]))
	}
	e.fieldFST[arrayPos][fieldID] = fst
	return fst, dvStart, dvEnd, nil
}
