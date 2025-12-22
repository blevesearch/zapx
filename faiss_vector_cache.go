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
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	faiss "github.com/blevesearch/go-faiss"
)

// -----------------------------------------------------------------------------
type vectorSelector struct {
	bitset []byte
	size   uint32
	count  uint32
}

// newVectorSelector creates a new vectorSelector with the given number of vectors
func newVectorSelector(numVecs uint32) *vectorSelector {
	bitmapSize := (numVecs + 7) / 8
	return &vectorSelector{
		bitset: make([]byte, bitmapSize),
		size:   numVecs,
		count:  0,
	}
}

// Set sets the bit at the given ID
func (b *vectorSelector) set(id uint32) {
	if id >= b.size {
		return
	}
	byteIndex := id / 8
	bitIndex := id % 8
	b.bitset[byteIndex] |= 1 << bitIndex
	b.count++
}

// Clear clears the bit at the given ID
func (b *vectorSelector) clear(id uint32) {
	if id >= b.size {
		return
	}
	byteIndex := id / 8
	bitIndex := id % 8
	b.bitset[byteIndex] &^= 1 << bitIndex
	b.count--
}

// SetMany sets the bits at the given IDs
func (b *vectorSelector) setMany(ids []int64) {
	for _, id := range ids {
		b.set(uint32(id))
	}
}

// SetMany sets the bits at the given IDs
func (b *vectorSelector) setMany32(ids []uint32) {
	for _, id := range ids {
		b.set(id)
	}
}

func (b *vectorSelector) clearMany(ids []int64) {
	for _, id := range ids {
		b.clear(uint32(id))
	}
}

func (b *vectorSelector) clearMany32(ids []uint32) {
	for _, id := range ids {
		b.clear(id)
	}
}

// Test returns true if the bit at the given ID is set
func (b *vectorSelector) test(id uint32) bool {
	if id >= b.size {
		return false
	}
	byteIndex := id / 8
	bitIndex := id % 8
	return (b.bitset[byteIndex]>>bitIndex)&1 == 1
}

// Bytes returns the raw []byte bitmap for use with C API
// This returns the underlying bitset slice directly
func (b *vectorSelector) bytes() []byte {
	return b.bitset
}

// active returns the number of selected IDs
func (b *vectorSelector) active() uint32 {
	return b.count
}

// size returns the size of the selector
func (b *vectorSelector) total() uint32 {
	return b.size
}

func (b *vectorSelector) clone() *vectorSelector {
	newVS := newVectorSelector(b.size)
	newVS.bitset = slices.Clone(b.bitset)
	newVS.count = b.count
	return newVS
}

func (b *vectorSelector) slice() []int64 {
	ids := make([]int64, 0, b.count)
	for i := uint32(0); i < b.size; i++ {
		if b.test(i) {
			ids = append(ids, int64(i))
		}
	}
	return ids
}

// -----------------------------------------------------------------------------
type idMapping struct {
	vecToDoc []uint32   // maps vector ID -> document ID (size = numVecs)
	docToVec [][]uint32 // maps document ID -> vector IDs (size = numDocs)
}

// newIDMapping creates a new idMapping with the specified sizes
// numVecs: number of vectors (for vecToDoc mapping)
// numDocs: number of documents (for docToVec mapping)
func newIDMapping(numVecs, numDocs uint32) *idMapping {
	return &idMapping{
		vecToDoc: make([]uint32, numVecs),
		docToVec: make([][]uint32, numDocs),
	}
}

func (m *idMapping) set(vecID uint32, docID uint32) {
	m.vecToDoc[vecID] = docID
	m.docToVec[docID] = append(m.docToVec[docID], vecID)
}

func (m *idMapping) numVectors() uint32 {
	return uint32(len(m.vecToDoc))
}

func (m *idMapping) numDocuments() uint32 {
	return uint32(len(m.docToVec))
}

func (m *idMapping) iterateVectors(f func(vecID uint32, docID uint32)) {
	for vecID, docID := range m.vecToDoc {
		f(uint32(vecID), docID)
	}
}

func (m *idMapping) docForVec(vecID uint32) (uint32, bool) {
	if vecID >= uint32(len(m.vecToDoc)) {
		return 0, false
	}
	return m.vecToDoc[vecID], true
}

func (m *idMapping) vecsForDoc(docID uint32) ([]uint32, bool) {
	if docID >= uint32(len(m.docToVec)) {
		return nil, false
	}
	return m.docToVec[docID], true
}

// -----------------------------------------------------------------------------

func newVectorIndexCache() *vectorIndexCache {
	return &vectorIndexCache{
		cache:   make(map[uint16]*cacheEntry),
		closeCh: make(chan struct{}),
	}
}

type vectorIndexCache struct {
	closeCh chan struct{}
	m       sync.RWMutex
	cache   map[uint16]*cacheEntry
}

func (vc *vectorIndexCache) Clear() {
	vc.m.Lock()
	close(vc.closeCh)

	// forcing a close on all indexes to avoid memory leaks.
	for _, entry := range vc.cache {
		entry.close()
	}
	vc.cache = nil
	vc.m.Unlock()
}

// loadOrCreate obtains the vector index from the cache or creates it if it's not
// present. It also returns the batch executor for the field if it's present in the
// cache.
func (vc *vectorIndexCache) loadOrCreate(fieldID uint16, mem []byte, numDocs uint32, except *roaring.Bitmap) (
	index *faiss.IndexImpl, mapping *idMapping, exclude *vectorSelector, err error) {
	vc.m.RLock()
	entry, ok := vc.cache[fieldID]
	if ok {
		vc.m.RUnlock()
		return entry.load(except)
	}

	vc.m.RUnlock()

	vc.m.Lock()
	defer vc.m.Unlock()

	entry, ok = vc.cache[fieldID]
	if ok {
		return entry.load(except)
	}

	return vc.createAndCacheLOCKED(fieldID, mem, numDocs, except)
}

// Rebuilding the cache on a miss.
func (vc *vectorIndexCache) createAndCacheLOCKED(fieldID uint16, mem []byte,
	numDocs uint32, except *roaring.Bitmap) (
	index *faiss.IndexImpl, mapping *idMapping, exclude *vectorSelector, err error) {
	// if the cache doesn't have the entry, construct the vector to doc id map and
	// the vector index out of the mem bytes and update the cache under lock.
	pos := 0
	numVecs, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if n <= 0 {
		return nil, nil, nil, fmt.Errorf("could not read numVecs")
	}
	// if no vectors or no documents, return empty cache entry
	if numVecs == 0 || numDocs == 0 {
		return nil, nil, nil, nil
	}
	pos += n
	// read the length of the vector to docID map (unused for now)
	_, n = binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if n <= 0 {
		return nil, nil, nil, fmt.Errorf("could not read vecDocIDMap length")
	}
	pos += n
	// create a mapping using the numVecs and numDocs
	mapping = newIDMapping(uint32(numVecs), numDocs)
	for i := uint32(0); i < uint32(numVecs); i++ {
		docID, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
		if n <= 0 {
			return nil, nil, nil, fmt.Errorf("could not read docID for vecID %d", i)
		}
		pos += n
		mapping.set(uint32(i), uint32(docID))
	}
	// read the type of the vector index (unused for now)
	_, n = binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if n <= 0 {
		return nil, nil, nil, fmt.Errorf("could not read faiss index type")
	}
	pos += n
	// read the faiss index size
	indexSize, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if n <= 0 {
		return nil, nil, nil, fmt.Errorf("could not read faiss index size")
	}
	pos += n
	// read the serialized vector index
	index, err = faiss.ReadIndexFromBuffer(mem[pos:pos+int(indexSize)], faissIOFlags)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("faiss index load error: %v", err)
	}
	// update the cache
	vc.insertLOCKED(fieldID, index, mapping)
	return index, mapping, getExcludedVectors(mapping, except), nil
}

func (vc *vectorIndexCache) insertLOCKED(fieldIDPlus1 uint16,
	index *faiss.IndexImpl, mapping *idMapping) {
	// the first time we've hit the cache, try to spawn a monitoring routine
	// which will reconcile the moving averages for all the fields being hit
	if len(vc.cache) == 0 {
		go vc.monitor()
	}
	// initializing the alpha with 0.4 essentially means that we are favoring
	// the history a little bit more relative to the current sample value.
	// this makes the average to be kept above the threshold value for a
	// longer time and thereby the index to be resident in the cache
	// for longer time.
	vc.cache[fieldIDPlus1] = createCacheEntry(index, mapping, 0.4)
}

func (vc *vectorIndexCache) incHit(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry, ok := vc.cache[fieldIDPlus1]
	if ok {
		entry.incHit()
	}
	vc.m.RUnlock()
}

func (vc *vectorIndexCache) decRef(fieldIDPlus1 uint16) {
	vc.m.RLock()
	entry, ok := vc.cache[fieldIDPlus1]
	if ok {
		entry.decRef()
	}
	vc.m.RUnlock()
}

func (vc *vectorIndexCache) cleanup() bool {
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
			delete(vc.cache, fieldIDPlus1)
			entry.close()
			continue
		}
		atomic.StoreUint64(&entry.tracker.sample, 0)
	}

	rv := len(vc.cache) == 0
	vc.m.Unlock()
	return rv
}

var monitorFreq = 1 * time.Second

func (vc *vectorIndexCache) monitor() {
	ticker := time.NewTicker(monitorFreq)
	defer ticker.Stop()
	for {
		select {
		case <-vc.closeCh:
			return
		case <-ticker.C:
			exit := vc.cleanup()
			if exit {
				// no entries to be monitored, exit
				return
			}
		}
	}
}

// -----------------------------------------------------------------------------

type ewma struct {
	alpha float64
	avg   float64
	// every hit to the cache entry is recorded as part of a sample
	// which will be used to calculate the average in the next cycle of average
	// computation (which is average traffic for the field till now). this is
	// used to track the per second hits to the cache entries.
	sample uint64
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

// -----------------------------------------------------------------------------

func createCacheEntry(index *faiss.IndexImpl, mapping *idMapping, alpha float64) *cacheEntry {
	ce := &cacheEntry{
		index:   index,
		mapping: mapping,
		tracker: &ewma{
			alpha:  alpha,
			sample: 1,
		},
		refs: 1,
	}
	return ce
}

type cacheEntry struct {
	tracker *ewma

	// this is used to track the live references to the cache entry,
	// such that while we do a cleanup() and we see that the avg is below a
	// threshold we close/cleanup only if the live refs to the cache entry is 0.
	refs int64

	index   *faiss.IndexImpl
	mapping *idMapping
}

func (ce *cacheEntry) incHit() {
	atomic.AddUint64(&ce.tracker.sample, 1)
}

func (ce *cacheEntry) addRef() {
	atomic.AddInt64(&ce.refs, 1)
}

func (ce *cacheEntry) decRef() {
	atomic.AddInt64(&ce.refs, -1)
}

func (ce *cacheEntry) load(except *roaring.Bitmap) (*faiss.IndexImpl, *idMapping, *vectorSelector, error) {
	ce.incHit()
	ce.addRef()
	return ce.index, ce.mapping, getExcludedVectors(ce.mapping, except), nil
}

func (ce *cacheEntry) close() {
	go func() {
		ce.index.Close()
		ce.index = nil
		ce.mapping = nil
	}()
}

// -----------------------------------------------------------------------------

func getExcludedVectors(idMap *idMapping, except *roaring.Bitmap) (exclude *vectorSelector) {
	numVecs := idMap.numVectors()
	if except != nil && !except.IsEmpty() {
		idMap.iterateVectors(func(vecID uint32, docID uint32) {
			if except.Contains(docID) {
				if exclude == nil {
					exclude = newVectorSelector(numVecs)
				}
				exclude.set(vecID)
			}
		})
	}
	return exclude
}
