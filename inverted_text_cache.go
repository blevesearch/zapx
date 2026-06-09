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

// WAND / MaxScore cache hierarchy
//
// Three levels exist, longest-lived first:
//
//  ┌─ Segment (SegmentBase.invIndexCache) ─────────── this file ───────┐
//  │  Per (field, term): maxTFNorm = max BM25 tf-norm across all docs   │
//  │  in this segment.  Lives as long as the segment file is open;      │
//  │  freed on segment GC (merge or close).  Shared by ALL concurrent   │
//  │  queries via RWMutex — many readers, one writer on first access.   │
//  └───────────────────────────────────────────────────────────────────-┘
//       ↑ aggregated by
//  ┌─ IndexSnapshotTermFieldReader.MaxTFNorm() ────────────────────────┐
//  │  Iterates all N segments, returns max.  Called once per query per  │
//  │  term via TermSearcher.MaxImpact().                                │
//  │  FUTURE: cache max-across-segments at IndexSnapshot level to       │
//  │  replace N segment lookups with one snapshot lookup per term.      │
//  └───────────────────────────────────────────────────────────────────┘
//       ↑ multiplied by IDF × queryNorm to give MaxImpact
//  ┌─ TermSearcher.cachedMaxImpact ────────────────────────────────────┐
//  │  Per-query, per-TermSearcher.  MaxImpact = IDF × maxTFNorm ×      │
//  │  queryNorm.  queryNorm = 1/√(Σweights²) is query-composition-     │
//  │  specific, so the product cannot be pushed below query level.      │
//  └───────────────────────────────────────────────────────────────────┘
//       ↑ read once into a flat slice by
//  ┌─ DisjunctionSliceSearcher.wandMaxImpacts ─────────────────────────┐
//  │  Per-query []float64, one slot per sub-searcher.  Eliminates       │
//  │  per-candidate type assertions and interface dispatch in the        │
//  │  wandAboveThreshold hot loop.                                       │
//  └───────────────────────────────────────────────────────────────────┘

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

// loadOrCreate loads the inverted index cache for the specified fieldID if it is already present,
// or creates it if not. The inverted index cache for a fieldID consists of an FST (Finite State Transducer):
// - A Vellum FST (Finite State Transducer) representing the TermDictionary.
// This function returns the loaded or newly created FST, and the number of bytes read from the provided memory slice,
// if the cache was created.
func (sc *invertedIndexCache) loadOrCreate(fieldID uint16, mem []byte, fr *FileReader) (*vellum.FST, uint64, error) {
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

	return sc.createAndCacheLOCKED(fieldID, mem, fr)
}

// createAndCacheLOCKED creates the inverted index cache for the specified fieldID and caches it.
func (sc *invertedIndexCache) createAndCacheLOCKED(fieldID uint16, mem []byte, fr *FileReader) (*vellum.FST, uint64, error) {
	var pos uint64
	vellumLen, read := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	if vellumLen == 0 || read <= 0 {
		return nil, 0, fmt.Errorf("vellum length is 0")
	}
	pos += uint64(read)
	fstBytes, err := fr.process(mem[pos : pos+vellumLen])
	if err != nil {
		return nil, 0, fmt.Errorf("error processing vellum bytes: %v", err)
	}
	fst, err := vellum.Load(fstBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("vellum err: %v", err)
	}
	pos += vellumLen
	sc.insertLOCKED(fieldID, fst)
	return fst, pos, nil
}

// insertLOCKED inserts the vellum FST into the cache for the specified fieldID.
func (sc *invertedIndexCache) insertLOCKED(fieldID uint16, fst *vellum.FST) {
	_, ok := sc.cache[fieldID]
	if !ok {
		sc.cache[fieldID] = &invertedCacheEntry{
			fst: fst,
		}
	}
}

// getOrCreateMaxTFNormEntry returns the invertedCacheEntry for fieldID,
// creating an empty entry if none exists yet (for segments that have no
// FST loaded but still need the maxTFNorm cache).
func (sc *invertedIndexCache) getOrCreateMaxTFNormEntry(fieldID uint16) *invertedCacheEntry {
	sc.m.RLock()
	entry, ok := sc.cache[fieldID]
	sc.m.RUnlock()
	if ok {
		return entry
	}
	sc.m.Lock()
	defer sc.m.Unlock()
	if entry, ok = sc.cache[fieldID]; ok {
		return entry
	}
	entry = &invertedCacheEntry{}
	if sc.cache == nil {
		sc.cache = make(map[uint16]*invertedCacheEntry)
	}
	sc.cache[fieldID] = entry
	return entry
}

// invertedCacheEntry is the per-field cache entry for a segment.
// It holds the vellum FST term dictionary and lazy caches used for
// WAND / MaxScore query pruning and repeated FST traversal avoidance.
type invertedCacheEntry struct {
	fst *vellum.FST

	// maxTFNorm caches, per term, the maximum BM25 tf-norm contribution
	// seen across all documents in this segment for that term.  Computed
	// lazily on first use; evicted automatically when the segment is
	// merged/GC'd.
	//
	// Eviction within a living segment:
	//   - capped at maxTFNormCacheSize entries (once full, new terms are
	//     skipped — hot terms queried first win the slots, which is the
	//     right policy for WAND)
	//   - invalidated per-entry when avgDocLength changes (stored alongside
	//     the cached value so a changed corpus triggers a recompute)
	maxTFNormMu    sync.RWMutex
	maxTFNormCache map[string]maxTFNormEntry

	// termOffsetCache maps term → posting-list offset within the segment
	// file, avoiding repeated FST traversals for repeated queries on the
	// same term.  Uses sync.Map (write-once, read-many pattern).
	// Only populated for terms that are found in the segment.
	termOffsetCache sync.Map // key: string, val: uint64
}

// maxTFNormEntry pairs a cached tf-norm max with the avgDocLength it was
// computed under.  If the corpus grows and avgDocLength shifts, the entry
// is recomputed on next access.
type maxTFNormEntry struct {
	avgDocLen float32 // avgDocLength at compute time (rounded to float32)
	value     float32 // max( sqrt(freq)×k1 / (sqrt(freq) + k1×(1−b+b×fl/avgdl)) )
}

// maxTFNormCacheSize caps the number of terms cached per field per segment.
// 100 000 entries × ~36 bytes/entry ≈ 3.6 MB per field per segment.
const maxTFNormCacheSize = 100_000

func (ce *invertedCacheEntry) load() (*vellum.FST, uint64, error) {
	return ce.fst, 0, nil
}

// getMaxTFNorm returns the cached maxTFNorm for term (with the given
// avgDocLength), or (0, false) on a miss.
func (ce *invertedCacheEntry) getMaxTFNorm(term string, avgDocLen float32) (float32, bool) {
	ce.maxTFNormMu.RLock()
	e, ok := ce.maxTFNormCache[term]
	ce.maxTFNormMu.RUnlock()
	if !ok || e.avgDocLen != avgDocLen {
		return 0, false
	}
	return e.value, true
}

// setMaxTFNorm stores the maxTFNorm for term if the cache is not full.
func (ce *invertedCacheEntry) setMaxTFNorm(term string, avgDocLen float32, v float32) {
	ce.maxTFNormMu.Lock()
	if len(ce.maxTFNormCache) < maxTFNormCacheSize {
		if ce.maxTFNormCache == nil {
			ce.maxTFNormCache = make(map[string]maxTFNormEntry, 256)
		}
		ce.maxTFNormCache[term] = maxTFNormEntry{avgDocLen: avgDocLen, value: v}
	}
	ce.maxTFNormMu.Unlock()
}
