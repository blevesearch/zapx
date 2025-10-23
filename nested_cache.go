package zap

import (
	"encoding/binary"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

func newNestedIndexCache() *nestedIndexCache {
	return &nestedIndexCache{}
}

type nestedIndexCache struct {
	m sync.RWMutex

	cache *nestedCacheEntry
}

// Clear clears the nested index cache, removing cached edge lists, ancestry, and descendants.
func (nc *nestedIndexCache) Clear() {
	nc.m.Lock()
	nc.cache = nil
	nc.m.Unlock()
}

// Returns edgeList, ancestry, descendants
func (nc *nestedIndexCache) loadOrCreate(edgeListOffset uint64, mem []byte) *nestedCacheEntry {
	nc.m.RLock()
	if nc.cache != nil {
		nc.m.RUnlock()
		return nc.cache
	}
	nc.m.RUnlock()

	nc.m.Lock()
	defer nc.m.Unlock()

	if nc.cache != nil {
		return nc.cache
	}

	return nc.createAndCacheLOCKED(edgeListOffset, mem)
}

// createAndCacheLOCKED creates and caches a nested cache entry (edge list, ancestry, descendants) for the given edge list offset and memory slice.
func (sc *nestedIndexCache) createAndCacheLOCKED(edgeListOffset uint64, mem []byte) *nestedCacheEntry {
	// pos stores the current read position
	pos := edgeListOffset
	// read number of subDocs which is also the number of edges
	numEdges := binary.BigEndian.Uint64(mem[pos : pos+8])
	pos += 8
	// if no edges or no subDocs, return empty cache
	if numEdges == 0 {
		sc.cache = &nestedCacheEntry{}
		return sc.cache
	}
	// edgeList as a map[node]parent
	edgeList := make(map[uint64]uint64, numEdges)
	for i := uint64(0); i < numEdges; i++ {
		child := binary.BigEndian.Uint64(mem[pos : pos+8])
		pos += 8
		parent := binary.BigEndian.Uint64(mem[pos : pos+8])
		pos += 8
		edgeList[child] = parent
	}

	sc.cache = &nestedCacheEntry{
		edgeList: edgeList,
	}

	return sc.cache
}

func getDocAncestors(edgeList map[uint64]uint64, docNum uint64) []uint64 {
	var ancestors []uint64
	current := docNum
	for {
		parent, ok := edgeList[current]
		if !ok {
			break
		}
		ancestors = append(ancestors, parent)
		current = parent
	}
	return ancestors
}

func (nc *nestedIndexCache) getAncestry(edgeListOffset uint64, mem []byte, docNum uint64) []uint64 {
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil || cache.edgeList == nil {
		return nil
	}
	return getDocAncestors(cache.edgeList, docNum)
}

func (nc *nestedIndexCache) getEdgeList(edgeListOffset uint64, mem []byte) map[uint64]uint64 {
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil || cache.edgeList == nil {
		return nil
	}
	return cache.edgeList
}

func (nc *nestedIndexCache) getNumSubDocs(edgeListOffset uint64, mem []byte) uint64 {
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil {
		return 0
	}
	return uint64(len(cache.edgeList))
}

// countRoot returns the number of root documents in the given bitmap
func (nc *nestedIndexCache) countRoot(edgeListOffset uint64, mem []byte, bm *roaring.Bitmap) uint64 {
	var totalDocs uint64
	if bm == nil {
		// if bitmap is empty, return 0
		return totalDocs
	}
	totalDocs = bm.GetCardinality()
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil {
		// if cache is nil, no nested docs, so all docs are root docs
		// so just return the cardinality of the bitmap
		return totalDocs
	}
	// count sub docs in the bitmap, a sub doc is one that has a parent in the edge list
	var subDocCount uint64
	bm.Iterate(func(docNum uint32) bool {
		if _, ok := cache.edgeList[uint64(docNum)]; ok {
			subDocCount++
		}
		return true
	})
	// root docs = total docs - sub docs
	if totalDocs < subDocCount {
		// should not happen, but just in case
		return 0
	}
	return totalDocs - subDocCount
}

type nestedCacheEntry struct {
	// edgeList[node] = parent
	edgeList map[uint64]uint64
}
