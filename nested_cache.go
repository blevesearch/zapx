package zap

import (
	"encoding/binary"
	"sync"
)

func newNestedIndexCache() *nestedIndexCache {
	return &nestedIndexCache{}
}

type nestedIndexCache struct {
	m sync.RWMutex

	cache *nestedCacheEntry
}

// Clear clears the synonym cache which would mean that the termID to term map would no longer be available.
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

// createAndCacheLOCKED creates the synonym index cache for the specified fieldID and caches it.
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
	// build ancestry using DFS + memoization
	ancestry := make(map[uint64][]uint64, numEdges)
	// memoized DFS
	var getAncestors func(uint64) []uint64
	getAncestors = func(node uint64) []uint64 {
		// if already computed, return
		if val, ok := ancestry[node]; ok {
			return val
		}
		if parent, ok := edgeList[node]; ok {
			// compute parent's ancestry + parent itself
			res := append([]uint64{parent}, getAncestors(parent)...)
			ancestry[node] = res
			return res
		}
		return nil
	}

	for child := range edgeList {
		// only store if non-empty ancestry
		if v := getAncestors(child); len(v) > 0 {
			ancestry[child] = v
		}
	}

	descendants := make(map[uint64][]uint64, numEdges)

	// Build descendants using ancestry
	for node := uint64(0); node < numEdges; node++ {
		if ancestors, ok := ancestry[node]; ok {
			for _, ancestor := range ancestors {
				descendants[ancestor] = append(descendants[ancestor], node)
			}
		}
	}

	sc.cache = &nestedCacheEntry{
		edgeList:    edgeList,
		ancestry:    ancestry,
		descendants: descendants,
	}

	return sc.cache
}

func (nc *nestedIndexCache) getAncestry(edgeListOffset uint64, mem []byte, docNum uint64) []uint64 {
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil || cache.ancestry == nil {
		return nil
	}
	return cache.ancestry[docNum]
}

func (nc *nestedIndexCache) getDescendants(edgeListOffset uint64, mem []byte, docNum uint64) []uint64 {
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil || cache.ancestry == nil {
		return nil
	}
	return cache.descendants[docNum]
}

func (nc *nestedIndexCache) getEdgeList(edgeListOffset uint64, mem []byte) map[uint64]uint64 {
	cache := nc.loadOrCreate(edgeListOffset, mem)
	if cache == nil || cache.ancestry == nil {
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

type nestedCacheEntry struct {
	// edgeList[node] = parent
	edgeList map[uint64]uint64
	// ancestry[node] = list of parents
	ancestry map[uint64][]uint64
	// descendants[parent] = list of children
	descendants map[uint64][]uint64
}
