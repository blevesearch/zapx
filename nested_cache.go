package zap

import (
	"encoding/binary"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
)

type nestedIndexCache struct {
	cache *nestedCacheEntry
}

// newNestedIndexCache creates a new nested index cache instance, which contains cached edge list
// for a nested segment, pass in the edgeListOffset and memory slice of the segment to initialize it.
func newNestedIndexCache() *nestedIndexCache {
	return &nestedIndexCache{}
}

func (nc *nestedIndexCache) initialize(edgeListOffset uint64, mem []byte) {
	// pos stores the current read position
	pos := edgeListOffset
	// read number of subDocs which is also the number of edges
	numEdges := binary.BigEndian.Uint64(mem[pos : pos+8])
	pos += 8
	// if no edges or no subDocs, return
	if numEdges == 0 {
		return
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
	nc.cache = &nestedCacheEntry{
		edgeList: edgeList,
	}
}

type nestedCacheEntry struct {
	// edgeList[child] = parent
	edgeList map[uint64]uint64
}

// Clear clears the nested index cache, removing the cached edge list
func (nc *nestedIndexCache) Clear() {
	nc.cache = nil
}
func (nc *nestedIndexCache) ancestry(docNum uint64, prealloc []index.AncestorID) []index.AncestorID {
	cache := nc.cache
	// add self as first ancestor
	prealloc = append(prealloc, index.NewAncestorID(docNum))
	if cache == nil || cache.edgeList == nil {
		return prealloc
	}
	current := docNum
	for {
		parent, ok := cache.edgeList[current]
		if !ok {
			break
		}
		prealloc = append(prealloc, index.NewAncestorID(parent))
		current = parent
	}
	return prealloc
}

func (nc *nestedIndexCache) edgeList() map[uint64]uint64 {
	cache := nc.cache
	if cache == nil || cache.edgeList == nil {
		return nil
	}
	return cache.edgeList
}

func (nc *nestedIndexCache) countNested() uint64 {
	cache := nc.cache
	if cache == nil {
		return 0
	}
	return uint64(len(cache.edgeList))
}

// countRoot returns the number of root documents in the given bitmap
func (nc *nestedIndexCache) countRoot(bm *roaring.Bitmap) uint64 {
	var totalDocs uint64
	if bm == nil {
		// if bitmap is empty, return 0
		return totalDocs
	}
	totalDocs = bm.GetCardinality()
	cache := nc.cache
	if cache == nil {
		// if cache is nil, no nested docs, so all docs are root docs
		// so just return the cardinality of the bitmap
		return totalDocs
	}
	// count nested documents in the bitmap, a nested doc is one that has a parent in the edge list
	var nestedDocCount uint64
	bm.Iterate(func(docNum uint32) bool {
		if _, ok := cache.edgeList[uint64(docNum)]; ok {
			nestedDocCount++
		}
		return true
	})
	// root docs = total docs - nested docs
	if totalDocs < nestedDocCount {
		// should not happen, but just in case
		return 0
	}
	return totalDocs - nestedDocCount
}
