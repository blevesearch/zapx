//  Copyright (c) 2026 Couchbase, Inc.
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
	"container/heap"
	"math"
	"sort"
)

// Helpers shared by the sections that persist a sorted array of uint64 keys
// alongside a parallel array of uint32 payloads: the geo shape v2 section,
// whose keys are cell IDs and payloads are geo document IDs, and the number v2
// section, whose keys are sortable-encoded values and payloads are segment
// document numbers.

// pairDropped marks a payload whose document did not survive a merge.
const pairDropped = uint32(math.MaxUint32)

// sortedPairCursor iterates one segment's sorted (key, payload) pairs during a
// k-way merge. It skips pairs whose payload was dropped and translates each
// surviving payload into the merged segment's space.
type sortedPairCursor struct {
	keys     []uint64
	payloads []uint32 // parallel to keys

	// remap maps an old payload to its merged payload, or pairDropped if the
	// corresponding document was deleted during the merge.
	remap []uint32

	pos        int
	curKey     uint64
	curPayload uint32
}

// next advances to the next surviving pair, populating curKey and curPayload
// with the key and its remapped payload. It returns false once the cursor is
// exhausted.
func (c *sortedPairCursor) next() bool {
	for c.pos < len(c.keys) {
		key := c.keys[c.pos]
		newPayload := c.remap[c.payloads[c.pos]]
		c.pos++

		if newPayload == pairDropped {
			continue
		}
		c.curKey = key
		c.curPayload = newPayload
		return true
	}
	return false
}

// pairCursorHeap is a min-heap of cursors ordered by their current key, used to
// drive the k-way merge of the pre-sorted per-segment runs.
type pairCursorHeap []*sortedPairCursor

func (h pairCursorHeap) Len() int           { return len(h) }
func (h pairCursorHeap) Less(i, j int) bool { return h[i].curKey < h[j].curKey }
func (h pairCursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *pairCursorHeap) Push(x interface{}) {
	*h = append(*h, x.(*sortedPairCursor))
}

func (h *pairCursorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

// kWayMergePairs merges the pre-sorted runs held by the given cursors into
// ascending key order, appending the merged keys and their parallel payloads to
// outKeys and outPayloads respectively and returning the extended slices.
func kWayMergePairs(cursors []*sortedPairCursor, outKeys []uint64,
	outPayloads []uint32) ([]uint64, []uint32) {
	h := make(pairCursorHeap, 0, len(cursors))
	for _, c := range cursors {
		if c.next() {
			h = append(h, c)
		}
	}
	heap.Init(&h)

	for h.Len() > 0 {
		c := h[0]
		outKeys = append(outKeys, c.curKey)
		outPayloads = append(outPayloads, c.curPayload)
		if c.next() {
			// current cursor still has pairs; restore heap order in place
			heap.Fix(&h, 0)
		} else {
			// cursor exhausted; drop it from the heap
			heap.Pop(&h)
		}
	}

	return outKeys, outPayloads
}

// arrayPair holds references to both slices to swap and sort them in tandem.
// primary holds the sort keys; secondary holds the parallel payloads that must
// move with them.
type arrayPair struct {
	primary   []uint64
	secondary []uint32
}

func (a arrayPair) Len() int {
	return len(a.primary)
}

func (a arrayPair) Less(i, j int) bool {
	return a.primary[i] < a.primary[j]
}

func (a arrayPair) Swap(i, j int) {
	a.primary[i], a.primary[j] = a.primary[j], a.primary[i]
	a.secondary[i], a.secondary[j] = a.secondary[j], a.secondary[i]
}

func sortArrayPair(primary []uint64, secondary []uint32) ([]uint64, []uint32) {
	// Protect against mismatched slice lengths
	if len(primary) != len(secondary) {
		panic("slices must be of equal length")
	}

	sort.Sort(arrayPair{primary: primary, secondary: secondary})
	return primary, secondary
}
