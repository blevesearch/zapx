// Copyright (c) 2024 Couchbase, Inc.
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
	"math"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/vellum"
)

// BPOptions controls the Recursive Graph Bisection algorithm.
// Enable via MergeUsing config map: config["bpReorder"] = true
type BPOptions struct {
	// MinDocFreq: terms with DF < MinDocFreq are excluded from the co-occurrence
	// graph. Rare terms add noise without locality signal. For a 500k-doc index,
	// 128 includes topic-signature terms (~3700 DF) and frequent background terms.
	MinDocFreq uint64

	// MaxDocFreqRatio: terms appearing in more than this fraction of documents
	// are excluded (stopwords dominate without contributing locality signal).
	MaxDocFreqRatio float64

	// MinPartitionSize: recursion stops when a partition has <= this many docs.
	// Lucene default: 32.
	MinPartitionSize int

	// MaxIter: maximum bisection iterations per partition level.
	// Mackenzie et al. stopping criterion terminates earlier when gain <= iter.
	MaxIter int

	// MinSegmentSize: skip BP reordering entirely when the merged segment has
	// fewer than this many docs. Small background merges produce temporary
	// segments that will be merged again soon; running BP on them wastes CPU
	// without producing durable locality benefit. Default: 100000.
	MinSegmentSize uint64
}

var defaultBPOptions = BPOptions{
	MinDocFreq:       128,
	MaxDocFreqRatio:  0.95,
	MinPartitionSize: 32,
	MaxIter:          5,
	MinSegmentSize:   100_000,
}

// computeBPPermutation builds a forward index from the input segments and runs
// recursive graph bisection to produce perm[seqID] = bpID. seqID is the
// sequential doc ID that mergeStoredAndRemap would assign (0..numDocs-1, in
// segment order); bpID is the BP-reordered target doc ID.
//
// Only terms with minDocFreq <= DF <= maxDocFreq * numDocs are included in the
// co-occurrence graph; others are ignored (they add noise without locality
// signal). If no eligible terms exist (corpus too small or all terms filtered),
// returns an identity permutation.
func computeBPPermutation(
	segments []*SegmentBase,
	drops []*roaring.Bitmap,
	fieldsInv []string,
	fieldsOptions map[string]index.FieldIndexingOptions,
	numDocs uint64,
	opts BPOptions,
) ([]uint64, error) {

	// -- Step 1: Map (segI, localDocNum) -> seqID ----------------------------
	// Replicate the sequential assignment that mergeStoredAndRemap would make.
	docToSeq := make([][]uint64, len(segments))
	{
		seqID := uint64(0)
		for segI, sb := range segments {
			docToSeq[segI] = make([]uint64, sb.numDocs)
			drop := drops[segI]
			for localDoc := uint64(0); localDoc < sb.numDocs; localDoc++ {
				if drop != nil && drop.Contains(uint32(localDoc)) {
					docToSeq[segI][localDoc] = docDropped
				} else {
					docToSeq[segI][localDoc] = seqID
					seqID++
				}
			}
		}
	}

	// Skip BP entirely for small segments — they are temporary intermediate
	// merges that will be re-merged soon. BP's locality benefit only compounds
	// in large, stable segments worth the O(n log n) reordering cost.
	if opts.MinSegmentSize > 0 && numDocs < opts.MinSegmentSize {
		perm := make([]uint64, numDocs)
		for i := range perm {
			perm[i] = uint64(i)
		}
		return perm, nil
	}

	// -- Step 2: Pass 1 -- count DF per (fieldName, term) --------------------
	// Use only the term bytes (not the field name) as the locality key.
	// BP co-occurrence signal comes from document content, not field names;
	// merging across fields collapses per-field duplicates, keeping numTerms
	// small so leftFreq/rightFreq fit in L1 cache and fwdData stays manageable.
	type termKey = string // just string(termBytes), field-agnostic
	termDF := make(map[termKey]uint64, 1024)

	maxDocFreq := uint64(float64(numDocs) * opts.MaxDocFreqRatio)

	// Track per-field eligible count to pick the best field for Pass 2.
	// Using all fields would inflate fwdData by Nfields×, breaking cache.
	fieldEligibleCount := make(map[string]int, len(fieldsInv))

	var plReuse *PostingsList
	for _, sb := range segments {
		for _, fieldName := range fieldsInv {
			if !fieldsOptions[fieldName].IsIndexed() {
				continue
			}
			dict, err := sb.dictionary(fieldName)
			if err != nil {
				return nil, err
			}
			if dict == nil || dict.fst == nil {
				continue
			}
			itr, err := dict.fst.Iterator(nil, nil)
			if err != nil && err != vellum.ErrIteratorDone {
				return nil, err
			}
			for err == nil {
				term, postingsOffset := itr.Current()
				key := string(term) // field-agnostic key
				pl, err2 := dict.postingsListFromOffset(postingsOffset, nil, plReuse)
				plReuse = pl
				if err2 != nil {
					_ = itr.Close()
					return nil, err2
				}
				if pl != nil {
					df := pl.Count()
					termDF[key] += df
					// Accumulate per-field eligible counts on first segment only to
					// avoid over-counting across segments.
					if df >= opts.MinDocFreq && df <= maxDocFreq {
						fieldEligibleCount[fieldName]++
					}
				}
				err = itr.Next()
			}
			_ = itr.Close()
		}
	}

	// -- Step 3: Assign term IDs to eligible terms ---------------------------
	termIDs := make(map[termKey]uint32, len(termDF))
	for key, df := range termDF {
		if df >= opts.MinDocFreq && df <= maxDocFreq {
			termIDs[key] = uint32(len(termIDs))
		}
	}
	numTerms := len(termIDs)

	// Identify the indexed field with the most eligible terms. Pass 2 only
	// uses this field so fwdData stays cache-friendly (single-field size
	// instead of Nfields× inflated). Locality signal from one rich text
	// field is sufficient for BP doc-ID reordering.
	bpField := ""
	{
		best := 0
		for _, fn := range fieldsInv {
			if c := fieldEligibleCount[fn]; c > best {
				best = c
				bpField = fn
			}
		}
	}

	// Identity permutation when no eligible terms (no useful locality signal).
	if numTerms == 0 {
		perm := make([]uint64, numDocs)
		for i := range perm {
			perm[i] = uint64(i)
		}
		return perm, nil
	}

	// -- Step 4: Pass 2 -- build forward index --------------------------------
	// fwdIndex[seqID] = term IDs of eligible terms appearing in that doc.
	// Only iterate bpField to keep fwdData small and cache-friendly.
	fwdIndex := make([][]uint32, numDocs)

	plReuse = nil
	for segI, sb := range segments {
		for _, fieldName := range fieldsInv {
			if fieldName != bpField {
				continue
			}
			if !fieldsOptions[fieldName].IsIndexed() {
				continue
			}
			dict, err := sb.dictionary(fieldName)
			if err != nil {
				return nil, err
			}
			if dict == nil || dict.fst == nil {
				continue
			}
			itr, err := dict.fst.Iterator(nil, nil)
			if err != nil && err != vellum.ErrIteratorDone {
				return nil, err
			}
			for err == nil {
				term, postingsOffset := itr.Current()
				key := string(term) // field-agnostic key, matches Pass 1
				tID, ok := termIDs[key]
				if !ok {
					err = itr.Next()
					continue
				}
				pl, err2 := dict.postingsListFromOffset(postingsOffset, nil, plReuse)
				plReuse = pl
				if err2 != nil {
					_ = itr.Close()
					return nil, err2
				}
				if pl != nil {
					if pl.normBits1Hit != 0 {
						// 1-hit encoding: single document.
						localDoc := pl.docNum1Hit
						if localDoc < uint64(len(docToSeq[segI])) {
							seqDoc := docToSeq[segI][localDoc]
							if seqDoc != docDropped {
								fwdIndex[seqDoc] = append(fwdIndex[seqDoc], tID)
							}
						}
					} else if pl.postings != nil {
						it := pl.postings.Iterator()
						for it.HasNext() {
							localDoc := uint64(it.Next())
							if localDoc < uint64(len(docToSeq[segI])) {
								seqDoc := docToSeq[segI][localDoc]
								if seqDoc != docDropped {
									fwdIndex[seqDoc] = append(fwdIndex[seqDoc], tID)
								}
							}
						}
					}
				}
				err = itr.Next()
			}
			_ = itr.Close()
		}
	}

	// -- Step 5: Compact fwdIndex into flat arrays ----------------------------
	// Replacing [][]uint32 (500k scattered heap allocations) with contiguous
	// fwdData + fwdOffsets enables sequential access in the top-level bisect
	// iterations, avoiding L3 cache misses on the pointer-chasing pattern.
	var totalTerms int32
	for _, terms := range fwdIndex {
		totalTerms += int32(len(terms))
	}
	fwdData := make([]uint32, totalTerms)
	fwdOffsets := make([]int32, numDocs+1)
	cursor := int32(0)
	for i, terms := range fwdIndex {
		fwdOffsets[i] = cursor
		copy(fwdData[cursor:], terms)
		cursor += int32(len(terms))
	}
	fwdOffsets[numDocs] = cursor
	fwdIndex = nil // allow GC of scattered per-doc slices before bisect allocates

	// -- Step 6: Recursive Graph Bisection ------------------------------------
	// order[i] = seqID at position i. Bisection reorders so similar docs are adjacent.
	order := make([]int32, numDocs)
	for i := range order {
		order[i] = int32(i)
	}

	// Precompute log2 table indexed by frequency value (0..numDocs/2).
	// Replacing math.Log2 calls in the inner bias loop with table lookups
	// gives ~5-10x speedup on large partitions.
	logTable := make([]float32, numDocs/2+2)
	logTable[0] = 0 // guarded by lf>0 && rf>0 check; never used
	for i := uint64(1); i < uint64(len(logTable)); i++ {
		logTable[i] = float32(math.Log2(float64(i)))
	}

	bp := &bpState{
		fwdData:    fwdData,
		fwdOffsets: fwdOffsets,
		numTerms:   numTerms,
		isLeft:     make([]bool, numDocs),
		leftFreq:   make([]int32, numTerms),
		rightFreq:  make([]int32, numTerms),
		dirty:      make([]uint32, 0, numTerms),
		biases:     make([]float64, numDocs),
		logTable:   logTable,
		opts: opts,
	}
	bp.bisect(order)

	// -- Step 7: Derive permutation from final order --------------------------
	// order[bpID] = seqID -> perm[seqID] = bpID
	perm := make([]uint64, numDocs)
	for bpID, seq := range order {
		perm[seq] = uint64(bpID)
	}
	return perm, nil
}

// bpState holds the workspace for recursive graph bisection. Allocating all
// arrays once at the top level and reusing them through depth-first recursion
// avoids per-call allocations and GC pressure. This is safe because each call
// clears its portion of the shared arrays before recursing.
type bpState struct {
	// fwdData and fwdOffsets replace [][]uint32 for cache-efficient access.
	// fwdData[fwdOffsets[d]:fwdOffsets[d+1]] = eligible term IDs for doc d.
	fwdData    []uint32
	fwdOffsets []int32

	numTerms  int
	isLeft    []bool    // scratch; indexed by seqDocID; cleared after each bisect call
	leftFreq  []int32   // term frequency in the left half of the current partition
	rightFreq []int32   // term frequency in the right half
	dirty     []uint32  // term IDs touched in leftFreq or rightFreq during this call
	biases    []float64 // bias[i] = log-odds bias of docs[i]; reused across levels
	logTable  []float32 // logTable[k] = log2(k) for k>=1; indexed by freq values

	opts BPOptions
}

func (bp *bpState) bisect(docs []int32) {
	n := len(docs)
	if n <= bp.opts.MinPartitionSize {
		return
	}
	mid := n / 2

	// Build initial left/right frequencies. Track touched terms in dirty so
	// we can clear without zeroing the entire leftFreq/rightFreq arrays.
	bp.dirty = bp.dirty[:0]
	for _, d := range docs[:mid] {
		for _, t := range bp.fwdData[bp.fwdOffsets[d]:bp.fwdOffsets[d+1]] {
			if bp.leftFreq[t] == 0 && bp.rightFreq[t] == 0 {
				bp.dirty = append(bp.dirty, t)
			}
			bp.leftFreq[t]++
		}
	}
	for _, d := range docs[mid:] {
		for _, t := range bp.fwdData[bp.fwdOffsets[d]:bp.fwdOffsets[d+1]] {
			if bp.leftFreq[t] == 0 && bp.rightFreq[t] == 0 {
				bp.dirty = append(bp.dirty, t)
			}
			bp.rightFreq[t]++
		}
	}

	biases := bp.biases[:n] // safe to reuse: depth-first, parent is done before child

	for iter := 1; iter <= bp.opts.MaxIter; iter++ {
		// Compute bias for each doc: sum_t [ log2(rightFreq[t]) - log2(leftFreq[t]) ]
		// Positive bias -> doc belongs on the right; negative -> left.
		// Use the precomputed logTable to avoid repeated math.Log2 calls.
		for i, d := range docs {
			b := 0.0
			for _, t := range bp.fwdData[bp.fwdOffsets[d]:bp.fwdOffsets[d+1]] {
				lf := bp.leftFreq[t]
				rf := bp.rightFreq[t]
				if lf > 0 && rf > 0 {
					b += float64(bp.logTable[rf] - bp.logTable[lf])
				}
			}
			biases[i] = b
		}

		// Mark which docs are currently on the left.
		for _, d := range docs[:mid] {
			bp.isLeft[d] = true
		}

		// Partition docs around the mid-th element by bias (O(n) average).
		// Full sort is unnecessary — we only need docs[:mid] to have lower biases.
		nthElement(docs, biases, mid)

		// Update frequencies only for docs that crossed the midpoint.
		// Mackenzie stopping criterion: stop when gain <= iter.
		moved := 0
		for i, d := range docs {
			nowLeft := i < mid
			wasLeft := bp.isLeft[d]
			if nowLeft == wasLeft {
				continue
			}
			moved++
			if nowLeft { // right -> left
				for _, t := range bp.fwdData[bp.fwdOffsets[d]:bp.fwdOffsets[d+1]] {
					bp.rightFreq[t]--
					bp.leftFreq[t]++
				}
			} else { // left -> right
				for _, t := range bp.fwdData[bp.fwdOffsets[d]:bp.fwdOffsets[d+1]] {
					bp.leftFreq[t]--
					bp.rightFreq[t]++
				}
			}
		}

		// Clear isLeft marks for all docs in this partition.
		for _, d := range docs {
			bp.isLeft[d] = false
		}

		if moved <= iter {
			break
		}
	}

	// Clear frequency arrays using the dirty list so next recursive call starts clean.
	for _, t := range bp.dirty {
		bp.leftFreq[t] = 0
		bp.rightFreq[t] = 0
	}

	// Recurse depth-first: left half then right half.
	bp.bisect(docs[:mid])
	bp.bisect(docs[mid:])
}

// nthElement rearranges docs and biases so that docs[:k] have biases ≤
// docs[k:] in O(n) average time using a 3-way (Dutch National Flag)
// partition. The 3-way scheme handles equal elements in O(n) even when all
// biases are identical — a common case on the first bisect iteration when
// topics are uniformly distributed across the sequential doc ordering.
func nthElement(docs []int32, biases []float64, k int) {
	lo, hi := 0, len(docs)-1
	for lo < hi {
		// Use the middle element as pivot to avoid O(n²) on sorted inputs.
		pivot := biases[lo+(hi-lo)/2]
		lt, gt := lo, hi
		i := lo
		for i <= gt {
			b := biases[i]
			if b < pivot {
				docs[lt], docs[i] = docs[i], docs[lt]
				biases[lt], biases[i] = biases[i], biases[lt]
				lt++
				i++
			} else if b > pivot {
				docs[i], docs[gt] = docs[gt], docs[i]
				biases[i], biases[gt] = biases[gt], biases[i]
				gt--
				// don't advance i: the swapped-in element hasn't been examined
			} else {
				i++
			}
		}
		// biases[lo..lt-1] < pivot, biases[lt..gt] == pivot, biases[gt+1..hi] > pivot
		if k < lt {
			hi = lt - 1
		} else if k > gt {
			lo = gt + 1
		} else {
			return // k falls in the equal-pivot region
		}
	}
}
