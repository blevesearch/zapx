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

import "math"

// This file implements segment.BlockMaxPostingsIterator on *PostingsIterator,
// exposing the block-max metadata (minNormID/maxTF) and batch block decode
// that the postings block format has carried since it was introduced -- see
// the "SKIP ENTRIES" note in postings_format.go for why the metadata exists
// and what it bounds. Nothing here changes the on-disk format or the write
// path; it only reads what is already there.
//
// None of these methods are meaningful for a 1-hit posting list (a single
// posting inlined into the FST value, with no blockCursor behind it) or for
// a field that was indexed without frequencies (there is no tf to bound, and
// the skip entry's minNormID/maxTF bytes are zero-valued, not a real bound).
// Both cases report ok == false, mirroring how block_max_score returns None
// for tantivy's VInt tail.

// SeekBlock implements segment.BlockMaxPostingsIterator. It moves only the
// skip list, never decoding a posting block, so it is cheap to call
// repeatedly while probing for a block worth decoding.
func (i *PostingsIterator) SeekBlock(docNum uint64) (uint64, bool) {
	if i.is1Hit || i.postings == nil {
		return 0, false
	}

	target := uint32(docNumTerminated)
	if docNum <= uint64(docNumTerminated) {
		target = uint32(docNum)
	}

	// A block move invalidates whatever i.cur pointed at, and loadBlock has
	// not run for the new block, so the ordinary Next/Advance path must be
	// forced back onto its general (re-seek, re-decode) path rather than its
	// "already positioned" fast path.
	i.cursor.seekBlock(target)
	i.positioned = false

	if i.cursor.isExhausted() {
		return 0, false
	}
	return uint64(i.cursor.entry.lastDoc), true
}

// BlockMaxTF implements segment.BlockMaxPostingsIterator.
func (i *PostingsIterator) BlockMaxTF() (uint64, bool) {
	if !i.blockMaxAvailable() {
		return 0, false
	}
	if i.cursor.entry.maxTF == math.MaxUint8 {
		// Saturated at encode time -- see encodeBlockMaxTF -- meaning the
		// true maximum could not be represented and must be treated as
		// unbounded rather than plugged into a per-block formula.
		return math.MaxUint64, true
	}
	return uint64(i.cursor.entry.maxTF), true
}

// BlockMinNormID implements segment.BlockMaxPostingsIterator.
func (i *PostingsIterator) BlockMinNormID() (uint8, bool) {
	if !i.blockMaxAvailable() {
		return 0, false
	}
	return i.cursor.entry.minNormID, true
}

// blockMaxAvailable reports whether the skip entry the cursor currently sits
// on carries a meaningful block-max bound.
func (i *PostingsIterator) blockMaxAvailable() bool {
	return !i.is1Hit && i.postings != nil && i.cursor.hasFreqs && !i.cursor.isExhausted()
}

// NormFromID implements segment.BlockMaxPostingsIterator.
func (i *PostingsIterator) NormFromID(normID uint8) float64 {
	return normFactorFromID(normID)
}

// CurrentBlock implements segment.BlockMaxPostingsIterator. It force-decodes
// the block the cursor currently sits on and hands back doc numbers and term
// frequencies as iterator-owned []uint64 slices -- a thin widening adapter
// over the blockCursor's native [128]uint32 arrays, which the batch-scoring
// (conjunction) path can score directly without going through Next/Advance
// per document. Norms are included (via the same per-doc lookup Posting.Norm
// uses) so that scoring a candidate never needs a separate seek.
func (i *PostingsIterator) CurrentBlock() ([]uint64, []uint64, []float64, bool) {
	if i.is1Hit || i.postings == nil || i.cursor.isExhausted() {
		return nil, nil, nil, false
	}
	if err := i.cursor.loadBlock(); err != nil {
		i.err = err
		return nil, nil, nil, false
	}
	n := i.cursor.numDocs()
	if n == 0 {
		return nil, nil, nil, false
	}

	if cap(i.blockDocsBuf) < n {
		i.blockDocsBuf = make([]uint64, n)
		i.blockFreqsBuf = make([]uint64, n)
		i.blockNormsBuf = make([]float64, n)
	}
	docs, freqs := i.cursor.docs(), i.cursor.freqs()
	for k := 0; k < n; k++ {
		i.blockDocsBuf[k] = uint64(docs[k])
		i.blockFreqsBuf[k] = uint64(freqs[k])
		i.blockNormsBuf[k] = normFactorFromID(i.normIDOf(docs[k]))
	}
	return i.blockDocsBuf[:n], i.blockFreqsBuf[:n], i.blockNormsBuf[:n], true
}
