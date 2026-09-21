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

// This file implements segment.BlockMaxPostingsIterator on *PostingsIterator:
// a per-block score bound the postings block format has carried since it was
// introduced (see postingsSerializer.blockBound in postings_format.go), plus
// a bulk block decode that fills a caller's own arrays directly rather than
// handing back iterator-owned ones -- the batch-scoring (conjunction) path
// drives this across several clauses at once, and the collector's bulk
// collection path drives it for a single term.
//
// BlockMax and ShallowAdvance are deliberately two separate methods, not
// one that both peeks and moves: an earlier version of this file combined
// them into a single SeekBlock call, which -- because it always had to
// invalidate the iterator's "already positioned" fast path to stay correct
// -- silently turned every ordinary ScoreBulk-driven scan into a full
// re-seek per block, even for blocks that were never actually skipped.
// BlockMax here never moves anything; only ShallowAdvance does.
package zap

// BlockMax implements segment.BlockMaxPostingsIterator. It reports a bound
// -- the highest term frequency and the most favorable (lowest, i.e.
// shortest-field) norm factor -- for every document up to and including
// lastDoc, without decoding any block payload, plus how many documents that
// span covers so a caller that skips it can still count them exactly.
//
// The bound is only ever available in one of two shapes, matched by the
// switch below: freshly positioned (the block c.entry currently describes
// is entirely unvisited -- the very start of a scan, or right after a
// ShallowAdvance) or fully drained (every document up to c.entry.lastDoc
// has already been returned via ordinary iteration, so the bound that
// matters now is the *next* block's, peeked without disturbing anything).
// Anything in between -- mid-block, some but not all of it already visited
// -- still uses the current block's own bound (it covers the whole block
// regardless of how much has been consumed), but docCount is corrected down
// by however many of those documents are already spoken for, so a caller
// that skips the remainder via ShallowAdvance never double-counts them.
func (i *PostingsIterator) BlockMax() (maxTF uint64, maxNormFactor float64, lastDoc uint64, docCount int, ok bool) {
	if i.is1Hit || i.postings == nil {
		return 0, 0, 0, 0, false
	}

	var minNormID uint8
	var tf uint32
	var ld uint32
	var dc int
	switch {
	case !i.positioned:
		minNormID, tf, ld, dc, ok = i.cursor.blockBound()
	case i.cur+1 >= i.cursor.nDocs:
		minNormID, tf, ld, dc, ok = i.cursor.peekNextBlockBound()
	default:
		minNormID, tf, ld, dc, ok = i.cursor.blockBound()
		if ok {
			dc -= i.cur + 1
		}
	}
	if !ok {
		return 0, 0, 0, 0, false
	}
	return uint64(tf), normFactorFromID(minNormID), uint64(ld), dc, true
}

// ShallowAdvance implements segment.BlockMaxPostingsIterator. It moves the
// cursor to the block that could contain target, touching only the skip
// list -- no block payload is decoded. Call BlockMax again afterward for
// the new position's bound before deciding whether to fetch it for real.
//
// Like Advance, target must be strictly greater than any document number
// this iterator has already produced or shallow-advanced past; this only
// ever moves forward. Safe to call even when BlockMax reported ok=false: it
// is then a no-op on the bound itself, though the cursor still moves (or
// exhausts) as directed.
func (i *PostingsIterator) ShallowAdvance(target uint64) error {
	if i.is1Hit {
		return nil
	}
	// docNumTerminated is reserved as the padding/sentinel value and is
	// never a real document number (see postings_format.go), so a target
	// at or beyond it can never be found.
	if target >= uint64(docNumTerminated) {
		i.cursor.exhausted = true
	} else {
		i.cursor.seekBlock(uint32(target))
	}
	// The decoded block, if any, no longer matches where the cursor now
	// sits; the next real Next()/Advance()/NextBlock() call must reload
	// for real rather than taking the "already positioned" fast path.
	i.positioned = false
	return nil
}

// NextBlock implements segment.BlockMaxPostingsIterator. It fills the
// caller's docNums/freqs/norms (all three must have equal length) with up
// to that many postings and returns how many were written; a return of 0
// means the list is exhausted. globalOffset is added to every doc number
// written, so a caller spanning several segments can hand back one
// continuous doc-ID stream without a separate translation pass.
//
// This is the bulk counterpart to ordinary Next/Advance: handing back flat
// arrays lets a caller score a whole block without materialising a
// per-document object, amortising the call across the block instead of
// paying it once per document. Locations are never decoded here, so a
// caller needing term vectors must stay on the generic Next/Advance path
// -- same as is1Hit, an in-memory bitmap-driven iterator, or an exhausted
// postings list, none of which have a decoded block to copy from; all four
// fall back to nextBlockOneAtATime, which is correct for all of them (if
// slower) rather than a second bespoke implementation for each.
func (i *PostingsIterator) NextBlock(docNums []uint64, freqs []uint64,
	norms []float64, globalOffset uint64) (int, error) {
	if i.err != nil {
		return 0, i.err
	}
	if i.is1Hit || i.postings == nil || i.includeLocs {
		return i.nextBlockOneAtATime(docNums, freqs, norms, globalOffset)
	}

	c := &i.cursor
	wantFreqNorm := i.includeFreqNorm

	n := 0
	for n < len(docNums) {
		start := c.nDocs
		if i.positioned {
			start = i.cur + 1
		}
		if start >= c.nDocs {
			if i.positioned {
				c.nextBlock()
			}
			if c.exhausted {
				break
			}
			if err := c.loadBlock(); err != nil {
				return n, err
			}
			if c.nDocs == 0 {
				break
			}
			i.positioned = true
			start = 0
		}

		end := c.nDocs
		if room := start + len(docNums) - n; room < end {
			end = room
		}

		for k := start; k < end; k++ {
			d := c.buf.docs[k]
			if i.exceptItr != nil && i.isDeleted(d) {
				continue
			}
			docNums[n] = uint64(d) + globalOffset
			if wantFreqNorm {
				freqs[n] = uint64(c.buf.freqs[k])
				norms[n] = normFactorFromID(i.normIDOf(d))
			} else {
				freqs[n] = 1
				norms[n] = 1
			}
			n++
		}

		i.cur = end - 1
		i.nextAllowed = c.buf.docs[end-1] + 1
	}
	return n, nil
}

// nextBlockOneAtATime serves the forms of iteration that have no decoded
// block behind them (see NextBlock's doc comment for which ones, and why
// they share this one fallback instead of each getting a bespoke path).
func (i *PostingsIterator) nextBlockOneAtATime(docNums []uint64, freqs []uint64,
	norms []float64, globalOffset uint64) (int, error) {
	n := 0
	for n < len(docNums) {
		docNum, exists, err := i.nextDocNumAtOrAfter(0)
		if err != nil {
			return n, err
		}
		if !exists {
			break
		}
		docNums[n] = docNum + globalOffset
		if i.includeFreqNorm {
			freqs[n] = i.currFreq()
			norms[n] = normFactorFromID(i.currNormID(uint32(docNum)))
		} else {
			freqs[n] = 1
			norms[n] = 1
		}
		n++
	}
	return n, nil
}
