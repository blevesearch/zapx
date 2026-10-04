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
	"encoding/binary"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// BlockPostingsIterator is the variant of PostingsIterator that hands out the
// postings of a list a block at a time, as bare doc numbers, frequencies and
// norms: it implements segment.BlockCursor (and segment.BlockMaxCursor), the
// interfaces the consumers of such postings use.
//
// It is built on the blockCursor of postings_block.go, as PostingsIterator is.
// blockCursor knows the format: it moves over the skip data and decodes blocks.
// BlockPostingsIterator takes care of what is around the raw cursor, as
// PostingsIterator does: 1-hit lists, deleted docs, resolving norms from the
// field's norms column, and, which PostingsIterator has no need of, the bounds of
// blocks. Unlike PostingsIterator it knows nothing about locations. A PostingsList
// hands one out through BlockCursor (see posting.go).
type BlockPostingsIterator struct {
	pl *PostingsList
	c  blockCursor

	withFreqs bool
	withNorms bool

	// exceptItr walks the deleted-doc bitmap in lockstep with the postings; it
	// is nil when nothing is deleted.
	exceptItr roaring.IntPeekable

	// is1Hit lists have no blockCursor behind them: the one posting is handed
	// out by the first call that gets to it.
	is1Hit   bool
	consumed bool

	// lastBounds are the bounds of the block that was decoded last
	lastBounds segment.BlockBounds

	// The field's norms column, flattened so that the per-block loop does not
	// re-decide its shape.
	normDense   []uint8
	normConst   float32
	normIsDense bool
}

var _ segment.BlockCursor = (*BlockPostingsIterator)(nil)
var _ segment.BlockMaxCursor = (*BlockPostingsIterator)(nil)

// Count implements segment.BlockCursor.
func (b *BlockPostingsIterator) Count() uint64 { return b.pl.Count() }

// LiveCount implements segment.BlockCursor.
func (b *BlockPostingsIterator) LiveCount() (uint64, error) {
	p := b.pl
	if p.except == nil || p.except.IsEmpty() {
		return p.Count(), nil
	}
	// walk a cursor of its own, without freqs or norms: only the doc numbers
	// are decoded, and the deleted ones are dropped as they go by
	sc, err := p.BlockCursor(false, false, nil)
	if err != nil {
		return 0, err
	}
	var blk segment.PostingsBlock
	var live uint64
	for {
		n, err := sc.NextBlock(&blk)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return live, nil
		}
		live += uint64(n)
	}
}

// BytesRead implements segment.BlockCursor.
func (b *BlockPostingsIterator) BytesRead() uint64 { return b.c.bytesRead }

// NextBlock implements segment.BlockCursor.
func (b *BlockPostingsIterator) NextBlock(out *segment.PostingsBlock) (int, error) {
	if b.is1Hit {
		return b.next1Hit(0, out), nil
	}
	for !b.consumed && !b.c.isExhausted() {
		n, err := b.loadInto(0, out)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return n, nil
		}
		// every doc in the block was deleted
	}
	return 0, nil
}

// SeekBlock implements segment.BlockCursor.
func (b *BlockPostingsIterator) SeekBlock(target uint64, out *segment.PostingsBlock) (int, error) {
	if b.is1Hit {
		return b.next1Hit(target, out), nil
	}
	if target > math.MaxUint32 {
		b.consumed = true
		return 0, nil
	}
	if b.consumed {
		return 0, nil
	}

	// skip data only; the cursor never moves backwards
	b.c.seekBlock(uint32(target))
	for !b.c.isExhausted() {
		n, err := b.loadInto(uint32(target), out)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			return n, nil
		}
		// nothing left at or after target in this block (it was deleted)
	}
	return 0, nil
}

// loadInto decodes the block the cursor is on, copies the live entries that
// are >= lo into out, and steps the cursor to the next block.
func (b *BlockPostingsIterator) loadInto(lo uint32, out *segment.PostingsBlock) (int, error) {
	c := &b.c
	if err := c.loadBlock(); err != nil {
		return 0, err
	}

	n := c.numDocs()
	docs := c.docs()
	start := 0
	if lo > 0 && docs[0] < lo {
		start = searchBlock(docs, lo)
	}
	n -= start
	if n < 0 {
		n = 0
	}
	copy(out.Docs[:n], docs[start:start+n])
	if b.withFreqs {
		copy(out.Freqs[:n], c.freqs()[start:start+n])
	}

	b.lastBounds = b.entryBounds()

	// after this the block buffer isn't needed: step ahead
	c.nextBlock()

	if b.exceptItr != nil {
		n = b.dropDeleted(out, n)
	}
	if b.withNorms {
		b.fillNorms(out, n)
	}
	return n, nil
}

// dropDeleted compacts out so that it holds no deleted doc, returning the new
// length. The bitmap and the postings are both ascending, so advancing the
// peekable iterator is amortised constant work.
func (b *BlockPostingsIterator) dropDeleted(out *segment.PostingsBlock, n int) int {
	w := 0
	for i := 0; i < n; i++ {
		d := out.Docs[i]
		b.exceptItr.AdvanceIfNeeded(d)
		if b.exceptItr.HasNext() && b.exceptItr.PeekNext() == d {
			continue
		}
		if w != i {
			out.Docs[w] = d
			if b.withFreqs {
				out.Freqs[w] = out.Freqs[i]
			}
		}
		w++
	}
	return w
}

// fillNorms resolves the norm factor of the first n docs of out.
//
// For a column that has a norm per doc this is a gather: each doc number picks
// a byte of the column, which picks a factor of a 256 entry table. The block
// is scored as a whole by SIMD kernels afterwards, so this loop, scalar as it
// has to be (neither NEON nor SSE2 has a gather), is a good part of what a
// block costs. So the work in it is only that: the column is checked once for
// the block, not for each doc, and the loop has no other bounds check to pay
// than the column's.
func (b *BlockPostingsIterator) fillNorms(out *segment.PostingsBlock, n int) {
	if n <= 0 {
		return
	}
	docs := out.Docs[:n]
	norms := out.Norms[:n]
	if !b.normIsDense {
		c := b.normConst
		for i := range norms {
			norms[i] = c
		}
		return
	}

	dense := b.normDense
	// Docs ascend, so the last tells whether any is past the column: not the
	// case for a column that covers the segment, which is all of them.
	if int(docs[n-1]) < len(dense) {
		for i, d := range docs {
			norms[i] = fieldNormFactor[dense[d]]
		}
		return
	}
	for i, d := range docs {
		var id uint8 // a doc past the column has no norm: id 0, as in norms.id
		if int(d) < len(dense) {
			id = dense[d]
		}
		norms[i] = fieldNormFactor[id]
	}
}

// next1Hit hands out the inline posting of a 1-hit list if it's live and >= lo.
func (b *BlockPostingsIterator) next1Hit(lo uint64, out *segment.PostingsBlock) int {
	if b.consumed {
		return 0
	}
	b.consumed = true

	p := b.pl
	if p.docNum1Hit == DocNum1HitFinished || p.docNum1Hit < lo {
		return 0
	}
	if p.docNum1Hit > math.MaxUint32 {
		return 0
	}
	d := uint32(p.docNum1Hit)
	if p.except != nil && p.except.Contains(d) {
		return 0
	}
	out.Docs[0] = d
	b.lastBounds = b.oneHitBounds()
	if b.withFreqs {
		out.Freqs[0] = uint32(p.freq1Hit)
	}
	if b.withNorms {
		out.Norms[0] = fieldNormFactor[p.norms.id(d)]
	}
	return 1
}

// ---------------------------------------------------------------------------
//
// BLOCK MAX
//
// ---------------------------------------------------------------------------

// exhaustedBounds are the bounds of no block at all: nothing to score.
var exhaustedBounds = segment.BlockBounds{LastDoc: docNumTerminated, FreqBounded: true}

// oneHitBounds are the bounds of a 1-hit list's only posting
func (b *BlockPostingsIterator) oneHitBounds() segment.BlockBounds {
	p := b.pl
	if p.docNum1Hit == DocNum1HitFinished || p.docNum1Hit > math.MaxUint32 {
		return exhaustedBounds
	}
	d := uint32(p.docNum1Hit)
	return segment.BlockBounds{
		LastDoc:     d,
		MaxFreq:     uint32(p.freq1Hit),
		FreqBounded: true,
		MaxNorm:     fieldNormFactor[p.norms.id(d)],
	}
}

// entryBounds are the bounds of the block the skip cursor is on.
func (b *BlockPostingsIterator) entryBounds() segment.BlockBounds {
	c := &b.c
	if c.exhausted {
		return exhaustedBounds
	}
	return skipEntryBounds(&c.entry, c.hasFreqs)
}

func skipEntryBounds(e *skipEntry, hasFreqs bool) segment.BlockBounds {
	bd := segment.BlockBounds{LastDoc: e.lastDoc}
	if !hasFreqs {
		// every frequency is 1, and nothing is known about the norms
		bd.MaxFreq, bd.FreqBounded = 1, true
		bd.MaxNorm = float32(math.Inf(1))
		return bd
	}
	bd.MaxFreq = uint32(e.maxTF)
	// a saturated maxTF means "at least this", which bounds nothing
	bd.FreqBounded = e.maxTF < math.MaxUint8
	bd.MaxNorm = fieldNormFactor[e.minNormID]
	return bd
}

// boundsProbe is how many blocks, from the cursor's, BoundsAt looks at one by one
// before it searches for the target.
const boundsProbe = 4

// BoundsAt implements segment.BlockMaxCursor.
func (b *BlockPostingsIterator) BoundsAt(target uint32) (segment.BlockBounds, bool) {
	if b.is1Hit {
		bd := b.oneHitBounds()
		return bd, bd.LastDoc != docNumTerminated && bd.LastDoc >= target
	}
	if b.pl.sb == nil {
		return exhaustedBounds, false
	}
	c := &b.c
	total := c.numFullBlocks
	if c.tailLen > 0 {
		total++
	}
	// the first block whose last doc is >= target; the skip entries are
	// fixed-width and their last docs ascend
	lastDocAt := func(i int) uint32 {
		if i < c.numFullBlocks {
			return binary.LittleEndian.Uint32(c.skip[i*c.entryLen:])
		}
		return c.tailEntry.lastDoc
	}
	lo, hi := 0, total
	// Asked about in order, which is how a pruner walks a list, the answer is
	// the block the cursor is at or one of the few after it: probe those first,
	// unless the target is behind the cursor.
	if i := c.blockIdx; i < total && (i == 0 || target > lastDocAt(i-1)) {
		lo = i
		for lo < total && lo < i+boundsProbe && lastDocAt(lo) < target {
			lo++
		}
		if lo >= total || lastDocAt(lo) >= target {
			hi = lo
		}
	}
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if lastDocAt(mid) < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == total {
		return exhaustedBounds, false
	}
	if lo < c.numFullBlocks {
		var e skipEntry
		e.decode(c.skip[lo*c.entryLen:], c.hasFreqs)
		return skipEntryBounds(&e, c.hasFreqs), true
	}
	return skipEntryBounds(&c.tailEntry, c.hasFreqs), true
}

// DecodedBounds implements segment.BlockMaxCursor.
func (b *BlockPostingsIterator) DecodedBounds() segment.BlockBounds { return b.lastBounds }

// TermBounds implements segment.BlockMaxCursor.
func (b *BlockPostingsIterator) TermBounds() segment.BlockBounds {
	if b.is1Hit {
		return b.oneHitBounds()
	}
	if b.pl.sb == nil {
		return exhaustedBounds
	}
	c := &b.c
	rv := segment.BlockBounds{FreqBounded: true}
	first := true
	add := func(e *skipEntry) {
		bd := skipEntryBounds(e, c.hasFreqs)
		rv.LastDoc = bd.LastDoc
		if first {
			rv.MaxNorm = bd.MaxNorm
			first = false
		}
		if bd.MaxNorm > rv.MaxNorm {
			rv.MaxNorm = bd.MaxNorm
		}
		if !bd.FreqBounded {
			rv.FreqBounded = false
		} else if bd.MaxFreq > rv.MaxFreq {
			rv.MaxFreq = bd.MaxFreq
		}
	}
	var e skipEntry
	for i := 0; i < c.numFullBlocks; i++ {
		e.decode(c.skip[i*c.entryLen:], c.hasFreqs)
		add(&e)
	}
	if c.tailLen > 0 {
		add(&c.tailEntry)
	}
	if first {
		return exhaustedBounds
	}
	return rv
}
