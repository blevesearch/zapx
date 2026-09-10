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
	"fmt"

	"github.com/blevesearch/freeway/bitpack"
)

// blockCursor walks one term's postings a block at a time.  It owns two roles
// that tantivy splits between SkipReader and BlockSegmentPostings: stepping
// through the skip list, which needs no payload at all, and decoding the block
// the skip list landed on.
//
// Every loaded block presents exactly 128 slots in docs, non-decreasing, padded
// out with docNumTerminated.  Callers can therefore index docs without a bounds
// test, and the in-block search below can run without a branch.
type blockCursor struct {
	sb *SegmentBase

	payloadStart uint64
	payloadLen   uint64
	skip         []byte // the decoded skip region; empty only when docFreq == 0

	docFreq       uint32
	numFullBlocks int
	tailLen       int
	entryLen      int
	hasFreqs      bool
	wantFreqs     bool

	// tailLastDoc is the tail's own last, largest doc number, read once from
	// the skip trailer. It gives seekBlock a real bound to compare the tail
	// against instead of always having to decode it. Meaningless when
	// tailLen == 0.
	tailLastDoc uint32

	// Position in the skip list.  blockIdx counts full blocks; reaching
	// numFullBlocks means the uvarint tail, and going past it means the list is
	// spent.
	blockIdx  int
	entry     skipEntry
	inTail    bool
	exhausted bool

	// prevLastDoc is the last doc number of the preceding block, which is the
	// anchor the current block's deltas are measured against.
	prevLastDoc uint32

	// buf holds the decoded block. It lives behind a pointer, and is only
	// allocated once something actually has to be decoded, because a
	// PostingsIterator embeds a blockCursor and a query over many small
	// segments creates far more iterators than it decodes blocks -- inline
	// arrays would mean a kilobyte allocated and zeroed per iterator.
	buf *blockBuf

	nDocs        int
	blockLastDoc uint32
	loaded       bool

	bytesRead uint64
}

// blockBuf is one decoded block: 128 doc numbers and 128 frequencies.
type blockBuf struct {
	docs  [postingsBlockLen]uint32
	freqs [postingsBlockLen]uint32
}

// allOnes seeds the frequency array for terms that do not record frequencies,
// and for readers that asked not to decode them.  A missing frequency reads as
// 1, which is what earlier zap versions returned.
var allOnes = func() (a [postingsBlockLen]uint32) {
	for i := range a {
		a[i] = 1
	}
	return
}()

var allTerminated = func() (a [postingsBlockLen]uint32) {
	for i := range a {
		a[i] = docNumTerminated
	}
	return
}()

// init points the cursor at a term.  The footer has already been decoded, which
// is where the three region offsets come from.
func (c *blockCursor) init(sb *SegmentBase, h *termFooter,
	payloadStart, skipStart uint64, wantFreqs bool) error {
	c.sb = sb
	c.payloadStart = payloadStart
	c.payloadLen = h.payloadLen
	c.docFreq = h.docFreq
	c.hasFreqs = h.hasFreqs()
	c.wantFreqs = wantFreqs && c.hasFreqs
	c.numFullBlocks = int(h.docFreq) / postingsBlockLen
	c.tailLen = int(h.docFreq) % postingsBlockLen
	c.entryLen = skipEntryLen(c.hasFreqs)

	c.skip = nil
	c.tailLastDoc = docNumTerminated
	if h.skipLen > 0 {
		raw, err := sb.fileReader.process(sb.mem[skipStart : skipStart+h.skipLen])
		if err != nil {
			return fmt.Errorf("error processing skip data: %v", err)
		}
		want := skipDataLen(c.numFullBlocks, c.tailLen, c.hasFreqs)
		if len(raw) != want {
			return fmt.Errorf("corrupt skip data: %d bytes for %d blocks, want %d",
				len(raw), c.numFullBlocks, want)
		}
		c.skip = raw
		c.bytesRead += h.skipLen
		if c.tailLen > 0 {
			off := c.numFullBlocks * c.entryLen
			if c.numFullBlocks > 0 {
				off += skipTailOffsetLen
			}
			c.tailLastDoc = binary.LittleEndian.Uint32(c.skip[off:])
		}
	} else if c.numFullBlocks > 0 || c.tailLen > 0 {
		return fmt.Errorf("corrupt term: %d full blocks, %d tail docs, but no skip data",
			c.numFullBlocks, c.tailLen)
	}

	if c.buf == nil {
		c.buf = &blockBuf{}
	}
	if !c.wantFreqs {
		copy(c.buf.freqs[:], allOnes[:])
	}
	c.rewind()
	return nil
}

// rewind returns the cursor to the first block without re-reading the footer.
func (c *blockCursor) rewind() {
	c.blockIdx = 0
	c.prevLastDoc = 0
	c.loaded = false
	c.selectBlock()
}

// selectBlock reads the skip entry for blockIdx, or switches to the tail, or
// marks the cursor spent.
func (c *blockCursor) selectBlock() {
	switch {
	case c.blockIdx < c.numFullBlocks:
		c.entry.decode(c.skip[c.blockIdx*c.entryLen:], c.hasFreqs)
		c.inTail = false
		c.exhausted = false
	case c.blockIdx == c.numFullBlocks && c.tailLen > 0:
		// tailLastDoc came from the skip trailer, so seekBlock's ordinary bound
		// check applies to the tail exactly like a full block: a target past
		// the tail's own range exhausts the cursor without decoding it.
		c.entry = skipEntry{lastDoc: c.tailLastDoc}
		c.inTail = true
		c.exhausted = false
	default:
		c.inTail = false
		c.exhausted = true
	}
}

// nextBlock steps one block forward.
func (c *blockCursor) nextBlock() {
	if c.exhausted {
		return
	}
	if !c.inTail {
		c.prevLastDoc = c.entry.lastDoc
	}
	c.blockIdx++
	c.loaded = false
	c.selectBlock()
}

// seekBlock walks forward to the first block -- full or tail -- that could
// contain target, or exhausts the cursor if none can.  It touches only the
// skip list -- a linear scan over fixed-size records, one per 128 documents,
// plus the tail's own lastDoc bound -- and never decodes a payload, which is
// what makes a long forward skip cheap even when the answer turns out not to
// exist at all.
func (c *blockCursor) seekBlock(target uint32) {
	for !c.exhausted && c.entry.lastDoc < target {
		c.nextBlock()
	}
}

// blockOffsetAt returns the payload-relative offset of block i.  Index
// numFullBlocks is the tail, whose offset is the trailing value the writer
// appended after the last entry; that also gives the last full block its length.
func (c *blockCursor) blockOffsetAt(i int) uint32 {
	if i >= c.numFullBlocks {
		return binary.LittleEndian.Uint32(c.skip[c.numFullBlocks*c.entryLen:])
	}
	return binary.LittleEndian.Uint32(c.skip[i*c.entryLen+4:])
}

func (c *blockCursor) loadBlock() error {
	if c.loaded {
		return nil
	}
	c.loaded = true

	if c.exhausted {
		copy(c.buf.docs[:], allTerminated[:])
		c.nDocs = 0
		c.blockLastDoc = docNumTerminated
		return nil
	}
	if c.inTail {
		return c.loadTail()
	}

	start := c.payloadStart + uint64(c.entry.blockOffset)
	end := c.payloadStart + uint64(c.blockOffsetAt(c.blockIdx+1))
	if end < start || end > uint64(len(c.sb.mem)) {
		return fmt.Errorf("corrupt postings block range [%d,%d)", start, end)
	}
	raw, err := c.sb.fileReader.process(c.sb.mem[start:end])
	if err != nil {
		return fmt.Errorf("error processing postings block: %v", err)
	}
	c.bytesRead += end - start

	docBytes := bitpack.BlockBytes(c.entry.docNumBits)
	if len(raw) < docBytes {
		return fmt.Errorf("corrupt postings block: %d bytes, need %d", len(raw), docBytes)
	}
	bitpack.UnpackDelta1(raw, c.prevLastDoc, c.entry.docNumBits, &c.buf.docs)
	if c.wantFreqs {
		tfBytes := bitpack.BlockBytes(c.entry.tfNumBits)
		if len(raw) < docBytes+tfBytes {
			return fmt.Errorf("corrupt postings block: missing frequency bytes")
		}
		bitpack.UnpackPlus1(raw[docBytes:], c.entry.tfNumBits, &c.buf.freqs)
	}
	c.nDocs = postingsBlockLen
	c.blockLastDoc = c.entry.lastDoc
	return nil
}

// loadTail decodes the fewer-than-128 documents that could not fill a block.
// They are plain uvarints: there is no bit width to save on a run this short,
// and it keeps the reader on the existing uvarint decoder.
func (c *blockCursor) loadTail() error {
	var tailOffset uint64
	if c.numFullBlocks > 0 {
		// With no full blocks the tail starts at the payload itself; the skip
		// trailer in that case holds only tailLastDoc, not a tail offset.
		tailOffset = uint64(c.blockOffsetAt(c.numFullBlocks))
	}
	start := c.payloadStart + tailOffset
	end := c.payloadStart + c.payloadLen
	if end < start || end > uint64(len(c.sb.mem)) {
		return fmt.Errorf("corrupt postings tail range [%d,%d)", start, end)
	}
	raw, err := c.sb.fileReader.process(c.sb.mem[start:end])
	if err != nil {
		return fmt.Errorf("error processing postings tail: %v", err)
	}
	c.bytesRead += end - start

	r := memUvarintReader{S: raw}
	prev := c.prevLastDoc
	for i := 0; i < c.tailLen; i++ {
		delta, err := r.ReadUvarint()
		if err != nil {
			return fmt.Errorf("error reading tail doc delta: %v", err)
		}
		prev += uint32(delta)
		c.buf.docs[i] = prev
	}
	if c.hasFreqs {
		if c.wantFreqs {
			for i := 0; i < c.tailLen; i++ {
				freq, err := r.ReadUvarint()
				if err != nil {
					return fmt.Errorf("error reading tail freq: %v", err)
				}
				c.buf.freqs[i] = uint32(freq)
			}
		}
	}
	copy(c.buf.docs[c.tailLen:], allTerminated[c.tailLen:])
	c.nDocs = c.tailLen
	c.blockLastDoc = prev
	return nil
}

// b2i is the branchless comparison helper the in-block search is built from;
// the compiler turns it into a set-on-condition.
func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// searchBlock returns the index of the first entry in a decoded block that is
// greater than or equal to target -- a lower bound.
//
// This is the 8-ary search of Schlegel, Gemulla and Lehner rather than a binary
// search: 128 narrows to 16, then to 2, then a two-element scan, for sixteen
// comparisons instead of seven.  All sixteen are branchless, and the seven
// probes of each round are independent loads the processor can issue together,
// so the extra comparisons cost less than the mispredictions they replace.
//
// It relies on the padding invariant: the block always holds 128 non-decreasing
// entries ending in a value no legal target can exceed, so the result is always
// in range.
func searchBlock(arr *[postingsBlockLen]uint32, target uint32) int {
	base, span := 0, postingsBlockLen
	for {
		step := span / 8
		if step == 0 {
			break
		}
		count := 0
		for i := 1; i < 8; i++ {
			count += b2i(arr[base+i*step-1] < target)
		}
		base += count * step
		span = step
	}
	count := 0
	for i := 0; i < span; i++ {
		count += b2i(arr[base+i] < target)
	}
	return base + count
}
