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

// blockBuf holds one decoded block: 128 docs and their corresponding freqs
type blockBuf struct {
	docs  [postingsBlockLen]uint32
	freqs [postingsBlockLen]uint32
}

// blockCursor iterates over the postings one block at a time. It reads the following
// streams of data:
//  1. The skip data - This contains metadata for the actual postings block, which is
//     used to choose the appropriate SIMD kernels to decode a block, whether the block
//     should be skipped, the offset of the block, etc.
//  2. The postings list - This is organized as blocks of 128 docs along with their freqs.
//     Once we have the skip data for the block, we use it to decode an entire block at
//     a time which we'll populate in a blockBuf. The consumer can read this buf for the
//     next 128 postings, then continue to load other block when required.
//
// There are 2 types of APIs that blockCursor has:
//  1. Block movement: make the cursor move between blocks. This "movement" is really
//     just iterating over the skip data of each block, not the postings list itself.
//  2. Block reading: read the data that the cursor is currently pointing to. MUST CALL
//     loadBlock() PRIOR TO READING DATA FROM THE BLOCK!!!
//
// The standard usage pattern would be to use the block movement APIs to move the cursor
// to a block of interest. Then you call loadBlock(), then access that block's postings
// through the block reading APIs.
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

	// The final block of the postings list will be uvarints rather than
	// using the bitpacked format if it doesn't have 128 docs. In this case, we
	// decode the tail entry ahead of time since it requires special
	// handling. Will not exist when the final block is 128 docs
	// (docFreq % 128 == 0)
	tailEntry skipEntry

	// Position in the skip list.  blockIdx counts full blocks; reaching
	// numFullBlocks means the uvarint tail, and going past it means the list is
	// spent.
	blockIdx  int
	entry     skipEntry // skipEntry that the cursor is currently pointing to
	inTail    bool
	exhausted bool

	// prevLastDoc is the last doc number of the preceding block, which is the
	// anchor the current block's deltas are measured against.
	prevLastDoc uint32

	// buf holds the decoded block. It lives behind a pointer and is
	// allocated lazily in loadBlock, not here in init -- a PostingsIterator
	// embeds a blockCursor, a query over many small segments creates far
	// more iterators than it decodes blocks, and an iterator that never
	// gets past skip-list movement should never pay for this.
	buf *blockBuf

	nDocs        int
	blockLastDoc uint32
	loaded       bool

	bytesRead uint64
}

// allOnes is a 128 size array containing just 1s, useful in cases where
// we are ignoring freqs.
var allOnes = func() (a [postingsBlockLen]uint32) {
	for i := range a {
		a[i] = 1
	}
	return
}()

// allTerminated is a 128 size array containing just "terminated" doc nums
// which are used to signify that the postings list has come to an end.
var allTerminated = func() (a [postingsBlockLen]uint32) {
	for i := range a {
		a[i] = docNumTerminated
	}
	return
}()

// -------------------------------------------------------------------------
//
// BLOCK MOVEMENT APIs
//
// -------------------------------------------------------------------------

// init points the cursor at a term.  The footer has already been decoded and
// is self-describing at this point -- it is where the region offsets come
// from, not a separate argument.
func (c *blockCursor) init(sb *SegmentBase, h *termFooter, wantFreqs bool) error {
	c.sb = sb
	c.payloadStart = h.payloadStart
	c.payloadLen = h.payloadLen
	c.docFreq = h.docFreq
	c.hasFreqs = h.hasFreqs()
	c.wantFreqs = wantFreqs && c.hasFreqs
	c.numFullBlocks = int(h.docFreq) / postingsBlockLen
	c.tailLen = int(h.docFreq) % postingsBlockLen
	c.entryLen = skipEntryLen(c.hasFreqs)

	c.tailEntry = skipEntry{}
	if h.skipLen > 0 {
		raw, err := sb.fileReader.process(sb.mem[h.skipStart : h.skipStart+h.skipLen])
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
			c.tailEntry.decodeTail(c.skip[c.numFullBlocks*c.entryLen:], c.hasFreqs)
		}
	} else if c.numFullBlocks > 0 || c.tailLen > 0 {
		return fmt.Errorf("corrupt term: %d full blocks, %d tail docs, but no skip data",
			c.numFullBlocks, c.tailLen)
	}

	// buf is lazy initialized in loadBlock for a fresh iterator. However, in case this
	// iterator is reused as passed into PostingsList.Iterator(), this buf may already
	// be initialized from the previous use: we set it to all 1s here in case we don't
	// care about freqs.
	if c.buf != nil && !c.wantFreqs {
		copy(c.buf.freqs[:], allOnes[:])
	}
	c.rewind()
	return nil
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
	// loads the block's skip data: basically
	// makes the blockCursor "point" to the block
	c.selectBlock()
}

// seekBlock iterates over the skip data to find the first block that could
// contain target. If the target is higher than the doc count of the final
// block, the cursor will be exhausted.
//
// If the cursor isn't exhausted, that doesn't mean the postings list has
// necessarily found the document: it just means that if the doc was present
// in any block, it would be in the block that the cursor is pointing at. You
// will still have to load the block and check the docnums yourself.
func (c *blockCursor) seekBlock(target uint32) {
	for !c.exhausted && c.entry.lastDoc < target {
		c.nextBlock()
	}
}

// isExhausted reports whether the cursor reached the end of the postings list.
// Use as a termination case for calling nextBlock, or to check if seekBlock
// found a block that could contain target.
func (c *blockCursor) isExhausted() bool { return c.exhausted }

// -------------------------------------------------------------------------
//
// BLOCK READING APIs
//
// -------------------------------------------------------------------------

// This function _has_ to be called prior to any of the following block reading
// functions. loadBlock actually unpacks the block and populates buf, which is
// where the block reading APIs are served from.
func (c *blockCursor) loadBlock() error {
	if c.loaded {
		return nil
	}
	c.loaded = true

	// lazy allocation of buf, since it's a pretty expensive
	// operation.
	if c.buf == nil {
		c.buf = &blockBuf{}
		if !c.wantFreqs {
			copy(c.buf.freqs[:], allOnes[:])
		}
	}

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

// numDocs is how many entries of the currently loaded block are real postings
// rather than terminator padding.
func (c *blockCursor) numDocs() int { return c.nDocs }

// lastDoc is the highest doc number in the currently loaded block.
func (c *blockCursor) lastDoc() uint32 { return c.blockLastDoc }

// docs returns the currently loaded block's docnum array
func (c *blockCursor) docs() *[postingsBlockLen]uint32 { return &c.buf.docs }

// freqs returns the currently loaded block's freq array.
func (c *blockCursor) freqs() *[postingsBlockLen]uint32 { return &c.buf.freqs }

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

// -------------------------------------------------------------------------
//
// HELPERS
//
// -------------------------------------------------------------------------

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
		// tailEntry came from the skip region, so seekBlock's ordinary bound
		// check applies to the tail exactly like a full block: a target past
		// the tail's own range exhausts the cursor without decoding it.
		c.entry = c.tailEntry
		c.inTail = true
		c.exhausted = false
	default:
		c.inTail = false
		c.exhausted = true
	}
}

// blockOffsetAt returns the payload-relative offset of block i.  Index
// numFullBlocks is the tail's offset, which also gives the last full block
// its length; when there is no tail at all, the payload's own length plays
// that role instead, since nothing follows the last full block to record one.
func (c *blockCursor) blockOffsetAt(i int) uint32 {
	if i >= c.numFullBlocks {
		if c.tailLen == 0 {
			return uint32(c.payloadLen)
		}
		return c.tailEntry.blockOffset
	}
	return binary.LittleEndian.Uint32(c.skip[i*c.entryLen+4:])
}

// loadTail decodes the fewer-than-128 documents that could not fill a block.
// They are plain uvarints: there is no bit width to save on a run this short,
// and it keeps the reader on the existing uvarint decoder.
func (c *blockCursor) loadTail() error {
	// tailEntry.blockOffset is 0 when there are no full blocks -- the tail
	// starts at the payload itself in that case, which is exactly what a
	// zero offset already means.
	start := c.payloadStart + uint64(c.tailEntry.blockOffset)
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
