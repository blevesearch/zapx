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
	"math"

	"github.com/blevesearch/freeway/bitpack"
)

// The on-disk shape of one term's postings.
//
// Doc numbers and term frequencies are bitpacked 128 at a time (see package
// bitpack); whatever is left over at the end of the list, fewer than 128
// documents, is written as plain uvarints.  A flat skip list carries one
// fixed-size entry per full block, which is what lets a reader hop to the block
// containing a target document without decoding anything.
//
// Field norms are not here at all.  They live in a dense column shared by every
// term of the field -- see normsColumn -- because a term appearing in a million
// documents would otherwise carry a million copies of the same few lengths.
//
// The regions are written in the order a writer can produce them, and the
// footer comes last so nothing has to be buffered or back-patched:
//
//	[payload]    blocks, then the uvarint tail
//	[locs]       chunkedIntCoder blob, unchanged from earlier zap versions
//	[skip]       one entry per full block, then a trailing tail offset
//	[footer]     <- the FST value points here
//
// The reader walks backwards from the footer: skip starts at footer-skipLen,
// locs at skip-locsLen, payload at locs-payloadLen.

const postingsBlockLen = bitpack.BlockLen

// Term footer flags.
const (
	termHasFreqs = 1 << 0
	termHasLocs  = 1 << 1
)

// docNumTerminated pads a decoded doc block out to a full 128 entries.  Keeping
// every block the same length, non-decreasing, and ending in a value no target
// can exceed is what makes the in-block search branchless.
const docNumTerminated = uint32(math.MaxUint32)

// Skip entry layout.  Each entry describes one full block:
//
//	u32 lastDocInBlock
//	u32 blockByteOffset      (from the start of the payload)
//	u8  docNumBits
//	u8  tfNumBits            \
//	u8  minNormID             > only when the term records frequencies
//	u8  maxTF                /
//
// followed, after the last entry and only once there is at least one full
// block, by a u32 giving the byte offset of the uvarint tail -- or, when
// there is no tail, the end of the last full block's byte range.
//
// followed, whenever the term has a tail (fewer than 128 trailing documents
// that didn't fill a block), by a u32 giving the tail's own last, largest doc
// number. Without this, a seek past a term's entire range has no way to tell
// "the tail might still hold the target" apart from "the tail's own range
// ends before the target", and has to decode the tail's uvarint stream just
// to find out which. With it, that seek is a skip-metadata comparison, the
// same as seeking past a full block. This matters more than it might look:
// most terms in a real dictionary never fill even one 128-document block, so
// they are nothing but a tail, and this is the only bound they get.
//
// Storing the block's byte offset rather than deriving it from the bit widths
// costs four bytes per 128 documents and buys two things: each block can be run
// through the writer's process() hook independently, so encryption and
// compression callbacks work without the reader having to decode a whole term
// to reach one block; and advancing the skip cursor becomes a load instead of
// an arithmetic reconstruction.
const (
	skipEntryLenNoFreqs   = 9
	skipEntryLenWithFreqs = 12
	skipTailOffsetLen     = 4
	skipTailLastDocLen    = 4
)

func skipEntryLen(hasFreqs bool) int {
	if hasFreqs {
		return skipEntryLenWithFreqs
	}
	return skipEntryLenNoFreqs
}

// skipDataLen is the size of the whole skip region for a term, which is empty
// only for a term with neither a full block nor a tail.
func skipDataLen(numFullBlocks, tailLen int, hasFreqs bool) int {
	n := numFullBlocks * skipEntryLen(hasFreqs)
	if numFullBlocks > 0 {
		n += skipTailOffsetLen
	}
	if tailLen > 0 {
		n += skipTailLastDocLen
	}
	return n
}

type skipEntry struct {
	lastDoc     uint32
	blockOffset uint32
	docNumBits  uint8
	tfNumBits   uint8
	minNormID   uint8
	maxTF       uint8
}

func (e *skipEntry) encode(dst []byte, hasFreqs bool) int {
	binary.LittleEndian.PutUint32(dst[0:4], e.lastDoc)
	binary.LittleEndian.PutUint32(dst[4:8], e.blockOffset)
	dst[8] = e.docNumBits
	if !hasFreqs {
		return skipEntryLenNoFreqs
	}
	dst[9] = e.tfNumBits
	dst[10] = e.minNormID
	dst[11] = e.maxTF
	return skipEntryLenWithFreqs
}

func (e *skipEntry) decode(src []byte, hasFreqs bool) {
	e.lastDoc = binary.LittleEndian.Uint32(src[0:4])
	e.blockOffset = binary.LittleEndian.Uint32(src[4:8])
	e.docNumBits = src[8]
	if !hasFreqs {
		e.tfNumBits, e.minNormID, e.maxTF = 0, 0, 0
		return
	}
	e.tfNumBits = src[9]
	e.minNormID = src[10]
	e.maxTF = src[11]
}

// The block-max pair, minNormID and maxTF, bounds the BM25 contribution of
// every document in the block: the score rises with term frequency and falls
// with field length, so the shortest field paired with the highest frequency
// dominates.  The two need not come from the same document, which makes the
// bound looser than tantivy's joint argmax but keeps it a bound -- and a bound
// is all a block-max pruner requires.  Nothing consumes these yet; they are
// written now so that turning on block-max WAND later is not another format
// break.
//
// encodeBlockMaxTF caps a term frequency into a byte.  A saturated value
// decodes back as "unbounded", which over-estimates the block maximum, and an
// over-estimate is still a valid upper bound.
func encodeBlockMaxTF(tf uint32) uint8 {
	if tf >= math.MaxUint8 {
		return math.MaxUint8
	}
	return uint8(tf)
}

// ------------------------------------------------------------- term footer

type termFooter struct {
	docFreq    uint32
	flags      uint8
	payloadLen uint64
	locsLen    uint64
	skipLen    uint64

	// locChunkSize is the chunking the location blob was written with, carried
	// here rather than re-derived from the doc frequency the way earlier zap
	// versions did.  A merge cannot know a term's post-deletion frequency until
	// it has streamed the whole thing, but the location encoder needs its chunk
	// size before the first write; recording what was actually used removes the
	// coupling entirely.  Only present when the term records locations.
	locChunkSize uint64
}

func (h *termFooter) hasFreqs() bool { return h.flags&termHasFreqs != 0 }
func (h *termFooter) hasLocs() bool  { return h.flags&termHasLocs != 0 }

// maxTermFooterLen bounds the encoded footer: five uvarints and a flag byte.
const maxTermFooterLen = 5*binary.MaxVarintLen64 + 1

func (h *termFooter) encode(dst []byte) int {
	n := binary.PutUvarint(dst, uint64(h.docFreq))
	dst[n] = h.flags
	n++
	n += binary.PutUvarint(dst[n:], h.payloadLen)
	n += binary.PutUvarint(dst[n:], h.locsLen)
	n += binary.PutUvarint(dst[n:], h.skipLen)
	if h.hasLocs() {
		n += binary.PutUvarint(dst[n:], h.locChunkSize)
	}
	return n
}

// decode reads a footer from mem at offset, and returns the absolute offsets of
// the three regions that precede it.
func (h *termFooter) decode(mem []byte, offset uint64) (
	payloadStart, locsStart, skipStart uint64, err error) {
	buf := memAt(mem, offset, maxTermFooterLen)

	docFreq, n := binary.Uvarint(buf)
	if n <= 0 {
		return 0, 0, 0, fmt.Errorf("corrupt term footer: bad docFreq at offset %d", offset)
	}
	if docFreq > math.MaxUint32 {
		return 0, 0, 0, fmt.Errorf("corrupt term footer: docFreq %d out of range", docFreq)
	}
	h.docFreq = uint32(docFreq)
	if n >= len(buf) {
		return 0, 0, 0, fmt.Errorf("corrupt term footer: truncated at offset %d", offset)
	}
	h.flags = buf[n]
	n++

	var read int
	h.payloadLen, read = binary.Uvarint(buf[n:])
	if read <= 0 {
		return 0, 0, 0, fmt.Errorf("corrupt term footer: bad payloadLen at offset %d", offset)
	}
	n += read
	h.locsLen, read = binary.Uvarint(buf[n:])
	if read <= 0 {
		return 0, 0, 0, fmt.Errorf("corrupt term footer: bad locsLen at offset %d", offset)
	}
	n += read
	h.skipLen, read = binary.Uvarint(buf[n:])
	if read <= 0 {
		return 0, 0, 0, fmt.Errorf("corrupt term footer: bad skipLen at offset %d", offset)
	}
	n += read
	h.locChunkSize = 0
	if h.hasLocs() {
		h.locChunkSize, read = binary.Uvarint(buf[n:])
		if read <= 0 {
			return 0, 0, 0, fmt.Errorf("corrupt term footer: bad locChunkSize at offset %d", offset)
		}
	}

	total := h.payloadLen + h.locsLen + h.skipLen
	if total > offset {
		return 0, 0, 0, fmt.Errorf(
			"corrupt term footer at offset %d: regions total %d bytes", offset, total)
	}
	skipStart = offset - h.skipLen
	locsStart = skipStart - h.locsLen
	payloadStart = locsStart - h.payloadLen
	return payloadStart, locsStart, skipStart, nil
}

// memAt returns up to n bytes of mem starting at offset, clamped to the end of
// the buffer.  Varint decoding wants a window it can over-read; clamping keeps
// a footer near the end of the segment from slicing out of range.
func memAt(mem []byte, offset uint64, n int) []byte {
	if offset >= uint64(len(mem)) {
		return nil
	}
	end := offset + uint64(n)
	if end > uint64(len(mem)) {
		end = uint64(len(mem))
	}
	return mem[offset:end]
}

// --------------------------------------------------------------- norms column

// Field norms: one quantized byte per document, in a column shared by every
// term of the field.  See fieldnorm.go for the quantization.
const (
	normsKindAbsent   = 0
	normsKindConstant = 1
	normsKindDense    = 2
)

type normsColumn struct {
	kind     uint8
	constant uint8
	dense    []uint8
}

// normsAbsent is the column a field without norms gets; every lookup returns
// id 0.
var normsAbsent = &normsColumn{kind: normsKindAbsent}

// id returns the quantized field length for a document.  A document that does
// not contain the field reads as 0, matching what the writer back-fills.
func (n *normsColumn) id(docNum uint32) uint8 {
	switch n.kind {
	case normsKindDense:
		if int(docNum) < len(n.dense) {
			return n.dense[docNum]
		}
	case normsKindConstant:
		return n.constant
	}
	return 0
}

// encodeNormsColumn writes the column for one field.  ids has one entry per
// document in the segment; a field that every document agrees on -- a keyword
// field, say -- collapses to two bytes instead of one per document, which is
// what keeps a wide schema from paying numFields*numDocs.
func encodeNormsColumn(ids []uint8, dst []byte) []byte {
	if len(ids) == 0 {
		return append(dst, normsKindAbsent)
	}
	constant := true
	for _, id := range ids[1:] {
		if id != ids[0] {
			constant = false
			break
		}
	}
	if constant {
		return append(dst, normsKindConstant, ids[0])
	}
	dst = append(dst, normsKindDense)
	return append(dst, ids...)
}

func decodeNormsColumn(buf []byte, numDocs uint64) (*normsColumn, error) {
	if len(buf) == 0 {
		return normsAbsent, nil
	}
	switch buf[0] {
	case normsKindAbsent:
		return normsAbsent, nil
	case normsKindConstant:
		if len(buf) < 2 {
			return nil, fmt.Errorf("corrupt norms column: truncated constant")
		}
		return &normsColumn{kind: normsKindConstant, constant: buf[1]}, nil
	case normsKindDense:
		dense := buf[1:]
		if uint64(len(dense)) != numDocs {
			return nil, fmt.Errorf("corrupt norms column: %d entries for %d docs",
				len(dense), numDocs)
		}
		return &normsColumn{kind: normsKindDense, dense: dense}, nil
	}
	return nil, fmt.Errorf("corrupt norms column: unknown kind %d", buf[0])
}

// ------------------------------------------------------------------- tooling

// TermPostingsInfo describes one term's postings on disk. It exists for the zap
// command-line tool, which lives outside this package and cannot reach the
// footer codec directly.
type TermPostingsInfo struct {
	OneHit     bool
	DocNum     uint64 // only meaningful when OneHit
	DocFreq    uint32
	HasFreqs   bool
	HasLocs    bool
	NumBlocks  int
	TailLen    int
	PayloadLen uint64
	LocsLen    uint64
	SkipLen    uint64

	PayloadStart uint64
	LocsStart    uint64
	SkipStart    uint64
	LocChunkSize uint64
}

// DescribeTermPostings decodes the term footer an FST value points at.
func DescribeTermPostings(mem []byte, fstVal uint64) (*TermPostingsInfo, error) {
	if fstVal&FSTValEncodingMask == FSTValEncoding1Hit {
		return &TermPostingsInfo{
			OneHit:  true,
			DocNum:  FSTValDecode1Hit(fstVal),
			DocFreq: 1,
		}, nil
	}
	var h termFooter
	payloadStart, locsStart, skipStart, err := h.decode(mem, fstVal)
	if err != nil {
		return nil, err
	}
	return &TermPostingsInfo{
		DocFreq:      h.docFreq,
		HasFreqs:     h.hasFreqs(),
		HasLocs:      h.hasLocs(),
		NumBlocks:    int(h.docFreq) / postingsBlockLen,
		TailLen:      int(h.docFreq) % postingsBlockLen,
		PayloadLen:   h.payloadLen,
		LocsLen:      h.locsLen,
		SkipLen:      h.skipLen,
		PayloadStart: payloadStart,
		LocsStart:    locsStart,
		SkipStart:    skipStart,
		LocChunkSize: h.locChunkSize,
	}, nil
}
