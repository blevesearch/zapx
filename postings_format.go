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

// The code here decides the on disk shape of the postings list along with
// the norms column (which is persisted separately, refer to zap.md)
//
// The norms column is not part of the postings list anymore like prior
// versions of zapx, and is instead just a list organized by docid. The
// norms are also quantized for compression; see fieldnorm.go.
//
// Docids and freqs are bitpacked in blocks of 128, and if the tail of the
// postings list contains less than 128 docs, we use varints to pack them
// instead. The postingsSerializer struct here is responsible for writing
// the postings list, and the blockCursor in postings_block.go reads it.
//
// The locs column follows the old intcoder/intdecoder format.
//
// The skip entries contain metadata about each block, including the tail
// block. The skipEntry struct is responsible for encoding/decoding it, and
// the blockCursor iterates over all the skip entries.
//
// Finally, the term footer contains the offsets and lengths about the previous
// sections. The vellum FST points to the term footer.
//
// The regions are organized as follows:
// (--- per field ---)
// [norms column]
//    (--- per term ---)
//    [postings list]
//    [locs]
//    [skip entries]
//    [term footer]

// --------------------------------------------------------------------------
//
// NORMS
//
// --------------------------------------------------------------------------

// Field norms: one quantized byte per document, in a column shared by every
// term of the field.  See fieldnorm.go for the quantization.
const (
	// Used when field norms are not needed/used
	normsKindAbsent = 0
	// Used when all documents in the field have the same field norm. Instead
	// of storing an entry for every doc separately, we store only one entry
	// representing all docs.
	normsKindConstant = 1
	// Used when documents do not have the same norm in a field. We store a
	// unique entry per doc.
	normsKindDense = 2
)

type normsColumn struct {
	kind     uint8
	constant uint8
	dense    []uint8
}

// normsAbsent is the column a field without norms gets; every lookup returns
// id 0.
var normsAbsent = &normsColumn{kind: normsKindAbsent}

// id returns the quantized field length for a document. A document without
// the field returns 0
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

// Writes the column for one field.
//
// normsKindAbsent:   [0]
//
// normsKindConstant: [1][constantNorm] -> constantNorm shared by all docs
//
// normsKindDense:    [2][norm1][norm2]...[normn] -> one norm per doc
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

// --------------------------------------------------------------------------
//
// POSTINGS LIST
//
// --------------------------------------------------------------------------

// Signifies the number of postings bitpacked into a block. This depends on
// the SIMD kernels defined in freeway/bitpack, so if this number needs to be
// changed the SIMD kernels have to be rewritten to support that new block
// width.
const postingsBlockLen = bitpack.BlockLen

// postingsSerializer writes one term's postings in the block format described
// above.  It is the only implementation of the write side: building a fresh
// segment and merging existing ones both drive it, so there is no way for the
// two to drift apart.
//
// The caller's protocol for one term is:
//
//	s.StartTerm(hasFreqs, hasLocs, normIDs)
//	for each doc in increasing doc number: s.AddDoc(docNum, freq)
//	if v, ok := s.OneHit(); ok {
//	        // nothing was written; v is the FST value
//	} else {
//	        s.FinishPayload()
//	        _, locBytes, _ := locEncoder.writeAt(w)
//	        off, _ := s.Close(uint64(locBytes), locChunkSize)
//	}
//
// Everything but the skip buffer streams straight to the writer, so a term with
// a million documents costs a 128-slot staging block and about eight kilobytes
// of skip data, not a copy of its whole postings list.
//
// Close, the last step of the protocol, is under TERM FOOTER below rather
// than here alongside the rest of this type's methods: its job is writing
// the footer (and the skip region the footer points at), not the postings
// list, so it's grouped with the format it produces instead of the type it
// happens to be a method of.
type postingsSerializer struct {
	// w is the segment writer every term's postings land in. It is set once,
	// for the life of the serializer -- a single build or merge writes to one
	// FileWriter throughout -- rather than threaded through every method call.
	w *FileWriter

	docs  [postingsBlockLen]uint32
	freqs [postingsBlockLen]uint32
	n     int // documents staged in docs/freqs

	docFreq  uint32
	lastDoc  uint32 // last doc of the last flushed block: the delta anchor
	hasFreqs bool
	hasLocs  bool

	// normIDs is the field's quantized norm column, indexed by doc number.  It
	// is only read to compute the per-block score bound, and may be nil.
	normIDs []uint8

	payloadStart uint64
	payloadEnd   uint64

	skipBuf   []byte
	packBuf   []byte
	tailBuf   []byte
	footerBuf []byte
}

func (s *postingsSerializer) StartTerm(hasFreqs, hasLocs bool, normIDs []uint8) {
	s.n = 0
	s.docFreq = 0
	s.lastDoc = 0
	s.hasFreqs = hasFreqs
	s.hasLocs = hasLocs
	s.normIDs = normIDs
	s.payloadStart = uint64(s.w.Count())
	s.payloadEnd = s.payloadStart
	s.skipBuf = s.skipBuf[:0]
}

// AddDoc stages one posting.  Doc numbers must arrive strictly increasing.
func (s *postingsSerializer) AddDoc(docNum, freq uint32) error {
	s.docs[s.n] = docNum
	s.freqs[s.n] = freq
	s.n++
	s.docFreq++
	if s.n == postingsBlockLen {
		return s.flushBlock()
	}
	return nil
}

// blockBound returns the pair that bounds the BM25 contribution of the first
// n staged documents: the shortest field length and the highest term
// frequency -- what a skip entry's minNormID/maxTF record; see the note
// under SKIP ENTRIES on why the two need not come from the same document.
// Called with postingsBlockLen for a full block and with s.n for the tail.
func (s *postingsSerializer) blockBound(n int) (minNormID, maxTF uint8) {
	minNormID = 0xFF
	var mtf uint32
	for i := 0; i < n; i++ {
		var id uint8
		if int(s.docs[i]) < len(s.normIDs) {
			id = s.normIDs[s.docs[i]]
		}
		if id < minNormID {
			minNormID = id
		}
		if s.freqs[i] > mtf {
			mtf = s.freqs[i]
		}
	}
	return minNormID, encodeBlockMaxTF(mtf)
}

func (s *postingsSerializer) flushBlock() error {
	blockOffset := uint64(s.w.Count()) - s.payloadStart
	if blockOffset > math.MaxUint32 {
		return fmt.Errorf("postings payload for a single term exceeds 4GiB")
	}

	docNumBits := bitpack.MaxBitsDelta1(&s.docs, s.lastDoc)
	size := bitpack.BlockBytes(docNumBits)
	var tfNumBits uint8
	if s.hasFreqs {
		tfNumBits = bitpack.MaxBitsMinus1(&s.freqs)
		size += bitpack.BlockBytes(tfNumBits)
	}

	if cap(s.packBuf) < size {
		s.packBuf = make([]byte, size)
	}
	buf := s.packBuf[:size]
	bitpack.PackDelta1(&s.docs, s.lastDoc, docNumBits, buf)
	if s.hasFreqs {
		bitpack.PackMinus1(&s.freqs, tfNumBits, buf[bitpack.BlockBytes(docNumBits):])
	}

	// Each block goes through the writer's hook on its own.  That is what lets
	// a reader with an encryption or compression callback decode a single
	// block rather than a whole term, and it is why the skip entry records the
	// block's byte offset instead of deriving it from the bit widths.
	processed := s.w.process(buf)
	if _, err := s.w.Write(processed); err != nil {
		return err
	}

	e := skipEntry{
		lastDoc:     s.docs[postingsBlockLen-1],
		blockOffset: uint32(blockOffset),
		docNumBits:  docNumBits,
		tfNumBits:   tfNumBits,
	}
	if s.hasFreqs {
		e.minNormID, e.maxTF = s.blockBound(postingsBlockLen)
	}
	var entry [skipEntryLenWithFreqs]byte
	s.skipBuf = append(s.skipBuf, entry[:e.encode(entry[:], s.hasFreqs)]...)

	s.lastDoc = s.docs[postingsBlockLen-1]
	s.n = 0
	return nil
}

// OneHit reports the FST value for a term that can skip the general encoding
// entirely: a single document, no locations, and a doc number and frequency
// that both fit the bits available.  Nothing has been written to w at that
// point, because a term this small never filled a block.
//
// The long tail of a real dictionary is terms like this, so avoiding an
// indirection for them is worth the special case.  Unlike earlier zap versions
// the norm does not have to be carried in the FST value -- it is in the
// field's norm column -- which leaves the encoding to just the doc number and
// the frequency.  When the field has frequencies disabled, freq is not read
// from the document at all: 1 is encoded, matching what a reader gets back
// for any term in such a field regardless of encoding (see wantFreqs in
// postings_block.go).
func (s *postingsSerializer) OneHit() (uint64, bool) {
	if s.docFreq != 1 || s.hasLocs || s.n != 1 {
		return 0, false
	}
	freq := uint64(1)
	if s.hasFreqs {
		freq = uint64(s.freqs[0])
		if !under32Bits(freq) {
			return 0, false
		}
	}
	docNum := uint64(s.docs[0])
	if !under32Bits(docNum) {
		return 0, false
	}
	return FSTValEncode1Hit(docNum, freq), true
}

// FinishPayload flushes the leftover documents as a uvarint tail and closes out
// the payload region.  Fewer than 128 documents cannot be bitpacked, and the
// tail is short by construction, so uvarints cost nothing worth optimising.
func (s *postingsSerializer) FinishPayload() error {
	tailOffset := uint64(s.w.Count()) - s.payloadStart

	if s.n > 0 {
		if tailOffset > math.MaxUint32 {
			return fmt.Errorf("postings payload for a single term exceeds 4GiB")
		}

		buf := s.tailBuf[:0]
		// Plain deltas here, not delta-minus-one: the tail is decoded by the
		// uvarint reader, which has no bit width to save.
		prev := s.lastDoc
		for i := 0; i < s.n; i++ {
			buf = binary.AppendUvarint(buf, uint64(s.docs[i]-prev))
			prev = s.docs[i]
		}
		if s.hasFreqs {
			for i := 0; i < s.n; i++ {
				buf = binary.AppendUvarint(buf, uint64(s.freqs[i]))
			}
		}
		s.tailBuf = buf

		processed := s.w.process(buf)
		if _, err := s.w.Write(processed); err != nil {
			return err
		}

		// The tail is a skipEntry in every sense that matters -- lastDoc is
		// what lets a seek past a term's entire range conclude "not present"
		// from skip metadata alone instead of always decoding the tail, and
		// blockOffset is where its bytes start, exactly like a full block.
		// This is the common case: most terms in a real dictionary never
		// fill even one 128-document block, so they are nothing but a tail,
		// and this is the only bound they get.
		e := skipEntry{
			lastDoc:     prev,
			blockOffset: uint32(tailOffset),
		}
		if s.hasFreqs {
			e.minNormID, e.maxTF = s.blockBound(s.n)
		}
		var entry [tailEntryLenWithFreqs]byte
		s.skipBuf = append(s.skipBuf, entry[:e.encodeTail(entry[:], s.hasFreqs)]...)
	}

	s.payloadEnd = uint64(s.w.Count())
	s.n = 0
	return nil
}

// BytesWritten reports the payload plus skip bytes this term contributed, for
// the segment's write statistics.
func (s *postingsSerializer) BytesWritten() uint64 {
	return (s.payloadEnd - s.payloadStart) + uint64(len(s.skipBuf))
}

// --------------------------------------------------------------------------
//
// LOCS
//
// --------------------------------------------------------------------------

// Term locations are not encoded or decoded in this file. They are a
// chunkedIntCoder/chunkedIntDecoder blob (intcoder.go/intdecoder.go),
// unchanged from earlier zap versions, written once per field rather than
// once per term, so nothing here re-derives their format. What does live
// here is the metadata a term's footer needs to find and size that blob --
// locsLen and locChunkSize, under TERM FOOTER below -- and the hasLocs flag
// recording whether a term has one at all.

// --------------------------------------------------------------------------
//
// SKIP ENTRIES
//
// --------------------------------------------------------------------------

// Skip entry layout.  Each full block gets one entry:
//
//	u32 lastDocInBlock
//	u32 blockByteOffset      (from the start of the payload)
//	u8  docNumBits
//	u8  tfNumBits            \
//	u8  minNormID             > only when the term records frequencies
//	u8  maxTF                /
//
// followed, whenever the term has a tail (fewer than 128 trailing documents
// that didn't fill a block -- the common case, since most terms in a real
// dictionary never fill even one block), by one more entry describing it:
//
//	u32 lastDocInTail
//	u32 tailByteOffset       (from the start of the payload)
//	u8  minNormID            \  only when the term records frequencies;
//	u8  maxTF                /  no docNumBits/tfNumBits -- the tail is a
//	                            plain uvarint run, never bit-packed, so
//	                            there is no bit width to record
//
// The tail's entry is a skipEntry in every sense that matters -- same
// fields, same meaning, decoded into the same struct a reader already uses
// for full blocks, so nothing consuming it (seekBlock's bound check, and
// later a block-max pruner) needs a separate code path for "am I looking at
// a block or the tail". It is two bytes shorter on the wire only because
// docNumBits/tfNumBits can never mean anything for it, not because of some
// second condition layered on top of hasFreqs.
//
// Without lastDocInTail, a seek past a term's entire range would have no way
// to tell "the tail might still hold the target" apart from "the tail's own
// range ends before the target", and would have to decode the tail's uvarint
// stream just to find out which. With it, that seek is a skip-metadata
// comparison, the same as seeking past a full block.
//
// A term whose docFreq is an exact multiple of 128 has no tail at all, and
// so no trailing entry either; the last full block's byte range in that case
// is bounded by the term footer's own payloadLen instead.
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
	tailEntryLenNoFreqs   = 8
	tailEntryLenWithFreqs = 10
)

func skipEntryLen(hasFreqs bool) int {
	if hasFreqs {
		return skipEntryLenWithFreqs
	}
	return skipEntryLenNoFreqs
}

func tailEntryLen(hasFreqs bool) int {
	if hasFreqs {
		return tailEntryLenWithFreqs
	}
	return tailEntryLenNoFreqs
}

// skipDataLen is the size of the whole skip region for a term, which is empty
// only for a term with neither a full block nor a tail.
func skipDataLen(numFullBlocks, tailLen int, hasFreqs bool) int {
	n := numFullBlocks * skipEntryLen(hasFreqs)
	if tailLen > 0 {
		n += tailEntryLen(hasFreqs)
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

// encodeTail is encode without docNumBits/tfNumBits: the tail is never
// bit-packed, so there is no bit width to record for it.
func (e *skipEntry) encodeTail(dst []byte, hasFreqs bool) int {
	binary.LittleEndian.PutUint32(dst[0:4], e.lastDoc)
	binary.LittleEndian.PutUint32(dst[4:8], e.blockOffset)
	if !hasFreqs {
		return tailEntryLenNoFreqs
	}
	dst[8] = e.minNormID
	dst[9] = e.maxTF
	return tailEntryLenWithFreqs
}

func (e *skipEntry) decodeTail(src []byte, hasFreqs bool) {
	e.lastDoc = binary.LittleEndian.Uint32(src[0:4])
	e.blockOffset = binary.LittleEndian.Uint32(src[4:8])
	e.docNumBits, e.tfNumBits = 0, 0
	if !hasFreqs {
		e.minNormID, e.maxTF = 0, 0
		return
	}
	e.minNormID = src[8]
	e.maxTF = src[9]
}

// The block-max pair, minNormID and maxTF, bounds the BM25 contribution of
// every document in the block (or tail): the score rises with term frequency
// and falls with field length, so the shortest field paired with the highest
// frequency dominates.  The two need not come from the same document, which
// makes the bound looser than tantivy's joint argmax but keeps it a bound --
// and a bound is all a block-max pruner requires.  Nothing consumes these
// yet; they are written now so that turning on block-max WAND later is not
// another format break.
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

// --------------------------------------------------------------------------
//
// TERM FOOTER
//
// --------------------------------------------------------------------------

// Term footer flags.
const (
	termHasFreqs = 1 << 0
	termHasLocs  = 1 << 1
)

type termFooter struct {
	docFreq    uint32
	flags      uint8
	payloadLen uint64
	locsLen    uint64
	skipLen    uint64

	// payloadStart, locsStart, and skipStart are the absolute offsets of the
	// three regions the footer sits after, resolved once by decode from the
	// footer's own position (its only input that isn't already a field here)
	// and cached rather than recomputed by every caller that needs them.
	payloadStart uint64
	locsStart    uint64
	skipStart    uint64

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

// decode reads a footer from mem at offset, and resolves the absolute offsets
// of the three regions that precede it (payloadStart, locsStart, skipStart)
// onto the footer itself, so it is fully self-describing from here on.
func (h *termFooter) decode(mem []byte, offset uint64) error {
	buf := memAt(mem, offset, maxTermFooterLen)

	docFreq, n := binary.Uvarint(buf)
	if n <= 0 {
		return fmt.Errorf("corrupt term footer: bad docFreq at offset %d", offset)
	}
	if docFreq > math.MaxUint32 {
		return fmt.Errorf("corrupt term footer: docFreq %d out of range", docFreq)
	}
	h.docFreq = uint32(docFreq)
	if n >= len(buf) {
		return fmt.Errorf("corrupt term footer: truncated at offset %d", offset)
	}
	h.flags = buf[n]
	n++

	var read int
	h.payloadLen, read = binary.Uvarint(buf[n:])
	if read <= 0 {
		return fmt.Errorf("corrupt term footer: bad payloadLen at offset %d", offset)
	}
	n += read
	h.locsLen, read = binary.Uvarint(buf[n:])
	if read <= 0 {
		return fmt.Errorf("corrupt term footer: bad locsLen at offset %d", offset)
	}
	n += read
	h.skipLen, read = binary.Uvarint(buf[n:])
	if read <= 0 {
		return fmt.Errorf("corrupt term footer: bad skipLen at offset %d", offset)
	}
	n += read
	h.locChunkSize = 0
	if h.hasLocs() {
		h.locChunkSize, read = binary.Uvarint(buf[n:])
		if read <= 0 {
			return fmt.Errorf("corrupt term footer: bad locChunkSize at offset %d", offset)
		}
	}

	total := h.payloadLen + h.locsLen + h.skipLen
	if total > offset {
		return fmt.Errorf(
			"corrupt term footer at offset %d: regions total %d bytes", offset, total)
	}
	h.skipStart = offset - h.skipLen
	h.locsStart = h.skipStart - h.locsLen
	h.payloadStart = h.locsStart - h.payloadLen
	return nil
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

// Close writes the skip region and the term footer, and returns the footer's
// offset, which is what goes into the FST. The last step of
// postingsSerializer's protocol -- see the type's own doc comment, under
// POSTINGS LIST, for why this method lives here instead of alongside it.
func (s *postingsSerializer) Close(locsLen, locChunkSize uint64) (uint64, error) {
	var skipLen uint64
	if len(s.skipBuf) > 0 {
		processed := s.w.process(s.skipBuf)
		if _, err := s.w.Write(processed); err != nil {
			return 0, err
		}
		skipLen = uint64(len(processed))
	}

	footerOffset := uint64(s.w.Count())
	h := termFooter{
		docFreq:    s.docFreq,
		payloadLen: s.payloadEnd - s.payloadStart,
		locsLen:    locsLen,
		skipLen:    skipLen,
	}
	if s.hasFreqs {
		h.flags |= termHasFreqs
	}
	if s.hasLocs && locsLen > 0 {
		h.flags |= termHasLocs
		h.locChunkSize = locChunkSize
	}

	if cap(s.footerBuf) < maxTermFooterLen {
		s.footerBuf = make([]byte, maxTermFooterLen)
	}
	buf := s.footerBuf[:maxTermFooterLen]
	if _, err := s.w.Write(buf[:h.encode(buf)]); err != nil {
		return 0, err
	}

	// A footer at offset zero would be indistinguishable from "this term has no
	// postings", which is how the writer signals an empty term to the FST
	// builder.  The field's data never starts at zero -- the segment begins
	// with stored fields -- so this cannot happen in practice.
	if footerOffset == 0 {
		return 0, fmt.Errorf("postings footer landed at offset 0")
	}
	return footerOffset, nil
}

// -------------------------------------------------------------------- tooling

// TermPostingsInfo describes one term's postings on disk. It exists for the zap
// command-line tool, which lives outside this package and cannot reach the
// footer codec directly.
type TermPostingsInfo struct {
	OneHit     bool
	DocNum     uint64 // only meaningful when OneHit
	Freq       uint64 // only meaningful when OneHit
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
		docNum, freq := FSTValDecode1Hit(fstVal)
		return &TermPostingsInfo{
			OneHit:  true,
			DocNum:  docNum,
			Freq:    freq,
			DocFreq: 1,
		}, nil
	}
	var h termFooter
	if err := h.decode(mem, fstVal); err != nil {
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
		PayloadStart: h.payloadStart,
		LocsStart:    h.locsStart,
		SkipStart:    h.skipStart,
		LocChunkSize: h.locChunkSize,
	}, nil
}
