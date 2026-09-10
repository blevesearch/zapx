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

// postingsSerializer writes one term's postings in the block format described
// in postings_format.go.  It is the only implementation of the write side:
// building a fresh segment and merging existing ones both drive it, so there is
// no way for the two to drift apart.
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

	skipBuf []byte
	packBuf []byte
	tailBuf []byte
	footerBuf  []byte
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

// blockBound returns the pair that bounds the BM25 contribution of every
// document in the staged block: the shortest field length and the highest term
// frequency.  See the note in postings_format.go on why the two need not come
// from the same document.
func (s *postingsSerializer) blockBound() (minNormID, maxTF uint8) {
	minNormID = 0xFF
	var mtf uint32
	for i := 0; i < postingsBlockLen; i++ {
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
		e.minNormID, e.maxTF = s.blockBound()
	}
	var entry [skipEntryLenWithFreqs]byte
	s.skipBuf = append(s.skipBuf, entry[:e.encode(entry[:], s.hasFreqs)]...)

	s.lastDoc = s.docs[postingsBlockLen-1]
	s.n = 0
	return nil
}

// OneHit reports the FST value for a term that can skip the general encoding
// entirely: a single document, a frequency of one, no locations, and a doc
// number that fits the 31 bits available.  Nothing has been written to w at
// that point, because a term this small never filled a block.
//
// The long tail of a real dictionary is terms like this, so avoiding an
// indirection for them is worth the special case.  Unlike earlier zap versions
// the norm does not have to be carried in the FST value -- it is in the field's
// norm column -- which leaves the encoding to just the doc number.
func (s *postingsSerializer) OneHit() (uint64, bool) {
	if s.docFreq != 1 || s.hasLocs || s.n != 1 {
		return 0, false
	}
	if s.hasFreqs && s.freqs[0] != 1 {
		return 0, false
	}
	docNum := uint64(s.docs[0])
	if !under32Bits(docNum) {
		return 0, false
	}
	return FSTValEncode1Hit(docNum), true
}

// FinishPayload flushes the leftover documents as a uvarint tail and closes out
// the payload region.  Fewer than 128 documents cannot be bitpacked, and the
// tail is short by construction, so uvarints cost nothing worth optimising.
func (s *postingsSerializer) FinishPayload() error {
	tailOffset := uint64(s.w.Count()) - s.payloadStart

	var tailLastDoc uint32
	if s.n > 0 {
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
		tailLastDoc = prev

		processed := s.w.process(buf)
		if _, err := s.w.Write(processed); err != nil {
			return err
		}
	}

	// The skip list records where the tail begins, which also gives the last
	// full block its length. Only meaningful once there is at least one full
	// block -- with none, the tail starts at the payload itself.
	if len(s.skipBuf) > 0 {
		if tailOffset > math.MaxUint32 {
			return fmt.Errorf("postings payload for a single term exceeds 4GiB")
		}
		var off [skipTailOffsetLen]byte
		binary.LittleEndian.PutUint32(off[:], uint32(tailOffset))
		s.skipBuf = append(s.skipBuf, off[:]...)
	}

	// The tail's own last doc number lets a seek past a term's entire range --
	// full blocks and tail alike -- conclude "not present" from skip metadata,
	// the same way it already can for a target past a full block. Without
	// this, that seek would have to decode the tail's uvarint stream just to
	// discover it doesn't contain the target. This is the common case: most
	// terms in a real dictionary never fill even one 128-document block, so
	// they are nothing but a tail, and this is the only bound they get.
	if s.n > 0 {
		var last [skipTailLastDocLen]byte
		binary.LittleEndian.PutUint32(last[:], tailLastDoc)
		s.skipBuf = append(s.skipBuf, last[:]...)
	}

	s.payloadEnd = uint64(s.w.Count())
	s.n = 0
	return nil
}

// Close writes the skip region and the term footer, and returns the footer's
// offset, which is what goes into the FST.
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

// BytesWritten reports the payload plus skip bytes this term contributed, for
// the segment's write statistics.
func (s *postingsSerializer) BytesWritten() uint64 {
	return (s.payloadEnd - s.payloadStart) + uint64(len(s.skipBuf))
}
