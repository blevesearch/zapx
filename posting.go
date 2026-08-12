//  Copyright (c) 2017 Couchbase, Inc.
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
	"reflect"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizePostingsList int
var reflectStaticSizePostingsIterator int
var reflectStaticSizePosting int
var reflectStaticSizeLocation int

func init() {
	var pl PostingsList
	reflectStaticSizePostingsList = int(reflect.TypeOf(pl).Size())
	var pi PostingsIterator
	reflectStaticSizePostingsIterator = int(reflect.TypeOf(pi).Size())
	var p Posting
	reflectStaticSizePosting = int(reflect.TypeOf(p).Size())
	var l Location
	reflectStaticSizeLocation = int(reflect.TypeOf(l).Size())
}

// FST or vellum value (uint64) encoding is determined by the top two
// highest-order or most significant bits...
//
//  encoding  : MSB
//  name      : 63  62  61...to...bit #0 (LSB)
//  ----------+---+---+---------------------------------------------------
//   general  : 0 | 0 | 62-bits of postingsOffset.
//   ~        : 0 | 1 | reserved for future.
//   1-hit    : 1 | 0 | 31-bits of positive float31 norm | 31-bits docNum.
//   ~        : 1 | 1 | reserved for future.
//
// Encoding "general" is able to handle all cases, where the
// postingsOffset points to more information about the postings for
// the term.
//
// Encoding "1-hit" is used to optimize a commonly seen case when a
// term has only a single hit.  For example, a term in the _id field
// will have only 1 hit.  The "1-hit" encoding is used for a term
// in a field when...
//
// - term vector info is disabled for that field;
// - and, the term appears in only a single doc for that field;
// - and, the term's freq is exactly 1 in that single doc for that field;
// - and, the docNum must fit into 31-bits;
//
// Otherwise, the "general" encoding is used instead.
//
// In the "1-hit" encoding, the field in that single doc may have
// other terms, which is supported in the "1-hit" encoding by the
// positive float31 norm.

const FSTValEncodingMask = uint64(0xc000000000000000)
const FSTValEncodingGeneral = uint64(0x0000000000000000)
const FSTValEncoding1Hit = uint64(0x8000000000000000)

func FSTValEncode1Hit(docNum uint64, normBits uint64) uint64 {
	return FSTValEncoding1Hit | ((mask31Bits & normBits) << 31) | (mask31Bits & docNum)
}

func FSTValDecode1Hit(v uint64) (docNum uint64, normBits uint64) {
	return (mask31Bits & v), (mask31Bits & (v >> 31))
}

const mask31Bits = uint64(0x000000007fffffff)

func under32Bits(x uint64) bool {
	return x <= mask31Bits
}

const DocNum1HitFinished = math.MaxUint64

var NormBits1Hit = uint64(1)

// PostingsList is an in-memory representation of a postings list
type PostingsList struct {
	sb             *SegmentBase
	postingsOffset uint64
	freqOffset     uint64
	locOffset      uint64
	postings       *roaring.Bitmap
	except         *roaring.Bitmap

	// when normBits1Hit != 0, then this postings list came from a
	// 1-hit encoding, and only the docNum1Hit & normBits1Hit apply
	docNum1Hit   uint64
	normBits1Hit uint64

	chunkSize uint64

	bytesRead uint64
}

// represents an immutable, empty postings list
var emptyPostingsList = &PostingsList{}

func (p *PostingsList) Size() int {
	sizeInBytes := reflectStaticSizePostingsList + SizeOfPtr

	if p.except != nil {
		sizeInBytes += int(p.except.GetSizeInBytes())
	}

	return sizeInBytes
}

func (p *PostingsList) OrInto(receiver *roaring.Bitmap) {
	if p.normBits1Hit != 0 {
		receiver.Add(uint32(p.docNum1Hit))
		return
	}

	if p.postings != nil {
		receiver.Or(p.postings)
	}
}

// Iterator returns an iterator for this postings list
func (p *PostingsList) Iterator(includeFreq, includeNorm, includeLocs bool,
	prealloc segment.PostingsIterator) segment.PostingsIterator {
	if p.normBits1Hit == 0 && p.postings == nil {
		return emptyPostingsIterator
	}

	var preallocPI *PostingsIterator
	pi, ok := prealloc.(*PostingsIterator)
	if ok && pi != nil {
		preallocPI = pi
	}
	if preallocPI == emptyPostingsIterator {
		preallocPI = nil
	}

	return p.iterator(includeFreq, includeNorm, includeLocs, preallocPI)
}

func (p *PostingsList) iterator(includeFreq, includeNorm, includeLocs bool,
	rv *PostingsIterator) *PostingsIterator {
	if rv == nil {
		rv = &PostingsIterator{}
	} else {
		freqNormReader := rv.freqNormReader
		if freqNormReader != nil {
			freqNormReader.reset()
		}

		locReader := rv.locReader
		if locReader != nil {
			locReader.reset()
		}

		nextLocs := rv.nextLocs[:0]
		nextSegmentLocs := rv.nextSegmentLocs[:0]

		buf := rv.buf

		*rv = PostingsIterator{} // clear the struct

		rv.freqNormReader = freqNormReader
		rv.locReader = locReader

		rv.nextLocs = nextLocs
		rv.nextSegmentLocs = nextSegmentLocs

		rv.buf = buf
	}

	rv.postings = p
	rv.includeFreqNorm = includeFreq || includeNorm || includeLocs
	rv.includeLocs = includeLocs

	if p.normBits1Hit != 0 {
		// "1-hit" encoding
		rv.docNum1Hit = p.docNum1Hit
		rv.normBits1Hit = p.normBits1Hit

		if p.except != nil && p.except.Contains(uint32(rv.docNum1Hit)) {
			rv.docNum1Hit = DocNum1HitFinished
		}

		return rv
	}

	// "general" encoding, check if empty
	if p.postings == nil {
		return rv
	}

	// initialize freq chunk reader
	if rv.includeFreqNorm {
		rv.freqNormReader = newChunkedIntDecoder(p.sb.mem, p.freqOffset, rv.freqNormReader, p.sb.fileReader)
		rv.incrementBytesRead(rv.freqNormReader.getBytesRead())
	}

	// initialize the loc chunk reader
	if rv.includeLocs {
		rv.locReader = newChunkedIntDecoder(p.sb.mem, p.locOffset, rv.locReader, p.sb.fileReader)
		rv.incrementBytesRead(rv.locReader.getBytesRead())
	}

	rv.all = p.postings.Iterator()
	if p.except != nil {
		rv.ActualBM = roaring.AndNot(p.postings, p.except)
		rv.Actual = rv.ActualBM.Iterator()
	} else {
		rv.ActualBM = p.postings
		rv.Actual = rv.all // Optimize to use same iterator for all & Actual.
	}

	return rv
}

// Count returns the number of items on this postings list
func (p *PostingsList) Count() uint64 {
	var n, e uint64
	if p.normBits1Hit != 0 {
		n = 1
		if p.except != nil && p.except.Contains(uint32(p.docNum1Hit)) {
			e = 1
		}
	} else if p.postings != nil {
		n = p.postings.GetCardinality()
		if p.except != nil {
			e = p.postings.AndCardinality(p.except)
		}
	}
	return n - e
}

// Implements the segment.DiskStatsReporter interface
// The purpose of this implementation is to get
// the bytes read from the postings lists stored
// on disk, while querying
func (p *PostingsList) ResetBytesRead(val uint64) {
	p.bytesRead = val
}

func (p *PostingsList) BytesRead() uint64 {
	return p.bytesRead
}

func (p *PostingsList) incrementBytesRead(val uint64) {
	p.bytesRead += val
}

func (p *PostingsList) BytesWritten() uint64 {
	return 0
}

func (rv *PostingsList) read(postingsOffset uint64, d *Dictionary) error {
	rv.postingsOffset = postingsOffset

	// handle "1-hit" encoding special case
	if rv.postingsOffset&FSTValEncodingMask == FSTValEncoding1Hit {
		return rv.init1Hit(postingsOffset)
	}

	// read the location of the freq/norm details
	var n uint64
	var read int

	rv.freqOffset, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+binary.MaxVarintLen64])
	n += uint64(read)

	rv.locOffset, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+n+binary.MaxVarintLen64])
	n += uint64(read)

	var postingsLen uint64
	postingsLen, read = binary.Uvarint(d.sb.mem[postingsOffset+n : postingsOffset+n+binary.MaxVarintLen64])
	n += uint64(read)

	roaringBytes, err := d.sb.fileReader.process(d.sb.mem[postingsOffset+n : postingsOffset+n+postingsLen])
	if err != nil {
		return err
	}

	rv.incrementBytesRead(n + postingsLen)

	if rv.postings == nil {
		rv.postings = roaring.NewBitmap()
	}
	_, err = rv.postings.FromBuffer(roaringBytes)
	if err != nil {
		return fmt.Errorf("error loading roaring bitmap: %v", err)
	}

	chunkSize, err := getChunkSize(d.sb.chunkMode,
		rv.postings.GetCardinality(), d.sb.numDocs)
	if err != nil {
		return fmt.Errorf("failed to get chunk size: %v", err)
	}

	rv.chunkSize = chunkSize

	return nil
}

func (rv *PostingsList) init1Hit(fstVal uint64) error {
	docNum, normBits := FSTValDecode1Hit(fstVal)

	rv.docNum1Hit = docNum
	rv.normBits1Hit = normBits

	return nil
}

// PostingsIterator provides a way to iterate through the postings list
type PostingsIterator struct {
	postings *PostingsList
	all      roaring.IntPeekable
	Actual   roaring.IntPeekable
	ActualBM *roaring.Bitmap

	currChunk      uint32
	freqNormReader *chunkedIntDecoder
	locReader      *chunkedIntDecoder

	// cached result of the last doc-number-to-chunk division, valid for doc
	// numbers in [chunkLo, chunkHi). See chunkOf.
	chunkCached uint32
	chunkLo     uint64
	chunkHi     uint64

	next            Posting            // reused across Next() calls
	nextLocs        []Location         // reused across Next() calls
	nextSegmentLocs []segment.Location // reused across Next() calls

	docNum1Hit   uint64
	normBits1Hit uint64

	buf []byte

	includeFreqNorm bool
	includeLocs     bool

	bytesRead uint64
}

var emptyPostingsIterator = &PostingsIterator{}

func (i *PostingsIterator) Size() int {
	sizeInBytes := reflectStaticSizePostingsIterator + SizeOfPtr +
		i.next.Size()
	// account for freqNormReader, locReader if we start using this.
	for _, entry := range i.nextLocs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

// Implements the segment.DiskStatsReporter interface
// The purpose of this implementation is to get
// the bytes read from the disk which includes
// the freqNorm and location specific information
// of a hit
func (i *PostingsIterator) ResetBytesRead(val uint64) {
	i.bytesRead = val
}

func (i *PostingsIterator) BytesRead() uint64 {
	return i.bytesRead
}

func (i *PostingsIterator) incrementBytesRead(val uint64) {
	i.bytesRead += val
}

func (i *PostingsIterator) BytesWritten() uint64 {
	return 0
}

// chunkOf maps a doc number to the freq/norm chunk holding it.
//
// This is otherwise a hardware integer division on every document of every
// scan — chunkSize is maxDocs/numChunks, so it is not a power of two and the
// compiler cannot strength-reduce it. The bounds of the last chunk computed
// are cached, and since a scan walks doc numbers in order it crosses a chunk
// boundary only once every chunkSize documents.
//
// Callers must have already rejected a zero chunkSize.
func (i *PostingsIterator) chunkOf(n uint32) uint32 {
	nn := uint64(n)
	if nn >= i.chunkLo && nn < i.chunkHi {
		return i.chunkCached
	}
	cs := i.postings.chunkSize
	c := nn / cs
	i.chunkCached = uint32(c)
	i.chunkLo = c * cs
	i.chunkHi = i.chunkLo + cs
	return i.chunkCached
}

func (i *PostingsIterator) loadChunk(chunk int) error {
	if i.includeFreqNorm {
		err := i.freqNormReader.loadChunk(chunk)
		if err != nil {
			return err
		}

		// assign the bytes read at this point, since
		// the postingsIterator is tracking only the chunk loaded
		// and the cumulation is tracked correctly in the downstream
		// intDecoder
		i.ResetBytesRead(i.freqNormReader.getBytesRead())
	}

	if i.includeLocs {
		err := i.locReader.loadChunk(chunk)
		if err != nil {
			return err
		}
		i.ResetBytesRead(i.locReader.getBytesRead())
	}

	i.currChunk = uint32(chunk)
	return nil
}

func (i *PostingsIterator) readFreqNormHasLocs() (uint64, uint64, bool, error) {
	if i.normBits1Hit != 0 {
		return 1, i.normBits1Hit, false, nil
	}

	freqHasLocs, err := i.freqNormReader.readUvarint()
	if err != nil {
		return 0, 0, false, fmt.Errorf("error reading frequency: %v", err)
	}

	freq, hasLocs := decodeFreqHasLocs(freqHasLocs)
	if freq == 0 {
		return freq, 0, hasLocs, nil
	}

	normBits, err := i.freqNormReader.readUvarint()
	if err != nil {
		return 0, 0, false, fmt.Errorf("error reading norm: %v", err)
	}

	return freq, normBits, hasLocs, nil
}

func (i *PostingsIterator) skipFreqNormReadHasLocs() (bool, error) {
	if i.normBits1Hit != 0 {
		return false, nil
	}

	freqHasLocs, err := i.freqNormReader.readUvarint()
	if err != nil {
		return false, fmt.Errorf("error reading freqHasLocs: %v", err)
	}

	freq, hasLocs := decodeFreqHasLocs(freqHasLocs)
	if freq == 0 {
		return hasLocs, nil
	}

	i.freqNormReader.SkipUvarint() // Skip normBits.

	return hasLocs, nil // See decodeFreqHasLocs() / hasLocs.
}

func encodeFreqHasLocs(freq uint64, hasLocs bool) uint64 {
	rv := freq << 1
	if hasLocs {
		rv = rv | 0x01 // 0'th LSB encodes whether there are locations
	}
	return rv
}

func decodeFreqHasLocs(freqHasLocs uint64) (uint64, bool) {
	freq := freqHasLocs >> 1
	hasLocs := freqHasLocs&0x01 != 0
	return freq, hasLocs
}

// readLocation processes all the integers on the stream representing a single
// location.
func (i *PostingsIterator) readLocation(l *Location) error {
	// read off field
	fieldID, err := i.locReader.readUvarint()
	if err != nil {
		return fmt.Errorf("error reading location field: %v", err)
	}
	// read off pos
	pos, err := i.locReader.readUvarint()
	if err != nil {
		return fmt.Errorf("error reading location pos: %v", err)
	}
	// read off start
	start, err := i.locReader.readUvarint()
	if err != nil {
		return fmt.Errorf("error reading location start: %v", err)
	}
	// read off end
	end, err := i.locReader.readUvarint()
	if err != nil {
		return fmt.Errorf("error reading location end: %v", err)
	}
	// read off num array pos
	numArrayPos, err := i.locReader.readUvarint()
	if err != nil {
		return fmt.Errorf("error reading location num array pos: %v", err)
	}

	l.field = i.postings.sb.fieldsInv[fieldID]
	l.pos = pos
	l.start = start
	l.end = end

	if cap(l.ap) < int(numArrayPos) {
		l.ap = make([]uint64, int(numArrayPos))
	} else {
		l.ap = l.ap[:int(numArrayPos)]
	}

	// read off array positions
	for k := 0; k < int(numArrayPos); k++ {
		ap, err := i.locReader.readUvarint()
		if err != nil {
			return fmt.Errorf("error reading array position: %v", err)
		}

		l.ap[k] = ap
	}

	return nil
}

// FillTermFieldDoc advances the iterator to the first document at or after
// atOrAfter and writes that posting directly into rv, reporting false once the
// postings list is exhausted. Pass 0 for a plain forward step.
//
// This is a fast path for the very common case where term vectors are not
// required. The generic Next() route boxes a *Posting into an interface and
// then costs four virtual calls (Number/Frequency/Norm/Locations) plus a struct
// clear for every document; profiling a term scan put that glue at roughly a
// quarter of the whole inner loop. Locations are deliberately not decoded here,
// so callers that need them must stay on the generic path.
func (i *PostingsIterator) FillTermFieldDoc(rv *index.TermFieldDoc,
	globalOffset, atOrAfter uint64, includeFreq, includeNorm bool) (bool, error) {
	docNum, exists, err := i.nextDocNumAtOrAfter(atOrAfter)
	if err != nil || !exists {
		return false, err
	}

	rv.ID = index.NewIndexInternalID(rv.ID, docNum+globalOffset)

	if !i.includeFreqNorm {
		return true, nil
	}

	freq, normBits, _, err := i.readFreqNormHasLocs()
	if err != nil {
		return false, err
	}
	if includeFreq {
		rv.Freq = freq
	}
	if includeNorm {
		rv.Norm = normFromFieldLen(uint32(normBits))
	}

	return true, nil
}

// NextBlock fills the caller's arrays with up to len(docNums) postings and
// returns how many were written; a return of 0 means the list is exhausted.
//
// This is the bulk counterpart to FillTermFieldDoc. Handing back flat arrays
// lets the caller score a whole block without materialising a per-document
// object, and amortises the call across the block instead of paying it per
// document. Locations are not decoded, so callers needing term vectors must
// stay on the generic path.
func (i *PostingsIterator) NextBlock(docNums []uint64, freqs []uint64, norms []float64,
	globalOffset uint64) (int, error) {
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
			freq, normBits, _, err := i.readFreqNormHasLocs()
			if err != nil {
				return n, err
			}
			freqs[n] = freq
			norms[n] = normFromFieldLen(uint32(normBits))
		} else {
			freqs[n] = 1
			norms[n] = 1
		}
		n++
	}
	return n, nil
}

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) Next() (segment.Posting, error) {
	return i.nextAtOrAfter(0)
}

// Advance returns the posting at the specified docNum or it is not present
// the next posting, or if the end is reached, nil
func (i *PostingsIterator) Advance(docNum uint64) (segment.Posting, error) {
	return i.nextAtOrAfter(docNum)
}

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) nextAtOrAfter(atOrAfter uint64) (segment.Posting, error) {
	docNum, exists, err := i.nextDocNumAtOrAfter(atOrAfter)
	if err != nil || !exists {
		return nil, err
	}

	i.next = Posting{} // clear the struct
	rv := &i.next
	rv.docNum = docNum

	if !i.includeFreqNorm {
		return rv, nil
	}

	var normBits uint64
	var hasLocs bool

	rv.freq, normBits, hasLocs, err = i.readFreqNormHasLocs()
	if err != nil {
		return nil, err
	}

	rv.norm = math.Float32frombits(uint32(normBits))

	if i.includeLocs && hasLocs {
		// prepare locations into reused slices, where we assume
		// rv.freq >= "number of locs", since in a composite field,
		// some component fields might have their IncludeTermVector
		// flags disabled while other component fields are enabled
		if rv.freq > 0 {
			if cap(i.nextLocs) >= int(rv.freq) {
				i.nextLocs = i.nextLocs[0:rv.freq]
			} else {
				i.nextLocs = make([]Location, rv.freq, rv.freq*2)
			}
			if cap(i.nextSegmentLocs) < int(rv.freq) {
				i.nextSegmentLocs = make([]segment.Location, rv.freq, rv.freq*2)
			}
			rv.locs = i.nextSegmentLocs[:0]
		}

		numLocsBytes, err := i.locReader.readUvarint()
		if err != nil {
			return nil, fmt.Errorf("error reading location numLocsBytes: %v", err)
		}

		j := 0
		var nextLoc *Location
		startBytesRemaining := i.locReader.Len() // # bytes remaining in the locReader
		for startBytesRemaining-i.locReader.Len() < int(numLocsBytes) {
			if len(i.nextLocs) > j {
				nextLoc = &i.nextLocs[j]
			} else {
				nextLoc = &Location{}
			}

			err := i.readLocation(nextLoc)
			if err != nil {
				return nil, err
			}

			rv.locs = append(rv.locs, nextLoc)
			j++
		}
	}

	return rv, nil
}

// nextDocNum returns the next docNum on the postings list, and also
// sets up the currChunk / loc related fields of the iterator.
func (i *PostingsIterator) nextDocNumAtOrAfter(atOrAfter uint64) (uint64, bool, error) {
	if i.normBits1Hit != 0 {
		if i.docNum1Hit == DocNum1HitFinished {
			return 0, false, nil
		}
		if i.docNum1Hit < atOrAfter {
			// advanced past our 1-hit
			i.docNum1Hit = DocNum1HitFinished // consume our 1-hit docNum
			return 0, false, nil
		}
		docNum := i.docNum1Hit
		i.docNum1Hit = DocNum1HitFinished // consume our 1-hit docNum
		return docNum, true, nil
	}

	if i.Actual == nil || !i.Actual.HasNext() {
		return 0, false, nil
	}

	if i.postings == nil || i.postings == emptyPostingsList {
		// couldn't find anything
		return 0, false, nil
	}

	if i.postings.postings == i.ActualBM {
		return i.nextDocNumAtOrAfterClean(atOrAfter)
	}

	if i.postings.chunkSize == 0 {
		return 0, false, ErrChunkSizeZero
	}

	i.Actual.AdvanceIfNeeded(uint32(atOrAfter))

	if !i.Actual.HasNext() || !i.all.HasNext() {
		// couldn't find anything
		return 0, false, nil
	}

	n := i.Actual.Next()
	allN := i.all.Next()
	nChunk := i.chunkOf(n)

	// when allN becomes >= to here, then allN is in the same chunk as nChunk.
	allNReachesNChunk := nChunk * uint32(i.postings.chunkSize)

	// n is the next actual hit (excluding some postings), and
	// allN is the next hit in the full postings, and
	// if they don't match, move 'all' forwards until they do
	for allN != n {
		// we've reached same chunk, so move the freq/norm/loc decoders forward
		if i.includeFreqNorm && allN >= allNReachesNChunk {
			err := i.currChunkNext(nChunk)
			if err != nil {
				return 0, false, err
			}
		}

		if !i.all.HasNext() {
			return 0, false, nil
		}

		allN = i.all.Next()
	}

	if i.includeFreqNorm && (i.currChunk != nChunk || i.freqNormReader.isNil()) {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return 0, false, fmt.Errorf("error loading chunk: %v", err)
		}
	}

	return uint64(n), true, nil
}

var freqHasLocs1Hit = encodeFreqHasLocs(1, false)

// nextBytes returns the docNum and the encoded freq & loc bytes for
// the next posting
func (i *PostingsIterator) nextBytes() (
	docNumOut uint64, freq uint64, normBits uint64,
	bytesFreqNorm []byte, bytesLoc []byte, err error) {
	docNum, exists, err := i.nextDocNumAtOrAfter(0)
	if err != nil || !exists {
		return 0, 0, 0, nil, nil, err
	}

	if i.normBits1Hit != 0 {
		if i.buf == nil {
			i.buf = make([]byte, binary.MaxVarintLen64*2)
		}
		n := binary.PutUvarint(i.buf, freqHasLocs1Hit)
		n += binary.PutUvarint(i.buf[n:], i.normBits1Hit)
		return docNum, uint64(1), i.normBits1Hit, i.buf[:n], nil, nil
	}

	startFreqNorm := i.freqNormReader.remainingLen()

	var hasLocs bool

	freq, normBits, hasLocs, err = i.readFreqNormHasLocs()
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}

	endFreqNorm := i.freqNormReader.remainingLen()
	bytesFreqNorm = i.freqNormReader.readBytes(startFreqNorm, endFreqNorm)

	if hasLocs {
		startLoc := i.locReader.remainingLen()

		numLocsBytes, err := i.locReader.readUvarint()
		if err != nil {
			return 0, 0, 0, nil, nil,
				fmt.Errorf("error reading location nextBytes numLocs: %v", err)
		}

		// skip over all the location bytes
		i.locReader.SkipBytes(int(numLocsBytes))

		endLoc := i.locReader.remainingLen()
		bytesLoc = i.locReader.readBytes(startLoc, endLoc)
	}

	return docNum, freq, normBits, bytesFreqNorm, bytesLoc, nil
}

// optimization when the postings list is "clean" (e.g., no updates &
// no deletions) where the all bitmap is the same as the actual bitmap
func (i *PostingsIterator) nextDocNumAtOrAfterClean(
	atOrAfter uint64) (uint64, bool, error) {
	if !i.includeFreqNorm {
		i.Actual.AdvanceIfNeeded(uint32(atOrAfter))

		if !i.Actual.HasNext() {
			return 0, false, nil // couldn't find anything
		}

		return uint64(i.Actual.Next()), true, nil
	}

	if i.postings != nil && i.postings.chunkSize == 0 {
		return 0, false, ErrChunkSizeZero
	}

	// freq-norm's needed, so maintain freq-norm chunk reader
	sameChunkNexts := 0 // # of times we called Next() in the same chunk
	n := i.Actual.Next()
	nChunk := i.chunkOf(n)

	for uint64(n) < atOrAfter && i.Actual.HasNext() {
		n = i.Actual.Next()

		nChunkPrev := nChunk
		nChunk = i.chunkOf(n)

		if nChunk != nChunkPrev {
			sameChunkNexts = 0
		} else {
			sameChunkNexts += 1
		}
	}

	if uint64(n) < atOrAfter {
		// couldn't find anything
		return 0, false, nil
	}

	for j := 0; j < sameChunkNexts; j++ {
		err := i.currChunkNext(nChunk)
		if err != nil {
			return 0, false, fmt.Errorf("error optimized currChunkNext: %v", err)
		}
	}

	if i.currChunk != nChunk || i.freqNormReader.isNil() {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return 0, false, fmt.Errorf("error loading chunk: %v", err)
		}
	}

	return uint64(n), true, nil
}

func (i *PostingsIterator) currChunkNext(nChunk uint32) error {
	if i.currChunk != nChunk || i.freqNormReader.isNil() {
		err := i.loadChunk(int(nChunk))
		if err != nil {
			return fmt.Errorf("error loading chunk: %v", err)
		}
	}

	// read off freq/offsets even though we don't care about them
	hasLocs, err := i.skipFreqNormReadHasLocs()
	if err != nil {
		return err
	}

	if i.includeLocs && hasLocs {
		numLocsBytes, err := i.locReader.readUvarint()
		if err != nil {
			return fmt.Errorf("error reading location numLocsBytes: %v", err)
		}

		// skip over all the location bytes
		i.locReader.SkipBytes(int(numLocsBytes))
	}

	return nil
}

// DocNum1Hit returns the docNum and true if this is "1-hit" optimized
// and the docNum is available.
func (p *PostingsIterator) DocNum1Hit() (uint64, bool) {
	if p.normBits1Hit != 0 && p.docNum1Hit != DocNum1HitFinished {
		return p.docNum1Hit, true
	}
	return 0, false
}

// ActualBitmap returns the underlying actual bitmap
// which can be used up the stack for optimizations
func (p *PostingsIterator) ActualBitmap() *roaring.Bitmap {
	return p.ActualBM
}

// ReplaceActual replaces the ActualBM with the provided
// bitmap
func (p *PostingsIterator) ReplaceActual(abm *roaring.Bitmap) {
	p.ActualBM = abm
	p.Actual = abm.Iterator()
}

// PostingsIteratorFromBitmap constructs a PostingsIterator given an
// "actual" bitmap.
func PostingsIteratorFromBitmap(bm *roaring.Bitmap,
	includeFreqNorm, includeLocs bool) (segment.PostingsIterator, error) {
	return &PostingsIterator{
		ActualBM:        bm,
		Actual:          bm.Iterator(),
		includeFreqNorm: includeFreqNorm,
		includeLocs:     includeLocs,
	}, nil
}

// PostingsIteratorFrom1Hit constructs a PostingsIterator given a
// 1-hit docNum.
func PostingsIteratorFrom1Hit(docNum1Hit uint64,
	includeFreqNorm, includeLocs bool) (segment.PostingsIterator, error) {
	return &PostingsIterator{
		docNum1Hit:      docNum1Hit,
		normBits1Hit:    NormBits1Hit,
		includeFreqNorm: includeFreqNorm,
		includeLocs:     includeLocs,
	}, nil
}

// Posting is a single entry in a postings list
type Posting struct {
	docNum uint64
	freq   uint64
	norm   float32
	locs   []segment.Location
}

func (p *Posting) Size() int {
	sizeInBytes := reflectStaticSizePosting

	for _, entry := range p.locs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

// Number returns the document number of this posting in this segment
func (p *Posting) Number() uint64 {
	return p.docNum
}

// Frequency returns the frequencies of occurrence of this term in this doc/field
func (p *Posting) Frequency() uint64 {
	return p.freq
}

// maxNormCache bounds normCache; field lengths beyond it fall through to the
// direct computation.
const maxNormCache = 4096

// normCache maps a field length to 1/sqrt(fieldLength). The value stored in a
// posting's norm is the field length reinterpreted as float32 bits (see the
// writer's use of math.Float32frombits on the field length), so the norm is a
// pure function of a small integer. Norm() is called once per posting on every
// scored query, and this keeps a sqrt and a division out of that inner loop.
var normCache [maxNormCache]float32

func init() {
	for i := 0; i < maxNormCache; i++ {
		normCache[i] = float32(1.0 / math.Sqrt(float64(i)))
	}
}

// Norm returns the normalization factor for this posting
func (p *Posting) Norm() float64 {
	return normFromFieldLen(math.Float32bits(p.norm))
}

// normFromFieldLen maps a stored field length to its normalization factor.
func normFromFieldLen(fieldLen uint32) float64 {
	if fieldLen < maxNormCache {
		return float64(normCache[fieldLen])
	}
	return float64(float32(1.0 / math.Sqrt(float64(fieldLen))))
}

// Locations returns the location information for each occurrence
func (p *Posting) Locations() []segment.Location {
	return p.locs
}

// NormUint64 returns the norm value as uint64
func (p *Posting) NormUint64() uint64 {
	return uint64(math.Float32bits(p.norm))
}

// Location represents the location of a single occurrence
type Location struct {
	field string
	pos   uint64
	start uint64
	end   uint64
	ap    []uint64
}

func (l *Location) Size() int {
	return reflectStaticSizeLocation +
		len(l.field) +
		len(l.ap)*SizeOfUint64
}

// Field returns the name of the field (useful in composite fields to know
// which original field the value came from)
func (l *Location) Field() string {
	return l.field
}

// Start returns the start byte offset of this occurrence
func (l *Location) Start() uint64 {
	return l.start
}

// End returns the end byte offset of this occurrence
func (l *Location) End() uint64 {
	return l.end
}

// Pos returns the 1-based phrase position of this occurrence
func (l *Location) Pos() uint64 {
	return l.pos
}

// ArrayPositions returns the array position vector associated with this occurrence
func (l *Location) ArrayPositions() []uint64 {
	return l.ap
}
