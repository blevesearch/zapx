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
	"fmt"
	"math"
	"reflect"

	"github.com/RoaringBitmap/roaring/v2"
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
//	encoding  : MSB
//	name      : 63  62  61...to...bit #0 (LSB)
//	----------+---+---+---------------------------------------------------
//	 general  : 0 | 0 | 62-bits offset of the term footer.
//	 ~        : 0 | 1 | reserved for future.
//	 1-hit    : 1 | 0 | 31 unused bits | 31-bits docNum.
//	 ~        : 1 | 1 | reserved for future.
//
// Encoding "general" is able to handle all cases, where the offset points at
// the term footer described in postings_format.go.
//
// Encoding "1-hit" optimizes the very common case of a term with a single hit
// -- a term in the _id field, or the long tail of any real dictionary. It
// applies when:
//
//   - the term appears in only a single doc for that field;
//   - and, the term's freq is exactly 1 in that doc;
//   - and, term vectors are not recorded for the term;
//   - and, the docNum fits into 31-bits.
//
// Earlier zap versions also had to pack the norm into this value. That is no
// longer necessary: the norm lives in the field's norm column, keyed by doc
// number, so a 1-hit term needs nothing but the doc number itself.
const FSTValEncodingMask = uint64(0xc000000000000000)
const FSTValEncodingGeneral = uint64(0x0000000000000000)
const FSTValEncoding1Hit = uint64(0x8000000000000000)

func FSTValEncode1Hit(docNum uint64) uint64 {
	return FSTValEncoding1Hit | (mask31Bits & docNum)
}

func FSTValDecode1Hit(v uint64) (docNum uint64) {
	return mask31Bits & v
}

const mask31Bits = uint64(0x000000007fffffff)

func under32Bits(x uint64) bool {
	return x <= mask31Bits
}

const DocNum1HitFinished = math.MaxUint64

// PostingsList is an in-memory representation of a postings list
type PostingsList struct {
	sb    *SegmentBase
	norms *normsColumn

	footer       termFooter
	payloadStart uint64
	locsStart    uint64
	skipStart    uint64

	except *roaring.Bitmap

	// when is1Hit, the whole list is the single docNum1Hit and none of the
	// region offsets above are meaningful
	is1Hit     bool
	docNum1Hit uint64

	// docBM is the postings list as a roaring bitmap, created on demand and
	// cached.
	// It's needed for resolving external _id strings to internal doc
	// numbers, not on the scan path, so will not impact search latency
	// TODO: will it blow up memory footprint?
	docBM *roaring.Bitmap

	// chunkSize governs the location blob, which keeps the chunked encoding of
	// earlier zap versions
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
	if p.docBM != nil {
		sizeInBytes += int(p.docBM.GetSizeInBytes())
	}

	return sizeInBytes
}

func (p *PostingsList) OrInto(receiver *roaring.Bitmap) {
	if p.is1Hit {
		receiver.Add(uint32(p.docNum1Hit))
		return
	}
	bm, err := p.docBitmap()
	if err != nil || bm == nil {
		return
	}
	receiver.Or(bm)
}

// docBitmap materialises the postings list as a roaring bitmap, minus any
// deletions, and caches it. See the note on PostingsList.docBM.
func (p *PostingsList) docBitmap() (*roaring.Bitmap, error) {
	if p.docBM != nil {
		return p.docBM, nil
	}
	if p.is1Hit {
		bm := roaring.New()
		if p.docNum1Hit != DocNum1HitFinished &&
			(p.except == nil || !p.except.Contains(uint32(p.docNum1Hit))) {
			bm.Add(uint32(p.docNum1Hit))
		}
		p.docBM = bm
		return bm, nil
	}
	if p.sb == nil || p.footer.docFreq == 0 {
		p.docBM = roaring.New()
		return p.docBM, nil
	}

	var c blockCursor
	if err := c.init(p.sb, &p.footer, p.payloadStart, p.skipStart, false); err != nil {
		return nil, err
	}
	docNums := make([]uint32, 0, p.footer.docFreq)
	for !c.exhausted {
		if err := c.loadBlock(); err != nil {
			return nil, err
		}
		docNums = append(docNums, c.buf.docs[:c.nDocs]...)
		c.nextBlock()
	}
	bm := roaring.New()
	bm.AddMany(docNums)
	if p.except != nil && !p.except.IsEmpty() {
		bm.AndNot(p.except)
	}
	p.docBM = bm
	return bm, nil
}

// Iterator returns an iterator for this postings list
func (p *PostingsList) Iterator(includeFreq, includeNorm, includeLocs bool,
	prealloc segment.PostingsIterator) segment.PostingsIterator {
	if !p.is1Hit && p.sb == nil {
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

	rv, err := p.iterator(includeFreq, includeNorm, includeLocs, preallocPI)
	if err != nil {
		// The segment.PostingsIterator contract has no way to report a failure
		// here; surface it on the first Next()/Advance() instead.
		rv.err = err
	}
	return rv
}

func (p *PostingsList) iterator(includeFreq, includeNorm, includeLocs bool,
	rv *PostingsIterator) (*PostingsIterator, error) {
	if rv == nil {
		rv = &PostingsIterator{}
	} else {
		locReader := rv.locReader
		if locReader != nil {
			locReader.reset()
		}
		nextLocs := rv.nextLocs[:0]
		nextSegmentLocs := rv.nextSegmentLocs[:0]
		buf := rv.buf
		blockBuf := rv.cursor.buf

		*rv = PostingsIterator{} // clear the struct

		rv.locReader = locReader
		rv.nextLocs = nextLocs
		rv.nextSegmentLocs = nextSegmentLocs
		rv.buf = buf
		rv.cursor.buf = blockBuf
	}

	rv.postings = p
	rv.includeFreqNorm = includeFreq || includeNorm || includeLocs
	rv.includeLocs = includeLocs
	rv.docNum1Hit = DocNum1HitFinished

	if p.except != nil && !p.except.IsEmpty() {
		// Deletions are walked in lockstep with the postings rather than
		// pre-ANDed into a new bitmap: both sequences are ascending, so a merge
		// scan costs nothing per document and, unlike roaring.AndNot, allocates
		// nothing per term.
		rv.exceptItr = p.except.Iterator()
	}

	if p.is1Hit {
		rv.is1Hit = true
		rv.cursor.exhausted = true
		rv.docNum1Hit = p.docNum1Hit
		if rv.exceptItr != nil && p.except.Contains(uint32(rv.docNum1Hit)) {
			rv.docNum1Hit = DocNum1HitFinished
		}
		return rv, nil
	}

	if p.footer.docFreq == 0 {
		rv.cursor.exhausted = true
		return rv, nil
	}

	rv.normIsDense = p.norms.kind == normsKindDense
	rv.normDense = p.norms.dense
	rv.normConst = normFactorFromID(p.norms.constant)
	if p.norms.kind == normsKindAbsent {
		rv.normConst = normFactorFromID(0)
	}
	rv.fastScan = rv.exceptItr == nil && !includeLocs

	err := rv.cursor.init(p.sb, &p.footer, p.payloadStart, p.skipStart, rv.includeFreqNorm)
	if err != nil {
		return rv, err
	}

	if rv.includeLocs && p.footer.hasLocs() {
		rv.locReader = newChunkedIntDecoder(p.sb.mem, p.locsStart, rv.locReader, p.sb.fileReader)
		rv.hasLocs = true
		rv.locChunk = -1
	}

	return rv, nil
}

// rawDocFreq is the number of postings recorded on disk, before any deletions
// are taken into account.
func (p *PostingsList) rawDocFreq() uint64 {
	if p.is1Hit {
		return 1
	}
	return uint64(p.footer.docFreq)
}

// Count returns the number of items on this postings list
func (p *PostingsList) Count() uint64 {
	if p.is1Hit {
		if p.docNum1Hit == DocNum1HitFinished {
			return 0
		}
		if p.except != nil && p.except.Contains(uint32(p.docNum1Hit)) {
			return 0
		}
		return 1
	}
	// Without deletions the answer is on the term footer, so the common case
	// costs nothing. With deletions it needs the doc set, which is materialised
	// once and cached.
	if p.except == nil || p.except.IsEmpty() {
		return uint64(p.footer.docFreq)
	}
	bm, err := p.docBitmap()
	if err != nil {
		return 0
	}
	return bm.GetCardinality()
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
	rv.sb = d.sb
	rv.norms = d.norms
	if rv.norms == nil {
		rv.norms = normsAbsent
	}

	// handle "1-hit" encoding special case
	if postingsOffset&FSTValEncodingMask == FSTValEncoding1Hit {
		rv.is1Hit = true
		rv.docNum1Hit = FSTValDecode1Hit(postingsOffset)
		return nil
	}

	payloadStart, locsStart, skipStart, err := rv.footer.decode(d.sb.mem, postingsOffset)
	if err != nil {
		return err
	}
	rv.payloadStart = payloadStart
	rv.locsStart = locsStart
	rv.skipStart = skipStart
	// Only the footer has actually been read. The payload is charged block by
	// block as the cursor decodes it, which is the point of the format: a query
	// that skips most of a long postings list should not be billed for it.
	rv.incrementBytesRead(uint64(maxTermFooterLen))

	// The location blob keeps the chunked encoding of earlier zap versions, but
	// its chunk size is read from the footer instead of being re-derived from a
	// cardinality the reader has to recompute. Reader and writer cannot
	// disagree about a number that was written down.
	rv.chunkSize = rv.footer.locChunkSize

	return nil
}

// PostingsIterator provides a way to iterate through the postings list
type PostingsIterator struct {
	postings *PostingsList

	cursor     blockCursor
	cur        int // index of the current posting within the decoded block
	positioned bool

	// nextAllowed is the lowest doc number the next call may return: one past
	// whatever was returned last.
	nextAllowed uint32

	// exceptItr walks the deleted-doc bitmap in lockstep with the postings.
	exceptItr roaring.IntPeekable

	locReader *chunkedIntDecoder
	locChunk  int
	hasLocs   bool

	// is1Hit marks a list whose single posting is encoded inline in the FST
	// value. There is no block cursor behind it, so iteration must stop as soon
	// as that one doc number has been handed out.
	is1Hit bool

	// fastScan records that this iterator is a plain block-backed scan: no
	// inline encoding, no replacement bitmap, no deletions, no locations. That
	// combination is the overwhelming majority of term scans, and it lets a
	// forward step inside an already-decoded block skip every check the general
	// path has to make.
	fastScan bool

	// The field's norm column, flattened onto the iterator so the hot loop does
	// not re-decide its shape per document.
	normDense   []uint8
	normConst   float64
	normIsDense bool

	next            Posting            // reused across Next() calls
	nextLocs        []Location         // reused across Next() calls
	nextSegmentLocs []segment.Location // reused across Next() calls

	docNum1Hit uint64

	buf []byte

	includeFreqNorm bool
	includeLocs     bool

	err error

	bytesRead uint64
}

var emptyPostingsIterator = &PostingsIterator{docNum1Hit: DocNum1HitFinished}

func (i *PostingsIterator) Size() int {
	sizeInBytes := reflectStaticSizePostingsIterator + SizeOfPtr +
		i.next.Size()
	for _, entry := range i.nextLocs {
		sizeInBytes += entry.Size()
	}

	return sizeInBytes
}

// Implements the segment.DiskStatsReporter interface.
//
// The count is assembled from the three readers that actually touch the
// mapping -- the block cursor, the location decoder, and whatever the iterator
// itself charged at setup -- rather than being overwritten by whichever one
// reported last. Callers take deltas of this across a query, so it has to be
// cumulative and monotonic between resets.
func (i *PostingsIterator) ResetBytesRead(val uint64) {
	i.bytesRead = val
	i.cursor.bytesRead = 0
	if i.locReader != nil {
		i.locReader.bytesRead = 0
	}
}

func (i *PostingsIterator) BytesRead() uint64 {
	rv := i.bytesRead + i.cursor.bytesRead
	if i.locReader != nil {
		rv += i.locReader.getBytesRead()
	}
	return rv
}

func (i *PostingsIterator) BytesWritten() uint64 {
	return 0
}

// isDeleted reports whether a doc number has been deleted. The bitmap and the
// postings are both ascending, so advancing the peekable cursor is amortised
// constant work.
func (i *PostingsIterator) isDeleted(docNum uint32) bool {
	if i.exceptItr == nil {
		return false
	}
	i.exceptItr.AdvanceIfNeeded(docNum)
	return i.exceptItr.HasNext() && i.exceptItr.PeekNext() == docNum
}

// positionAt moves the cursor onto the first posting at or after lo, reporting
// false once the list runs out.
func (i *PostingsIterator) positionAt(lo uint32) (bool, error) {
	c := &i.cursor

	if i.positioned {
		// Already there. Happens when a caller advances to a doc it has just
		// been handed.
		if i.cur < c.nDocs && c.buf.docs[i.cur] >= lo {
			return true, nil
		}
		// The overwhelmingly common case: the answer is the very next slot.
		i.cur++
		if i.cur < c.nDocs && c.buf.docs[i.cur] >= lo {
			return true, nil
		}
		// Still inside this block, but further along.
		if i.cur < c.nDocs && c.blockLastDoc >= lo {
			i.cur = searchBlock(&c.buf.docs, lo)
			return i.cur < c.nDocs, nil
		}
	}

	// Walk the skip list to the block that could hold lo, then decode just
	// that one block.
	c.seekBlock(lo)
	if c.exhausted {
		i.positioned = false
		return false, nil
	}
	if err := c.loadBlock(); err != nil {
		return false, err
	}
	i.positioned = true
	i.cur = searchBlock(&c.buf.docs, lo)
	return i.cur < c.nDocs, nil
}

// advanceOne steps to the next posting without skipping, which is what the
// location path needs: the location blob is a per-document stream, so every
// document has to be walked past even when it is not going to be returned.
func (i *PostingsIterator) advanceOne() (bool, error) {
	c := &i.cursor
	if i.positioned {
		i.cur++
		if i.cur < c.nDocs {
			return true, nil
		}
		c.nextBlock()
	}
	if c.exhausted {
		i.positioned = false
		return false, nil
	}
	if err := c.loadBlock(); err != nil {
		return false, err
	}
	i.positioned = true
	i.cur = 0
	return i.cur < c.nDocs, nil
}

// stepFast advances one posting inside the block that is already decoded. It
// reports false whenever anything at all is unusual -- the block is spent, the
// iterator is not a plain scan, nothing is decoded yet -- and the caller falls
// back to the general path.
func (i *PostingsIterator) stepFast() (uint32, bool) {
	if !i.fastScan || !i.positioned {
		return 0, false
	}
	k := i.cur + 1
	if k >= i.cursor.nDocs {
		return 0, false
	}
	i.cur = k
	d := i.cursor.buf.docs[k]
	i.nextAllowed = d + 1
	return d, true
}

// nextDocNumAtOrAfter returns the next live doc number at or after atOrAfter.
func (i *PostingsIterator) nextDocNumAtOrAfter(atOrAfter uint64) (uint64, bool, error) {
	if i.err != nil {
		return 0, false, i.err
	}

	if i.is1Hit {
		if i.docNum1Hit == DocNum1HitFinished {
			return 0, false, nil
		}
		docNum := i.docNum1Hit
		i.docNum1Hit = DocNum1HitFinished // consume our 1-hit docNum
		if docNum < atOrAfter {
			return 0, false, nil
		}
		return docNum, true, nil
	}

	if i.docNum1Hit != DocNum1HitFinished {
		docNum := i.docNum1Hit
		i.docNum1Hit = DocNum1HitFinished // consume our 1-hit docNum
		if docNum < atOrAfter {
			return 0, false, nil
		}
		return docNum, true, nil
	}

	if i.postings == nil {
		return 0, false, nil
	}

	lo := i.nextAllowed
	if atOrAfter > uint64(docNumTerminated) {
		return 0, false, nil
	}
	if uint32(atOrAfter) > lo {
		lo = uint32(atOrAfter)
	}

	for {
		var ok bool
		var err error
		if i.includeLocs {
			ok, err = i.stepWithLocs(lo)
		} else {
			ok, err = i.positionAt(lo)
		}
		if err != nil || !ok {
			return 0, false, err
		}
		docNum := i.cursor.buf.docs[i.cur]
		if docNum == docNumTerminated {
			return 0, false, nil
		}
		i.nextAllowed = docNum + 1
		if !i.isDeleted(docNum) {
			return uint64(docNum), true, nil
		}
		// A deleted document still occupies a slot in the location stream, so
		// its group has to be discarded here. stepWithLocs only skips the
		// documents it passes over on the way to lo; this one sits exactly at
		// lo and was handed back before the deletion check rejected it.
		if err := i.skipLocations(docNum); err != nil {
			return 0, false, err
		}
		if docNum == math.MaxUint32 {
			return 0, false, nil
		}
		lo = docNum + 1
	}
}

// stepWithLocs advances to lo one document at a time, discarding the location
// group of every document passed over so the location stream stays aligned.
func (i *PostingsIterator) stepWithLocs(lo uint32) (bool, error) {
	for {
		ok, err := i.advanceOne()
		if err != nil || !ok {
			return false, err
		}
		docNum := i.cursor.buf.docs[i.cur]
		if docNum >= lo {
			return true, nil
		}
		if err := i.skipLocations(docNum); err != nil {
			return false, err
		}
	}
}

// freqAt returns the frequency of the posting the cursor is sitting on.
func (i *PostingsIterator) currFreq(docNum uint32) uint64 {
	if !i.includeFreqNorm || !i.positioned {
		return 1
	}
	if i.cur < i.cursor.nDocs && i.cursor.buf.docs[i.cur] == docNum {
		return uint64(i.cursor.buf.freqs[i.cur])
	}
	return 1
}

// normIDOf reads a document's quantized norm ID directly, without the
// 256-entry table lookup, for the Posting struct, which stores the id and
// converts only if Norm() is called.
func (i *PostingsIterator) normIDOf(docNum uint32) uint8 {
	if i.normIsDense {
		if int(docNum) < len(i.normDense) {
			return i.normDense[docNum]
		}
		return 0
	}
	return i.postings.norms.constant
}

func (i *PostingsIterator) currNormID(docNum uint32) uint8 {
	if i.postings == nil || i.postings.norms == nil {
		return 0
	}
	return i.postings.norms.id(docNum)
}

// ------------------------------------------------------------------ locations

func (i *PostingsIterator) loadLocChunk(docNum uint32) error {
	if i.locReader == nil {
		return nil
	}
	if i.postings.chunkSize == 0 {
		return ErrChunkSizeZero
	}
	chunk := int(uint64(docNum) / i.postings.chunkSize)
	if chunk == i.locChunk {
		return nil
	}
	if err := i.locReader.loadChunk(chunk); err != nil {
		return fmt.Errorf("error loading location chunk: %v", err)
	}
	i.locChunk = chunk
	return nil
}

// skipLocations discards one document's location group. A term that records
// locations writes a group for every posting -- possibly an empty one -- so
// this is a fixed cost per document walked past and needs no per-document flag.
func (i *PostingsIterator) skipLocations(docNum uint32) error {
	if !i.hasLocs {
		return nil
	}
	if err := i.loadLocChunk(docNum); err != nil {
		return err
	}
	numLocsBytes, err := i.locReader.readUvarint()
	if err != nil {
		return fmt.Errorf("error reading location numLocsBytes: %v", err)
	}
	i.locReader.SkipBytes(int(numLocsBytes))
	return nil
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

	if int(fieldID) >= len(i.postings.sb.fieldsInv) {
		return fmt.Errorf("corrupt location: field id %d out of range", fieldID)
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

// readLocations fills rv with the location group of the doc the cursor is on.
func (i *PostingsIterator) readLocations(rv *Posting, docNum uint32) error {
	if err := i.loadLocChunk(docNum); err != nil {
		return err
	}

	// The location count is bounded by the frequency, but only loosely: in a
	// composite field some component fields may have term vectors switched off
	// while others have them on, so the group can be shorter than the freq.
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
		return fmt.Errorf("error reading location numLocsBytes: %v", err)
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

		if err := i.readLocation(nextLoc); err != nil {
			return err
		}

		rv.locs = append(rv.locs, nextLoc)
		j++
	}
	return nil
}

// --------------------------------------------------------------- the iterator

// Next returns the next posting on the postings list, or nil at the end
func (i *PostingsIterator) Next() (segment.Posting, error) {
	return i.nextAtOrAfter(0)
}

// Advance returns the posting at the specified docNum or it is not present
// the next posting, or if the end is reached, nil
func (i *PostingsIterator) Advance(docNum uint64) (segment.Posting, error) {
	return i.nextAtOrAfter(docNum)
}

func (i *PostingsIterator) nextAtOrAfter(atOrAfter uint64) (segment.Posting, error) {
	if atOrAfter == 0 && i.includeFreqNorm {
		if d, ok := i.stepFast(); ok {
			rv := &i.next
			rv.docNum = uint64(d)
			rv.freq = uint64(i.cursor.buf.freqs[i.cur])
			rv.normID = i.normIDOf(d)
			rv.locs = nil
			return rv, nil
		}
	}

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

	rv.freq = i.currFreq(uint32(docNum))
	rv.normID = i.currNormID(uint32(docNum))

	if i.hasLocs {
		if err := i.readLocations(rv, uint32(docNum)); err != nil {
			return nil, err
		}
	}

	return rv, nil
}

// nextWithLocBytes returns the next posting along with the raw encoded bytes of
// its location group, so a merge can move locations across without decoding and
// re-encoding them.
//
// Unlike earlier zap versions there are no frequency or norm bytes to copy:
// frequencies are bitpacked and have to be re-packed against the new doc
// numbering, and norms are not in the postings at all.
func (i *PostingsIterator) nextWithLocBytes() (
	docNumOut uint64, freq uint64, bytesLoc []byte, exists bool, err error) {
	docNum, exists, err := i.nextDocNumAtOrAfter(0)
	if err != nil || !exists {
		return 0, 0, nil, false, err
	}
	freq = i.currFreq(uint32(docNum))

	if i.hasLocs {
		if err := i.loadLocChunk(uint32(docNum)); err != nil {
			return 0, 0, nil, false, err
		}
		startLoc := i.locReader.remainingLen()
		numLocsBytes, err := i.locReader.readUvarint()
		if err != nil {
			return 0, 0, nil, false,
				fmt.Errorf("error reading location numLocsBytes: %v", err)
		}
		i.locReader.SkipBytes(int(numLocsBytes))
		endLoc := i.locReader.remainingLen()
		bytesLoc = i.locReader.readBytes(startLoc, endLoc)
	}

	return docNum, freq, bytesLoc, true, nil
}

// PostingsIteratorFrom1Hit constructs a PostingsIterator given a
// 1-hit docNum.
func PostingsIteratorFrom1Hit(docNum1Hit uint64,
	includeFreqNorm, includeLocs bool) (segment.PostingsIterator, error) {
	return &PostingsIterator{
		is1Hit:          true,
		docNum1Hit:      docNum1Hit,
		includeFreqNorm: includeFreqNorm,
		includeLocs:     includeLocs,
	}, nil
}

// Posting is a single entry in a postings list
type Posting struct {
	docNum uint64
	freq   uint64
	normID uint8
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

// Norm returns the normalization factor for this posting: one lookup into the
// 256-entry table built from the field-norm quantization, with no arithmetic on
// the scan path at all.
func (p *Posting) Norm() float64 {
	return normFactorFromID(p.normID)
}

// NormUint64 returns the field length this posting's norm stands for.
func (p *Posting) NormUint64() uint64 {
	return uint64(idToFieldNorm(p.normID))
}

// Locations returns the location information for each occurrence
func (p *Posting) Locations() []segment.Location {
	return p.locs
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
