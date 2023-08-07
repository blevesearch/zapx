package zap

import (
	"encoding/binary"
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type VecPosting struct {
	docNum uint64
	score  float32
}

func (vp *VecPosting) Number() uint64 {
	return vp.docNum
}

func (vp *VecPosting) Score() float32 {
	return vp.score
}

func (vp *VecPosting) Size() int {
	return 0
}

// =============================================================================

// the vector postings list is supposed to store the docNum and its similarity
// score as a vector postings entry in it.
// The way in which is it stored is using a roaring64 bitmap.
// the docNum is stored in high 32 and the lower 32 bits contains the score value.
// the score is actually a float32 value and in order to store it as a uint32 in
// the bitmap, we use the IEEE 754 floating point format.
type VecPostingsList struct {
	except   *roaring64.Bitmap
	postings *roaring64.Bitmap
}

var emptyVecPostingsIterator = &VecPostingsIterator{}
var emptyVecPostingsList = &VecPostingsList{}

func (vpl *VecPostingsList) Iterator(prealloc segment.VecPostingsIterator) segment.VecPostingsIterator {

	// includeFreq, includeNorm, includeLocations are not used.
	// just reusing the PostingsList interface, perhaps might be a bit of cleanup
	// over there.

	// tbd: do we check the cardinality of postings and scores?

	var preallocPI *VecPostingsIterator
	pi, ok := prealloc.(*VecPostingsIterator)
	if ok && pi != nil {
		preallocPI = pi
	}
	if preallocPI == emptyVecPostingsIterator {
		preallocPI = nil
	}

	return vpl.iterator(preallocPI)
}

func (p *VecPostingsList) iterator(rv *VecPostingsIterator) *VecPostingsIterator {

	if rv == nil {
		rv = &VecPostingsIterator{}
	} else {
		*rv = VecPostingsIterator{} // clear the struct
	}
	// think on some of the edge cases over here.
	if p.postings == nil {
		return rv
	}
	rv.postings = p
	rv.all = p.postings.Iterator()
	if p.except != nil {
		rv.ActualBM = roaring64.AndNot(p.postings, p.except)
		rv.Actual = rv.ActualBM.Iterator()
	} else {
		rv.ActualBM = p.postings
		rv.Actual = rv.all // Optimize to use same iterator for all & Actual.
	}
	return rv
}

func (vpl *VecPostingsList) Size() int {
	return 0
}

func (vpl *VecPostingsList) Count() uint64 {
	return 0
}

func (vpl *VecPostingsList) ResetBytesRead(val uint64) {

}

func (vpl *VecPostingsList) BytesRead() uint64 {
	return 0
}

func (vpl *VecPostingsList) BytesWritten() uint64 {
	return 0
}

// =============================================================================

const maskLow32Bits = 0x7fffffff

type VecPostingsIterator struct {
	postings *VecPostingsList
	all      roaring64.IntPeekable64
	Actual   roaring64.IntPeekable64
	ActualBM *roaring64.Bitmap

	next VecPosting // reused across Next() calls
}

func (i *VecPostingsIterator) nextCodeAtOrAfterClean(atOrAfter uint64) (uint64, bool, error) {
	i.Actual.AdvanceIfNeeded(atOrAfter)

	if !i.Actual.HasNext() {
		return 0, false, nil // couldn't find anything
	}

	return i.Actual.Next(), true, nil
}

func (i *VecPostingsIterator) nextCodeAtOrAfter(atOrAfter uint64) (uint64, bool, error) {
	if i.Actual == nil || !i.Actual.HasNext() {
		return 0, false, nil
	}

	if i.postings == nil || i.postings == emptyVecPostingsList {
		// couldn't find anything
		return 0, false, nil
	}

	if i.postings.postings == i.ActualBM {
		return i.nextCodeAtOrAfterClean(atOrAfter)
	}

	i.Actual.AdvanceIfNeeded(atOrAfter)

	if !i.Actual.HasNext() || !i.all.HasNext() {
		// couldn't find anything
		return 0, false, nil
	}

	n := i.Actual.Next()
	allN := i.all.Next()

	// advancing the scores bitmap iterator parallely with the postings iterator.
	// given their cardinality are same, allScore would have correspond to the
	// right docNum's score.

	// chunk info not used.
	// nChunk := n / uint32(i.postings.chunkSize)

	// when allN becomes >= to here, then allN is in the same chunk as nChunk.
	// allNReachesNChunk := nChunk * uint32(i.postings.chunkSize)

	// n is the next actual hit (excluding some postings), and
	// allN is the next hit in the full postings, and
	// if they don't match, move 'all' forwards until they do.
	for allN != n {
		if !i.all.HasNext() {
			return 0, false, nil
		}
		allN = i.all.Next()
	}

	return uint64(n), true, nil
}

func (itr *VecPostingsIterator) Next() (segment.VecPosting, error) {
	return itr.nextAtOrAfter(0)
}

func (itr *VecPostingsIterator) Advance(docNum uint64) (segment.VecPosting, error) {
	return itr.nextAtOrAfter(docNum)
}

// Next returns the next posting on the postings list, or nil at the end
func (i *VecPostingsIterator) nextAtOrAfter(atOrAfter uint64) (segment.VecPosting, error) {
	atOrAfter = atOrAfter << 31
	code, exists, err := i.nextCodeAtOrAfter(atOrAfter)
	if err != nil || !exists {
		return nil, err
	}

	i.next = VecPosting{} // clear the struct
	rv := &i.next
	rv.score = math.Float32frombits(uint32(code & maskLow32Bits))
	rv.docNum = code >> 31

	return rv, nil
}

func (itr *VecPostingsIterator) Size() int {
	return 0
}

func (vpl *VecPostingsIterator) ResetBytesRead(val uint64) {

}

func (vpl *VecPostingsIterator) BytesRead() uint64 {
	return 0
}

func (vpl *VecPostingsIterator) BytesWritten() uint64 {
	return 0
}

func (sb *SegmentBase) SimilarVectors(field string, qVector []float32, k int64, except *roaring.Bitmap) (segment.VecPostingsList, error) {

	// 1. returned postings list (of type PostingsList) has two types of information - docNum and its score.
	// 2. both the values can be represented using roaring bitmaps.
	// 3. the Iterator (of type PostingsIterator) returned would operate in terms of VPostings.
	// 4. the VPostings would just have the docNum and the score. Every call of Next()
	//    and Advance just returns the next VPosting. The caller would do a vp.DocNum()
	//    and the Score() to get the values

	rv := &VecPostingsList{
		except:   nil, // todo: handle the except bitmap within postings iterator.
		postings: roaring64.New(),
	}

	vecDocIDMap := make(map[int64][]uint32)
	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 > 0 {
		vectorSection := sb.fieldsSectionsMap[fieldIDPlus1-1][sectionVectorIndex]
		if vectorSection > 0 {
			// not a good idea to cache the vector index perhaps, since it could be quite huge.
			pos := int(vectorSection)
			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			indexBytes := sb.mem[pos : pos+int(indexSize)]
			pos += int(indexSize)

			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			// read the vec IDs. todo: cache the vecID to docIDs mapping for a fieldID
			for i := 0; i < int(numVecs); i++ {
				vecID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				bitMapLen, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				roaringBytes := sb.mem[pos : pos+int(bitMapLen)]
				pos += int(bitMapLen)

				bitMap := roaring.NewBitmap()
				_, err := bitMap.FromBuffer(roaringBytes)
				if err != nil {
					return nil, err
				}

				vecDocIDMap[int64(vecID)] = bitMap.ToArray()
			}

			vecIndex, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
			if err != nil {
				return nil, err
			}

			scores, ids, err := vecIndex.Search(qVector, k)
			if err != nil {
				return nil, err
			}

			for i := 0; i < len(ids); i++ {
				vecID := ids[i]
				docIDs := vecDocIDMap[vecID]
				var code uint64

				for _, docID := range docIDs {
					if except != nil && except.Contains(docID) {
						// ignore the deleted doc
						continue
					}
					code = uint64(docID)<<31 | uint64(math.Float32bits(scores[i]))
					rv.postings.Add(uint64(code))
				}
			}
		}
	}

	return rv, nil
}
