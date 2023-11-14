//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"
	"math"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	faiss "github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizeVecPostingsList int
var reflectStaticSizeVecPostingsIterator int
var reflectStaticSizeVecPosting int

func init() {
	var pl VecPostingsList
	reflectStaticSizeVecPostingsList = int(reflect.TypeOf(pl).Size())
	var pi VecPostingsIterator
	reflectStaticSizeVecPostingsIterator = int(reflect.TypeOf(pi).Size())
	var p VecPosting
	reflectStaticSizeVecPosting = int(reflect.TypeOf(p).Size())
}

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
	sizeInBytes := reflectStaticSizePosting

	return sizeInBytes
}

// =============================================================================

// the vector postings list is supposed to store the docNum and its similarity
// score as a vector postings entry in it.
// The way in which is it stored is using a roaring64 bitmap.
// the docNum is stored in high 32 and the lower 32 bits contains the score value.
// the score is actually a float32 value and in order to store it as a uint32 in
// the bitmap, we use the IEEE 754 floating point format.
//
// each entry in the roaring64 bitmap of the vector postings list is a 64 bit
// number which looks like this:
// MSB                         LSB
// |64 63 62 ... 32| 31 30 ... 0|
// |    <docNum>   |   <score>  |
type VecPostingsList struct {
	// todo: perhaps we don't even need to store a bitmap if there is only
	// one similar vector the query, but rather store it as a field value
	// in the struct
	except   *roaring64.Bitmap
	postings *roaring64.Bitmap
}

var emptyVecPostingsIterator = &VecPostingsIterator{}
var emptyVecPostingsList = &VecPostingsList{}

func (vpl *VecPostingsList) Iterator(prealloc segment.VecPostingsIterator) segment.VecPostingsIterator {

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

func (p *VecPostingsList) Size() int {
	sizeInBytes := reflectStaticSizeVecPostingsList + SizeOfPtr

	if p.except != nil {
		sizeInBytes += int(p.except.GetSizeInBytes())
	}

	return sizeInBytes
}

func (p *VecPostingsList) Count() uint64 {
	n := p.postings.GetCardinality()
	var e uint64
	if p.except != nil {
		e = p.postings.AndCardinality(p.except)
	}
	return n - e
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

// a transformation function which stores both the score and the docNum as a single
// entry which is a uint64 number.
func getVectorCode(docNum uint32, score float32) uint64 {
	return uint64(docNum)<<31 | uint64(math.Float32bits(score))
}

// Next returns the next posting on the vector postings list, or nil at the end
func (i *VecPostingsIterator) nextAtOrAfter(atOrAfter uint64) (segment.VecPosting, error) {
	// transform the docNum provided to the vector code format and use that to
	// get the next entry. the comparison still happens docNum wise since after
	// the transformation, the docNum occupies the upper 32 bits just an entry in
	// the postings list
	atOrAfter = getVectorCode(uint32(atOrAfter), 0)
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

func (itr *VecPostingsIterator) Next() (segment.VecPosting, error) {
	return itr.nextAtOrAfter(0)
}

func (itr *VecPostingsIterator) Advance(docNum uint64) (segment.VecPosting, error) {
	return itr.nextAtOrAfter(docNum)
}

func (i *VecPostingsIterator) Size() int {
	sizeInBytes := reflectStaticSizePostingsIterator + SizeOfPtr +
		i.next.Size()

	return sizeInBytes
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
	// 3. the Iterator (of type PostingsIterator) returned would operate in terms of VecPostings.
	// 4. VecPostings would just have the docNum and the score. Every call of Next()
	//    and Advance just returns the next VecPostings. The caller would do a vp.Number()
	//    and the Score() to get the corresponding values
	rv := &VecPostingsList{
		except:   nil, // todo: handle the except bitmap within postings iterator.
		postings: roaring64.New(),
	}

	vecDocIDMap := make(map[int64][]uint32)
	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 > 0 {
		vectorSection := sb.fieldsSectionsMap[fieldIDPlus1-1][sectionFaissVectorIndex]
		// check if the field has a vector section in the segment.
		if vectorSection > 0 {
			pos := int(vectorSection)

			// loading doc values - adhering to the sections format. never
			// valid values for vector section
			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			// todo: not a good idea to cache the vector index perhaps, since it could be quite huge.
			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			indexBytes := sb.mem[pos : pos+int(indexSize)]
			pos += int(indexSize)

			// read the number vectors indexed for this field and load the vector to docID mapping.
			// todo: cache the vecID to docIDs mapping for a fieldID
			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			for i := 0; i < int(numVecs); i++ {
				vecID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				numDocs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				bitMap := roaring.NewBitmap()
				if numDocs == 1 {
					docID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
					pos += n
					bitMap.Add(uint32(docID))
				} else {
					bitMapLen, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
					pos += n

					roaringBytes := sb.mem[pos : pos+int(bitMapLen)]
					pos += int(bitMapLen)

					_, err := bitMap.FromBuffer(roaringBytes)
					if err != nil {
						return nil, err
					}
				}

				vecDocIDMap[int64(vecID)] = bitMap.ToArray()
			}

			vecIndex, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
			if err != nil {
				return nil, err
			}
			defer vecIndex.Close()

			scores, ids, err := vecIndex.Search(qVector, k)
			if err != nil {
				return nil, err
			}

			// for every similar vector returned by the Search() API, add the corresponding
			// docID and the score to the newly created vecPostingsList
			for i := 0; i < len(ids); i++ {
				vecID := ids[i]
				docIDs := vecDocIDMap[vecID]
				var code uint64

				for _, docID := range docIDs {
					if except != nil && except.Contains(docID) {
						// ignore the deleted doc
						continue
					}
					code = getVectorCode(docID, scores[i])
					rv.postings.Add(uint64(code))
				}
			}
		}
	}

	return rv, nil
}
