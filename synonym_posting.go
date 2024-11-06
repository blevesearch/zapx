//  Copyright (c) 2024 Couchbase, Inc.
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
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizeSynonymsList int
var reflectStaticSizeSynonymsIterator int
var reflectStaticSizeSynonym int

func init() {
	var sl SynonymsList
	reflectStaticSizeSynonymsList = int(reflect.TypeOf(sl).Size())
	var si SynonymsIterator
	reflectStaticSizeSynonymsIterator = int(reflect.TypeOf(si).Size())
	var s Synonym
	reflectStaticSizePosting = int(reflect.TypeOf(s).Size())
}

type SynonymsList struct {
	sb             *SegmentBase
	synonymsOffset uint64
	synonyms       *roaring64.Bitmap
	except         *roaring.Bitmap

	synIDTermMap map[uint32][]byte

	buffer *bytes.Reader
}

// represents an immutable, empty synonyms list
var emptySynonymsList = &SynonymsList{}

func (p *SynonymsList) Size() int {
	sizeInBytes := reflectStaticSizeSynonymsList + SizeOfPtr

	if p.except != nil {
		sizeInBytes += int(p.except.GetSizeInBytes())
	}

	return sizeInBytes
}

// Iterator returns an iterator for this postings list
func (s *SynonymsList) Iterator(prealloc segment.SynonymsIterator) segment.SynonymsIterator {
	if s.synonyms == nil {
		return emptySynonymsIterator
	}

	var preallocSI *SynonymsIterator
	pi, ok := prealloc.(*SynonymsIterator)
	if ok && pi != nil {
		preallocSI = pi
	}
	if preallocSI == emptySynonymsIterator {
		preallocSI = nil
	}

	return s.iterator(preallocSI)
}

func (s *SynonymsList) iterator(rv *SynonymsIterator) *SynonymsIterator {
	if rv == nil {
		rv = &SynonymsIterator{}
	} else {
		*rv = SynonymsIterator{} // clear the struct
	}
	rv.synonyms = s
	rv.except = s.except
	rv.Actual = s.synonyms.Iterator()
	rv.ActualBM = s.synonyms

	rv.synIDTermMap = s.synIDTermMap

	return rv
}

func (rv *SynonymsList) read(synonymsOffset uint64, t *Thesaurus) error {
	rv.synonymsOffset = synonymsOffset

	var n uint64
	var read int

	var synonymsLen uint64
	synonymsLen, read = binary.Uvarint(t.sb.mem[synonymsOffset+n : synonymsOffset+n+binary.MaxVarintLen64])
	n += uint64(read)

	roaringBytes := t.sb.mem[synonymsOffset+n : synonymsOffset+n+synonymsLen]

	if rv.synonyms == nil {
		rv.synonyms = roaring64.NewBitmap()
	}

	rv.buffer.Reset(roaringBytes)

	_, err := rv.synonyms.ReadFrom(rv.buffer)
	if err != nil {
		return fmt.Errorf("error loading roaring bitmap: %v", err)
	}

	return nil
}

// -----------------------------------------------------------------------------
// SynonymsIterator provides a way to iterate through the synonyms list
type SynonymsIterator struct {
	synonyms *SynonymsList
	except   *roaring.Bitmap

	Actual   roaring64.IntPeekable64
	ActualBM *roaring64.Bitmap

	synIDTermMap map[uint32][]byte
	nextSyn      Synonym
}

var emptySynonymsIterator = &SynonymsIterator{}

func (i *SynonymsIterator) Size() int {
	sizeInBytes := reflectStaticSizeSynonymsIterator + SizeOfPtr +
		i.nextSyn.Size()

	return sizeInBytes
}

func (i *SynonymsIterator) Next() (segment.Synonym, error) {
	return i.next()
}

func (i *SynonymsIterator) next() (segment.Synonym, error) {
	synID, docNum, exists, err := i.nextSynonym()
	if err != nil || !exists {
		return nil, err
	}

	if i.synIDTermMap == nil {
		return nil, fmt.Errorf("synIDTermMap is nil")
	}

	// If the synonymID is not found in the map, return an error
	term, exists := i.synIDTermMap[synID]
	if !exists {
		return nil, fmt.Errorf("synonymID %d not found in map", synID)
	}

	i.nextSyn = Synonym{} // clear the struct
	rv := &i.nextSyn
	rv.term = string(term)
	rv.synID = synID
	rv.docNum = docNum

	return rv, nil
}

func (i *SynonymsIterator) nextSynonym() (uint32, uint32, bool, error) {
	// If no synonyms are available, return early
	if i.Actual == nil || i.synonyms == nil || i.synonyms == emptySynonymsList {
		return 0, 0, false, nil
	}

	var code uint64
	var docNum uint32
	var synID uint32

	// Loop to find the next valid docNum, checking against the except
	for i.Actual.HasNext() {
		code = i.Actual.Next()
		synID, docNum = decodeSynonym(code)

		// If docNum is not in the 'except' set, it's a valid result
		if i.except == nil || !i.except.Contains(docNum) {
			return synID, docNum, true, nil
		}
	}

	// If no valid docNum is found, return false
	return 0, 0, false, nil
}

type Synonym struct {
	term   string
	synID  uint32
	docNum uint32
}

func (p *Synonym) Size() int {
	sizeInBytes := reflectStaticSizePosting + SizeOfPtr +
		len(p.term)

	return sizeInBytes
}

func (s *Synonym) Term() string {
	return s.term
}

func (s *Synonym) SynonymID() uint32 {
	return s.synID
}

func (s *Synonym) DocNum() uint32 {
	return s.docNum
}

func decodeSynonym(synonymCode uint64) (synonymID uint32, docID uint32) {
	return uint32(synonymCode >> 32), uint32(synonymCode)
}
