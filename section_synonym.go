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
	"io"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
)

func init() {
	registerSegmentSection(SectionSynonymIndex, &synonymIndexSection{})
	invertedIndexExclusionChecks = append(invertedIndexExclusionChecks, func(field index.Field) bool {
		_, ok := field.(index.SynonymField)
		return ok
	})
}

// -----------------------------------------------------------------------------

type synonymIndexOpaque struct {
	results []index.Document

	// indicates whether the following structs are initialized
	init bool

	// FieldsMap maps field name to field id and must be set in
	// the index opaque using the key "fieldsMap"
	// used for ensuring accurate mapping between fieldID and
	// thesaurusID
	//  name -> field id
	FieldsMap map[string]uint16

	// ThesaurusMap adds 1 to thesaurus id to avoid zero value issues
	//  name -> thesaurus id + 1
	ThesaurusMap map[string]uint16

	// ThesaurusMapInv is the inverse of ThesaurusMap
	//  thesaurus id + 1 -> name
	ThesaurusInv []string

	// Thesaurus for each thesaurus ID
	//  thesaurus id -> LHS term -> synonym postings list id + 1
	Thesauri []map[string]uint64

	// LHS Terms for each thesaurus ID, where terms are sorted ascending
	//  thesaurus id -> []term
	ThesaurusKeys [][]string

	// FieldIDtoThesaurusID maps the field id to the thesaurus id
	//  field id -> thesaurus id
	FieldIDtoThesaurusID map[uint16]int

	// SynonymIDtoTerm maps synonym id to term for each thesaurus
	//  thesaurus id -> synonym id -> term
	SynonymTermToID []map[string]uint32

	// SynonymTermToID maps term to synonym id for each thesaurus
	//  thesaurus id -> term -> synonym id
	// this is the inverse of SynonymIDtoTerm for each thesaurus
	SynonymIDtoTerm []map[uint32]string

	//  synonym postings list -> synonym bitmap
	Synonyms []*roaring64.Bitmap

	builder    *vellum.Builder
	builderBuf bytes.Buffer

	tmp0 []byte

	thesaurusAddrs map[int]int
}

func (so *synonymIndexOpaque) Set(key string, value interface{}) {
	switch key {
	case "results":
		so.results = value.([]index.Document)
	case "fieldsMap":
		so.FieldsMap = value.(map[string]uint16)
	}
}

func (so *synonymIndexOpaque) Reset() (err error) {
	// cleanup stuff over here
	so.results = nil
	so.init = false
	so.ThesaurusMap = nil
	so.ThesaurusInv = nil
	for i := range so.Thesauri {
		so.Thesauri[i] = nil
	}
	so.Thesauri = so.Thesauri[:0]
	for i := range so.ThesaurusKeys {
		so.ThesaurusKeys[i] = so.ThesaurusKeys[i][:0]
	}
	so.ThesaurusKeys = so.ThesaurusKeys[:0]
	for _, idn := range so.Synonyms {
		idn.Clear()
	}
	so.Synonyms = so.Synonyms[:0]
	so.builderBuf.Reset()
	if so.builder != nil {
		err = so.builder.Reset(&so.builderBuf)
	}
	so.FieldIDtoThesaurusID = nil
	so.SynonymTermToID = nil
	so.SynonymIDtoTerm = nil

	so.tmp0 = so.tmp0[:0]
	return err
}

func (so *synonymIndexOpaque) process(field index.SynonymField, fieldID uint16, docNum uint32) {
	if !so.init && so.results != nil {
		so.realloc()
		so.init = true
	}

	tid := so.FieldIDtoThesaurusID[fieldID]

	thesaurus := so.Thesauri[tid]

	termSynMap := so.SynonymTermToID[tid]

	field.IterateSynonyms(func(term string, synonyms []string) {
		pid := thesaurus[term] - 1

		bs := so.Synonyms[pid]

		for _, syn := range synonyms {
			code := encodeSynonym(termSynMap[syn], docNum)
			bs.Add(code)
		}
	})
}

func (so *synonymIndexOpaque) realloc() {
	var pidNext int
	var sidNext uint32
	so.ThesaurusMap = map[string]uint16{}
	so.FieldIDtoThesaurusID = map[uint16]int{}

	for _, result := range so.results {
		if synDoc, ok := result.(index.SynonymDocument); ok {
			synDoc.VisitSynonymFields(func(synField index.SynonymField) {
				fieldIDPlus1 := so.FieldsMap[synField.Name()]
				so.getOrDefineThesaurus(fieldIDPlus1-1, synField.Name())
			})
		}
	}

	for _, result := range so.results {
		if synDoc, ok := result.(index.SynonymDocument); ok {
			synDoc.VisitSynonymFields(func(synField index.SynonymField) {
				fieldIDPlus1 := so.FieldsMap[synField.Name()]
				thesaurusID := uint16(so.getOrDefineThesaurus(fieldIDPlus1-1, synField.Name()))

				thesaurus := so.Thesauri[thesaurusID]
				thesaurusKeys := so.ThesaurusKeys[thesaurusID]

				synTermMap := so.SynonymIDtoTerm[thesaurusID]

				termSynMap := so.SynonymTermToID[thesaurusID]

				synField.IterateSynonyms(func(term string, synonyms []string) {
					_, exists := thesaurus[term]
					if !exists {
						pidNext++
						pidPlus1 := uint64(pidNext)

						thesaurus[term] = pidPlus1
						thesaurusKeys = append(thesaurusKeys, term)
					}
					for _, syn := range synonyms {
						_, exists := termSynMap[syn]
						if !exists {
							sidNext++
							sidPlus1 := sidNext
							termSynMap[syn] = sidPlus1
							synTermMap[sidPlus1] = syn
						}
					}
				})
				so.ThesaurusKeys[thesaurusID] = thesaurusKeys
			})
		}
	}

	numSynonymsLists := pidNext

	if cap(so.Synonyms) >= numSynonymsLists {
		so.Synonyms = so.Synonyms[:numSynonymsLists]
	} else {
		synonyms := make([]*roaring64.Bitmap, numSynonymsLists)
		copy(synonyms, so.Synonyms[:cap(so.Synonyms)])
		for i := 0; i < numSynonymsLists; i++ {
			if synonyms[i] == nil {
				synonyms[i] = roaring64.New()
			}
		}
		so.Synonyms = synonyms
	}

	for _, thes := range so.ThesaurusKeys {
		sort.Strings(thes)
	}
}

func (so *synonymIndexOpaque) getOrDefineThesaurus(fieldID uint16, thesaurusName string) int {
	thesaurusIDPlus1, exists := so.ThesaurusMap[thesaurusName]
	if !exists {
		thesaurusIDPlus1 = uint16(len(so.ThesaurusInv) + 1)
		so.ThesaurusMap[thesaurusName] = thesaurusIDPlus1
		so.ThesaurusInv = append(so.ThesaurusInv, thesaurusName)

		so.Thesauri = append(so.Thesauri, make(map[string]uint64))

		so.SynonymIDtoTerm = append(so.SynonymIDtoTerm, make(map[uint32]string))

		so.SynonymTermToID = append(so.SynonymTermToID, make(map[string]uint32))

		so.FieldIDtoThesaurusID[fieldID] = int(thesaurusIDPlus1 - 1)

		n := len(so.ThesaurusKeys)
		if n < cap(so.ThesaurusKeys) {
			so.ThesaurusKeys = so.ThesaurusKeys[:n+1]
			so.ThesaurusKeys[n] = so.ThesaurusKeys[n][:0]
		} else {
			so.ThesaurusKeys = append(so.ThesaurusKeys, []string(nil))
		}
	}

	return int(thesaurusIDPlus1 - 1)
}

func (so *synonymIndexOpaque) grabBuf(size int) []byte {
	buf := so.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		so.tmp0 = buf
	}
	return buf[:size]
}

func (so *synonymIndexOpaque) writeThesauri(w *CountHashWriter) (thesOffsets []uint64, err error) {

	if so.results == nil || len(so.results) == 0 {
		return nil, nil
	}

	thesOffsets = make([]uint64, len(so.ThesaurusInv))

	buf := so.grabBuf(binary.MaxVarintLen64)

	if so.builder == nil {
		so.builder, err = vellum.New(&so.builderBuf, nil)
		if err != nil {
			return nil, err
		}
	}

	for thesaurusID, terms := range so.ThesaurusKeys {
		thes := so.Thesauri[thesaurusID]
		for _, term := range terms { // terms are already sorted
			pid := thes[term] - 1
			postingsBS := so.Synonyms[pid]
			postingsOffset, err := writeSynonyms(postingsBS, w, buf)
			if err != nil {
				return nil, err
			}

			if postingsOffset > uint64(0) {
				err = so.builder.Insert([]byte(term), postingsOffset)
				if err != nil {
					return nil, err
				}
			}
		}

		err = so.builder.Close()
		if err != nil {
			return nil, err
		}

		thesOffsets[thesaurusID] = uint64(w.Count())

		vellumData := so.builderBuf.Bytes()

		// write out the length of the vellum data
		n := binary.PutUvarint(buf, uint64(len(vellumData)))
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return nil, err
		}

		// reset vellum for reuse
		so.builderBuf.Reset()

		err = so.builder.Reset(&so.builderBuf)
		if err != nil {
			return nil, err
		}

		// write out the synTermMap for this thesaurus
		err := writeSynTermMap(so.SynonymIDtoTerm[thesaurusID], w, buf)
		if err != nil {
			return nil, err
		}

		thesaurusStart := w.Count()

		n = binary.PutUvarint(buf, fieldNotUninverted)
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		n = binary.PutUvarint(buf, fieldNotUninverted)
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		n = binary.PutUvarint(buf, thesOffsets[thesaurusID])
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}
		so.thesaurusAddrs[thesaurusID] = thesaurusStart
	}
	return thesOffsets, nil
}

// -----------------------------------------------------------------------------

type synonymIndexSection struct {
}

func (s *synonymIndexSection) getSynonymIndexOpaque(opaque map[int]resetable) *synonymIndexOpaque {
	if _, ok := opaque[SectionSynonymIndex]; !ok {
		opaque[SectionSynonymIndex] = s.InitOpaque(nil)
	}
	return opaque[SectionSynonymIndex].(*synonymIndexOpaque)
}

func (s *synonymIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &synonymIndexOpaque{
		thesaurusAddrs: map[int]int{},
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

func (s *synonymIndexSection) Process(opaque map[int]resetable, docNum uint32, field index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}
	if sf, ok := field.(index.SynonymField); ok {
		so := s.getSynonymIndexOpaque(opaque)
		so.process(sf, fieldID, docNum)
	}
}

func (s *synonymIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {
	synIndexOpaque := s.getSynonymIndexOpaque(opaque)
	_, err = synIndexOpaque.writeThesauri(w)
	return 0, err
}

func (s *synonymIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	synIndexOpaque := s.getSynonymIndexOpaque(opaque)
	tid, exists := synIndexOpaque.FieldIDtoThesaurusID[uint16(fieldID)]
	if !exists {
		return 0
	}
	return synIndexOpaque.thesaurusAddrs[tid]
}

func (s *synonymIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64,
	w *CountHashWriter, closeCh chan struct{}) error {
	so := s.getSynonymIndexOpaque(opaque)
	thesaurusAddrs, fieldIDtoThesaurusID, err := mergeAndPersistSynonymSection(segments, drops, fieldsInv, newDocNumsIn, w, closeCh)
	if err != nil {
		return err
	}

	so.thesaurusAddrs = thesaurusAddrs
	so.FieldIDtoThesaurusID = fieldIDtoThesaurusID
	return nil
}

// -----------------------------------------------------------------------------

func encodeSynonym(synonymID uint32, docID uint32) uint64 {
	return uint64(synonymID)<<32 | uint64(docID)
}

func writeSynonyms(postings *roaring64.Bitmap, w *CountHashWriter, bufMaxVarintLen64 []byte) (
	offset uint64, err error) {
	termCardinality := postings.GetCardinality()
	if termCardinality <= 0 {
		return 0, nil
	}

	postingsOffset := uint64(w.Count())

	err = writeRoaringSynonymWithLen(postings, w, bufMaxVarintLen64)
	if err != nil {
		return 0, err
	}

	return postingsOffset, nil
}

func writeSynTermMap(synTermMap map[uint32]string, w *CountHashWriter, bufMaxVarintLen64 []byte) error {
	if len(synTermMap) == 0 {
		return nil
	}
	n := binary.PutUvarint(bufMaxVarintLen64, uint64(len(synTermMap)))
	_, err := w.Write(bufMaxVarintLen64[:n])
	if err != nil {
		return err
	}

	for sid, term := range synTermMap {
		n = binary.PutUvarint(bufMaxVarintLen64, uint64(sid))
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return err
		}

		n = binary.PutUvarint(bufMaxVarintLen64, uint64(len(term)))
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return err
		}

		_, err = w.Write([]byte(term))
		if err != nil {
			return err
		}
	}

	return nil
}

// writes out the length of the roaring bitmap in bytes as varint
// then writes out the roaring bitmap itself
func writeRoaringSynonymWithLen(r *roaring64.Bitmap, w io.Writer,
	reuseBufVarint []byte) error {
	buf, err := r.ToBytes()
	if err != nil {
		return err
	}

	// write out the length
	n := binary.PutUvarint(reuseBufVarint, uint64(len(buf)))
	_, err = w.Write(reuseBufVarint[:n])
	if err != nil {
		return err
	}

	// write out the roaring bytes
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func mergeAndPersistSynonymSection(segments []*SegmentBase, dropsIn []*roaring.Bitmap,
	fieldsInv []string, newDocNumsIn [][]uint64, w *CountHashWriter,
	closeCh chan struct{}) (map[int]int, map[uint16]int, error) {

	var bufMaxVarintLen64 []byte = make([]byte, binary.MaxVarintLen64)

	var synonyms *SynonymsList
	var synItr *SynonymsIterator

	thesaurusAddrs := make(map[int]int)

	var vellumBuf bytes.Buffer
	newVellum, err := vellum.New(&vellumBuf, nil)
	if err != nil {
		return nil, nil, err
	}

	newRoaring := roaring64.NewBitmap()

	newDocNums := make([][]uint64, 0, len(segments))

	drops := make([]*roaring.Bitmap, 0, len(segments))

	thesauri := make([]*Thesaurus, 0, len(segments))

	itrs := make([]vellum.Iterator, 0, len(segments))

	fieldIDtoThesaurusID := make(map[uint16]int)

	synTermMap := make(map[uint32]string)
	termSynMap := make(map[string]uint32)
	var thesaurusID int
	var newSynonymID uint32

	// for each field
	for fieldID, fieldName := range fieldsInv {
		// collect FST iterators from all active segments for this field
		newDocNums = newDocNums[:0]
		drops = drops[:0]
		thesauri = thesauri[:0]
		itrs = itrs[:0]
		newSynonymID = 0
		for syn := range synTermMap {
			delete(synTermMap, syn)
		}
		for syn := range termSynMap {
			delete(termSynMap, syn)
		}

		for segmentI, segment := range segments {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return nil, nil, seg.ErrClosed
			}

			thes, err2 := segment.thesaurus(fieldName)
			if err2 != nil {
				return nil, nil, err2
			}
			if thes != nil && thes.fst != nil {
				itr, err2 := thes.fst.Iterator(nil, nil)
				if err2 != nil && err2 != vellum.ErrIteratorDone {
					return nil, nil, err2
				}
				if itr != nil {
					newDocNums = append(newDocNums, newDocNumsIn[segmentI])
					if dropsIn[segmentI] != nil && !dropsIn[segmentI].IsEmpty() {
						drops = append(drops, dropsIn[segmentI])
					} else {
						drops = append(drops, nil)
					}
					thesauri = append(thesauri, thes)
					itrs = append(itrs, itr)
				}
			}
		}

		// if no iterators, skip this field
		if len(itrs) == 0 {
			continue
		}

		var prevTerm []byte

		newRoaring.Clear()

		finishTerm := func(term []byte) error {
			postingsOffset, err := writeSynonyms(newRoaring, w, bufMaxVarintLen64)
			if err != nil {
				return err
			}
			if postingsOffset > 0 {
				err = newVellum.Insert(term, postingsOffset)
				if err != nil {
					return err
				}
			}
			newRoaring.Clear()
			return nil
		}

		enumerator, err := newEnumerator(itrs)

		for err == nil {
			term, itrI, postingsOffset := enumerator.Current()

			if prevTerm != nil && !bytes.Equal(prevTerm, term) {
				// check for the closure in meantime
				if isClosed(closeCh) {
					return nil, nil, seg.ErrClosed
				}

				// if the term changed, write out the info collected
				// for the previous term
				err = finishTerm(prevTerm)
				if err != nil {
					return nil, nil, err
				}
			}

			synonyms, err = thesauri[itrI].synonymsListFromOffset(
				postingsOffset, drops[itrI], synonyms)
			if err != nil {
				return nil, nil, err
			}
			synItr = synonyms.iterator(synItr)

			newSynonymID, err = mergeSynonyms(synItr, newDocNums[itrI], newRoaring, synTermMap, termSynMap, newSynonymID)
			if err != nil {
				return nil, nil, err
			}

			prevTerm = prevTerm[:0] // copy to prevTerm in case Next() reuses term mem
			prevTerm = append(prevTerm, term...)
			err = enumerator.Next()
		}
		if err != vellum.ErrIteratorDone {
			return nil, nil, err
		}
		// close the enumerator to free the underlying iterators
		err = enumerator.Close()
		if err != nil {
			return nil, nil, err
		}

		if prevTerm != nil {
			err = finishTerm(prevTerm)
			if err != nil {
				return nil, nil, err
			}
		}

		err = newVellum.Close()
		if err != nil {
			return nil, nil, err
		}
		vellumData := vellumBuf.Bytes()

		thesOffset := uint64(w.Count())

		// write out the length of the vellum data
		n := binary.PutUvarint(bufMaxVarintLen64, uint64(len(vellumData)))
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return nil, nil, err
		}

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return nil, nil, err
		}

		// reset vellum buffer and vellum builder
		vellumBuf.Reset()
		err = newVellum.Reset(&vellumBuf)
		if err != nil {
			return nil, nil, err
		}

		// write out the synTermMap for this thesaurus
		err = writeSynTermMap(synTermMap, w, bufMaxVarintLen64)
		if err != nil {
			return nil, nil, err
		}

		thesStart := w.Count()

		n = binary.PutUvarint(bufMaxVarintLen64, fieldNotUninverted)
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return nil, nil, err
		}

		n = binary.PutUvarint(bufMaxVarintLen64, fieldNotUninverted)
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return nil, nil, err
		}

		n = binary.PutUvarint(bufMaxVarintLen64, thesOffset)
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return nil, nil, err
		}

		// if we have a new thesaurus, add it to the thesaurus map
		fieldIDtoThesaurusID[uint16(fieldID)] = thesaurusID
		thesaurusAddrs[thesaurusID] = thesStart
		thesaurusID++
	}

	return thesaurusAddrs, fieldIDtoThesaurusID, nil
}

func mergeSynonyms(synItr *SynonymsIterator, newDocNums []uint64, newRoaring *roaring64.Bitmap,
	synTermMap map[uint32]string, termSynMap map[string]uint32, newSynonymID uint32) (uint32, error) {
	next, err := synItr.Next()
	for next != nil && err == nil {
		synNewDocNum := newDocNums[next.Number()]
		if synNewDocNum == docDropped {
			return 0, fmt.Errorf("see hit with dropped docNum")
		}
		nextTerm := next.Term()
		var synNewID uint32
		if synID, ok := termSynMap[nextTerm]; ok {
			synNewID = synID
		} else {
			synNewID = newSynonymID
			termSynMap[nextTerm] = newSynonymID
			synTermMap[newSynonymID] = nextTerm
			newSynonymID++
		}
		synNewCode := encodeSynonym(synNewID, uint32(synNewDocNum))
		newRoaring.Add(synNewCode)
		next, err = synItr.Next()
	}
	return newSynonymID, nil
}

// -----------------------------------------------------------------------------
