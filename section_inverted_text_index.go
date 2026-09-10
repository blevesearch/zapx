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

package zap

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
)

func init() {
	registerSegmentSection(SectionInvertedTextIndex, &invertedTextIndexSection{})
}

type invertedTextIndexSection struct {
}

// This function checks whether the inverted text index section should avoid processing
// a particular field, preventing unnecessary work if another section will handle it.
//
// NOTE: The exclusion check is applicable only to the InvertedTextIndexSection
// because it serves as a catch-all section. This section processes every field
// unless explicitly excluded, similar to a "default" case in a switch statement.
// Other sections, such as VectorSection and SynonymSection, rely on inclusion
// checks to process only specific field types (e.g., index.VectorField or
// index.SynonymField). Any new section added in the future must define its
// special field type and inclusion logic explicitly.
var isFieldExcludedFromInvertedTextIndexSection = func(field index.Field) bool {
	for _, excludeField := range invertedTextIndexSectionExclusionChecks {
		if excludeField(field) {
			// atleast one section has agreed to exclude this field
			// from inverted text index section processing and has
			// agreed to process it independently
			return true
		}
	}
	// no section has excluded this field from inverted index processing
	// so it should be processed by the inverted index section
	return false
}

// List of checks to determine if a field is excluded from the inverted text index section
var invertedTextIndexSectionExclusionChecks = make([]func(field index.Field) bool, 0)

func (i *invertedTextIndexSection) Process(opaque map[int]resetable, docNum uint32, field index.Field, fieldID uint16) {
	if !isFieldExcludedFromInvertedTextIndexSection(field) {
		io := i.getInvertedIndexOpaque(opaque)
		io.process(field, fieldID, docNum)
	}
}

func (i *invertedTextIndexSection) Persist(opaque map[int]resetable, w *FileWriter) error {
	io := i.getInvertedIndexOpaque(opaque)
	return io.writeDicts(w)
}

func (i *invertedTextIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	io := i.getInvertedIndexOpaque(opaque)
	return io.fieldAddrs[fieldID]
}

// mergeNormsColumn builds the merged field-norm column for one field by
// remapping each source segment's column through the merge's doc renumbering.
//
// This is where moving norms out of the postings pays off twice over: what used
// to be a re-encode of one varint per posting is now a single pass over the
// documents, and with no deletions it degenerates to a copy.
func mergeNormsColumn(dicts []*Dictionary, newDocNums [][]uint64,
	dropped []bool, numDocs uint64, buf []uint8) []uint8 {
	if cap(buf) < int(numDocs) {
		buf = make([]uint8, numDocs)
	} else {
		buf = buf[:numDocs]
		clear(buf)
	}

	for i, d := range dicts {
		if d == nil || d.norms == nil || d.norms.kind == normsKindAbsent {
			continue
		}
		nd := newDocNums[i]
		if len(nd) == 0 {
			continue
		}
		switch d.norms.kind {
		case normsKindConstant:
			for _, nw := range nd {
				if nw != docDropped {
					buf[nw] = d.norms.constant
				}
			}
		case normsKindDense:
			dense := d.norms.dense
			if len(dense) > len(nd) {
				dense = dense[:len(nd)]
			}
			if !dropped[i] {
				// No deletions means the renumbering is a constant shift, so
				// the whole column moves in one copy.
				copy(buf[nd[0]:], dense)
				continue
			}
			for old, id := range dense {
				if nw := nd[old]; nw != docDropped {
					buf[nw] = id
				}
			}
		}
	}
	return buf
}

// writeNormsColumn emits a field's norm column and reports where it landed.
// It runs before the field's postings because the per-block score bounds are
// computed from it, the same ordering constraint tantivy has between its
// fieldnorm and postings files.
func writeNormsColumn(w *FileWriter, ids []uint8, buf []byte) (
	offset, length uint64, bufOut []byte, err error) {
	buf = encodeNormsColumn(ids, buf[:0])
	processed := w.process(buf)
	offset = uint64(w.Count())
	if _, err := w.Write(processed); err != nil {
		return 0, 0, buf, err
	}
	return offset, uint64(len(processed)), buf, nil
}

func mergeAndPersistInvertedSection(segments []*SegmentBase, dropsIn []*roaring.Bitmap,
	fieldsInv []string, fieldsMap map[string]uint16, fieldsOptions map[string]index.FieldIndexingOptions,
	fieldsSame bool, newDocNumsIn [][]uint64, newSegDocCount uint64, chunkMode uint32, w *FileWriter,
	closeCh chan struct{}) (map[int]int, error) {
	var bufMaxVarintLen64 []byte = make([]byte, binary.MaxVarintLen64)
	var bufLoc []uint64
	var normsBuf []byte
	var normIDs []uint8

	var postings *PostingsList
	var postItr *PostingsIterator

	fieldAddrs := make(map[int]int)
	dictOffsets := make([]uint64, len(fieldsInv))
	normsOffsets := make([]uint64, len(fieldsInv))
	normsLens := make([]uint64, len(fieldsInv))
	fieldDvLocsStart := make([]uint64, len(fieldsInv))
	fieldDvLocsEnd := make([]uint64, len(fieldsInv))

	// copying data directly is safe only if there are no
	// file callbacks that might modify the data in all
	// of the involved segments and the current writer
	copyFlag := true
	for _, segment := range segments {
		if segment.fileReader.id != "" {
			copyFlag = false
			break
		}
	}
	if w.id != "" {
		copyFlag = false
	}

	// The location encoder is initialised with a placeholder chunk size; the
	// real one is set per term, from the same number that goes into the term
	// footer.
	locEncoder := newChunkedIntCoder(1024, newSegDocCount-1)
	ser := &postingsSerializer{w: w}

	var vellumBuf bytes.Buffer
	newVellum, err := vellum.New(&vellumBuf, nil)
	if err != nil {
		return nil, err
	}

	newDocNums := make([][]uint64, 0, len(segments))
	dropped := make([]bool, 0, len(segments))
	dicts := make([]*Dictionary, 0, len(segments))
	itrs := make([]vellum.Iterator, 0, len(segments))
	segmentsInFocus := make([]*SegmentBase, 0, len(segments))
	// for each field
	for fieldID, fieldName := range fieldsInv {
		// collect FST iterators from all active segments for this field
		newDocNums = newDocNums[:0]
		dropped = dropped[:0]
		dicts = dicts[:0]
		itrs = itrs[:0]
		segmentsInFocus = segmentsInFocus[:0]
		for segmentI, segment := range segments {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return nil, seg.ErrClosed
			}
			// early exit if the field's index option is false
			if !fieldsOptions[fieldName].IsIndexed() {
				continue
			}

			dict, err2 := segment.dictionary(fieldName)
			if err2 != nil {
				return nil, err2
			}
			if dict != nil && dict.fst != nil {
				itr, err2 := dict.fst.Iterator(nil, nil)
				if err2 != nil && err2 != vellum.ErrIteratorDone {
					return nil, err2
				}
				if itr != nil {
					newDocNums = append(newDocNums, newDocNumsIn[segmentI])
					dropped = append(dropped,
						dropsIn[segmentI] != nil && !dropsIn[segmentI].IsEmpty())
					dicts = append(dicts, dict)
					itrs = append(itrs, itr)
					segmentsInFocus = append(segmentsInFocus, segment)
				}
			}
		}

		hasFreqs := !fieldsOptions[fieldName].SkipFreqNorm()

		// The norm column comes first: the block bounds written with each term
		// are computed from it.
		normsOffsets[fieldID] = 0
		normsLens[fieldID] = 0
		if hasFreqs && len(dicts) > 0 {
			normIDs = mergeNormsColumn(dicts, newDocNums, dropped, newSegDocCount, normIDs)
			normsOffsets[fieldID], normsLens[fieldID], normsBuf, err =
				writeNormsColumn(w, normIDs, normsBuf)
			if err != nil {
				return nil, err
			}
		} else {
			normIDs = normIDs[:0]
		}

		enumerator, err := newEnumerator(itrs)

		for err == nil {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return nil, seg.ErrClosed
			}

			term, _, _ := enumerator.Current()
			lowIdxs, lowVals := enumerator.GetLowIdxsAndValues()

			// Look at every contributing segment's term footer before writing
			// anything. Two things are needed up front: whether the merged term
			// records locations, because a term that does must carry a location
			// group for every posting, and an upper bound on its doc frequency
			// to size the location chunks. The bound is an upper one because
			// deletions are only discovered while streaming -- which is exactly
			// why the chunk size is recorded in the footer rather than being
			// re-derived by the reader.
			var upperDocFreq uint64
			termHasLocs := false
			for i, idx := range lowIdxs {
				postings, err = dicts[idx].postingsListFromOffset(lowVals[i], nil, postings)
				if err != nil {
					return nil, err
				}
				upperDocFreq += postings.rawDocFreq()
				if postings.footer.hasLocs() {
					termHasLocs = true
				}
			}

			locChunkSize, err2 := getChunkSize(chunkMode, upperDocFreq, newSegDocCount)
			if err2 != nil {
				return nil, err2
			}
			locEncoder.SetChunkSize(locChunkSize, newSegDocCount-1)

			ser.StartTerm(hasFreqs, termHasLocs, normIDs)

			for i, idx := range lowIdxs {
				var except *roaring.Bitmap
				if dropped[idx] {
					except = dropsForDict(dropsIn, segments, segmentsInFocus, idx)
				}
				postings, err = dicts[idx].postingsListFromOffset(lowVals[i], except, postings)
				if err != nil {
					return nil, err
				}

				postItr, err = postings.iterator(true, true, termHasLocs, postItr)
				if err != nil {
					return nil, err
				}

				// can only safely copy location bytes if all segments have the
				// same fields and all have an empty writer id (i.e. no callbacks)
				if fieldsSame && copyFlag {
					err = mergeTermPostingsByCopying(postItr, newDocNums[idx],
						termHasLocs, ser, locEncoder)
				} else {
					bufLoc, err = mergeTermPostings(fieldsMap, postItr, newDocNums[idx],
						termHasLocs, ser, locEncoder, bufLoc)
				}
				if err != nil {
					return nil, err
				}
			}

			if err = finishMergedTerm(w, ser, locEncoder, newVellum, term,
				locChunkSize); err != nil {
				return nil, err
			}

			// step the enumerator past every entry for this term
			for range lowIdxs {
				err = enumerator.Next()
				if err != nil {
					break
				}
			}
		}
		if err != vellum.ErrIteratorDone {
			return nil, err
		}
		// close the enumerator to free the underlying iterators
		err = enumerator.Close()
		if err != nil {
			return nil, err
		}

		dictOffset := uint64(w.Count())

		err = newVellum.Close()
		if err != nil {
			return nil, err
		}
		vellumData := w.process(vellumBuf.Bytes())

		// write out the length of the vellum data
		n := binary.PutUvarint(bufMaxVarintLen64, uint64(len(vellumData)))
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return nil, err
		}

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return nil, err
		}

		dictOffsets[fieldID] = dictOffset

		fieldDvLocsStart[fieldID] = uint64(w.Count())

		// update the field doc values
		// NOTE: doc values continue to use legacy chunk mode
		chunkSize, err := getChunkSize(LegacyChunkMode, 0, 0)
		if err != nil {
			return nil, err
		}
		if fieldsOptions[fieldName].SkipDVChunking() {
			chunkSize = 1
		}
		fdvEncoder := newChunkedContentCoder(chunkSize, newSegDocCount-1, w, true, fieldsOptions[fieldName].SkipDVCompression())

		fdvReadersAvailable := false
		var dvIterClone *docValueReader
		var dvIter *docValueReader
		for segmentI, segment := range segmentsInFocus {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return nil, seg.ErrClosed
			}
			// early exit if docvalues are not wanted for this field
			if !fieldsOptions[fieldName].IncludeDocValues() {
				continue
			}
			fieldIDPlus1 := uint16(segment.fieldsMap[fieldName])
			dvIter = segment.fieldDvReaders[SectionInvertedTextIndex][fieldIDPlus1-1]
			if dvIter != nil {
				fdvReadersAvailable = true
				dvIterClone = dvIter.cloneInto(dvIterClone)
				err = dvIterClone.iterateAllDocValues(segment, func(docNum uint64, terms []byte) error {
					if newDocNums[segmentI][docNum] == docDropped {
						return nil
					}
					err := fdvEncoder.Add(newDocNums[segmentI][docNum], terms)
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
			}
		}

		if fdvReadersAvailable {
			err = fdvEncoder.Close()
			if err != nil {
				return nil, err
			}

			// persist the doc value details for this field
			_, err = fdvEncoder.Write()
			if err != nil {
				return nil, err
			}

			// get the field doc value offset (end)
			fieldDvLocsEnd[fieldID] = uint64(w.Count())
		} else {
			fieldDvLocsStart[fieldID] = fieldNotUninverted
			fieldDvLocsEnd[fieldID] = fieldNotUninverted
		}

		fieldStart := w.Count()

		err = writeFieldFooter(w, bufMaxVarintLen64, fieldDvLocsStart[fieldID],
			fieldDvLocsEnd[fieldID], dictOffsets[fieldID],
			normsOffsets[fieldID], normsLens[fieldID])
		if err != nil {
			return nil, err
		}

		fieldAddrs[fieldID] = fieldStart

		// reset vellum buffer and vellum builder
		vellumBuf.Reset()
		err = newVellum.Reset(&vellumBuf)
		if err != nil {
			return nil, err
		}
	}
	return fieldAddrs, nil
}

// dropsForDict maps an index into the per-field dicts slice back to the
// deletion bitmap of the segment it came from.
func dropsForDict(dropsIn []*roaring.Bitmap, segments []*SegmentBase,
	segmentsInFocus []*SegmentBase, idx int) *roaring.Bitmap {
	target := segmentsInFocus[idx]
	for i, s := range segments {
		if s == target {
			return dropsIn[i]
		}
	}
	return nil
}

// finishMergedTerm closes out one term: the uvarint tail, the location blob,
// the skip list and the footer, then the FST entry.
func finishMergedTerm(w *FileWriter, ser *postingsSerializer, locEncoder *chunkedIntCoder,
	builder *vellum.Builder, term []byte, locChunkSize uint64) error {
	if ser.docFreq == 0 {
		locEncoder.Reset()
		return nil
	}

	if fstVal, ok := ser.OneHit(); ok {
		locEncoder.Reset()
		return builder.Insert(term, fstVal)
	}

	if err := ser.FinishPayload(); err != nil {
		return err
	}

	locEncoder.Close()
	_, locBytes, err := locEncoder.writeAt(w)
	if err != nil {
		return err
	}
	locEncoder.Reset()

	offset, err := ser.Close(uint64(locBytes), locChunkSize)
	if err != nil {
		return err
	}
	return builder.Insert(term, offset)
}

// writeFieldFooter records where each of a field's regions ended up. The first
// three entries are unchanged from earlier zap versions -- other sections read
// the doc-value pair from the same place -- and the norm column's location is
// appended after them.
func writeFieldFooter(w *FileWriter, buf []byte,
	dvStart, dvEnd, dictOffset, normsOffset, normsLen uint64) error {
	for _, v := range []uint64{dvStart, dvEnd, dictOffset, normsOffset, normsLen} {
		n := binary.PutUvarint(buf, v)
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}
	return nil
}

func (i *invertedTextIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64,
	w *FileWriter, closeCh chan struct{}) error {
	io := i.getInvertedIndexOpaque(opaque)
	fieldAddrs, err := mergeAndPersistInvertedSection(segments, drops, fieldsInv,
		io.FieldsMap, io.FieldsOptions, io.fieldsSame, newDocNumsIn, io.numDocs, io.chunkMode, w, closeCh)
	if err != nil {
		return err
	}

	io.fieldAddrs = fieldAddrs
	return nil
}

func (i *invertedIndexOpaque) grabBuf(size int) []byte {
	buf := i.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		i.tmp0 = buf
	}
	return buf[:size]
}

func (i *invertedIndexOpaque) incrementBytesWritten(bytes uint64) {
	i.bytesWritten += bytes
}

func (i *invertedIndexOpaque) BytesWritten() uint64 {
	return i.bytesWritten
}

func (i *invertedIndexOpaque) BytesRead() uint64 {
	return 0
}

func (i *invertedIndexOpaque) ResetBytesRead(uint64) {}

func (io *invertedIndexOpaque) writeDicts(w *FileWriter) error {
	if len(io.results) == 0 {
		return nil
	}

	numDocs := uint64(len(io.results))

	dictOffsets := make([]uint64, len(io.FieldsInv))
	normsOffsets := make([]uint64, len(io.FieldsInv))
	normsLens := make([]uint64, len(io.FieldsInv))
	var err error

	fdvOffsetsStart := make([]uint64, len(io.FieldsInv))
	fdvOffsetsEnd := make([]uint64, len(io.FieldsInv))

	buf := io.grabBuf(binary.MaxVarintLen64)

	// The location encoder is initialised with a placeholder chunk size; the
	// real one is set per term and recorded in that term's footer.
	locEncoder := newChunkedIntCoder(1024, numDocs-1)
	ser := &postingsSerializer{w: w}

	var docTermMap [][]byte
	var normsBuf []byte

	if io.builder == nil {
		io.builder, err = vellum.New(&io.builderBuf, nil)
		if err != nil {
			return err
		}
	}

	for fieldID, terms := range io.DictKeys {
		if cap(docTermMap) < len(io.results) {
			docTermMap = make([][]byte, len(io.results))
		} else {
			docTermMap = docTermMap[:len(io.results)]
			for docNum := range docTermMap { // reset the docTermMap
				docTermMap[docNum] = docTermMap[docNum][:0]
			}
		}

		dict := io.Dicts[fieldID]
		hasFreqs := !io.FieldsOptions[io.FieldsInv[fieldID]].SkipFreqNorm()

		// The field's norm column is written before its postings, because the
		// per-block score bounds are computed from it.
		normIDs := io.NormIDs[fieldID]
		if !hasFreqs {
			normIDs = nil
		}
		if len(normIDs) > 0 {
			normsOffsets[fieldID], normsLens[fieldID], normsBuf, err =
				writeNormsColumn(w, normIDs, normsBuf)
			if err != nil {
				return err
			}
			io.incrementBytesWritten(normsLens[fieldID])
		}

		for _, term := range terms { // terms are already sorted
			pid := dict[term] - 1

			docNums := io.Postings[pid]
			if len(docNums) == 0 {
				continue
			}
			freqs := io.Freqs[pid]
			locs := io.Locs[pid]

			// A term that records locations carries a location group for every
			// one of its postings, empty ones included, so a reader walking the
			// location stream stays aligned without a per-document flag.
			termHasLocs := io.numLocsPerPostingsList[pid] > 0

			locChunkSize, err := getChunkSize(io.chunkMode, uint64(len(docNums)), numDocs)
			if err != nil {
				return err
			}
			locEncoder.SetChunkSize(locChunkSize, numDocs-1)

			ser.StartTerm(hasFreqs, termHasLocs, normIDs)

			for i, docNum := range docNums {
				freq := freqs[i]

				if err = ser.AddDoc(docNum, freq); err != nil {
					return err
				}

				if termHasLocs {
					docLocs := locs[i]

					numBytesLocs := 0
					for _, loc := range docLocs {
						numBytesLocs += totalUvarintBytes(
							uint64(loc.fieldID), loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)), loc.arrayposs)
					}

					if err = locEncoder.Add(uint64(docNum), uint64(numBytesLocs)); err != nil {
						return err
					}
					for _, loc := range docLocs {
						err = locEncoder.Add(uint64(docNum),
							uint64(loc.fieldID), loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)))
						if err != nil {
							return err
						}
						if err = locEncoder.Add(uint64(docNum), loc.arrayposs...); err != nil {
							return err
						}
					}
				}

				docTermMap[docNum] = append(
					append(docTermMap[docNum], term...),
					index.DocValueTermSeparator)
			}

			if err = finishMergedTerm(w, ser, locEncoder, io.builder,
				[]byte(term), locChunkSize); err != nil {
				return err
			}
			io.incrementBytesWritten(ser.BytesWritten())
			io.incrementBytesWritten(locEncoder.getBytesWritten())
		}

		err = io.builder.Close()
		if err != nil {
			return err
		}

		// record where this dictionary starts
		dictOffsets[fieldID] = uint64(w.Count())

		vellumData := w.process(io.builderBuf.Bytes())

		// write out the length of the vellum data
		n := binary.PutUvarint(buf, uint64(len(vellumData)))
		_, err = w.Write(buf[:n])
		if err != nil {
			return err
		}

		io.incrementBytesWritten(uint64(len(vellumData)))

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return err
		}

		// reset vellum for reuse
		io.builderBuf.Reset()

		err = io.builder.Reset(&io.builderBuf)
		if err != nil {
			return err
		}

		// write the field doc values
		// NOTE: doc values continue to use legacy chunk mode
		chunkSize, err := getChunkSize(LegacyChunkMode, 0, 0)
		if err != nil {
			return err
		}
		if io.FieldsOptions[io.FieldsInv[fieldID]].SkipDVChunking() {
			chunkSize = 1
		}
		fdvEncoder := newChunkedContentCoder(chunkSize, numDocs-1, w, false, io.FieldsOptions[io.FieldsInv[fieldID]].SkipDVCompression())
		if io.IncludeDocValues[fieldID] {
			for docNum, docTerms := range docTermMap {
				if fieldTermMap, ok := io.extraDocValues[docNum]; ok {
					if sTerms, ok := fieldTermMap[uint16(fieldID)]; ok {
						for _, sTerm := range sTerms {
							docTerms = append(append(docTerms, sTerm...), index.DocValueTermSeparator)
						}
					}
				}
				if len(docTerms) > 0 {
					err = fdvEncoder.Add(uint64(docNum), docTerms)
					if err != nil {
						return err
					}
				}
			}
			err = fdvEncoder.Close()
			if err != nil {
				return err
			}

			io.incrementBytesWritten(fdvEncoder.getBytesWritten())

			fdvOffsetsStart[fieldID] = uint64(w.Count())

			_, err = fdvEncoder.Write()
			if err != nil {
				return err
			}

			fdvOffsetsEnd[fieldID] = uint64(w.Count())
			fdvEncoder.Reset()
		} else {
			fdvOffsetsStart[fieldID] = fieldNotUninverted
			fdvOffsetsEnd[fieldID] = fieldNotUninverted
		}

		fieldStart := w.Count()

		err = writeFieldFooter(w, buf, fdvOffsetsStart[fieldID], fdvOffsetsEnd[fieldID],
			dictOffsets[fieldID], normsOffsets[fieldID], normsLens[fieldID])
		if err != nil {
			return err
		}

		io.fieldAddrs[fieldID] = fieldStart
	}

	return nil
}

func (io *invertedIndexOpaque) process(field index.Field, fieldID uint16, docNum uint32) {
	if !io.init && io.results != nil {
		io.realloc()
		io.init = true
	}

	// if the fieldID is MaxUint16, it's mainly indicated that the caller has
	// finished invoking the process() for every field on that doc.
	if fieldID == math.MaxUint16 {
		for fid, tfs := range io.reusableFieldTFs {
			if tfs == nil {
				continue
			}
			dict := io.Dicts[fid]

			// The field's length is quantized once per document into the
			// field's norm column, rather than being copied alongside every
			// posting of every term the document contains.
			if len(io.NormIDs[fid]) > 0 {
				io.NormIDs[fid][docNum] = fieldNormToID(uint32(io.reusableFieldLens[fid]))
			}

			for term, tf := range tfs {
				pid := dict[term] - 1
				io.Postings[pid] = append(io.Postings[pid], docNum)
				io.Freqs[pid] = append(io.Freqs[pid], uint32(tf.Frequency()))

				n := len(tf.Locations)
				docLocs := io.locsRemaining[pid][:n:n]
				for i, loc := range tf.Locations {
					var locf = uint16(fid)
					if loc.Field != "" {
						locf = uint16(io.getOrDefineField(loc.Field))
					}
					var arrayposs []uint64
					if len(loc.ArrayPositions) > 0 {
						arrayposs = loc.ArrayPositions
					}
					docLocs[i] = interimLoc{
						fieldID:   locf,
						pos:       uint64(loc.Position),
						start:     uint64(loc.Start),
						end:       uint64(loc.End),
						arrayposs: arrayposs,
					}
				}
				io.locsRemaining[pid] = io.locsRemaining[pid][n:]
				io.Locs[pid] = append(io.Locs[pid], docLocs)
			}
		}
		for i := 0; i < len(io.FieldsInv); i++ { // clear these for reuse
			io.reusableFieldLens[i] = 0
			io.reusableFieldTFs[i] = nil
		}
		return
	}

	io.reusableFieldLens[fieldID] += field.AnalyzedLength()
	existingFreqs := io.reusableFieldTFs[fieldID]
	if existingFreqs != nil {
		existingFreqs.MergeAll(field.Name(), field.AnalyzedTokenFrequencies())
	} else {
		io.reusableFieldTFs[fieldID] = field.AnalyzedTokenFrequencies()
	}
}

func (i *invertedIndexOpaque) initDictsAndKeysFromFields() {
	numFields := len(i.FieldsInv)

	// Resize or allocate Dicts
	if cap(i.Dicts) >= numFields {
		i.Dicts = i.Dicts[:numFields]
	} else {
		i.Dicts = make([]map[string]uint64, numFields)
	}

	// Resize or allocate DictKeys
	if cap(i.DictKeys) >= numFields {
		i.DictKeys = i.DictKeys[:numFields]
	} else {
		i.DictKeys = make([][]string, numFields)
	}

	for idx := 0; idx < numFields; idx++ {
		// --- Dicts ---
		if i.Dicts[idx] == nil {
			i.Dicts[idx] = make(map[string]uint64)
		} else {
			clear(i.Dicts[idx])
		}

		// --- DictKeys ---
		if i.DictKeys[idx] != nil {
			i.DictKeys[idx] = i.DictKeys[idx][:0]
		} else {
			i.DictKeys[idx] = nil
		}
	}
}

func (i *invertedIndexOpaque) realloc() {
	var pidNext int

	var totTFs int
	var totLocs int

	// initialize dicts and dict keys from fieldsMap
	i.initDictsAndKeysFromFields()

	visitField := func(field index.Field, docNum int) {
		fieldID := uint16(i.getOrDefineField(field.Name()))

		dict := i.Dicts[fieldID]
		dictKeys := i.DictKeys[fieldID]

		tfs := field.AnalyzedTokenFrequencies()
		for term, tf := range tfs {
			pidPlus1, exists := dict[term]
			if !exists {
				pidNext++
				pidPlus1 = uint64(pidNext)

				dict[term] = pidPlus1
				dictKeys = append(dictKeys, term)

				i.numTermsPerPostingsList = append(i.numTermsPerPostingsList, 0)
				i.numLocsPerPostingsList = append(i.numLocsPerPostingsList, 0)
			}

			pid := pidPlus1 - 1

			i.numTermsPerPostingsList[pid] += 1
			i.numLocsPerPostingsList[pid] += len(tf.Locations)

			totLocs += len(tf.Locations)
		}

		totTFs += len(tfs)

		i.DictKeys[fieldID] = dictKeys
		if field.Options().IncludeDocValues() {
			i.IncludeDocValues[fieldID] = true
		}

		if f, ok := field.(index.GeoShapeField); ok {
			if _, exists := i.extraDocValues[docNum]; !exists {
				i.extraDocValues[docNum] = make(map[uint16][][]byte)
			}
			i.extraDocValues[docNum][fieldID] = append(i.extraDocValues[docNum][fieldID], f.EncodedShape())
		}
	}

	if cap(i.IncludeDocValues) >= len(i.FieldsInv) {
		i.IncludeDocValues = i.IncludeDocValues[:len(i.FieldsInv)]
	} else {
		i.IncludeDocValues = make([]bool, len(i.FieldsInv))
	}

	if i.extraDocValues == nil {
		i.extraDocValues = map[int]map[uint16][][]byte{}
	}

	for docNum, result := range i.results {
		// walk each composite field
		result.VisitComposite(func(field index.CompositeField) {
			visitField(field, docNum)
		})

		// walk each field
		result.VisitFields(func(field index.Field) {
			visitField(field, docNum)
		})
	}

	numPostingsLists := pidNext

	// Doc numbers accumulate into plain slices carved out of one backing array.
	// They arrive already sorted -- process() is called per document in order --
	// so there is nothing a bitmap would add here beyond its own overhead.
	if cap(i.Postings) >= numPostingsLists {
		i.Postings = i.Postings[:numPostingsLists]
	} else {
		i.Postings = make([][]uint32, numPostingsLists)
	}

	if cap(i.postingsBacking) >= totTFs {
		i.postingsBacking = i.postingsBacking[:totTFs]
	} else {
		i.postingsBacking = make([]uint32, totTFs)
	}

	postingsBacking := i.postingsBacking
	for pid, numTerms := range i.numTermsPerPostingsList {
		i.Postings[pid] = postingsBacking[0:0]
		postingsBacking = postingsBacking[numTerms:]
	}

	if cap(i.Freqs) >= numPostingsLists {
		i.Freqs = i.Freqs[:numPostingsLists]
	} else {
		i.Freqs = make([][]uint32, numPostingsLists)
	}

	if cap(i.freqsBacking) >= totTFs {
		i.freqsBacking = i.freqsBacking[:totTFs]
	} else {
		i.freqsBacking = make([]uint32, totTFs)
	}

	freqsBacking := i.freqsBacking
	for pid, numTerms := range i.numTermsPerPostingsList {
		i.Freqs[pid] = freqsBacking[0:0]
		freqsBacking = freqsBacking[numTerms:]
	}

	// Locs mirrors Freqs one level deeper: one []interimLoc per doc-occurrence,
	// carved from locsPerDocBacking, each in turn carved from locsBacking as
	// process() fills them in via locsRemaining.
	if cap(i.Locs) >= numPostingsLists {
		i.Locs = i.Locs[:numPostingsLists]
	} else {
		i.Locs = make([][][]interimLoc, numPostingsLists)
	}

	if cap(i.locsPerDocBacking) >= totTFs {
		i.locsPerDocBacking = i.locsPerDocBacking[:totTFs]
	} else {
		i.locsPerDocBacking = make([][]interimLoc, totTFs)
	}

	locsPerDocBacking := i.locsPerDocBacking
	for pid, numTerms := range i.numTermsPerPostingsList {
		i.Locs[pid] = locsPerDocBacking[0:0]
		locsPerDocBacking = locsPerDocBacking[numTerms:]
	}

	if cap(i.locsBacking) >= totLocs {
		i.locsBacking = i.locsBacking[:totLocs]
	} else {
		i.locsBacking = make([]interimLoc, totLocs)
	}

	if cap(i.locsRemaining) >= numPostingsLists {
		i.locsRemaining = i.locsRemaining[:numPostingsLists]
	} else {
		i.locsRemaining = make([][]interimLoc, numPostingsLists)
	}

	locsBacking := i.locsBacking
	for pid, numLocs := range i.numLocsPerPostingsList {
		i.locsRemaining[pid] = locsBacking[:numLocs]
		locsBacking = locsBacking[numLocs:]
	}

	for _, dict := range i.DictKeys {
		sort.Strings(dict)
	}

	// One norm column per field, indexed by doc number. Documents that do not
	// contain the field keep the zero the allocation leaves behind, which is
	// exactly what the reader expects.
	numFields := len(i.FieldsInv)
	numDocsInBatch := len(i.results)
	if cap(i.NormIDs) >= numFields {
		i.NormIDs = i.NormIDs[:numFields]
	} else {
		i.NormIDs = make([][]uint8, numFields)
	}
	for fid := 0; fid < numFields; fid++ {
		if cap(i.NormIDs[fid]) >= numDocsInBatch {
			i.NormIDs[fid] = i.NormIDs[fid][:numDocsInBatch]
			clear(i.NormIDs[fid])
		} else {
			i.NormIDs[fid] = make([]uint8, numDocsInBatch)
		}
	}

	if cap(i.reusableFieldTFs) >= len(i.FieldsInv) {
		i.reusableFieldTFs = i.reusableFieldTFs[:len(i.FieldsInv)]
	} else {
		i.reusableFieldTFs = make([]index.TokenFrequencies, len(i.FieldsInv))
	}

	if cap(i.reusableFieldLens) >= len(i.FieldsInv) {
		i.reusableFieldLens = i.reusableFieldLens[:len(i.FieldsInv)]
	} else {
		i.reusableFieldLens = make([]int, len(i.FieldsInv))
	}
}

func (i *invertedTextIndexSection) getInvertedIndexOpaque(opaque map[int]resetable) *invertedIndexOpaque {
	if _, ok := opaque[SectionInvertedTextIndex]; !ok {
		opaque[SectionInvertedTextIndex] = i.InitOpaque(nil)
	}
	return opaque[SectionInvertedTextIndex].(*invertedIndexOpaque)
}

func (i *invertedIndexOpaque) getOrDefineField(fieldName string) int {
	fieldIDPlus1, exists := i.FieldsMap[fieldName]
	if !exists {
		fieldIDPlus1 = uint16(len(i.FieldsInv) + 1)
		i.FieldsMap[fieldName] = fieldIDPlus1
		i.FieldsInv = append(i.FieldsInv, fieldName)

		i.Dicts = append(i.Dicts, make(map[string]uint64))

		n := len(i.DictKeys)
		if n < cap(i.DictKeys) {
			i.DictKeys = i.DictKeys[:n+1]
			i.DictKeys[n] = i.DictKeys[n][:0]
		} else {
			i.DictKeys = append(i.DictKeys, []string(nil))
		}
	}

	return int(fieldIDPlus1 - 1)
}

func (i *invertedTextIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &invertedIndexOpaque{
		fieldAddrs: map[int]int{},
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

type invertedIndexOpaque struct {
	bytesWritten uint64 // atomic access to this variable, moved to top to correct alignment issues on ARM, 386 and 32-bit MIPS.

	results []index.Document

	chunkMode uint32

	// indicates whethere the following structs are initialized
	init bool

	// FieldsMap adds 1 to field id to avoid zero value issues
	//  name -> field id + 1
	FieldsMap map[string]uint16

	// FieldsInv is the inverse of FieldsMap
	//  field id -> name
	FieldsInv []string

	// Field indexing options
	//  field name -> options
	FieldsOptions map[string]index.FieldIndexingOptions

	// Term dictionaries for each field
	//  field id -> term -> postings list id + 1
	Dicts []map[string]uint64

	// Terms for each field, where terms are sorted ascending
	//  field id -> []term
	DictKeys [][]string

	// Fields whose IncludeDocValues is true
	//  field id -> bool
	IncludeDocValues []bool

	// postings id -> ascending docNums
	Postings        [][]uint32
	postingsBacking []uint32

	// field id -> docNum -> quantized field length
	NormIDs [][]uint8

	// postings id -> freqs, one for each docNum in postings
	Freqs        [][]uint32
	freqsBacking []uint32

	// postings id -> doc index (same index as Postings/Freqs) -> that doc's
	// locs. A doc with no locations for the term still gets an (empty) entry,
	// so len(Locs[pid][i]) is the location count for Postings[pid][i] -- there
	// is no separate count to keep in sync.
	Locs              [][][]interimLoc
	locsPerDocBacking [][]interimLoc
	locsBacking       []interimLoc

	// locsRemaining is the per-pid unconsumed tail of locsBacking, used only
	// while process() is carving each doc's slice out of it.
	locsRemaining [][]interimLoc

	numTermsPerPostingsList []int // key is postings list id
	numLocsPerPostingsList  []int // key is postings list id

	// store terms that are unnecessary for the term dictionaries but needed in doc values
	// eg - encoded geoshapes
	// docNum -> fieldID -> terms
	extraDocValues map[int]map[uint16][][]byte

	builder    *vellum.Builder
	builderBuf bytes.Buffer

	// reusable stuff for processing fields etc.
	reusableFieldLens []int
	reusableFieldTFs  []index.TokenFrequencies

	tmp0 []byte

	fieldAddrs map[int]int

	fieldsSame bool
	numDocs    uint64
}

func (io *invertedIndexOpaque) Reset() (err error) {
	// cleanup stuff over here
	io.results = nil
	io.init = false
	io.chunkMode = 0
	io.FieldsMap = nil
	io.FieldsOptions = nil
	io.FieldsInv = nil
	for i := range io.Dicts {
		io.Dicts[i] = nil
	}
	io.Dicts = io.Dicts[:0]
	for i := range io.DictKeys {
		io.DictKeys[i] = io.DictKeys[i][:0]
	}
	io.DictKeys = io.DictKeys[:0]
	for i := range io.IncludeDocValues {
		io.IncludeDocValues[i] = false
	}
	io.IncludeDocValues = io.IncludeDocValues[:0]
	for i := range io.Postings {
		io.Postings[i] = nil
	}
	io.Postings = io.Postings[:0]
	io.postingsBacking = io.postingsBacking[:0]
	for i := range io.NormIDs {
		io.NormIDs[i] = io.NormIDs[i][:0]
	}
	io.NormIDs = io.NormIDs[:0]
	for i := range io.Freqs {
		io.Freqs[i] = nil
	}
	io.Freqs = io.Freqs[:0]
	io.freqsBacking = io.freqsBacking[:0]
	for i := range io.Locs {
		io.Locs[i] = nil
	}
	io.Locs = io.Locs[:0]
	for i := range io.locsPerDocBacking {
		io.locsPerDocBacking[i] = nil
	}
	io.locsPerDocBacking = io.locsPerDocBacking[:0]
	for i := range io.locsBacking {
		io.locsBacking[i] = interimLoc{}
	}
	io.locsBacking = io.locsBacking[:0]
	for i := range io.locsRemaining {
		io.locsRemaining[i] = nil
	}
	io.locsRemaining = io.locsRemaining[:0]
	io.numTermsPerPostingsList = io.numTermsPerPostingsList[:0]
	io.numLocsPerPostingsList = io.numLocsPerPostingsList[:0]
	io.builderBuf.Reset()
	if io.builder != nil {
		err = io.builder.Reset(&io.builderBuf)
	}

	io.reusableFieldLens = io.reusableFieldLens[:0]
	io.reusableFieldTFs = io.reusableFieldTFs[:0]

	io.tmp0 = io.tmp0[:0]
	io.extraDocValues = nil
	atomic.StoreUint64(&io.bytesWritten, 0)
	io.fieldsSame = false
	io.numDocs = 0

	clear(io.fieldAddrs)

	return err
}
func (i *invertedIndexOpaque) Set(key string, val interface{}) {
	switch key {
	case "results":
		i.results = val.([]index.Document)
	case "chunkMode":
		i.chunkMode = val.(uint32)
	case "fieldsSame":
		i.fieldsSame = val.(bool)
	case "fieldsMap":
		i.FieldsMap = val.(map[string]uint16)
	case "fieldsOptions":
		i.FieldsOptions = val.(map[string]index.FieldIndexingOptions)
	case "fieldsInv":
		i.FieldsInv = val.([]string)
	case "numDocs":
		i.numDocs = val.(uint64)
	}
}
