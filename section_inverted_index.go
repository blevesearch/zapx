package zap

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
)

type invertedIndexSection struct {
}

func (i *invertedIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {
	invIndexOpaque := i.getInvertedIndexOpaque(opaque)
	// no need to handle the ^uint16(0) case here, its handled in opaque?
	invIndexOpaque.process(field, fieldID, docNum)
}

func (i *invertedIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {
	invIndexOpaque := i.getInvertedIndexOpaque(opaque)
	_, _ = invIndexOpaque.writeDicts(w)
	return 0, nil
}

func (i *invertedIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	invIndexOpaque := i.getInvertedIndexOpaque(opaque)
	return invIndexOpaque.fieldAddrs[fieldID]
}

func mergeAndPersistInvertedSection(segments []*SegmentBase, dropsIn []*roaring.Bitmap,
	fieldsInv []string, fieldsMap map[string]uint16, fieldsSame bool,
	newDocNumsIn [][]uint64, newSegDocCount uint64, chunkMode uint32,
	w *CountHashWriter, closeCh chan struct{}) (map[int]int, uint64, error) {
	var bufMaxVarintLen64 []byte = make([]byte, binary.MaxVarintLen64)
	var bufLoc []uint64

	var postings *PostingsList
	var postItr *PostingsIterator

	fieldAddrs := make(map[int]int)
	dictOffsets := make([]uint64, len(fieldsInv))
	fieldDvLocsStart := make([]uint64, len(fieldsInv))
	fieldDvLocsEnd := make([]uint64, len(fieldsInv))

	// these int coders are initialized with chunk size 1024
	// however this will be reset to the correct chunk size
	// while processing each individual field-term section
	tfEncoder := newChunkedIntCoder(1024, newSegDocCount-1)
	locEncoder := newChunkedIntCoder(1024, newSegDocCount-1)

	var vellumBuf bytes.Buffer
	newVellum, err := vellum.New(&vellumBuf, nil)
	if err != nil {
		return nil, 0, err
	}

	newRoaring := roaring.NewBitmap()

	// for each field
	for fieldID, fieldName := range fieldsInv {
		// collect FST iterators from all active segments for this field
		var newDocNums [][]uint64
		var drops []*roaring.Bitmap
		var dicts []*Dictionary
		var itrs []vellum.Iterator

		var segmentsInFocus []*SegmentBase

		for segmentI, segment := range segments {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return nil, 0, seg.ErrClosed
			}

			dict, err2 := segment.dictionary(fieldName)
			if err2 != nil {
				return nil, 0, err2
			}
			if dict != nil && dict.fst != nil {
				itr, err2 := dict.fst.Iterator(nil, nil)
				if err2 != nil && err2 != vellum.ErrIteratorDone {
					return nil, 0, err2
				}
				if itr != nil {
					newDocNums = append(newDocNums, newDocNumsIn[segmentI])
					if dropsIn[segmentI] != nil && !dropsIn[segmentI].IsEmpty() {
						drops = append(drops, dropsIn[segmentI])
					} else {
						drops = append(drops, nil)
					}
					dicts = append(dicts, dict)
					itrs = append(itrs, itr)
					segmentsInFocus = append(segmentsInFocus, segment)
				}
			}
		}

		var prevTerm []byte

		newRoaring.Clear()

		var lastDocNum, lastFreq, lastNorm uint64

		// determines whether to use "1-hit" encoding optimization
		// when a term appears in only 1 doc, with no loc info,
		// has freq of 1, and the docNum fits into 31-bits
		use1HitEncoding := func(termCardinality uint64) (bool, uint64, uint64) {
			if termCardinality == uint64(1) && locEncoder.FinalSize() <= 0 {
				docNum := uint64(newRoaring.Minimum())
				if under32Bits(docNum) && docNum == lastDocNum && lastFreq == 1 {
					return true, docNum, lastNorm
				}
			}
			return false, 0, 0
		}

		finishTerm := func(term []byte) error {
			tfEncoder.Close()
			locEncoder.Close()

			postingsOffset, err := writePostings(newRoaring,
				tfEncoder, locEncoder, use1HitEncoding, w, bufMaxVarintLen64)
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

			tfEncoder.Reset()
			locEncoder.Reset()

			lastDocNum = 0
			lastFreq = 0
			lastNorm = 0

			return nil
		}

		enumerator, err := newEnumerator(itrs)

		for err == nil {
			term, itrI, postingsOffset := enumerator.Current()

			if !bytes.Equal(prevTerm, term) {
				// check for the closure in meantime
				if isClosed(closeCh) {
					return nil, 0, seg.ErrClosed
				}

				// if the term changed, write out the info collected
				// for the previous term
				err = finishTerm(prevTerm)
				if err != nil {
					return nil, 0, err
				}
			}
			if !bytes.Equal(prevTerm, term) || prevTerm == nil {
				// compute cardinality of field-term in new seg
				var newCard uint64
				lowItrIdxs, lowItrVals := enumerator.GetLowIdxsAndValues()
				for i, idx := range lowItrIdxs {
					pl, err := dicts[idx].postingsListFromOffset(lowItrVals[i], drops[idx], nil)
					if err != nil {
						return nil, 0, err
					}
					newCard += pl.Count()
				}
				// compute correct chunk size with this
				chunkSize, err := getChunkSize(chunkMode, newCard, newSegDocCount)
				if err != nil {
					return nil, 0, err
				}
				// update encoders chunk
				tfEncoder.SetChunkSize(chunkSize, newSegDocCount-1)
				locEncoder.SetChunkSize(chunkSize, newSegDocCount-1)
			}

			postings, err = dicts[itrI].postingsListFromOffset(
				postingsOffset, drops[itrI], postings)
			if err != nil {
				return nil, 0, err
			}

			postItr = postings.iterator(true, true, true, postItr)

			if fieldsSame {
				// can optimize by copying freq/norm/loc bytes directly
				lastDocNum, lastFreq, lastNorm, err = mergeTermFreqNormLocsByCopying(
					term, postItr, newDocNums[itrI], newRoaring,
					tfEncoder, locEncoder)
			} else {
				lastDocNum, lastFreq, lastNorm, bufLoc, err = mergeTermFreqNormLocs(
					fieldsMap, term, postItr, newDocNums[itrI], newRoaring,
					tfEncoder, locEncoder, bufLoc)
			}
			if err != nil {
				return nil, 0, err
			}

			prevTerm = prevTerm[:0] // copy to prevTerm in case Next() reuses term mem
			prevTerm = append(prevTerm, term...)

			err = enumerator.Next()
		}
		if err != vellum.ErrIteratorDone {
			return nil, 0, err
		}

		err = finishTerm(prevTerm)
		if err != nil {
			return nil, 0, err
		}

		dictOffset := uint64(w.Count())

		err = newVellum.Close()
		if err != nil {
			return nil, 0, err
		}
		vellumData := vellumBuf.Bytes()

		// write out the length of the vellum data
		n := binary.PutUvarint(bufMaxVarintLen64, uint64(len(vellumData)))
		_, err = w.Write(bufMaxVarintLen64[:n])
		if err != nil {
			return nil, 0, err
		}

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return nil, 0, err
		}

		dictOffsets[fieldID] = dictOffset

		// perhaps rather than populating the slice we can just write the offsets at
		// the beginning/end of the section. the format followed should be consistent
		// across process, persist and merge
		//
		// get the field doc value offset (start)
		fieldDvLocsStart[fieldID] = uint64(w.Count())

		// update the field doc values
		// NOTE: doc values continue to use legacy chunk mode
		chunkSize, err := getChunkSize(LegacyChunkMode, 0, 0)
		if err != nil {
			return nil, 0, err
		}
		fdvEncoder := newChunkedContentCoder(chunkSize, newSegDocCount-1, w, true)

		fdvReadersAvailable := false
		var dvIterClone *docValueReader
		for segmentI, segment := range segmentsInFocus {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return nil, 0, seg.ErrClosed
			}

			fieldIDPlus1 := uint16(segment.fieldsMap[fieldName])
			if dvIter, exists := segment.fieldDvReaders[fieldIDPlus1-1]; exists &&
				dvIter != nil {
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
					return nil, 0, err
				}
			}
		}

		if fdvReadersAvailable {
			err = fdvEncoder.Close()
			if err != nil {
				return nil, 0, err
			}

			// persist the doc value details for this field
			_, err = fdvEncoder.Write()
			if err != nil {
				return nil, 0, err
			}

			// get the field doc value offset (end)
			fieldDvLocsEnd[fieldID] = uint64(w.Count())
		} else {
			fieldDvLocsStart[fieldID] = fieldNotUninverted
			fieldDvLocsEnd[fieldID] = fieldNotUninverted
		}

		fieldStart := w.Count()

		// todo: uvarint these offsets
		err = binary.Write(w, binary.BigEndian, fieldDvLocsStart[fieldID])
		if err != nil {
			return nil, 0, err
		}

		err = binary.Write(w, binary.BigEndian, fieldDvLocsEnd[fieldID])
		if err != nil {
			return nil, 0, err
		}

		err = binary.Write(w, binary.BigEndian, dictOffsets[fieldID])
		if err != nil {
			return nil, 0, err
		}
		fieldAddrs[fieldID] = fieldStart

		// reset vellum buffer and vellum builder
		vellumBuf.Reset()
		err = newVellum.Reset(&vellumBuf)
		if err != nil {
			return nil, 0, err
		}
	}

	fieldDvLocsOffset := uint64(w.Count())

	return fieldAddrs, fieldDvLocsOffset, nil
}

func (i *invertedIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {

	io := i.getInvertedIndexOpaque(opaque)
	fieldAddrs, _, err := mergeAndPersistInvertedSection(segments, drops, fieldsInv,
		io.FieldsMap, io.fieldsSame, newDocNumsIn, io.numDocs, io.chunkMode, w, closeCh)
	if err != nil {
		return err
	}

	io.fieldAddrs = fieldAddrs

	return nil
}

// todo: is it possible to merge this resuable stuff with the interim's tmp0?
func (i *invertedIndexOpaque) grabBuf(size int) []byte {
	buf := i.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		i.tmp0 = buf
	}
	return buf[0:size]
}

func (io *invertedIndexOpaque) writeDicts(w *CountHashWriter) (dictOffsets []uint64, err error) {

	dictOffsets = make([]uint64, len(io.FieldsInv))

	fdvOffsetsStart := make([]uint64, len(io.FieldsInv))
	fdvOffsetsEnd := make([]uint64, len(io.FieldsInv))

	buf := io.grabBuf(binary.MaxVarintLen64)

	// these int coders are initialized with chunk size 1024
	// however this will be reset to the correct chunk size
	// while processing each individual field-term section
	tfEncoder := newChunkedIntCoder(1024, uint64(len(io.results)-1))
	locEncoder := newChunkedIntCoder(1024, uint64(len(io.results)-1))

	var docTermMap [][]byte

	if io.builder == nil {
		io.builder, err = vellum.New(&io.builderBuf, nil)
		if err != nil {
			return nil, err
		}
	}

	for fieldID, terms := range io.DictKeys {
		if cap(docTermMap) < len(io.results) {
			docTermMap = make([][]byte, len(io.results))
		} else {
			docTermMap = docTermMap[0:len(io.results)]
			for docNum := range docTermMap { // reset the docTermMap
				docTermMap[docNum] = docTermMap[docNum][:0]
			}
		}

		dict := io.Dicts[fieldID]

		for _, term := range terms { // terms are already sorted
			pid := dict[term] - 1

			postingsBS := io.Postings[pid]

			freqNorms := io.FreqNorms[pid]
			freqNormOffset := 0

			locs := io.Locs[pid]
			locOffset := 0

			chunkSize, err := getChunkSize(io.chunkMode, postingsBS.GetCardinality(), uint64(len(io.results)))
			if err != nil {
				return nil, err
			}
			tfEncoder.SetChunkSize(chunkSize, uint64(len(io.results)-1))
			locEncoder.SetChunkSize(chunkSize, uint64(len(io.results)-1))

			postingsItr := postingsBS.Iterator()
			for postingsItr.HasNext() {
				docNum := uint64(postingsItr.Next())

				freqNorm := freqNorms[freqNormOffset]

				// check if freq/norm is enabled
				if freqNorm.freq > 0 {
					err = tfEncoder.Add(docNum,
						encodeFreqHasLocs(freqNorm.freq, freqNorm.numLocs > 0),
						uint64(math.Float32bits(freqNorm.norm)))
				} else {
					// if disabled, then skip the norm part
					err = tfEncoder.Add(docNum,
						encodeFreqHasLocs(freqNorm.freq, freqNorm.numLocs > 0))
				}
				if err != nil {
					return nil, err
				}

				if freqNorm.numLocs > 0 {
					numBytesLocs := 0
					for _, loc := range locs[locOffset : locOffset+freqNorm.numLocs] {
						numBytesLocs += totalUvarintBytes(
							uint64(loc.fieldID), loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)), loc.arrayposs)
					}

					err = locEncoder.Add(docNum, uint64(numBytesLocs))
					if err != nil {
						return nil, err
					}
					for _, loc := range locs[locOffset : locOffset+freqNorm.numLocs] {
						err = locEncoder.Add(docNum,
							uint64(loc.fieldID), loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)))
						if err != nil {
							return nil, err
						}

						err = locEncoder.Add(docNum, loc.arrayposs...)
						if err != nil {
							return nil, err
						}
					}
					locOffset += freqNorm.numLocs
				}

				freqNormOffset++

				docTermMap[docNum] = append(
					append(docTermMap[docNum], term...),
					termSeparator)
			}

			tfEncoder.Close()
			locEncoder.Close()
			// io.incrementBytesWritten(locEncoder.getBytesWritten())

			postingsOffset, err :=
				writePostings(postingsBS, tfEncoder, locEncoder, nil, w, buf)
			if err != nil {
				return nil, err
			}

			if postingsOffset > uint64(0) {
				err = io.builder.Insert([]byte(term), postingsOffset)
				if err != nil {
					return nil, err
				}
			}

			tfEncoder.Reset()
			locEncoder.Reset()
		}

		err = io.builder.Close()
		if err != nil {
			return nil, err
		}

		// record where this dictionary starts
		dictOffsets[fieldID] = uint64(w.Count())

		vellumData := io.builderBuf.Bytes()

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

		// io.incrementBytesWritten(uint64(len(vellumData)))

		// reset vellum for reuse
		io.builderBuf.Reset()

		err = io.builder.Reset(&io.builderBuf)
		if err != nil {
			return nil, err
		}

		// write the field doc values
		// NOTE: doc values continue to use legacy chunk mode
		chunkSize, err := getChunkSize(LegacyChunkMode, 0, 0)
		if err != nil {
			return nil, err
		}

		fdvEncoder := newChunkedContentCoder(chunkSize, uint64(len(io.results)-1), w, false)
		if io.IncludeDocValues[fieldID] {
			for docNum, docTerms := range docTermMap {
				if len(docTerms) > 0 {
					err = fdvEncoder.Add(uint64(docNum), docTerms)
					if err != nil {
						return nil, err
					}
				}
			}
			err = fdvEncoder.Close()
			if err != nil {
				return nil, err
			}

			// io.incrementBytesWritten(fdvEncoder.getBytesWritten())

			fdvOffsetsStart[fieldID] = uint64(w.Count())

			_, err = fdvEncoder.Write()
			if err != nil {
				return nil, err
			}

			fdvOffsetsEnd[fieldID] = uint64(w.Count())
			fdvEncoder.Reset()
		} else {
			fdvOffsetsStart[fieldID] = fieldNotUninverted
			fdvOffsetsEnd[fieldID] = fieldNotUninverted
		}

		fieldStart := w.Count()

		// todo: uvarint these offsets
		err = binary.Write(w, binary.BigEndian, fdvOffsetsStart[fieldID])
		if err != nil {
			return nil, err
		}

		err = binary.Write(w, binary.BigEndian, fdvOffsetsEnd[fieldID])
		if err != nil {
			return nil, err
		}

		err = binary.Write(w, binary.BigEndian, dictOffsets[fieldID])
		if err != nil {
			return nil, err
		}

		io.fieldAddrs[fieldID] = fieldStart
		// must record/update the addrForField info for this field over here?
	}

	// fdvIndexOffset := uint64(w.Count())

	// log.Printf("fdvIndexOffset: %d", fdvIndexOffset)

	return dictOffsets, nil
}

func (io *invertedIndexOpaque) process(field index.Field, fieldID uint16, docNum uint64) {
	if !io.init && io.results != nil {
		io.prepareDicts()
		io.init = true
	}

	if fieldID == ^uint16(0) {
		for fid, tfs := range io.reusableFieldTFs {
			dict := io.Dicts[fid]
			norm := math.Float32frombits(uint32(io.reusableFieldLens[fid]))

			for term, tf := range tfs {
				pid := dict[term] - 1
				bs := io.Postings[pid]
				bs.Add(uint32(docNum))

				io.FreqNorms[pid] = append(io.FreqNorms[pid],
					interimFreqNorm{
						freq:    uint64(tf.Frequency()),
						norm:    norm,
						numLocs: len(tf.Locations),
					})

				if len(tf.Locations) > 0 {
					locs := io.Locs[pid]

					for _, loc := range tf.Locations {
						var locf = uint16(fid)
						if loc.Field != "" {
							locf = uint16(io.getOrDefineField(loc.Field))
						}
						var arrayposs []uint64
						if len(loc.ArrayPositions) > 0 {
							arrayposs = loc.ArrayPositions
						}
						locs = append(locs, interimLoc{
							fieldID:   locf,
							pos:       uint64(loc.Position),
							start:     uint64(loc.Start),
							end:       uint64(loc.End),
							arrayposs: arrayposs,
						})
					}

					io.Locs[pid] = locs
				}
			}
		}

		for i := 0; i < len(io.FieldsInv); i++ { // clear these for reuse
			io.reusableFieldLens[i] = 0
			io.reusableFieldTFs[i] = nil
		}
		return
	}

	if len(io.reusableFieldTFs) == 0 {
		io.reusableFieldTFs = make([]index.TokenFrequencies, len(io.FieldsInv))
	}
	if len(io.reusableFieldLens) == 0 {
		io.reusableFieldLens = make([]int, len(io.FieldsInv))
	}

	io.reusableFieldLens[fieldID] += field.AnalyzedLength()

	existingFreqs := io.reusableFieldTFs[fieldID]
	if existingFreqs != nil {
		existingFreqs.MergeAll(field.Name(), field.AnalyzedTokenFrequencies())
	} else {
		// log.Printf("analyzed token freqs %v", field.AnalyzedTokenFrequencies())
		io.reusableFieldTFs[fieldID] = field.AnalyzedTokenFrequencies()
	}
}

func (i *invertedIndexOpaque) prepareDicts() {
	var pidNext int

	var totTFs int
	var totLocs int
	i.FieldsMap = map[string]uint16{}

	i.getOrDefineField("_id") // _id field is fieldID 0

	for _, result := range i.results {
		result.VisitComposite(func(field index.CompositeField) {
			i.getOrDefineField(field.Name())
		})
		result.VisitFields(func(field index.Field) {
			i.getOrDefineField(field.Name())
		})
	}

	sort.Strings(i.FieldsInv[1:]) // keep _id as first field

	for fieldID, fieldName := range i.FieldsInv {
		i.FieldsMap[fieldName] = uint16(fieldID + 1)
	}

	visitField := func(field index.Field) {
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
	}

	if i.IncludeDocValues == nil {
		if cap(i.IncludeDocValues) >= len(i.FieldsInv) {
			i.IncludeDocValues = i.IncludeDocValues[:len(i.FieldsInv)]
		} else {
			i.IncludeDocValues = make([]bool, len(i.FieldsInv))
		}
	}

	for _, result := range i.results {
		// walk each composite field
		result.VisitComposite(func(field index.CompositeField) {
			visitField(field)
		})

		// walk each field
		result.VisitFields(visitField)
	}

	numPostingsLists := pidNext

	if cap(i.Postings) >= numPostingsLists {
		i.Postings = i.Postings[:numPostingsLists]
	} else {
		postings := make([]*roaring.Bitmap, numPostingsLists)
		copy(postings, i.Postings[:cap(i.Postings)])
		for i := 0; i < numPostingsLists; i++ {
			if postings[i] == nil {
				postings[i] = roaring.New()
			}
		}
		i.Postings = postings
	}

	if cap(i.FreqNorms) >= numPostingsLists {
		i.FreqNorms = i.FreqNorms[:numPostingsLists]
	} else {
		i.FreqNorms = make([][]interimFreqNorm, numPostingsLists)
	}

	if cap(i.freqNormsBacking) >= totTFs {
		i.freqNormsBacking = i.freqNormsBacking[:totTFs]
	} else {
		i.freqNormsBacking = make([]interimFreqNorm, totTFs)
	}

	freqNormsBacking := i.freqNormsBacking
	for pid, numTerms := range i.numTermsPerPostingsList {
		i.FreqNorms[pid] = freqNormsBacking[0:0]
		freqNormsBacking = freqNormsBacking[numTerms:]
	}

	if cap(i.Locs) >= numPostingsLists {
		i.Locs = i.Locs[:numPostingsLists]
	} else {
		i.Locs = make([][]interimLoc, numPostingsLists)
	}

	if cap(i.locsBacking) >= totLocs {
		i.locsBacking = i.locsBacking[:totLocs]
	} else {
		i.locsBacking = make([]interimLoc, totLocs)
	}

	locsBacking := i.locsBacking
	for pid, numLocs := range i.numLocsPerPostingsList {
		i.Locs[pid] = locsBacking[0:0]
		locsBacking = locsBacking[numLocs:]
	}

	for _, dict := range i.DictKeys {
		sort.Strings(dict)
	}
}

func (i *invertedIndexSection) getInvertedIndexOpaque(opaque map[int]resetable) *invertedIndexOpaque {
	if _, ok := opaque[sectionInvertedIndex]; !ok {
		opaque[sectionInvertedIndex] = i.InitOpaque(nil)
	}
	return opaque[sectionInvertedIndex].(*invertedIndexOpaque)
}

// revisit this function's purpose etc.
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

func (i *invertedIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &invertedIndexOpaque{
		fieldAddrs: map[int]int{},
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

type invertedIndexOpaque struct {
	results []index.Document

	chunkMode uint32

	FieldsInv []string

	FieldsMap map[string]uint16

	// indicates whethere the following structs are initialized
	init bool

	Dicts []map[string]uint64

	// Terms for each field, where terms are sorted ascending
	//  field id -> []term
	DictKeys [][]string

	// Fields whose IncludeDocValues is true
	//  field id -> bool
	IncludeDocValues []bool

	// postings id -> bitmap of docNums
	Postings []*roaring.Bitmap

	// postings id -> freq/norm's, one for each docNum in postings
	FreqNorms        [][]interimFreqNorm
	freqNormsBacking []interimFreqNorm

	// postings id -> locs, one for each freq
	Locs        [][]interimLoc
	locsBacking []interimLoc

	numTermsPerPostingsList []int // key is postings list id
	numLocsPerPostingsList  []int // key is postings list id

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
	io.chunkMode = 0
	io.FieldsMap = nil
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
	for _, idn := range io.Postings {
		idn.Clear()
	}
	io.Postings = io.Postings[:0]
	io.FreqNorms = io.FreqNorms[:0]
	for i := range io.freqNormsBacking {
		io.freqNormsBacking[i] = interimFreqNorm{}
	}
	io.freqNormsBacking = io.freqNormsBacking[:0]
	io.Locs = io.Locs[:0]
	for i := range io.locsBacking {
		io.locsBacking[i] = interimLoc{}
	}
	io.locsBacking = io.locsBacking[:0]
	io.numTermsPerPostingsList = io.numTermsPerPostingsList[:0]
	io.numLocsPerPostingsList = io.numLocsPerPostingsList[:0]
	io.builderBuf.Reset()
	if io.builder != nil {
		err = io.builder.Reset(&io.builderBuf)
	}

	io.reusableFieldLens = io.reusableFieldLens[:0]
	io.reusableFieldTFs = io.reusableFieldTFs[:0]

	io.tmp0 = io.tmp0[:0]
	io.fieldsSame = false
	io.numDocs = 0

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
	case "numDocs":
		i.numDocs = val.(uint64)
	}
}
