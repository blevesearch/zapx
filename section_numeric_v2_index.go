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
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// numericV2FormatVersion is written at the head of every field's payload so a
// future change to the layout can be made additively.
const numericV2FormatVersion uint64 = 1

func init() {
	registerSegmentSection(SectionNumericV2Index, &numericV2IndexSection{})
	invertedTextIndexSectionExclusionChecks = append(
		invertedTextIndexSectionExclusionChecks,
		func(f index.Field) bool {
			_, ok := f.(index.NumericV2Field)
			return ok
		})
}

type numericV2IndexSection struct {
}

func (n *numericV2IndexSection) Process(opaque map[int]resetable, docNum uint32,
	f index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}
	if nf, ok := f.(index.NumericV2Field); ok {
		no := n.getNumericV2IndexOpaque(opaque)
		no.process(nf, f.Options().IncludeDocValues(), fieldID, docNum)
	}
}

func (n *numericV2IndexSection) Persist(opaque map[int]resetable, w *FileWriter) error {
	no := n.getNumericV2IndexOpaque(opaque)
	return no.persist(w)
}

func (n *numericV2IndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	no := n.getNumericV2IndexOpaque(opaque)
	return no.fieldAddrs[uint16(fieldID)]
}

func (n *numericV2IndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &numericV2IndexSectionOpaque{
		fieldAddrs:   make(map[uint16]int),
		indexContent: make(map[uint16]*numericIndexContent),
	}
	for k, v := range args {
		rv.Set(k, v)
	}
	return rv
}

func (n *numericV2IndexSection) getNumericV2IndexOpaque(
	opaque map[int]resetable) *numericV2IndexSectionOpaque {
	if _, ok := opaque[SectionNumericV2Index]; !ok {
		opaque[SectionNumericV2Index] = n.InitOpaque(nil)
	}
	return opaque[SectionNumericV2Index].(*numericV2IndexSectionOpaque)
}

// -----------------------------------------------------------------------------

type numericV2IndexSectionOpaque struct {
	// indexContent holds the numeric index content for each field ID
	indexContent map[uint16]*numericIndexContent
	// fieldAddrs holds the starting address of each field's index content in the file
	fieldAddrs map[uint16]int
	// fieldsOptions holds the indexing options for each field name
	fieldsOptions map[string]index.FieldIndexingOptions
	// fieldsInv maps a field ID to its name, which the persist path needs in
	// order to look up the field's options
	fieldsInv []string
	// numDocs is the total number of documents in the segment being written,
	// needed to size the doc value chunk table.
	numDocs uint64

	bytesWritten uint64

	// temporary buffer for reuse
	tmp []byte

	init bool
}

func (n *numericV2IndexSectionOpaque) Reset() error {
	n.tmp = n.tmp[:0]
	n.init = false
	n.numDocs = 0
	clear(n.indexContent)
	clear(n.fieldAddrs)
	// drop the references rather than clearing: both are owned by the caller
	n.fieldsOptions = nil
	n.fieldsInv = nil
	return nil
}

func (n *numericV2IndexSectionOpaque) Set(key string, value interface{}) {
	switch key {
	case "fieldsOptions":
		n.fieldsOptions = value.(map[string]index.FieldIndexingOptions)
	case "fieldsInv":
		n.fieldsInv = value.([]string)
	case "results":
		// the persist path: one result per document
		n.numDocs = uint64(len(value.([]index.Document)))
	case "numDocs":
		// the merge path: the merged segment's document count
		n.numDocs = value.(uint64)
	}
}

func (n *numericV2IndexSectionOpaque) grabBuf(size int) []byte {
	buf := n.tmp
	if cap(buf) < size {
		buf = make([]byte, size)
		n.tmp = buf
	}
	return buf[:size]
}

func (n *numericV2IndexSectionOpaque) alloc() {
	n.indexContent = make(map[uint16]*numericIndexContent)
	// The other fields will already be initialized during opaque creation
}

func (n *numericV2IndexSectionOpaque) process(f index.NumericV2Field,
	includeDocValues bool, fieldID uint16, docNum uint32) {
	if !n.init {
		n.alloc()
		n.init = true
	}

	indexContent, ok := n.indexContent[fieldID]
	if !ok {
		indexContent = &numericIndexContent{}
		n.indexContent[fieldID] = indexContent
	}

	indexContent.process(f, includeDocValues, docNum)
}

func (n *numericV2IndexSectionOpaque) incrementBytesWritten(val uint64) {
	atomic.AddUint64(&n.bytesWritten, val)
}

func (n *numericV2IndexSectionOpaque) BytesWritten() uint64 {
	return atomic.LoadUint64(&n.bytesWritten)
}

// -----------------------------------------------------------------------------

// numericIndexContent holds one field's number_v2 content.
//
// During Process the entries accumulate in document order, because
// processDocuments visits documents by ascending doc number. persist relies on
// that: it emits the doc value block first, which requires document order, and
// only then sorts values/docNums by value for the search arrays.
type numericIndexContent struct {
	// values holds each indexed value, sortable-encoded. Sorted ascending
	// once persist or merge has finished with it.
	values []uint64
	// docNums holds the segment document number owning each value, parallel
	// to values.
	docNums []uint32

	// dvTerms is the concatenation of every entry's prefix-coded doc value
	// term, each followed by index.DocValueTermSeparator, in document order.
	// Empty when the field does not include doc values.
	dvTerms []byte
	// dvDocNums holds the distinct document numbers that contributed to
	// dvTerms, ascending. dvEnds[i] is the end offset in dvTerms of
	// dvDocNums[i]'s run; the run starts at dvEnds[i-1], or 0 for i == 0.
	dvDocNums []uint32
	dvEnds    []uint32

	init bool
}

func (nc *numericIndexContent) alloc() {
	nc.values = make([]uint64, 0)
	nc.docNums = make([]uint32, 0)
}

func (nc *numericIndexContent) process(f index.NumericV2Field,
	includeDocValues bool, docNum uint32) {
	if !nc.init {
		nc.alloc()
		nc.init = true
	}

	nc.values = append(nc.values, f.SortableValue())
	nc.docNums = append(nc.docNums, docNum)

	if !includeDocValues {
		return
	}

	// a document with a multi-valued field contributes several entries in a
	// row, which share one doc value run
	if len(nc.dvDocNums) == 0 || nc.dvDocNums[len(nc.dvDocNums)-1] != docNum {
		nc.dvDocNums = append(nc.dvDocNums, docNum)
		nc.dvEnds = append(nc.dvEnds, 0)
	}

	nc.dvTerms = append(nc.dvTerms, f.DocValueTerm()...)
	nc.dvTerms = append(nc.dvTerms, index.DocValueTermSeparator)
	nc.dvEnds[len(nc.dvEnds)-1] = uint32(len(nc.dvTerms))
}

// dvRun returns the doc value bytes for the i'th distinct document.
func (nc *numericIndexContent) dvRun(i int) []byte {
	var start uint32
	if i > 0 {
		start = nc.dvEnds[i-1]
	}
	return nc.dvTerms[start:nc.dvEnds[i]]
}

// -----------------------------------------------------------------------------

func (n *numericV2IndexSectionOpaque) persist(w *FileWriter) error {
	for fieldID, content := range n.indexContent {
		if len(content.values) == 0 {
			continue
		}

		// The doc value block is written before the field record so that its
		// offsets are known, and before the arrays below are sorted, because
		// doc values must be emitted in ascending document order.
		dvStart, dvEnd := uint64(fieldNotUninverted), uint64(fieldNotUninverted)
		if len(content.dvDocNums) > 0 {
			encoder, err := n.newDocValueEncoder(n.optionsForFieldID(fieldID), w)
			if err != nil {
				return err
			}
			for i, docNum := range content.dvDocNums {
				if err = encoder.Add(uint64(docNum), content.dvRun(i)); err != nil {
					return err
				}
			}
			if dvStart, dvEnd, err = n.flushDocValues(encoder, w); err != nil {
				return err
			}
		}

		fieldStart := w.Count()
		if err := n.writeFieldRecordHeader(dvStart, dvEnd, w); err != nil {
			return err
		}

		// sort by value for the search arrays; the doc values written above
		// keep their document ordering
		content.values, content.docNums =
			sortArrayPair(content.values, content.docNums)

		if err := n.writeIndexContent(content, w); err != nil {
			return err
		}

		n.incrementBytesWritten(uint64(w.Count() - fieldStart))
		n.fieldAddrs[fieldID] = fieldStart
	}

	return nil
}

// newDocValueEncoder builds a content coder for one field's doc values, honouring
// the field's chunking and compression options exactly as the inverted text
// section does.
func (n *numericV2IndexSectionOpaque) newDocValueEncoder(
	opts index.FieldIndexingOptions, w *FileWriter) (*chunkedContentCoder, error) {
	// NOTE: doc values continue to use legacy chunk mode
	chunkSize, err := getChunkSize(LegacyChunkMode, 0, 0)
	if err != nil {
		return nil, err
	}
	if opts.SkipDVChunking() {
		chunkSize = 1
	}
	if n.numDocs == 0 {
		return nil, fmt.Errorf("number_v2: cannot encode doc values for an " +
			"empty segment")
	}
	return newChunkedContentCoder(chunkSize, n.numDocs-1, w, false,
		opts.SkipDVCompression()), nil
}

// flushDocValues closes the encoder, writes its block, and returns the block's
// start and end offsets in the file.
func (n *numericV2IndexSectionOpaque) flushDocValues(encoder *chunkedContentCoder,
	w *FileWriter) (uint64, uint64, error) {
	if err := encoder.Close(); err != nil {
		return 0, 0, err
	}
	n.incrementBytesWritten(encoder.getBytesWritten())

	start := uint64(w.Count())
	if _, err := encoder.Write(); err != nil {
		return 0, 0, err
	}
	return start, uint64(w.Count()), nil
}

// writeFieldRecordHeader writes the doc value offset pair that every section's
// field record begins with.
func (n *numericV2IndexSectionOpaque) writeFieldRecordHeader(dvStart, dvEnd uint64,
	w *FileWriter) error {
	tempBuf := n.grabBuf(binary.MaxVarintLen64)
	for _, v := range []uint64{dvStart, dvEnd} {
		sz := binary.PutUvarint(tempBuf, v)
		if _, err := w.Write(tempBuf[:sz]); err != nil {
			return err
		}
	}
	return nil
}

func (n *numericV2IndexSectionOpaque) writeIndexContent(content *numericIndexContent,
	w *FileWriter) error {
	tempBuf := n.grabBuf(binary.MaxVarintLen64)

	// Write the format version
	sz := binary.PutUvarint(tempBuf, numericV2FormatVersion)
	if _, err := w.Write(tempBuf[:sz]); err != nil {
		return err
	}

	// Write the number of entries
	sz = binary.PutUvarint(tempBuf, uint64(len(content.values)))
	if _, err := w.Write(tempBuf[:sz]); err != nil {
		return err
	}

	// Write the values, ascending
	if _, err := w.WriteUint64Array(content.values); err != nil {
		return err
	}

	// Write the parallel segment doc numbers
	if _, err := w.WriteUint32Array(content.docNums); err != nil {
		return err
	}

	return nil
}

// loadNumericIndexContent decodes a field's search arrays from mem. It does not
// read the doc value block, which is reached through the field record's offsets
// instead.
func loadNumericIndexContent(r *FileReader, mem []byte) (*numericIndexContent, error) {
	var pos uint64

	formatVersion, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if formatVersion != numericV2FormatVersion {
		return nil, fmt.Errorf("unsupported number_v2 format version %d",
			formatVersion)
	}

	numEntries, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if numEntries == 0 {
		return nil, fmt.Errorf("no numeric entries found")
	}

	values, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	docNums, _, _, err := r.ReadUint32Array(mem[pos:])
	if err != nil {
		return nil, err
	}

	if len(values) != len(docNums) {
		return nil, fmt.Errorf("number_v2 arrays disagree in length: "+
			"%d values, %d doc numbers", len(values), len(docNums))
	}

	return &numericIndexContent{
		values:  values,
		docNums: docNums,
	}, nil
}

// -----------------------------------------------------------------------------

// numericIndexInfo pairs one segment's content with the doc number remapping
// that applies to it.
type numericIndexInfo struct {
	segment *SegmentBase
	content *numericIndexContent

	// remap maps a segment document number to its number in the merged
	// segment, with pairDropped marking deleted documents.
	remap []uint32
}

func (n *numericV2IndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64, w *FileWriter,
	closeCh chan struct{}) error {
	no := n.getNumericV2IndexOpaque(opaque)
	indexInfos := make([]*numericIndexInfo, 0, len(segments))

	for fieldID, fieldName := range fieldsInv {
		// Skip fields that are not indexed
		if !no.fieldsOptions[fieldName].IsIndexed() {
			continue
		}

		indexInfos = indexInfos[:0]

		for segI, sb := range segments {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}
			// Skip if the field is not present in the segment
			if _, ok := sb.fieldsMap[fieldName]; !ok {
				continue
			}

			// Obtain the field record position
			pos := int(sb.fieldsSectionsMap[sb.fieldsMap[fieldName]-1][SectionNumericV2Index])
			if pos == 0 {
				continue
			}

			// skip the doc value offsets; the doc values are merged separately
			// below, through the segment's own doc value reader
			for i := 0; i < 2; i++ {
				_, sz := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += sz
			}

			content, err := loadNumericIndexContent(sb.fileReader, sb.mem[pos:])
			if err != nil {
				return err
			}

			indexInfos = append(indexInfos, &numericIndexInfo{
				segment: sb,
				content: content,
				remap:   buildDocNumRemap(newDocNumsIn[segI]),
			})
		}

		if len(indexInfos) == 0 {
			continue
		}

		// Merge in memory first: a field whose every document was dropped must
		// not leave an orphan doc value block behind in the file.
		mergedContent := no.mergeIndexContents(indexInfos)
		if mergedContent == nil {
			continue
		}

		// Doc values next, for the same reason as in persist: the offsets have
		// to be known before the field record is written.
		dvStart, dvEnd := uint64(fieldNotUninverted), uint64(fieldNotUninverted)
		if no.fieldsOptions[fieldName].IncludeDocValues() {
			var err error
			dvStart, dvEnd, err = no.mergeDocValues(fieldName, indexInfos, w, closeCh)
			if err != nil {
				return err
			}
		}

		fieldStart := w.Count()
		if err := no.writeFieldRecordHeader(dvStart, dvEnd, w); err != nil {
			return err
		}
		if err := no.writeIndexContent(mergedContent, w); err != nil {
			return err
		}

		no.incrementBytesWritten(uint64(w.Count() - fieldStart))
		no.fieldAddrs[uint16(fieldID)] = fieldStart
	}

	return nil
}

// buildDocNumRemap narrows the merge's newDocNums for one segment into the
// uint32 space the pair cursors work in, marking dropped documents.
func buildDocNumRemap(newDocNums []uint64) []uint32 {
	rv := make([]uint32, len(newDocNums))
	for i, newDocNum := range newDocNums {
		if newDocNum == docDropped {
			rv[i] = pairDropped
			continue
		}
		rv[i] = uint32(newDocNum)
	}
	return rv
}

// mergeIndexContents k-way merges the per-segment value runs, which are each
// already sorted, into a single ascending run with doc numbers remapped into
// the merged segment's space. Returns nil when no entry survived.
func (n *numericV2IndexSectionOpaque) mergeIndexContents(
	indexInfos []*numericIndexInfo) *numericIndexContent {
	var totalEntries int
	for _, info := range indexInfos {
		totalEntries += len(info.content.values)
	}
	if totalEntries == 0 {
		return nil
	}

	cursors := make([]*sortedPairCursor, 0, len(indexInfos))
	for _, info := range indexInfos {
		cursors = append(cursors, &sortedPairCursor{
			keys:     info.content.values,
			payloads: info.content.docNums,
			remap:    info.remap,
		})
	}

	values, docNums := kWayMergePairs(cursors,
		make([]uint64, 0, totalEntries), make([]uint32, 0, totalEntries))
	if len(values) == 0 {
		return nil
	}

	return &numericIndexContent{values: values, docNums: docNums}
}

// mergeDocValues concatenates the segments' existing doc value blocks into a new
// one, remapping document numbers and dropping deleted documents.
//
// Correctness here rests on an ordering invariant that chunkedContentCoder.Add
// requires but does not check: doc numbers must strictly increase.
// mergeStoredAndRemap assigns merged doc numbers by walking segments in order
// and doc numbers ascending within each segment, and iterateAllDocValues walks
// chunks in order, so visiting the segments in the same order satisfies it.
func (n *numericV2IndexSectionOpaque) mergeDocValues(fieldName string,
	indexInfos []*numericIndexInfo, w *FileWriter, closeCh chan struct{}) (uint64, uint64, error) {
	encoder, err := n.newDocValueEncoder(n.fieldsOptions[fieldName], w)
	if err != nil {
		return 0, 0, err
	}

	var found bool
	var dvIterClone *docValueReader
	for _, info := range indexInfos {
		if isClosed(closeCh) {
			return 0, 0, seg.ErrClosed
		}

		sb := info.segment
		fieldIDPlus1, ok := sb.fieldsMap[fieldName]
		if !ok {
			continue
		}
		secDvReaders := sb.fieldDvReaders[SectionNumericV2Index]
		if secDvReaders == nil {
			continue
		}
		dvIter := secDvReaders[fieldIDPlus1-1]
		if dvIter == nil {
			continue
		}
		found = true

		dvIterClone = dvIter.cloneInto(dvIterClone)
		err = dvIterClone.iterateAllDocValues(sb, func(docNum uint64, terms []byte) error {
			newDocNum := info.remap[docNum]
			if newDocNum == pairDropped {
				return nil
			}
			return encoder.Add(uint64(newDocNum), terms)
		})
		if err != nil {
			return 0, 0, err
		}
	}

	if !found {
		return fieldNotUninverted, fieldNotUninverted, nil
	}

	return n.flushDocValues(encoder, w)
}

// optionsForFieldID resolves a field's indexing options on the persist path,
// where the opaque is keyed by field ID rather than by name.
func (n *numericV2IndexSectionOpaque) optionsForFieldID(fieldID uint16) index.FieldIndexingOptions {
	if int(fieldID) < len(n.fieldsInv) {
		return n.fieldsOptions[n.fieldsInv[fieldID]]
	}
	return 0
}
