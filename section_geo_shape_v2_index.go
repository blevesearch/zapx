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
	"sort"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

func init() {
	registerSegmentSection(SectionGeoShapeV2Index, &geoShapeV2IndexSection{})
	invertedTextIndexSectionExclusionChecks = append(
		invertedTextIndexSectionExclusionChecks,
		func(f index.Field) bool {
			_, ok := f.(index.GeoShapeV2Field)
			return ok
		})
}

type geoShapeV2IndexSection struct {
}

func (g *geoShapeV2IndexSection) Process(opaque map[int]resetable, docNum uint32,
	f index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}
	if gsf, ok := f.(index.GeoShapeV2Field); ok {
		gs := g.getGeoShapeV2IndexOpaque(opaque)
		gs.process(gsf, fieldID, docNum)
	}
}

func (g *geoShapeV2IndexSection) Persist(opaque map[int]resetable, w *FileWriter) error {
	gs := g.getGeoShapeV2IndexOpaque(opaque)
	return gs.persist(w)
}

func (g *geoShapeV2IndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	gs := g.getGeoShapeV2IndexOpaque(opaque)
	return gs.fieldAddrs[uint16(fieldID)]
}

type geoIndexInfo struct {
	content *geoIndexContent

	// newDocNums maps a document's old segment doc number (the index) to its
	// doc number in the merged segment, with docDropped marking deleted docs.
	newDocNums []uint64
}

func (g *geoShapeV2IndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64, w *FileWriter,
	closeCh chan struct{}) error {

	gs := g.getGeoShapeV2IndexOpaque(opaque)
	indexInfos := make([]*geoIndexInfo, 0, len(segments))

	for fieldID, fieldName := range fieldsInv {
		// Skip fields that are not indexed
		if !gs.fieldsOptions[fieldName].IsIndexed() {
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

			// Obtain the field start position
			pos := int(sb.fieldsSectionsMap[sb.fieldsMap[fieldName]-1][SectionGeoShapeV2Index])
			if pos == 0 {
				continue
			}

			// skip doc values, as we don't support them for geo shapes
			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			// Load the geo index content for the field from the segment
			content, err := loadGeoIndexContent(sb.fileReader, sb.mem[pos:])
			if err != nil {
				return err
			}

			// Append the content and the corresponding doc number remapping for this segment
			indexInfos = append(indexInfos, &geoIndexInfo{
				content:    content,
				newDocNums: newDocNumsIn[segI],
			})
		}

		// Merge the index contents from all segments for this field
		indexContent, err := gs.mergeIndexContents(indexInfos)
		if err != nil {
			return err
		}

		// If the merged index content is nil, it means there are
		// no valid documents for this field across all segments,
		// so we skip writing it.
		if indexContent == nil {
			continue
		}

		// record the starting position of this field's index content
		fieldStart := w.Count()
		tempBuf := gs.grabBuf(binary.MaxVarintLen64)
		// Write two varints for indicating no doc values
		n := binary.PutUvarint(tempBuf, fieldNotUninverted)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		n = binary.PutUvarint(tempBuf, fieldNotUninverted)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}

		// Write the merged index content for this field to the file
		err = gs.writeIndexContent(indexContent, w)
		if err != nil {
			return err
		}

		gs.incrementBytesWritten(uint64(w.Count() - fieldStart))
		gs.fieldAddrs[uint16(fieldID)] = fieldStart
	}

	return nil
}

func (g *geoShapeV2IndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &geoShapeV2IndexSectionOpaque{
		fieldAddrs:   make(map[uint16]int),
		indexContent: make(map[uint16]*geoIndexContent),
	}
	for k, v := range args {
		rv.Set(k, v)
	}
	return rv
}

func (g *geoShapeV2IndexSection) getGeoShapeV2IndexOpaque(
	opaque map[int]resetable) *geoShapeV2IndexSectionOpaque {
	if _, ok := opaque[SectionGeoShapeV2Index]; !ok {
		opaque[SectionGeoShapeV2Index] = g.InitOpaque(nil)
	}
	return opaque[SectionGeoShapeV2Index].(*geoShapeV2IndexSectionOpaque)
}

type geoShapeV2IndexSectionOpaque struct {
	results []index.Document

	// indexContent holds the geo index content for each field ID
	indexContent map[uint16]*geoIndexContent
	// fieldAddrs holds the starting address of each field's index content in the file
	fieldAddrs map[uint16]int
	// fieldsOptions holds the indexing options for each field name
	fieldsOptions map[string]index.FieldIndexingOptions

	bytesWritten uint64

	// temporary buffer for reuse
	tmp []byte

	init bool
}

func (g *geoShapeV2IndexSectionOpaque) Reset() error {
	g.results = nil
	g.tmp = g.tmp[:0]
	g.init = false
	clear(g.indexContent)
	clear(g.fieldAddrs)
	clear(g.fieldsOptions)
	return nil
}

func (g *geoShapeV2IndexSectionOpaque) grabBuf(size int) []byte {
	buf := g.tmp
	if cap(buf) < size {
		buf = make([]byte, size)
		g.tmp = buf
	}
	return buf[:size]
}

func (g *geoShapeV2IndexSectionOpaque) Set(key string, value interface{}) {
	switch key {
	case "results":
		g.results = value.([]index.Document)
	case "fieldsOptions":
		g.fieldsOptions = value.(map[string]index.FieldIndexingOptions)
	}
}

func (g *geoShapeV2IndexSectionOpaque) alloc() {
	g.indexContent = make(map[uint16]*geoIndexContent)
	// The other fields will already be initialized during opaque creation
}

func (g *geoShapeV2IndexSectionOpaque) process(f index.GeoShapeV2Field, fieldID uint16,
	docNum uint32) {
	if !g.init {
		g.alloc()
		g.init = true
	}

	indexContent, ok := g.indexContent[fieldID]
	if !ok {
		indexContent = &geoIndexContent{}
		g.indexContent[fieldID] = indexContent
	}

	indexContent.process(f, docNum)
}

// geoIndexContent holds the geo index content for a specific field ID
type geoIndexContent struct {
	innerCells  []uint64
	innerDocIDs []uint64

	crossCells  []uint64
	crossDocIDs []uint64

	docNums        []uint64
	docScoresInner []uint64
	docScoresCross []uint64

	boundingBoxes [][]byte
	shapes        [][]byte

	init bool
}

func (g *geoIndexContent) alloc() {
	g.innerCells = make([]uint64, 0)
	g.innerDocIDs = make([]uint64, 0)
	g.crossCells = make([]uint64, 0)
	g.crossDocIDs = make([]uint64, 0)

	g.docNums = make([]uint64, 0)
	g.docScoresInner = make([]uint64, 0)
	g.docScoresCross = make([]uint64, 0)

	g.boundingBoxes = make([][]byte, 0)
	g.shapes = make([][]byte, 0)
}

func (g *geoIndexContent) process(f index.GeoShapeV2Field, docNum uint32) {
	if !g.init {
		g.alloc()
		g.init = true
	}

	innerCells := f.InnerCells()
	crossCells := f.CrossCells()

	docID := uint64(len(g.docNums))
	g.docNums = append(g.docNums, uint64(docNum))

	// Append inner cells along with their corresponding doc IDs
	g.innerCells = append(g.innerCells, innerCells...)
	for _ = range innerCells {
		g.innerDocIDs = append(g.innerDocIDs, docID)
	}

	// Append cross cells along with their corresponding doc IDs
	g.crossCells = append(g.crossCells, crossCells...)
	for _ = range crossCells {
		g.crossDocIDs = append(g.crossDocIDs, docID)
	}

	// Append bounding box bytes, shape bytes and the document score
	g.boundingBoxes = append(g.boundingBoxes, f.EncodedBoundingBox())
	g.shapes = append(g.shapes, f.EncodedShape())
	innerScore, crossScore := f.Scores()
	g.docScoresInner = append(g.docScoresInner, innerScore)
	g.docScoresCross = append(g.docScoresCross, crossScore)
}

func (g *geoShapeV2IndexSectionOpaque) persist(w *FileWriter) error {

	tempBuf := g.grabBuf(binary.MaxVarintLen64)
	for fieldID, content := range g.indexContent {
		// Record the starting position of this field's index content
		fieldStart := w.Count()

		// Write two varints for indicating no doc values
		n := binary.PutUvarint(tempBuf, fieldNotUninverted)
		_, err := w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		n = binary.PutUvarint(tempBuf, fieldNotUninverted)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}

		// Write the index content for this field to the file
		err = g.writeIndexContent(content, w)
		if err != nil {
			return err
		}

		g.incrementBytesWritten(uint64(w.Count() - fieldStart))
		g.fieldAddrs[fieldID] = fieldStart
	}

	return nil
}

func (g *geoShapeV2IndexSectionOpaque) writeIndexContent(content *geoIndexContent, w *FileWriter) error {
	tempBuf := g.grabBuf(binary.MaxVarintLen64)

	// Write docNums
	numDocs := uint64(len(content.docNums))
	n := binary.PutUvarint(tempBuf, numDocs)
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	// Write Doc ID to Doc Num mapping
	_, err = w.WriteUint64Array(content.docNums)
	if err != nil {
		return err
	}

	// Write the Document Scores Inner
	_, err = w.WriteUint64Array(content.docScoresInner)
	if err != nil {
		return err
	}

	// Write the Document Scores Cross
	_, err = w.WriteUint64Array(content.docScoresCross)
	if err != nil {
		return err
	}

	// Sort Inner Cells and Doc IDs in tandem
	inner, innerDocIDs := sortArrayPair(content.innerCells, content.innerDocIDs)

	// Write Inner Cells
	_, err = w.WriteUint64Array(inner)
	if err != nil {
		return err
	}

	// Write Inner Cell Doc IDs
	_, err = w.WriteUint64Array(innerDocIDs)
	if err != nil {
		return err
	}

	// Sort Cross Cells and Doc IDs in tandem
	cross, crossDocIDs := sortArrayPair(content.crossCells, content.crossDocIDs)

	// Write Cross Cells
	_, err = w.WriteUint64Array(cross)
	if err != nil {
		return err
	}

	// Write Cross Cell Doc IDs
	_, err = w.WriteUint64Array(crossDocIDs)
	if err != nil {
		return err
	}

	// Write Bounding Boxes and Offsets
	_, err = w.WriteArrayWithOffsets(content.boundingBoxes)
	if err != nil {
		return err
	}

	// Write Shapes and Offsets
	_, err = w.WriteArrayWithOffsets(content.shapes)
	if err != nil {
		return err
	}

	return nil
}

// loadGeoIndexContent decodes a field's complete geo index content from mem.
func loadGeoIndexContent(r *FileReader, mem []byte) (*geoIndexContent, error) {
	var pos uint64

	// Load Num Docs
	numDocs, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if numDocs == 0 {
		return nil, fmt.Errorf("no geo docs found")
	}

	// Load Doc ID to Doc Num mapping
	docNums, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load the Document Scores Inner
	docScoresInner, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load the Document Scores Cross
	docScoresCross, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load Inner Cells
	innerCells, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load Inner Cell Doc IDs
	innerDocIDs, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load Cross Cells
	crossCells, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load Cross Cell Doc IDs
	crossDocIDs, _, shift, err := r.ReadUint64Array(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load BBox Metadata
	bBoxes, shift, err := r.ReadArrayWithOffsets(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	// Load Shape Metadata
	shapes, shift, err := r.ReadArrayWithOffsets(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	return &geoIndexContent{
		docNums:        docNums,
		docScoresInner: docScoresInner,
		docScoresCross: docScoresCross,
		innerCells:     innerCells,
		innerDocIDs:    innerDocIDs,
		crossCells:     crossCells,
		crossDocIDs:    crossDocIDs,
		boundingBoxes:  bBoxes,
		shapes:         shapes,
	}, nil
}

// mergeIndexContents combines the geo index contents of multiple segments into
// a single geoIndexContent, dropping deleted documents and remapping doc numbers.
func (g *geoShapeV2IndexSectionOpaque) mergeIndexContents(indexInfos []*geoIndexInfo) (*geoIndexContent, error) {
	mergedContent := &geoIndexContent{}
	mergedContent.alloc()

	var numDocs uint64
	newGeoDocIDs := make(map[uint64]uint64)

	// Assign geo docIDs in the merged segment and build a mapping from
	// merged-segment doc number to merged geo docID
	for _, indexInfo := range indexInfos {
		for _, oldDocNum := range indexInfo.content.docNums {
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}

			newGeoDocID := numDocs
			numDocs++
			newGeoDocIDs[newDocNum] = newGeoDocID
			mergedContent.docNums = append(mergedContent.docNums, newDocNum)
		}
	}

	if numDocs == 0 {
		return nil, nil
	}

	for _, indexInfo := range indexInfos {
		// Merge inner cells and their corresponding geo docIDs
		for i, cell := range indexInfo.content.innerCells {
			// Calculate the merged geo docID for the current cell's document
			geoDocID := indexInfo.content.innerDocIDs[i]
			oldDocNum := indexInfo.content.docNums[geoDocID]
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}
			newGeoDocID := newGeoDocIDs[newDocNum]
			mergedContent.innerCells = append(mergedContent.innerCells, cell)
			mergedContent.innerDocIDs = append(mergedContent.innerDocIDs, newGeoDocID)
		}
		// Merge cross cells and their corresponding geo docIDs
		for i, cell := range indexInfo.content.crossCells {
			// Calculate the merged geo docID for the current cell's document
			geoDocID := indexInfo.content.crossDocIDs[i]
			oldDocNum := indexInfo.content.docNums[geoDocID]
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}
			newGeoDocID := newGeoDocIDs[newDocNum]
			mergedContent.crossCells = append(mergedContent.crossCells, cell)
			mergedContent.crossDocIDs = append(mergedContent.crossDocIDs, newGeoDocID)
		}
		// Merge bounding boxes
		for i, bbox := range indexInfo.content.boundingBoxes {
			geoDocID := uint64(i)
			oldDocNum := indexInfo.content.docNums[geoDocID]
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.boundingBoxes = append(mergedContent.boundingBoxes, bbox)
		}
		// Merge shapes
		for i, shape := range indexInfo.content.shapes {
			geoDocID := uint64(i)
			oldDocNum := indexInfo.content.docNums[geoDocID]
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.shapes = append(mergedContent.shapes, shape)
		}
		// Merge document scores inner
		for i, score := range indexInfo.content.docScoresInner {
			geoDocID := uint64(i)
			oldDocNum := indexInfo.content.docNums[geoDocID]
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.docScoresInner = append(mergedContent.docScoresInner, score)
		}
		// Merge document scores cross
		for i, score := range indexInfo.content.docScoresCross {
			geoDocID := uint64(i)
			oldDocNum := indexInfo.content.docNums[geoDocID]
			newDocNum := indexInfo.newDocNums[oldDocNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.docScoresCross = append(mergedContent.docScoresCross, score)
		}
	}

	// Sort the merged inner and cross cells along with their corresponding doc IDs
	mergedContent.innerCells, mergedContent.innerDocIDs = sortArrayPair(mergedContent.innerCells, mergedContent.innerDocIDs)
	mergedContent.crossCells, mergedContent.crossDocIDs = sortArrayPair(mergedContent.crossCells, mergedContent.crossDocIDs)

	return mergedContent, nil
}

func (g *geoShapeV2IndexSectionOpaque) incrementBytesWritten(val uint64) {
	atomic.AddUint64(&g.bytesWritten, val)
}

func (g *geoShapeV2IndexSectionOpaque) BytesWritten() uint64 {
	return atomic.LoadUint64(&g.bytesWritten)
}

// arrayPair holds references to both slices to swap and sort them in tandem
type arrayPair struct {
	primary   []uint64
	secondary []uint64
}

func (a arrayPair) Len() int {
	return len(a.primary)
}

func (a arrayPair) Less(i, j int) bool {
	return a.primary[i] < a.primary[j]
}

func (a arrayPair) Swap(i, j int) {
	a.primary[i], a.primary[j] = a.primary[j], a.primary[i]
	a.secondary[i], a.secondary[j] = a.secondary[j], a.secondary[i]
}

func sortArrayPair(primary []uint64, secondary []uint64) ([]uint64, []uint64) {
	// Protect against mismatched slice lengths
	if len(primary) != len(secondary) {
		panic("slices must be of equal length")
	}

	sort.Sort(arrayPair{primary: primary, secondary: secondary})
	return primary, secondary
}
