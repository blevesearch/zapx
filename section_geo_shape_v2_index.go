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

	docIDMap []uint64
}

func (g *geoShapeV2IndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64, w *FileWriter,
	closeCh chan struct{}) error {

	gs := g.getGeoShapeV2IndexOpaque(opaque)
	indexInfos := make([]*geoIndexInfo, 0, len(segments))

	for fieldID, fieldName := range fieldsInv {

		if !gs.fieldsOptions[fieldName].IsIndexed() {
			continue
		}

		indexInfos = indexInfos[:0]

		for segI, sb := range segments {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}

			if _, ok := sb.fieldsMap[fieldName]; !ok {
				continue
			}

			pos := int(sb.fieldsSectionsMap[sb.fieldsMap[fieldName]-1][SectionGeoShapeV2Index])
			if pos == 0 {
				continue
			}

			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			indexInfo := &geoIndexInfo{
				docIDMap: newDocNumsIn[segI],
			}
			var err error

			indexInfo.content, err = loadGeoIndexContent(sb.fileReader, sb.mem[pos:])
			if err != nil {
				return err
			}

			indexInfos = append(indexInfos, indexInfo)
		}

		indexContent, err := gs.mergeIndexContents(indexInfos)
		if err != nil {
			return err
		}

		if indexContent == nil {
			continue
		}

		fieldStart := w.Count()
		tempBuf := gs.grabBuf(binary.MaxVarintLen64)
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

		err = gs.writeIndexContent(indexContent, w)
		if err != nil {
			return err
		}

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

func (g *geoShapeV2IndexSection) getGeoShapeV2IndexOpaque(opaque map[int]resetable) *geoShapeV2IndexSectionOpaque {
	if _, ok := opaque[SectionGeoShapeV2Index]; !ok {
		opaque[SectionGeoShapeV2Index] = g.InitOpaque(nil)
	}
	return opaque[SectionGeoShapeV2Index].(*geoShapeV2IndexSectionOpaque)
}

type geoShapeV2IndexSectionOpaque struct {
	results []index.Document

	indexContent  map[uint16]*geoIndexContent
	fieldAddrs    map[uint16]int
	fieldsOptions map[string]index.FieldIndexingOptions

	bytesWritten uint64
	tmp          []byte
	tmp2         []byte
	init         bool
}

func (g *geoShapeV2IndexSectionOpaque) Reset() error {
	g.results = nil
	clear(g.indexContent)
	g.tmp = g.tmp[:0]
	g.tmp2 = g.tmp2[:0]
	g.init = false
	g.fieldsOptions = nil
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

func (g *geoShapeV2IndexSectionOpaque) grabBuf2(size int) []byte {
	buf := g.tmp2
	if cap(buf) < size {
		buf = make([]byte, size)
		g.tmp2 = buf
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

type geoIndexContent struct {
	innerCells  []uint64
	innerDocIDs []uint64

	crossCells  []uint64
	crossDocIDs []uint64

	docNums   []uint64
	docScores []uint64

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

	g.innerCells = append(g.innerCells, innerCells...)
	for _ = range innerCells {
		g.innerDocIDs = append(g.innerDocIDs, docID)
	}

	g.crossCells = append(g.crossCells, crossCells...)
	for _ = range crossCells {
		g.crossDocIDs = append(g.crossDocIDs, docID)
	}

	g.boundingBoxes = append(g.boundingBoxes, f.EncodedBoundingBox())
	g.shapes = append(g.shapes, f.EncodedShape())
	g.docScores = append(g.docScores, f.Score())
}

func (g *geoShapeV2IndexSectionOpaque) persist(w *FileWriter) error {

	tempBuf := g.grabBuf(binary.MaxVarintLen64)
	for fieldID, content := range g.indexContent {
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

		err = g.writeIndexContent(content, w)
		if err != nil {
			return err
		}

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
	buf := g.grabBuf2(8 * len(content.docNums))
	for i, docNum := range content.docNums {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], docNum)
	}

	_, err = w.WriteArray(buf)
	if err != nil {
		return err
	}

	// Write the Document Scores
	buf = g.grabBuf2(8 * len(content.docScores))
	for i, docScore := range content.docScores {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], docScore)
	}
	_, err = w.WriteArray(buf)
	if err != nil {
		return err
	}

	// Sort Inner Cells and Doc IDs in tandem
	inner, innerDocIDs := sortArrayPair(content.innerCells, content.innerDocIDs)

	// Write Inner Cells
	buf = g.grabBuf2(8 * len(inner))
	for i, cell := range inner {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], cell)
	}
	_, err = w.WriteArray(buf)
	if err != nil {
		return err
	}

	// Write Inner Cell Doc IDs
	buf = g.grabBuf2(8 * len(innerDocIDs))
	for i, docID := range innerDocIDs {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], docID)
	}
	_, err = w.WriteArray(buf)
	if err != nil {
		return err
	}

	// Sort Cross Cells and Doc IDs in tandem
	cross, crossDocIDs := sortArrayPair(content.crossCells, content.crossDocIDs)

	// Write Cross Cells
	buf = g.grabBuf2(8 * len(cross))
	for i, cell := range cross {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], cell)
	}
	_, err = w.WriteArray(buf)
	if err != nil {
		return err
	}

	// Write Cross Cell Doc IDs
	buf = g.grabBuf2(8 * len(crossDocIDs))
	for i, docID := range crossDocIDs {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], docID)
	}
	_, err = w.WriteArray(buf)
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

func loadGeoIndexContent(r *FileReader, mem []byte) (*geoIndexContent, error) {
	var pos uint64

	// Load Num Docs
	numDocs, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if numDocs == 0 {
		return nil, fmt.Errorf("no geo docs found")
	}

	// Load Doc ID to Doc Num mapping
	buf, shift, err := r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	docNums := make([]uint64, numDocs)
	for i := 0; i < int(numDocs); i++ {
		docNums[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load the Document Scores
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	docScores := make([]uint64, numDocs)
	for i := 0; i < int(numDocs); i++ {
		docScores[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Inner Cells
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	innerCells := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		innerCells[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Inner Cell Doc IDs
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	innerDocIDs := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		innerDocIDs[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Cross Cells
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	crossCells := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		crossCells[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Cross Cell Doc IDs
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	crossDocIDs := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		crossDocIDs[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

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
		docNums:       docNums,
		docScores:     docScores,
		innerCells:    innerCells,
		innerDocIDs:   innerDocIDs,
		crossCells:    crossCells,
		crossDocIDs:   crossDocIDs,
		boundingBoxes: bBoxes,
		shapes:        shapes,
	}, nil
}

func (g *geoShapeV2IndexSectionOpaque) mergeIndexContents(indexInfos []*geoIndexInfo) (*geoIndexContent, error) {
	mergedContent := &geoIndexContent{}
	mergedContent.alloc()

	var numDocs uint64
	newDocNumMapping := make(map[uint64]uint64)

	for _, indexInfo := range indexInfos {
		for _, docNum := range indexInfo.content.docNums {
			newDocNum := indexInfo.docIDMap[docNum]
			if newDocNum == docDropped {
				continue
			}

			newDocNumInternal := numDocs
			numDocs++
			newDocNumMapping[newDocNum] = newDocNumInternal
			mergedContent.docNums = append(mergedContent.docNums, newDocNum)
		}
	}

	if numDocs == 0 {
		return nil, nil
	}

	for _, indexInfo := range indexInfos {
		for i, cell := range indexInfo.content.innerCells {
			internalDocNum := indexInfo.content.innerDocIDs[i]
			docNum := indexInfo.content.docNums[internalDocNum]
			newDocNum := indexInfo.docIDMap[docNum]
			if newDocNum == docDropped {
				continue
			}
			newDocNumInternal := newDocNumMapping[newDocNum]
			mergedContent.innerCells = append(mergedContent.innerCells, cell)
			mergedContent.innerDocIDs = append(mergedContent.innerDocIDs, newDocNumInternal)
		}
		for i, cell := range indexInfo.content.crossCells {
			internalDocNum := indexInfo.content.crossDocIDs[i]
			docNum := indexInfo.content.docNums[internalDocNum]
			newDocNum := indexInfo.docIDMap[docNum]
			if newDocNum == docDropped {
				continue
			}
			newDocNumInternal := newDocNumMapping[newDocNum]
			mergedContent.crossCells = append(mergedContent.crossCells, cell)
			mergedContent.crossDocIDs = append(mergedContent.crossDocIDs, newDocNumInternal)
		}
		for i, bbox := range indexInfo.content.boundingBoxes {
			internalDocNum := uint64(i)
			docNum := indexInfo.content.docNums[internalDocNum]
			newDocNum := indexInfo.docIDMap[docNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.boundingBoxes = append(mergedContent.boundingBoxes, bbox)
		}
		for i, shape := range indexInfo.content.shapes {
			internalDocNum := uint64(i)
			docNum := indexInfo.content.docNums[internalDocNum]
			newDocNum := indexInfo.docIDMap[docNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.shapes = append(mergedContent.shapes, shape)
		}
		for i, score := range indexInfo.content.docScores {
			internalDocNum := uint64(i)
			docNum := indexInfo.content.docNums[internalDocNum]
			newDocNum := indexInfo.docIDMap[docNum]
			if newDocNum == docDropped {
				continue
			}
			mergedContent.docScores = append(mergedContent.docScores, score)
		}
	}

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

// arrayPair holds references to both slices to swap them in tandem
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
