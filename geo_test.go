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
	"bytes"
	"math"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// -----------------------------------------------------------------------------
// stubs

// stubGeoShapeV2Field implements index.Field and index.GeoShapeV2Field.
type stubGeoShapeV2Field struct {
	name       string
	innerCells []uint64
	crossCells []uint64
	bbox       []byte
	shape      []byte
	scoreInner uint64
	scoreCross uint64
}

func (s *stubGeoShapeV2Field) Name() string {
	return s.name
}

func (s *stubGeoShapeV2Field) Value() []byte {
	return nil
}

func (s *stubGeoShapeV2Field) ArrayPositions() []uint64 {
	return nil
}

func (s *stubGeoShapeV2Field) EncodedFieldType() byte {
	return 'g'
}

func (s *stubGeoShapeV2Field) Analyze() {
}

func (s *stubGeoShapeV2Field) Options() index.FieldIndexingOptions {
	return index.IndexField
}

func (s *stubGeoShapeV2Field) AnalyzedLength() int {
	return 0
}

func (s *stubGeoShapeV2Field) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return nil
}

func (s *stubGeoShapeV2Field) NumPlainTextBytes() uint64 {
	return 0
}

func (s *stubGeoShapeV2Field) InnerCells() []uint64 {
	return s.innerCells
}

func (s *stubGeoShapeV2Field) CrossCells() []uint64 {
	return s.crossCells
}

func (s *stubGeoShapeV2Field) EncodedBoundingBox() []byte {
	return s.bbox
}

func (s *stubGeoShapeV2Field) EncodedShape() []byte {
	return s.shape
}

func (s *stubGeoShapeV2Field) Scores() (uint64, uint64) {
	return s.scoreInner, s.scoreCross
}

// stubGeoDocument holds a mix of text and geo fields.
type stubGeoDocument struct {
	id     string
	fields []index.Field
}

func (s *stubGeoDocument) ID() string {
	return s.id
}

func (s *stubGeoDocument) Size() int {
	return 0
}

func (s *stubGeoDocument) VisitFields(visitor index.FieldVisitor) {
	for _, f := range s.fields {
		visitor(f)
	}
}

func (s *stubGeoDocument) HasComposite() bool {
	return false
}

func (s *stubGeoDocument) VisitComposite(visitor index.CompositeFieldVisitor) {
}

func (s *stubGeoDocument) NumPlainTextBytes() uint64 {
	return 0
}

func (s *stubGeoDocument) StoredFieldsBytes() uint64 {
	return 0
}

func (s *stubGeoDocument) AddIDField() {
}

func (s *stubGeoDocument) Indexed() bool {
	return true
}

// -----------------------------------------------------------------------------
// test data helpers

const geoTestFieldName = "polygon"

// geoTestDoc describes one test document; hasGeo controls whether the
// document carries the geo field at all.
type geoTestDoc struct {
	id     string
	hasGeo bool

	inner []uint64
	cross []uint64
	bbox  []byte
	shape []byte

	scoreInner uint64
	scoreCross uint64
}

func newGeoStubDocument(d geoTestDoc) index.Document {
	fields := []index.Field{
		newStubFieldSplitString("_id", nil, d.id, true, false, false),
		newStubFieldSplitString("desc", nil, "some thing", true, false, true),
	}
	if d.hasGeo {
		fields = append(fields, &stubGeoShapeV2Field{
			name:       geoTestFieldName,
			innerCells: d.inner,
			crossCells: d.cross,
			bbox:       d.bbox,
			shape:      d.shape,
			scoreInner: d.scoreInner,
			scoreCross: d.scoreCross,
		})
	}
	return &stubGeoDocument{
		id:     d.id,
		fields: fields,
	}
}

func buildGeoTestSegment(docs []geoTestDoc) (*SegmentBase, error) {
	results := make([]index.Document, 0, len(docs))
	for _, d := range docs {
		results = append(results, newGeoStubDocument(d))
	}
	segBase, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	if err != nil {
		return nil, err
	}
	return segBase.(*SegmentBase), nil
}

func persistGeoTestSegment(docs []geoTestDoc, path string) error {
	_ = os.RemoveAll(path)
	sb, err := buildGeoTestSegment(docs)
	if err != nil {
		return err
	}
	return PersistSegmentBase(sb, path)
}

// sortedCellPairs returns the (cell, geoDocID) pairs of the given docs sorted
// by cell, mirroring what the geo section persists. cells selects the inner or
// cross cell list of a doc. The docs must be the geo docs in geo docID order,
// and cell values must be unique across docs for the expectation to be
// deterministic.
func sortedCellPairs(docs []geoTestDoc, cells func(geoTestDoc) []uint64) ([]uint64, []uint32) {
	var cellVals []uint64
	var docIDs []uint32
	for geoDocID, d := range docs {
		for _, cell := range cells(d) {
			cellVals = append(cellVals, cell)
			docIDs = append(docIDs, uint32(geoDocID))
		}
	}
	return sortArrayPair(cellVals, docIDs)
}

// verifyGeoShapeV2Data checks geoData against the expected geo docs (in geo
// docID order) and their expected segment doc numbers.
func verifyGeoShapeV2Data(t *testing.T, geoData seg.GeoShapeV2Data,
	geoDocs []geoTestDoc, wantDocNums []uint32) {
	t.Helper()

	if geoData == nil {
		t.Fatal("expected geo data, got nil")
	}

	numDocs := uint32(len(geoDocs))
	if geoData.NumDocs() != uint64(numDocs) {
		t.Fatalf("expected %d geo docs, got %d", numDocs, geoData.NumDocs())
	}

	if !reflect.DeepEqual(geoData.DocNums(), wantDocNums) {
		t.Fatalf("expected doc nums %v, got %v", wantDocNums, geoData.DocNums())
	}

	wantScoresInner := make([]uint64, 0, len(geoDocs))
	wantScoresCross := make([]uint64, 0, len(geoDocs))
	for _, d := range geoDocs {
		wantScoresInner = append(wantScoresInner, d.scoreInner)
		wantScoresCross = append(wantScoresCross, d.scoreCross)
	}
	gotScoresInner, gotScoresCross := geoData.DocScores()
	if !reflect.DeepEqual(gotScoresInner, wantScoresInner) {
		t.Fatalf("expected inner doc scores %v, got %v", wantScoresInner, gotScoresInner)
	}
	if !reflect.DeepEqual(gotScoresCross, wantScoresCross) {
		t.Fatalf("expected cross doc scores %v, got %v", wantScoresCross, gotScoresCross)
	}

	wantInner, wantInnerDocIDs := sortedCellPairs(geoDocs,
		func(d geoTestDoc) []uint64 { return d.inner })
	if !equalUint64Slices(geoData.InnerCells(), wantInner) {
		t.Fatalf("expected inner cells %v, got %v", wantInner, geoData.InnerCells())
	}
	if !equalUint64Slices(geoData.InnerDocIDs(), wantInnerDocIDs) {
		t.Fatalf("expected inner doc IDs %v, got %v", wantInnerDocIDs, geoData.InnerDocIDs())
	}

	wantCross, wantCrossDocIDs := sortedCellPairs(geoDocs,
		func(d geoTestDoc) []uint64 { return d.cross })
	if !equalUint64Slices(geoData.CrossCells(), wantCross) {
		t.Fatalf("expected cross cells %v, got %v", wantCross, geoData.CrossCells())
	}
	if !equalUint64Slices(geoData.CrossDocIDs(), wantCrossDocIDs) {
		t.Fatalf("expected cross doc IDs %v, got %v", wantCrossDocIDs, geoData.CrossDocIDs())
	}

	for geoDocID, d := range geoDocs {
		bbox, err := geoData.BoundingBox(uint32(geoDocID))
		if err != nil {
			t.Fatalf("bounding box for geo docID %d: %v", geoDocID, err)
		}
		if !bytes.Equal(bbox, d.bbox) {
			t.Fatalf("expected bounding box %q for geo docID %d, got %q",
				d.bbox, geoDocID, bbox)
		}

		shape, err := geoData.Shape(uint32(geoDocID))
		if err != nil {
			t.Fatalf("shape for geo docID %d: %v", geoDocID, err)
		}
		if !bytes.Equal(shape, d.shape) {
			t.Fatalf("expected shape %q for geo docID %d, got %q",
				d.shape, geoDocID, shape)
		}
	}

	// out of range geo docIDs must error
	if _, err := geoData.BoundingBox(numDocs); err == nil {
		t.Fatal("expected error for out of range bounding box geo docID")
	}
	if _, err := geoData.Shape(numDocs); err == nil {
		t.Fatal("expected error for out of range shape geo docID")
	}

	// score maps from the pool must be empty, even after returning a dirtied one
	scores := geoData.GetScoreMap()
	if len(scores) != 0 {
		t.Fatalf("expected empty score map, got %d entries", len(scores))
	}
	for i := uint32(0); i < numDocs; i++ {
		scores[i] = uint64(i) + 1
	}
	geoData.PutScoreMap(scores)
	scores = geoData.GetScoreMap()
	if len(scores) != 0 {
		t.Fatalf("expected empty score map after reuse, got %d entries", len(scores))
	}
	geoData.PutScoreMap(scores)
}

func equalUint64Slices[T comparable](got, want []T) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// tests

// TestGeoIndexSectionRoundTrip builds a segment containing geo shape fields,
// persists it, reopens it and verifies the geo data read back through
// GeoShapeV2Data, including the exclusion bitmap translation.
func TestGeoIndexSectionRoundTrip(t *testing.T) {
	docs := []geoTestDoc{
		{
			id:         "a",
			hasGeo:     true,
			inner:      []uint64{30, 10},
			cross:      []uint64{25},
			bbox:       []byte("bbox-a"),
			shape:      []byte("shape-a"),
			scoreInner: 100,
			scoreCross: 100 + 1000,
		},
		{
			// no geo field: must not appear in the geo data, and the
			// geo docNums mapping must skip its doc number
			id:     "b",
			hasGeo: false,
		},
		{
			id:         "c",
			hasGeo:     true,
			inner:      []uint64{20, 5},
			cross:      []uint64{35, 15},
			bbox:       []byte("bbox-c"),
			shape:      []byte("shape-c"),
			scoreInner: 102,
			scoreCross: 102 + 1000,
		},
	}
	geoDocs := []geoTestDoc{docs[0], docs[2]}

	tmpPath := getTempPath("geo-roundtrip.zap")
	if err := persistGeoTestSegment(docs, tmpPath); err != nil {
		t.Fatal(err)
	}

	segment, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		if cerr := segment.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	geoSeg := segment.(*Segment)

	geoData, err := geoSeg.GeoShapeV2Data(geoTestFieldName, nil)
	if err != nil {
		t.Fatal(err)
	}
	verifyGeoShapeV2Data(t, geoData, geoDocs, []uint32{0, 2})

	// with a nil except bitmap, nothing is excluded
	if geoData.Excluded() != nil {
		t.Fatalf("expected nil excluded bitmap, got %v", geoData.Excluded())
	}
	geoData.Close()

	// fields without geo data must return nil without error
	for _, field := range []string{"desc", "nonexistent"} {
		nilData, err := geoSeg.GeoShapeV2Data(field, nil)
		if err != nil {
			t.Fatal(err)
		}
		if nilData != nil {
			t.Fatalf("expected nil geo data for field %q", field)
		}
	}

	// reopen the segment (fresh cache) and verify that an except bitmap in
	// segment doc number space is translated into geo docID space
	segment2, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		if cerr := segment2.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	except := roaring.NewBitmap()
	except.AddInt(2) // segment doc number of doc "c"
	geoData2, err := segment2.(*Segment).GeoShapeV2Data(geoTestFieldName, except)
	if err != nil {
		t.Fatal(err)
	}
	if geoData2 == nil {
		t.Fatal("expected geo data, got nil")
	}
	defer geoData2.Close()

	excluded := geoData2.Excluded()
	if excluded == nil || excluded.GetCardinality() != 1 || !excluded.Contains(1) {
		t.Fatalf("expected excluded bitmap containing only geo docID 1, got %v", excluded)
	}
}

// TestGeoIndexMerge merges segments with and without geo shape fields, with
// one document dropped, and verifies the merged geo data: dropped documents
// vanish from every array, doc numbers are remapped and cells are re-sorted.
func TestGeoIndexMerge(t *testing.T) {
	segADocs := []geoTestDoc{
		{
			id:         "a0",
			hasGeo:     true,
			inner:      []uint64{30, 10},
			bbox:       []byte("bbox-a0"),
			shape:      []byte("shape-a0"),
			scoreInner: 100,
			scoreCross: 100 + 1000,
		},
		{
			// dropped during the merge
			id:         "a1",
			hasGeo:     true,
			inner:      []uint64{20},
			cross:      []uint64{40},
			bbox:       []byte("bbox-a1"),
			shape:      []byte("shape-a1"),
			scoreInner: 101,
			scoreCross: 101 + 1000,
		},
	}
	segBDocs := []geoTestDoc{
		{
			id:         "b0",
			hasGeo:     true,
			inner:      []uint64{5, 50},
			cross:      []uint64{45},
			bbox:       []byte("bbox-b0"),
			shape:      []byte("shape-b0"),
			scoreInner: 200,
			scoreCross: 200 + 1000,
		},
		{
			id:     "b1",
			hasGeo: false,
		},
		{
			id:         "b2",
			hasGeo:     true,
			cross:      []uint64{60, 55},
			bbox:       []byte("bbox-b2"),
			shape:      []byte("shape-b2"),
			scoreInner: 202,
			scoreCross: 202 + 1000,
		},
	}
	// no geo field anywhere in this segment
	segCDocs := []geoTestDoc{
		{
			id:     "c0",
			hasGeo: false,
		},
	}

	tmpPaths := []string{
		getTempPath("geo-merge-a.zap"),
		getTempPath("geo-merge-b.zap"),
		getTempPath("geo-merge-c.zap"),
	}
	for i, docs := range [][]geoTestDoc{segADocs, segBDocs, segCDocs} {
		if err := persistGeoTestSegment(docs, tmpPaths[i]); err != nil {
			t.Fatal(err)
		}
	}

	segsToMerge := make([]seg.Segment, len(tmpPaths))
	for i, path := range tmpPaths {
		segment, err := zapPlugin.Open(path)
		if err != nil {
			t.Fatalf("error opening segment: %v", err)
		}
		defer func() {
			if cerr := segment.Close(); cerr != nil {
				t.Fatalf("error closing segment: %v", cerr)
			}
		}()
		segsToMerge[i] = segment
	}

	// drop doc a1 from the first segment
	drops := make([]*roaring.Bitmap, len(segsToMerge))
	drops[0] = roaring.NewBitmap()
	drops[0].AddInt(1)

	mergedPath := getTempPath("geo-merge-out.zap")
	_ = os.RemoveAll(mergedPath)
	_, _, err := zapPlugin.Merge(segsToMerge, drops, mergedPath, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	merged, err := zapPlugin.Open(mergedPath)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		if cerr := merged.Close(); cerr != nil {
			t.Fatalf("error closing merged segment: %v", cerr)
		}
	}()

	mergedSeg := merged.(*Segment)
	if mergedSeg.Count() != 5 {
		t.Fatalf("expected 5 docs in merged segment, got %d", mergedSeg.Count())
	}

	geoData, err := mergedSeg.GeoShapeV2Data(geoTestFieldName, nil)
	if err != nil {
		t.Fatal(err)
	}
	if geoData == nil {
		t.Fatal("expected geo data in merged segment, got nil")
	}
	defer geoData.Close()

	// surviving geo docs in merged order: a0 -> doc num 0, b0 -> 1, b2 -> 3
	// (a1 dropped, b1 and c0 have no geo field)
	geoDocs := []geoTestDoc{segADocs[0], segBDocs[0], segBDocs[2]}
	verifyGeoShapeV2Data(t, geoData, geoDocs, []uint32{0, 1, 3})
}

// makeNewDocNums builds an old-segment-docNum -> merged-docNum slice of the
// given length, defaulting every entry to docDropped and overriding the
// entries named in m. This is the merge-wide remapping, which stays uint64.
func makeNewDocNums(size int, m map[int]uint64) []uint64 {
	s := make([]uint64, size)
	for i := range s {
		s[i] = docDropped
	}
	for k, v := range m {
		s[k] = v
	}
	return s
}

// drainCursor repeatedly calls next() and collects the (cell, docID) pairs the
// cursor yields until it is exhausted.
func drainCursor(c *geoCellCursor) (cells []uint64, docIDs []uint32) {
	for c.next() {
		cells = append(cells, c.curCell)
		docIDs = append(docIDs, c.curDocID)
	}
	return cells, docIDs
}

// newIdentityCursor builds a cursor whose remap is the identity: curDocID
// always equals the old geo docID at the current position and nothing is
// dropped. This isolates kWayMergeCells from the remap contents.
func newIdentityCursor(cells []uint64, docIDs []uint32) *geoCellCursor {
	var n uint32
	for _, d := range docIDs {
		if d+1 > n {
			n = d + 1
		}
	}
	remap := make([]uint32, n)
	for i := uint32(0); i < n; i++ {
		remap[i] = i
	}
	return &geoCellCursor{cells: cells, docIDs: docIDs, remap: remap}
}

// sortPairs sorts (cell, docID) pairs by cell then docID so that mergers whose
// ordering among equal cells is unspecified can still be compared by multiset.
func sortPairs(cells []uint64, docIDs []uint32) (sortedCells []uint64, sortedDocIDs []uint32) {
	type pair struct {
		cell  uint64
		docID uint32
	}
	pairs := make([]pair, len(cells))
	for i := range cells {
		pairs[i] = pair{cells[i], docIDs[i]}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].cell != pairs[j].cell {
			return pairs[i].cell < pairs[j].cell
		}
		return pairs[i].docID < pairs[j].docID
	})
	sortedCells = make([]uint64, len(pairs))
	sortedDocIDs = make([]uint32, len(pairs))
	for i, p := range pairs {
		sortedCells[i] = p.cell
		sortedDocIDs[i] = p.docID
	}
	return sortedCells, sortedDocIDs
}

// newGeoIndexInfo builds a geoIndexInfo for testing buildGeoDocRemaps. docNums
// maps old geo docID -> old segment doc number (uint32); newDocNums maps old
// segment doc number -> merged doc number (docDropped if deleted).
func newGeoIndexInfo(docNums []uint32, newDocNums []uint64) *geoIndexInfo {
	return &geoIndexInfo{
		content:    &geoIndexContent{docNums: docNums},
		newDocNums: newDocNums,
	}
}

func TestGeoCellCursorNext(t *testing.T) {
	tests := []struct {
		name    string
		cursor  *geoCellCursor
		wantCel []uint64
		wantDoc []uint32
	}{
		{
			name: "no drops, remaps geo docIDs",
			cursor: &geoCellCursor{
				cells:  []uint64{10, 20, 30},
				docIDs: []uint32{0, 0, 1},
				remap:  []uint32{5, 7}, // old geoID 0 -> 5, 1 -> 7
			},
			wantCel: []uint64{10, 20, 30},
			wantDoc: []uint32{5, 5, 7},
		},
		{
			name: "middle document dropped is skipped",
			cursor: &geoCellCursor{
				cells:  []uint64{10, 20, 25, 30},
				docIDs: []uint32{0, 1, 1, 2},
				remap:  []uint32{0, uint32(math.MaxUint32), 1},
			},
			wantCel: []uint64{10, 30},
			wantDoc: []uint32{0, 1},
		},
		{
			name: "leading and trailing documents dropped",
			cursor: &geoCellCursor{
				cells:  []uint64{10, 20, 30, 40},
				docIDs: []uint32{0, 0, 1, 2},
				remap:  []uint32{uint32(math.MaxUint32), 0, uint32(math.MaxUint32)},
			},
			wantCel: []uint64{30},
			wantDoc: []uint32{0},
		},
		{
			name: "all documents dropped yields nothing",
			cursor: &geoCellCursor{
				cells:  []uint64{10, 20},
				docIDs: []uint32{0, 1},
				remap:  []uint32{uint32(math.MaxUint32), uint32(math.MaxUint32)},
			},
			wantCel: nil,
			wantDoc: nil,
		},
		{
			name: "empty run yields nothing",
			cursor: &geoCellCursor{
				cells:  nil,
				docIDs: nil,
				remap:  nil,
			},
			wantCel: nil,
			wantDoc: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCel, gotDoc := drainCursor(tt.cursor)
			if !reflect.DeepEqual(gotCel, tt.wantCel) {
				t.Fatalf("cells: got %v, want %v", gotCel, tt.wantCel)
			}
			if !reflect.DeepEqual(gotDoc, tt.wantDoc) {
				t.Fatalf("docIDs: got %v, want %v", gotDoc, tt.wantDoc)
			}
			// once exhausted, next() must keep returning false
			if tt.cursor.next() {
				t.Fatal("next() returned true after the cursor was exhausted")
			}
		})
	}
}

func TestBuildGeoDocRemaps(t *testing.T) {
	t.Run("assigns merged geo docIDs across segments, skipping drops", func(t *testing.T) {
		indexInfos := []*geoIndexInfo{
			// seg0: doc at geoID 0 (docNum 5) kept, geoID 1 (docNum 6) dropped
			newGeoIndexInfo([]uint32{5, 6}, makeNewDocNums(10, map[int]uint64{5: 100})),
			// seg1: doc at geoID 0 (docNum 7) kept
			newGeoIndexInfo([]uint32{7}, makeNewDocNums(10, map[int]uint64{7: 101})),
			// seg2: docs at geoID 0 (docNum 8) and 1 (docNum 9) kept
			newGeoIndexInfo([]uint32{8, 9}, makeNewDocNums(10, map[int]uint64{8: 102, 9: 103})),
		}

		merged := &geoIndexContent{}
		segRemaps, numDocs := buildGeoDocRemaps(indexInfos, merged)

		if numDocs != 4 {
			t.Fatalf("numDocs: got %d, want 4", numDocs)
		}

		wantRemaps := [][]uint32{
			{0, uint32(math.MaxUint32)},
			{1},
			{2, 3},
		}
		if !reflect.DeepEqual(segRemaps, wantRemaps) {
			t.Fatalf("segRemaps: got %v, want %v", segRemaps, wantRemaps)
		}

		// merged doc numbers must be appended in merged geo docID order
		wantDocNums := []uint32{100, 101, 102, 103}
		if !reflect.DeepEqual(merged.docNums, wantDocNums) {
			t.Fatalf("merged docNums: got %v, want %v", merged.docNums, wantDocNums)
		}
	})

	t.Run("all documents dropped yields zero docs", func(t *testing.T) {
		indexInfos := []*geoIndexInfo{
			newGeoIndexInfo([]uint32{5, 6}, makeNewDocNums(10, nil)),
		}
		merged := &geoIndexContent{}
		segRemaps, numDocs := buildGeoDocRemaps(indexInfos, merged)

		if numDocs != 0 {
			t.Fatalf("numDocs: got %d, want 0", numDocs)
		}
		if !reflect.DeepEqual(segRemaps, [][]uint32{{uint32(math.MaxUint32), uint32(math.MaxUint32)}}) {
			t.Fatalf("segRemaps: got %v", segRemaps)
		}
		if len(merged.docNums) != 0 {
			t.Fatalf("merged docNums: got %v, want empty", merged.docNums)
		}
	})
}

func TestKWayMergeCells(t *testing.T) {
	t.Run("interleaved runs merge into sorted order", func(t *testing.T) {
		cursors := []*geoCellCursor{
			newIdentityCursor([]uint64{10, 40}, []uint32{0, 0}),
			newIdentityCursor([]uint64{5, 50}, []uint32{1, 1}),
			newIdentityCursor([]uint64{20, 30}, []uint32{2, 2}),
		}
		gotCel, gotDoc := kWayMergeCells(cursors, nil, nil)

		wantCel := []uint64{5, 10, 20, 30, 40, 50}
		wantDoc := []uint32{1, 0, 2, 2, 0, 1}
		if !reflect.DeepEqual(gotCel, wantCel) {
			t.Fatalf("cells: got %v, want %v", gotCel, wantCel)
		}
		if !reflect.DeepEqual(gotDoc, wantDoc) {
			t.Fatalf("docIDs: got %v, want %v", gotDoc, wantDoc)
		}
	})

	t.Run("empty cursors are skipped", func(t *testing.T) {
		cursors := []*geoCellCursor{
			newIdentityCursor(nil, nil),
			newIdentityCursor([]uint64{7, 9}, []uint32{0, 1}),
			newIdentityCursor(nil, nil),
		}
		gotCel, gotDoc := kWayMergeCells(cursors, nil, nil)
		if !reflect.DeepEqual(gotCel, []uint64{7, 9}) {
			t.Fatalf("cells: got %v, want [7 9]", gotCel)
		}
		if !reflect.DeepEqual(gotDoc, []uint32{0, 1}) {
			t.Fatalf("docIDs: got %v, want [0 1]", gotDoc)
		}
	})

	t.Run("no cursors yields empty output", func(t *testing.T) {
		gotCel, gotDoc := kWayMergeCells(nil, nil, nil)
		if len(gotCel) != 0 || len(gotDoc) != 0 {
			t.Fatalf("expected empty output, got cells %v docIDs %v", gotCel, gotDoc)
		}
	})

	t.Run("duplicate cells across runs are all preserved", func(t *testing.T) {
		cursors := []*geoCellCursor{
			newIdentityCursor([]uint64{10, 20}, []uint32{0, 0}),
			newIdentityCursor([]uint64{10, 30}, []uint32{1, 1}),
			newIdentityCursor([]uint64{20}, []uint32{2}),
		}
		gotCel, gotDoc := kWayMergeCells(cursors, nil, nil)

		// output must be non-decreasing by cell
		for i := 1; i < len(gotCel); i++ {
			if gotCel[i] < gotCel[i-1] {
				t.Fatalf("output not sorted at %d: %v", i, gotCel)
			}
		}
		// and must contain exactly the input (cell, docID) multiset
		gotCelS, gotDocS := sortPairs(gotCel, gotDoc)
		wantCelS, wantDocS := sortPairs(
			[]uint64{10, 20, 10, 30, 20},
			[]uint32{0, 0, 1, 1, 2},
		)
		if !reflect.DeepEqual(gotCelS, wantCelS) || !reflect.DeepEqual(gotDocS, wantDocS) {
			t.Fatalf("multiset mismatch: got (%v,%v), want (%v,%v)",
				gotCelS, gotDocS, wantCelS, wantDocS)
		}
	})

	t.Run("appends to existing output slices", func(t *testing.T) {
		cursors := []*geoCellCursor{
			newIdentityCursor([]uint64{2, 4}, []uint32{0, 0}),
		}
		gotCel, gotDoc := kWayMergeCells(cursors, []uint64{99}, []uint32{88})
		if !reflect.DeepEqual(gotCel, []uint64{99, 2, 4}) {
			t.Fatalf("cells: got %v, want [99 2 4]", gotCel)
		}
		if !reflect.DeepEqual(gotDoc, []uint32{88, 0, 0}) {
			t.Fatalf("docIDs: got %v, want [88 0 0]", gotDoc)
		}
	})

	t.Run("merge skips dropped documents while remapping", func(t *testing.T) {
		// segment 0: cell 10 (geoID 0 -> merged 0), cell 20 (geoID 1 dropped)
		seg0 := &geoCellCursor{
			cells:  []uint64{10, 20},
			docIDs: []uint32{0, 1},
			remap:  []uint32{0, uint32(math.MaxUint32)},
		}
		// segment 1: cell 15 (geoID 0 -> merged 1)
		seg1 := &geoCellCursor{
			cells:  []uint64{15},
			docIDs: []uint32{0},
			remap:  []uint32{1},
		}
		gotCel, gotDoc := kWayMergeCells([]*geoCellCursor{seg0, seg1}, nil, nil)
		if !reflect.DeepEqual(gotCel, []uint64{10, 15}) {
			t.Fatalf("cells: got %v, want [10 15]", gotCel)
		}
		if !reflect.DeepEqual(gotDoc, []uint32{0, 1}) {
			t.Fatalf("docIDs: got %v, want [0 1]", gotDoc)
		}
	})
}
