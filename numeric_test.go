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

// stubNumericV2Field implements index.Field and index.NumericV2Field. The doc
// value term is a stand-in for a prefix-coded term: zapx cannot import
// bleve/numeric, and the section only ever copies these bytes.
type stubNumericV2Field struct {
	name    string
	value   uint64
	dvTerm  []byte
	options index.FieldIndexingOptions
}

func (s *stubNumericV2Field) Name() string                                     { return s.name }
func (s *stubNumericV2Field) Value() []byte                                    { return s.dvTerm }
func (s *stubNumericV2Field) ArrayPositions() []uint64                         { return nil }
func (s *stubNumericV2Field) EncodedFieldType() byte                           { return 'm' }
func (s *stubNumericV2Field) Analyze()                                         {}
func (s *stubNumericV2Field) Options() index.FieldIndexingOptions              { return s.options }
func (s *stubNumericV2Field) AnalyzedLength() int                              { return 0 }
func (s *stubNumericV2Field) AnalyzedTokenFrequencies() index.TokenFrequencies { return nil }
func (s *stubNumericV2Field) NumPlainTextBytes() uint64                        { return 0 }
func (s *stubNumericV2Field) SortableValue() uint64                            { return s.value }
func (s *stubNumericV2Field) DocValueTerm() []byte                             { return s.dvTerm }

type stubNumericDocument struct {
	id     string
	fields []index.Field
}

func (s *stubNumericDocument) ID() string                                         { return s.id }
func (s *stubNumericDocument) Size() int                                          { return 0 }
func (s *stubNumericDocument) HasComposite() bool                                 { return false }
func (s *stubNumericDocument) VisitComposite(visitor index.CompositeFieldVisitor) {}
func (s *stubNumericDocument) NumPlainTextBytes() uint64                          { return 0 }
func (s *stubNumericDocument) StoredFieldsBytes() uint64                          { return 0 }
func (s *stubNumericDocument) AddIDField()                                        {}
func (s *stubNumericDocument) Indexed() bool                                      { return true }

func (s *stubNumericDocument) VisitFields(visitor index.FieldVisitor) {
	for _, f := range s.fields {
		visitor(f)
	}
}

// -----------------------------------------------------------------------------
// test data helpers

const numTestFieldName = "price"

// numTestDoc describes one test document. values is empty for a document that
// does not carry the numeric field at all, and holds several entries for a
// multi-valued field.
type numTestDoc struct {
	id       string
	values   []uint64
	noDocVal bool
}

func numDVTerm(v uint64) []byte {
	return []byte(fmt.Sprintf("t%d", v))
}

func newNumericStubDocument(d numTestDoc) index.Document {
	fields := []index.Field{
		newStubFieldSplitString("_id", nil, d.id, true, false, false),
		newStubFieldSplitString("desc", nil, "some thing", true, false, true),
	}
	opts := index.IndexField | index.DocValues
	if d.noDocVal {
		opts = index.IndexField
	}
	for _, v := range d.values {
		fields = append(fields, &stubNumericV2Field{
			name:    numTestFieldName,
			value:   v,
			dvTerm:  numDVTerm(v),
			options: opts,
		})
	}
	return &stubNumericDocument{id: d.id, fields: fields}
}

func buildNumericTestSegment(docs []numTestDoc) (*SegmentBase, error) {
	results := make([]index.Document, 0, len(docs))
	for _, d := range docs {
		results = append(results, newNumericStubDocument(d))
	}
	segBase, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	if err != nil {
		return nil, err
	}
	return segBase.(*SegmentBase), nil
}

func persistNumericTestSegment(docs []numTestDoc, path string) error {
	_ = os.RemoveAll(path)
	sb, err := buildNumericTestSegment(docs)
	if err != nil {
		return err
	}
	return PersistSegmentBase(sb, path)
}

// collectDocValues gathers the doc value terms visible for each document, keyed
// by doc number.
func collectDocValues(t *testing.T, sb *SegmentBase, field string,
	numDocs uint64) map[uint64][]string {
	t.Helper()
	rv := map[uint64][]string{}
	var dvs seg.DocVisitState
	var err error
	for docNum := uint64(0); docNum < numDocs; docNum++ {
		dvs, err = sb.VisitDocValues(docNum, []string{field},
			func(f string, term []byte) {
				if f != field {
					t.Fatalf("unexpected field %q in doc values", f)
				}
				rv[docNum] = append(rv[docNum], string(term))
			}, dvs)
		if err != nil {
			t.Fatalf("VisitDocValues(%d): %v", docNum, err)
		}
	}
	return rv
}

// -----------------------------------------------------------------------------

// TestNumericIndexSectionRoundTrip builds a segment containing number_v2
// fields, persists it, reopens it and verifies both the search arrays and the
// doc values read back.
func TestNumericIndexSectionRoundTrip(t *testing.T) {
	docs := []numTestDoc{
		{id: "a", values: []uint64{30, 10}}, // multi-valued
		{id: "b"},                           // no numeric field at all
		{id: "c", values: []uint64{20}},
		{id: "d", values: []uint64{10}}, // duplicate value across docs
	}

	path := getTempPath("numeric-roundtrip.zap")
	if err := persistNumericTestSegment(docs, path); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(path) }()

	segment, err := zapPlugin.Open(path)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		if cerr := segment.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	sb := segment.(*Segment)

	data, err := sb.NumericV2Data(numTestFieldName)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("expected numeric data for the field")
	}
	defer data.Close()

	// values must come back sorted ascending, doc numbers carried along
	wantValues := []uint64{10, 10, 20, 30}
	if !reflect.DeepEqual(data.Values(), wantValues) {
		t.Fatalf("values: got %v, want %v", data.Values(), wantValues)
	}

	// doc 0 owns 10 and 30; doc 2 owns 20; doc 3 owns 10. Doc 1 has no entry.
	// The two 10s may appear in either order relative to each other.
	gotPairs := map[uint64][]uint32{}
	for i, v := range data.Values() {
		gotPairs[v] = append(gotPairs[v], data.DocNums()[i])
	}
	for _, dn := range gotPairs {
		sort.Slice(dn, func(i, j int) bool { return dn[i] < dn[j] })
	}
	wantPairs := map[uint64][]uint32{10: {0, 3}, 20: {2}, 30: {0}}
	if !reflect.DeepEqual(gotPairs, wantPairs) {
		t.Fatalf("value -> docNums: got %v, want %v", gotPairs, wantPairs)
	}

	// a field with no entry for a document must not produce a doc number for it
	for _, dn := range data.DocNums() {
		if dn == 1 {
			t.Fatal("doc 1 has no numeric field but appears in docNums")
		}
	}

	// doc values, in document order, one term per value
	gotDV := collectDocValues(t, &sb.SegmentBase, numTestFieldName, sb.numDocs)
	wantDV := map[uint64][]string{
		0: {"t30", "t10"}, // document order of the field instances, not value order
		2: {"t20"},
		3: {"t10"},
	}
	if !reflect.DeepEqual(gotDV, wantDV) {
		t.Fatalf("doc values: got %v, want %v", gotDV, wantDV)
	}
}

// TestNumericIndexSectionNoDocValues verifies that a field whose options omit
// DocValues writes the sentinel offsets and yields no doc value reader, while
// the search arrays are unaffected.
func TestNumericIndexSectionNoDocValues(t *testing.T) {
	docs := []numTestDoc{
		{id: "a", values: []uint64{7}, noDocVal: true},
		{id: "b", values: []uint64{3}, noDocVal: true},
	}

	path := getTempPath("numeric-nodv.zap")
	if err := persistNumericTestSegment(docs, path); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(path) }()

	segment, err := zapPlugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = segment.Close() }()

	sb := segment.(*Segment)

	data, err := sb.NumericV2Data(numTestFieldName)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("expected numeric data for the field")
	}
	defer data.Close()

	if want := []uint64{3, 7}; !reflect.DeepEqual(data.Values(), want) {
		t.Fatalf("values: got %v, want %v", data.Values(), want)
	}

	if got := collectDocValues(t, &sb.SegmentBase, numTestFieldName, sb.numDocs); len(got) != 0 {
		t.Fatalf("expected no doc values, got %v", got)
	}

	// the field record must carry the sentinel offsets
	fieldIDPlus1 := sb.fieldsMap[numTestFieldName]
	pos := sb.fieldsSectionsMap[fieldIDPlus1-1][SectionNumericV2Index]
	if pos == 0 {
		t.Fatal("expected a number_v2 section for the field")
	}
	dvStart, n := readUvarintAt(sb.mem, pos)
	dvEnd, _ := readUvarintAt(sb.mem, pos+n)
	if dvStart != fieldNotUninverted || dvEnd != fieldNotUninverted {
		t.Fatalf("expected sentinel doc value offsets, got %d/%d", dvStart, dvEnd)
	}
}

// TestNumericIndexMissingField verifies that a segment with no number_v2 field
// at all returns nil rather than erroring.
func TestNumericIndexMissingField(t *testing.T) {
	path := getTempPath("numeric-missing.zap")
	if err := persistNumericTestSegment([]numTestDoc{{id: "a"}}, path); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(path) }()

	segment, err := zapPlugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = segment.Close() }()

	data, err := segment.(*Segment).NumericV2Data(numTestFieldName)
	if err != nil {
		t.Fatalf("expected no error for an absent field, got %v", err)
	}
	if data != nil {
		t.Fatal("expected nil data for an absent field")
	}
}

// TestNumericIndexMerge merges three segments with deletions and verifies that
// the merged search arrays stay sorted with remapped doc numbers, and that the
// merged doc values stay in ascending document order.
func TestNumericIndexMerge(t *testing.T) {
	segDocs := [][]numTestDoc{
		{
			{id: "a0", values: []uint64{50}},
			{id: "a1", values: []uint64{15}}, // dropped below
			{id: "a2", values: []uint64{40, 5}},
		},
		{
			{id: "b0"}, // no numeric field
			{id: "b1", values: []uint64{25}},
		},
		{
			{id: "c0", values: []uint64{35}},
			{id: "c1", values: []uint64{45}}, // dropped below
		},
	}

	tmpPaths := make([]string, len(segDocs))
	for i, docs := range segDocs {
		tmpPaths[i] = getTempPath(fmt.Sprintf("numeric-merge-%d.zap", i))
		if err := persistNumericTestSegment(docs, tmpPaths[i]); err != nil {
			t.Fatal(err)
		}
		defer func(p string) { _ = os.RemoveAll(p) }(tmpPaths[i])
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

	drops := make([]*roaring.Bitmap, len(segsToMerge))
	drops[0] = roaring.NewBitmap()
	drops[0].AddInt(1) // a1
	drops[2] = roaring.NewBitmap()
	drops[2].AddInt(1) // c1

	mergedPath := getTempPath("numeric-merge-out.zap")
	_ = os.RemoveAll(mergedPath)
	defer func() { _ = os.RemoveAll(mergedPath) }()

	if _, _, err := zapPlugin.Merge(segsToMerge, drops, mergedPath, nil, nil); err != nil {
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

	data, err := mergedSeg.NumericV2Data(numTestFieldName)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("expected numeric data in the merged segment")
	}
	defer data.Close()

	// surviving values: a0=50, a2={40,5}, b1=25, c0=35 -- a1 and c1 dropped
	wantValues := []uint64{5, 25, 35, 40, 50}
	if !reflect.DeepEqual(data.Values(), wantValues) {
		t.Fatalf("merged values: got %v, want %v", data.Values(), wantValues)
	}

	// merged doc numbers: a0=0, a2=1, b0=2, b1=3, c0=4
	wantOwner := map[uint64]uint32{5: 1, 40: 1, 50: 0, 25: 3, 35: 4}
	for i, v := range data.Values() {
		if got, want := data.DocNums()[i], wantOwner[v]; got != want {
			t.Fatalf("value %d: got docNum %d, want %d", v, got, want)
		}
	}

	// merged doc values, keyed by merged doc number
	gotDV := collectDocValues(t, &mergedSeg.SegmentBase, numTestFieldName, mergedSeg.numDocs)
	wantDV := map[uint64][]string{
		0: {"t50"},
		1: {"t40", "t5"},
		3: {"t25"},
		4: {"t35"},
	}
	if !reflect.DeepEqual(gotDV, wantDV) {
		t.Fatalf("merged doc values: got %v, want %v", gotDV, wantDV)
	}

	// the chunk header must be sorted by doc number: chunkedContentCoder
	// requires ascending Add calls but never checks, and a violation shows up
	// only as a failed binary search later
	assertDocValueChunksSorted(t, &mergedSeg.SegmentBase, numTestFieldName)
}

// TestNumericIndexMergeAllDropped verifies that a field whose every document is
// deleted leaves no number_v2 section behind, and in particular no orphan doc
// value block.
func TestNumericIndexMergeAllDropped(t *testing.T) {
	path := getTempPath("numeric-alldropped.zap")
	docs := []numTestDoc{{id: "a", values: []uint64{1}}, {id: "b", values: []uint64{2}}}
	if err := persistNumericTestSegment(docs, path); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(path) }()

	segment, err := zapPlugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = segment.Close() }()

	drops := []*roaring.Bitmap{roaring.BitmapOf(0, 1)}

	mergedPath := getTempPath("numeric-alldropped-out.zap")
	_ = os.RemoveAll(mergedPath)
	defer func() { _ = os.RemoveAll(mergedPath) }()

	if _, _, err := zapPlugin.Merge([]seg.Segment{segment}, drops, mergedPath, nil, nil); err != nil {
		t.Fatal(err)
	}

	merged, err := zapPlugin.Open(mergedPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = merged.Close() }()

	if merged.Count() != 0 {
		t.Fatalf("expected an empty merged segment, got %d docs", merged.Count())
	}

	data, err := merged.(*Segment).NumericV2Data(numTestFieldName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Fatal("expected no numeric data after every document was dropped")
	}
}

// -----------------------------------------------------------------------------
// helpers

func readUvarintAt(mem []byte, pos uint64) (uint64, uint64) {
	v, n := binary.Uvarint(mem[pos:])
	return v, uint64(n)
}

// assertDocValueChunksSorted walks every doc value chunk for a field and checks
// that the per-chunk headers are in ascending doc number order.
func assertDocValueChunksSorted(t *testing.T, sb *SegmentBase, field string) {
	t.Helper()
	fieldIDPlus1, ok := sb.fieldsMap[field]
	if !ok {
		t.Fatalf("field %q not in segment", field)
	}
	dvr := sb.fieldDocValueReader(fieldIDPlus1 - 1)
	if dvr == nil {
		t.Fatalf("no doc value reader for field %q", field)
	}
	clone := dvr.cloneInto(nil)
	for chunk := 0; chunk < len(clone.chunkOffsets); chunk++ {
		if err := clone.loadDvChunk(uint64(chunk), sb); err != nil {
			t.Fatalf("loadDvChunk(%d): %v", chunk, err)
		}
		for i := 1; i < len(clone.curChunkHeader); i++ {
			if clone.curChunkHeader[i-1].DocNum >= clone.curChunkHeader[i].DocNum {
				t.Fatalf("chunk %d header not ascending: %d then %d", chunk,
					clone.curChunkHeader[i-1].DocNum, clone.curChunkHeader[i].DocNum)
			}
		}
	}
}
