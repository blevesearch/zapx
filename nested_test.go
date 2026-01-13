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
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// stubNestedDocument implements both Document and NestedDocument interfaces
type stubNestedDocument struct {
	id        string
	fields    []*stubField
	composite []*stubField
	children  []index.Document
}

func (s *stubNestedDocument) StoredFieldsBytes() uint64 {
	return 0
}

func (s *stubNestedDocument) ID() string {
	return s.id
}

func (s *stubNestedDocument) Size() int {
	return 0
}

func (s *stubNestedDocument) VisitFields(visitor index.FieldVisitor) {
	for _, f := range s.fields {
		visitor(f)
	}
}

func (s *stubNestedDocument) HasComposite() bool {
	return len(s.composite) > 0
}

func (s *stubNestedDocument) VisitComposite(visitor index.CompositeFieldVisitor) {
	for _, c := range s.composite {
		visitor(c)
	}
}

func (s *stubNestedDocument) NumPlainTextBytes() uint64 {
	return 0
}

func (s *stubNestedDocument) AddIDField() {
}

func (s *stubNestedDocument) Indexed() bool {
	return true
}

func (s *stubNestedDocument) VisitNestedDocuments(visitor func(doc index.Document)) {
	for _, child := range s.children {
		visitor(child)
	}
}

func newStubNestedDocument(id string, fields []*stubField, children []index.Document) *stubNestedDocument {
	rv := &stubNestedDocument{
		id:       id,
		fields:   fields,
		children: children,
	}
	// fixup composite (_all field)
	cf := &stubField{
		name:           "_all",
		value:          nil,
		arrayPositions: nil,
		encodedType:    'c',
		options:        index.IndexField | index.IncludeTermVectors,
		analyzedLen:    0,
		analyzedFreqs:  make(index.TokenFrequencies),
	}
	for _, f := range rv.fields {
		if f.name == "_id" {
			continue
		}
		cf.analyzedLen += f.analyzedLen
		cf.analyzedFreqs.MergeAll(f.name, f.analyzedFreqs)
	}
	rv.composite = []*stubField{cf}
	return rv
}

type testNestedDoc struct {
	id       string
	fields   map[string]string
	children []*testNestedDoc
}

func buildNestedDocument(def *testNestedDoc) *stubNestedDocument {
	var fields []*stubField
	// Always add _id field
	fields = append(fields, newStubFieldSplitString("_id", nil, def.id, true, false, false))
	for fieldName, value := range def.fields {
		fields = append(fields, newStubFieldSplitString(fieldName, nil, value, true, false, true))
	}
	// Build children recursively
	var children []index.Document
	for _, childDef := range def.children {
		children = append(children, buildNestedDocument(childDef))
	}
	return newStubNestedDocument(def.id, fields, children)
}

func buildTestSegmentForNested(results []index.Document) (*SegmentBase, error) {
	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode)
	if err != nil {
		return nil, err
	}
	return seg.(*SegmentBase), nil
}

// buildNestedSegment builds and persists a segment from test definitions, then reopens it
func buildNestedSegment(testDocs []*testNestedDoc) (segment.Segment, string, error) {
	var docs []index.Document
	for _, def := range testDocs {
		docs = append(docs, buildNestedDocument(def))
	}

	sb, err := buildTestSegmentForNested(docs)
	if err != nil {
		return nil, "", err
	}

	tmpDir, err := os.MkdirTemp("", "zap-nested-")
	if err != nil {
		return nil, "", err
	}
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return nil, "", err
	}

	err = PersistSegmentBase(sb, tmpDir)
	if err != nil {
		return nil, "", err
	}

	seg, err := zapPlugin.Open(tmpDir)
	if err != nil {
		return nil, "", err
	}

	return seg, tmpDir, nil
}

// mergeNestedSegments merges segments with optional drops and validates result
func mergeNestedSegments(segs []segment.Segment, drops []*roaring.Bitmap) (string, segment.Segment, error) {
	tmpDir, err := os.MkdirTemp("", "mergedzap-nested-")
	if err != nil {
		return "", nil, err
	}
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return "", nil, err
	}

	_, _, err = zapPlugin.Merge(segs, drops, tmpDir, nil, nil)
	if err != nil {
		return "", nil, err
	}

	seg, err := zapPlugin.Open(tmpDir)
	if err != nil {
		return "", nil, err
	}

	return tmpDir, seg, nil
}

func countFlattenedDocs(defs []*testNestedDoc) uint64 {
	var count uint64
	for _, def := range defs {
		count += countFlattenedDoc(def)
	}
	return count
}

func countFlattenedDoc(def *testNestedDoc) uint64 {
	count := uint64(1)
	for _, child := range def.children {
		count += countFlattenedDoc(child)
	}
	return count
}

func countNestedDocsOnly(defs []*testNestedDoc) uint64 {
	var count uint64
	for _, def := range defs {
		count += countNestedDocOnly(def)
	}
	return count
}

func countNestedDocOnly(def *testNestedDoc) uint64 {
	var count uint64
	for _, child := range def.children {
		count += 1 + countNestedDocOnly(child)
	}
	return count
}

func TestNestedSegment(t *testing.T) {
	// Test data structure:
	// company (root)
	// ├── departments[] (nested level 1)
	// │     ├── employees[] (nested level 2)
	// │     └── projects[] (nested level 2)
	// └── locations[] (nested level 1)

	testDocs := []*testNestedDoc{
		{
			id: "company1",
			fields: map[string]string{
				"name": "TechCorp",
				"type": "technology",
			},
			children: []*testNestedDoc{
				{
					id: "dept1",
					fields: map[string]string{
						"dept_name": "Engineering",
						"budget":    "high",
					},
					children: []*testNestedDoc{
						{
							id:     "emp1",
							fields: map[string]string{"emp_name": "Alice", "role": "Engineer"},
						},
						{
							id:     "emp2",
							fields: map[string]string{"emp_name": "Bob", "role": "Manager"},
						},
					},
				},
				{
					id: "dept2",
					fields: map[string]string{
						"dept_name": "Sales",
						"budget":    "medium",
					},
					children: []*testNestedDoc{
						{
							id:     "emp3",
							fields: map[string]string{"emp_name": "Eve", "role": "Salesperson"},
						},
					},
				},
				{
					id:     "loc1",
					fields: map[string]string{"city": "Athens", "country": "Greece"},
				},
			},
		},
		{
			id: "company2",
			fields: map[string]string{
				"name": "BizInc",
				"type": "consulting",
			},
			children: []*testNestedDoc{
				{
					id: "dept3",
					fields: map[string]string{
						"dept_name": "Marketing",
						"budget":    "high",
					},
					children: []*testNestedDoc{
						{
							id:     "emp4",
							fields: map[string]string{"emp_name": "Frank", "role": "Marketer"},
						},
					},
				},
				{
					id:     "loc2",
					fields: map[string]string{"city": "London", "country": "UK"},
				},
			},
		},
	}

	// Single segment test
	seg, dir, err := buildNestedSegment(testDocs)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	defer func() {
		cerr := seg.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		rerr := os.RemoveAll(dir)
		if rerr != nil {
			t.Fatalf("error removing dir: %v", rerr)
		}
	}()

	sb, ok := seg.(*Segment)
	if !ok {
		t.Fatalf("expected *Segment, got %T", seg)
	}

	// Verify total document count
	expectedTotalDocs := countFlattenedDocs(testDocs)
	if sb.Count() != expectedTotalDocs {
		t.Fatalf("expected %d total docs, got %d", expectedTotalDocs, sb.Count())
	}

	// Verify nested document count
	expectedNestedDocs := countNestedDocsOnly(testDocs)
	actualNestedDocs := sb.countNested()
	if actualNestedDocs != expectedNestedDocs {
		t.Fatalf("expected %d nested docs, got %d", expectedNestedDocs, actualNestedDocs)
	}

	// Verify edge list exists
	el := sb.EdgeList()
	if el == nil {
		t.Fatal("expected non-nil edge list")
	}
	if el.Count() != expectedNestedDocs {
		t.Fatalf("expected edge list count %d, got %d", expectedNestedDocs, el.Count())
	}

	// Test ancestry lookups
	// Document layout (preorder):
	// 0: company1 (root)
	// 1: dept1 (child of 0)
	// 2: emp1 (child of 1)
	// 3: emp2 (child of 1)
	// 4: dept2 (child of 0)
	// 5: emp3 (child of 4)
	// 6: loc1 (child of 0)
	// 7: company2 (root)
	// 8: dept3 (child of 7)
	// 9: emp4 (child of 8)
	// 10: loc2 (child of 7)

	testAncestry := []struct {
		docNum           uint64
		expectedAncestry []uint64
	}{
		{0, []uint64{0}},       // company1 is root
		{1, []uint64{1, 0}},    // dept1 -> company1
		{2, []uint64{2, 1, 0}}, // emp1 -> dept1 -> company1
		{3, []uint64{3, 1, 0}}, // emp2 -> dept1 -> company1
		{4, []uint64{4, 0}},    // dept2 -> company1
		{5, []uint64{5, 4, 0}}, // emp3 -> dept2 -> company1
		{6, []uint64{6, 0}},    // loc1 -> company1
		{7, []uint64{7}},       // company2 is root
		{8, []uint64{8, 7}},    // dept3 -> company2
		{9, []uint64{9, 8, 7}}, // emp4 -> dept3 -> company2
		{10, []uint64{10, 7}},  // loc2 -> company2
	}

	for _, tc := range testAncestry {
		ancestry := sb.Ancestors(tc.docNum, nil)
		if len(ancestry) != len(tc.expectedAncestry) {
			t.Errorf("docNum %d: expected %d ancestors, got %d", tc.docNum, len(tc.expectedAncestry), len(ancestry))
			continue
		}
		for i, expected := range tc.expectedAncestry {
			if uint64(ancestry[i]) != expected {
				t.Errorf("docNum %d: ancestor[%d] expected %d, got %d", tc.docNum, i, expected, uint64(ancestry[i]))
			}
		}
	}

	rootCount := sb.CountRoot(nil)
	expectedRootCount := uint64(2) // company1 and company2
	if rootCount != expectedRootCount {
		t.Fatalf("expected %d root docs, got %d", expectedRootCount, rootCount)
	}

	deleted := roaring.New()
	deleted.Add(0) // delete company1 (root)
	rootCountWithDeletes := sb.CountRoot(deleted)
	if rootCountWithDeletes != 1 { // only company2 remains
		t.Fatalf("expected 1 root doc after delete, got %d", rootCountWithDeletes)
	}

	// Test AddNestedDocuments - when root is deleted, children should be added
	drops := roaring.New()
	drops.Add(0) // delete company1
	dropsWithNested := sb.AddNestedDocuments(drops)
	// Should now contain: 0, 1, 2, 3, 4, 5, 6 (company1 and all its nested docs)
	expectedDrops := []uint32{0, 1, 2, 3, 4, 5, 6}
	for _, docNum := range expectedDrops {
		if !dropsWithNested.Contains(docNum) {
			t.Errorf("expected doc %d to be in drops after AddNestedDocuments", docNum)
		}
	}
	// company2 and its children should NOT be in drops
	notExpected := []uint32{7, 8, 9, 10}
	for _, docNum := range notExpected {
		if dropsWithNested.Contains(docNum) {
			t.Errorf("did not expect doc %d to be in drops", docNum)
		}
	}
}

func TestNestedSegmentMerge(t *testing.T) {
	// Test merging multiple segments with nested documents

	// Segment 1: company1 with nested docs
	testDocs1 := []*testNestedDoc{
		{
			id:     "company1",
			fields: map[string]string{"name": "TechCorp"},
			children: []*testNestedDoc{
				{
					id:     "dept1",
					fields: map[string]string{"dept_name": "Engineering"},
					children: []*testNestedDoc{
						{id: "emp1", fields: map[string]string{"emp_name": "Alice"}},
						{id: "emp2", fields: map[string]string{"emp_name": "Bob"}},
					},
				},
			},
		},
	}

	// Segment 2: company2 with nested docs
	testDocs2 := []*testNestedDoc{
		{
			id:     "company2",
			fields: map[string]string{"name": "BizInc"},
			children: []*testNestedDoc{
				{
					id:     "dept2",
					fields: map[string]string{"dept_name": "Sales"},
					children: []*testNestedDoc{
						{id: "emp3", fields: map[string]string{"emp_name": "Charlie"}},
					},
				},
			},
		},
	}

	// Segment 3: company3 with flat docs only (no nesting)
	testDocs3 := []*testNestedDoc{
		{
			id:     "company3",
			fields: map[string]string{"name": "FlatCorp"},
			// no children
		},
	}

	// Build individual segments
	seg1, dir1, err := buildNestedSegment(testDocs1)
	if err != nil {
		t.Fatalf("error building segment 1: %v", err)
	}
	seg2, dir2, err := buildNestedSegment(testDocs2)
	if err != nil {
		t.Fatalf("error building segment 2: %v", err)
	}
	seg3, dir3, err := buildNestedSegment(testDocs3)
	if err != nil {
		t.Fatalf("error building segment 3: %v", err)
	}

	// Verify individual segment nested counts
	sb1 := seg1.(*Segment)
	if sb1.countNested() != 3 { // dept1, emp1, emp2
		t.Fatalf("seg1: expected 3 nested docs, got %d", sb1.countNested())
	}
	sb2 := seg2.(*Segment)
	if sb2.countNested() != 2 { // dept2, emp3
		t.Fatalf("seg2: expected 2 nested docs, got %d", sb2.countNested())
	}
	sb3 := seg3.(*Segment)
	if sb3.countNested() != 0 { // no nested docs
		t.Fatalf("seg3: expected 0 nested docs, got %d", sb3.countNested())
	}

	// Verify total counts
	expectedTotal1 := countFlattenedDocs(testDocs1)
	if sb1.Count() != expectedTotal1 {
		t.Fatalf("seg1: expected %d total docs, got %d", expectedTotal1, sb1.Count())
	}

	expectedTotal2 := countFlattenedDocs(testDocs2)
	if sb2.Count() != expectedTotal2 {
		t.Fatalf("seg2: expected %d total docs, got %d", expectedTotal2, sb2.Count())
	}

	expectedTotal3 := countFlattenedDocs(testDocs3)
	if sb3.Count() != expectedTotal3 {
		t.Fatalf("seg3: expected %d total docs, got %d", expectedTotal3, sb3.Count())
	}

	// Merge without drops
	drops := []*roaring.Bitmap{roaring.New(), roaring.New(), roaring.New()}
	mergeDir, mergedSeg, err := mergeNestedSegments(
		[]segment.Segment{seg1, seg2, seg3},
		drops,
	)
	if err != nil {
		t.Fatalf("error merging segments: %v", err)
	}

	// Verify merged segment
	mergedSb := mergedSeg.(*Segment)
	expectedTotal := countFlattenedDocs(testDocs1) + countFlattenedDocs(testDocs2) + countFlattenedDocs(testDocs3)
	if mergedSb.Count() != expectedTotal {
		t.Fatalf("merged: expected %d total docs, got %d", expectedTotal, mergedSb.Count())
	}

	expectedNested := countNestedDocsOnly(testDocs1) + countNestedDocsOnly(testDocs2) + countNestedDocsOnly(testDocs3)
	if mergedSb.countNested() != expectedNested {
		t.Fatalf("merged: expected %d nested docs, got %d", expectedNested, mergedSb.countNested())
	}

	// Verify CountRoot
	expectedRootCount := uint64(3) // company1, company2, company3
	rootCount := mergedSb.CountRoot(nil)
	if rootCount != expectedRootCount {
		t.Fatalf("merged: expected %d root docs, got %d", expectedRootCount, rootCount)
	}

	// Verify edge list in merged segment
	el := mergedSb.EdgeList()
	if el == nil {
		t.Fatal("expected non-nil edge list in merged segment")
	}
	if el.Count() != expectedNested {
		t.Fatalf("merged edge list: expected %d edges, got %d", expectedNested, el.Count())
	}

	// Verify ancestry in merged segment
	testAncestry := []struct {
		docNum           uint64
		expectedAncestry []uint64
	}{
		{0, []uint64{0}},       // company1
		{1, []uint64{1, 0}},    // dept1 -> company1
		{2, []uint64{2, 1, 0}}, // emp1 -> dept1 -> company1
		{3, []uint64{3, 1, 0}}, // emp2 -> dept1 -> company1
		{4, []uint64{4}},       // company2
		{5, []uint64{5, 4}},    // dept2 -> company2
		{6, []uint64{6, 5, 4}}, // emp3 -> dept2 -> company2
		{7, []uint64{7}},       // company3
	}

	for _, tc := range testAncestry {
		ancestry := mergedSb.Ancestors(tc.docNum, nil)
		if len(ancestry) != len(tc.expectedAncestry) {
			t.Errorf("merged docNum %d: expected %d ancestors, got %d", tc.docNum, len(tc.expectedAncestry), len(ancestry))
			continue
		}
		for i, expected := range tc.expectedAncestry {
			if uint64(ancestry[i]) != expected {
				t.Errorf("merged docNum %d: ancestor[%d] expected %d, got %d", tc.docNum, i, expected, uint64(ancestry[i]))
			}
		}
	}

	// Cleanup
	if err := mergedSeg.Close(); err != nil {
		t.Fatalf("error closing merged segment: %v", err)
	}
	if err := os.RemoveAll(mergeDir); err != nil {
		t.Fatalf("error removing merge dir: %v", err)
	}
	for i, seg := range []segment.Segment{seg1, seg2, seg3} {
		if err := seg.Close(); err != nil {
			t.Fatalf("error closing segment %d: %v", i, err)
		}
	}
	for i, dir := range []string{dir1, dir2, dir3} {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("error removing dir %d: %v", i, err)
		}
	}
}

func TestNestedSegmentMergeWithDeletes(t *testing.T) {
	testDocs1 := []*testNestedDoc{
		{
			id:     "company1",
			fields: map[string]string{"name": "TechCorp"},
			children: []*testNestedDoc{
				{id: "dept1", fields: map[string]string{"dept_name": "Engineering"}},
				{id: "emp1", fields: map[string]string{"emp_name": "Alice"}},
			},
		},
	}

	testDocs2 := []*testNestedDoc{
		{
			id:     "company2",
			fields: map[string]string{"name": "BizInc"},
			children: []*testNestedDoc{
				{id: "dept2", fields: map[string]string{"dept_name": "Sales"}},
			},
		},
	}

	seg1, dir1, err := buildNestedSegment(testDocs1)
	if err != nil {
		t.Fatalf("error building segment 1: %v", err)
	}
	seg2, dir2, err := buildNestedSegment(testDocs2)
	if err != nil {
		t.Fatalf("error building segment 2: %v", err)
	}

	// Delete company1 (doc 0) from segment 1 - this should cascade to dept1 and emp1
	drops1 := roaring.New()
	drops1.Add(0) // company1
	// Use AddNestedDocuments to cascade deletes
	sb1 := seg1.(*Segment)
	drops1 = sb1.AddNestedDocuments(drops1)
	// Verify cascaded deletes
	if !drops1.Contains(1) || !drops1.Contains(2) {
		t.Fatal("AddNestedDocuments should have added children to drops")
	}

	drops2 := roaring.New() // no deletes in segment 2

	// Merge with deletes
	mergeDir, mergedSeg, err := mergeNestedSegments(
		[]segment.Segment{seg1, seg2},
		[]*roaring.Bitmap{drops1, drops2},
	)
	if err != nil {
		t.Fatalf("error merging segments: %v", err)
	}

	// Verify merged segment only has company2 and its children
	mergedSb := mergedSeg.(*Segment)
	// seg1 had 3 docs (company1, dept1, emp1) all deleted
	// seg2 had 2 docs (company2, dept2)
	// merged should have 2 docs
	if mergedSb.Count() != 2 {
		t.Fatalf("expected 2 docs in merged segment, got %d", mergedSb.Count())
	}
	// Should have 1 nested doc (dept2)
	if mergedSb.countNested() != 1 {
		t.Fatalf("expected 1 nested doc in merged segment, got %d", mergedSb.countNested())
	}
	// CoutntRoot should be 1 (company2)
	rootCount := mergedSb.CountRoot(nil)
	if rootCount != 1 {
		t.Fatalf("expected 1 root doc in merged segment, got %d", rootCount)
	}
	// Should have 1 edge in edge list
	el := mergedSb.EdgeList()
	if el == nil {
		t.Fatal("expected non-nil edge list in merged segment")
	}

	if el.Count() != 1 {
		t.Fatalf("merged edge list: expected 1 edge, got %d", el.Count())
	}
	// Verify ancestry
	testAncestry := []struct {
		docNum           uint64
		expectedAncestry []uint64
	}{
		{0, []uint64{0}},    // company2
		{1, []uint64{1, 0}}, // dept2 -> company2
	}

	for _, tc := range testAncestry {
		ancestry := mergedSb.Ancestors(tc.docNum, nil)
		if len(ancestry) != len(tc.expectedAncestry) {
			t.Errorf("merged docNum %d: expected %d ancestors, got %d", tc.docNum, len(tc.expectedAncestry), len(ancestry))
			continue
		}
		for i, expected := range tc.expectedAncestry {
			if uint64(ancestry[i]) != expected {
				t.Errorf("merged docNum %d: ancestor[%d] expected %d, got %d", tc.docNum, i, expected, uint64(ancestry[i]))
			}
		}
	}

	// Cleanup
	if err := mergedSeg.Close(); err != nil {
		t.Fatalf("error closing merged segment: %v", err)
	}
	if err := os.RemoveAll(mergeDir); err != nil {
		t.Fatalf("error removing merge dir: %v", err)
	}
	if err := seg1.Close(); err != nil {
		t.Fatalf("error closing segment 1: %v", err)
	}
	if err := seg2.Close(); err != nil {
		t.Fatalf("error closing segment 2: %v", err)
	}
	if err := os.RemoveAll(dir1); err != nil {
		t.Fatalf("error removing dir1: %v", err)
	}
	if err := os.RemoveAll(dir2); err != nil {
		t.Fatalf("error removing dir2: %v", err)
	}
}

func TestNestedSegmentDeepHierarchy(t *testing.T) {
	// Test with deeper nesting levels (3+ levels)

	testDocs := []*testNestedDoc{
		{
			id:     "root",
			fields: map[string]string{"name": "Root"},
			children: []*testNestedDoc{
				{
					id:     "level1",
					fields: map[string]string{"name": "Level1"},
					children: []*testNestedDoc{
						{
							id:     "level2",
							fields: map[string]string{"name": "Level2"},
							children: []*testNestedDoc{
								{
									id:     "level3",
									fields: map[string]string{"name": "Level3"},
									children: []*testNestedDoc{
										{
											id:     "level4",
											fields: map[string]string{"name": "Level4"},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	seg, dir, err := buildNestedSegment(testDocs)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	defer func() {
		if cerr := seg.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		if rerr := os.RemoveAll(dir); rerr != nil {
			t.Fatalf("error removing dir: %v", rerr)
		}
	}()

	sb := seg.(*Segment)

	// Total docs: root, level1, level2, level3, level4 = 5
	if sb.Count() != 5 {
		t.Fatalf("expected 5 docs, got %d", sb.Count())
	}

	// Nested docs: level1, level2, level3, level4 = 4
	if sb.countNested() != 4 {
		t.Fatalf("expected 4 nested docs, got %d", sb.countNested())
	}

	// Test ancestry for deepest document (level4)
	// Document layout: 0=root, 1=level1, 2=level2, 3=level3, 4=level4
	ancestry := sb.Ancestors(4, nil)
	expectedAncestry := []uint64{4, 3, 2, 1, 0}
	if len(ancestry) != len(expectedAncestry) {
		t.Fatalf("expected %d ancestors, got %d", len(expectedAncestry), len(ancestry))
	}
	for i, expected := range expectedAncestry {
		if uint64(ancestry[i]) != expected {
			t.Errorf("ancestor[%d] expected %d, got %d", i, expected, uint64(ancestry[i]))
		}
	}

	// Test AddNestedDocuments - deleting root should cascade to all
	drops := roaring.New()
	drops.Add(0) // delete root
	dropsWithNested := sb.AddNestedDocuments(drops)
	for i := uint32(0); i <= 4; i++ {
		if !dropsWithNested.Contains(i) {
			t.Errorf("expected doc %d to be in drops", i)
		}
	}
}

func TestNestedSegmentMultipleRootsWithChildren(t *testing.T) {
	// Test multiple root documents each with different nesting structures

	testDocs := []*testNestedDoc{
		{
			id:     "root1",
			fields: map[string]string{"name": "Root1"},
			children: []*testNestedDoc{
				{id: "child1a", fields: map[string]string{"name": "Child1A"}},
				{id: "child1b", fields: map[string]string{"name": "Child1B"}},
			},
		},
		{
			id:     "root2",
			fields: map[string]string{"name": "Root2"},
			// no children - flat document
		},
		{
			id:     "root3",
			fields: map[string]string{"name": "Root3"},
			children: []*testNestedDoc{
				{
					id:     "child3a",
					fields: map[string]string{"name": "Child3A"},
					children: []*testNestedDoc{
						{id: "grandchild3a1", fields: map[string]string{"name": "Grandchild3A1"}},
					},
				},
			},
		},
	}

	seg, dir, err := buildNestedSegment(testDocs)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	defer func() {
		if cerr := seg.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		if rerr := os.RemoveAll(dir); rerr != nil {
			t.Fatalf("error removing dir: %v", rerr)
		}
	}()

	sb := seg.(*Segment)

	// Document layout:
	// 0: root1
	// 1: child1a (child of 0)
	// 2: child1b (child of 0)
	// 3: root2 (no children)
	// 4: root3
	// 5: child3a (child of 4)
	// 6: grandchild3a1 (child of 5)

	// Total: 7 docs
	if sb.Count() != 7 {
		t.Fatalf("expected 7 docs, got %d", sb.Count())
	}

	// Nested: child1a, child1b, child3a, grandchild3a1 = 4
	if sb.countNested() != 4 {
		t.Fatalf("expected 4 nested docs, got %d", sb.countNested())
	}

	// Root count: root1, root2, root3 = 3
	if sb.CountRoot(nil) != 3 {
		t.Fatalf("expected 3 root docs, got %d", sb.CountRoot(nil))
	}

	// Test partial delete - delete root1, should cascade to child1a, child1b
	drops := roaring.New()
	drops.Add(0) // delete root1
	dropsWithNested := sb.AddNestedDocuments(drops)

	// Should contain 0, 1, 2
	for _, d := range []uint32{0, 1, 2} {
		if !dropsWithNested.Contains(d) {
			t.Errorf("expected doc %d in drops", d)
		}
	}
	// Should NOT contain 3, 4, 5, 6
	for _, d := range []uint32{3, 4, 5, 6} {
		if dropsWithNested.Contains(d) {
			t.Errorf("did not expect doc %d in drops", d)
		}
	}
}

func TestNestedSegmentEdgeListIteration(t *testing.T) {
	// Test EdgeList iteration functionality

	testDocs := []*testNestedDoc{
		{
			id:     "root",
			fields: map[string]string{"name": "Root"},
			children: []*testNestedDoc{
				{id: "child1", fields: map[string]string{"name": "Child1"}},
				{id: "child2", fields: map[string]string{"name": "Child2"}},
				{
					id:     "child3",
					fields: map[string]string{"name": "Child3"},
					children: []*testNestedDoc{
						{id: "grandchild1", fields: map[string]string{"name": "Grandchild1"}},
					},
				},
			},
		},
	}

	seg, dir, err := buildNestedSegment(testDocs)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	defer func() {
		if cerr := seg.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		if rerr := os.RemoveAll(dir); rerr != nil {
			t.Fatalf("error removing dir: %v", rerr)
		}
	}()

	sb := seg.(*Segment)
	el := sb.EdgeList()
	if el == nil {
		t.Fatal("expected non-nil edge list")
	}

	// Document layout:
	// 0: root
	// 1: child1 (parent: 0)
	// 2: child2 (parent: 0)
	// 3: child3 (parent: 0)
	// 4: grandchild1 (parent: 3)

	expectedEdges := map[uint64]uint64{
		1: 0, // child1 -> root
		2: 0, // child2 -> root
		3: 0, // child3 -> root
		4: 3, // grandchild1 -> child3
	}

	// Verify edge count
	if el.Count() != uint64(len(expectedEdges)) {
		t.Fatalf("expected %d edges, got %d", len(expectedEdges), el.Count())
	}

	// Verify Parent lookups
	for child, expectedParent := range expectedEdges {
		parent, ok := el.Parent(child)
		if !ok {
			t.Errorf("expected parent for child %d", child)
			continue
		}
		if parent != expectedParent {
			t.Errorf("child %d: expected parent %d, got %d", child, expectedParent, parent)
		}
	}

	// Verify root has no parent
	if _, ok := el.Parent(0); ok {
		t.Error("root should have no parent")
	}

	// Test iteration
	foundEdges := make(map[uint64]uint64)
	el.Iterate(func(child uint64, parent uint64) bool {
		foundEdges[child] = parent
		return true
	})

	if len(foundEdges) != len(expectedEdges) {
		t.Fatalf("iteration: expected %d edges, found %d", len(expectedEdges), len(foundEdges))
	}
	for child, parent := range expectedEdges {
		if foundEdges[child] != parent {
			t.Errorf("iteration: child %d expected parent %d, got %d", child, parent, foundEdges[child])
		}
	}
}

func TestNestedSegmentNoNesting(t *testing.T) {
	testDocs := []*testNestedDoc{
		{id: "doc1", fields: map[string]string{"name": "Doc1"}},
		{id: "doc2", fields: map[string]string{"name": "Doc2"}},
		{id: "doc3", fields: map[string]string{"name": "Doc3"}},
	}

	seg, dir, err := buildNestedSegment(testDocs)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	defer func() {
		if cerr := seg.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		if rerr := os.RemoveAll(dir); rerr != nil {
			t.Fatalf("error removing dir: %v", rerr)
		}
	}()

	sb := seg.(*Segment)

	// All docs are root docs
	if sb.Count() != 3 {
		t.Fatalf("expected 3 docs, got %d", sb.Count())
	}
	if sb.countNested() != 0 {
		t.Fatalf("expected 0 nested docs, got %d", sb.countNested())
	}
	if sb.CountRoot(nil) != 3 {
		t.Fatalf("expected 3 root docs, got %d", sb.CountRoot(nil))
	}

	// Edge list should be nil for flat documents
	el := sb.EdgeList()
	if el != nil {
		t.Fatal("expected nil edge list for flat documents")
	}

	// Ancestry for any doc should just return itself
	for i := uint64(0); i < 3; i++ {
		ancestry := sb.Ancestors(i, nil)
		if len(ancestry) != 1 {
			t.Errorf("doc %d: expected 1 ancestor (self), got %d", i, len(ancestry))
		}
		if len(ancestry) > 0 && uint64(ancestry[0]) != i {
			t.Errorf("doc %d: expected self in ancestry, got %d", i, uint64(ancestry[0]))
		}
	}

	// AddNestedDocuments should not modify drops for flat docs
	drops := roaring.New()
	drops.Add(0)
	dropsWithNested := sb.AddNestedDocuments(drops)
	if dropsWithNested.GetCardinality() != 1 {
		t.Errorf("expected drops to have 1 element, got %d", dropsWithNested.GetCardinality())
	}
}

func TestNestedSegmentMixedMerge(t *testing.T) {
	nestedDocs := []*testNestedDoc{
		{
			id:     "nested_root",
			fields: map[string]string{"name": "NestedRoot"},
			children: []*testNestedDoc{
				{id: "nested_child", fields: map[string]string{"name": "NestedChild"}},
			},
		},
	}

	// Segment without nested docs
	flatDocs := []*testNestedDoc{
		{id: "flat1", fields: map[string]string{"name": "Flat1"}},
		{id: "flat2", fields: map[string]string{"name": "Flat2"}},
	}

	seg1, dir1, err := buildNestedSegment(nestedDocs)
	if err != nil {
		t.Fatalf("error building nested segment: %v", err)
	}
	seg2, dir2, err := buildNestedSegment(flatDocs)
	if err != nil {
		t.Fatalf("error building flat segment: %v", err)
	}

	// Verify pre-merge state
	sb1 := seg1.(*Segment)
	sb2 := seg2.(*Segment)
	if sb1.countNested() != 1 {
		t.Fatalf("seg1: expected 1 nested doc, got %d", sb1.countNested())
	}
	if sb2.countNested() != 0 {
		t.Fatalf("seg2: expected 0 nested docs, got %d", sb2.countNested())
	}

	// Merge
	drops := []*roaring.Bitmap{roaring.New(), roaring.New()}
	mergeDir, mergedSeg, err := mergeNestedSegments([]segment.Segment{seg1, seg2}, drops)
	if err != nil {
		t.Fatalf("error merging: %v", err)
	}

	mergedSb := mergedSeg.(*Segment)

	// Total: 2 (1 root + 1 nested) + 2 (flat) = 4
	if mergedSb.Count() != 4 {
		t.Fatalf("expected 4 docs, got %d", mergedSb.Count())
	}

	// Nested: 1 (nested_child)
	if mergedSb.countNested() != 1 {
		t.Fatalf("expected 1 nested doc, got %d", mergedSb.countNested())
	}

	// Root: 3 (nested_root, flat1, flat2)
	if mergedSb.CountRoot(nil) != 3 {
		t.Fatalf("expected 3 root docs, got %d", mergedSb.CountRoot(nil))
	}

	// Cleanup
	if err := mergedSeg.Close(); err != nil {
		t.Fatalf("error closing merged segment: %v", err)
	}
	if err := os.RemoveAll(mergeDir); err != nil {
		t.Fatalf("error removing merge dir: %v", err)
	}
	if err := seg1.Close(); err != nil {
		t.Fatalf("error closing seg1: %v", err)
	}
	if err := seg2.Close(); err != nil {
		t.Fatalf("error closing seg2: %v", err)
	}
	if err := os.RemoveAll(dir1); err != nil {
		t.Fatalf("error removing dir1: %v", err)
	}
	if err := os.RemoveAll(dir2); err != nil {
		t.Fatalf("error removing dir2: %v", err)
	}
}
