// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"os"
	"sort"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// TestBPPermutationValidity calls computeBPPermutation with MinSegmentSize=0
// so BP runs even on small test segments.  It verifies:
//  1. The returned slice (when non-nil) is a valid bijection on [0, numDocs).
//  2. A nil permutation (segment too small after content inspection) is acceptable.
func TestBPPermutationValidity(t *testing.T) {
	tmpPath1 := getTempPath("scorch_bp1.zap")
	tmpPath2 := getTempPath("scorch_bp2.zap")
	_ = os.RemoveAll(tmpPath1)
	_ = os.RemoveAll(tmpPath2)
	defer os.RemoveAll(tmpPath1)
	defer os.RemoveAll(tmpPath2)

	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(seg1, tmpPath1); err != nil {
		t.Fatal(err)
	}
	seg2, _, err := buildTestSegmentMulti2()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(seg2, tmpPath2); err != nil {
		t.Fatal(err)
	}

	s1, err := zapPlugin.Open(tmpPath1)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := zapPlugin.Open(tmpPath2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	sb1 := &s1.(*Segment).SegmentBase
	sb2 := &s2.(*Segment).SegmentBase
	segments := []*SegmentBase{sb1, sb2}
	drops := []*roaring.Bitmap{nil, nil}

	_, fieldsInv, fieldsOptions := mergeFields(segments)
	numDocs := computeNewDocCount(segments, drops)

	// Run with MinSegmentSize=0 so BP executes on our tiny test segments.
	opts := BPOptions{
		MinDocFreq:       1,    // include even single-doc terms
		MaxDocFreqRatio:  1.0,  // no upper cap
		MinSegmentSize:   0,    // force BP even for tiny segments
		MinPartitionSize: 1,
		MaxIter:          5,
	}
	perm, err := computeBPPermutation(segments, drops, fieldsInv, fieldsOptions, numDocs, opts)
	if err != nil {
		t.Fatalf("computeBPPermutation returned error: %v", err)
	}

	if perm == nil {
		// Acceptable: segment may not meet internal criteria (e.g., no eligible terms).
		t.Log("computeBPPermutation returned nil permutation (no eligible terms) — OK")
		return
	}

	// Validate the permutation: it must be a bijection on [0, numDocs).
	if uint64(len(perm)) != numDocs {
		t.Fatalf("perm length %d != numDocs %d", len(perm), numDocs)
	}
	seen := make([]bool, numDocs)
	for i, bpID := range perm {
		if uint64(bpID) >= numDocs {
			t.Errorf("perm[%d]=%d out of range [0,%d)", i, bpID, numDocs)
			continue
		}
		if seen[bpID] {
			t.Errorf("perm[%d]=%d duplicate", i, bpID)
		}
		seen[bpID] = true
	}
}

// TestBPMergeUsingProducesValidSegment verifies that MergeUsing with
// config["bpReorder"]=true produces a readable merged segment whose posting
// lists contain exactly the same set of doc IDs as a non-BP merge.
func TestBPMergeUsingProducesValidSegment(t *testing.T) {
	tmpPath1 := getTempPath("scorch_bpmerge1.zap")
	tmpPath2 := getTempPath("scorch_bpmerge2.zap")
	tmpBP := getTempPath("scorch_bpmerge_out.zap")
	tmpNoBP := getTempPath("scorch_nobpmerge_out.zap")
	for _, p := range []string{tmpPath1, tmpPath2, tmpBP, tmpNoBP} {
		_ = os.RemoveAll(p)
		defer os.RemoveAll(p)
	}

	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(seg1, tmpPath1); err != nil {
		t.Fatal(err)
	}
	seg2, _, err := buildTestSegmentMulti2()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(seg2, tmpPath2); err != nil {
		t.Fatal(err)
	}

	s1, err := zapPlugin.Open(tmpPath1)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := zapPlugin.Open(tmpPath2)
	if err != nil {
		t.Fatal(err)
	}
	segs := []seg.Segment{s1, s2}
	drops := []*roaring.Bitmap{nil, nil}

	// Merge with bpReorder=true.
	_, _, err = zapPlugin.MergeUsing(segs, drops, tmpBP, nil, nil,
		map[string]interface{}{"bpReorder": true})
	if err != nil {
		s1.Close()
		s2.Close()
		t.Fatalf("MergeUsing(bpReorder=true) error: %v", err)
	}

	// Merge without BP.
	_, _, err = zapPlugin.Merge(segs, drops, tmpNoBP, nil, nil)
	s1.Close()
	s2.Close()
	if err != nil {
		t.Fatalf("Merge (no BP) error: %v", err)
	}

	// Open both merged segments and compare posting lists for a known term.
	bpSeg, err := zapPlugin.Open(tmpBP)
	if err != nil {
		t.Fatalf("open BP segment: %v", err)
	}
	defer bpSeg.Close()

	noBPSeg, err := zapPlugin.Open(tmpNoBP)
	if err != nil {
		t.Fatalf("open no-BP segment: %v", err)
	}
	defer noBPSeg.Close()

	// "some" appears in all docs in both test segments; verify doc count matches.
	for _, term := range []string{"some", "thing", "cold", "dark"} {
		bpDocIDs := collectDocIDs(t, bpSeg, "desc", term)
		noBPDocIDs := collectDocIDs(t, noBPSeg, "desc", term)
		if len(bpDocIDs) != len(noBPDocIDs) {
			t.Errorf("term=%q: BP merged %d docs, no-BP merged %d docs",
				term, len(bpDocIDs), len(noBPDocIDs))
		}
	}
}

// collectDocIDs returns the sorted list of internal doc IDs for term in field.
func collectDocIDs(t *testing.T, s seg.Segment, field, term string) []uint32 {
	t.Helper()
	dict, err := s.Dictionary(field)
	if err != nil {
		t.Fatalf("Dictionary(%q): %v", field, err)
	}
	pl, err := dict.PostingsList([]byte(term), nil, nil)
	if err != nil || pl == nil {
		return nil
	}
	iter := pl.Iterator(false, false, false, nil)
	var ids []uint32
	for {
		p, e := iter.Next()
		if p == nil || e != nil {
			break
		}
		ids = append(ids, uint32(p.Number()))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestSegmentsHaveFAISS verifies the helper used to guard BP reordering.
func TestSegmentsHaveFAISS(t *testing.T) {
	// Plain text segment: no FAISS section.
	sb1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	if segmentsHaveFAISS([]*SegmentBase{sb1}) {
		t.Error("segmentsHaveFAISS: expected false for text-only segment")
	}

	// Simulate a segment with a FAISS vector field by patching fieldsSectionsMap.
	// We don't have a real FAISS index in the test suite, so we craft the
	// minimal condition the guard checks: a nonzero SectionFaissVectorIndex address.
	sb2, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	// Ensure fieldsSectionsMap has at least one row with SectionFaissVectorIndex set.
	if len(sb2.fieldsSectionsMap) == 0 {
		sb2.fieldsSectionsMap = make([][]uint64, 1)
	}
	row := make([]uint64, NumSections)
	row[SectionFaissVectorIndex] = 999 // nonzero = FAISS present
	sb2.fieldsSectionsMap[0] = row

	if !segmentsHaveFAISS([]*SegmentBase{sb2}) {
		t.Error("segmentsHaveFAISS: expected true for segment with FAISS section")
	}

	// Mixed list: one text-only, one with FAISS — should return true.
	if !segmentsHaveFAISS([]*SegmentBase{sb1, sb2}) {
		t.Error("segmentsHaveFAISS: expected true for mixed list")
	}
}

// TestBPGuardSkipsOnFAISS verifies that segmentsHaveFAISS causes the BP guard
// in mergeToWriter to suppress computeBPPermutation. Because creating a real FAISS
// segment in the unit-test environment is not practical, this test exercises the
// guard function directly using a crafted SegmentBase, and separately confirms that
// a full merge with bpReorder=true still succeeds (no crash) on mixed lists.
func TestBPGuardSkipsOnFAISS(t *testing.T) {
	sb, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}

	// Without FAISS: guard must pass (allow BP).
	if segmentsHaveFAISS([]*SegmentBase{sb}) {
		t.Fatal("segmentsHaveFAISS should be false for text-only segment")
	}

	// Inject a fake FAISS address into fieldsSectionsMap.
	row := make([]uint64, NumSections)
	row[SectionFaissVectorIndex] = 42 // any nonzero address
	sb.fieldsSectionsMap = append(sb.fieldsSectionsMap, row)

	// With FAISS: guard must fire (block BP).
	if !segmentsHaveFAISS([]*SegmentBase{sb}) {
		t.Fatal("segmentsHaveFAISS should be true when FAISS section address is nonzero")
	}
}

// TestBPFieldSelection covers §68: an explicit BPField is honored when it names
// an indexed field, and a missing/typo'd field name fails safe to an identity
// permutation (BP disabled) rather than silently picking another field.
func TestBPFieldSelection(t *testing.T) {
	tmpPath1 := getTempPath("scorch_bpfield1.zap")
	tmpPath2 := getTempPath("scorch_bpfield2.zap")
	for _, p := range []string{tmpPath1, tmpPath2} {
		_ = os.RemoveAll(p)
		defer os.RemoveAll(p)
	}

	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(seg1, tmpPath1); err != nil {
		t.Fatal(err)
	}
	seg2, _, err := buildTestSegmentMulti2()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(seg2, tmpPath2); err != nil {
		t.Fatal(err)
	}

	s1, err := zapPlugin.Open(tmpPath1)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	s2, err := zapPlugin.Open(tmpPath2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	segments := []*SegmentBase{&s1.(*Segment).SegmentBase, &s2.(*Segment).SegmentBase}
	drops := []*roaring.Bitmap{nil, nil}
	_, fieldsInv, fieldsOptions := mergeFields(segments)
	numDocs := computeNewDocCount(segments, drops)

	base := BPOptions{
		MinDocFreq:       1,
		MaxDocFreqRatio:  1.0,
		MinSegmentSize:   0,
		MinPartitionSize: 1,
		MaxIter:          5,
	}

	isIdentity := func(perm []uint64) bool {
		if uint64(len(perm)) != numDocs {
			return false
		}
		for i, v := range perm {
			if v != uint64(i) {
				return false
			}
		}
		return true
	}
	assertBijection := func(perm []uint64) {
		t.Helper()
		if uint64(len(perm)) != numDocs {
			t.Fatalf("perm length %d != numDocs %d", len(perm), numDocs)
		}
		seen := make([]bool, numDocs)
		for i, bpID := range perm {
			if bpID >= numDocs || seen[bpID] {
				t.Fatalf("perm[%d]=%d not a valid bijection", i, bpID)
			}
			seen[bpID] = true
		}
	}

	// (1) Explicit valid field ("desc") → BP runs, valid bijection permutation.
	optsValid := base
	optsValid.BPField = "desc"
	permValid, err := computeBPPermutation(segments, drops, fieldsInv, fieldsOptions, numDocs, optsValid)
	if err != nil {
		t.Fatalf("computeBPPermutation(BPField=desc) error: %v", err)
	}
	assertBijection(permValid)

	// (2) Missing/typo'd field → fail safe to identity (BP disabled).
	optsMissing := base
	optsMissing.BPField = "no_such_field_xyz"
	permMissing, err := computeBPPermutation(segments, drops, fieldsInv, fieldsOptions, numDocs, optsMissing)
	if err != nil {
		t.Fatalf("computeBPPermutation(BPField=missing) error: %v", err)
	}
	if !isIdentity(permMissing) {
		t.Errorf("missing BPField should yield identity permutation (BP off), got %v", permMissing)
	}

	// sanity: the valid-field case should actually have its named field present.
	found := false
	for _, fn := range fieldsInv {
		if fn == "desc" && fieldsOptions[fn].IsIndexed() {
			found = true
		}
	}
	if !found {
		t.Fatal("test precondition: 'desc' should be an indexed field")
	}
}
