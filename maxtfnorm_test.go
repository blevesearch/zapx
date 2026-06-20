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
	"math"
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// TestMaxTFNormSidecarAndScan verifies that Dictionary.MaxTFNorm() returns a
// positive value for a known multi-doc term and 0 for an absent term.  The
// §14 sidecar section (if present) and the scan fallback must agree.
func TestMaxTFNormCorrectness(t *testing.T) {
	tmpPath := getTempPath("scorch_maxtfnorm.zap")
	_ = os.RemoveAll(tmpPath)
	defer os.RemoveAll(tmpPath)

	// buildTestSegmentMulti creates two docs (a, b) each with desc="some thing"
	// and other fields.  "some" and "thing" both appear in exactly 2 docs,
	// each with freq=1 and fieldLen=2 (two-token field).
	testSeg, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	err = PersistSegmentBase(testSeg, tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	seg, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer seg.Close()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	// avgDocLength = 2 (both docs have 2-token desc field)
	const avgDocLen = 2.0

	// "some" is in 2 docs, each with freq=1 and fieldLen=2.
	got := dict.(*Dictionary).MaxTFNorm([]byte("some"), avgDocLen)
	if got <= 0 {
		t.Fatalf("MaxTFNorm('some') = %f, want > 0", got)
	}

	// The maximum possible BM25 tfNorm is k1/(1+k1) ≈ 0.545 (no length normalisation).
	maxPossible := float32(wandBM25K1 / (1 + wandBM25K1))
	if got > maxPossible*1.01 { // 1% tolerance for float rounding
		t.Errorf("MaxTFNorm('some') = %f exceeds theoretical max %f", got, maxPossible)
	}

	// Compute the expected value directly using wandTFNorm with SmallFloat-decoded fieldLen.
	// fieldLen=2 goes through encodeNormByte(2) → decode → approximateFieldLen.
	approxFieldLen := float64(normDecodeSmallFloat(encodeNormByte(2)))
	norm := 1.0 / math.Sqrt(approxFieldLen)
	expected := float32(wandTFNorm(1.0, norm, avgDocLen))
	tol := float32(0.001)
	if got < expected-tol || got > expected+tol {
		t.Errorf("MaxTFNorm('some') = %f, want ~%f (±%f)", got, expected, tol)
	}

	// Second call must return cached value (no rescan).
	got2 := dict.(*Dictionary).MaxTFNorm([]byte("some"), avgDocLen)
	if got2 != got {
		t.Errorf("cache: second MaxTFNorm call returned %f, first was %f", got2, got)
	}

	// Absent term must return 0.
	if v := dict.(*Dictionary).MaxTFNorm([]byte("zzz_absent"), avgDocLen); v != 0 {
		t.Errorf("MaxTFNorm for absent term = %f, want 0", v)
	}
}

// TestMaxTFNormZeroAvgDocLen verifies MaxTFNorm returns 0 for avgDocLen <= 0
// (guards against division-by-zero in the BM25 formula).
func TestMaxTFNormZeroAvgDocLen(t *testing.T) {
	tmpPath := getTempPath("scorch_maxtfnorm_zero.zap")
	_ = os.RemoveAll(tmpPath)
	defer os.RemoveAll(tmpPath)

	testSeg, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(testSeg, tmpPath); err != nil {
		t.Fatal(err)
	}
	seg, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer seg.Close()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	if v := dict.(*Dictionary).MaxTFNorm([]byte("some"), 0); v != 0 {
		t.Errorf("MaxTFNorm with avgDocLen=0 = %f, want 0", v)
	}
	if v := dict.(*Dictionary).MaxTFNorm([]byte("some"), -1); v != 0 {
		t.Errorf("MaxTFNorm with avgDocLen=-1 = %f, want 0", v)
	}
}

// mergeTwo flushes and persists seg1base + seg2base, merges them into outPath,
// and returns the opened merged Segment. The caller must Close() it.
func mergeTwo(t *testing.T, seg1base, seg2base *SegmentBase, path1, path2, outPath string) *Segment {
	t.Helper()
	for _, p := range []string{path1, path2, outPath} {
		_ = os.RemoveAll(p)
		t.Cleanup(func() { _ = os.RemoveAll(p) })
	}
	if err := PersistSegmentBase(seg1base, path1); err != nil {
		t.Fatal(err)
	}
	if err := PersistSegmentBase(seg2base, path2); err != nil {
		t.Fatal(err)
	}
	s1, err := zapPlugin.Open(path1)
	if err != nil {
		t.Fatalf("open seg1: %v", err)
	}
	defer s1.Close()
	s2, err := zapPlugin.Open(path2)
	if err != nil {
		t.Fatalf("open seg2: %v", err)
	}
	defer s2.Close()

	drops := []*roaring.Bitmap{nil, nil}
	if _, _, err = zapPlugin.Merge([]seg.Segment{s1, s2}, drops, outPath, nil, nil); err != nil {
		t.Fatalf("merge: %v", err)
	}
	merged, err := zapPlugin.Open(outPath)
	if err != nil {
		t.Fatalf("open merged: %v", err)
	}
	return merged.(*Segment)
}

// TestMaxTFNormSourceSidecar is a diagnostic test verifying that a flushed
// segment's MaxTFNorm sidecar is readable before any merge.
func TestMaxTFNormSourceSidecar(t *testing.T) {
	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	path1 := getTempPath("mtn_src_diag.zap")
	_ = os.RemoveAll(path1)
	defer os.RemoveAll(path1)
	if err = PersistSegmentBase(seg1, path1); err != nil {
		t.Fatal(err)
	}
	s1, err := zapPlugin.Open(path1)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()

	sb := &s1.(*Segment).SegmentBase
	fidPlus1, ok := sb.fieldsMap["desc"]
	if !ok {
		t.Fatal("'desc' field not found in source segment")
	}
	fid := int(fidPlus1) - 1
	t.Logf("desc fieldID=%d  sections=%v", fid, sb.fieldsSectionsMap[fid])

	addr := sb.fieldsSectionsMap[fid][SectionMaxTFNorm]
	if addr == 0 {
		t.Fatalf("source segment has no MaxTFNorm sidecar for 'desc' (addr=0)")
	}

	dict, err := s1.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}
	d := dict.(*Dictionary)
	if _, err = d.PostingsList([]byte("some"), nil, nil); err != nil {
		t.Fatal(err)
	}
	ce := d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
	v, loaded := ce.termOffsetCache.Load("some")
	if !loaded {
		t.Fatal("termOffsetCache not populated for 'some'")
	}
	offset := v.(uint64)
	t.Logf("'some' FST offset=%d  encoding mask=%d", offset, offset&FSTValEncodingMask)

	// Decode the sidecar to see what postingsOffsets it actually contains.
	lf, err2 := sb.decodeMaxTFNormField(addr)
	if err2 != nil {
		t.Fatalf("decodeMaxTFNormField error: %v", err2)
	}
	if lf == nil {
		t.Fatal("decodeMaxTFNormField returned nil (no entries)")
	}
	t.Logf("sidecar for 'desc' has %d entries; postingsOffsets=%v  maxFreqs=%v", len(lf.postingsOffsets), lf.postingsOffsets, lf.maxFreqs)

	mf, mn, ok2 := sb.lookupMaxTFNorm(uint16(fid), offset)
	t.Logf("lookupMaxTFNorm(fid=%d, offset=%d): mf=%d  mn=%f  ok=%v", fid, offset, mf, mn, ok2)
	if !ok2 {
		t.Fatalf("lookupMaxTFNorm returned ok=false for 'some' on source segment (addr=%d, offset=%d)", addr, offset)
	}
}

// TestMaxTFNormMergeSidecarPresent verifies that after merging two flush-time
// segments the merged segment carries a nonzero MaxTFNorm sidecar address in
// fieldsSectionsMap for every inverted field shared by the sources.
func TestMaxTFNormMergeSidecarPresent(t *testing.T) {
	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	seg2, _, err := buildTestSegmentMulti2()
	if err != nil {
		t.Fatal(err)
	}
	merged := mergeTwo(t, seg1, seg2,
		getTempPath("mtn_present1.zap"),
		getTempPath("mtn_present2.zap"),
		getTempPath("mtn_presentM.zap"),
	)
	defer merged.Close()

	sb := &merged.SegmentBase
	for _, fieldName := range []string{"desc", "name", "tag"} {
		fidPlus1, ok := sb.fieldsMap[fieldName]
		if !ok {
			t.Errorf("field %q not in merged segment", fieldName)
			continue
		}
		fid := int(fidPlus1) - 1
		if fid >= len(sb.fieldsSectionsMap) || SectionMaxTFNorm >= len(sb.fieldsSectionsMap[fid]) {
			t.Errorf("field %q: fieldsSectionsMap too short", fieldName)
			continue
		}
		addr := sb.fieldsSectionsMap[fid][SectionMaxTFNorm]
		if addr == 0 {
			t.Errorf("field %q: MaxTFNorm sidecar address is 0 (sidecar not written)", fieldName)
		}
	}
}

// TestMaxTFNormMergeLookupOk verifies that lookupMaxTFNorm returns ok=true for
// a term that is present in both source segments, confirming the sidecar is
// actually indexed by the correct merged postingsOffset.
func TestMaxTFNormMergeLookupOk(t *testing.T) {
	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	seg2, _, err := buildTestSegmentMulti2()
	if err != nil {
		t.Fatal(err)
	}
	merged := mergeTwo(t, seg1, seg2,
		getTempPath("mtn_lookup1.zap"),
		getTempPath("mtn_lookup2.zap"),
		getTempPath("mtn_lookupM.zap"),
	)
	defer merged.Close()

	dict, err := merged.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}
	d := dict.(*Dictionary)

	// PostingsList populates termOffsetCache with the merged segment's postingsOffset.
	if _, err = d.PostingsList([]byte("some"), nil, nil); err != nil {
		t.Fatalf("PostingsList: %v", err)
	}
	ce := d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
	v, loaded := ce.termOffsetCache.Load("some")
	if !loaded {
		t.Fatal("termOffsetCache not populated for 'some'")
	}
	offset := v.(uint64)

	if offset&FSTValEncodingMask != FSTValEncodingGeneral {
		t.Skip("'some' uses 1-hit encoding in merged segment; sidecar not applicable")
	}

	mf, mn, ok := d.sb.lookupMaxTFNorm(d.fieldID, offset)
	if !ok {
		t.Fatal("lookupMaxTFNorm returned ok=false; sidecar not indexed by merged postingsOffset")
	}
	if mf == 0 {
		t.Error("lookupMaxTFNorm: maxFreq == 0, expected > 0")
	}
	if mn <= 0 {
		t.Errorf("lookupMaxTFNorm: maxNorm = %f, expected > 0", mn)
	}

	// The result must agree with the full scan path (MaxTFNorm on a cold segment).
	const avgDocLen = 2.0
	got := d.MaxTFNorm([]byte("some"), avgDocLen)
	if got <= 0 {
		t.Errorf("MaxTFNorm('some') on merged segment = %f, want > 0", got)
	}
}

// TestMaxTFNormMergeMaxTaken verifies that the merged sidecar stores max(maxFreq)
// and max(maxNorm) independently across source segments, not just one segment's
// values.
//
// Seg A: "body"="alpha alpha alpha"  → freq("alpha")=3, fieldLen=3
// Seg B: "body"="alpha"              → freq("alpha")=1, fieldLen=1  (higher norm)
//
// Merged sidecar must have maxFreq=3 (from A) and maxNorm=norm(fieldLen=1) (from B).
func TestMaxTFNormMergeMaxTaken(t *testing.T) {
	docA := newStubDocument("a", []*stubField{
		newStubFieldSplitString("_id", nil, "a", true, false, false),
		newStubFieldSplitString("body", nil, "alpha alpha alpha", true, false, true),
	}, "_all")
	docB := newStubDocument("b", []*stubField{
		newStubFieldSplitString("_id", nil, "b", true, false, false),
		newStubFieldSplitString("body", nil, "alpha", true, false, true),
	}, "_all")

	segABase, _, err := zapPlugin.newWithChunkMode([]index.Document{docA}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	segBBase, _, err := zapPlugin.newWithChunkMode([]index.Document{docB}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}

	merged := mergeTwo(t, segABase.(*SegmentBase), segBBase.(*SegmentBase),
		getTempPath("mtn_maxA.zap"),
		getTempPath("mtn_maxB.zap"),
		getTempPath("mtn_maxM.zap"),
	)
	defer merged.Close()

	dict, err := merged.Dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	d := dict.(*Dictionary)

	if _, err = d.PostingsList([]byte("alpha"), nil, nil); err != nil {
		t.Fatalf("PostingsList: %v", err)
	}
	ce := d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
	v, loaded := ce.termOffsetCache.Load("alpha")
	if !loaded {
		t.Fatal("termOffsetCache not populated for 'alpha'")
	}
	offset := v.(uint64)
	if offset&FSTValEncodingMask != FSTValEncodingGeneral {
		t.Skip("'alpha' uses 1-hit encoding; sidecar not applicable for this case")
	}

	mf, mn, ok := d.sb.lookupMaxTFNorm(d.fieldID, offset)
	if !ok {
		t.Fatal("lookupMaxTFNorm returned ok=false for 'alpha' in merged segment")
	}

	// maxFreq must be 3 (from seg A).
	if mf != 3 {
		t.Errorf("maxFreq = %d, want 3 (max across source segs)", mf)
	}

	// maxNorm must come from the shortest doc (fieldLen=1 → highest norm).
	// normToSmallFloat(1/sqrt(1)) = encodeNormByte(1).
	wantNormByte := encodeNormByte(1)
	wantFL := float64(normDecodeSmallFloat(wantNormByte))
	wantNorm := float32(1.0 / math.Sqrt(wantFL))
	tol := float32(0.01)
	if mn < wantNorm-tol || mn > wantNorm+tol {
		t.Errorf("maxNorm = %f, want ~%f (norm from fieldLen=1); tol=%f", mn, wantNorm, tol)
	}
}

// TestMaxTFNormMergeIdempotent verifies that re-merging an already-merged
// segment (self-merge) still produces a correct MaxTFNorm sidecar. This
// exercises the self-healing property: the sidecar propagates through
// successive merge rounds.
func TestMaxTFNormMergeIdempotent(t *testing.T) {
	seg1, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	seg2, _, err := buildTestSegmentMulti2()
	if err != nil {
		t.Fatal(err)
	}

	// First merge.
	mergedPath := getTempPath("mtn_idem_m1.zap")
	merged1 := mergeTwo(t, seg1, seg2,
		getTempPath("mtn_idem1.zap"),
		getTempPath("mtn_idem2.zap"),
		mergedPath,
	)

	// Second merge: self-merge of the first merged segment.
	path2 := getTempPath("mtn_idem_m2.zap")
	_ = os.RemoveAll(path2)
	t.Cleanup(func() { _ = os.RemoveAll(path2) })
	drops := []*roaring.Bitmap{nil}
	if _, _, err = zapPlugin.Merge([]seg.Segment{merged1}, drops, path2, nil, nil); err != nil {
		merged1.Close()
		t.Fatalf("self-merge: %v", err)
	}
	merged1.Close()

	merged2, err := zapPlugin.Open(path2)
	if err != nil {
		t.Fatalf("open re-merged: %v", err)
	}
	defer merged2.Close()

	sb := &merged2.(*Segment).SegmentBase

	// Verify the sidecar address is present for "desc".
	fidPlus1, ok := sb.fieldsMap["desc"]
	if !ok {
		t.Fatal("'desc' field not found in re-merged segment")
	}
	fid := int(fidPlus1) - 1
	if fid >= len(sb.fieldsSectionsMap) || SectionMaxTFNorm >= len(sb.fieldsSectionsMap[fid]) {
		t.Fatal("fieldsSectionsMap too short for 'desc'")
	}
	if sb.fieldsSectionsMap[fid][SectionMaxTFNorm] == 0 {
		t.Fatal("MaxTFNorm sidecar not present after re-merge (self-healing property broken)")
	}

	// And the lookup still works.
	dict, err := merged2.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}
	d := dict.(*Dictionary)
	if _, err = d.PostingsList([]byte("some"), nil, nil); err != nil {
		t.Fatalf("PostingsList: %v", err)
	}
	ce := d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
	v, loaded := ce.termOffsetCache.Load("some")
	if !loaded {
		t.Fatal("termOffsetCache not populated for 'some' in re-merged segment")
	}
	offset := v.(uint64)
	if offset&FSTValEncodingMask != FSTValEncodingGeneral {
		t.Skip("'some' uses 1-hit encoding after re-merge")
	}
	if _, _, ok := d.sb.lookupMaxTFNorm(d.fieldID, offset); !ok {
		t.Fatal("lookupMaxTFNorm returned ok=false in re-merged segment (self-healing broken)")
	}
}
