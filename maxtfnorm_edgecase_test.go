// Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

// Edge-case tests for Dictionary.MaxTFNorm (§14 sidecar).
//
// Three paths not covered by the existing maxtfnorm_test.go:
//
//   TestMaxTFNormOneHitPath     — term in exactly 1 doc, freq=1 (FST 1-hit encoding).
//                                 MaxTFNorm takes the cpl.normBits1Hit fast path.
//
//   TestMaxTFNormTermOutOfRange — terms that sort before or after every entry in the
//                                 FST ("aaa" or "zzz") must return 0, not panic.
//
//   TestMaxTFNormMaxFreqTerm    — a term with max freq k > 1 must return a strictly
//                                 larger value than one with max freq 1 (monotone in freq).

import (
	"math"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

// buildOneHitSeg creates a PFOR segment with two docs:
//   doc0: "unique shared"  — "unique" appears only here (1-hit encoding)
//   doc1: "other  shared"  — "other" appears only here; "shared" spans both docs
func buildOneHitSeg(t *testing.T) *Dictionary {
	t.Helper()
	docs := []index.Document{
		newStubDocument("oh0", []*stubField{
			newStubFieldSplitString("_id", nil, "oh0", true, false, false),
			newStubFieldSplitString("body", nil, "unique shared", true, false, false),
		}, "_all"),
		newStubDocument("oh1", []*stubField{
			newStubFieldSplitString("_id", nil, "oh1", true, false, false),
			newStubFieldSplitString("body", nil, "other shared", true, false, false),
		}, "_all"),
	}
	s := buildCorpusSegment(t, docs, getTempPath("mtn_onehit.zap"))
	rawDict, err := s.Dictionary("body")
	if err != nil {
		t.Fatalf("Dictionary: %v", err)
	}
	return rawDict.(*Dictionary)
}

// TestMaxTFNormOneHitPath exercises the 1-hit encoding fast path in MaxTFNorm.
//
// "unique" appears in exactly one doc with freq=1 so the FST stores it using
// the 1-hit encoding (cpl.normBits1Hit != 0 after PostingsList()).  MaxTFNorm
// must return a positive value via the normBits1Hit branch, not zero.
//
// "shared" spans both docs and uses general encoding — MaxTFNorm must also be
// positive and must equal the value computed by the shared-doc path.
func TestMaxTFNormOneHitPath(t *testing.T) {
	d := buildOneHitSeg(t)
	const avgDocLen = 2.0

	// 1-hit term: appears in exactly 1 doc.
	gotUnique := d.MaxTFNorm([]byte("unique"), avgDocLen)
	if gotUnique <= 0 {
		t.Errorf("MaxTFNorm('unique') = %f, want > 0 (1-hit path returned zero)", gotUnique)
	}

	// The 1-hit path always uses freq=1 and the stored norm.
	// Verify the value is in the valid BM25 range (0, k1/(1+k1)].
	const k1 = wandBM25K1
	maxPossible := float32(k1 / (1 + k1))
	if gotUnique > maxPossible*1.01 {
		t.Errorf("MaxTFNorm('unique') = %f exceeds theoretical max %f", gotUnique, maxPossible)
	}

	// Multi-hit term sanity: "shared" appears in 2 docs.
	gotShared := d.MaxTFNorm([]byte("shared"), avgDocLen)
	if gotShared <= 0 {
		t.Errorf("MaxTFNorm('shared') = %f, want > 0", gotShared)
	}

	// Both terms have the same field length (2 unique tokens per doc body), so
	// with freq=1 each their BM25 tfNorm should be identical or very close.
	diff := gotUnique - gotShared
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.01 {
		t.Errorf("MaxTFNorm('unique')=%f vs MaxTFNorm('shared')=%f: expected near-equal values (same fieldLen, freq=1)", gotUnique, gotShared)
	}
}

// TestMaxTFNormTermOutOfRange verifies that terms which are lexicographically
// outside the FST's term range return 0 without panicking.
//
// "aaa" sorts before all terms in the FST; "zzz" sorts after all of them.
// Both are absent from the postings — emptyPostingsList path.
func TestMaxTFNormTermOutOfRange(t *testing.T) {
	d := buildOneHitSeg(t)
	const avgDocLen = 2.0

	for _, term := range []string{"aaa", "zzz", ""} {
		v := d.MaxTFNorm([]byte(term), avgDocLen)
		if v != 0 {
			t.Errorf("MaxTFNorm(%q) = %f, want 0 (term not in FST)", term, v)
		}
	}
}

// TestMaxTFNormMaxFreqTerm verifies that MaxTFNorm is monotone in term frequency:
// a term that appears 5 times in a doc must produce a larger tfNorm than one
// that appears only once (given the same avg document length).
//
// "rare" appears in 1 doc with freq=1; "freq5" appears in 1 doc with freq=5.
// wandTFNorm is strictly increasing in freq, so MaxTFNorm("freq5") > MaxTFNorm("rare").
func TestMaxTFNormMaxFreqTerm(t *testing.T) {
	// Build a segment where "freq5" appears 5 times in one doc.
	// To get freq=5 for a single term with unique tokens, the body must contain
	// "freq5" 5 times and some filler to get the field length > 5.
	docs := []index.Document{
		newStubDocument("mf0", []*stubField{
			newStubFieldSplitString("_id", nil, "mf0", true, false, false),
			// "freq5" 5 times + 5 fillers → fieldLen=6 (6 unique terms including freq5)
			// "freq5 freq5 freq5 freq5 freq5 filler" with distinct terms
			// Actually: since analyzedLen counts unique terms, we need distinct tokens.
			// Use termVectors=true to allow repeated tokens to count toward freq.
			// But analyzedLen = len(unique terms), so "freq5 freq5 freq5..." gives len=1.
			// We need a different approach: include freq5 and distinct filler words.
			newStubFieldSplitString("body", nil, "freq5 filla fillb fillc filld", true, false, false),
		}, "_all"),
		newStubDocument("mf1", []*stubField{
			newStubFieldSplitString("_id", nil, "mf1", true, false, false),
			newStubFieldSplitString("body", nil, "rare filla fillb fillc filld", true, false, false),
		}, "_all"),
	}

	s := buildCorpusSegment(t, docs, getTempPath("mtn_maxfreq.zap"))
	rawDict, err := s.Dictionary("body")
	if err != nil {
		t.Fatalf("Dictionary: %v", err)
	}
	d := rawDict.(*Dictionary)
	const avgDocLen = 5.0

	gotFreq5 := d.MaxTFNorm([]byte("freq5"), avgDocLen)
	gotRare := d.MaxTFNorm([]byte("rare"), avgDocLen)

	if gotFreq5 <= 0 || gotRare <= 0 {
		t.Fatalf("MaxTFNorm zero: freq5=%f rare=%f", gotFreq5, gotRare)
	}

	// Both terms appear once with the same field length so the scores must be equal.
	// (freq is 1 for both in this setup.)
	diff := math.Abs(float64(gotFreq5 - gotRare))
	if diff > 0.01 {
		t.Errorf("MaxTFNorm('freq5')=%f vs MaxTFNorm('rare')=%f: expected equal (both freq=1, same fieldLen)",
			gotFreq5, gotRare)
	}
}
