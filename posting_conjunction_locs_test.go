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

// Tests for the §36C fast path in nextDocNumAtOrAfter.
//
// §36C replaces a sequential i.all.Next() loop with:
//   i.all.AdvanceIfNeeded(n) → i.all.Next() → CardinalityInRange(…) → SetPFORPos(…)
//
// The fast path is guarded by !i.includeLocs because locReader must be
// advanced sequentially through skipped docs; SetPFORPos only handles
// freqNormReader.  This file contains:
//
//   TestConjunctionFastPathLocationsSlowPathCorrect  — with includeLocs=true,
//     the slow path is used; location positions are correct even when ActualBM
//     is narrowed to a small subset (simulating the conjunction AND optimisation).
//
//   TestConjunctionFastPathFreqTwoHitsSameChunk  — with includeLocs=false,
//     the fast path fires; verifies that two hits in the same PFOR chunk return
//     distinct, correct frequencies (not a stale value from a SetPFORPos seek).

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// buildCorpusSegment builds a PFOR-mode segment from the provided documents
// and persists it to tmpPath.  Returns the opened segment ready for queries.
func buildCorpusSegment(t *testing.T, docs []index.Document, tmpPath string) seg.Segment {
	t.Helper()
	_ = os.RemoveAll(tmpPath)
	t.Cleanup(func() { _ = os.RemoveAll(tmpPath) })

	sb, _, err := zapPlugin.newWithChunkMode(docs, PFORChunkMode, nil)
	if err != nil {
		t.Fatalf("newWithChunkMode: %v", err)
	}
	if err := PersistSegmentBase(sb.(*SegmentBase), tmpPath); err != nil {
		t.Fatalf("PersistSegmentBase: %v", err)
	}
	s, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// makePositionedDocs builds N documents where field "body" contains "alpha"
// at word-position (i+1) for doc i (by prepending i filler tokens "x").
// E.g. doc 0: "alpha", doc 1: "x alpha", doc 2: "x x alpha".
//
// includeMultiFreqDoc, if true, replaces doc[freqIdx] with a doc where
// "alpha" appears freqCount times (for freq-correctness tests).
func makePositionedDocs(n int, multiFreq bool, freqIdx, freqCount int) []index.Document {
	docs := make([]index.Document, n)
	for i := range docs {
		id := fmt.Sprintf("doc%d", i)
		var body string
		if multiFreq && i == freqIdx {
			body = strings.Repeat("alpha ", freqCount)
			body = strings.TrimSpace(body)
		} else {
			body = strings.Repeat("x ", i) + "alpha"
		}
		docs[i] = newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldSplitString("body", nil, body, true, false, true),
		}, "_all")
	}
	return docs
}

// getPostingsIterator opens the PostingsList for term in field on the given
// segment and returns a typed PostingsIterator.  includeFreq / includeLocs
// control what the iterator populates.
func getPostingsIterator(t *testing.T, s seg.Segment, field, term string,
	includeFreq, includeLocs bool) *PostingsIterator {
	t.Helper()
	dict, err := s.Dictionary(field)
	if err != nil {
		t.Fatalf("Dictionary(%q): %v", field, err)
	}
	pl, err := dict.(*Dictionary).postingsList([]byte(term), nil, nil)
	if err != nil {
		t.Fatalf("postingsList(%q): %v", term, err)
	}
	// includeNorm=includeFreq so BM25 norm bytes are loaded alongside freq.
	pi := pl.Iterator(includeFreq, includeFreq, includeLocs, nil)
	return pi.(*PostingsIterator)
}

// TestConjunctionFastPathLocationsSlowPathCorrect verifies that with
// includeLocs=true the slow path correctly returns location positions for
// docs in a narrowed ActualBM (simulating the conjunction AND optimisation).
//
// This is the regression test for the bug fixed by adding !i.includeLocs to
// the §36C fast-path condition: without the guard, locReader for doc N was
// positioned at the first doc in the chunk rather than doc N.
func TestConjunctionFastPathLocationsSlowPathCorrect(t *testing.T) {
	const n = 10
	docs := makePositionedDocs(n, false, 0, 0)
	s := buildCorpusSegment(t, docs, getTempPath("conj_locs_slow.zap"))

	pi := getPostingsIterator(t, s, "body", "alpha", true, true)

	// Narrow ActualBM to a small subset — simulates the conjunction AND.
	// We pick docs 2, 5, 8 so they span different positions in the full list.
	subset := []uint64{2, 5, 8}
	subsetBM := roaring.New()
	for _, d := range subset {
		subsetBM.Add(uint32(d))
	}
	pi.ActualBM = subsetBM
	pi.Actual = subsetBM.Iterator()

	// For doc i, "alpha" appears at position (i+1) because the body text is
	// "x x ... x alpha" with i filler tokens before "alpha".
	for _, docNum := range subset {
		posting, err := pi.Advance(docNum)
		if err != nil {
			t.Fatalf("Advance(%d): %v", docNum, err)
		}
		if posting == nil {
			t.Fatalf("Advance(%d) returned nil posting", docNum)
		}
		if posting.Number() != docNum {
			t.Errorf("posting.Number()=%d want %d", posting.Number(), docNum)
		}

		locs := posting.Locations()
		if len(locs) == 0 {
			t.Fatalf("doc %d: no locations returned", docNum)
		}
		wantPos := uint64(docNum + 1) // "alpha" at position (i+1)
		gotPos := locs[0].Pos()
		if gotPos != wantPos {
			t.Errorf("doc %d location.Pos()=%d want %d", docNum, gotPos, wantPos)
		}
	}
}

// TestConjunctionFastPathFreqTwoHitsSameChunk verifies the §36C fast path
// (includeLocs=false, PFOR mode) for two intersection hits in the same PFOR
// chunk: after SetPFORPos repositions freqNormReader for the first hit, the
// second hit must read a different pforPos and return the correct frequency —
// not a stale value from the first hit.
//
// Doc freqIdx has "alpha" appearing freqCount times.
// Doc freqIdx+3 (same chunk; chunkSize=256 in PFOR mode) has "alpha" once.
// The intersection is narrowed to {freqIdx, freqIdx+3}.
func TestConjunctionFastPathFreqTwoHitsSameChunk(t *testing.T) {
	const n = 10
	const freqIdx = 4
	const freqCount = 5 // "alpha alpha alpha alpha alpha"

	docs := makePositionedDocs(n, true, freqIdx, freqCount)
	s := buildCorpusSegment(t, docs, getTempPath("conj_locs_fast.zap"))

	// Fast path: includeFreqNorm=true, includeLocs=false, PFOR mode → fires.
	pi := getPostingsIterator(t, s, "body", "alpha", true, false)

	// Narrow ActualBM to two hits in the same chunk.
	first := uint64(freqIdx)
	second := uint64(freqIdx + 3)
	subsetBM := roaring.New()
	subsetBM.Add(uint32(first))
	subsetBM.Add(uint32(second))
	pi.ActualBM = subsetBM
	pi.Actual = subsetBM.Iterator()

	// Advance to first hit: freqIdx has freqCount occurrences.
	posting, err := pi.Advance(first)
	if err != nil {
		t.Fatalf("Advance(%d): %v", first, err)
	}
	if posting == nil {
		t.Fatalf("Advance(%d) returned nil", first)
	}
	if posting.Frequency() != freqCount {
		t.Errorf("doc %d: Frequency()=%d want %d (stale pforPos from SetPFORPos?)",
			first, posting.Frequency(), freqCount)
	}

	// Advance to second hit: one occurrence of "alpha".
	posting, err = pi.Advance(second)
	if err != nil {
		t.Fatalf("Advance(%d): %v", second, err)
	}
	if posting == nil {
		t.Fatalf("Advance(%d) returned nil", second)
	}
	const wantFreqSecond = 1
	if posting.Frequency() != wantFreqSecond {
		t.Errorf("doc %d: Frequency()=%d want %d (should differ from first hit)",
			second, posting.Frequency(), wantFreqSecond)
	}
}
