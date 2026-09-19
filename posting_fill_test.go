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
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

// getFillIterator opens a fresh *PostingsIterator for term, asserting it also
// implements bleve's termFieldDocFiller shape (checked structurally here via
// a direct method call, since zapx does not import bleve's own unexported
// interface).
func getFillIterator(t *testing.T, sb *SegmentBase, field, term string) *PostingsIterator {
	t.Helper()
	dict, err := sb.dictionary(field)
	if err != nil {
		t.Fatal(err)
	}
	pl, err := dict.postingsList([]byte(term), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	itr, ok := pl.Iterator(true, true, false, nil).(*PostingsIterator)
	if !ok {
		t.Fatalf("term %q: postings list iterator is not *PostingsIterator", term)
	}
	return itr
}

// TestFillTermFieldDocLinearScan drives FillTermFieldDoc with atOrAfter==0
// repeatedly -- the Next()-equivalent usage -- across every term the model
// covers (full-corpus, scattered, 1-hit, 1-hit-with-frequency-greater-than-1)
// and checks the result against the same model TestBlockPostingsRoundTrip
// checks the generic Next() path against, including a non-zero globalOffset
// so an omitted "+= globalOffset" cannot slip by unnoticed.
func TestFillTermFieldDocLinearScan(t *testing.T) {
	const numDocs = 300
	const globalOffset = 1 << 20
	sb, model := buildBlockTestSegment(t, numDocs, false)

	for _, term := range sortedTerms(model.terms) {
		term := term
		t.Run(term, func(t *testing.T) {
			want := model.terms[term]
			itr := getFillIterator(t, sb, "body", term)

			var got int
			var rv index.TermFieldDoc
			for {
				found, err := itr.FillTermFieldDoc(&rv, globalOffset, 0, true, true)
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					break
				}
				if got >= len(want) {
					t.Fatalf("posting %d: got an extra posting (docNum %d), want only %d total",
						got, rv.ID.Value()-globalOffset, len(want))
				}
				w := want[got]
				docNum := rv.ID.Value()
				if docNum != w.docNum+globalOffset {
					t.Fatalf("posting %d: got docNum %d, want %d (globalOffset %d)",
						got, docNum, w.docNum+globalOffset, globalOffset)
				}
				if rv.Freq != w.freq {
					t.Fatalf("posting %d, docNum %d: got freq %d, want %d", got, w.docNum, rv.Freq, w.freq)
				}
				if rv.Norm != normFactorFromID(w.normID) {
					t.Fatalf("posting %d, docNum %d: got norm %v, want %v", got, w.docNum, rv.Norm, normFactorFromID(w.normID))
				}
				got++
			}
			if got != len(want) {
				t.Fatalf("got %d postings, want %d", got, len(want))
			}

			// One extra call past exhaustion must stay false, not panic or
			// resurrect a stale posting.
			found, err := itr.FillTermFieldDoc(&rv, globalOffset, 0, true, true)
			if err != nil || found {
				t.Fatalf("post-exhaustion call: found=%v err=%v, want false/nil", found, err)
			}
		})
	}
}

// TestFillTermFieldDocSeek drives FillTermFieldDoc with a forward-only
// sequence of explicit atOrAfter targets -- the Advance()-equivalent usage
// bleve's blockConjunction.scoreCandidates actually exercises for every
// surviving candidate's secondary clauses -- including targets that land
// between two real postings, to check the "first document at or after" skip
// semantics rather than just exact-match lookups.
func TestFillTermFieldDocSeek(t *testing.T) {
	const numDocs = 300
	sb, model := buildBlockTestSegment(t, numDocs, false)

	for _, term := range sortedTerms(model.terms) {
		term := term
		t.Run(term, func(t *testing.T) {
			want := model.terms[term]
			if len(want) == 0 {
				return
			}
			itr := getFillIterator(t, sb, "body", term)

			// One probe per want[] entry, each consuming exactly that entry:
			// for every other entry whose docNum isn't adjacent to the
			// previous entry's (so a gap actually exists), probe the empty
			// document right before it instead of the entry's own docNum --
			// exercising "first document at or after" rather than only ever
			// hitting an exact match. Never both for the same entry (that
			// would ask the iterator to seek to a document at or before one
			// it already returned, which even the generic Advance() only
			// supports by rebuilding the reader from scratch). Strictly
			// increasing target values follow automatically, since want[]
			// itself is in strictly increasing docNum order.
			var targets []uint64
			prevDocNum := uint64(0)
			for i, w := range want {
				if i%2 == 1 && w.docNum > 0 && (i == 0 || w.docNum-1 > prevDocNum) {
					targets = append(targets, w.docNum-1)
				} else {
					targets = append(targets, w.docNum)
				}
				prevDocNum = w.docNum
			}

			var rv index.TermFieldDoc
			for i, target := range targets {
				found, err := itr.FillTermFieldDoc(&rv, 0, target, true, true)
				if err != nil {
					t.Fatal(err)
				}
				w := want[i]
				if !found {
					t.Fatalf("target %d: got none, want docNum %d", target, w.docNum)
				}
				if rv.ID.Value() != w.docNum {
					t.Fatalf("target %d: got docNum %d, want %d", target, rv.ID.Value(), w.docNum)
				}
				if rv.Freq != w.freq {
					t.Fatalf("target %d, docNum %d: got freq %d, want %d", target, w.docNum, rv.Freq, w.freq)
				}
				if rv.Norm != normFactorFromID(w.normID) {
					t.Fatalf("target %d, docNum %d: got norm %v, want %v", target, w.docNum, rv.Norm, normFactorFromID(w.normID))
				}
			}
			// One extra seek past the last real posting must report none.
			found, err := itr.FillTermFieldDoc(&rv, 0, want[len(want)-1].docNum+1, true, true)
			if err != nil || found {
				t.Fatalf("post-exhaustion seek: found=%v err=%v, want false/nil", found, err)
			}
		})
	}
}

// TestFillTermFieldDocOmitsFreqNorm confirms includeFreq/includeNorm actually
// gate what gets written, matching the generic Advance/Next path's own
// contract, rather than FillTermFieldDoc always populating both regardless
// of what the caller asked for.
func TestFillTermFieldDocOmitsFreqNorm(t *testing.T) {
	const numDocs = 300
	sb, model := buildBlockTestSegment(t, numDocs, false)
	term := "mod3"
	want := model.terms[term]
	if len(want) < 2 {
		t.Fatalf("test term %q too small: %d postings", term, len(want))
	}

	itr := getFillIterator(t, sb, "body", term)
	var rv index.TermFieldDoc
	found, err := itr.FillTermFieldDoc(&rv, 0, 0, false, false)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if rv.ID.Value() != want[0].docNum {
		t.Fatalf("docNum got %d, want %d", rv.ID.Value(), want[0].docNum)
	}
	if rv.Freq != 0 {
		t.Fatalf("Freq = %d, want 0 (includeFreq=false)", rv.Freq)
	}
	if rv.Norm != 0 {
		t.Fatalf("Norm = %v, want 0 (includeNorm=false)", rv.Norm)
	}
}
