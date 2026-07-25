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
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
)

// TestAdaptiveChunkSize pins the mode-1028 rule (§75), and that the removed mode
// 1027 stays removed. chunkMode is persisted per segment and both reader and
// writers re-derive chunkSize from it, so a mode whose rule changed underneath it
// would be misread rather than rejected: a low-cardinality term taken as one chunk
// out of a directory that actually holds hundreds.
func TestAdaptiveChunkSize(t *testing.T) {
	const maxDocs = 70000

	cases := []struct {
		mode        uint32
		cardinality uint64
		want        uint64
	}{
		// 1028: at or below one block's worth of documents, a single chunk spans
		// the segment, so the directory holds one entry.
		{PFORChunkMode, 1, maxDocs},
		{PFORChunkMode, 4, maxDocs},
		{PFORChunkMode, pforBlockSize - 1, maxDocs},
		{PFORChunkMode, pforBlockSize, maxDocs},
		// Above it, docNum-aligned windows, which is what makes a chunk index
		// derivable from a docNum as docNum/chunkSize.
		{PFORChunkMode, pforBlockSize + 1, pforBlockSize},
		{PFORChunkMode, 33000, pforBlockSize},
	}

	for _, tc := range cases {
		got, err := getChunkSize(tc.mode, tc.cardinality, maxDocs)
		if err != nil {
			t.Errorf("getChunkSize(%d, %d, %d): %v", tc.mode, tc.cardinality, maxDocs, err)
			continue
		}
		if got != tc.want {
			t.Errorf("getChunkSize(mode=%d, cardinality=%d, maxDocs=%d) = %d, want %d",
				tc.mode, tc.cardinality, maxDocs, got, tc.want)
		}
	}

	// A single chunk needs a non-zero span to cover.
	if _, err := getChunkSize(PFORChunkMode, 1, 0); err != ErrChunkSizeZero {
		t.Errorf("getChunkSize(1028, 1, 0) error = %v, want ErrChunkSizeZero", err)
	}

	if DefaultChunkMode != PFORChunkMode {
		t.Errorf("DefaultChunkMode = %d, want %d", DefaultChunkMode, PFORChunkMode)
	}

	// Mode 1027 — PFOR with unconditional 256-docNum windows — was removed rather
	// than redefined, so a segment written by an earlier prototype build fails
	// loudly on its first postings read instead of taking a low-cardinality term as
	// one chunk out of a directory that holds hundreds. Silent misreads are the
	// thing being prevented here; v18 is unreleased, so nothing else is owed.
	if _, err := getChunkSize(1027, 1, maxDocs); err == nil {
		t.Error("getChunkSize(1027, ...) must fail: a stale prototype segment has to " +
			"error rather than silently misread its chunk directory")
	}
}

// adaptiveTestDocs builds documents where term cardinality straddles the
// pforBlockSize threshold, so one segment exercises both branches of the mode-1028
// rule at once:
//
//	"everywhere" — in every doc         -> above the threshold, windowed
//	"many"       — in half the docs     -> above the threshold, windowed
//	"rare%d"     — in exactly one doc   -> single chunk, the §75 case
//	"few%d"      — in three docs        -> single chunk
//
// A present-but-sparse term is the interesting one: it takes the general postings
// path rather than the 1-hit encoding as soon as its cardinality exceeds 1 or it
// carries locations, which is exactly the entity-corpus shape.
func adaptiveTestDocs(n int, idPrefix string) []index.Document {
	rv := make([]index.Document, 0, n)
	for i := 0; i < n; i++ {
		terms := "everywhere"
		if i%2 == 0 {
			terms += " many"
		}
		terms += fmt.Sprintf(" rare%d %d few%d", i, i, i/3)
		id := fmt.Sprintf("%s%05d", idPrefix, i)
		rv = append(rv, newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldSplitString("body", nil, terms, true, false, true),
		}, "_all"))
	}
	return rv
}

// adaptivePostings flattens a term's postings so two chunk modes can be compared
// field by field.
func adaptivePostings(t *testing.T, sb *SegmentBase, terms []string) map[string][]uint64 {
	t.Helper()

	dict, err := sb.Dictionary("body")
	if err != nil {
		t.Fatalf("dictionary: %v", err)
	}
	rv := map[string][]uint64{}
	for _, term := range terms {
		pl, err := dict.PostingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatalf("postings %q: %v", term, err)
		}
		var flat []uint64
		itr := pl.Iterator(true, true, true, nil)
		for {
			post, err := itr.Next()
			if err != nil {
				t.Fatalf("iterate %q: %v", term, err)
			}
			if post == nil {
				break
			}
			flat = append(flat, post.Number(), post.Frequency(),
				uint64(len(post.Locations())), math.Float64bits(post.Norm()))
		}
		rv[term] = flat
	}
	return rv
}

var adaptiveTerms = []string{"everywhere", "many", "rare0", "rare599", "rare1199",
	"few0", "few100", "few399"}

// TestAdaptiveChunkModeRoundTrip is the end-to-end check: a segment written with
// mode 1028 must read back identical freqs, norms and locations for terms on both
// sides of the cardinality threshold. A wrong chunk size does not fail loudly — it
// reads whatever bytes sit at the computed offset — so this compares against
// LegacyChunkMode, which stores the same postings as plain varints with no PFOR
// blocks and no adaptive sizing, and is therefore an independent witness.
func TestAdaptiveChunkModeRoundTrip(t *testing.T) {
	docs := adaptiveTestDocs(1200, "d")

	collect := func(mode uint32) map[string][]uint64 {
		t.Helper()
		sb, _, err := zapPlugin.newWithChunkMode(docs, mode, nil)
		if err != nil {
			t.Fatalf("mode %d: build: %v", mode, err)
		}
		base := sb.(*SegmentBase)
		if base.chunkMode != mode {
			t.Fatalf("mode %d: segment reports chunkMode %d", mode, base.chunkMode)
		}
		return adaptivePostings(t, base, adaptiveTerms)
	}

	legacy := collect(LegacyChunkMode)
	adaptive := collect(PFORChunkMode)

	for _, term := range adaptiveTerms {
		want, got := legacy[term], adaptive[term]
		if len(want) == 0 {
			t.Errorf("%q: no postings under the legacy mode; the test data does not cover it", term)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("%q: adaptive returned %d posting fields, legacy %d",
				term, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%q: posting data differs at field %d: adaptive %d, legacy %d",
					term, i, got[i], want[i])
				break
			}
		}
	}

	// Confirm the sparse branch actually engaged, so this cannot pass by mode 1028
	// silently falling through to fixed windows.
	cs, err := getChunkSize(PFORChunkMode, 1, 1200)
	if err != nil {
		t.Fatal(err)
	}
	if cs != 1200 {
		t.Fatalf("sparse branch not engaged: chunkSize=%d", cs)
	}
}

// TestAdaptiveChunkModeMergeMixed exercises what §4's notes single out as the place
// codec bugs hide: merging segments written under different chunk modes. The merge
// re-derives chunkSize per term from the OUTPUT mode and the merged cardinality, so
// a term that was windowed in its source segment can become a single chunk in the
// merged one and vice versa.
func TestAdaptiveChunkModeMergeMixed(t *testing.T) {
	for _, combo := range []struct {
		name string
		a, b uint32
		out  uint32
	}{
		{"legacy+pfor -> pfor", LegacyChunkMode, PFORChunkMode, PFORChunkMode},
		{"pfor+legacy -> pfor", PFORChunkMode, LegacyChunkMode, PFORChunkMode},
		{"pfor+pfor -> pfor", PFORChunkMode, PFORChunkMode, PFORChunkMode},
		{"pfor+pfor -> legacy", PFORChunkMode, PFORChunkMode, LegacyChunkMode},
	} {
		t.Run(combo.name, func(t *testing.T) {
			sbA, _, err := zapPlugin.newWithChunkMode(adaptiveTestDocs(600, "a"), combo.a, nil)
			if err != nil {
				t.Fatal(err)
			}
			sbB, _, err := zapPlugin.newWithChunkMode(adaptiveTestDocs(600, "b"), combo.b, nil)
			if err != nil {
				t.Fatal(err)
			}

			mergedPath := getTempPath("scorch-adaptive-merge.zap")
			_ = os.RemoveAll(mergedPath)
			defer func() { _ = os.RemoveAll(mergedPath) }()

			_, _, err = mergeSegmentBases(
				[]*SegmentBase{sbA.(*SegmentBase), sbB.(*SegmentBase)},
				[]*roaring.Bitmap{nil, nil}, mergedPath, combo.out, nil, nil, nil)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}

			merged, err := zapPlugin.Open(mergedPath)
			if err != nil {
				t.Fatalf("open merged: %v", err)
			}
			defer func() { _ = merged.Close() }()

			if got := merged.Count(); got != 1200 {
				t.Errorf("merged count = %d, want 1200", got)
			}
			if got := merged.(*Segment).chunkMode; got != combo.out {
				t.Errorf("merged chunkMode = %d, want %d", got, combo.out)
			}

			// "everywhere" is above the threshold, "rare0"/"few0" below it; all must
			// survive with sane freqs. A chunk read from the wrong offset shows up
			// here as a zero or absurd frequency rather than an error.
			dict, err := merged.Dictionary("body")
			if err != nil {
				t.Fatal(err)
			}
			for _, term := range []string{"everywhere", "many", "rare0", "few0"} {
				pl, err := dict.PostingsList([]byte(term), nil, nil)
				if err != nil {
					t.Fatalf("postings %q: %v", term, err)
				}
				n := 0
				itr := pl.Iterator(true, true, true, nil)
				for {
					post, err := itr.Next()
					if err != nil {
						t.Fatalf("iterate %q: %v", term, err)
					}
					if post == nil {
						break
					}
					if f := post.Frequency(); f == 0 || f > 16 {
						t.Errorf("%q: doc %d has frequency %d — chunk data was read from "+
							"the wrong offset", term, post.Number(), f)
					}
					n++
				}
				if n == 0 {
					t.Errorf("%q: no postings after merge", term)
				}
			}
		})
	}
}
