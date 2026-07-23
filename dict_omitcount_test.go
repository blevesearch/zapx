//	Copyright (c) 2026 Couchbase, Inc.
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
	"testing"

	"github.com/blevesearch/vellum/levenshtein"
)

// TestAutomatonIteratorOmitCountSemantics asserts that the count-omitting
// automaton iterator visits exactly the same terms, in the same order, with
// the same edit distances as the regular iterator — the only difference being
// that DictEntry.Count is left unpopulated (0) instead of the real count.
func TestAutomatonIteratorOmitCountSemantics(t *testing.T) {
	seg, _, err := buildBenchFuzzySegment(500, 800, 30)
	if err != nil {
		t.Fatal(err)
	}
	dict, err := seg.dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	lb, err := levenshtein.NewLevenshteinAutomatonBuilder(2, true)
	if err != nil {
		t.Fatal(err)
	}
	dfa, err := lb.BuildDfa("term0250", 2)
	if err != nil {
		t.Fatal(err)
	}

	type entry struct {
		term         string
		editDistance uint8
		count        uint64
	}

	// with-count reference
	var withCount []entry
	itr := dict.AutomatonIterator(dfa, nil, nil)
	tfd, err := itr.Next()
	for err == nil && tfd != nil {
		withCount = append(withCount, entry{tfd.Term, tfd.EditDistance, tfd.Count})
		tfd, err = itr.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	// omit-count
	var omit []entry
	itr2 := dict.AutomatonIteratorOmitCount(dfa, nil, nil)
	tfd, err = itr2.Next()
	for err == nil && tfd != nil {
		omit = append(omit, entry{tfd.Term, tfd.EditDistance, tfd.Count})
		tfd, err = itr2.Next()
	}
	if err != nil {
		t.Fatal(err)
	}

	if len(withCount) == 0 {
		t.Fatal("expected at least one candidate term")
	}
	if len(withCount) != len(omit) {
		t.Fatalf("term count mismatch: withCount=%d omit=%d", len(withCount), len(omit))
	}
	for i := range withCount {
		if withCount[i].term != omit[i].term {
			t.Fatalf("term[%d] mismatch: %q vs %q", i, withCount[i].term, omit[i].term)
		}
		if withCount[i].editDistance != omit[i].editDistance {
			t.Fatalf("editDistance[%d] mismatch for %q: %d vs %d",
				i, withCount[i].term, withCount[i].editDistance, omit[i].editDistance)
		}
		// the whole point: with-count reports a real (>0) count, omit reports 0
		if withCount[i].count == 0 {
			t.Fatalf("expected non-zero count for %q in with-count path", withCount[i].term)
		}
		if omit[i].count != 0 {
			t.Fatalf("expected zero (omitted) count for %q, got %d", omit[i].term, omit[i].count)
		}
	}
}
