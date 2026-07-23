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
	"fmt"
	"strings"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum/levenshtein"
)

// buildBenchFuzzySegment builds an in-memory segment whose "desc" field has
// numTerms distinct terms spread across numDocs documents, so that each term's
// postings list spans many docs (a real multi-hit roaring bitmap rather than
// the 1-hit fast path). This makes the per-term postings read that populates
// DictEntry.Count actually cost something.
func buildBenchFuzzySegment(numTerms, numDocs, perDoc int) (*SegmentBase, []string, error) {
	terms := make([]string, numTerms)
	for i := range terms {
		terms[i] = fmt.Sprintf("term%04d", i)
	}
	results := make([]index.Document, numDocs)
	for d := 0; d < numDocs; d++ {
		docTerms := make([]string, perDoc)
		for j := 0; j < perDoc; j++ {
			docTerms[j] = terms[(d*perDoc+j)%numTerms]
		}
		id := fmt.Sprintf("%d", d)
		results[d] = newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldSplitString("desc", nil, strings.Join(docTerms, " "), true, false, true),
		}, "_all")
	}
	seg, _, err := zapPlugin.newWithChunkMode(results, 1024, nil)
	if err != nil {
		return nil, nil, err
	}
	return seg.(*SegmentBase), terms, nil
}

// countIteratedTerms drains a dictionary iterator and returns how many terms it
// visited, so the compiler can't elide the work.
func countIteratedTerms(b *testing.B, itr segment.DictionaryIterator) int {
	n := 0
	tfd, err := itr.Next()
	for err == nil && tfd != nil {
		n++
		tfd, err = itr.Next()
	}
	if err != nil {
		b.Fatal(err)
	}
	return n
}

// BenchmarkFuzzyIterator compares the cost of collecting fuzzy candidate terms
// with and without populating DictEntry.Count. The count-omitting variant skips
// a postings-list read (roaring bitmap deserialization) per visited term, which
// is the work candidate collectors (fuzzy/regexp) discard anyway.
func BenchmarkFuzzyIterator(b *testing.B) {
	seg, _, err := buildBenchFuzzySegment(1000, 3000, 40)
	if err != nil {
		b.Fatal(err)
	}
	dict, err := seg.dictionary("desc")
	if err != nil {
		b.Fatal(err)
	}

	lb, err := levenshtein.NewLevenshteinAutomatonBuilder(2, true)
	if err != nil {
		b.Fatal(err)
	}
	dfa, err := lb.BuildDfa("term0500", 2)
	if err != nil {
		b.Fatal(err)
	}

	// sanity: how many candidates does this automaton match, and are they
	// multi-hit? (reported once, outside the timed loop)
	matched := countIteratedTerms(b, dict.AutomatonIterator(dfa, nil, nil))
	b.Logf("fuzzy automaton matched %d candidate terms", matched)

	b.Run("WithCount", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			itr := dict.AutomatonIterator(dfa, nil, nil)
			_ = countIteratedTerms(b, itr)
		}
	})

	b.Run("OmitCount", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			itr := dict.AutomatonIteratorOmitCount(dfa, nil, nil)
			_ = countIteratedTerms(b, itr)
		}
	})
}
