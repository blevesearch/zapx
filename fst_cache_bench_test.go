package zap

import (
	"fmt"
	"strings"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

// buildBenchDictSegment builds an in-memory segment whose "desc" field has
// numTerms distinct terms, producing an FST of that size.
func buildBenchDictSegment(numTerms int) (*SegmentBase, []string, error) {
	terms := make([]string, numTerms)
	for i := range terms {
		terms[i] = fmt.Sprintf("benchterm%06dsuffix", i)
	}
	doc := newStubDocument("a", []*stubField{
		newStubFieldSplitString("_id", nil, "a", true, false, false),
		newStubFieldSplitString("desc", nil, strings.Join(terms, " "), true, false, true),
	}, "_all")
	results := []index.Document{doc}
	seg, _, err := zapPlugin.newWithChunkMode(results, 1024, nil)
	if err != nil {
		return nil, nil, err
	}
	return seg.(*SegmentBase), terms, nil
}

// BenchmarkPostingsListLookup measures repeated postingsList() calls for a
// working set of terms. The FST-resolved offset is cached per term, so after
// the first lookup each call skips the FST traversal.
func BenchmarkPostingsListLookup(b *testing.B) {
	seg, terms, err := buildBenchDictSegment(5000)
	if err != nil {
		b.Fatal(err)
	}
	dict, err := seg.dictionary("desc")
	if err != nil {
		b.Fatal(err)
	}

	// working set of 200 hot terms, queried round-robin (all cache hits after
	// the first pass).
	const hot = 200
	queryTerms := make([][]byte, hot)
	for i := range queryTerms {
		queryTerms[i] = []byte(terms[(i*13)%len(terms)])
	}

	var pl *PostingsList
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		p, err := dict.postingsList(queryTerms[n%hot], nil, pl)
		if err != nil {
			b.Fatal(err)
		}
		pl = p
	}
}
