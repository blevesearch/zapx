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
	"testing"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// TestBlockMaxCapability exercises segment.BlockMaxPostingsIterator end to
// end: this is the query-level test TestTailBlockMaxBound's own comment
// deferred ("nothing consumes these yet"), now that something does.
func TestBlockMaxCapability(t *testing.T) {
	const numDocs = 300
	sb, model := buildBlockTestSegment(t, numDocs, false)

	boundOf := func(postings []expPosting) (minNormID uint8, maxTF uint64) {
		minNormID = 0xFF
		var maxFreq uint64
		for _, p := range postings {
			if p.normID < minNormID {
				minNormID = p.normID
			}
			if p.freq > maxFreq {
				maxFreq = p.freq
			}
		}
		if encodeBlockMaxTF(uint32(maxFreq)) == math.MaxUint8 {
			return minNormID, math.MaxUint64
		}
		return minNormID, maxFreq
	}

	getIterator := func(t *testing.T, term string) (*PostingsIterator, segment.BlockMaxPostingsIterator) {
		t.Helper()
		dict, err := sb.dictionary("body")
		if err != nil {
			t.Fatal(err)
		}
		pl, err := dict.postingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		itr := pl.Iterator(true, true, false, nil).(*PostingsIterator)
		bm, ok := segment.PostingsIterator(itr).(segment.BlockMaxPostingsIterator)
		if !ok {
			t.Fatalf("term %q: *PostingsIterator does not implement segment.BlockMaxPostingsIterator", term)
		}
		return itr, bm
	}

	checkBound := func(t *testing.T, bm segment.BlockMaxPostingsIterator, postings []expPosting, wantLastDoc uint64, wantDocCount int) {
		t.Helper()
		wantMinNormID, wantMaxTF := boundOf(postings)
		wantMaxNormFactor := normFactorFromID(wantMinNormID)
		maxTF, maxNormFactor, lastDoc, docCount, ok := bm.BlockMax()
		if !ok {
			t.Fatal("BlockMax() reported no bound")
		}
		if lastDoc != wantLastDoc {
			t.Fatalf("BlockMax() lastDoc = %d, want %d", lastDoc, wantLastDoc)
		}
		if docCount != wantDocCount {
			t.Fatalf("BlockMax() docCount = %d, want %d", docCount, wantDocCount)
		}
		if maxTF != wantMaxTF {
			t.Fatalf("BlockMax() maxTF = %d, want %d", maxTF, wantMaxTF)
		}
		if maxNormFactor != wantMaxNormFactor {
			t.Fatalf("BlockMax() maxNormFactor = %v, want %v", maxNormFactor, wantMaxNormFactor)
		}
	}

	decodeAndCheck := func(t *testing.T, bm segment.BlockMaxPostingsIterator, want []expPosting) {
		t.Helper()
		docs := make([]uint64, len(want))
		freqs := make([]uint64, len(want))
		norms := make([]float64, len(want))
		n, err := bm.NextBlock(docs, freqs, norms, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(want) {
			t.Fatalf("NextBlock() returned %d postings, want %d", n, len(want))
		}
		for k := range want {
			wantNorm := normFactorFromID(want[k].normID)
			if docs[k] != want[k].docNum || freqs[k] != want[k].freq || norms[k] != wantNorm {
				t.Fatalf("NextBlock()[%d] = (doc %d, freq %d, norm %v), want (doc %d, freq %d, norm %v)",
					k, docs[k], freqs[k], norms[k], want[k].docNum, want[k].freq, wantNorm)
			}
		}
	}

	t.Run("full block, then peek block 1, then tail", func(t *testing.T) {
		term := "all" // present in every doc: numFullBlocks == 2, tailLen == 44 for 300 docs
		postings := model.terms[term]
		if len(postings) != numDocs {
			t.Fatalf("expected %q in all %d docs, got %d", term, numDocs, len(postings))
		}
		_, bm := getIterator(t, term)

		// A fresh iterator, before any Next/Advance/ShallowAdvance/NextBlock,
		// reports block 0's bound directly (BlockMax's "!positioned" case).
		checkBound(t, bm, postings[:postingsBlockLen], postings[postingsBlockLen-1].docNum, postingsBlockLen)

		// Bulk-decode block 0 in one call and check it against the model
		// exactly. This fully drains the block (positioned=true, cur at the
		// last index), which is what lets the next BlockMax call below
		// exercise the "fully drained, peek the next block" path instead of
		// repeating block 0's own bound.
		decodeAndCheck(t, bm, postings[:postingsBlockLen])

		// Fully drained: BlockMax must now report block 1's bound, peeked
		// without decoding or disturbing anything.
		checkBound(t, bm, postings[postingsBlockLen:2*postingsBlockLen], postings[2*postingsBlockLen-1].docNum, postingsBlockLen)

		// Shallow-advance straight into the tail without ever decoding
		// block 1, then confirm the bound matches the tail exactly.
		tailStart := 2 * postingsBlockLen
		if err := bm.ShallowAdvance(postings[tailStart].docNum); err != nil {
			t.Fatal(err)
		}
		checkBound(t, bm, postings[tailStart:], postings[len(postings)-1].docNum, len(postings)-tailStart)

		// Bulk-decode the tail and check its contents.
		decodeAndCheck(t, bm, postings[tailStart:])

		// Past the end: no more blocks, and NextBlock reports exhaustion.
		if err := bm.ShallowAdvance(uint64(numDocs) + 1000); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, ok := bm.BlockMax(); ok {
			t.Fatal("BlockMax() after exhaustion reported a bound")
		}
		docs := make([]uint64, 10)
		freqs := make([]uint64, 10)
		norms := make([]float64, 10)
		if n, err := bm.NextBlock(docs, freqs, norms, 0); err != nil || n != 0 {
			t.Fatalf("NextBlock() after exhaustion = (%d, %v), want (0, nil)", n, err)
		}
	})

	t.Run("NextBlock resumes correctly across a physical block boundary", func(t *testing.T) {
		// A caller's own batch size (e.g. the collector's 256-doc
		// DocScoreBlock) need not line up with this format's 128-doc
		// physical block at all -- NextBlock must transparently span
		// blocks, and must also correctly resume mid-block when the
		// caller's buffer is smaller than a physical block.
		term := "all"
		postings := model.terms[term]
		_, bm := getIterator(t, term)

		const step = 37 // deliberately not a divisor of postingsBlockLen or numDocs
		var got []expPosting
		for {
			docs := make([]uint64, step)
			freqs := make([]uint64, step)
			norms := make([]float64, step)
			n, err := bm.NextBlock(docs, freqs, norms, 0)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
			for k := 0; k < n; k++ {
				got = append(got, expPosting{docs[k], freqs[k], 0}) // normID recovered below
			}
			if n < step {
				break
			}
		}
		if len(got) != len(postings) {
			t.Fatalf("stepped NextBlock produced %d postings, want %d", len(got), len(postings))
		}
		for k := range postings {
			if got[k].docNum != postings[k].docNum || got[k].freq != postings[k].freq {
				t.Fatalf("stepped NextBlock[%d] = (doc %d, freq %d), want (doc %d, freq %d)",
					k, got[k].docNum, got[k].freq, postings[k].docNum, postings[k].freq)
			}
		}
	})

	t.Run("ordinary Next resumes correctly after a partial NextBlock", func(t *testing.T) {
		// A caller (the bulk collector) can stop calling NextBlock at any
		// point -- there is no requirement to drain a physical block
		// completely -- and a DIFFERENT caller (a WAND-ineligible query
		// falling back to the scalar path mid-scan) must still be able to
		// continue correctly with ordinary Next() from exactly where that
		// left off.
		term := "all"
		postings := model.terms[term]
		itr, bm := getIterator(t, term)

		const partial = 40 // less than postingsBlockLen: stops mid-block
		docs := make([]uint64, partial)
		freqs := make([]uint64, partial)
		norms := make([]float64, partial)
		n, err := bm.NextBlock(docs, freqs, norms, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != partial {
			t.Fatalf("NextBlock returned %d postings, want %d", n, partial)
		}

		var got []expPosting
		for {
			next, err := itr.Next()
			if err != nil {
				t.Fatal(err)
			}
			if next == nil {
				break
			}
			pp := next.(*Posting)
			got = append(got, expPosting{pp.Number(), pp.Frequency(), pp.normID})
		}
		want := postings[partial:]
		if len(got) != len(want) {
			t.Fatalf("post-partial-NextBlock iteration yielded %d postings, want %d", len(got), len(want))
		}
		for k := range want {
			if got[k] != want[k] {
				t.Fatalf("post-partial-NextBlock posting[%d] = %+v, want %+v", k, got[k], want[k])
			}
		}
	})

	t.Run("ShallowAdvance does not corrupt ordinary iteration", func(t *testing.T) {
		term := "all"
		postings := model.terms[term]
		itr, bm := getIterator(t, term)

		// Shallow-seek deep into the list and peek its bound, without ever
		// calling Next/Advance -- this is exactly the sequence a block-max
		// pruning loop performs before deciding a block is worth scoring
		// for real. Unlike NextBlock, BlockMax never consumes anything, so
		// (unlike the sibling subtest above) this leaves every document in
		// the block -- including mid itself -- still there to Advance to.
		mid := postingsBlockLen + 5
		if err := bm.ShallowAdvance(postings[mid].docNum); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, ok := bm.BlockMax(); !ok {
			t.Fatal("BlockMax reported no bound")
		}

		// The ordinary path must still resolve to the correct posting from
		// wherever ShallowAdvance left the cursor -- despite i.positioned
		// having just been driven through the block-max capability rather
		// than Next/Advance.
		p, err := itr.Advance(postings[mid].docNum)
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			t.Fatalf("Advance(%d) returned nil", postings[mid].docNum)
		}
		if p.Number() != postings[mid].docNum || p.Frequency() != postings[mid].freq {
			t.Fatalf("Advance(%d) = (doc %d, freq %d), want (doc %d, freq %d)",
				postings[mid].docNum, p.Number(), p.Frequency(), postings[mid].docNum, postings[mid].freq)
		}

		// And plain forward iteration from here must still walk the rest of
		// the list correctly.
		var got []expPosting
		got = append(got, expPosting{p.Number(), p.Frequency(), p.(*Posting).normID})
		for {
			next, err := itr.Next()
			if err != nil {
				t.Fatal(err)
			}
			if next == nil {
				break
			}
			pp := next.(*Posting)
			got = append(got, expPosting{pp.Number(), pp.Frequency(), pp.normID})
		}
		want := postings[mid:]
		if len(got) != len(want) {
			t.Fatalf("post-ShallowAdvance iteration yielded %d postings, want %d", len(got), len(want))
		}
		for k := range want {
			if got[k] != want[k] {
				t.Fatalf("post-ShallowAdvance posting[%d] = %+v, want %+v", k, got[k], want[k])
			}
		}
	})

	t.Run("1-hit term reports no capability", func(t *testing.T) {
		term := "uniq0"
		if len(model.terms[term]) != 1 {
			t.Fatalf("expected %q to be a 1-hit term, got %d postings", term, len(model.terms[term]))
		}
		_, bm := getIterator(t, term)
		if _, _, _, _, ok := bm.BlockMax(); ok {
			t.Fatal("BlockMax on a 1-hit term reported a bound")
		}
		if err := bm.ShallowAdvance(0); err != nil {
			t.Fatalf("ShallowAdvance on a 1-hit term returned an error: %v", err)
		}
		docs := make([]uint64, 1)
		freqs := make([]uint64, 1)
		norms := make([]float64, 1)
		n, err := bm.NextBlock(docs, freqs, norms, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 || docs[0] != model.terms[term][0].docNum {
			t.Fatalf("NextBlock on a 1-hit term = (n=%d, doc=%d), want (1, %d)", n, docs[0], model.terms[term][0].docNum)
		}
	})
}

// TestBlockMaxCapabilityNoFreqField checks that a field indexed without
// frequencies reports no block-max bound (there is no tf to bound, and the
// skip entry's minNormID/maxTF bytes are zero-valued rather than a real
// bound) while NextBlock -- which only needs doc numbers, not the bound --
// still works.
func TestBlockMaxCapabilityNoFreqField(t *testing.T) {
	const numDocs = 300
	results := make([]index.Document, 0, numDocs)
	for i := 0; i < numDocs; i++ {
		f := newStubFieldSplitString("kw", nil, "constant value", false, false, false)
		f.options = index.IndexField | index.SkipFreqNorm
		doc := newStubDocument(fmt.Sprintf("d%d", i), []*stubField{
			newStubFieldSplitString("_id", nil, fmt.Sprintf("d%d", i), true, false, false),
			f,
		}, "_all")
		results = append(results, doc)
	}
	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	sb := seg.(*SegmentBase)

	dict, err := sb.dictionary("kw")
	if err != nil {
		t.Fatal(err)
	}
	pl, err := dict.postingsList([]byte("constant"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	itr := pl.Iterator(true, true, false, nil).(*PostingsIterator)
	bm := segment.PostingsIterator(itr).(segment.BlockMaxPostingsIterator)

	if _, _, _, _, ok := bm.BlockMax(); ok {
		t.Fatal("BlockMax on a no-freq field reported a bound")
	}
	docs := make([]uint64, postingsBlockLen)
	freqs := make([]uint64, postingsBlockLen)
	norms := make([]float64, postingsBlockLen)
	n, err := bm.NextBlock(docs, freqs, norms, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != postingsBlockLen {
		t.Fatalf("NextBlock on a no-freq field returned %d docs, want %d", n, postingsBlockLen)
	}
	for k := 0; k < n; k++ {
		if docs[k] != uint64(k) {
			t.Fatalf("NextBlock()[%d] = doc %d, want %d", k, docs[k], k)
		}
		if freqs[k] != 1 {
			t.Fatalf("NextBlock()[%d] = freq %d, want 1 (no-freq field placeholder)", k, freqs[k])
		}
	}
}
