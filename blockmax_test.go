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

	t.Run("full block then tail", func(t *testing.T) {
		term := "all" // present in every doc: numFullBlocks == 2, tailLen == 44 for 300 docs
		postings := model.terms[term]
		if len(postings) != numDocs {
			t.Fatalf("expected %q in all %d docs, got %d", term, numDocs, len(postings))
		}
		_, bm := getIterator(t, term)

		// Shallow-seek to the very first doc: must land on block 0 without
		// having decoded anything, and report block 0's bound.
		lastDoc, ok := bm.SeekBlock(postings[0].docNum)
		if !ok {
			t.Fatal("SeekBlock(0) reported no block")
		}
		if lastDoc != postings[postingsBlockLen-1].docNum {
			t.Fatalf("SeekBlock(0) lastDocInBlock = %d, want %d", lastDoc, postings[postingsBlockLen-1].docNum)
		}
		wantMinNormID, wantMaxTF := boundOf(postings[:postingsBlockLen])
		if got, ok := bm.BlockMaxTF(); !ok || got != wantMaxTF {
			t.Fatalf("block 0 BlockMaxTF() = (%d, %v), want (%d, true)", got, ok, wantMaxTF)
		}
		if got, ok := bm.BlockMinNormID(); !ok || got != wantMinNormID {
			t.Fatalf("block 0 BlockMinNormID() = (%d, %v), want (%d, true)", got, ok, wantMinNormID)
		}
		if got, want := bm.NormFromID(wantMinNormID), normFactorFromID(wantMinNormID); got != want {
			t.Fatalf("NormFromID(%d) = %v, want %v", wantMinNormID, got, want)
		}

		// Batch-decode the same block and check it against the model exactly.
		docs, freqs, norms, ok := bm.CurrentBlock()
		if !ok {
			t.Fatal("CurrentBlock() reported no block")
		}
		if len(docs) != postingsBlockLen || len(freqs) != postingsBlockLen || len(norms) != postingsBlockLen {
			t.Fatalf("CurrentBlock() returned %d docs / %d freqs / %d norms, want %d", len(docs), len(freqs), len(norms), postingsBlockLen)
		}
		for k := 0; k < postingsBlockLen; k++ {
			wantNorm := normFactorFromID(postings[k].normID)
			if docs[k] != postings[k].docNum || freqs[k] != postings[k].freq || norms[k] != wantNorm {
				t.Fatalf("CurrentBlock()[%d] = (doc %d, freq %d, norm %v), want (doc %d, freq %d, norm %v)",
					k, docs[k], freqs[k], norms[k], postings[k].docNum, postings[k].freq, wantNorm)
			}
		}

		// Skip straight past block 0 and block 1 into the tail using only
		// shallow seeks, then confirm the bound matches the tail.
		tailStart := 2 * postingsBlockLen
		lastDoc, ok = bm.SeekBlock(postings[tailStart].docNum)
		if !ok {
			t.Fatal("SeekBlock(tail) reported no block")
		}
		if lastDoc != postings[len(postings)-1].docNum {
			t.Fatalf("SeekBlock(tail) lastDocInBlock = %d, want %d", lastDoc, postings[len(postings)-1].docNum)
		}
		wantMinNormID, wantMaxTF = boundOf(postings[tailStart:])
		if got, ok := bm.BlockMaxTF(); !ok || got != wantMaxTF {
			t.Fatalf("tail BlockMaxTF() = (%d, %v), want (%d, true)", got, ok, wantMaxTF)
		}
		if got, ok := bm.BlockMinNormID(); !ok || got != wantMinNormID {
			t.Fatalf("tail BlockMinNormID() = (%d, %v), want (%d, true)", got, ok, wantMinNormID)
		}
		docs, freqs, norms, ok = bm.CurrentBlock()
		if !ok {
			t.Fatal("CurrentBlock() on the tail reported no block")
		}
		wantTail := postings[tailStart:]
		if len(docs) != len(wantTail) {
			t.Fatalf("CurrentBlock() on the tail returned %d docs, want %d", len(docs), len(wantTail))
		}
		for k := range wantTail {
			wantNorm := normFactorFromID(wantTail[k].normID)
			if docs[k] != wantTail[k].docNum || freqs[k] != wantTail[k].freq || norms[k] != wantNorm {
				t.Fatalf("tail CurrentBlock()[%d] = (doc %d, freq %d, norm %v), want (doc %d, freq %d, norm %v)",
					k, docs[k], freqs[k], norms[k], wantTail[k].docNum, wantTail[k].freq, wantNorm)
			}
		}

		// Past the end: no more blocks.
		if _, ok := bm.SeekBlock(uint64(numDocs) + 1000); ok {
			t.Fatal("SeekBlock past the end of the list reported a block")
		}
		if _, ok := bm.BlockMaxTF(); ok {
			t.Fatal("BlockMaxTF() after exhaustion reported a bound")
		}
		if _, _, _, ok := bm.CurrentBlock(); ok {
			t.Fatal("CurrentBlock() after exhaustion reported a block")
		}
	})

	t.Run("SeekBlock does not corrupt ordinary iteration", func(t *testing.T) {
		term := "all"
		postings := model.terms[term]
		itr, bm := getIterator(t, term)

		// Shallow-seek deep into the list and force a batch decode, without
		// ever calling Next/Advance -- this is exactly the sequence a
		// block-max pruning loop performs before deciding a block is worth
		// scoring for real.
		mid := postingsBlockLen + 5
		if _, ok := bm.SeekBlock(postings[mid].docNum); !ok {
			t.Fatal("SeekBlock reported no block")
		}
		if _, _, _, ok := bm.CurrentBlock(); !ok {
			t.Fatal("CurrentBlock reported no block")
		}

		// The ordinary path must still resolve to the correct posting,
		// despite i.positioned/i.cur having been left pointing at block 0's
		// decode from the previous subtest's iterator (a fresh iterator
		// here, but the same underlying blockCursor/PostingsIterator code
		// path that a live one would reuse across many probes).
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
			t.Fatalf("post-SeekBlock iteration yielded %d postings, want %d", len(got), len(want))
		}
		for k := range want {
			if got[k] != want[k] {
				t.Fatalf("post-SeekBlock posting[%d] = %+v, want %+v", k, got[k], want[k])
			}
		}
	})

	t.Run("1-hit term reports no capability", func(t *testing.T) {
		term := "uniq0"
		if len(model.terms[term]) != 1 {
			t.Fatalf("expected %q to be a 1-hit term, got %d postings", term, len(model.terms[term]))
		}
		_, bm := getIterator(t, term)
		if _, ok := bm.SeekBlock(0); ok {
			t.Fatal("SeekBlock on a 1-hit term reported a block")
		}
		if _, ok := bm.BlockMaxTF(); ok {
			t.Fatal("BlockMaxTF on a 1-hit term reported a bound")
		}
		if _, ok := bm.BlockMinNormID(); ok {
			t.Fatal("BlockMinNormID on a 1-hit term reported a bound")
		}
		if _, _, _, ok := bm.CurrentBlock(); ok {
			t.Fatal("CurrentBlock on a 1-hit term reported a block")
		}
	})
}

// TestBlockMaxCapabilityNoFreqField checks that a field indexed without
// frequencies reports no block-max bound (there is no tf to bound, and the
// skip entry's minNormID/maxTF bytes are zero-valued rather than a real
// bound) while CurrentBlock -- which only needs doc numbers, not the bound --
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

	if _, ok := bm.SeekBlock(0); !ok {
		t.Fatal("SeekBlock on a no-freq field reported no block, want a block with no bound")
	}
	if _, ok := bm.BlockMaxTF(); ok {
		t.Fatal("BlockMaxTF on a no-freq field reported a bound")
	}
	if _, ok := bm.BlockMinNormID(); ok {
		t.Fatal("BlockMinNormID on a no-freq field reported a bound")
	}
	docs, _, _, ok := bm.CurrentBlock()
	if !ok {
		t.Fatal("CurrentBlock on a no-freq field reported no block")
	}
	if len(docs) != postingsBlockLen {
		t.Fatalf("CurrentBlock() on a no-freq field returned %d docs, want %d", len(docs), postingsBlockLen)
	}
	for k, d := range docs {
		if d != uint64(k) {
			t.Fatalf("CurrentBlock()[%d] = doc %d, want %d", k, d, k)
		}
	}
}
