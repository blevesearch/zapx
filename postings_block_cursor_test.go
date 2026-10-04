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
	"math/rand"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

const blockCursorTestDocs = 700

func buildBlockCursorTestSegment(t *testing.T) *SegmentBase {
	t.Helper()
	var results []index.Document
	for i := 0; i < blockCursorTestDocs; i++ {
		toks := []string{"common"}
		if i%2 == 0 {
			toks = append(toks, "even")
		}
		if i%3 == 0 {
			toks = append(toks, "tri")
		}
		if i%9 == 0 { // frequency 3 for this term
			toks = append(toks, "tri", "tri")
		}
		if i == 421 {
			toks = append(toks, "rare")
		}
		if i == 12 || i == 640 {
			toks = append(toks, "pair")
		}
		// uneven field lengths so that the norms column is dense
		for k := 0; k < i%7; k++ {
			toks = append(toks, fmt.Sprintf("filler%d", k))
		}
		id := fmt.Sprintf("doc%d", i)
		results = append(results, newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldSplitString("body", nil, strings.Join(toks, " "), false, false, false),
		}, "_all"))
	}
	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	return seg.(*SegmentBase)
}

type cursorPosting struct {
	doc  uint32
	freq uint32
	norm float64
}

// viaIterator is the reference: what the existing PostingsIterator returns.
func viaIterator(t *testing.T, pl *PostingsList) []cursorPosting {
	t.Helper()
	itr := pl.Iterator(true, true, false, nil)
	var rv []cursorPosting
	for {
		p, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if p == nil {
			return rv
		}
		rv = append(rv, cursorPosting{uint32(p.Number()), uint32(p.Frequency()), p.Norm()})
	}
}

func drainBlockCursor(t *testing.T, c segment.BlockCursor) []cursorPosting {
	t.Helper()
	var blk segment.PostingsBlock
	var rv []cursorPosting
	for {
		n, err := c.NextBlock(&blk)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return rv
		}
		if n > segment.PostingsBlockLen {
			t.Fatalf("block of %d", n)
		}
		for i := 0; i < n; i++ {
			rv = append(rv, cursorPosting{blk.Docs[i], blk.Freqs[i], float64(blk.Norms[i])})
		}
	}
}

func comparePostings(t *testing.T, what string, got, want []cursorPosting) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d postings, want %d", what, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: posting %d: got %+v, want %+v", what, i, got[i], want[i])
		}
	}
}

func deletionSets() map[string]*roaring.Bitmap {
	rnd := rand.New(rand.NewSource(7))
	random := roaring.New()
	for i := 0; i < blockCursorTestDocs; i++ {
		if rnd.Intn(10) == 0 {
			random.Add(uint32(i))
		}
	}
	firstBlock := roaring.New() // wipes out the first 128 doc numbers
	firstBlock.AddRange(0, 128)
	everything := roaring.New()
	everything.AddRange(0, blockCursorTestDocs)
	return map[string]*roaring.Bitmap{
		"none":        nil,
		"empty":       roaring.New(),
		"random":      random,
		"first-block": firstBlock,
		"everything":  everything,
		"the-rare":    roaring.BitmapOf(421),
	}
}

func TestBlockCursorMatchesPostingsIterator(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, err := sb.Dictionary("body")
	if err != nil {
		t.Fatal(err)
	}

	// common: 700 docs (5 blocks + tail); even: 350; tri: 234 (1 block + tail);
	// rare: a 1-hit list; pair: 2 docs (only a tail); missing: no such term
	terms := []string{"common", "even", "tri", "rare", "pair", "filler3", "missing"}
	for delName, except := range deletionSets() {
		for _, term := range terms {
			what := fmt.Sprintf("term=%s deleted=%s", term, delName)
			pl, err := dict.PostingsList([]byte(term), except, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := viaIterator(t, pl.(*PostingsList))

			prov, ok := pl.(segment.BlockCursorProvider)
			if !ok {
				t.Fatal("PostingsList isn't a BlockCursorProvider")
			}
			cur, err := prov.BlockCursor(true, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			comparePostings(t, what, drainBlockCursor(t, cur), want)

			// exhausted stays exhausted
			var blk segment.PostingsBlock
			if n, err := cur.NextBlock(&blk); n != 0 || err != nil {
				t.Fatalf("%s: after exhaustion got n=%d err=%v", what, n, err)
			}

			// reusing the cursor on the same list gives the same answer
			cur, err = prov.BlockCursor(true, true, cur)
			if err != nil {
				t.Fatal(err)
			}
			comparePostings(t, what+" (reused)", drainBlockCursor(t, cur), want)
		}
	}
}

func TestBlockCursorWithoutFreqsAndNorms(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	for _, term := range []string{"common", "rare"} {
		pl, err := dict.PostingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := viaIterator(t, pl.(*PostingsList))
		cur, err := pl.(segment.BlockCursorProvider).BlockCursor(false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		var blk segment.PostingsBlock
		i := 0
		for {
			n, err := cur.NextBlock(&blk)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
			for j := 0; j < n; j++ {
				if blk.Docs[j] != want[i].doc {
					t.Fatalf("%s: doc %d: got %d want %d", term, i, blk.Docs[j], want[i].doc)
				}
				i++
			}
		}
		if i != len(want) {
			t.Fatalf("%s: got %d docs want %d", term, i, len(want))
		}
	}
}

func TestBlockCursorSeekBlock(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	rnd := rand.New(rand.NewSource(11))

	for delName, except := range deletionSets() {
		for _, term := range []string{"common", "even", "tri", "rare", "pair"} {
			pl, err := dict.PostingsList([]byte(term), except, nil)
			if err != nil {
				t.Fatal(err)
			}
			all := viaIterator(t, pl.(*PostingsList))

			for trial := 0; trial < 40; trial++ {
				target := uint64(rnd.Intn(blockCursorTestDocs + 20))
				what := fmt.Sprintf("term=%s deleted=%s target=%d", term, delName, target)

				// expected: everything >= target
				var want []cursorPosting
				for _, p := range all {
					if uint64(p.doc) >= target {
						want = append(want, p)
					}
				}

				cur, err := pl.(segment.BlockCursorProvider).BlockCursor(true, true, nil)
				if err != nil {
					t.Fatal(err)
				}
				var blk segment.PostingsBlock
				n, err := cur.SeekBlock(target, &blk)
				if err != nil {
					t.Fatal(err)
				}
				var got []cursorPosting
				for i := 0; i < n; i++ {
					got = append(got, cursorPosting{blk.Docs[i], blk.Freqs[i], float64(blk.Norms[i])})
				}
				// nothing below target comes back, and the first block starts
				// at the first live posting >= target
				if len(want) == 0 {
					if n != 0 {
						t.Fatalf("%s: expected exhaustion, got %d", what, n)
					}
					continue
				}
				if n == 0 || got[0] != want[0] {
					t.Fatalf("%s: first posting %+v, want %+v", what, got, want[0])
				}
				// the rest of the list follows with NextBlock
				got = append(got, drainBlockCursor(t, cur)...)
				comparePostings(t, what, got, want)
			}
		}
	}
}

func TestBlockCursorSeekBlockWalksForward(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	pl, _ := dict.PostingsList([]byte("common"), nil, nil)
	cur, _ := pl.(segment.BlockCursorProvider).BlockCursor(true, false, nil)

	var blk segment.PostingsBlock
	n, err := cur.SeekBlock(300, &blk)
	if err != nil || n == 0 || blk.Docs[0] != 300 {
		t.Fatalf("seek 300: n=%d err=%v first=%d", n, err, blk.Docs[0])
	}
	// seeking backwards doesn't rewind
	n, err = cur.SeekBlock(10, &blk)
	if err != nil || n == 0 || blk.Docs[0] <= 300 {
		t.Fatalf("seek back: n=%d err=%v first=%d", n, err, blk.Docs[0])
	}
	// past the end
	n, err = cur.SeekBlock(1<<40, &blk)
	if err != nil || n != 0 {
		t.Fatalf("seek past end: n=%d err=%v", n, err)
	}
}

func TestBlockCursorEmptyPostingsList(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	pl, _ := dict.PostingsList([]byte("missing"), nil, nil)
	cur, err := pl.(segment.BlockCursorProvider).BlockCursor(true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	var blk segment.PostingsBlock
	if n, err := cur.NextBlock(&blk); n != 0 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if cur.Count() != 0 {
		t.Fatalf("count %d", cur.Count())
	}
}

func TestBlockCursorLiveCount(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	for delName, except := range deletionSets() {
		for _, term := range []string{"common", "even", "tri", "rare", "pair", "missing"} {
			pl, err := dict.PostingsList([]byte(term), except, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(len(viaIterator(t, pl.(*PostingsList))))
			cur, err := pl.(segment.BlockCursorProvider).BlockCursor(true, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			// the position of the cursor doesn't matter, and isn't changed
			var blk segment.PostingsBlock
			_, _ = cur.NextBlock(&blk)
			got, err := cur.LiveCount()
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("term=%s deleted=%s: live count %d, want %d", term, delName, got, want)
			}
			rest := drainBlockCursor(t, cur)
			all := viaIterator(t, pl.(*PostingsList))
			if len(rest) > len(all) {
				t.Fatalf("term=%s deleted=%s: cursor moved by LiveCount", term, delName)
			}
		}
	}
}

// buildSaturatedSegment has a term with 300 docs, in one of which the term has
// a frequency far past what a skip entry's maxTF can record.
func buildSaturatedSegment(t *testing.T) *SegmentBase {
	t.Helper()
	var results []index.Document
	for i := 0; i < 300; i++ {
		toks := []string{"big"}
		if i == 200 {
			for k := 0; k < 400; k++ {
				toks = append(toks, "big")
			}
		}
		id := fmt.Sprintf("doc%d", i)
		results = append(results, newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldSplitString("body", nil, strings.Join(toks, " "), false, false, false),
		}, "_all"))
	}
	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	return seg.(*SegmentBase)
}

func blockMaxCursor(t *testing.T, pl interface{}) (segment.BlockCursor, segment.BlockMaxCursor) {
	t.Helper()
	cur, err := pl.(segment.BlockCursorProvider).BlockCursor(true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	bm, ok := cur.(segment.BlockMaxCursor)
	if !ok {
		t.Fatal("not a BlockMaxCursor")
	}
	return cur, bm
}

// every block's bounds bound its postings, and are exact when the frequency
// isn't saturated
func TestBlockMaxBoundsBoundThePostings(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	for delName, except := range deletionSets() {
		for _, term := range []string{"common", "even", "tri", "rare", "pair", "filler3"} {
			pl, err := dict.PostingsList([]byte(term), except, nil)
			if err != nil {
				t.Fatal(err)
			}
			cur, bm := blockMaxCursor(t, pl)

			var blk segment.PostingsBlock
			for {
				n, err := cur.NextBlock(&blk)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				got := bm.DecodedBounds()
				if got.LastDoc < blk.Docs[n-1] {
					t.Fatalf("%s/%s: last doc %d below the block's own %d", delName, term, got.LastDoc, blk.Docs[n-1])
				}
				if delName == "none" && got.LastDoc != blk.Docs[n-1] {
					t.Fatalf("%s/%s: last doc %d, block ends at %d", delName, term, got.LastDoc, blk.Docs[n-1])
				}
				// asking for the block by its first posting gives the same bounds
				// (with deletions the first posting may be of a later block's range)
				if at, ok := bm.BoundsAt(blk.Docs[0]); delName == "none" && (!ok || at != got) {
					t.Fatalf("%s/%s: bounds at %d are %+v, decoded %+v", delName, term, blk.Docs[0], at, got)
				}
				for i := 0; i < n; i++ {
					if got.FreqBounded && blk.Freqs[i] > got.MaxFreq {
						t.Fatalf("%s/%s: freq %d above the block's bound %d", delName, term, blk.Freqs[i], got.MaxFreq)
					}
					if blk.Norms[i] > got.MaxNorm {
						t.Fatalf("%s/%s: norm %v above the block's bound %v", delName, term, blk.Norms[i], got.MaxNorm)
					}
				}
			}
		}
	}
}

// BoundsAt describes the block that holds the first posting >= target, and
// doesn't move the cursor
func TestBlockMaxBoundsAt(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	rnd := rand.New(rand.NewSource(3))
	for _, term := range []string{"common", "even", "tri", "pair", "rare", "missing"} {
		pl, _ := dict.PostingsList([]byte(term), nil, nil)
		all := viaIterator(t, pl.(*PostingsList))
		cur, bm := blockMaxCursor(t, pl)

		// walk the blocks once, to know what each one is
		var blocks []segment.BlockBounds
		var firsts []uint32
		{
			c2, bm2 := blockMaxCursor(t, pl)
			var blk segment.PostingsBlock
			for {
				n, err := c2.NextBlock(&blk)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				blocks = append(blocks, bm2.DecodedBounds())
				firsts = append(firsts, blk.Docs[0])
			}
		}

		for trial := 0; trial < 80; trial++ {
			target := uint32(rnd.Intn(blockCursorTestDocs + 30))
			bd, ok := bm.BoundsAt(target)

			var wantIdx = -1
			for i, b := range blocks {
				if b.LastDoc >= target {
					wantIdx = i
					break
				}
			}
			if ok != (wantIdx >= 0) {
				t.Fatalf("%s target=%d: ok=%v, but a block holding it exists: %v", term, target, ok, wantIdx >= 0)
			}
			if !ok {
				continue
			}
			if bd != blocks[wantIdx] {
				t.Fatalf("%s target=%d: bounds %+v, want %+v", term, target, bd, blocks[wantIdx])
			}
			_ = firsts
			_ = all
		}

		// asking didn't move anything
		got := drainBlockCursor(t, cur)
		if len(got) != len(all) {
			t.Fatalf("%s: BoundsAt moved the cursor: %d postings, want %d", term, len(got), len(all))
		}
	}
}

func TestBlockMaxTermBounds(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	for _, term := range []string{"common", "even", "tri", "pair", "rare", "missing"} {
		pl, _ := dict.PostingsList([]byte(term), nil, nil)
		cur, bm := blockMaxCursor(t, pl)
		tb := bm.TermBounds()

		var blk segment.PostingsBlock
		var maxFreq uint32
		var maxNorm float32
		var last uint32
		for {
			n, err := cur.NextBlock(&blk)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				break
			}
			for i := 0; i < n; i++ {
				maxFreq = max(maxFreq, blk.Freqs[i])
				maxNorm = max(maxNorm, blk.Norms[i])
			}
			last = blk.Docs[n-1]
		}
		if term == "missing" {
			if tb.LastDoc != math.MaxUint32 {
				t.Fatalf("missing: %+v", tb)
			}
			continue
		}
		if tb.LastDoc != last || !tb.FreqBounded || tb.MaxFreq != maxFreq || tb.MaxNorm != maxNorm {
			t.Fatalf("%s: term bounds %+v, postings say last=%d maxFreq=%d maxNorm=%v", term, tb, last, maxFreq, maxNorm)
		}
	}
}

func TestBlockMaxSaturatedFrequency(t *testing.T) {
	sb := buildSaturatedSegment(t)
	dict, _ := sb.Dictionary("body")
	pl, _ := dict.PostingsList([]byte("big"), nil, nil)
	cur, bm := blockMaxCursor(t, pl)

	if tb := bm.TermBounds(); tb.FreqBounded {
		t.Fatalf("a frequency of 400 can't be bounded by a byte: %+v", tb)
	}

	// doc 200 is in the second block; the first one and the tail are bounded
	var blk segment.PostingsBlock
	var bounded []bool
	for {
		n, err := cur.NextBlock(&blk)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		bounded = append(bounded, bm.DecodedBounds().FreqBounded)
	}
	if len(bounded) != 3 || !bounded[0] || bounded[1] || !bounded[2] {
		t.Fatalf("blocks bounded: %v, want [true false true]", bounded)
	}
}

func TestBlockMaxOneHit(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	pl, _ := dict.PostingsList([]byte("rare"), nil, nil) // 1-hit
	_, bm := blockMaxCursor(t, pl)

	if _, ok := bm.BoundsAt(422); ok {
		t.Fatal("the only doc is 421")
	}
	bd, ok := bm.BoundsAt(421)
	if !ok || bd.LastDoc != 421 || bd.MaxFreq != 1 || !bd.FreqBounded {
		t.Fatalf("%+v %v", bd, ok)
	}
	if tb := bm.TermBounds(); tb != bd {
		t.Fatalf("term bounds %+v, want %+v", tb, bd)
	}
}

// fillNormsReference is fillNorms as it was written first, per doc checks and all
func fillNormsReference(b *PostingsBlockCursor, out *segment.PostingsBlock, n int) {
	if !b.normIsDense {
		for i := 0; i < n; i++ {
			out.Norms[i] = b.normConst
		}
		return
	}
	dense := b.normDense
	for i := 0; i < n; i++ {
		var id uint8
		if d := int(out.Docs[i]); d < len(dense) {
			id = dense[d]
		}
		out.Norms[i] = fieldNormFactor[id]
	}
}

func TestFillNormsMatchesReference(t *testing.T) {
	rnd := rand.New(rand.NewSource(5))
	dense := make([]uint8, 5000)
	for i := range dense {
		dense[i] = uint8(rnd.Intn(256))
	}
	for _, c := range []struct {
		name  string
		dense bool
		cut   int // length of the column
	}{{"dense", true, 5000}, {"dense, short column", true, 2500}, {"constant", false, 0}} {
		b := &PostingsBlockCursor{normIsDense: c.dense, normConst: fieldNormFactor[77]}
		if c.dense {
			b.normDense = dense[:c.cut]
		}
		for _, n := range []int{0, 1, 2, 7, 64, 127, 128} {
			var blk, want segment.PostingsBlock
			d := uint32(rnd.Intn(30))
			for i := 0; i < n; i++ {
				blk.Docs[i] = d
				d += 1 + uint32(rnd.Intn(40)) // some are past the short column
			}
			want = blk
			b.fillNorms(&blk, n)
			fillNormsReference(b, &want, n)
			for i := 0; i < n; i++ {
				if blk.Norms[i] != want.Norms[i] {
					t.Fatalf("%s n=%d doc %d: %v, want %v", c.name, n, blk.Docs[i], blk.Norms[i], want.Norms[i])
				}
			}
		}
	}
}

func BenchmarkFillNorms(b *testing.B) {
	rnd := rand.New(rand.NewSource(5))
	dense := make([]uint8, 50000)
	for i := range dense {
		dense[i] = uint8(rnd.Intn(256))
	}
	for _, gap := range []int{1, 10, 100} { // distance between the docs of a block
		cur := &PostingsBlockCursor{normIsDense: true, normDense: dense}
		var blk segment.PostingsBlock
		for i := range blk.Docs {
			blk.Docs[i] = uint32(10 + i*gap)
		}
		b.Run(fmt.Sprintf("gap=%d/new", gap), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cur.fillNorms(&blk, 128)
			}
		})
		b.Run(fmt.Sprintf("gap=%d/reference", gap), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				fillNormsReference(cur, &blk, 128)
			}
		})
	}
}

// BoundsAt answers the same wherever the cursor is: ahead of the target, on
// it, or short of it by more blocks than the fast path probes.
func TestBlockMaxBoundsAtWhileMoving(t *testing.T) {
	sb := buildBlockCursorTestSegment(t)
	dict, _ := sb.Dictionary("body")
	rnd := rand.New(rand.NewSource(11))
	for _, term := range []string{"common", "even", "tri", "pair", "rare", "missing"} {
		pl, _ := dict.PostingsList([]byte(term), nil, nil)

		var blocks []segment.BlockBounds
		{
			c2, bm2 := blockMaxCursor(t, pl)
			var blk segment.PostingsBlock
			for {
				n, err := c2.NextBlock(&blk)
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
				blocks = append(blocks, bm2.DecodedBounds())
			}
		}

		for round := 0; round < 30; round++ {
			cur, bm := blockMaxCursor(t, pl)
			var blk segment.PostingsBlock
			for step := 0; step < 12; step++ {
				for ask := 0; ask < 8; ask++ {
					target := uint32(rnd.Intn(blockCursorTestDocs + 30))
					bd, ok := bm.BoundsAt(target)
					want := -1
					for i, b := range blocks {
						if b.LastDoc >= target {
							want = i
							break
						}
					}
					if ok != (want >= 0) || (ok && bd != blocks[want]) {
						t.Fatalf("%s round %d step %d target=%d: got %+v/%v, want block %d of %d",
							term, round, step, target, bd, ok, want, len(blocks))
					}
				}
				var n int
				var err error
				if rnd.Intn(2) == 0 {
					n, err = cur.NextBlock(&blk)
				} else {
					n, err = cur.SeekBlock(uint64(rnd.Intn(blockCursorTestDocs)), &blk)
				}
				if err != nil {
					t.Fatal(err)
				}
				if n == 0 {
					break
				}
			}
		}
	}
}
