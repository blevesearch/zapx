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
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// The tests in this file exist because the rest of the suite builds segments of
// a handful of documents, which never fill the 128-document block the format is
// built around. Everything interesting -- the skip list, the block boundary,
// the uvarint tail, the in-block search -- only shows up past that size.

// expPosting is the model these tests check the segment against.
type expPosting struct {
	docNum uint64
	freq   uint64
	normID uint8
}

// blockTestModel describes a synthetic corpus and what each term's postings
// should look like once written and read back.
type blockTestModel struct {
	numDocs int
	// term -> postings, in doc order
	terms map[string][]expPosting
}

// buildBlockTestSegment makes a segment whose terms span every case that
// matters: present in every document (packs at zero bits), present in a
// scattered subset, present exactly once, and repeated within a document so the
// frequency is not 1.
func buildBlockTestSegment(t *testing.T, numDocs int, termVectors bool) (
	*SegmentBase, *blockTestModel) {
	t.Helper()

	model := &blockTestModel{numDocs: numDocs, terms: map[string][]expPosting{}}
	results := make([]index.Document, 0, numDocs)

	for i := 0; i < numDocs; i++ {
		var tokens []string
		// "all" is in every document, so its deltas are all 1 and the doc block
		// packs at zero bits.
		tokens = append(tokens, "all")
		// A term repeated within the document, to exercise frequencies.
		reps := 1 + i%5
		for r := 0; r < reps; r++ {
			tokens = append(tokens, "rep")
		}
		for _, k := range []int{2, 3, 7, 50, 977} {
			if i%k == 0 {
				tokens = append(tokens, fmt.Sprintf("mod%d", k))
			}
		}
		// A term unique to this document: the 1-hit case.
		tokens = append(tokens, fmt.Sprintf("uniq%d", i))
		// Pad so field lengths vary and the norm column is not constant.
		for p := 0; p < i%37; p++ {
			tokens = append(tokens, "pad")
		}

		value := strings.Join(tokens, " ")
		f := newStubFieldSplitString("body", nil, value, false, false, termVectors)
		if !termVectors {
			f.options = index.IndexField
			for _, tf := range f.analyzedFreqs {
				tf.Locations = nil
			}
		}
		doc := newStubDocument(fmt.Sprintf("d%d", i), []*stubField{
			newStubFieldSplitString("_id", nil, fmt.Sprintf("d%d", i), true, false, false),
			f,
		}, "_all")
		results = append(results, doc)

		// The stub sets a field's analyzed length to its distinct token count,
		// which is what the norm column will quantize.
		normID := fieldNormToID(uint32(len(f.analyzedFreqs)))
		for term, tf := range f.analyzedFreqs {
			model.terms[term] = append(model.terms[term], expPosting{
				docNum: uint64(i),
				freq:   uint64(tf.Frequency()),
				normID: normID,
			})
		}
	}

	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	return seg.(*SegmentBase), model
}

// collect walks a postings list and returns what it yielded.
func collect(t *testing.T, sb *SegmentBase, field, term string,
	except *roaring.Bitmap, includeLocs bool) []expPosting {
	t.Helper()
	dict, err := sb.dictionary(field)
	if err != nil {
		t.Fatal(err)
	}
	if dict == nil {
		return nil
	}
	pl, err := dict.postingsList([]byte(term), except, nil)
	if err != nil {
		t.Fatal(err)
	}
	itr := pl.Iterator(true, true, includeLocs, nil)
	var got []expPosting
	for {
		next, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if next == nil {
			break
		}
		p, ok := next.(*Posting)
		if !ok {
			t.Fatalf("unexpected posting type %T", next)
		}
		got = append(got, expPosting{p.Number(), p.Frequency(), p.normID})
	}
	return got
}

func sortedTerms(m map[string][]expPosting) []string {
	rv := make([]string, 0, len(m))
	for k := range m {
		rv = append(rv, k)
	}
	sort.Strings(rv)
	return rv
}

func TestBlockPostingsRoundTrip(t *testing.T) {
	for _, numDocs := range []int{1, 2, 127, 128, 129, 255, 256, 257, 1000, 3000} {
		t.Run(fmt.Sprintf("docs%d", numDocs), func(t *testing.T) {
			sb, model := buildBlockTestSegment(t, numDocs, false)
			for _, term := range sortedTerms(model.terms) {
				want := model.terms[term]
				got := collect(t, sb, "body", term, nil, false)
				if !reflect.DeepEqual(got, want) {
					if len(got) != len(want) {
						t.Fatalf("term %q: got %d postings, want %d", term, len(got), len(want))
					}
					for i := range got {
						if got[i] != want[i] {
							t.Fatalf("term %q posting %d: got %+v want %+v",
								term, i, got[i], want[i])
						}
					}
				}
			}
		})
	}
}

// TestBlockPostingsCount checks that a doc frequency read straight off the term
// header agrees with what iteration produces.
func TestBlockPostingsCount(t *testing.T) {
	sb, model := buildBlockTestSegment(t, 1000, false)
	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range sortedTerms(model.terms) {
		pl, err := dict.postingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := pl.Count(), uint64(len(model.terms[term])); got != want {
			t.Errorf("term %q: Count() = %d, want %d", term, got, want)
		}
	}
}

func TestBlockPostingsAdvance(t *testing.T) {
	sb, model := buildBlockTestSegment(t, 3000, false)
	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}

	for _, stride := range []uint64{1, 2, 17, 128, 129, 511, 4096} {
		for _, term := range []string{"all", "rep", "mod2", "mod7", "mod50", "mod977"} {
			want := model.terms[term]
			pl, err := dict.postingsList([]byte(term), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			itr := pl.Iterator(true, true, false, nil)

			var got []expPosting
			var target uint64
			for {
				next, err := itr.Advance(target)
				if err != nil {
					t.Fatal(err)
				}
				if next == nil {
					break
				}
				p := next.(*Posting)
				got = append(got, expPosting{p.Number(), p.Frequency(), p.normID})
				target = p.Number() + stride
			}

			// The expected result is every posting whose doc number is the
			// first at or after each successive target.
			var exp []expPosting
			t2 := uint64(0)
			for _, w := range want {
				if w.docNum >= t2 {
					exp = append(exp, w)
					t2 = w.docNum + stride
				}
			}
			if !reflect.DeepEqual(got, exp) {
				t.Fatalf("term %q stride %d: got %d postings, want %d\ngot:  %v\nwant: %v",
					term, stride, len(got), len(exp), got, exp)
			}
		}
	}
}

func TestBlockPostingsWithDeletions(t *testing.T) {
	const numDocs = 2000
	sb, model := buildBlockTestSegment(t, numDocs, false)

	rng := rand.New(rand.NewSource(7))
	except := roaring.New()
	deleted := map[uint64]bool{}
	for i := 0; i < numDocs/3; i++ {
		d := uint64(rng.Intn(numDocs))
		except.Add(uint32(d))
		deleted[d] = true
	}
	// Also delete a whole contiguous block, which is the case that makes a
	// reader skip an entire decoded block.
	for d := uint64(256); d < 384; d++ {
		except.Add(uint32(d))
		deleted[d] = true
	}

	for _, term := range sortedTerms(model.terms) {
		var want []expPosting
		for _, p := range model.terms[term] {
			if !deleted[p.docNum] {
				want = append(want, p)
			}
		}
		got := collect(t, sb, "body", term, except, false)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("term %q with deletions: got %d, want %d\ngot:  %v\nwant: %v",
				term, len(got), len(want), got, want)
		}

		dict, _ := sb.dictionary("body")
		pl, err := dict.postingsList([]byte(term), except, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := pl.Count(), uint64(len(want)); got != want {
			t.Errorf("term %q: Count() with deletions = %d, want %d", term, got, want)
		}
	}
}

func TestBlockPostingsLocations(t *testing.T) {
	const numDocs = 700
	sb, model := buildBlockTestSegment(t, numDocs, true)

	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}

	for _, term := range sortedTerms(model.terms) {
		want := model.terms[term]
		pl, err := dict.postingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		itr := pl.Iterator(true, true, true, nil)

		i := 0
		for {
			next, err := itr.Next()
			if err != nil {
				t.Fatalf("term %q: %v", term, err)
			}
			if next == nil {
				break
			}
			if i >= len(want) {
				t.Fatalf("term %q: more postings than expected", term)
			}
			if next.Number() != want[i].docNum || next.Frequency() != want[i].freq {
				t.Fatalf("term %q posting %d: got doc %d freq %d, want doc %d freq %d",
					term, i, next.Number(), next.Frequency(), want[i].docNum, want[i].freq)
			}
			locs := next.Locations()
			if uint64(len(locs)) != want[i].freq {
				t.Fatalf("term %q doc %d: got %d locations, want %d",
					term, next.Number(), len(locs), want[i].freq)
			}
			for _, loc := range locs {
				if loc.Field() != "body" {
					t.Fatalf("term %q doc %d: location field %q, want body",
						term, next.Number(), loc.Field())
				}
				if loc.Pos() == 0 {
					t.Fatalf("term %q doc %d: location position 0", term, next.Number())
				}
			}
			i++
		}
		if i != len(want) {
			t.Fatalf("term %q: got %d postings, want %d", term, i, len(want))
		}
	}
}

// TestBlockPostingsLocationsWithAdvance exercises the location path under
// skipping, where the reader has to walk past documents it will not return in
// order to keep the location stream aligned.
func TestBlockPostingsLocationsWithAdvance(t *testing.T) {
	sb, model := buildBlockTestSegment(t, 900, true)
	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}

	for _, term := range []string{"all", "rep", "mod3"} {
		pl, err := dict.postingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		itr := pl.Iterator(true, true, true, nil)

		want := model.terms[term]
		var target uint64
		wi := 0
		for {
			next, err := itr.Advance(target)
			if err != nil {
				t.Fatal(err)
			}
			if next == nil {
				break
			}
			for wi < len(want) && want[wi].docNum < target {
				wi++
			}
			if wi >= len(want) || want[wi].docNum != next.Number() {
				t.Fatalf("term %q: Advance(%d) gave doc %d, want %d",
					term, target, next.Number(), want[wi].docNum)
			}
			if uint64(len(next.Locations())) != want[wi].freq {
				t.Fatalf("term %q doc %d: got %d locations, want %d",
					term, next.Number(), len(next.Locations()), want[wi].freq)
			}
			target = next.Number() + 101
		}
	}
}

func TestBlockPostingsNormsRoundTrip(t *testing.T) {
	sb, model := buildBlockTestSegment(t, 800, false)
	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	if dict.norms == nil || dict.norms.kind == normsKindAbsent {
		t.Fatal("field has no norm column")
	}
	for _, term := range sortedTerms(model.terms) {
		for _, want := range model.terms[term] {
			if got := dict.norms.id(uint32(want.docNum)); got != want.normID {
				t.Fatalf("doc %d: norm id %d, want %d", want.docNum, got, want.normID)
			}
		}
	}
	// A field whose documents all share a length collapses to the constant
	// encoding rather than one byte per document.
	if dict.norms.kind != normsKindDense {
		t.Errorf("expected a dense norm column for varying lengths, got kind %d",
			dict.norms.kind)
	}
}

func TestNormsColumnConstantEncoding(t *testing.T) {
	results := make([]index.Document, 0, 300)
	for i := 0; i < 300; i++ {
		results = append(results, newStubDocument(fmt.Sprintf("d%d", i), []*stubField{
			newStubFieldSplitString("_id", nil, fmt.Sprintf("d%d", i), true, false, false),
			newStubFieldSplitString("kw", nil, "constant value", false, false, false),
		}, "_all"))
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
	if dict.norms.kind != normsKindConstant {
		t.Fatalf("expected a constant norm column, got kind %d", dict.norms.kind)
	}
}

// TestBlockPostingsMerge covers the merge path at a size where the block
// structure has to be rebuilt against renumbered documents.
func TestBlockPostingsMerge(t *testing.T) {
	for _, withDrops := range []bool{false, true} {
		name := "clean"
		if withDrops {
			name = "withDrops"
		}
		t.Run(name, func(t *testing.T) {
			const perSeg = 700
			const numSegs = 3

			var sbs []*SegmentBase
			var models []*blockTestModel
			for s := 0; s < numSegs; s++ {
				sb, model := buildBlockTestSegment(t, perSeg, true)
				sbs = append(sbs, sb)
				models = append(models, model)
			}

			drops := make([]*roaring.Bitmap, numSegs)
			dropped := make([]map[uint64]bool, numSegs)
			for s := range drops {
				dropped[s] = map[uint64]bool{}
				if withDrops {
					bm := roaring.New()
					for d := 0; d < perSeg; d += 5 {
						bm.Add(uint32(d))
						dropped[s][uint64(d)] = true
					}
					drops[s] = bm
				}
			}

			tmp := getTempPath(fmt.Sprintf("zap-merge-block-%s.zap", name))
			_ = os.RemoveAll(tmp)
			defer func() { _ = os.RemoveAll(tmp) }()

			newDocNums, _, err := mergeSegmentBases(sbs, drops, tmp, DefaultChunkMode,
				nil, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}

			merged, err := zapPlugin.Open(tmp)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = merged.Close() }()
			msb := &merged.(*Segment).SegmentBase

			// Rebuild the model in the merged doc numbering.
			want := map[string][]expPosting{}
			for s, model := range models {
				for term, postings := range model.terms {
					for _, p := range postings {
						if dropped[s][p.docNum] {
							continue
						}
						nd := newDocNums[s][p.docNum]
						if nd == docDropped {
							t.Fatalf("segment %d doc %d unexpectedly dropped", s, p.docNum)
						}
						want[term] = append(want[term], expPosting{nd, p.freq, p.normID})
					}
				}
			}
			for term := range want {
				sort.Slice(want[term], func(a, b int) bool {
					return want[term][a].docNum < want[term][b].docNum
				})
			}

			for _, term := range sortedTerms(want) {
				got := collect(t, msb, "body", term, nil, false)
				if !reflect.DeepEqual(got, want[term]) {
					if len(got) != len(want[term]) {
						t.Fatalf("term %q: merged gave %d postings, want %d",
							term, len(got), len(want[term]))
					}
					for i := range got {
						if got[i] != want[term][i] {
							t.Fatalf("term %q posting %d: got %+v want %+v",
								term, i, got[i], want[term][i])
						}
					}
				}
			}

			// Locations must survive the merge too.
			mdict, err := msb.dictionary("body")
			if err != nil {
				t.Fatal(err)
			}
			pl, err := mdict.postingsList([]byte("rep"), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			itr := pl.Iterator(true, true, true, nil)
			seen := 0
			for {
				next, err := itr.Next()
				if err != nil {
					t.Fatal(err)
				}
				if next == nil {
					break
				}
				if uint64(len(next.Locations())) != next.Frequency() {
					t.Fatalf("merged doc %d: %d locations for freq %d",
						next.Number(), len(next.Locations()), next.Frequency())
				}
				seen++
			}
			if seen != len(want["rep"]) {
				t.Fatalf("merged term rep: walked %d postings, want %d", seen, len(want["rep"]))
			}
		})
	}
}

// TestTailSeekPastRangeSkipsDecode guards the fix for a real gap in the tail:
// without a real bound on it, seeking past a term's entire range always had
// to decode the tail's uvarint stream before it could report "not found" --
// unlike a full block, whose skip entry already lets a seek past it skip the
// payload entirely. This covers both a term that is nothing but a tail (no
// full blocks at all, the common case for a real dictionary's long tail) and
// a term with full blocks followed by a tail, and checks both that the
// answer is still right and that the tail is never decoded when the target
// is out of range.
func TestTailSeekPastRangeSkipsDecode(t *testing.T) {
	const numDocs = 300
	sb, model := buildBlockTestSegment(t, numDocs, false)

	cases := []struct {
		name string
		term string
	}{
		{"allTail", "mod50"},      // docFreq ~6: numFullBlocks == 0, nothing but a tail
		{"blocksPlusTail", "all"}, // docFreq == numDocs: full blocks, then a tail
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			postings := model.terms[tc.term]
			if len(postings) == 0 {
				t.Fatalf("term %q has no postings in the model", tc.term)
			}
			lastDoc := postings[len(postings)-1].docNum

			dict, err := sb.dictionary("body")
			if err != nil {
				t.Fatal(err)
			}
			pl, err := dict.postingsList([]byte(tc.term), nil, nil)
			if err != nil {
				t.Fatal(err)
			}

			// A target beyond every posting must come back empty, and must not
			// have decoded a block -- full or tail -- to find that out.
			itr := pl.Iterator(true, true, false, nil).(*PostingsIterator)
			farBeyond := lastDoc + uint64(numDocs) + 1000
			next, err := itr.Advance(farBeyond)
			if err != nil {
				t.Fatal(err)
			}
			if next != nil {
				t.Fatalf("term %q: Advance(%d) returned a posting, want none", tc.term, farBeyond)
			}
			if !itr.cursor.exhausted {
				t.Fatalf("term %q: Advance(%d) left the cursor un-exhausted", tc.term, farBeyond)
			}
			if itr.cursor.loaded {
				t.Fatalf("term %q: Advance(%d) decoded a block it should have ruled out from skip metadata alone",
					tc.term, farBeyond)
			}

			// The boundary itself must still be found: seeking to the term's
			// real last doc must succeed.
			itr2 := pl.Iterator(true, true, false, nil).(*PostingsIterator)
			got, err := itr2.Advance(lastDoc)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || got.Number() != lastDoc {
				t.Fatalf("term %q: Advance(%d) (the term's own last doc) should have found it", tc.term, lastDoc)
			}

			// One past the boundary must not be found, and by the same
			// no-decode path as the far-beyond case above.
			itr3 := pl.Iterator(true, true, false, nil).(*PostingsIterator)
			got3, err := itr3.Advance(lastDoc + 1)
			if err != nil {
				t.Fatal(err)
			}
			if got3 != nil {
				t.Fatalf("term %q: Advance(%d) returned a posting, want none", tc.term, lastDoc+1)
			}
		})
	}
}

// TestSearchBlock checks the branchless in-block lower bound against a linear
// scan, including the padded region past the end of a partial block.
func TestSearchBlock(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for trial := 0; trial < 500; trial++ {
		var arr [postingsBlockLen]uint32
		n := 1 + rng.Intn(postingsBlockLen)
		v := uint32(rng.Intn(10))
		for i := 0; i < n; i++ {
			v += 1 + uint32(rng.Intn(50))
			arr[i] = v
		}
		for i := n; i < postingsBlockLen; i++ {
			arr[i] = docNumTerminated
		}

		for probe := 0; probe < 20; probe++ {
			target := uint32(rng.Intn(int(v) + 10))
			want := 0
			for want < postingsBlockLen && arr[want] < target {
				want++
			}
			if got := searchBlock(&arr, target); got != want {
				t.Fatalf("searchBlock(target=%d) = %d, want %d", target, got, want)
			}
		}
	}
}

// TestOneHitEncoding verifies that a singleton term takes the inline encoding,
// which is the shape most of a real dictionary has.
func TestOneHitEncoding(t *testing.T) {
	sb, _ := buildBlockTestSegment(t, 500, false)
	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	val, exists, err := dict.fstReader.Get([]byte("uniq42"))
	if err != nil || !exists {
		t.Fatalf("term uniq42 missing: exists=%v err=%v", exists, err)
	}
	if val&FSTValEncodingMask != FSTValEncoding1Hit {
		t.Fatalf("expected uniq42 to use the 1-hit encoding, got %x", val)
	}
	if got := FSTValDecode1Hit(val); got != 42 {
		t.Fatalf("1-hit docNum = %d, want 42", got)
	}

	got := collect(t, sb, "body", "uniq42", nil, false)
	if len(got) != 1 || got[0].docNum != 42 || got[0].freq != 1 {
		t.Fatalf("uniq42 postings = %v", got)
	}

	// "all" is in every document, so it must not be 1-hit.
	val, _, err = dict.fstReader.Get([]byte("all"))
	if err != nil {
		t.Fatal(err)
	}
	if val&FSTValEncodingMask == FSTValEncoding1Hit {
		t.Fatal("term present in every document should not be 1-hit encoded")
	}
}

var _ segment.PostingsIterator = (*PostingsIterator)(nil)
