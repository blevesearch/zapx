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
	"bytes"
	"fmt"
	"math"
	"testing"
)

// checkTermBounds checks the bounds of every term of the field against what is
// true of its postings, which are in want: the footer has the block-max pair of
// the term exactly when it should (the term has frequencies and enough skip
// entries), the pair is the highest frequency (as a skip entry holds it) and the
// shortest field of the term, and TermBounds gives the same as walking the skip
// entries does, for a term that has the pair and for one that doesn't.
func checkTermBounds(t *testing.T, sb *SegmentBase, field string, want map[string][]expPosting) (withPair, without int) {
	t.Helper()
	dict, err := sb.dictionary(field)
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range sortedTerms(want) {
		postings := want[term]
		pl, err := dict.postingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pl.is1Hit {
			continue
		}
		it, err := pl.BlockIterator(true, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		bi := it.(*BlockPostingsIterator)
		h := &pl.footer
		entries := len(postings) / 128
		if len(postings)%128 > 0 {
			entries++
		}
		expectPair := h.hasFreqs() && entries >= termBoundsMinEntries
		if h.hasBounds() != expectPair {
			t.Fatalf("term %q (%d postings, %d skip entries): footer has the pair: %v, want %v",
				term, len(postings), entries, h.hasBounds(), expectPair)
		}

		var maxFreq uint64
		minNorm := uint8(0xFF)
		for _, p := range postings {
			if p.freq > maxFreq {
				maxFreq = p.freq
			}
			if p.normID < minNorm {
				minNorm = p.normID
			}
		}
		wantTF := encodeBlockMaxTF(uint32(maxFreq))
		if expectPair {
			if h.maxTF != wantTF || h.minNormID != minNorm {
				t.Fatalf("term %q: the footer says maxTF %d minNormID %d, want %d and %d",
					term, h.maxTF, h.minNormID, wantTF, minNorm)
			}
			withPair++
		} else {
			without++
		}

		got, scan := bi.TermBounds(), bi.termBoundsByScan()
		if got.LastDoc != scan.LastDoc || got.LastDoc != uint32(postings[len(postings)-1].docNum) {
			t.Fatalf("term %q: last doc %d (scan %d), want %d",
				term, got.LastDoc, scan.LastDoc, postings[len(postings)-1].docNum)
		}
		if got.FreqBounded != scan.FreqBounded || got.FreqBounded != (wantTF < math.MaxUint8) {
			t.Fatalf("term %q: frequency bounded %v (scan %v), want %v", term, got.FreqBounded, scan.FreqBounded, wantTF < math.MaxUint8)
		}
		// (MaxFreq says nothing when the frequencies aren't bounded)
		if got.FreqBounded && (got.MaxFreq != scan.MaxFreq || got.MaxFreq != uint32(maxFreq)) {
			t.Fatalf("term %q: max frequency %d (scan %d), want %d", term, got.MaxFreq, scan.MaxFreq, maxFreq)
		}
		if got.MaxNorm != scan.MaxNorm || got.MaxNorm != fieldNormFactor[minNorm] {
			t.Fatalf("term %q: max norm %v (scan %v), want %v", term, got.MaxNorm, scan.MaxNorm, fieldNormFactor[minNorm])
		}
	}
	return withPair, without
}

// The bounds of a term, which a pruner asks for before it reads anything, are in
// its footer once the term has several skip entries, and are right: for lists of
// every length, with and without a tail.
func TestTermBoundsOfASegment(t *testing.T) {
	for _, numDocs := range []int{100, 400, 600, 1500, 5000} {
		sb, model := buildBlockTestSegment(t, numDocs, false)
		withPair, without := checkTermBounds(t, sb, "body", model.terms)
		if numDocs >= 600 && withPair == 0 {
			t.Fatalf("%d docs: no term has the pair in its footer: the test proves nothing", numDocs)
		}
		if without == 0 {
			t.Fatalf("%d docs: every term has the pair: the test proves nothing about those that don't", numDocs)
		}
	}
}

// The frequency a skip entry, and so the footer, holds is a byte: one that is 255 or
// more is "at least that", and bounds nothing. Read back through the real read
// path, at the edge of that and past it, for a term that has the pair in its footer.
func TestTermBoundsOfATermThatSaturatesTheFrequency(t *testing.T) {
	repeat := func(term string, n int) []string {
		rv := make([]string, n)
		for i := range rv {
			rv[i] = term
		}
		return rv
	}
	const numDocs = 3000
	sb, model := buildBlockTestSegmentWith(t, numDocs, false, func(i int) []string {
		var rv []string
		rv = append(rv, "many") // (every doc has it, so it has many entries)
		switch i {
		case 1501:
			rv = append(rv, repeat("big", 300)...) // saturates
			rv = append(rv, repeat("edge", 254)...)
			rv = append(rv, repeat("over", 255)...)
		case 2999:
			rv = append(rv, repeat("big", 40)...)
		}
		if i%3 == 0 {
			rv = append(rv, "big", "edge", "over") // so that they have enough entries
		}
		return rv
	})
	checkTermBounds(t, sb, "body", model.terms)

	dict, err := sb.dictionary("body")
	if err != nil {
		t.Fatal(err)
	}
	bounded := map[string]bool{"big": false, "edge": true, "over": false, "many": true}
	for term, wantBounded := range bounded {
		pl, err := dict.postingsList([]byte(term), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !pl.footer.hasBounds() {
			t.Fatalf("term %q has no pair in its footer: the test proves nothing", term)
		}
		it, err := pl.BlockIterator(true, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := it.(*BlockPostingsIterator).TermBounds().FreqBounded; got != wantBounded {
			t.Fatalf("term %q: frequency bounded %v, want %v", term, got, wantBounded)
		}
	}
}

// Segments written without the pair (before the footer had it) are read by walking
// the skip entries, with the same bounds.
func TestTermBoundsWithoutThePairInTheFooter(t *testing.T) {
	old := termBoundsMinEntries
	termBoundsMinEntries = math.MaxInt
	t.Cleanup(func() { termBoundsMinEntries = old })

	sb, model := buildBlockTestSegment(t, 3000, false)
	withPair, without := checkTermBounds(t, sb, "body", model.terms)
	if withPair != 0 || without == 0 {
		t.Fatalf("%d terms have the pair, %d don't: want none to have it", withPair, without)
	}
}

// What the serializer writes in the footer, for terms that saturate the byte a
// frequency is kept in or don't, with any number of entries, and without
// frequencies: exactly the block-max pair of the term, and only when it is meant
// to be there.
func TestPostingsSerializerWritesTheBoundsOfTheTerm(t *testing.T) {
	type tc struct {
		name     string
		docs     int
		hasFreqs bool
		freqOf   func(i int) uint32
	}
	ones := func(int) uint32 { return 1 }
	cases := []tc{
		{"3 entries", 300, true, ones},
		{"3 full blocks, no tail", 384, true, ones},
		{"3 blocks and a tail", 400, true, ones},
		{"many blocks", 5000, true, ones},
		{"a big frequency in the middle", 5000, true, func(i int) uint32 {
			if i == 2500 {
				return 1000
			}
			return 1 + uint32(i%7)
		}},
		{"a frequency of 254, the largest that bounds", 1000, true, func(i int) uint32 {
			if i == 123 {
				return 254
			}
			return 2
		}},
		{"a frequency of 255, the first that doesn't", 1000, true, func(i int) uint32 {
			if i == 999 {
				return 255
			}
			return 2
		}},
		{"in the tail only", 1000, true, func(i int) uint32 {
			if i == 990 { // in the tail: the last 104 of 1000
				return 77
			}
			return 1
		}},
		{"no frequencies", 5000, false, ones},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// field norms: every doc's own, so that the shortest field is a
			// doc in the middle of the list, sometimes in a block of its own
			const total = 6000
			normIDs := make([]uint8, total)
			for i := range normIDs {
				normIDs[i] = uint8(40 + (i*7)%150)
			}
			normIDs[c.docs/3] = 12 // the shortest

			var buf bytes.Buffer
			w := NewFileWriterEmpty(NewCountHashWriter(&buf))
			if _, err := w.Write([]byte{0xAB}); err != nil { // the footer is never at offset 0
				t.Fatal(err)
			}
			ser := &postingsSerializer{w: w}
			ser.StartTerm(c.hasFreqs, false, normIDs)
			var maxFreq uint32
			wantNorm := uint8(0xFF)
			for i := 0; i < c.docs; i++ {
				doc := uint32(i * 2) // gaps, so that doc numbers aren't the position
				if int(doc) >= total {
					doc = uint32(i)
				}
				f := c.freqOf(i)
				if err := ser.AddDoc(doc, f); err != nil {
					t.Fatal(err)
				}
				if f > maxFreq {
					maxFreq = f
				}
				if id := normIDs[doc]; id < wantNorm {
					wantNorm = id
				}
			}
			if err := ser.FinishPayload(); err != nil {
				t.Fatal(err)
			}
			off, err := ser.Close(0, 0)
			if err != nil {
				t.Fatal(err)
			}

			var h termFooter
			if err := h.decode(buf.Bytes(), off); err != nil {
				t.Fatal(err)
			}
			entries := c.docs / 128
			if c.docs%128 > 0 {
				entries++
			}
			wantPair := c.hasFreqs && entries >= termBoundsMinEntries
			if h.hasBounds() != wantPair {
				t.Fatalf("%d entries: the footer has the pair: %v, want %v", entries, h.hasBounds(), wantPair)
			}
			if wantPair && (h.maxTF != encodeBlockMaxTF(maxFreq) || h.minNormID != wantNorm) {
				t.Fatalf("maxTF %d minNormID %d, want %d and %d", h.maxTF, h.minNormID, encodeBlockMaxTF(maxFreq), wantNorm)
			}
			if !wantPair && (h.maxTF != 0 || h.minNormID != 0) {
				t.Fatalf("a footer without the pair reads one: %d, %d", h.maxTF, h.minNormID)
			}
			if h.docFreq != uint32(c.docs) {
				t.Fatalf("docFreq %d, want %d", h.docFreq, c.docs)
			}
		})
	}
}

// footerInMemory puts the encoded footer where the footer decoder expects to find
// it: after the regions it describes, which it checks are no longer than the offset.
func footerInMemory(h termFooter) (mem []byte, offset uint64) {
	buf := make([]byte, maxTermFooterLen)
	n := h.encode(buf)
	offset = h.payloadLen + h.locsLen + h.skipLen
	return append(make([]byte, offset), buf[:n]...), offset
}

// A footer is read back as written, with and without locations and the pair, and a
// footer that was cut short in its pair isn't read.
func TestTermFooterRoundTrip(t *testing.T) {
	for _, withLocs := range []bool{false, true} {
		for _, withPair := range []bool{false, true} {
			what := fmt.Sprintf("locations %v, pair %v", withLocs, withPair)
			h := termFooter{docFreq: 123456, flags: termHasFreqs, payloadLen: 300000, locsLen: 77, skipLen: 9999}
			if withLocs {
				h.flags |= termHasLocs
				h.locChunkSize = 1024
			}
			if withPair {
				h.flags |= termHasBounds
				h.minNormID, h.maxTF = 17, 250
			}
			mem, offset := footerInMemory(h)
			var d termFooter
			if err := d.decode(mem, offset); err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if d.docFreq != h.docFreq || d.flags != h.flags || d.payloadLen != h.payloadLen ||
				d.locsLen != h.locsLen || d.skipLen != h.skipLen || d.locChunkSize != h.locChunkSize ||
				d.minNormID != h.minNormID || d.maxTF != h.maxTF {
				t.Fatalf("%s: decoded %+v, wrote %+v", what, d, h)
			}
			if d.skipStart != offset-h.skipLen || d.locsStart != d.skipStart-h.locsLen ||
				d.payloadStart != d.locsStart-h.payloadLen {
				t.Fatalf("%s: the regions are at %d, %d, %d for a footer at %d", what, d.payloadStart, d.locsStart, d.skipStart, offset)
			}
			if withPair {
				var short termFooter
				if err := short.decode(mem[:len(mem)-1], offset); err == nil {
					t.Fatalf("%s: a footer without the last byte of its pair was read", what)
				}
			}
		}
	}
}
