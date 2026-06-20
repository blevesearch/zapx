// Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

// Tests for norm column preservation across merge with dropped documents (§20/v18+).
//
// The norm column stores one SmallFloat-encoded fieldLen byte per doc.  When a
// merge drops some docs, the merged segment must compact the norm column so that
// the surviving doc's norm byte is at the correct merged docNum — if a dropped
// doc's byte "shifts" the column incorrectly, every subsequent doc gets the wrong
// field length and scores drift upward or downward.
//
// TestNormColumnPreservedAfterMergeWithDrops builds two source segments, drops
// the first doc of the second segment during merge, and verifies that each of
// the four surviving docs has the expected raw norm byte (= encodeNormByte(fieldLen)).

import (
	"fmt"
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// filler words used to pad field length. All docs start with "alpha" so it
// always appears in the "body" posting list.
var fillerWords = []string{
	"alpha", "beta", "gamma", "delta", "epsilon",
	"zeta", "eta", "theta", "iota", "kappa",
}

// makeBodyDocs builds n documents where "body" contains (startIdx+i+1) unique
// terms starting with "alpha" for doc i (i in [0, n)).  The field length
// (unique-term count) equals (startIdx+i+1) so each doc gets a distinct norm byte.
// No location vectors are stored.
func makeBodyDocs(n, startIdx int) []index.Document {
	docs := make([]index.Document, n)
	for i := range docs {
		id := fmt.Sprintf("nbdoc%d", startIdx+i)
		fieldLen := startIdx + i + 1
		body := buildBody(fieldLen)
		docs[i] = newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldSplitString("body", nil, body, true, false, false),
		}, "_all")
	}
	return docs
}

// buildBody returns a space-separated string of the first fieldLen filler words.
// All bodies start with "alpha" so the "alpha" term appears in every document.
func buildBody(fieldLen int) string {
	if fieldLen <= 0 {
		return "alpha"
	}
	b := make([]byte, 0, fieldLen*7)
	for j := 0; j < fieldLen; j++ {
		if j > 0 {
			b = append(b, ' ')
		}
		word := fillerWords[j%len(fillerWords)]
		b = append(b, word...)
	}
	return string(b)
}

// TestNormColumnPreservedAfterMergeWithDrops verifies that after merging two
// segments — with the first doc of the second segment dropped — the merged
// norm column has the correct SmallFloat byte for each surviving doc.
//
// Source segments:
//   seg1: doc0 (fieldLen=1), doc1 (fieldLen=2), doc2 (fieldLen=3)
//   seg2: doc3 (fieldLen=4) [DROPPED], doc4 (fieldLen=5)
//
// Merged segment: 4 docs.
//   merged doc0 ← seg1 doc0: fieldLen=1
//   merged doc1 ← seg1 doc1: fieldLen=2
//   merged doc2 ← seg1 doc2: fieldLen=3
//   merged doc3 ← seg2 doc4: fieldLen=5 (doc3 was dropped)
func TestNormColumnPreservedAfterMergeWithDrops(t *testing.T) {
	// Build source segments.
	seg1docs := makeBodyDocs(3, 0) // fieldLens 1, 2, 3
	seg2docs := makeBodyDocs(2, 3) // fieldLens 4, 5

	path1 := getTempPath("norm_drop_s1.zap")
	path2 := getTempPath("norm_drop_s2.zap")
	outPath := getTempPath("norm_drop_out.zap")
	for _, p := range []string{path1, path2, outPath} {
		_ = os.RemoveAll(p)
		t.Cleanup(func() { _ = os.RemoveAll(p) })
	}

	sb1, _, err := zapPlugin.newWithChunkMode(seg1docs, PFORChunkMode, nil)
	if err != nil {
		t.Fatalf("newWithChunkMode seg1: %v", err)
	}
	if err = PersistSegmentBase(sb1.(*SegmentBase), path1); err != nil {
		t.Fatalf("PersistSegmentBase seg1: %v", err)
	}

	sb2, _, err := zapPlugin.newWithChunkMode(seg2docs, PFORChunkMode, nil)
	if err != nil {
		t.Fatalf("newWithChunkMode seg2: %v", err)
	}
	if err = PersistSegmentBase(sb2.(*SegmentBase), path2); err != nil {
		t.Fatalf("PersistSegmentBase seg2: %v", err)
	}

	s1, err := zapPlugin.Open(path1)
	if err != nil {
		t.Fatalf("open seg1: %v", err)
	}
	defer s1.Close()
	s2, err := zapPlugin.Open(path2)
	if err != nil {
		t.Fatalf("open seg2: %v", err)
	}
	defer s2.Close()

	// Drop the first doc (docNum=0) of seg2.
	drop2 := roaring.New()
	drop2.Add(0)
	drops := []*roaring.Bitmap{nil, drop2}

	if _, _, err = zapPlugin.Merge([]seg.Segment{s1, s2}, drops, outPath, nil, nil); err != nil {
		t.Fatalf("merge: %v", err)
	}
	merged, err := zapPlugin.Open(outPath)
	if err != nil {
		t.Fatalf("open merged: %v", err)
	}
	defer merged.Close()

	// Get a PostingsIterator with the norm column loaded.
	dict, err := merged.Dictionary("body")
	if err != nil {
		t.Fatalf("Dictionary: %v", err)
	}
	pl, err := dict.(*Dictionary).postingsList([]byte("alpha"), nil, nil)
	if err != nil {
		t.Fatalf("postingsList: %v", err)
	}
	// includeFreq=true, includeNorm=true loads the norm column slice.
	pi := pl.Iterator(true, true, false, nil).(*PostingsIterator)

	// Expected fieldLens for merged docNums 0..3.
	// Merged doc3 = seg2 doc4 (fieldLen=5), NOT seg2 doc3 (fieldLen=4, dropped).
	wantFieldLens := []uint32{1, 2, 3, 5}

	for docNum, wantLen := range wantFieldLens {
		gotByte := pi.NormColumnByte(uint64(docNum))
		wantByte := encodeNormByte(wantLen)
		if gotByte != wantByte {
			t.Errorf("merged docNum %d: NormColumnByte=%d want %d (fieldLen=%d; dropped doc shifted the column?)",
				docNum, gotByte, wantByte, wantLen)
		}
		// Also verify the decoded field length is close to expected.
		gotLen := normDecodeSmallFloat(gotByte)
		if gotLen != wantLen {
			// encodeNormByte is lossy; allow up to ±1 after round-trip.
			diff := int(gotLen) - int(wantLen)
			if diff < 0 {
				diff = -diff
			}
			if diff > 1 {
				t.Errorf("merged docNum %d: decoded fieldLen=%d want %d (±1 SmallFloat tolerance)",
					docNum, gotLen, wantLen)
			}
		}
	}
}
