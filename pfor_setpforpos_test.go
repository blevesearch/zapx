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

// Tests for chunkedIntDecoder.SetPFORPos (§36C support function).
//
// SetPFORPos(pos) sets pforPos = pos so the next readUvarint() returns
// pforDecoded[pos] instead of scanning sequentially.  It is invoked by
// nextDocNumAtOrAfter when the §36C fast path fires:
//   ActualBM narrowed to a conjunction subset + !includeLocs + PFOR mode.
//
// The test exercises SetPFORPos at three boundary positions within one PFOR
// chunk: the first entry (pos=0), a middle entry, and the last entry —
// confirming that each random seek returns the correct frequency, not the
// value at pforPos=0 (a regression if SetPFORPos is not called between Advances).

import (
	"fmt"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
)

// makeMultiFreqDocs builds n documents where "alpha" appears exactly (i+1)
// times in document i (i in [0, n)).  No location vectors are stored so that
// the §36C fast path (includeLocs=false) fires in the test.
func makeMultiFreqDocs(n int) []index.Document {
	docs := make([]index.Document, n)
	for i := range docs {
		id := fmt.Sprintf("mfdoc%d", i)
		freq := i + 1
		body := strings.TrimSpace(strings.Repeat("alpha ", freq))
		docs[i] = newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			// termVectors=false → hasLocs=false → §36C fast path can fire.
			newStubFieldSplitString("body", nil, body, true, false, false),
		}, "_all")
	}
	return docs
}

// TestSetPFORPosSeekBoundaries verifies that SetPFORPos correctly repositions
// the PFOR decoder to positions 0, mid, and last within a single chunk.
//
// A PFOR segment is built with n docs where doc i has "alpha" with freq=i+1.
// All n docs fit in one chunk (pforBlockSize=256 >> n=8).  The test narrows
// ActualBM to a single doc at a time (forcing the §36C fast path to call
// SetPFORPos to the doc's ordinal in the chunk) and checks Frequency().
func TestSetPFORPosSeekBoundaries(t *testing.T) {
	const n = 8
	docs := makeMultiFreqDocs(n)
	s := buildCorpusSegment(t, docs, getTempPath("pfor_setpforpos.zap"))

	tests := []struct {
		docNum   uint64
		wantFreq uint64
	}{
		{0, 1}, // first entry in chunk (SetPFORPos(0))
		{3, 4}, // middle entry (SetPFORPos(3))
		{7, 8}, // last entry (SetPFORPos(7))
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("doc%d_freq%d", tc.docNum, tc.wantFreq), func(t *testing.T) {
			pi := getPostingsIterator(t, s, "body", "alpha", true, false)

			// Narrow ActualBM to a single doc so §36C fires and SetPFORPos is
			// called to seek to tc.docNum's ordinal in the chunk.
			single := roaring.New()
			single.Add(uint32(tc.docNum))
			pi.ActualBM = single
			pi.Actual = single.Iterator()

			posting, err := pi.Advance(tc.docNum)
			if err != nil {
				t.Fatalf("Advance(%d): %v", tc.docNum, err)
			}
			if posting == nil {
				t.Fatalf("Advance(%d) returned nil", tc.docNum)
			}
			if posting.Number() != tc.docNum {
				t.Errorf("posting.Number()=%d want %d", posting.Number(), tc.docNum)
			}
			if posting.Frequency() != tc.wantFreq {
				t.Errorf("Frequency()=%d want %d (SetPFORPos seek error?)",
					posting.Frequency(), tc.wantFreq)
			}
		})
	}
}

// TestSetPFORPosSequentialVsSeek verifies that iterating all docs sequentially
// and seeking to each doc individually (via single-doc ActualBM) produce the
// same Frequency() values — confirming SetPFORPos does not corrupt subsequent reads.
func TestSetPFORPosSequentialVsSeek(t *testing.T) {
	const n = 8
	docs := makeMultiFreqDocs(n)
	s := buildCorpusSegment(t, docs, getTempPath("pfor_setpforpos_vs.zap"))

	// Sequential pass: iterate all docs with full ActualBM.
	seqFreqs := make([]uint64, n)
	{
		pi := getPostingsIterator(t, s, "body", "alpha", true, false)
		for i := 0; i < n; i++ {
			posting, err := pi.Next()
			if err != nil {
				t.Fatalf("Next() at i=%d: %v", i, err)
			}
			if posting == nil {
				t.Fatalf("Next() returned nil at i=%d", i)
			}
			seqFreqs[i] = posting.Frequency()
		}
	}

	// Seek pass: for each doc, create a fresh iterator narrowed to that doc.
	for docNum := 0; docNum < n; docNum++ {
		pi := getPostingsIterator(t, s, "body", "alpha", true, false)
		single := roaring.New()
		single.Add(uint32(docNum))
		pi.ActualBM = single
		pi.Actual = single.Iterator()

		posting, err := pi.Advance(uint64(docNum))
		if err != nil {
			t.Fatalf("Advance(%d): %v", docNum, err)
		}
		if posting == nil {
			t.Fatalf("Advance(%d) returned nil", docNum)
		}
		got := posting.Frequency()
		if got != seqFreqs[docNum] {
			t.Errorf("doc %d: seek freq=%d sequential freq=%d (SetPFORPos divergence)",
				docNum, got, seqFreqs[docNum])
		}
	}
}
