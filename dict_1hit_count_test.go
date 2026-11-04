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
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// newStubFieldNoTermVectors builds a single-token field with term vectors off.
// newStubFieldSplitString always sets index.IncludeTermVectors, and a term with
// locations to store is never eligible for the 1-hit encoding.
func newStubFieldNoTermVectors(name, value string) *stubField {
	freqs := make(index.TokenFrequencies)
	token := &index.TokenFreq{Term: []byte(value)}
	token.SetFrequency(1)
	freqs[value] = token
	return &stubField{
		name:          name,
		value:         []byte(value),
		encodedType:   't',
		options:       index.IndexField | index.StoreField,
		analyzedLen:   len(freqs),
		analyzedFreqs: freqs,
	}
}

// A term held by exactly one document is written with the "1-hit" FST value
// encoding, which only the merge produces. DictionaryIterator reuses a single
// PostingsList across entries, so reading a 1-hit term must not leave state
// behind that makes a later multi-document term report a count of 1.
func TestDictionaryIteratorCountAfter1HitTerm(t *testing.T) {
	// "aaa" holds one document and sorts before "bbb", which holds three.
	docs := []index.Document{
		newStubDocument("a", []*stubField{
			newStubFieldSplitString("_id", nil, "a", true, false, false),
			newStubFieldNoTermVectors("domain", "aaa"),
		}, "_all"),
	}
	for _, id := range []string{"b", "c", "d"} {
		docs = append(docs, newStubDocument(id, []*stubField{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldNoTermVectors("domain", "bbb"),
		}, "_all"))
	}

	tmpPath := getTempPath("scorch-1hit.zap")
	mergedPath := getTempPath("scorch-1hit-merged.zap")
	_ = os.RemoveAll(tmpPath)
	_ = os.RemoveAll(mergedPath)
	defer func() {
		_ = os.RemoveAll(tmpPath)
		_ = os.RemoveAll(mergedPath)
	}()

	base, _, err := zapPlugin.newWithChunkMode(docs, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(base.(*SegmentBase), tmpPath); err != nil {
		t.Fatal(err)
	}
	segment, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		if cerr := segment.Close(); cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	// The merge writes the 1-hit encoding; the initial persist does not.
	segsToMerge := []seg.Segment{segment}
	drops := make([]*roaring.Bitmap, len(segsToMerge))
	if _, _, err = zapPlugin.Merge(segsToMerge, drops, mergedPath, nil, nil); err != nil {
		t.Fatal(err)
	}
	merged, err := zapPlugin.Open(mergedPath)
	if err != nil {
		t.Fatalf("error opening merged segment: %v", err)
	}
	defer func() {
		if cerr := merged.Close(); cerr != nil {
			t.Fatalf("error closing merged segment: %v", cerr)
		}
	}()

	dict, err := merged.Dictionary("domain")
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]uint64{"aaa": 1, "bbb": 3}

	// One iterator across every term, which is what index.FieldDict does.
	got := map[string]uint64{}
	itr := dict.AutomatonIterator(nil, nil, nil)
	for {
		entry, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if entry == nil {
			break
		}
		got[entry.Term] = entry.Count
	}
	for term, wantCount := range want {
		if got[term] != wantCount {
			t.Errorf("shared iterator: term %q reported count %d, want %d",
				term, got[term], wantCount)
		}
	}

	// A fresh iterator per term must agree with the shared one.
	for term, wantCount := range want {
		single := dict.AutomatonIterator(nil, []byte(term), []byte(term+"\x00"))
		var found bool
		for {
			entry, err := single.Next()
			if err != nil {
				t.Fatal(err)
			}
			if entry == nil {
				break
			}
			if entry.Term != term {
				continue
			}
			found = true
			if entry.Count != wantCount {
				t.Errorf("fresh iterator: term %q reported count %d, want %d",
					term, entry.Count, wantCount)
			}
		}
		if !found {
			t.Errorf("fresh iterator: term %q not found", term)
		}
	}

	if t.Failed() {
		t.Log(fmt.Sprintf("counts from the shared iterator: %v", got))
	}
}
