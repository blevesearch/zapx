//  Copyright (c) 2017 Couchbase, Inc.
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
	"os"
	"reflect"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/vellum/levenshtein"
)

func buildTestSegmentForDict() (*SegmentBase, uint64, error) {
	doc := newStubDocument("a", []*stubField{
		newStubFieldSplitString("_id", nil, "a", true, false, false),
		newStubFieldSplitString("desc", nil, "apple ball cat dog egg fish bat", true, false, true),
	}, "_all")

	// forge analyzed docs
	results := []index.Document{
		doc,
	}

	seg, size, err := zapPlugin.newWithChunkMode(results, 1024)
	return seg.(*SegmentBase), size, err
}

func TestDictionary(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")

	testSeg, _, _ := buildTestSegmentForDict()
	err := PersistSegmentBase(testSeg, "/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	segment, err := zapPlugin.Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	dict, err := segment.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	// test basic full iterator
	expected := []string{"apple", "ball", "bat", "cat", "dog", "egg", "fish"}
	var got []string
	itr := dict.AutomatonIterator(nil, nil, nil)
	next, err := itr.Next()
	for next != nil && err == nil {
		got = append(got, next.Term)
		next, err = itr.Next()
	}
	if err != nil {
		t.Fatalf("dict itr error: %v", err)
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %v, got: %v", expected, got)
	}

	// test prefix iterator
	expected = []string{"ball", "bat"}
	got = got[:0]
	itr = dict.AutomatonIterator(nil, []byte("b"), []byte("c"))
	next, err = itr.Next()
	for next != nil && err == nil {
		got = append(got, next.Term)
		next, err = itr.Next()
	}
	if err != nil {
		t.Fatalf("dict itr error: %v", err)
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %v, got: %v", expected, got)
	}

	// test range iterator
	expected = []string{"cat", "dog", "egg"}
	got = got[:0]
	itr = dict.AutomatonIterator(nil, []byte("cat"), append([]byte("egg"), 0))
	next, err = itr.Next()
	for next != nil && err == nil {
		got = append(got, next.Term)
		next, err = itr.Next()
	}
	if err != nil {
		t.Fatalf("dict itr error: %v", err)
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %v, got: %v", expected, got)
	}
}

func TestDictionaryError(t *testing.T) {
	hash := make(map[uint8]levenshtein.LevenshteinAutomatonBuilder, 4)
	for i := 1; i <= 3; i++ {
		lb, err := levenshtein.NewLevenshteinAutomatonBuilder(uint8(i), false)
		if err != nil {
			t.Errorf("NewLevenshteinAutomatonBuilder(%d, false) failed, err: %v", i, err)
		}
		hash[uint8(i)] = *lb
	}

	_ = os.RemoveAll("/tmp/scorch.zap")

	testSeg, _, _ := buildTestSegmentForDict()
	err := PersistSegmentBase(testSeg, "/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	segment, err := zapPlugin.Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	dict, err := segment.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	lb := hash[uint8(2)]
	a, err := lb.BuildDfa("summer", 2)
	if err != nil {
		t.Fatal(err)
	}
	itr := dict.AutomatonIterator(a, nil, nil)
	if itr == nil {
		t.Fatalf("got nil itr")
	}
	nxt, err := itr.Next()
	if nxt != nil {
		t.Fatalf("expected nil next")
	}
	if err != nil {
		t.Fatalf("expected nil error from iterator, got: %v", err)
	}

	lb = hash[uint8(1)]
	a, err = lb.BuildDfa("cat", 1) // cat & bat
	if err != nil {
		t.Fatal(err)
	}
	itr = dict.AutomatonIterator(a, nil, nil)
	if itr == nil {
		t.Fatalf("got nil itr")
	}
	for i := 0; i < 2; i++ {
		nxt, err = itr.Next()
		if nxt == nil || err != nil {
			t.Fatalf("expected non-nil next and nil err, got: %v, %v", nxt, err)
		}
	}
	nxt, err = itr.Next()
	if nxt != nil || err != nil {
		t.Fatalf("expected nil next and nil err, got: %v, %v", nxt, err)
	}

	lb = hash[uint8(2)]
	a, err = lb.BuildDfa("cat", 2) // cat & bat
	if err != nil {
		t.Fatal(err)
	}
	itr = dict.AutomatonIterator(a, nil, nil)
	if itr == nil {
		t.Fatalf("got nil itr")
	}
	for i := 0; i < 2; i++ {
		nxt, err = itr.Next()
		if nxt == nil || err != nil {
			t.Fatalf("expected non-nil next and nil err, got: %v, %v", nxt, err)
		}
	}
	nxt, err = itr.Next()
	if nxt != nil || err != nil {
		t.Fatalf("expected nil next and nil err, got: %v, %v", nxt, err)
	}

	lb = hash[uint8(3)]
	a, err = lb.BuildDfa("cat", 3)
	if err != nil {
		t.Fatal(err)
	}
	itr = dict.AutomatonIterator(a, nil, nil)
	if itr == nil {
		t.Fatalf("got nil itr")
	}
	for i := 0; i < 5; i++ {
		nxt, err = itr.Next()
		if nxt == nil || err != nil {
			t.Fatalf("expected non-nil next and nil err, got: %v, %v", nxt, err)
		}
	}
	nxt, err = itr.Next()
	if nxt != nil || err != nil {
		t.Fatalf("expected nil next and nil err, got: %v, %v", nxt, err)
	}
}

func TestDictionaryBug1156(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")

	testSeg, _, _ := buildTestSegmentForDict()
	err := PersistSegmentBase(testSeg, "/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	segment, err := zapPlugin.Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", err)
		}
	}()

	dict, err := segment.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	// test range iterator
	expected := []string{"cat", "dog", "egg", "fish"}
	var got []string
	itr := dict.AutomatonIterator(nil, []byte("cat"), nil)
	next, err := itr.Next()
	for next != nil && err == nil {
		got = append(got, next.Term)
		next, err = itr.Next()
	}
	if err != nil {
		t.Fatalf("dict itr error: %v", err)
	}

	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %v, got: %v", expected, got)
	}
}
