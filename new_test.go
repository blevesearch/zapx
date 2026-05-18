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
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

// newStubFieldWithOptions builds a minimal stubField with a caller-controlled
// options bitmask. Tests for interim.convert use this to simulate mappings
// that emit the same field name multiple times with different option sets,
// which is what triggered https://github.com/blevesearch/bleve/issues/2332.
func newStubFieldWithOptions(name, value string, opts index.FieldIndexingOptions) *stubField {
	return &stubField{
		name:          name,
		value:         []byte(value),
		encodedType:   't',
		options:       opts,
		analyzedFreqs: make(index.TokenFrequencies),
	}
}

// TestConvertFieldsOptionsAccumulate covers the fix for
// https://github.com/blevesearch/bleve/issues/2332: when a document maps the
// same field name multiple times with different option sets (e.g. one entry
// indexed-and-stored for standard text, another indexed-only for an ngram
// analyzer, another indexed-only for a keyword analyzer), interim.convert
// must retain the union of all options. Before the fix it used `=` and the
// last visited field clobbered prior options, so StoreField could silently
// disappear depending on iteration order.
func TestConvertFieldsOptionsAccumulate(t *testing.T) {
	doc := &stubDocument{
		id: "a",
		fields: []*stubField{
			newStubFieldWithOptions("_id", "a", index.IndexField|index.StoreField),
			// standard text: indexed + stored + term vectors
			newStubFieldWithOptions("title", "hello world",
				index.IndexField|index.StoreField|index.IncludeTermVectors),
			// ngram: indexed only
			newStubFieldWithOptions("title", "hello world",
				index.IndexField|index.IncludeTermVectors),
			// keyword: indexed only, visited last — pre-fix this overwrote the
			// stored bit set by the first entry
			newStubFieldWithOptions("title", "hello world",
				index.IndexField),
		},
	}

	seg, _, err := zapPlugin.newWithChunkMode([]index.Document{doc}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	sb := seg.(*SegmentBase)

	want := index.IndexField | index.StoreField | index.IncludeTermVectors
	got := sb.fieldsOptions["title"]
	if got != want {
		t.Fatalf("title options got %v, want %v: StoreField from the first field "+
			"must survive even though the last entry omitted it", got, want)
	}
	if !got.IsStored() {
		t.Errorf("title must remain stored after multiple field visits, got %v", got)
	}
}

// TestConvertFieldsOptionsOrderIndependent verifies that the field options
// recorded by convert() are invariant under reordering of fields with the
// same name. OR-ing is commutative; the pre-fix `=` was not.
func TestConvertFieldsOptionsOrderIndependent(t *testing.T) {
	stored := newStubFieldWithOptions("title", "v", index.IndexField|index.StoreField)
	dv := newStubFieldWithOptions("title", "v", index.IndexField|index.DocValues)
	tv := newStubFieldWithOptions("title", "v", index.IndexField|index.IncludeTermVectors)

	docA := &stubDocument{
		id: "a",
		fields: []*stubField{
			newStubFieldWithOptions("_id", "a", index.IndexField|index.StoreField),
			stored, dv, tv,
		},
	}
	docB := &stubDocument{
		id: "b",
		fields: []*stubField{
			newStubFieldWithOptions("_id", "b", index.IndexField|index.StoreField),
			tv, dv, stored,
		},
	}

	segA, _, err := zapPlugin.newWithChunkMode([]index.Document{docA}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	segB, _, err := zapPlugin.newWithChunkMode([]index.Document{docB}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}

	gotA := segA.(*SegmentBase).fieldsOptions["title"]
	gotB := segB.(*SegmentBase).fieldsOptions["title"]
	if gotA != gotB {
		t.Errorf("field options must not depend on visit order: A=%v B=%v", gotA, gotB)
	}
	want := index.IndexField | index.StoreField | index.DocValues | index.IncludeTermVectors
	if gotA != want {
		t.Errorf("title options got %v, want %v (union of all option sets)", gotA, want)
	}
}

// TestConvertFieldsOptionsAcrossDocuments verifies the OR-merge also holds
// when the duplicate field name spans multiple documents in a single batch.
// FieldsOptions is shared across all docs in one convert() call, so the
// second doc's entry for "title" must not erase the first doc's StoreField.
func TestConvertFieldsOptionsAcrossDocuments(t *testing.T) {
	doc1 := &stubDocument{
		id: "a",
		fields: []*stubField{
			newStubFieldWithOptions("_id", "a", index.IndexField|index.StoreField),
			newStubFieldWithOptions("title", "hello",
				index.IndexField|index.StoreField),
		},
	}
	doc2 := &stubDocument{
		id: "b",
		fields: []*stubField{
			newStubFieldWithOptions("_id", "b", index.IndexField|index.StoreField),
			newStubFieldWithOptions("title", "world", index.IndexField),
		},
	}

	seg, _, err := zapPlugin.newWithChunkMode(
		[]index.Document{doc1, doc2}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	sb := seg.(*SegmentBase)

	want := index.IndexField | index.StoreField
	if got := sb.fieldsOptions["title"]; got != want {
		t.Errorf("title options got %v, want %v", got, want)
	}
}

// TestConvertCompositeFieldsOptionsAccumulate exercises the composite-field
// arm of convert(), which has its own copy of the same `|=` logic. A document
// with multiple composite entries for the same name must end up with the
// union of their options.
func TestConvertCompositeFieldsOptionsAccumulate(t *testing.T) {
	// cf1 carries the additional IncludeTermVectors bit; cf2 is indexed only.
	// Visit order matters here: cf2 comes last, so a plain `=` would clobber
	// the TermVectors bit set by cf1.
	cf1 := &stubField{
		name:          "_all",
		encodedType:   'c',
		options:       index.IndexField | index.IncludeTermVectors,
		analyzedFreqs: make(index.TokenFrequencies),
	}
	cf2 := &stubField{
		name:          "_all",
		encodedType:   'c',
		options:       index.IndexField,
		analyzedFreqs: make(index.TokenFrequencies),
	}

	doc := &stubDocument{
		id: "a",
		fields: []*stubField{
			newStubFieldWithOptions("_id", "a", index.IndexField|index.StoreField),
		},
		composite: []*stubField{cf1, cf2},
	}

	seg, _, err := zapPlugin.newWithChunkMode([]index.Document{doc}, DefaultChunkMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	sb := seg.(*SegmentBase)

	want := index.IndexField | index.IncludeTermVectors
	if got := sb.fieldsOptions["_all"]; got != want {
		t.Errorf("_all composite options got %v, want %v", got, want)
	}
}
