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
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

func TestBuild(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")

	sb, _, err := buildTestSegment()
	if err != nil {
		t.Fatal(err)
	}
	err = PersistSegmentBase(sb, "/tmp/scorch.zap")
	if err != nil {
		t.Fatal(err)
	}
}

func buildTestSegment() (*SegmentBase, uint64, error) {
	doc := newStubDocument("a", []*stubField{
		newStubFieldSplitString("_id", nil, "a", true, false, false),
		newStubFieldSplitString("name", nil, "wow", true, false, true),
		newStubFieldSplitString("desc", nil, "some thing", true, false, true),
		newStubFieldSplitString("tag", []uint64{0}, "cold", true, false, true),
		newStubFieldSplitString("tag", []uint64{1}, "dark", true, false, true),
	}, "_all")

	// forge analyzed docs
	results := []index.Document{
		doc,
	}

	seg, size, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	return seg.(*SegmentBase), size, err
}

func buildTestSegmentMulti() (*SegmentBase, uint64, error) {
	results := buildTestAnalysisResultsMulti()

	seg, size, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	return seg.(*SegmentBase), size, err
}

func buildTestSegmentMultiWithChunkFactor(chunkFactor uint32) (*SegmentBase, uint64, error) {
	results := buildTestAnalysisResultsMulti()

	seg, size, err := zapPlugin.newWithChunkMode(results, chunkFactor, nil)
	return seg.(*SegmentBase), size, err
}

func buildTestSegmentMultiWithDifferentFields(includeDocA, includeDocB bool) (*SegmentBase, uint64, error) {
	results := buildTestAnalysisResultsMultiWithDifferentFields(includeDocA, includeDocB)

	seg, size, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
	return seg.(*SegmentBase), size, err
}

func buildTestAnalysisResultsMulti() []index.Document {
	doc := newStubDocument("a", []*stubField{
		newStubFieldSplitString("_id", nil, "a", true, false, false),
		newStubFieldSplitString("name", nil, "wow", true, false, true),
		newStubFieldSplitString("desc", nil, "some thing", true, false, true),
		newStubFieldSplitString("tag", []uint64{0}, "cold", true, false, true),
		newStubFieldSplitString("tag", []uint64{1}, "dark", true, false, true),
	}, "_all")

	doc2 := newStubDocument("b", []*stubField{
		newStubFieldSplitString("_id", nil, "b", true, false, false),
		newStubFieldSplitString("name", nil, "who", true, false, true),
		newStubFieldSplitString("desc", nil, "some thing", true, false, true),
		newStubFieldSplitString("tag", []uint64{0}, "cold", true, false, true),
		newStubFieldSplitString("tag", []uint64{1}, "dark", true, false, true),
	}, "_all")

	// forge analyzed docs
	results := []index.Document{
		doc,
		doc2,
	}

	return results
}

func buildTestAnalysisResultsMultiWithDifferentFields(includeDocA, includeDocB bool) []index.Document {
	results := []index.Document{}

	if includeDocA {
		doc := newStubDocument("a", []*stubField{
			newStubFieldSplitString("_id", nil, "a", true, false, false),
			newStubFieldSplitString("name", nil, "ABC", true, false, true),
			newStubFieldSplitString("dept", nil, "ABC dept", true, false, true),
			newStubFieldSplitString("manages.id", nil, "XYZ", true, false, true),
			newStubFieldSplitString("manages.count", nil, "1", true, false, true),
		}, "_all")

		result := doc

		results = append(results, result)
	}

	if includeDocB {
		doc := newStubDocument("b", []*stubField{
			newStubFieldSplitString("_id", nil, "b", true, false, false),
			newStubFieldSplitString("name", nil, "XYZ", true, false, true),
			newStubFieldSplitString("dept", nil, "ABC dept", true, false, true),
			newStubFieldSplitString("reportsTo.id", nil, "ABC", true, false, true),
		}, "_all")

		result := doc

		results = append(results, result)
	}

	return results
}

func buildTestSegmentWithDefaultFieldMapping(chunkFactor uint32) (
	*SegmentBase, []string, error) {
	doc := newStubDocument("a", []*stubField{
		newStubFieldSplitString("_id", nil, "a", true, false, false),
		newStubFieldSplitString("name", nil, "wow", true, true, true),
		newStubFieldSplitString("desc", nil, "some thing", true, true, true),
		newStubFieldSplitString("tag", []uint64{0}, "cold", true, true, true),
	}, "_all")

	var fields []string
	fields = append(fields, "_id")
	fields = append(fields, "name")
	fields = append(fields, "desc")
	fields = append(fields, "tag")

	// forge analyzed docs
	results := []index.Document{
		doc,
	}

	sb, _, err := zapPlugin.newWithChunkMode(results, chunkFactor, nil)

	return sb.(*SegmentBase), fields, err
}
