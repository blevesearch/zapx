//  Copyright (c) 2024 Couchbase, Inc.
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
	"sort"
	"strconv"
	"testing"

	"errors"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func createEquivalentSynonymMap(input []string, resultMap map[string][]string) map[string][]string {
	if resultMap == nil {
		resultMap = make(map[string][]string)
	}
	for _, elem := range input {
		for _, otherElem := range input {
			if elem != otherElem {
				resultMap[elem] = append(resultMap[elem], otherElem)
			}
		}
	}
	return resultMap
}

func buildTestSynonymDocument(id string, collection string, terms []string, synonyms []string) index.Document {
	synDefs := make(map[string][]string)
	if terms == nil {
		synDefs = createEquivalentSynonymMap(synonyms, synDefs)
	} else {
		for _, term := range terms {
			synDefs[term] = synonyms
		}
	}
	synonymDefs := make([]index.SynonymDefinition, 0, len(synDefs))
	for term, synonyms := range synDefs {
		synonymDefs = append(synonymDefs, newStubSynonymDefinition(term, synonyms))
	}
	synDoc := newStubSynonymDocument(id, newStubSynonymField(collection, "standard", synonymDefs))
	synDoc.AddIDField()
	return synDoc
}

func buildTestSegmentForThesaurus(results []index.Document) (*SegmentBase, error) {
	seg, _, err := zapPlugin.newWithChunkMode(results, 1024)
	return seg.(*SegmentBase), err
}

func extractSynonymsForTermFromThesaurus(thes segment.Thesaurus, term string) ([]string, error) {
	list, err := thes.SynonymsList([]byte(term), nil, nil)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, errors.New("expected synonyms list")
	}
	listItr := list.Iterator(nil)
	if listItr == nil {
		return nil, errors.New("expected non-nil iterator")
	}
	var synonyms []string
	for {
		next, err := listItr.Next()
		if err != nil {
			return nil, err
		}
		if next == nil {
			break
		}
		synonyms = append(synonyms, next.Term())
	}
	return synonyms, nil
}

func testSegmentSynonymAccuracy(collectionName string, testSynonymMap map[string][]string, seg segment.Segment) error {
	dict, err := seg.Dictionary(collectionName)
	if err != nil {
		return err
	}
	if dict != emptyDictionary {
		pl, err := dict.PostingsList([]byte{'a'}, nil, nil)
		if err != nil {
			return err
		}
		if pl != emptyPostingsList {
			return errors.New("expected empty postings list")
		}
	}
	synSeg, ok := seg.(segment.SynonymSegment)
	if !ok {
		return errors.New("expected synonym segment")
	}
	thes, err := synSeg.Thesaurus(collectionName)
	if err != nil {
		return err
	}
	if thes == emptyThesaurus {
		return errors.New("expected a thesaurus")
	}
	for term, expectedSynonyms := range testSynonymMap {
		synonyms, err := extractSynonymsForTermFromThesaurus(thes, term)
		if err != nil {
			return err
		}
		if len(synonyms) != len(expectedSynonyms) {
			return errors.New("unexpected number of synonyms")
		}
		sort.Strings(synonyms)
		sort.Strings(expectedSynonyms)
		for i, synonym := range synonyms {
			if synonym != expectedSynonyms[i] {
				return errors.New("unexpected synonym")
			}
		}
	}
	return nil
}

type testSynonymDefinition struct {
	terms    []string
	synonyms []string
}

func createExpectedSynonymMap(input []testSynonymDefinition) map[string][]string {
	rv := make(map[string][]string)
	for _, testSynonymDefinition := range input {
		if testSynonymDefinition.terms == nil {
			rv = createEquivalentSynonymMap(testSynonymDefinition.synonyms, rv)
		} else {
			for _, term := range testSynonymDefinition.terms {
				rv[term] = append(rv[term], testSynonymDefinition.synonyms...)
			}
		}
	}
	return rv
}

func TestThesaurusSingleSegment(t *testing.T) {
	err := os.RemoveAll("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error removing directory: %v", err)
	}
	collectionName := "coll1"
	testSynonymDefinitions := []testSynonymDefinition{
		{
			terms: nil,
			synonyms: []string{
				"adeptness",
				"aptitude",
				"facility",
				"faculty",
				"capacity",
				"power",
				"knack",
				"proficiency",
				"ability",
			},
		},
		{
			terms: []string{"afflict"},
			synonyms: []string{
				"affect",
				"bother",
				"distress",
				"oppress",
				"trouble",
				"torment",
			},
		},
		{
			terms: []string{"capacity"},
			synonyms: []string{
				"volume",
				"content",
				"size",
				"dimensions",
				"measure",
			},
		},
	}
	var testSynonymDocuments []index.Document
	for i, testSynonymDefinition := range testSynonymDefinitions {
		testSynonymDocuments = append(testSynonymDocuments, buildTestSynonymDocument(
			strconv.Itoa(i),
			collectionName,
			testSynonymDefinition.terms,
			testSynonymDefinition.synonyms,
		))
	}
	sb, err := buildTestSegmentForThesaurus(testSynonymDocuments)
	if err != nil {
		t.Fatalf("error building test seg: %v", err)
	}
	err = PersistSegmentBase(sb, "/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error persisting seg: %v", err)
	}
	seg, err := zapPlugin.Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening seg: %v", err)
	}
	defer func() {
		cerr := seg.Close()
		if cerr != nil {
			t.Fatalf("error closing seg: %v", err)
		}
	}()
	err = testSegmentSynonymAccuracy(collectionName, createExpectedSynonymMap(testSynonymDefinitions), seg)
	if err != nil {
		t.Fatalf("error testing segment: %v", err)
	}
}
