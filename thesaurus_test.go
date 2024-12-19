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

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func buildTestSynonymDocument(id string, synonymSource string, terms []string, synonyms []string) index.Document {
	// Create the synonym document using stubs.
	stubAnalyzer := "standard"
	// Create the synonym field.
	synField := newStubSynonymField(synonymSource, stubAnalyzer, terms, synonyms)
	// Analyze the synonyms.
	synField.Analyze()
	// Create the synonym document.
	synDoc := newStubSynonymDocument(id, synField)
	synDoc.AddIDField()
	return synDoc
}

func buildTestSegmentForThesaurus(results []index.Document) (*SegmentBase, error) {
	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode)
	return seg.(*SegmentBase), err
}

func extractSynonymsForTermFromThesaurus(thes segment.Thesaurus, term string, except *roaring.Bitmap) ([]string, error) {
	list, err := thes.SynonymsList([]byte(term), except, nil)
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

func checkWithDeletes(except *roaring.Bitmap, collectionName string, testSynonymMap map[string][]string, seg segment.Segment) error {
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
	synSeg, ok := seg.(segment.ThesaurusSegment)
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
		synonyms, err := extractSynonymsForTermFromThesaurus(thes, term, except)
		if err != nil {
			return err
		}
		if len(synonyms) != len(expectedSynonyms) {
			return errors.New("unexpected number of synonyms, expected: " +
				strconv.Itoa(len(expectedSynonyms)) + " got: " +
				strconv.Itoa(len(synonyms)) + " for term: " + term)
		}
		sort.Strings(synonyms)
		sort.Strings(expectedSynonyms)
		for i, synonym := range synonyms {
			if synonym != expectedSynonyms[i] {
				return errors.New("unexpected synonym" + synonym + " for term: " + term)
			}
		}
	}
	return nil
}

func testSegmentSynonymAccuracy(testSynonymDefinitions []testSynonymDefinition, seg segment.Segment) error {
	collSynMap := make(map[string][]testSynonymDefinition)
	for _, testSynonymDefinition := range testSynonymDefinitions {
		collSynMap[testSynonymDefinition.collectionName] = append(collSynMap[testSynonymDefinition.collectionName], testSynonymDefinition)
	}
	for collectionName, testSynonymMap := range collSynMap {
		expectedSynonymMap := createExpectedSynonymMap(testSynonymMap)
		err := checkWithDeletes(nil, collectionName, expectedSynonymMap, seg)
		if err != nil {
			return err
		}
		for i := 0; i < len(testSynonymMap); i++ {
			except := roaring.New()
			except.Add(uint32(i))
			modifiedSynonymMap := append([]testSynonymDefinition{}, testSynonymMap[:i]...)
			modifiedSynonymMap = append(modifiedSynonymMap, testSynonymMap[i+1:]...)
			expectedSynonymMap = createExpectedSynonymMap(modifiedSynonymMap)
			err = checkWithDeletes(except, collectionName, expectedSynonymMap, seg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type testSynonymDefinition struct {
	collectionName string
	terms          []string
	synonyms       []string
}

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

func buildSegment(testSynonymDefinitions []testSynonymDefinition) (segment.Segment, string, error) {
	var testSynonymDocuments []index.Document
	for i, testSynonymDefinition := range testSynonymDefinitions {
		testSynonymDocuments = append(testSynonymDocuments, buildTestSynonymDocument(
			strconv.Itoa(i),
			testSynonymDefinition.collectionName,
			testSynonymDefinition.terms,
			testSynonymDefinition.synonyms,
		))
	}
	sb, err := buildTestSegmentForThesaurus(testSynonymDocuments)
	if err != nil {
		return nil, "", err
	}
	tmpDir, err := os.MkdirTemp("", "zap-")
	if err != nil {
		return nil, "", err
	}

	err = os.RemoveAll(tmpDir)
	if err != nil {
		return nil, "", err
	}
	err = PersistSegmentBase(sb, tmpDir)
	if err != nil {
		return nil, "", err
	}
	seg, err := zapPlugin.Open(tmpDir)
	if err != nil {
		return nil, "", err
	}
	err = testSegmentSynonymAccuracy(testSynonymDefinitions, seg)
	if err != nil {
		return nil, "", err
	}
	return seg, tmpDir, nil
}

func mergeSegments(segs []segment.Segment, drops []*roaring.Bitmap, testSynonymDefinitions []testSynonymDefinition) (string, error) {
	tmpDir, err := os.MkdirTemp("", "mergedzap-")
	if err != nil {
		return "", err
	}
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return "", err
	}
	// Test Merging of multiple segments
	_, _, err = zapPlugin.Merge(segs, drops, tmpDir, nil, nil)
	if err != nil {
		return "", err
	}

	seg, err := zapPlugin.Open(tmpDir)
	if err != nil {
		return "", err
	}
	err = testSegmentSynonymAccuracy(testSynonymDefinitions, seg)
	if err != nil {
		return "", err
	}
	cerr := seg.Close()
	if cerr != nil {
		return "", err
	}
	return tmpDir, nil
}

func TestSynonymSegment(t *testing.T) {
	synonymSourceOneName := "coll0"
	synonymSourceTwoName := "coll1"
	testSynonymDefinitions := []testSynonymDefinition{
		{
			collectionName: synonymSourceOneName,
			terms:          nil,
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
			collectionName: synonymSourceOneName,
			terms:          []string{"afflict"},
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
			collectionName: synonymSourceOneName,
			terms:          []string{"capacity"},
			synonyms: []string{
				"volume",
				"content",
				"size",
				"dimensions",
				"measure",
			},
		},
		{
			collectionName: synonymSourceTwoName,
			synonyms: []string{
				"absolutely",
				"unqualifiedly",
				"unconditionally",
				"unreservedly",
				"unexceptionally",
				"unequivocally",
			},
		},
		{
			collectionName: synonymSourceTwoName,
			terms:          []string{"abrupt"},
			synonyms: []string{
				"sudden",
				"hasty",
				"quick",
				"precipitate",
				"snappy",
			},
		},
	}
	// single segment test
	seg1, dir, err := buildSegment(testSynonymDefinitions)
	if err != nil {
		t.Fatalf("error building segment: %v", err)
	}
	defer func() {
		cerr := seg1.Close()
		if cerr != nil {
			t.Fatalf("error closing seg: %v", err)
		}
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatalf("error removing dir: %v", err)
		}
	}()

	// multiple segment test
	numSegs := 3
	numDocs := 5
	segData := make([][]testSynonymDefinition, numSegs)

	segData[0] = make([]testSynonymDefinition, 0)
	segData[0] = testSynonymDefinitions[:2] // 2 docs

	segData[1] = make([]testSynonymDefinition, 0)
	segData[1] = testSynonymDefinitions[2:4] // 2 docs

	segData[2] = make([]testSynonymDefinition, 0)
	segData[2] = testSynonymDefinitions[4:] // 1 doc

	segs := make([]segment.Segment, numSegs)
	dirs := make([]string, numSegs)
	for i, data := range segData {
		seg, dir, err := buildSegment(data)
		if err != nil {
			t.Fatalf("error building segment: %v", err)
		}
		segs[i] = seg
		dirs[i] = dir
	}
	drops := make([]*roaring.Bitmap, numDocs)
	for i := 0; i < numDocs; i++ {
		drops[i] = roaring.New()
	}
	mergeDir, err := mergeSegments(segs, drops, testSynonymDefinitions)
	if err != nil {
		t.Fatalf("error merging segments: %v", err)
	}
	for i := 0; i < numSegs; i++ {
		cerr := segs[i].Close()
		if cerr != nil {
			t.Fatalf("error closing seg: %v", err)
		}
		err := os.RemoveAll(dirs[i])
		if err != nil {
			t.Fatalf("error removing dir: %v", err)
		}
		err = os.RemoveAll(mergeDir)
		if err != nil {
			t.Fatalf("error removing dir: %v", err)
		}
	}
}
