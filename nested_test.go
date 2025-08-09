//  Copyright (c) 2025 Couchbase, Inc.
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
	"reflect"
	"slices"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type docAnalyzer struct{}

func (da *docAnalyzer) Analyze(doc index.Document) error {
	doc.VisitFields(func(field index.Field) {
		field.Analyze()
	})
	return nil
}

func docAToStub() index.Document {
	_ =
		`
			{
				"title": "Tech Insights",
				"posts": [
					{
						"title": "AI Trends",
						"published_date": "2025-04-22",
						"comments": [
							{
								"author": "Jane",
								"text": "Very informative!",
								"likes": 1
							},
							{
								"author": "Tom",
								"text": "Needs more detail.",
								"likes": 3
							}
						]
					},
					{
						"title": "Quantum Computing",
						"published_date": "2024-11-15",
						"comments": [
							{
								"author": "Jane",
								"text": "Mind-blowing!",
								"likes": 5
							}
						]
					}
				]
			}
  		`
	analyzer := &docAnalyzer{}
	blogTitleField := newStubFieldSplitString("title", nil, "Tech Insights", true, true, true)
	posts0TitleField := newStubFieldSplitString("title", nil, "AI Trends", true, true, true)
	posts0PublishedDateField := newStubFieldSplitString("published_date", nil, "2025-04-22", true, true, true)
	comments00FieldAuthor := newStubFieldSplitString("author", nil, "Jane", true, true, true)
	comments00FieldText := newStubFieldSplitString("text", nil, "Very informative!", true, true, true)
	comments00FieldLikes := newStubFieldSplitString("likes", nil, "1", true, true, true)
	comments00Doc := newStubNestedDocument("A0C0", []index.Field{
		comments00FieldAuthor,
		comments00FieldText,
		comments00FieldLikes,
	})

	comments01FieldAuthor := newStubFieldSplitString("author", nil, "Tom", true, true, true)
	comments01FieldText := newStubFieldSplitString("text", nil, "Needs more detail.", true, true, true)
	comments01FieldLikes := newStubFieldSplitString("likes", nil, "3", true, true, true)
	comments01Doc := newStubNestedDocument("A0C1", []index.Field{
		comments01FieldAuthor,
		comments01FieldText,
		comments01FieldLikes,
	})
	comments0NestField := newStubNestedField("comments", []index.NestedDocument{comments00Doc, comments01Doc}, analyzer)

	posts0Doc := newStubNestedDocument("A0", []index.Field{
		posts0TitleField,
		posts0PublishedDateField,
		comments0NestField,
	})

	posts1TitleField := newStubFieldSplitString("title", nil, "Quantum Computing", true, true, true)
	posts1PublishedDateField := newStubFieldSplitString("published_date", nil, "2024-11-15", true, true, true)
	comments10FieldAuthor := newStubFieldSplitString("author", nil, "Jane", true, true, true)
	comments10FieldText := newStubFieldSplitString("text", nil, "Mind-blowing!", true, true, true)
	comments10FieldLikes := newStubFieldSplitString("likes", nil, "5", true, true, true)
	comments10Doc := newStubNestedDocument("A1C0", []index.Field{
		comments10FieldAuthor,
		comments10FieldText,
		comments10FieldLikes,
	})
	comments1NestField := newStubNestedField("comments", []index.NestedDocument{comments10Doc}, analyzer)

	posts1Doc := newStubNestedDocument("A1", []index.Field{
		posts1TitleField,
		posts1PublishedDateField,
		comments1NestField,
	})

	postsNestedField := newStubNestedField("posts", []index.NestedDocument{posts0Doc, posts1Doc}, analyzer)

	docA := newStubNestedDocument("A", []index.Field{
		blogTitleField,
		postsNestedField,
	})

	return docA
}

func docBToStub() index.Document {
	_ =
		`
			{
				"title": "Science Digest",
				"posts": [
					{
						"title": "CRISPR And Gene Editing",
						"published_date": "2025-06-10",
						"comments": [
							{
								"author": "Alex",
								"text": "Fascinating potential.",
								"likes": 2
							}
						]
					}
				]
			}
		`
	analyzer := &docAnalyzer{}

	blogTitleField := newStubFieldSplitString("title", nil, "Science Digest", true, true, true)

	posts0TitleField := newStubFieldSplitString("title", nil, "CRISPR And Gene Editing", true, true, true)
	posts0PublishedDateField := newStubFieldSplitString("published_date", nil, "2025-06-10", true, true, true)

	comments00FieldAuthor := newStubFieldSplitString("author", nil, "Alex", true, true, true)
	comments00FieldText := newStubFieldSplitString("text", nil, "Fascinating potential.", true, true, true)
	comments00FieldLikes := newStubFieldSplitString("likes", nil, "2", true, true, true)

	comments00Doc := newStubNestedDocument("B0C0", []index.Field{
		comments00FieldAuthor,
		comments00FieldText,
		comments00FieldLikes,
	})

	comments0NestField := newStubNestedField("comments", []index.NestedDocument{comments00Doc}, analyzer)

	posts0Doc := newStubNestedDocument("B0", []index.Field{
		posts0TitleField,
		posts0PublishedDateField,
		comments0NestField,
	})

	postsNestedField := newStubNestedField("posts", []index.NestedDocument{posts0Doc}, analyzer)

	docB := newStubNestedDocument("B", []index.Field{
		blogTitleField,
		postsNestedField,
	})

	return docB
}

func docCToStub() index.Document {
	_ =
		`
			{
				"title": "Future Scope",
				"posts": [
					{
						"title": "SpaceX Mars Plans",
						"published_date": "2025-01-05",
						"comments": []
					},
					{
						"title": "Brain-Computer Interfaces",
						"published_date": "2024-12-12",
						"comments": [
							{
								"author": "Lina",
								"text": "A bit scary, honestly.",
								"likes": 0
							},
							{
								"author": "Ken",
								"text": "The Future is Now.",
								"likes": 4
							}
						]
					},
					{
						"title": "Fusion Energy Breakthroughs",
						"published_date": "2025-07-01",
						"comments": [
							{
								"author": "Sam",
								"text": "Finally!",
								"likes": 6
							}
						]
					}
				]
			}
		`

	analyzer := &docAnalyzer{}

	blogTitleField := newStubFieldSplitString("title", nil, "Future Scope", true, true, true)

	// Post 0: SpaceX Mars Plans (no comments)
	posts0TitleField := newStubFieldSplitString("title", nil, "SpaceX Mars Plans", true, true, true)
	posts0PublishedDateField := newStubFieldSplitString("published_date", nil, "2025-01-05", true, true, true)
	comments0NestField := newStubNestedField("comments", nil, analyzer)
	posts0Doc := newStubNestedDocument("C0", []index.Field{
		posts0TitleField,
		posts0PublishedDateField,
		comments0NestField,
	})

	// Post 1: Brain-Computer Interfaces
	posts1TitleField := newStubFieldSplitString("title", nil, "Brain-Computer Interfaces", true, true, true)
	posts1PublishedDateField := newStubFieldSplitString("published_date", nil, "2024-12-12", true, true, true)
	comments10FieldAuthor := newStubFieldSplitString("author", nil, "Lina", true, true, true)
	comments10FieldText := newStubFieldSplitString("text", nil, "A bit scary, honestly.", true, true, true)
	comments10FieldLikes := newStubFieldSplitString("likes", nil, "0", true, true, true)
	comments10Doc := newStubNestedDocument("C1C0", []index.Field{
		comments10FieldAuthor,
		comments10FieldText,
		comments10FieldLikes,
	})

	comments11FieldAuthor := newStubFieldSplitString("author", nil, "Ken", true, true, true)
	comments11FieldText := newStubFieldSplitString("text", nil, "The Future is Now.", true, true, true)
	comments11FieldLikes := newStubFieldSplitString("likes", nil, "4", true, true, true)
	comments11Doc := newStubNestedDocument("C1C1", []index.Field{
		comments11FieldAuthor,
		comments11FieldText,
		comments11FieldLikes,
	})

	comments1NestField := newStubNestedField("comments", []index.NestedDocument{comments10Doc, comments11Doc}, analyzer)

	posts1Doc := newStubNestedDocument("C1", []index.Field{
		posts1TitleField,
		posts1PublishedDateField,
		comments1NestField,
	})

	// Post 2: Fusion Energy Breakthroughs
	posts2TitleField := newStubFieldSplitString("title", nil, "Fusion Energy Breakthroughs", true, true, true)
	posts2PublishedDateField := newStubFieldSplitString("published_date", nil, "2025-07-01", true, true, true)
	comments20FieldAuthor := newStubFieldSplitString("author", nil, "Sam", true, true, true)
	comments20FieldText := newStubFieldSplitString("text", nil, "Finally!", true, true, true)
	comments20FieldLikes := newStubFieldSplitString("likes", nil, "6", true, true, true)
	comments20Doc := newStubNestedDocument("C2C0", []index.Field{
		comments20FieldAuthor,
		comments20FieldText,
		comments20FieldLikes,
	})
	comments2NestField := newStubNestedField("comments", []index.NestedDocument{comments20Doc}, analyzer)

	posts2Doc := newStubNestedDocument("C2", []index.Field{
		posts2TitleField,
		posts2PublishedDateField,
		comments2NestField,
	})

	// Combine posts
	postsNestedField := newStubNestedField("posts", []index.NestedDocument{
		posts0Doc,
		posts1Doc,
		posts2Doc,
	}, analyzer)

	docC := newStubNestedDocument("C", []index.Field{
		blogTitleField,
		postsNestedField,
	})

	return docC
}

func persistSegment(results []index.Document) (segment.Segment, string, error) {
	tmpDir, err := os.MkdirTemp("", "zap-")
	if err != nil {
		return nil, "", err
	}
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return nil, "", err
	}
	seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode)
	if err != nil {
		return nil, "", err
	}
	sb, ok := seg.(*SegmentBase)
	if !ok {
		return nil, "", fmt.Errorf("failed to cast segment to SegmentBase")
	}
	err = PersistSegmentBase(sb, tmpDir)
	if err != nil {
		return nil, "", err
	}
	seg, err = zapPlugin.Open(tmpDir)
	if err != nil {
		return nil, "", err
	}
	return seg, tmpDir, nil
}

func buildNestedSegment() (segment.Segment, error) {
	docA := docAToStub()
	docB := docBToStub()
	docC := docCToStub()

	results := []index.Document{docA, docB, docC}

	seg, _, err := persistSegment(results)
	if err != nil {
		return nil, err
	}
	return seg, nil
}

func buildNestedSegmentMerged() (segment.Segment, error) {
	docA := docAToStub()
	resultsA := []index.Document{docA}

	segA, _, err := persistSegment(resultsA)
	if err != nil {
		return nil, err
	}

	docB := docBToStub()
	resultsB := []index.Document{docB}
	segB, _, err := persistSegment(resultsB)
	if err != nil {
		return nil, err
	}

	docC := docCToStub()
	resultsC := []index.Document{docC}
	segC, _, err := persistSegment(resultsC)
	if err != nil {
		return nil, err
	}

	tmpDir, err := os.MkdirTemp("", "mergedzap-")
	if err != nil {
		return nil, err
	}
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return nil, err
	}
	drops := make([]*roaring.Bitmap, 3)
	for i := 0; i < 3; i++ {
		drops[i] = roaring.New()
	}
	// Test Merging of multiple segments
	_, _, err = zapPlugin.Merge([]segment.Segment{segA, segB, segC}, drops, tmpDir, nil, nil)
	if err != nil {
		return nil, err
	}

	rv, err := zapPlugin.Open(tmpDir)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

type stubNestedState struct {
	paths          []string
	arrayPositions []int
}

func newStubNestedState() index.NestedState {
	return &stubNestedState{
		paths:          make([]string, 0),
		arrayPositions: make([]int, 0),
	}
}

// Append returns a new stubNestedState with the given path and array position added.
// It does NOT modify the original stubNestedState.
func (s *stubNestedState) Append(path string, pos int) index.NestedState {
	return &stubNestedState{
		paths:          append(slices.Clone(s.paths), path),
		arrayPositions: append(slices.Clone(s.arrayPositions), pos),
	}
}

func (s *stubNestedState) Empty() bool {
	return len(s.paths) == 0 && len(s.arrayPositions) == 0
}

func (s *stubNestedState) Clear() {
	s.paths = s.paths[:0]
	s.arrayPositions = s.arrayPositions[:0]
}

func (s *stubNestedState) Root() string {
	if len(s.paths) == 0 {
		return ""
	}
	return s.paths[0]
}

func (s *stubNestedState) Iterator() index.NestedIterator {
	return &nestedIterator{
		paths:         s.paths,
		arrayPosition: s.arrayPositions,
		index:         0,
	}
}

type nestedIterator struct {
	paths         []string
	arrayPosition []int
	index         int
}

func (ni *nestedIterator) HasNext() bool {
	return ni.index < len(ni.paths)
}

func (ni *nestedIterator) Next() (string, int, bool) {
	if ni.index >= len(ni.paths) {
		return "", 0, false
	}
	path := ni.paths[ni.index]
	arrayPos := ni.arrayPosition[ni.index]
	ni.index++
	return path, arrayPos, true
}

func (ni *nestedIterator) Reset() {
	ni.index = 0
}

func (ni *nestedIterator) Size() int {
	return len(ni.paths)
}

func TestNestedDictionary(t *testing.T) {
	testSegment := func(t *testing.T, seg segment.Segment) {
		if ns, ok := seg.(segment.NestedSegment); ok {
			var dict segment.TermDictionary
			var iter segment.DictionaryIterator
			var expectedTerms []string
			var invertedIndex map[string][]int
			var gotTerms []string
			var gotInvertedIndex map[string][]int

			// Post 0 iterator
			p0 := newStubNestedState()
			p0 = p0.Append("posts", 0)

			dict, err := ns.NestedDictionary(p0, "title")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"AI", "And", "CRISPR", "Editing", "Gene", "Mars", "Plans", "SpaceX", "Trends"}
			invertedIndex = map[string][]int{
				"AI":      {0},
				"And":     {1},
				"CRISPR":  {1},
				"Editing": {1},
				"Gene":    {1},
				"Mars":    {2},
				"Plans":   {2},
				"SpaceX":  {2},
				"Trends":  {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].title: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].title: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Published Date
			dict, err = ns.NestedDictionary(p0, "published_date")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"2025-01-05", "2025-04-22", "2025-06-10"}
			invertedIndex = map[string][]int{
				"2025-01-05": {2},
				"2025-04-22": {0},
				"2025-06-10": {1},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].published_date: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].published_date: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 0 Comment 0 iterator
			p0c0 := newStubNestedState()
			p0c0 = p0c0.Append("posts", 0)
			p0c0 = p0c0.Append("comments", 0)

			dict, err = ns.NestedDictionary(p0c0, "author")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Alex", "Jane"}
			invertedIndex = map[string][]int{
				"Alex": {1},
				"Jane": {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].comments[0].author: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].comments[0].author: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Text
			dict, err = ns.NestedDictionary(p0c0, "text")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Fascinating", "Very", "informative!", "potential."}
			invertedIndex = map[string][]int{
				"Fascinating":  {1},
				"Very":         {0},
				"informative!": {0},
				"potential.":   {1},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].comments[0].text: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].comments[0].text: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Likes
			dict, err = ns.NestedDictionary(p0c0, "likes")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"1", "2"}
			invertedIndex = map[string][]int{
				"1": {0},
				"2": {1},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].comments[0].likes: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].comments[0].likes: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 0 Comment 1 iterator
			p0c1 := newStubNestedState()
			p0c1 = p0c1.Append("posts", 0)
			p0c1 = p0c1.Append("comments", 1)

			dict, err = ns.NestedDictionary(p0c1, "author")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Tom"}
			invertedIndex = map[string][]int{
				"Tom": {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].comments[1].author: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].comments[1].author: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p0c1, "text")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Needs", "detail.", "more"}
			invertedIndex = map[string][]int{
				"Needs":   {0},
				"detail.": {0},
				"more":    {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].comments[1].text: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].comments[1].text: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p0c1, "likes")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"3"}
			invertedIndex = map[string][]int{
				"3": {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[0].comments[1].likes: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[0].comments[1].likes: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 1 iterator
			p1 := newStubNestedState()
			p1 = p1.Append("posts", 1)

			dict, err = ns.NestedDictionary(p1, "title")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Brain-Computer", "Computing", "Interfaces", "Quantum"}
			invertedIndex = map[string][]int{
				"Brain-Computer": {2},
				"Computing":      {0},
				"Interfaces":     {2},
				"Quantum":        {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].title: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].title: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p1, "published_date")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"2024-11-15", "2024-12-12"}
			invertedIndex = map[string][]int{
				"2024-11-15": {0},
				"2024-12-12": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].published_date: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].published_date: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 1 Comment 0 iterator
			p1c0 := newStubNestedState()
			p1c0 = p1c0.Append("posts", 1)
			p1c0 = p1c0.Append("comments", 0)

			dict, err = ns.NestedDictionary(p1c0, "author")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Jane", "Lina"}
			invertedIndex = map[string][]int{
				"Jane": {0},
				"Lina": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].comments[0].author: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].comments[0].author: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p1c0, "text")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"A", "Mind-blowing!", "bit", "honestly.", "scary,"}
			invertedIndex = map[string][]int{
				"A":             {2},
				"Mind-blowing!": {0},
				"bit":           {2},
				"honestly.":     {2},
				"scary,":        {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].comments[0].text: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].comments[0].text: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p1c0, "likes")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"0", "5"}
			invertedIndex = map[string][]int{
				"0": {2},
				"5": {0},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].comments[0].likes: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].comments[0].likes: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 1 Comment 1 iterator
			p1c1 := newStubNestedState()
			p1c1 = p1c1.Append("posts", 1)
			p1c1 = p1c1.Append("comments", 1)

			dict, err = ns.NestedDictionary(p1c1, "author")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Ken"}
			invertedIndex = map[string][]int{
				"Ken": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].comments[1].author: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].comments[1].author: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p1c1, "text")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Future", "Now.", "The", "is"}
			invertedIndex = map[string][]int{
				"Future": {2},
				"Now.":   {2},
				"The":    {2},
				"is":     {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].comments[1].text: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].comments[1].text: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p1c1, "likes")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"4"}
			invertedIndex = map[string][]int{
				"4": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[1].comments[1].likes: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[1].comments[1].likes: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 2 iterator
			p2 := newStubNestedState()
			p2 = p2.Append("posts", 2)

			dict, err = ns.NestedDictionary(p2, "title")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Breakthroughs", "Energy", "Fusion"}
			invertedIndex = map[string][]int{
				"Breakthroughs": {2},
				"Energy":        {2},
				"Fusion":        {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[2].title: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[2].title: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p2, "published_date")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"2025-07-01"}
			invertedIndex = map[string][]int{
				"2025-07-01": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[2].published_date: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[2].published_date: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			// Post 2 Comment 0 iterator
			p2c0 := newStubNestedState()
			p2c0 = p2c0.Append("posts", 2)
			p2c0 = p2c0.Append("comments", 0)

			dict, err = ns.NestedDictionary(p2c0, "author")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Sam"}
			invertedIndex = map[string][]int{
				"Sam": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[2].comments[0].author: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[2].comments[0].author: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p2c0, "text")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"Finally!"}
			invertedIndex = map[string][]int{
				"Finally!": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[2].comments[0].text: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[2].comments[0].text: got %v, want %v", gotInvertedIndex, invertedIndex)
			}

			dict, err = ns.NestedDictionary(p2c0, "likes")
			if err != nil {
				t.Fatalf("failed to get nested dictionary: %v", err)
			}
			iter = dict.AutomatonIterator(nil, nil, nil)
			expectedTerms = []string{"6"}
			invertedIndex = map[string][]int{
				"6": {2},
			}
			gotTerms = []string{}
			gotInvertedIndex = map[string][]int{}
			for {
				en, err := iter.Next()
				if err != nil {
					t.Fatalf("iterator error: %v", err)
				}
				if en == nil {
					break
				}
				gotTerms = append(gotTerms, en.Term)
				pl, err := dict.PostingsList([]byte(en.Term), nil, nil)
				if err != nil {
					t.Fatalf("postings list error: %v", err)
				}
				itrpl := pl.Iterator(true, true, true, nil)
				docNums := []int{}
				for {
					en, err := itrpl.Next()
					if err != nil {
						t.Fatalf("pl next error: %v", err)
					}
					if en == nil {
						break
					}
					docNums = append(docNums, int(en.Number()))
				}
				gotInvertedIndex[en.Term] = docNums
			}
			if !reflect.DeepEqual(gotTerms, expectedTerms) {
				t.Errorf("unexpected terms for posts[2].comments[0].likes: got %v, want %v", gotTerms, expectedTerms)
			}
			if !reflect.DeepEqual(gotInvertedIndex, invertedIndex) {
				t.Errorf("unexpected inverted index for posts[2].comments[0].likes: got %v, want %v", gotInvertedIndex, invertedIndex)
			}
		}
	}

	// first test case: all 3 docs in one segment
	seg, err := buildNestedSegment()
	if err != nil {
		t.Fatalf("failed to build nested segment: %v", err)
	}
	testSegment(t, seg)

	// second test case: each doc in its own segment and then merged
	seg2, err := buildNestedSegmentMerged()
	if err != nil {
		t.Fatalf("failed to build nested segment: %v", err)
	}
	testSegment(t, seg2)

}
