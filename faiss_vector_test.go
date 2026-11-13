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

//go:build vectors
// +build vectors

package zap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func getStubDocScores(k int) (ids []uint64, scores []float32, err error) {
	for i := 0; i < k; i++ {
		ids = append(ids, uint64(i))
		scores = append(scores, float32(2*i+3)/float32(200))
	}
	// having some negative scores -> possible due to dot product
	scores[0] = -scores[0]
	scores[4] = -scores[4]
	return ids, scores, nil
}

func TestVecPostingsIterator(t *testing.T) {
	vecPL := &VecPostingsList{
		postings: roaring64.New(),
	}

	ids, scores, err := getStubDocScores(10)
	if err != nil {
		t.Fatal(err)
	}
	docIDs := make(map[uint64]float32)

	for i, id := range ids {
		code := getVectorCode(uint32(id), scores[i])
		vecPL.postings.Add(code)
		docIDs[id] = scores[i]
	}

	iter := vecPL.Iterator(nil)
	for i := 0; true; i++ {
		vp, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if vp == nil {
			break
		}
		if vp.Number() != ids[i] {
			t.Fatalf("expected %d, got %d", ids[i], vp.Number())
		}
		if vp.Score() != scores[i] {
			t.Fatalf("expected %f, got %f", scores[i], vp.Score())
		}
	}
}

type stubVecField struct {
	name        string
	value       []float32
	dims        int
	similarity  string
	encodedType byte
	options     index.FieldIndexingOptions
	optimizeFor string
}

// Vector is an implementation of the index.VectorField interface.
func (n *stubVecField) Vector() []float32 {
	return n.value
}

func (n *stubVecField) Similarity() string {
	return n.similarity
}

func (n *stubVecField) Dims() int {
	return n.dims
}

func (n *stubVecField) IndexOptimizedFor() string {
	return n.optimizeFor
}

func (n *stubVecField) Size() int {
	return 0
}

func (n *stubVecField) Name() string {
	return n.name
}

func (n *stubVecField) ArrayPositions() []uint64 {
	return nil
}

func (n *stubVecField) Options() index.FieldIndexingOptions {
	return n.options
}

func (n *stubVecField) NumPlainTextBytes() uint64 {
	return 0
}

func (n *stubVecField) AnalyzedLength() int {
	// dense vectors aren't analyzed
	return 0
}

func (n *stubVecField) EncodedFieldType() byte {
	return 'v'
}

func (n *stubVecField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	// dense vectors aren't analyzed
	return nil
}

func (n *stubVecField) Analyze() {
	// dense vectors aren't analyzed
}

func (n *stubVecField) Value() []byte {
	return nil
}

func newStubFieldVec(name string, vector []float32, d int, metric string, fieldOptions index.FieldIndexingOptions) index.Field {
	return &stubVecField{
		name:        name,
		value:       vector,
		dims:        d,
		similarity:  metric,
		encodedType: 'v',
		options:     fieldOptions,
	}
}

func newStubFieldVecWithOptimization(name string, vector []float32, d int, metric string, fieldOptions index.FieldIndexingOptions, optimizeFor string) index.Field {
	return &stubVecField{
		name:        name,
		value:       vector,
		dims:        d,
		similarity:  metric,
		encodedType: 'v',
		options:     fieldOptions,
		optimizeFor: optimizeFor,
	}
}

var stubVecData = [][]float32{
	{1.0, 2.0, 3.0},
	{12.0, 42.6, 78.65},
	{6.7, 0.876, 9.45},
	{7.437, 9.994, 0.407},
	{4.439, 0.307, 1.063},
	{6.653, 7.752, 0.972},

	// flattened nested vectors
	// len(vec) > dims and len(vec) % dims == 0
	{1.23, 2.45, 2.867, 3.33, 4.56, 5.67},
	{0.123, 0.456, 0.789, 0.987, 0.654, 0.321},
}

var stubVec1Data = [][]float32{
	{5.6, 2.3, 9.8},
	{89.1, 312.7, 940.65},
	{123.4, 8.98, 0.765},
	{0.413, 9.054, 3.393},
	{2.463, 3.388, 2.082},
	{3.371, 3.473, 6.906},
	{5.67, 4.56, 3.33, 2.867, 2.45, 1.23},
	{0.321, 0.654, 0.987, 0.789, 0.456, 0.123},
}

var stubInvalidVecData = [][]float32{
	{},
	{},
	{},
	{},
	{},
	{},
	{},
	{},
}

func buildMultiDocDataset(stubVecs, stubVecs1 [][]float32) []index.Document {
	doc1 := newStubDocument("a", []*stubField{
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

	doc3 := newVecStubDocument("c", []index.Field{
		newStubFieldSplitString("_id", nil, "c", true, false, false),
		newStubFieldVec("stubVec", stubVecs[0], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[0], 3, "l2", index.IndexField),
	})

	doc4 := newVecStubDocument("d", []index.Field{
		newStubFieldSplitString("_id", nil, "d", true, false, false),
		newStubFieldVec("stubVec", stubVecs[1], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[1], 3, "l2", index.IndexField),
	})
	doc5 := newVecStubDocument("e", []index.Field{
		newStubFieldSplitString("_id", nil, "e", true, false, false),
		newStubFieldVec("stubVec", stubVecs[2], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[2], 3, "l2", index.IndexField),
	})

	doc6 := newVecStubDocument("f", []index.Field{
		newStubFieldSplitString("_id", nil, "f", true, false, false),
		newStubFieldVec("stubVec", stubVecs[3], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[3], 3, "l2", index.IndexField),
	})
	doc7 := newVecStubDocument("g", []index.Field{
		newStubFieldSplitString("_id", nil, "g", true, false, false),
		newStubFieldVec("stubVec", stubVecs[4], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[4], 3, "l2", index.IndexField),
	})

	doc8 := newVecStubDocument("h", []index.Field{
		newStubFieldSplitString("_id", nil, "h", true, false, false),
		newStubFieldVec("stubVec", stubVecs[5], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[5], 3, "l2", index.IndexField),
	})

	doc9 := newVecStubDocument("i", []index.Field{
		newStubFieldSplitString("_id", nil, "i", true, false, false),
		newStubFieldVec("stubVec", stubVecs[6], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[6], 3, "l2", index.IndexField),
	})

	doc10 := newVecStubDocument("j", []index.Field{
		newStubFieldSplitString("_id", nil, "j", true, false, false),
		newStubFieldVec("stubVec", stubVecs[7], 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", stubVecs1[7], 3, "l2", index.IndexField),
	})

	results := []index.Document{
		doc1,
		doc2,
		doc3,
		doc4,
		doc5,
		doc6,
		doc7,
		doc8,
		doc9,
		doc10,
	}

	return results
}

type stubVecDocument struct {
	id        string
	fields    []index.Field
	composite []*stubField
}

func (s *stubVecDocument) StoredFieldsBytes() uint64 {
	return 0
}

func (s *stubVecDocument) ID() string {
	return s.id
}

func (s *stubVecDocument) Size() int {
	return 0
}

func (s *stubVecDocument) VisitFields(visitor index.FieldVisitor) {
	for _, f := range s.fields {
		visitor(f)
	}
}

func (s *stubVecDocument) HasComposite() bool {
	return len(s.composite) > 0
}

func (s *stubVecDocument) VisitComposite(visitor index.CompositeFieldVisitor) {
	for _, c := range s.composite {
		visitor(c)
	}
}

func (s *stubVecDocument) NumPlainTextBytes() uint64 {
	return 0
}

func (s *stubVecDocument) AddIDField() {

}

func (s *stubVecDocument) Indexed() bool {
	return true
}

func newVecStubDocument(id string, fields []index.Field) *stubVecDocument {
	return &stubVecDocument{
		id:     id,
		fields: fields,
	}
}

func getSectionContentOffsets(sb *SegmentBase, offset uint64) (
	docValueStart uint64,
	docValueEnd uint64,
	indexBytesLen uint64,
	indexBytesOffset uint64,
	numVecs uint64,
	vecDocIDsMappingOffset uint64,
) {
	pos := offset
	docValueStart, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	docValueEnd, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	// read the vector optimized for value
	_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	numVecs, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	// read the length of the vector to docID map
	idMapLen, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	vecDocIDsMappingOffset = pos

	// skip the vector to docID map
	pos += idMapLen

	// read the type of vector index
	_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	indexBytesLen, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	indexBytes, err := sb.fileReader.process(sb.mem[pos:int(pos+indexBytesLen)])
	pos += indexBytesLen
	if err != nil {
		return 0, 0, 0, 0, 0, 0
	}

	return docValueStart, docValueEnd, uint64(len(indexBytes)), indexBytesOffset, numVecs, vecDocIDsMappingOffset
}

func serializeVecs(dataset [][]float32) []float32 {
	var vecs []float32
	for _, vec := range dataset {
		vecs = append(vecs, vec...)
	}
	return vecs
}

func letsCreateVectorIndexOfTypeForTesting(inputData [][]float32, dims int,
	indexKey string, isIVF bool) (*faiss.IndexImpl, error) {
	// input dataset may have flattened nested vectors, len(vec) > dims
	// Let's fold them back into nested vectors
	var dataset [][]float32
	for _, vec := range inputData {
		numSubVecs := len(vec) / dims
		for i := 0; i < numSubVecs; i++ {
			subVec := vec[i*dims : (i+1)*dims]
			dataset = append(dataset, subVec)
		}
	}

	vecs := serializeVecs(dataset)

	idx, err := faiss.IndexFactory(dims, indexKey, faiss.MetricL2)
	if err != nil {
		return nil, err
	}

	if isIVF {
		err = idx.SetDirectMap(1)
		if err != nil {
			return nil, err
		}

		err = idx.Train(vecs)
		if err != nil {
			return nil, err
		}
	}

	idx.Add(vecs)

	return idx, nil
}

func calculateL2Score(qVec []float32, vec []float32) float32 {
	var score float32
	for i := 0; i < len(qVec); i++ {
		score += (qVec[i] - vec[i]) * (qVec[i] - vec[i])
	}
	return score
}

// returns true, if scores are equal upto precision
// false, expectedScore, gotScore, if scores are not equal
func compareL2Scores(gotScore float32, qVec []float32, vec []float32,
	precision int) (bool, int, int) {
	multiplier := float32(math.Pow10(precision))
	expectedScoreInt := int(calculateL2Score(qVec, vec) * multiplier)
	gotScoreInt := int(gotScore * multiplier)

	if gotScoreInt == expectedScoreInt {
		return true, expectedScoreInt, gotScoreInt
	}

	return false, expectedScoreInt, gotScoreInt
}

func TestVectorSegment(t *testing.T) {
	docs := buildMultiDocDataset(stubVecData, stubVec1Data)

	vecSegPlugin := &ZapPlugin{}
	seg, _, err := vecSegPlugin.New(docs)
	if err != nil {
		t.Fatal(err)
	}
	vecSegBase, ok := seg.(*SegmentBase)
	if !ok {
		t.Fatal("not a segment base")
	}

	path := "./test-seg"
	err = vecSegBase.Persist(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cerr := vecSegBase.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		_ = os.RemoveAll(path)
	}()

	segOnDisk, err := vecSegPlugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cerr := segOnDisk.Close()
		if cerr != nil {
			t.Fatalf("error closing segment on disk: %v", cerr)
		}
	}()

	fieldsSectionsMap := vecSegBase.fieldsSectionsMap
	stubVecFieldStartAddr := fieldsSectionsMap[vecSegBase.fieldsMap["stubVec"]-1][SectionFaissVectorIndex]
	docValueStart, docValueEnd, indexBytesLen, _,
		numVecs, _ := getSectionContentOffsets(vecSegBase, stubVecFieldStartAddr)

	if docValueStart != fieldNotUninverted {
		t.Fatal("vector field doesn't support doc values")
	}

	if docValueEnd != fieldNotUninverted {
		t.Fatal("vector field doesn't support doc values")
	}

	data := stubVecData
	vecIndex, err := letsCreateVectorIndexOfTypeForTesting(data, 3, "Flat", false)
	if err != nil {
		t.Fatalf("error creating vector index %v", err)
	}
	buf, err := faiss.WriteIndexIntoBuffer(vecIndex)
	if err != nil {
		t.Fatalf("error serializing vector index %v", err)
	}

	if indexBytesLen != uint64(len(buf)) {
		t.Fatalf("expected %d bytes got %d bytes", len(buf), indexBytesLen)
	}

	if numVecs != uint64(vecIndex.Ntotal()) {
		t.Fatalf("expected %d vecs got %d vecs", vecIndex.Ntotal(), numVecs)
	}

	queryVec := []float32{0.0, 0.0, 0.0}
	// With the deduplication logic for multi-vector documents, each document
	// appears only once in the results with its best score. The search continues
	// until k unique documents are found. Results are ordered by docID in the
	// postings list.
	//
	// Expected unique documents (ordered by squared L2 distance, lower = closer):
	//
	// First Iteration:
	// - docID 9: stubVecData[7][0:3] = {0.123, 0.456, 0.789}, L2 ≈ 0.84 (best)
	// - docID 9: stubVecData[7][3:6] = {0.987, 0.654, 0.321}, L2 ≈ 1.50 (duplicate, discarded)
	// - docID 2: stubVecData[0]      = {1.0, 2.0, 3.0},       L2 = 14
	// We have 2 unique docs so far < 3 requested, continue searching...
	//
	// Second Iteration:
	// - docID 8: stubVecData[6][0:3] = {1.23, 2.45, 2.867},   L2 ≈ 15.73 (best)
	// - docID 6: stubVecData[4]      = {4.439, 0.307, 1.063}, L2 ≈ 20.92
	// - docID 8: stubVecData[6][3:6] = {3.33, 4.56, 5.67},    L2 ≈ 64.03 (duplicate, discarded)
	// We have 4 unique docs now >= 3 requested, stop searching.
	//
	// Final unique docs: [9, 2, 8, 6]
	// Final scores: [0.85, 14, 15.74, 20.92]
	//
	// But iteration order is by docID: [2, 6, 8, 9]
	// Shortening to k will be done in bleve's knn collector
	hitDocIDs := []uint64{2, 6, 8, 9}
	hitVecs := [][]float32{data[0], data[4], data[6][0:3], data[7][0:3]}
	if vecSeg, ok := segOnDisk.(segment.VectorSegment); ok {
		vecIndex, err := vecSeg.InterpretVectorIndex("stubVec", nil)
		if err != nil {
			t.Fatal(err)
		}

		pl, err := vecIndex.Search(queryVec, 3, nil)
		if err != nil {
			vecIndex.Close()
			t.Fatal(err)
		}
		itr := pl.Iterator(nil)

		hitCounter := 0
		for {
			next, err := itr.Next()
			if err != nil {
				vecIndex.Close()
				t.Fatal(err)
			}
			if next == nil {
				break
			}

			if hitCounter >= len(hitDocIDs) {
				vecIndex.Close()
				t.Fatalf("more results than expected, got extra docID %d", next.Number())
			}

			expectedDocID := hitDocIDs[hitCounter]
			if next.Number() != expectedDocID {
				vecIndex.Close()
				t.Fatalf("expected %d got %d", expectedDocID, next.Number())
			}

			ok, expectedScore, gotScore := compareL2Scores(next.Score(),
				queryVec, hitVecs[hitCounter], 3)
			if !ok {
				vecIndex.Close()
				t.Fatalf("expected %d got %d", expectedScore, gotScore)
			}

			hitCounter++
		}
		vecIndex.Close()
	}
}

func TestPersistedVectorSegment(t *testing.T) {
	docs := buildMultiDocDataset(stubVecData, stubVec1Data)

	vecSegPlugin := &ZapPlugin{}
	seg, _, err := vecSegPlugin.New(docs)
	if err != nil {
		t.Fatal(err)
	}

	path := "./test-seg"
	if unPersistedSeg, ok := seg.(segment.UnpersistedSegment); ok {
		err = unPersistedSeg.Persist(path)
		if err != nil {
			t.Fatal(err)
		}
	}

	segOnDisk, err := vecSegPlugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cerr := segOnDisk.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		_ = os.RemoveAll(path)
	}()

	data := stubVecData
	queryVec := []float32{0.555, 0.555, 0.555}
	// With the deduplication logic for multi-vector documents, each document
	// appears only once in the results with its best score. The search continues
	// until k unique documents are found. Results are ordered by docID in the
	// postings list.
	//
	// Expected unique documents (ordered by squared L2 distance, lower = closer):
	//
	// First Iteration (k=3 requested):
	// - docID 9: stubVecData[7][0:3] = {0.123, 0.456, 0.789}, L2 ≈ 0.25 (best)
	// - docID 9: stubVecData[7][3:6] = {0.987, 0.654, 0.321}, L2 ≈ 0.25 (duplicate, discarded)
	// - docID 2: stubVecData[0]      = {1.0, 2.0, 3.0},       L2 ≈ 8.26
	// We have 2 unique docs so far < 3 requested, continue searching...
	//
	// Second Iteration (excluding vectors from first iteration):
	// - docID 8: stubVecData[6][0:3] = {1.23, 2.45, 2.867},   L2 ≈ 9.39 (best)
	// - docID 6: stubVecData[4]      = {4.439, 0.307, 1.063}, L2 ≈ 15.40
	// - docID 8: stubVecData[6][3:6] = {3.33, 4.56, 5.67},    L2 ≈ 49.90 (duplicate, discarded)
	//
	// We have 4 unique docs now >= 3 requested, stop searching.
	// Final unique docs: [9, 2, 8, 6]
	// Final scores: [0.24, 8.27, 9.39, 15.40]
	//
	// Iteration order is by docID: [2, 6, 8, 9]
	// Shortening to k will be done in bleve's knn collector
	hitDocIDs := []uint64{2, 6, 8, 9}
	hitVecs := [][]float32{data[0], data[4], data[6][0:3], data[7][0:3]}
	if vecSeg, ok := segOnDisk.(segment.VectorSegment); ok {
		vecIndex, err := vecSeg.InterpretVectorIndex("stubVec", nil)
		if err != nil {
			t.Fatal(err)
		}

		pl, err := vecIndex.Search(queryVec, 3, nil)
		if err != nil {
			vecIndex.Close()
			t.Fatal(err)
		}

		itr := pl.Iterator(nil)

		hitCounter := 0
		for {
			next, err := itr.Next()
			if err != nil {
				vecIndex.Close()
				t.Fatal(err)
			}
			if next == nil {
				break
			}

			if hitCounter >= len(hitDocIDs) {
				vecIndex.Close()
				t.Fatalf("more results than expected, got extra docID %d", next.Number())
			}

			expectedDocID := hitDocIDs[hitCounter]
			if next.Number() != expectedDocID {
				vecIndex.Close()
				t.Fatalf("expected %d got %d", expectedDocID, next.Number())
			}

			ok, expectedScore, gotScore := compareL2Scores(next.Score(),
				queryVec, hitVecs[hitCounter], 3)
			if !ok {
				vecIndex.Close()
				t.Fatalf("expected %d got %d", expectedScore, gotScore)
			}

			hitCounter++
		}
		vecIndex.Close()
	}
}

func TestValidVectorMerge(t *testing.T) {
	dataset1 := buildMultiDocDataset(stubVecData, stubVec1Data)
	dataset2 := buildMultiDocDataset(stubInvalidVecData, stubVec1Data)

	vecSegPlugin := &ZapPlugin{}
	seg, _, err := vecSegPlugin.New(dataset1)
	if err != nil {
		t.Fatal(err)
	}

	path1 := "./test-seg-1"
	if unPersistedSeg, ok := seg.(segment.UnpersistedSegment); ok {
		err = unPersistedSeg.Persist(path1)
		if err != nil {
			t.Fatal(err)
		}
	}

	segOnDisk1, err := vecSegPlugin.Open(path1)
	if err != nil {
		t.Fatal(err)
	}

	seg2, _, err := vecSegPlugin.New(dataset2)
	if err != nil {
		t.Fatal(err)
	}

	path2 := "./test-seg-2"
	if unPersistedSeg, ok := seg2.(segment.UnpersistedSegment); ok {
		err = unPersistedSeg.Persist(path2)
		if err != nil {
			t.Fatal(err)
		}
	}

	segOnDisk2, err := vecSegPlugin.Open(path2)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cerr := segOnDisk1.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		_ = os.RemoveAll(path1)

		cerr = segOnDisk2.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		_ = os.RemoveAll(path2)

	}()

	docsToDrop := make([]*roaring.Bitmap, 2)
	docsToDrop[0] = roaring.NewBitmap()

	for i := 0; i < len(dataset1); i++ {
		docsToDrop[0].AddInt(i) // ith doc of the first segment was updated in the 2nd one
	}

	mergedSegPath := "./test-merge-seg"

	_, _, err = vecSegPlugin.Merge([]segment.Segment{segOnDisk1, segOnDisk2}, docsToDrop, mergedSegPath, nil, nil)
	if err != nil {
		t.Fatalf("merge failed, err: %v", err)
	}

	mergedSeg, err := zapPlugin.Open(mergedSegPath)
	if err != nil {
		t.Fatalf("error while opening merged seg, err: %v", err)
	}

	defer func() {
		cerr := mergedSeg.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		_ = os.RemoveAll(mergedSegPath)
	}()

	seg3, _, err := vecSegPlugin.New(dataset1)
	if err != nil {
		t.Fatal(err)
	}

	path3 := "./test-seg-3"
	if unPersistedSeg, ok := seg3.(segment.UnpersistedSegment); ok {
		err = unPersistedSeg.Persist(path3)
		if err != nil {
			t.Fatal(err)
		}
	}

	segOnDisk3, err := vecSegPlugin.Open(path3)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		cerr := segOnDisk3.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
		_ = os.RemoveAll(path3)
	}()

	docsToDrop = make([]*roaring.Bitmap, 2)

	mergedSegPath1 := "./test-merge-seg-1"
	_, _, err = vecSegPlugin.Merge([]segment.Segment{mergedSeg, segOnDisk3}, docsToDrop, mergedSegPath1, nil, nil)
	if err != nil {
		t.Fatalf("merge failed, err: %v", err)
	}

	defer func() {
		_ = os.RemoveAll(mergedSegPath1)
	}()
}

func TestIndexExhaustion(t *testing.T) {
	for optim := range index.SupportedVectorIndexOptimizations {
		t.Run(optim, func(t *testing.T) {
			// Use 8-dimensional vectors (must be multiple of 8 for binary index)
			dims := 8
			numDocs := 5
			var docs []index.Document
			for i := 0; i < numDocs; i++ {
				vec := make([]float32, dims)
				for j := 0; j < dims; j++ {
					vec[j] = float32(i*dims + j)
				}
				docs = append(docs, newVecStubDocument(string(rune('a'+i)), []index.Field{
					newStubFieldSplitString("_id", nil, string(rune('a'+i)), true, false, false),
					newStubFieldVecWithOptimization("vec", vec, dims, index.CosineSimilarity, index.IndexField, optim),
				}))
			}
			vecSegPlugin := &ZapPlugin{}
			seg, _, err := vecSegPlugin.New(docs)
			if err != nil {
				t.Fatal(err)
			}
			path := "./test-seg-exhaustion-" + optim
			if unPersistedSeg, ok := seg.(segment.UnpersistedSegment); ok {
				err = unPersistedSeg.Persist(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			segOnDisk, err := vecSegPlugin.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cerr := segOnDisk.Close()
				if cerr != nil {
					t.Fatalf("error closing segment: %v", cerr)
				}
				_ = os.RemoveAll(path)
			}()
			if vecSeg, ok := segOnDisk.(segment.VectorSegment); ok {
				vecIndex, err := vecSeg.InterpretVectorIndex("vec", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer vecIndex.Close()
				queryVec := make([]float32, dims)
				pl, err := vecIndex.Search(queryVec, int64(numDocs*2), nil)
				if err != nil {
					t.Fatal(err)
				}

				itr := pl.Iterator(nil)
				resultCount := 0
				for {
					next, err := itr.Next()
					if err != nil {
						t.Fatal(err)
					}
					if next == nil {
						break
					}
					resultCount++
				}
				if resultCount != numDocs {
					t.Fatalf("expected %d results, got %d", numDocs, resultCount)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Sampled merge: choosing the training set
// ---------------------------------------------------------------------------

func fakeVecIndexes(sizes []int) []*vecIndexInfo {
	out := make([]*vecIndexInfo, len(sizes))
	for i, n := range sizes {
		ids := make([]int64, n)
		for j := range ids {
			ids[j] = int64(j)
		}
		out[i] = &vecIndexInfo{vecIds: ids}
	}
	return out
}

// The sample must total exactly what was asked for, and must never ask a
// segment for more vectors than it holds.
func TestSampleCountsExactAndBounded(t *testing.T) {
	cases := [][]int{
		{1000, 1000, 1000},
		{10, 999999, 50},      // wildly uneven
		{7, 7, 7, 7, 7, 7, 7}, // remainders everywhere
		{1},
	}
	for _, sizes := range cases {
		total := 0
		for _, n := range sizes {
			total += n
		}
		for _, want := range []int{1, 13, 100, total / 2, total} {
			if want > total {
				continue
			}
			vis := fakeVecIndexes(sizes)
			counts := sampleCountsForTraining(vis, want, total)
			sum := 0
			for i, c := range counts {
				if c > len(vis[i].vecIds) {
					t.Fatalf("sizes=%v want=%d: segment %d asked for %d of %d",
						sizes, want, i, c, len(vis[i].vecIds))
				}
				sum += c
			}
			if sum != want {
				t.Errorf("sizes=%v want=%d: allocated %d", sizes, want, sum)
			}
		}
	}
}

// Allocation must be proportional, which is the whole point: a segment holding
// most of the merge must supply most of the training data.
func TestSampleCountsAreProportional(t *testing.T) {
	sizes := []int{900000, 50000, 50000}
	vis := fakeVecIndexes(sizes)
	counts := sampleCountsForTraining(vis, 10000, 1000000)
	if counts[0] < 8800 || counts[0] > 9200 {
		t.Errorf("dominant segment got %d of 10000, want ~9000", counts[0])
	}
}

// Drawn ids must be unique, in range, sorted, and must leave the caller's
// vecIds alone - the add pass still walks those in their original order.
func TestSampleIDsAreValidAndNonDestructive(t *testing.T) {
	sizes := []int{500, 300}
	vis := fakeVecIndexes(sizes)
	orig := append([]int64(nil), vis[0].vecIds...)

	counts := sampleCountsForTraining(vis, 200, 800)
	got := sampleIDsForTraining(vis, counts)

	for i, ids := range got {
		seen := map[int64]bool{}
		for k, id := range ids {
			if seen[id] {
				t.Fatalf("segment %d: duplicate id %d", i, id)
			}
			seen[id] = true
			if id < 0 || int(id) >= sizes[i] {
				t.Fatalf("segment %d: id %d out of range", i, id)
			}
			if k > 0 && ids[k-1] > id {
				t.Fatalf("segment %d: not sorted at %d", i, k)
			}
		}
	}
	for i := range orig {
		if vis[0].vecIds[i] != orig[i] {
			t.Fatalf("caller's vecIds were reordered at %d", i)
		}
	}
}

// trainingSetSize is the single definition of how many vectors get trained on,
// now that train() takes whatever it is handed.
func TestTrainingSetSize(t *testing.T) {
	if got := trainingSetSize(0, 5000); got != 5000 {
		t.Errorf("no nlist: got %d, want all %d", got, 5000)
	}
	// 15M vectors, nlist = 4*sqrt(n): sampling should be a small fraction.
	if got := trainingSetSize(15491, 15000000); got >= 15000000/10 {
		t.Errorf("15M merge trains on %d, expected well under a tenth", got)
	}
	// A small set trains on everything.
	if got := trainingSetSize(4000, 1000); got != 1000 {
		t.Errorf("small set: got %d, want %d", got, 1000)
	}
}

// The chunk budget must hold memory flat as dimensionality changes - that is
// why it is expressed in bytes rather than in vectors.
func TestChunkIsAByteBudgetNotAVectorCount(t *testing.T) {
	for _, d := range []int{128, 768, 1536, 4096} {
		n := vectorsPerChunk(d)
		bytes := n * d * 4
		if bytes > reconstructChunkBytes {
			t.Errorf("dims=%d: chunk is %d bytes, over budget %d",
				d, bytes, reconstructChunkBytes)
		}
		if bytes < reconstructChunkBytes/2 {
			t.Errorf("dims=%d: chunk is %d bytes, wastes most of budget %d",
				d, bytes, reconstructChunkBytes)
		}
	}
	if vectorsPerChunk(0) < 1 {
		t.Error("degenerate dims must still yield at least one vector")
	}
}

// sampleTrainingVectors must draw whole rows of the source, never part of one
// and never the same row twice, and must carry the binary rows across in step
// with the float rows so that BIVF training sees a consistent pair.
func TestSampleTrainingVectors(t *testing.T) {
	const dims = 8
	const nvecs = 500

	// every component of row i has magnitude i+1, with a sign pattern derived
	// from i, so a row identifies its source and no two rows binarize alike.
	data := make([]float32, nvecs*dims)
	for i := 0; i < nvecs; i++ {
		for j := 0; j < dims; j++ {
			val := float32(i + 1)
			if (i>>(j%8))&1 == 1 {
				val = -val
			}
			data[i*dims+j] = val
		}
	}
	vecs, err := newVectorSet(dims, data)
	if err != nil {
		t.Fatal(err)
	}
	vecs.binarize()

	// asking for the whole set must not copy it
	if got := sampleTrainingVectors(vecs, nvecs); got != vecs {
		t.Error("n == nvecs should hand back the input untouched")
	}
	if got := sampleTrainingVectors(vecs, nvecs+10); got != vecs {
		t.Error("n > nvecs should hand back the input untouched")
	}

	const want = 120
	got := sampleTrainingVectors(vecs, want)
	if got.nvecs != want || len(got.floatData) != want*dims || got.dim != dims {
		t.Fatalf("got %d vectors of %d dims in %d floats, want %d / %d / %d",
			got.nvecs, got.dim, len(got.floatData), want, dims, want*dims)
	}
	if len(got.binaryData) != want*(dims/8) {
		t.Fatalf("binary data is %d bytes, want %d", len(got.binaryData), want*(dims/8))
	}

	stride := dims / 8
	seen := map[int]bool{}
	for i := 0; i < got.nvecs; i++ {
		row := got.floatData[i*dims : (i+1)*dims]
		src := int(math.Abs(float64(row[0]))) - 1
		if src < 0 || src >= nvecs {
			t.Fatalf("row %d carries %v, which is outside the source", i, row[0])
		}
		if seen[src] {
			t.Fatalf("source row %d was drawn twice", src)
		}
		seen[src] = true

		srcRow := vecs.floatData[src*dims : (src+1)*dims]
		for j := range row {
			if row[j] != srcRow[j] {
				t.Fatalf("row %d is not a whole copy of source row %d", i, src)
			}
		}
		gotBin := got.binaryData[i*stride : (i+1)*stride]
		wantBin := vecs.binaryData[src*stride : (src+1)*stride]
		if !bytes.Equal(gotBin, wantBin) {
			t.Fatalf("row %d: binary data does not match source row %d", i, src)
		}
	}
}

// ---------------------------------------------------------------------------
// Sampled merge: end to end
// ---------------------------------------------------------------------------

// buildVecSegOnDisk makes a persisted segment of n vectors, with ids prefixed
// so each segment's documents stay distinguishable after a merge.
func buildVecSegOnDisk(t *testing.T, prefix string, n, dims int,
	optimizeFor, metric string, rng *rand.Rand, path string) (segment.Segment, []float32, string) {
	t.Helper()
	var docs []index.Document
	var probeVec []float32
	var probeID string
	for i := 0; i < n; i++ {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rng.Float32()*2 - 1
		}
		id := fmt.Sprintf("%s-%d", prefix, i)
		if i == n/2 {
			probeVec = append([]float32(nil), vec...)
			probeID = id
		}
		docs = append(docs, newVecStubDocument(id, []index.Field{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldVecWithOptimization("vec", vec, dims,
				metric, index.IndexField, optimizeFor),
		}))
	}
	plugin := &ZapPlugin{}
	s, _, err := plugin.New(docs)
	if err != nil {
		t.Fatal(err)
	}
	if ups, ok := s.(segment.UnpersistedSegment); ok {
		if err = ups.Persist(path); err != nil {
			t.Fatal(err)
		}
	}
	onDisk, err := plugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return onDisk, probeVec, probeID
}

// The sampled merge must produce an index that holds every document and can
// find them again.
//
// Self-recall is the assertion worth making. An absolute hit count proves
// nothing on its own, because an IVF search probes only nprobe of nlist
// clusters and a correct index legitimately returns a fraction of the corpus.
// But a vector always falls in its own cluster, and its own cluster is the
// first one probed, so querying with a vector that went into the index must
// bring back that vector's document.
//
// The sizes matter. Sampling only bites once nlist is non-zero, which is from
// ivfThreshold upward, so a test built on the smaller stub datasets would not
// exercise this code at all.
//
// Run for a float32 optimization and a BIVF one, because they take different
// routes through the merge: BIVF binarizes every sample and every chunk, trains
// two indexes off the one sample, and writes a streamed backing index followed
// by a buffered binary one.
//
// Each is paired with a metric it can actually serve. A BIVF index keeps one
// sign bit per dimension, which discards magnitude, so Hamming distance
// approximates angular similarity and the pairing with Euclidean distance is
// meaningless -- under l2_norm a BIVF index does not return its own vectors,
// at any size.
func TestSampledMergeBuildsAUsableIndex(t *testing.T) {
	for _, tc := range []struct {
		optimizeFor string
		metric      string
	}{
		{index.IndexOptimizedForLatency, index.EuclideanDistance},
		{index.IndexBIVFWithBackingSQ8, index.CosineSimilarity},
	} {
		t.Run(tc.optimizeFor, func(t *testing.T) {
			testSampledMergeBuildsAUsableIndex(t, tc.optimizeFor, tc.metric)
		})
	}
}

func testSampledMergeBuildsAUsableIndex(t *testing.T, optimizeFor, metric string) {
	// 128 dimensions rather than 8, so a BIVF index has a realistic code space:
	// one sign bit per dimension means 8 dims would give only 256 distinct
	// codes for 2,700 vectors.
	const dims = 128
	const perSeg = 900
	const nSegs = 3
	total := perSeg * nSegs

	if nlist := determineCentroids(total); trainingSetSize(nlist, total) >= total {
		t.Fatalf("test does not exercise sampling: all %d vectors would be trained on", total)
	}

	rng := rand.New(rand.NewPCG(42, 1024))
	var segs []segment.Segment
	var paths []string
	var probeVec []float32
	var probeID string
	for i := 0; i < nSegs; i++ {
		p := fmt.Sprintf("./test-sampled-merge-%s-src-%d", optimizeFor, i)
		paths = append(paths, p)
		s, vec, id := buildVecSegOnDisk(t, fmt.Sprintf("s%d", i), perSeg, dims,
			optimizeFor, metric, rng, p)
		segs = append(segs, s)
		if i == nSegs/2 {
			probeVec, probeID = vec, id
		}
	}
	mergedPath := "./test-sampled-merge-" + optimizeFor + "-out"
	defer func() {
		for _, s := range segs {
			_ = s.Close()
		}
		for _, p := range paths {
			_ = os.RemoveAll(p)
		}
		_ = os.RemoveAll(mergedPath)
	}()

	plugin := &ZapPlugin{}
	drops := make([]*roaring.Bitmap, len(segs))
	if _, _, err := plugin.Merge(segs, drops, mergedPath, nil, nil); err != nil {
		t.Fatalf("merge: %v", err)
	}
	merged, err := plugin.Open(mergedPath)
	if err != nil {
		t.Fatalf("open merged: %v", err)
	}
	defer merged.Close()

	if got := merged.Count(); got != uint64(total) {
		t.Errorf("merged segment holds %d docs, want %d", got, total)
	}

	vecSeg, ok := merged.(segment.VectorSegment)
	if !ok {
		t.Fatal("merged segment is not a VectorSegment")
	}
	vecIndex, err := vecSeg.InterpretVectorIndex("vec", nil)
	if err != nil {
		t.Fatalf("InterpretVectorIndex: %v", err)
	}
	defer vecIndex.Close()

	const k = 10
	pl, err := vecIndex.Search(probeVec, k, nil)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	itr := pl.Iterator(nil)
	var hits []uint64
	for {
		next, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if next == nil {
			break
		}
		hits = append(hits, next.Number())
	}
	if len(hits) == 0 {
		t.Fatal("search returned nothing: the merged index is empty or untrained")
	}

	found := false
	for _, docNum := range hits {
		err = merged.VisitStoredFields(docNum, func(field string, _ byte,
			value []byte, _ []uint64) bool {
			if field == "_id" && string(value) == probeID {
				found = true
			}
			return true
		})
		if err != nil {
			t.Fatalf("VisitStoredFields(%d): %v", docNum, err)
		}
	}
	if !found {
		t.Errorf("querying with %q's own vector did not return it in the top %d of %d",
			probeID, k, total)
	}
}

// ---------------------------------------------------------------------------
// Fast merge: segment layout
// ---------------------------------------------------------------------------

// With fast merge on, a segment above the IVF threshold must get the
// quantized layout, not IVF,Flat -- otherwise it can never be merged into a
// large quantized target and every vector is reconstructed instead.
func TestFastMergeUsesQuantizedLayoutForSmallSegments(t *testing.T) {
	const nlist = 40
	cases := []struct {
		nvecs     int
		fastMerge bool
		want      string
	}{
		// Below the IVF threshold there are no centroids to share; Flat stands.
		{nvecs: 999, fastMerge: true, want: "Flat"},
		{nvecs: 999, fastMerge: false, want: "Flat"},
		// The band that the 16 MB segment bound actually produces (~4,000).
		{nvecs: 4000, fastMerge: true, want: fmt.Sprintf("IVF%d,SQ8", nlist)},
		{nvecs: 4000, fastMerge: false, want: fmt.Sprintf("IVF%d,Flat", nlist)},
		// Above the SQ8 threshold both agree, as before.
		{nvecs: 20000, fastMerge: true, want: fmt.Sprintf("IVF%d,SQ8", nlist)},
		{nvecs: 20000, fastMerge: false, want: fmt.Sprintf("IVF%d,SQ8", nlist)},
	}
	for _, c := range cases {
		got := determineFloat32IndexToUse(c.nvecs, nlist, index.IndexOptimizedForLatency, c.fastMerge)
		if got != c.want {
			t.Errorf("nvecs=%d fastMerge=%v: got %q, want %q", c.nvecs, c.fastMerge, got, c.want)
		}
	}
}

// The other optimizations already use a quantized layout throughout, so fast
// merge must not perturb them.
func TestFastMergeLeavesOtherOptimizationsAlone(t *testing.T) {
	const nlist = 40
	for _, opt := range []string{
		index.IndexOptimizedForMemoryEfficient,
		index.IndexIVFRaBitQ,
	} {
		on := determineFloat32IndexToUse(4000, nlist, opt, true)
		off := determineFloat32IndexToUse(4000, nlist, opt, false)
		if on != off {
			t.Errorf("%s: fast merge changed the layout (%q vs %q)", opt, on, off)
		}
	}
}

// isMergeable has to agree with the layout chooser: a fast-merge segment above
// ivfThreshold is built quantized and shares the centroid space, so holding it
// to ivfSq8Threshold would reject exactly the segments template adoption makes
// mergeable.
func TestIsMergeableTracksTheLayoutThreshold(t *testing.T) {
	mk := func(nvecs int, fastMerge bool) *faissFloat32Index {
		return &faissFloat32Index{
			params: newFaissIndexParams(index.IndexOptimizedForLatency, nvecs, 0, fastMerge),
		}
	}
	if !mk(4000, true).isMergeable() {
		t.Error("fast merge: a 4,000 vector segment is built quantized and must be mergeable")
	}
	if mk(4000, false).isMergeable() {
		t.Error("without fast merge a 4,000 vector segment is IVF,Flat and must not be mergeable")
	}
	if mk(999, true).isMergeable() {
		t.Error("below ivfThreshold there are no centroids to share")
	}
}
