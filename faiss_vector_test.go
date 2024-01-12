//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
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

func buildMultiDocDataset() []index.Document {

	stubVecs := stubVecData
	stubVecs1 := stubVec1Data

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

	indexBytesLen, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	indexBytesOffset = pos
	pos += indexBytesLen

	numVecs, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	vecDocIDsMappingOffset = pos

	return docValueStart, docValueEnd, indexBytesLen, indexBytesOffset, numVecs, vecDocIDsMappingOffset
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

	ids := make([]int64, len(dataset))
	for i := 0; i < len(dataset); i++ {
		ids[i] = int64(i)
	}

	if isIVF {
		err = idx.SetDirectMap(2)
		if err != nil {
			return nil, err
		}

		err = idx.Train(vecs)
		if err != nil {
			return nil, err
		}
	}

	idx.AddWithIDs(vecs, ids)

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
	docs := buildMultiDocDataset()

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

	segOnDisk, err := vecSegPlugin.Open(path)
	if err != nil {
		t.Fatal(err)
	}

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
	vecIndex, err := letsCreateVectorIndexOfTypeForTesting(data, 3, "IDMap2,Flat", false)
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
	hitDocIDs := []uint64{2, 9, 9}
	hitVecs := [][]float32{data[0], data[7][0:3], data[7][3:6]}
	if vecSeg, ok := segOnDisk.(segment.VectorSegment); ok {
		searchVectorIndex, closeVectorIndex, err := vecSeg.InterpretVectorIndex("stubVec")
		if err != nil {
			t.Fatal(err)
		}

		pl, err := searchVectorIndex("stubVec", []float32{0.0, 0.0, 0.0}, 3, nil)
		if err != nil {
			closeVectorIndex()
			t.Fatal(err)
		}
		itr := pl.Iterator(nil)

		hitCounter := 0
		for {
			next, err := itr.Next()
			if err != nil {
				closeVectorIndex()
				t.Fatal(err)
			}
			if next == nil {
				break
			}

			expectedDocID := hitDocIDs[hitCounter]
			if next.Number() != expectedDocID {
				t.Fatalf("expected %d got %d", expectedDocID, next.Number())
			}

			ok, expectedScore, gotScore := compareL2Scores(next.Score(),
				queryVec, hitVecs[hitCounter], 3)
			if !ok {
				t.Fatalf("expected %d got %d", expectedScore, gotScore)
			}

			hitCounter++
		}
		closeVectorIndex()
	}
}

// Test to check if 2 identical vectors return unique hashes.
func TestHashCode(t *testing.T) {
	var v1 []float32
	for i := 0; i < 10; i++ {
		v1 = append(v1, rand.Float32())
	}

	h1 := hashCode(v1)

	h2 := hashCode(v1)

	if h1 == h2 {
		t.Fatal("expected unique hashes for the same vector each time the " +
			"hash is computed")
	}
}

func TestPersistedVectorSegment(t *testing.T) {
	docs := buildMultiDocDataset()

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
	queryVec := []float32{0.0, 0.0, 0.0}
	hitDocIDs := []uint64{2, 9, 9}
	hitVecs := [][]float32{data[0], data[7][0:3], data[7][3:6]}
	if vecSeg, ok := segOnDisk.(segment.VectorSegment); ok {
		searchVectorIndex, closeVectorIndex, err := vecSeg.InterpretVectorIndex("stubVec")
		if err != nil {
			t.Fatal(err)
		}

		pl, err := searchVectorIndex("stubVec", []float32{0.0, 0.0, 0.0}, 3, nil)
		if err != nil {
			closeVectorIndex()
			t.Fatal(err)
		}

		itr := pl.Iterator(nil)

		hitCounter := 0
		for {
			next, err := itr.Next()
			if err != nil {
				closeVectorIndex()
				t.Fatal(err)
			}
			if next == nil {
				break
			}

			expectedDocID := hitDocIDs[hitCounter]
			if next.Number() != expectedDocID {
				t.Fatalf("expected %d got %d", expectedDocID, next.Number())
			}

			ok, expectedScore, gotScore := compareL2Scores(next.Score(),
				queryVec, hitVecs[hitCounter], 3)
			if !ok {
				t.Fatalf("expected %d got %d", expectedScore, gotScore)
			}

			hitCounter++
		}
		closeVectorIndex()
	}
}
