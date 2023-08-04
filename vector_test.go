package zap

import (
	"fmt"
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	index "github.com/blevesearch/bleve_index_api"
)

func getStubDocScores(k int) (ids []uint64, scores []float32, err error) {
	for i := 1; i <= k; i++ {
		ids = append(ids, uint64(i))
		scores = append(scores, float32((2*i+3)/200))
	}
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
		code := uint64(id)<<31 | uint64(math.Float32bits(scores[i]))
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

// DenseVector is an implementation of the index.DenseVectorField interface.
func (n *stubVecField) DenseVector() ([]float32, int, string) {
	return n.value, n.dims, n.similarity
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

func buildMultiDocDataset() []index.Document {
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
		newStubFieldVec("stubVec", []float32{1.0, 2.0, 3.0}, 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", []float32{5.6, 2.3, 9.8}, 3, "l2", index.IndexField),
	})

	doc4 := newVecStubDocument("c", []index.Field{
		newStubFieldSplitString("_id", nil, "d", true, false, false),
		newStubFieldVec("stubVec", []float32{12.0, 42.6, 78.65}, 3, "l2", index.IndexField),
		newStubFieldVec("stubVec2", []float32{89.1, 312.7, 940.65}, 3, "l2", index.IndexField),
	})

	results := []index.Document{
		doc1,
		doc2,
		doc3,
		doc4,
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

func TestVectorSegmentCreation(t *testing.T) {
	docs := buildMultiDocDataset()

	vecSegPlugin := &ZapPlugin{}
	vecSeg, _, err := vecSegPlugin.New(docs)
	if err != nil {
		t.Fatal(err)
	}
	vecSegBase, ok := vecSeg.(*SegmentBase)
	if !ok {
		t.Fatal("not a segment base")
	}
	fieldsSectionsMap := vecSegBase.fieldsSectionsMap
	fmt.Printf("vector section offset: %v %v\n", fieldsSectionsMap[sectionVectorIndex], vecSegBase.fieldsMap)
}
