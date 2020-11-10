package zap

import (
	"strings"

	index "github.com/blevesearch/bleve_index_api"
)

type stubDocument struct {
	id        string
	fields    []*stubField
	composite []*stubField
}

func (s *stubDocument) ID() string {
	return s.id
}

func (s *stubDocument) Size() int {
	return 0
}

func (s *stubDocument) VisitFields(visitor index.FieldVisitor) {
	for _, f := range s.fields {
		visitor(f)
	}
}

func (s *stubDocument) HasComposite() bool {
	return len(s.composite) > 0
}

func (s *stubDocument) VisitComposite(visitor index.CompositeFieldVisitor) {
	for _, c := range s.composite {
		visitor(c)
	}
}

func (s *stubDocument) addField(f *stubField) {
	s.fields = append(s.fields, f)
}

func (s *stubDocument) addComposite(f *stubField) {
	s.composite = append(s.composite, f)
}

func (s *stubDocument) NumPlainTextBytes() uint64 {
	return 0
}

func (s *stubDocument) AddIDField() {

}

func newStubDocument(id string, fields []*stubField, compositeName string) *stubDocument {
	rv := &stubDocument{
		id:     id,
		fields: fields,
	}
	// fixup composite
	cf := &stubField{
		name:           compositeName,
		value:          nil,
		arrayPositions: nil,
		encodedType:    'c',
		stored:         false,
		docVals:        false,
		analyzedLen:    0,
		analyzedFreqs:  make(index.TokenFrequencies),
	}
	for _, f := range rv.fields {
		if f.name == "_id" {
			continue
		}
		cf.analyzedLen += f.analyzedLen
		cf.analyzedFreqs.MergeAll(f.name, f.analyzedFreqs)
	}
	rv.composite = []*stubField{cf}

	return rv
}

type stubField struct {
	name           string
	value          []byte
	arrayPositions []uint64
	encodedType    byte
	stored         bool
	docVals        bool
	analyzedLen    int
	analyzedFreqs  index.TokenFrequencies
}

func newStubFieldSplitString(name string, arrayPositions []uint64, value string, stored, docVals, termVectors bool) *stubField {
	tokens := strings.Split(value, " ")
	analyzedFreqs := make(index.TokenFrequencies)
	var offset int
	for i, token := range tokens {
		curr, exists := analyzedFreqs[token]
		if exists {
			curr.SetFrequency(curr.Frequency() + 1)
			if termVectors {
				curr.Locations = append(curr.Locations, &index.TokenLocation{
					ArrayPositions: arrayPositions,
					Start:          offset,
					End:            offset + len(token),
					Position:       i + 1,
				})
			}
		} else {
			newToken := &index.TokenFreq{
				Term: []byte(token),
				Locations: []*index.TokenLocation{&index.TokenLocation{
					ArrayPositions: arrayPositions,
					Start:          offset,
					End:            offset + len(token),
					Position:       i + 1,
				}},
			}
			newToken.SetFrequency(1)
			analyzedFreqs[token] = newToken
		}

		offset += len(token) + 1
	}

	return &stubField{
		name:           name,
		value:          []byte(value),
		arrayPositions: arrayPositions,
		encodedType:    't',
		stored:         stored,
		docVals:        docVals,
		analyzedLen:    len(analyzedFreqs),
		analyzedFreqs:  analyzedFreqs,
	}
}

func (s *stubField) Name() string {
	return s.name
}

func (s *stubField) Value() []byte {
	return s.value
}

func (s *stubField) ArrayPositions() []uint64 {
	return s.arrayPositions
}

func (s *stubField) EncodedFieldType() byte {
	return s.encodedType
}

func (s *stubField) Analyze() {

}

func (s *stubField) IsIndexed() bool {
	return true
}

func (s *stubField) IncludeTermVectors() bool {
	return true
}

func (s *stubField) IsStored() bool {
	return s.stored
}

func (s *stubField) IncludeDocValues() bool {
	return s.docVals
}

func (s *stubField) AnalyzedLength() int {
	return s.analyzedLen
}

func (s *stubField) AnalyzedTokenFrequencies() index.TokenFrequencies {
	return s.analyzedFreqs
}

func (s *stubField) NumPlainTextBytes() uint64 {
	return 0
}

func (s *stubField) Compose(field string, length int, freq index.TokenFrequencies) {

}
