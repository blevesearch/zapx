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

func (s *stubDocument) StoredFieldsBytes() uint64 {
	return 0
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
		options:        index.IndexField | index.IncludeTermVectors,
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
	options        index.FieldIndexingOptions
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
				Locations: []*index.TokenLocation{{
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

	var fieldOptions = index.IndexField | index.IncludeTermVectors
	if stored {
		fieldOptions |= index.StoreField
	}
	if docVals {
		fieldOptions |= index.DocValues
	}

	return &stubField{
		name:           name,
		value:          []byte(value),
		arrayPositions: arrayPositions,
		encodedType:    't',
		options:        fieldOptions,
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

func (s *stubField) Options() index.FieldIndexingOptions {
	return s.options
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
