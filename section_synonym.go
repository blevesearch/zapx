package zap

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
)

type synonymSection struct {
}

type synonymOpaque struct {
	// maps a metadata key to the metadata by processing synonym fields from the synonym documents
	metadata map[string]*index.SynonymMetadata

	fieldAddrs map[uint16]int

	fieldsMap map[string]uint16

	// resusable buffers for temporary use
	tmp0          []byte
	rhsHashBuffer []uint64
}

func (so *synonymOpaque) Set(key string, value interface{}) {
	switch key {
	case "fieldsMap":
		so.fieldsMap = value.(map[string]uint16)
	}
}

func (so *synonymOpaque) Reset() error {
	so.tmp0 = so.tmp0[:0]
	so.rhsHashBuffer = so.rhsHashBuffer[:0]
	return nil
}

func (ss *synonymSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &synonymOpaque{
		metadata:   make(map[string]*index.SynonymMetadata),
		fieldsMap:  make(map[string]uint16),
		fieldAddrs: make(map[uint16]int),
	}
	for k, v := range args {
		rv.Set(k, v)
	}
	return rv
}

func (ss *synonymSection) getSynonymOpaque(opaque map[int]resetable) *synonymOpaque {
	if _, ok := opaque[sectionSynonym]; !ok {
		opaque[sectionSynonym] = ss.InitOpaque(nil)
	}
	return opaque[sectionSynonym].(*synonymOpaque)
}

func (ss *synonymSection) Process(opaque map[int]resetable, docNum uint32, field index.Field, fieldID uint16) {
	synOpaque := ss.getSynonymOpaque(opaque)
	synOpaque.process(field, fieldID, docNum)
}

func (so *synonymOpaque) process(field index.Field, fieldID uint16, docNum uint32) {
	// check if field is valid
	if fieldID == math.MaxUint16 {
		return
	}
	// check if field is a synonym field from a synonym document
	synField, isSynonymField := field.(index.SynonymField)
	if !isSynonymField {
		return
	}
	key := synField.Name()
	so.fieldsMap[key] = fieldID + 1
	synDef := synField.AnalyzedSynonymDefinition()
	if so.metadata[key] == nil {
		// metadata with this key not seen before so create a new one now
		so.metadata[key] = &index.SynonymMetadata{
			HashToSynonyms: make(map[uint64][]*index.SynonymDocumentMap),
			HashToPhrase:   make(map[uint64]*index.PhraseDocumentMap),
			SynonymFST:     nil,
		}
	}
	currMetadata := so.metadata[key]
	rhsBuffer := so.grabRHSBuf(len(synDef.Synonyms))
	for idx, rhs := range synDef.Synonyms {
		hashVal := hash(rhs)
		if currMetadata.HashToPhrase[hashVal] == nil {
			currMetadata.HashToPhrase[hashVal] = &index.PhraseDocumentMap{
				Phrase:  rhs,
				DocNums: make(map[uint32]struct{}),
			}
		}
		rhsBuffer[idx] = hashVal
	}
	if synDef.MappingType == index.EquivalentSynonymType {
		// Input is []; Synonyms is [A, B, C]
		for idx, hashVal := range rhsBuffer {
			// [A, B, C] are equivalent
			// create A = [B,C]; B = [A,C]; C = [A,B] {LHS = RHS}
			rhsBufferCopy := make([]uint64, len(rhsBuffer))
			copy(rhsBufferCopy, rhsBuffer)
			rhsBufferCopy = remove(rhsBufferCopy, idx)
			createMapping(currMetadata, hashVal, rhsBufferCopy, docNum)
		}
	} else if synDef.MappingType == index.ExplicitSynonymType {
		// [D, E, F] = [A, B, C]
		// Input is [D, E, F] synonyms is [A, B, C]
		for _, lhs := range synDef.Input {
			hashVal := hash(lhs)
			if currMetadata.HashToPhrase[hashVal] == nil {
				// this lhs has not been seen before so create a new entry
				currMetadata.HashToPhrase[hashVal] = &index.PhraseDocumentMap{
					Phrase:  lhs,
					DocNums: make(map[uint32]struct{}),
				}
			}
			createMapping(currMetadata, hashVal, rhsBuffer, docNum)
		}
	}
}

func createMapping(currMetadata *index.SynonymMetadata, lhs uint64, rhsValues []uint64, docNum uint32) {
	// hashVal is LHS of the synonym definition and rhsValues is the RHS
	// update the synonym metadata to reflect this
	// first update the hashToPhrase map to add the docNum to the LHS
	currMetadata.HashToPhrase[lhs].DocNums[docNum] = struct{}{}
	// iterate through all the RHS values in the current synonym definition
OUTER:
	for _, rhs := range rhsValues {
		// need to iterate through all the existing RHS values for this LHS
		// to identify if this RHS has been seen before or not
		// if it has been seen before, then just append the docNum to the existing entry
		// meaning the LHS = RHS is seen in 2 or more documents
		for _, currRHS := range currMetadata.HashToSynonyms[lhs] {
			if currRHS.Hash == rhs {
				currRHS.DocNums[docNum] = struct{}{}
				continue OUTER
			}
		}
		docNums := make(map[uint32]struct{})
		docNums[docNum] = struct{}{}
		// this RHS has not been seen before so create a new entry
		currMetadata.HashToSynonyms[lhs] = append(currMetadata.HashToSynonyms[lhs], &index.SynonymDocumentMap{
			Hash:    rhs,
			DocNums: docNums,
		})
	}
}

func (ss *synonymSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {
	so := ss.getSynonymOpaque(opaque)
	return so.persist(w)
}

func (so *synonymOpaque) persist(w *CountHashWriter) (n int64, err error) {
	buf := so.grabBuf(binary.MaxVarintLen64)
	for key, metadata := range so.metadata {
		// first construct an FST from the hashmaps and add it to the metadata
		err := addSynonymFST(metadata)
		if err != nil {
			return 0, err
		}
		// then persist the metadata to disk
		dataStart := uint64(w.Count())
		marshalledMetadata, err := json.Marshal(metadata)
		if err != nil {
			return 0, err
		}
		// first write out the length of the metadata
		n := binary.PutUvarint(buf, uint64(len(marshalledMetadata)))
		_, err = w.Write(buf[:n])
		if err != nil {
			return 0, err
		}
		// write out the metadata
		_, err = w.Write(marshalledMetadata)
		if err != nil {
			return 0, err
		}
		fieldStart := w.Count()
		// disable doc values for the all fields in the synonym section
		n = binary.PutUvarint(buf, uint64(fieldNotUninverted))
		_, err = w.Write(buf[:n])
		if err != nil {
			return 0, err
		}
		n = binary.PutUvarint(buf, uint64(fieldNotUninverted))
		_, err = w.Write(buf[:n])
		if err != nil {
			return 0, err
		}
		n = binary.PutUvarint(buf, dataStart)
		_, err = w.Write(buf[:n])
		if err != nil {
			return 0, err
		}
		so.fieldAddrs[so.fieldsMap[key]-1] = fieldStart
	}
	return 0, nil
}

func addSynonymFST(metadata *index.SynonymMetadata) error {
	type element struct {
		Key   string
		Value uint64
	}
	var elementList = make([]element, len(metadata.HashToSynonyms))
	var index = 0
	for k := range metadata.HashToSynonyms {
		elementList[index] = element{
			Key:   metadata.HashToPhrase[k].Phrase,
			Value: k,
		}
		index++
	}
	sort.Slice(elementList, func(i, j int) bool {
		return elementList[i].Key < elementList[j].Key
	})
	var buf bytes.Buffer
	builder, err := vellum.New(&buf, nil)
	if err != nil {
		return err
	}
	for _, e := range elementList {
		err = builder.Insert([]byte(e.Key), e.Value)
		if err != nil {
			return err
		}
	}
	if err = builder.Close(); err != nil {
		return err
	}
	metadata.SynonymFST = buf.Bytes()
	return nil
}

func (ss *synonymSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	so := ss.getSynonymOpaque(opaque)
	return so.fieldAddrs[uint16(fieldID)]

}

func (ss *synonymSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {

	so := ss.getSynonymOpaque(opaque)
	for _, key := range fieldsInv {
		keyData := index.SynonymMetadata{
			HashToPhrase:   make(map[uint64]*index.PhraseDocumentMap),
			HashToSynonyms: make(map[uint64][]*index.SynonymDocumentMap),
		}
		for segID, segment := range segments {
			// check for the closure in meantime
			if isClosed(closeCh) {
				return seg.ErrClosed
			}
			metadata, err := segment.SynonymMetadata(key)
			if err != nil {
				return err
			}
			if metadata == nil {
				continue
			}
			for hash, synonyms := range metadata.HashToSynonyms {
				lhsStruct := metadata.HashToPhrase[hash]
				validLHS := false
				for docnum := range lhsStruct.DocNums {
					if drops[segID] == nil || !drops[segID].Contains(docnum) {
						validLHS = true
						newLhsStruct, ok := keyData.HashToPhrase[hash] // newLhsStruct is the LHS of the synonym definition in the merged segment
						if !ok {
							// this lhs has not ben seen before so create a new entry
							newLhsStruct = &index.PhraseDocumentMap{
								Phrase:  lhsStruct.Phrase,
								DocNums: make(map[uint32]struct{}),
							}
							keyData.HashToPhrase[hash] = newLhsStruct
						}
						newLhsStruct.DocNums[uint32(newDocNumsIn[segID][docnum])] = struct{}{}
					}
				}
				if !validLHS {
					continue
				}
				for _, rhsStruct := range synonyms {
					var newRhsStruct *index.SynonymDocumentMap
					duplicateRHS := false

					for _, newDefRhsStruct := range keyData.HashToSynonyms[hash] {
						if newDefRhsStruct.Hash == rhsStruct.Hash {
							// this rhs has been seen before so just append the docNums to the existing entry
							duplicateRHS = true
							newRhsStruct = newDefRhsStruct
							break
						}
					}
					if newRhsStruct == nil {
						// this rhs has not been seen before so create a new entry
						newRhsStruct = &index.SynonymDocumentMap{
							Hash:    rhsStruct.Hash,
							DocNums: make(map[uint32]struct{}),
						}
					}
					if keyData.HashToPhrase[newRhsStruct.Hash] == nil {
						keyData.HashToPhrase[newRhsStruct.Hash] = &index.PhraseDocumentMap{
							Phrase:  metadata.HashToPhrase[newRhsStruct.Hash].Phrase,
							DocNums: make(map[uint32]struct{}),
						}
					}
					for docnum := range rhsStruct.DocNums {
						if drops[segID] == nil || !drops[segID].Contains(docnum) {
							newRhsStruct.DocNums[uint32(newDocNumsIn[segID][docnum])] = struct{}{}
						}
					}
					if !duplicateRHS {
						keyData.HashToSynonyms[hash] = append(keyData.HashToSynonyms[hash], newRhsStruct)
					}
				}
			}
		}
		if len(keyData.HashToSynonyms) != 0 {
			so.metadata[key] = &keyData
		}
	}

	_, err := so.persist(w)
	return err

}

// returns the hash of the input phrase
func hash(phrase string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(phrase))
	return h.Sum64()
}

// removes the element at index i from slice s and returns the new slice
func remove(s []uint64, i int) []uint64 {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func (so *synonymOpaque) grabBuf(size int) []byte {
	buf := so.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		so.tmp0 = buf
	}
	return buf[:size]
}

func (so *synonymOpaque) grabRHSBuf(size int) []uint64 {
	buf := so.rhsHashBuffer
	if cap(buf) < size {
		buf = make([]uint64, size)
		so.rhsHashBuffer = buf
	}
	return buf[:size]
}
