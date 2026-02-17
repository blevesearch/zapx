// go:build vectors
//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
)

func (sb *SegmentBase) GetCoarseQuantizer(field string) (*faiss.IndexImpl, error) {
	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 <= 0 {
		return nil, fmt.Errorf("field %s does not exist in segment", field)
	}

	vectorSection := sb.fieldsSectionsMap[fieldIDPlus1-1][SectionFaissVectorIndex]
	// check if the field has a vector section in the segment.
	if vectorSection <= 0 {
		return nil, fmt.Errorf("field %s does not have a vector section in the segment", field)
	}

	pos := int(vectorSection)
	// doc values
	_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n
	_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	// index optimization type
	optType, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	if index.VectorIndexOptimizationsReverseLookup[optType] != index.IndexOptimizedFastMerge {
		return nil, fmt.Errorf("unsupported vector index optimization type: %d", optType)
	}
	numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	for i := 0; i < int(numVecs); i++ {
		_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
		pos += n
	}
	_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n
	indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	faissIndex, err := faiss.ReadIndexFromBuffer(sb.mem[pos:pos+int(indexSize)], faiss.IOFlagReadMmap)
	if err != nil {
		return nil, err
	}
	return faissIndex, nil
}
