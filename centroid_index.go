// go:build vectors
//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"

	faiss "github.com/blevesearch/go-faiss"
)

func (sb *SegmentBase) GetCoarseQuantizer(field string) (*faiss.IndexImpl, error) {
	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 <= 0 {
		// fmt.Println("invalid field ID", fieldIDPlus1, field)
		return nil, nil
	}

	vectorSection := sb.fieldsSectionsMap[fieldIDPlus1-1][SectionFaissVectorIndex]
	// check if the field has a vector section in the segment.
	if vectorSection <= 0 {
		// fmt.Println("no vector section", fieldIDPlus1, field)
		return nil, nil
	}

	pos := int(vectorSection)
	// the below loop loads the following:
	// 1. doc values(first 2 iterations) - adhering to the sections format. never
	// valid values for vector section
	// 2. index optimization type. --> this should be set to fastmerge to safeguard against searches
	for i := 0; i < 3; i++ {
		_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
		pos += n
	}

	// if index optimization type is fastmerge, then don't search

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
