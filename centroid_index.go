// go:build vectors
//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"
	"fmt"

	faiss "github.com/blevesearch/go-faiss"
)

// type CentroidIndexSegment interface {
// 	segment.Segment
// 	GetCoarseQuantizer(field string) (*faiss.IndexImpl, error)
// }

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
	// 2. index optimization type.
	for i := 0; i < 3; i++ {
		_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
		pos += n
	}

	nvecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	if nvecs > 0 {
		fmt.Println("nvecs > 0", nvecs, field)
		return nil, fmt.Errorf("centroid index is supposed to be a template index")
	}
	indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += n

	fmt.Println("indexSize", indexSize, field)
	// centroid index doesn't have any data vectors in it, its just template with coarse quantizer
	faissIndex, err := faiss.ReadIndexFromBuffer(sb.mem[pos:pos+int(indexSize)], faiss.IOFlagReadMmap)
	if err != nil {
		return nil, err
	}

	fmt.Println("faissIndex", faissIndex != nil)
	fmt.Println("faissIndex.IsIVFIndex()", faissIndex.IsIVFIndex())
	fmt.Println("faissIndex.Ntotal()", faissIndex.Ntotal())
	fmt.Println("faissIndex.D()", faissIndex.D())
	return faissIndex, nil
}
