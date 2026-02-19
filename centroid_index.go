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

	if index.VectorIndexOptimizationsReverseLookup[int(optType)] != index.IndexOptimizedFastMerge {
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
