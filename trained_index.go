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
)

type trainedIndex interface {
	getIndex() faissIndex
	close() error
}

type trainedIndexWrapper struct {
	fieldID uint16

	cache *vectorIndexCache
	index faissIndex
}

func (ti *trainedIndexWrapper) getIndex() faissIndex {
	return ti.index
}

func (ti *trainedIndexWrapper) close() error {
	ti.cache.decRef(ti.fieldID)
	return nil
}

func (sb *SegmentBase) GetCoarseQuantizer(field string) (interface{}, error) {
	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 <= 0 {
		return nil, fmt.Errorf("field %s does not exist in segment", field)
	}

	vectorSection := sb.fieldsSectionsMap[fieldIDPlus1-1][SectionFaissVectorIndex]
	// check if the field has a vector section in the segment.
	if vectorSection <= 0 {
		return nil, fmt.Errorf("field %s does not have a vector section in the segment", field)
	}

	pos := vectorSection
	// doc values
	for i := 0; i < 2; i++ {
		_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
	}
	// read the index optimization type
	opt, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	// get the optimization type string from the reverse lookup map
	optStr := index.VectorIndexOptimizationsReverseLookup[int(opt)]

	useGPU := sb.fieldsOptions[field].UseGPU()
	index, _, _, err := sb.vecIndexCache.loadOrCreate(fieldIDPlus1-1, loadOrCreateOptions{
		mem:         sb.mem[pos:],
		numDocs:     uint32(sb.numDocs),
		useGPU:      useGPU,
		reader:      sb.fileReader,
		optStr:      optStr,
		skipMapping: true,
	})

	trainedIndex := &trainedIndexWrapper{
		fieldID: fieldIDPlus1 - 1,
		index:   index,
		cache:   sb.vecIndexCache,
	}

	return trainedIndex, err
}
