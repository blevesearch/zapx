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
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

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

	opts := newVectorCacheOptions(sb.mem[pos:], uint32(sb.numDocs), nil, false, sb.fileReader, optStr, true)
	return sb.trainedIndexCache.loadOrCreate(fieldIDPlus1-1, opts)
}

// SearchCentroids implements phase one of a two phase vector search: it ranks
// every centroid of this segment's trained index for the given field by its
// distance from qVector.
//
// The ranking is deliberately produced from the trained index rather than from
// any one data segment. Every segment written by the fast merge path carries a
// clone of this same coarse quantizer, so a single ranking is valid for all of
// them, and each can go straight to probing its own inverted lists instead of
// repeating this quantizer search. Segments that were built independently -
// small ones that never went through fast merge - will not match the layout and
// are searched the ordinary way; see vectorIndexWrapper.SearchPreassigned.
//
// A nil result with a nil error means there is nothing to rank: the field has
// no vector section, no trained index, or an index that is not IVF.
func (sb *SegmentBase) SearchCentroids(field string, qVector []float32) (
	*seg.PreassignedCentroids, error) {
	quantizer, err := sb.GetCoarseQuantizer(field)
	if err != nil {
		// the field simply has nothing trained for it; the caller falls back to
		// an ordinary per-segment search.
		return nil, nil
	}
	trainedIdx, ok := quantizer.(faissIndex)
	if !ok || trainedIdx == nil {
		return nil, nil
	}
	ivfPtr := trainedIdx.castIVF()
	if ivfPtr == nil {
		// nothing to pre-assign without a coarse quantizer.
		return nil, nil
	}
	if trainedIdx.dim() != len(qVector) {
		return nil, nil
	}
	nprobe, nlist := ivfPtr.ivfParams()
	if nlist <= 0 {
		return nil, nil
	}
	qVecSet, err := newVectorSet(len(qVector), qVector)
	if err != nil {
		return nil, err
	}
	// Rank a bounded prefix rather than all nlist centroids. The distance to
	// every centroid gets computed either way, but selecting the top n out of
	// nlist is not free, and ranking all of them measured several times more
	// expensive than the per-segment quantizer search this is meant to replace
	// - which only ever selects the top nprobe.
	//
	// The headroom above nprobe is what phase two widens into when
	// multi-vector deduplication leaves it short of k documents. That widening
	// is already a fallback, so capping it here costs nothing in the common
	// case and bounds both the selection work and the size of the result.
	//
	// A nil selector means no centroid is excluded.
	centroidsToRank := min(max(nprobe, 1)*preassignedCentroidOversample, nlist)
	centroidIDs, centroidDis, err := ivfPtr.searchQuantizer(qVecSet, nil, int64(centroidsToRank))
	if err != nil {
		return nil, err
	}
	return &seg.PreassignedCentroids{
		Source:    trainedIdx,
		IDs:       centroidIDs,
		Distances: centroidDis,
		Nprobe:    max(nprobe, 1),
	}, nil
}
