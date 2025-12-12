//  Copyright (c) 2025 Couchbase, Inc.
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
	"encoding/json"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bits-and-blooms/bitset"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// vectorIndexWrapper conforms to scorch_segment_api's VectorIndex interface
type vectorIndexWrapper struct {
	fIndex             faiss.FloatIndex
	bIndex             faiss.BinaryIndex
	vecDocIDMap        map[int64]uint32
	docVecIDMap        map[uint32][]int64
	vectorIDsToExclude []int64
	fieldIDPlus1       uint16
	vecIndexSize       uint64

	sb *SegmentBase
}

//  1. returned postings list (of type PostingsList) has two types of information - docNum and its score.
//  2. both the values can be represented using roaring bitmaps.
//  3. the Iterator (of type PostingsIterator) returned would operate in terms of VecPostings.
//  4. VecPostings would just have the docNum and the score. Every call of Next()
//     and Advance just returns the next VecPostings. The caller would do a vp.Number()
//     and the Score() to get the corresponding values
func (v *vectorIndexWrapper) Search(qVector []float32, k int64, params json.RawMessage) (
	segment.VecPostingsList, error) {

	rv := &VecPostingsList{
		except:   nil, // todo: handle the except bitmap within postings iterator.
		postings: roaring64.New(),
	}

	if v.fIndex == nil || v.fIndex.D() != len(qVector) {
		return rv, nil
	}

	if v.bIndex != nil && v.bIndex.D() != len(qVector) {
		return rv, nil
	}

	if v.bIndex != nil {
		err := v.searchWithBinary(qVector, k, params, rv)
		if err != nil {
			return nil, err
		}
	} else {
		err := v.searchWithFloat(qVector, k, params, rv)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (v *vectorIndexWrapper) searchWithFloat(qVector []float32, k int64, params json.RawMessage,
	pl *VecPostingsList) error {
	scores, ids, err := v.fIndex.SearchWithoutIDs(qVector, k,
		v.vectorIDsToExclude, params)
	if err != nil {
		return err
	}

	v.addIDsToPostingsList(pl, ids, scores)
	return nil
}

func (v *vectorIndexWrapper) searchWithBinary(qVector []float32, k int64, params json.RawMessage,
	pl *VecPostingsList) error {
	binaryQueryVector := convertToBinary(qVector, 1, v.fIndex.D())
	_, binIDs, err := v.bIndex.SearchBinaryWithoutIDs(binaryQueryVector, k*4,
		v.vectorIDsToExclude, params)
	if err != nil {
		return err
	}

	distances := make([]float32, k*4)
	err = v.fIndex.DistCompute(qVector, binIDs, int(k*4), distances)
	if err != nil {
		return err
	}

	scores, ids := TopNIDsByDistance(distances, binIDs, int(k))
	v.addIDsToPostingsList(pl, ids, scores)
	return nil
}

//  1. returned postings list (of type PostingsList) has two types of information - docNum and its score.
//  2. both the values can be represented using roaring bitmaps.
//  3. the Iterator (of type PostingsIterator) returned would operate in terms of VecPostings.
//  4. VecPostings would just have the docNum and the score. Every call of Next()
//     and Advance just returns the next VecPostings. The caller would do a vp.Number()
//     and the Score() to get the corresponding values
func (v *vectorIndexWrapper) SearchWithFilter(qVector []float32, k int64,
	eligibleDocIDs []uint64, params json.RawMessage) (
	segment.VecPostingsList, error) {
	// If every element in the index is eligible (full selectivity),
	// then this can basically be considered unfiltered kNN.
	if len(eligibleDocIDs) == int(v.sb.numDocs) {
		return v.Search(qVector, k, params)
	}

	rv := &VecPostingsList{
		except:   nil, // todo: handle the except bitmap within postings iterator.
		postings: roaring64.New(),
	}

	if v.fIndex == nil || v.fIndex.D() != len(qVector) {
		// vector index not found or dimensionality mismatched
		return rv, nil
	}

	if v.bIndex != nil && v.bIndex.D() != len(qVector) {
		// binary vector index dimensionality mismatched
		return rv, nil
	}

	// Check and proceed only if non-zero documents eligible per the filter query.
	if len(eligibleDocIDs) == 0 {
		return rv, nil
	}

	// vector IDs corresponding to the local doc numbers to be
	// considered for the search
	vectorIDsToInclude := make([]int64, 0, len(eligibleDocIDs))
	for _, id := range eligibleDocIDs {
		vecIDs := v.docVecIDMap[uint32(id)]
		// In the common case where vecIDs has only one element, which occurs
		// when a document has only one vector field, we can
		// avoid the unnecessary overhead of slice unpacking (append(vecIDs...)).
		// Directly append the single element for efficiency.
		if len(vecIDs) == 1 {
			vectorIDsToInclude = append(vectorIDsToInclude, vecIDs[0])
		} else {
			vectorIDsToInclude = append(vectorIDsToInclude, vecIDs...)
		}
	}
	// In case a doc has invalid vector fields but valid non-vector fields,
	// filter hit IDs may be ineligible for the kNN since the document does
	// not have any/valid vectors.
	if len(vectorIDsToInclude) == 0 {
		return rv, nil
	}
	// If the index is not an IVF index, then the search can be
	// performed directly, using the Flat index.
	if !v.fIndex.IsIVFIndex() {
		// vector IDs corresponding to the local doc numbers to be
		// considered for the search
		scores, ids, err := v.fIndex.SearchWithIDs(qVector, k,
			vectorIDsToInclude, params)
		if err != nil {
			return nil, err
		}
		v.addIDsToPostingsList(rv, ids, scores)
		return rv, nil
	}

	var ids []int64
	var include bool
	// If there are more elements to be included than excluded, it
	// might be quicker to use an exclusion selector as a filter
	// instead of an inclusion selector.
	if float32(len(eligibleDocIDs))/float32(len(v.docVecIDMap)) > 0.5 {
		// Use a bitset to efficiently track eligible document IDs.
		// This reduces the lookup cost when checking if a document ID is eligible,
		// compared to using a map or slice.
		bs := bitset.New(uint(len(eligibleDocIDs)))
		for _, docID := range eligibleDocIDs {
			bs.Set(uint(docID))
		}
		ids := make([]int64, 0, len(v.docVecIDMap)-len(vectorIDsToInclude))
		for docID, vecIDs := range v.docVecIDMap {
			// Check if the document ID is NOT in the eligible set, marking it as ineligible.
			if !bs.Test(uint(docID)) {
				// In the common case where vecIDs has only one element, which occurs
				// when a document has only one vector field, we can
				// avoid the unnecessary overhead of slice unpacking (append(vecIDs...)).
				// Directly append the single element for efficiency.
				if len(vecIDs) == 1 {
					ids = append(ids, vecIDs[0])
				} else {
					ids = append(ids, vecIDs...)
				}
			}
		}
		include = false
	} else {
		include = true
		ids = vectorIDsToInclude
	}

	if v.bIndex != nil {
		err := v.searchBinaryWithFilter(qVector, k, params, ids, include, rv)
		if err != nil {
			return nil, err
		}
	} else {
		err := v.searchFloatWithFilter(qVector, k, params, ids, vectorIDsToInclude, include, rv)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (v *vectorIndexWrapper) searchBinaryWithFilter(qVector []float32, k int64,
	params json.RawMessage, filteredIds []int64, include bool,
	pl *VecPostingsList) error {

	var ids []int64
	var err error
	binVec := convertToBinary(qVector, 1, v.fIndex.D())
	if include {
		_, ids, err = v.bIndex.SearchBinaryWithIDs(binVec, k*4, filteredIds, params)

	} else {
		_, ids, err = v.bIndex.SearchBinaryWithoutIDs(binVec, k*4, filteredIds, params)
	}
	if err != nil {
		return err
	}

	distances := make([]float32, len(ids))
	err = v.fIndex.DistCompute(qVector, ids, len(ids), distances)
	if err != nil {
		return err
	}
	scores, ids := TopNIDsByDistance(distances, ids, int(k))
	v.addIDsToPostingsList(pl, ids, scores)
	return nil
}

func (v *vectorIndexWrapper) searchFloatWithFilter(qVector []float32, k int64,
	params json.RawMessage, filteredIds []int64, idsToInclude []int64,
	include bool, pl *VecPostingsList) error {
	// Determining which clusters, identified by centroid ID,
	// have at least one eligible vector and hence, ought to be
	// probed.
	clusterVectorCounts, err := v.fIndex.ObtainClusterVectorCountsFromIVFIndex(idsToInclude)
	if err != nil {
		return err
	}
	// Ordering the retrieved centroid IDs by increasing order
	// of distance i.e. decreasing order of proximity to query vector.
	centroidIDs := make([]int64, 0, len(clusterVectorCounts))
	for centroidID := range clusterVectorCounts {
		centroidIDs = append(centroidIDs, centroidID)
	}
	closestCentroidIDs, centroidDistances, err :=
		v.fIndex.ObtainClustersWithDistancesFromIVFIndex(qVector, centroidIDs)
	if err != nil {
		return err
	}

	// Getting the nprobe value set at index time.
	nprobe := int(v.fIndex.GetNProbe())

	// Determining the minimum number of centroids to be probed
	// to ensure that at least 'k' vectors are collected while
	// examining at least 'nprobe' centroids.
	var eligibleDocsTillNow int64
	minEligibleCentroids := len(closestCentroidIDs)
	for i, centroidID := range closestCentroidIDs {
		eligibleDocsTillNow += clusterVectorCounts[centroidID]
		// Stop once we've examined at least 'nprobe' centroids and
		// collected at least 'k' vectors.
		if eligibleDocsTillNow >= k && i+1 >= nprobe {
			minEligibleCentroids = i + 1
			break
		}
	}

	var selector faiss.Selector
	if include {
		selector, err = faiss.NewIDSelectorBatch(filteredIds)
	} else {
		selector, err = faiss.NewIDSelectorNot(filteredIds)
	}
	if err != nil {
		return err
	}
	// If no error occurred during the creation of the selector, then
	// it should be deleted once the search is complete.
	defer selector.Delete()

	// Search the clusters specified by 'closestCentroidIDs' for
	// vectors whose IDs are present in 'vectorIDsToInclude'
	scores, ids, err := v.fIndex.SearchClustersFromIVFIndex(
		selector, closestCentroidIDs, minEligibleCentroids,
		k, qVector, centroidDistances, params)
	if err != nil {
		return err
	}
	v.addIDsToPostingsList(pl, ids, scores)
	return nil
}

func (v *vectorIndexWrapper) Close() {
	// skipping the closing because the index is cached and it's being
	// deferred to a later point of time.
	v.sb.vecIndexCache.decRef(v.fieldIDPlus1)
}

func (v *vectorIndexWrapper) Size() uint64 {
	return v.vecIndexSize
}

func (v *vectorIndexWrapper) ObtainKCentroidCardinalitiesFromIVFIndex(limit int, descending bool) ([]index.CentroidCardinality, error) {
	if v.fIndex == nil || !v.fIndex.IsIVFIndex() {
		return nil, nil
	}

	cardinalities, centroids, err := v.fIndex.ObtainKCentroidCardinalitiesFromIVFIndex(limit, descending)
	if err != nil {
		return nil, err
	}
	centroidCardinalities := make([]index.CentroidCardinality, len(cardinalities))
	for i, cardinality := range cardinalities {
		centroidCardinalities[i] = index.CentroidCardinality{
			Centroid:    centroids[i],
			Cardinality: cardinality,
		}
	}
	return centroidCardinalities, nil
}

// Utility function to add the corresponding docID and scores for each vector
// returned after the kNN query to the newly
// created vecPostingsList
func (v *vectorIndexWrapper) addIDsToPostingsList(pl *VecPostingsList, ids []int64, scores []float32) {
	for i := 0; i < len(ids); i++ {
		vecID := ids[i]
		// Checking if it's present in the vecDocIDMap.
		// If -1 is returned as an ID(insufficient vectors), this will ensure
		// it isn't added to the final postings list.
		if docID, ok := v.vecDocIDMap[vecID]; ok {
			code := getVectorCode(docID, scores[i])
			pl.postings.Add(code)
		}
	}
}

func TopNIDsByDistance(dist []float32, ids []int64, n int) ([]float32, []int64) {
	if n <= 0 || n > len(dist) {
		return nil, nil
	}

	// We want the N largest distances
	target := len(dist) - n

	quickselect(dist, ids, 0, len(dist)-1, target)

	// Return top-N IDs (unordered)
	return dist[target:], ids[target:]
}

func quickselect(dist []float32, ids []int64, left, right, k int) {
	for left < right {
		pivot := partition(dist, ids, left, right)
		if pivot == k {
			return
		} else if pivot < k {
			left = pivot + 1
		} else {
			right = pivot - 1
		}
	}
}

func partition(dist []float32, ids []int64, left, right int) int {
	pivotVal := dist[right]
	store := left

	for i := left; i < right; i++ {
		// We want largest distances ⇒ partition small ones left
		if dist[i] < pivotVal {
			dist[i], dist[store] = dist[store], dist[i]
			ids[i], ids[store] = ids[store], ids[i]
			store++
		}
	}

	dist[store], dist[right] = dist[right], dist[store]
	ids[store], ids[right] = ids[right], ids[store]
	return store
}
