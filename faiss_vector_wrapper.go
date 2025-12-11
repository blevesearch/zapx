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
	faiss "github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// vectorIndexWrapper conforms to scorch_segment_api's VectorIndex interface
type vectorIndexWrapper struct {
	vecIndex           *faiss.IndexImpl
	vecDocIDMap        map[int64]uint32
	docVecIDMap        map[uint32][]int64
	vectorIDsToExclude []int64
	fieldIDPlus1       uint16
	vecIndexSize       uint64

	sb *SegmentBase
}

func (v *vectorIndexWrapper) Search(qVector []float32, k int64,
	params json.RawMessage) (
	segment.VecPostingsList, error) {
	// 1. returned postings list (of type PostingsList) has two types of information - docNum and its score.
	// 2. both the values can be represented using roaring bitmaps.
	// 3. the Iterator (of type PostingsIterator) returned would operate in terms of VecPostings.
	// 4. VecPostings would just have the docNum and the score. Every call of Next()
	//    and Advance just returns the next VecPostings. The caller would do a vp.Number()
	//    and the Score() to get the corresponding values
	rv := &VecPostingsList{
		except:   nil, // todo: handle the except bitmap within postings iterator.
		postings: roaring64.New(),
	}

	if v.vecIndex == nil || v.vecIndex.D() != len(qVector) {
		// vector index not found or dimensionality mismatched
		return rv, nil
	}

	if v.sb.numDocs == 0 {
		return rv, nil
	}

	scores, ids, err := v.vecIndex.SearchWithoutIDs(qVector, k,
		v.vectorIDsToExclude, params)
	if err != nil {
		return nil, err
	}

	v.addIDsToPostingsList(rv, ids, scores)

	return rv, nil
}

func (v *vectorIndexWrapper) SearchWithFilter(qVector []float32, k int64,
	eligibleDocIDs []uint64, params json.RawMessage) (
	segment.VecPostingsList, error) {
	// If every element in the index is eligible (full selectivity),
	// then this can basically be considered unfiltered kNN.
	if len(eligibleDocIDs) == int(v.sb.numDocs) {
		return v.Search(qVector, k, params)
	}
	// 1. returned postings list (of type PostingsList) has two types of information - docNum and its score.
	// 2. both the values can be represented using roaring bitmaps.
	// 3. the Iterator (of type PostingsIterator) returned would operate in terms of VecPostings.
	// 4. VecPostings would just have the docNum and the score. Every call of Next()
	//    and Advance just returns the next VecPostings. The caller would do a vp.Number()
	//    and the Score() to get the corresponding values
	rv := &VecPostingsList{
		except:   nil, // todo: handle the except bitmap within postings iterator.
		postings: roaring64.New(),
	}
	if v.vecIndex == nil || v.vecIndex.D() != len(qVector) {
		// vector index not found or dimensionality mismatched
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
	if !v.vecIndex.IsIVFIndex() {
		// vector IDs corresponding to the local doc numbers to be
		// considered for the search
		scores, ids, err := v.vecIndex.SearchWithIDs(qVector, k,
			vectorIDsToInclude, params)
		if err != nil {
			return nil, err
		}
		v.addIDsToPostingsList(rv, ids, scores)
		return rv, nil
	}
	// Determining which clusters, identified by centroid ID,
	// have at least one eligible vector and hence, ought to be
	// probed.
	clusterVectorCounts, err := v.vecIndex.ObtainClusterVectorCountsFromIVFIndex(vectorIDsToInclude)
	if err != nil {
		return nil, err
	}
	var selector faiss.Selector
	// If there are more elements to be included than excluded, it
	// might be quicker to use an exclusion selector as a filter
	// instead of an inclusion selector.
	if float32(len(eligibleDocIDs))/float32(len(v.docVecIDMap)) > 0.5 {
		// Use a bitset to efficiently track eligible document IDs.
		// This reduces the lookup cost when checking if a document ID is eligible,
		// compared to using a map or slice.
		bs := bitset.New(uint(v.sb.numDocs))
		for _, docID := range eligibleDocIDs {
			bs.Set(uint(docID))
		}
		ineligibleVectorIDs := make([]int64, 0, len(v.vecDocIDMap)-len(vectorIDsToInclude))
		for docID, vecIDs := range v.docVecIDMap {
			// Check if the document ID is NOT in the eligible set, marking it as ineligible.
			if !bs.Test(uint(docID)) {
				// In the common case where vecIDs has only one element, which occurs
				// when a document has only one vector field, we can
				// avoid the unnecessary overhead of slice unpacking (append(vecIDs...)).
				// Directly append the single element for efficiency.
				if len(vecIDs) == 1 {
					ineligibleVectorIDs = append(ineligibleVectorIDs, vecIDs[0])
				} else {
					ineligibleVectorIDs = append(ineligibleVectorIDs, vecIDs...)
				}
			}
		}
		selector, err = faiss.NewIDSelectorNot(ineligibleVectorIDs)
	} else {
		selector, err = faiss.NewIDSelectorBatch(vectorIDsToInclude)
	}
	if err != nil {
		return nil, err
	}
	// If no error occurred during the creation of the selector, then
	// it should be deleted once the search is complete.
	defer selector.Delete()
	// Ordering the retrieved centroid IDs by increasing order
	// of distance i.e. decreasing order of proximity to query vector.
	centroidIDs := make([]int64, 0, len(clusterVectorCounts))
	for centroidID := range clusterVectorCounts {
		centroidIDs = append(centroidIDs, centroidID)
	}
	closestCentroidIDs, centroidDistances, err :=
		v.vecIndex.ObtainClustersWithDistancesFromIVFIndex(qVector, centroidIDs)
	if err != nil {
		return nil, err
	}
	// Getting the nprobe value set at index time.
	nprobe := int(v.vecIndex.GetNProbe())
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
	// Search the clusters specified by 'closestCentroidIDs' for
	// vectors whose IDs are present in 'vectorIDsToInclude'
	scores, ids, err := v.vecIndex.SearchClustersFromIVFIndex(
		selector, closestCentroidIDs, minEligibleCentroids,
		k, qVector, centroidDistances, params)
	if err != nil {
		return nil, err
	}
	v.addIDsToPostingsList(rv, ids, scores)
	return rv, nil
}

func (v *vectorIndexWrapper) Close() {
	// skipping the closing because the index is cached and it's being
	// deferred to a later point of time.
	v.sb.vecIndexCache.decRef(v.fieldIDPlus1)
}

func (v *vectorIndexWrapper) Size() uint64 {
	return v.vecIndexSize
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
