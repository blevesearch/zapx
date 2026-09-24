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
	"encoding/json"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// preassignedCentroidOversample is how many times nprobe worth of centroids
// phase one ranks. Phase two normally probes only the leading nprobe of them;
// the rest is headroom for the widening that multi-vector deduplication can
// ask for. Ranking the full nlist instead measured considerably more expensive
// than the per-segment quantizer search being replaced, which selects only the
// top nprobe, so the prefix is bounded.
const preassignedCentroidOversample = 4

// Two phase vector search, segment side.
//
// Phase one happens once per query, against the trained index, and produces a
// ranking of every centroid by its distance from the query vector (see
// SegmentBase.SearchCentroids). This file is phase two: probing the inverted
// lists that ranking names, in each segment, without asking that segment's own
// coarse quantizer where to look.
//
// The saving is one coarse quantizer search per segment per query, which for a
// large nlist is a scan over nlist * d floats. What makes it sound is that
// every segment written by the fast merge path holds a clone of the trained
// index's quantizer, so its list numbering is identical. Segments that were
// built on their own data - small ones that never went through fast merge, or
// anything written before training completed - carry a different layout, and
// are detected and sent down the ordinary search path instead.

// SharesCentroidLayout reports whether this segment's index numbers its
// clusters exactly as the index that produced pre does.
func (v *vectorIndexWrapper) SharesCentroidLayout(pre *segment.PreassignedCentroids) (
	bool, error) {
	if !pre.Valid() || v.index == nil || v.sb == nil || v.sb.vecIndexCache == nil {
		return false, nil
	}
	trained, ok := pre.Source.(faissIndex)
	if !ok || trained == nil {
		return false, nil
	}
	trainedIVF := trained.castIVF()
	if trainedIVF == nil {
		return false, nil
	}
	// the comparison walks the whole centroid table, so it is memoized per
	// cached index rather than repeated on every query.
	return v.sb.vecIndexCache.sharesLayout(v.fieldID, trainedIVF)
}

// SearchPreassigned is Search, probing the clusters named by pre rather than
// running this index's own coarse quantizer search. It falls back to Search
// whenever pre cannot be used here, so callers may pass it unconditionally.
func (v *vectorIndexWrapper) SearchPreassigned(qVector []float32, k int64,
	pre *segment.PreassignedCentroids, params json.RawMessage) (
	segment.VecPostingsList, error) {
	usable, err := v.SharesCentroidLayout(pre)
	if err != nil {
		return nil, err
	}
	if !usable {
		return v.Search(qVector, k, params)
	}
	return v.searchPreassigned(qVector, k, pre, params)
}

// searchPreassigned is SearchPreassigned with the layout check already done.
func (v *vectorIndexWrapper) searchPreassigned(qVector []float32, k int64,
	pre *segment.PreassignedCentroids, params json.RawMessage) (
	segment.VecPostingsList, error) {
	if v.index == nil {
		// vector index not found, so return empty postings list
		return emptyVecPostingsList, nil
	}
	if v.index.dim() != len(qVector) {
		// dimensionality mismatch, so return empty postings list
		return emptyVecPostingsList, nil
	}
	// check if number of docs or number of vectors is zero
	if v.mapping == nil || v.mapping.numVectors() == 0 || v.mapping.numDocuments() == 0 {
		// no vectors or no documents indexed, so return empty postings list
		return emptyVecPostingsList, nil
	}
	// check if all the vectors are excluded
	if v.exclude != nil && v.exclude.cardinality() == v.mapping.numVectors() {
		// all vectors excluded, so return empty postings list
		return emptyVecPostingsList, nil
	}
	ivfPtr := v.index.castIVF()
	if ivfPtr == nil {
		// a matching layout implies an IVF index, but stay defensive: without
		// one there are no clusters to probe.
		return v.Search(qVector, k, params)
	}
	// create a vector set using the query vector
	qVecSet, err := newVectorSet(len(qVector), qVector)
	if err != nil {
		return nil, err
	}
	rs, err := v.searchPreassignedClusters(ivfPtr, pre, qVecSet, k, v.exclude, params)
	if err != nil {
		return nil, err
	}
	// populate the postings list from the result set
	return getPostingsList(rs), nil
}

// SearchWithFilterPreassigned is SearchWithFilter, reusing pre's centroid
// ranking instead of running this index's own coarse quantizer search. It
// falls back to SearchWithFilter whenever pre cannot be used here.
func (v *vectorIndexWrapper) SearchWithFilterPreassigned(qVector []float32, k int64,
	eligibleList index.EligibleDocumentList, pre *segment.PreassignedCentroids,
	params json.RawMessage) (segment.VecPostingsList, error) {
	usable, err := v.SharesCentroidLayout(pre)
	if err != nil {
		return nil, err
	}
	if !usable {
		// a nil pre puts searchWithFilter back on the ordinary path.
		pre = nil
	}
	return v.searchWithFilter(qVector, k, eligibleList, pre, params)
}

// rankEligibleCentroids orders the centroids that hold at least one eligible
// vector by their distance from the query vector.
//
// With a nil pre this runs a coarse quantizer search restricted to those
// centroids. With a usable pre it instead walks the ranking phase one already
// produced and keeps the eligible entries, which yields the same order -
// both rank by distance from the same query vector against the same centroids
// - without touching the quantizer at all.
func (v *vectorIndexWrapper) rankEligibleCentroids(ivfPtr faissIndexIVF,
	qVecSet *vectorSet, clusterVectorCounts []int64, nlist int, k int64,
	pre *segment.PreassignedCentroids) ([]int64, []float32, error) {
	if pre != nil {
		eligibleCentroidIDs := make([]int64, 0, len(pre.IDs))
		centroidDistances := make([]float32, 0, len(pre.IDs))
		var eligibleVecs int64
		for i, centroidID := range pre.IDs {
			// FAISS pads a short result with -1; anything out of range would
			// index clusterVectorCounts out of bounds.
			if centroidID < 0 || centroidID >= int64(len(clusterVectorCounts)) {
				continue
			}
			if clusterVectorCounts[centroidID] > 0 {
				eligibleCentroidIDs = append(eligibleCentroidIDs, centroidID)
				centroidDistances = append(centroidDistances, pre.Distances[i])
				eligibleVecs += clusterVectorCounts[centroidID]
			}
		}
		// Phase one ranks only a prefix of the centroids, which is ample for an
		// unfiltered query but not always for a selective filter: the eligible
		// vectors may sit in clusters that fall outside it. When the prefix
		// does not reach k eligible vectors and there were centroids it never
		// looked at, fall through to the full quantizer search rather than
		// return a short result.
		if eligibleVecs >= k || len(pre.IDs) >= nlist {
			return eligibleCentroidIDs, centroidDistances, nil
		}
	}

	// Create a bitmap for the eligible centroids to be considered for probing.
	centroidBM := newBitmap(uint32(nlist))
	centroidCount := 0
	for centroidID, vectorCount := range clusterVectorCounts {
		// Only centroids with at least one eligible vector are considered.
		if vectorCount > 0 {
			// since we are adding only unique centroid IDs, this is simply an
			// increment and we can avoid a population count at the end
			centroidCount++
			centroidBM.set(uint32(centroidID))
		}
	}
	if centroidCount == 0 {
		return nil, nil, nil
	}
	// create a FAISS selector based on the centroid bitmap
	centroidSelector, err := getIncludeSelector(centroidBM)
	if err != nil {
		return nil, nil, err
	}
	defer centroidSelector.Delete()
	// Search the coarse quantizer to order the centroids based on proximity
	// to the query vector.
	return ivfPtr.searchQuantizer(qVecSet, centroidSelector, int64(centroidCount))
}

// searchPreassignedClusters is the unfiltered counterpart of
// searchClustersFromIVFIndex: it probes the clusters pre names, excluding any
// vector IDs in the exclude bitmap, and retries with a wider probe when
// multi-vector documents leave it short of k unique documents.
func (v *vectorIndexWrapper) searchPreassignedClusters(ivfPtr faissIndexIVF,
	pre *segment.PreassignedCentroids, qVecSet *vectorSet, k int64,
	exclude *bitmap, params json.RawMessage) (resultSet, error) {
	centroidIDs := pre.IDs
	// Distances is at least as long as IDs (PreassignedCentroids.Valid checks
	// this); trim so the two line up exactly for FAISS.
	centroidDistances := pre.Distances[:len(centroidIDs)]
	// Start from the trained index's nprobe, which is also the nprobe every
	// fast merged segment was written with, so this probes exactly what an
	// ordinary search on this segment would have.
	centroidsToProbe := min(max(pre.Nprobe, 1), len(centroidIDs))

	return v.docSearch(k, v.sb.numDocs,
		func() ([]float32, []int64, error) {
			// build the FAISS selector based on the exclude bitmap, if any. A
			// nil bitmap yields a nil selector, meaning nothing is excluded.
			// NOTE: The bitmap selector is just a wrapper over the exclude
			// bitmap which is shared across the CGO layer.
			sel, err := getExcludeSelector(exclude)
			if err != nil {
				return nil, nil, err
			}
			// NOTE: the selector being freed does NOT free the inner bitmap, as
			// we control its lifecycle in GO, to reuse the bitmap across
			// iterations, if needed, for multi-vector document retrieval.
			if sel != nil {
				defer sel.Delete()
			}
			return ivfPtr.searchClusters(centroidIDs, centroidDistances,
				centroidsToProbe, qVecSet, k, sel, params)
		},
		func(numIter int, labels []int64) bool {
			// if this is the first loop iteration and we have < k unique
			// docIDs, we must clone the existing exclude bitmap before
			// modifying it to avoid modifying the original bitmap owned by the
			// segment's cache entry
			if numIter == 1 {
				if exclude == nil {
					exclude = newBitmap(v.mapping.numVectors())
				} else {
					exclude = exclude.clone()
				}
			}
			// if we have iterated atleast nprobeIncreaseThreshold times and
			// still have not found enough unique docIDs, widen the probe for
			// the next iteration to reach vectors in further out clusters.
			// The ceiling is what phase one ranked, not nlist; beyond that we
			// have no distances to probe with and the loop relies on the
			// exclude set to make progress instead.
			if numIter >= nprobeIncreaseThreshold && centroidsToProbe < len(centroidIDs) {
				// increase by 50% of the remaining centroids, but at least by 1
				// to ensure progress.
				increaseAmount := max((len(centroidIDs)-centroidsToProbe)/2, 1)
				centroidsToProbe = min(centroidsToProbe+increaseAmount, len(centroidIDs))
			}
			// prepare the exclude list for the next iteration by adding the
			// vector ids retrieved in this iteration
			for _, vecID := range labels {
				// should not happen, but just a safeguard, as we catch -1
				// in the main loop
				if vecID == -1 {
					continue
				}
				exclude.set(uint32(vecID))
			}
			// only continue searching if there are vectors left to find
			return exclude.cardinality() != v.mapping.numVectors()
		})
}
