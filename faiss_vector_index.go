//  Copyright (c) 2024 Couchbase, Inc.
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
	"errors"

	"github.com/blevesearch/go-faiss"
)

var (
	ErrNilIndex error = errors.New("faiss index cannot be nil")
)

// Abstract interface for Faiss vector indices, which are returned by the go-faiss library.
type faissIndex interface {
	// adds the given vectors to the index.
	add(vecs *vectorSet) error
	// closes the index and releases any associated resources.
	close()
	// returns the dimensionality of the vectors in the index.
	dim() int
	// returns the metric type used by the index, which determines how distances between vectors are computed during search.
	metricType() int
	// performs a search on the index using the provided query vector and parameters, with an optional
	// exclude selector to indicate a "blocklist" of indexed vectors to ignore during search.
	// It returns the distances and corresponding vector IDs of the top k results.
	searchWithoutIDs(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error)
	// performs a search on the index using the provided query vector and parameters, with a required
	// include selector to indicate an "allowlist" of indexed vectors to consider during search.
	// It returns the distances and corresponding vector IDs of the top k results.
	searchWithIDs(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error)
	// serializes the index into a byte slice,
	// which can be stored or transmitted.
	serialize() ([]byte, error)
	// returns the size of the index in bytes.
	size() uint64
	// -----------------------------------------------------------------
	// casting methods to access index-specific operations below
	// -----------------------------------------------------------------
	// returns the underlying IVF index if this is an IVF index,
	// and a boolean indicating whether the cast was successful.
	castIVF() faissIndexIVF
	// returns the underlying SQ index if this is an SQ index,
	// and a boolean indicating whether the cast was successful.
	castSQ() faissIndexSQ
}

// Interface for IVF-specific operations on Faiss vector indices.
type faissIndexIVF interface {
	// returns the count of the selected vector IDs in each
	// cluster of the IVF index, based on the provided selector.
	clusterVectorCounts(sel faiss.Selector, nlist int) ([]int64, error)
	// returns the top K cardinalities (number of vectors) of the centroids in the IVF index.
	centroidCardinalities(limit int, descending bool) ([]uint64, [][]float32, error)
	// returns the IVF index parameters, nprobe and nlist from the ivf index.
	ivfParams() (nprobe, nlist int)
	// performs a search on the flat index quantizer of the IVF index, considering only the
	// clusters selected by the centroidSelector and returns the search results.
	searchQuantizer(qVector *vectorSet, centroidSelector faiss.Selector, centroidCount int64) ([]int64, []float32, error)
	// performs a search on the IVF index by probing the specified clusters and returns the search results.
	// We restrict the search to a caller-supplied set of pre-assigned clusters rather than probing internally.
	searchClusters(eligibleCentroidIDs []int64, centroidDis []float32,
		centroidsToProbe int, qVecSet *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error)
	// sets the direct map type for the IVF index. The direct map is essential for
	// reconstructing vectors based on their sequential vector IDs in future merges.
	setDirectMap(directMapType int) error
	// sets the number of probes (nprobe) for the IVF index. nprobe determines how many
	// inverted lists are probed during search, and is a key parameter that controls the
	// trade-off between search accuracy and latency.
	setNProbe(nprobe int32)
	// trains the IVF index on the provided training data and adds the vectors to
	// the trained index. The training step performs k-means clustering to partition
	// the data space, which enables efficient non-exhaustive search during query time.
	trainAndAdd(trainingData *vectorSet, vecsToAdd *vectorSet) error
}

type faissIndexSQ interface {
	// trains the SQ index on the provided training data and adds the vectors to
	// the trained index. The training step performs quantization of the vector space,
	// which enables efficient storage and search of high-dimensional vectors.
	trainAndAdd(trainingData *vectorSet, vecsToAdd *vectorSet) error
}
