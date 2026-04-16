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

	faiss "github.com/blevesearch/go-faiss"
)

// ---------------------------------
// Faiss Binary IVF Index
// ---------------------------------
type faissBinaryIndex struct {
	backing *faiss.IndexImpl
	binary  *faiss.BinaryIndexImpl
}

func newFaissBinaryIndex(binary *faiss.BinaryIndexImpl, backing *faiss.IndexImpl) (index faissIndex, err error) {
	// the binary index cannot be nil, but the backing index can be nil, depending on the use case.
	if binary == nil {
		return nil, errNilIndex
	}
	return &faissBinaryIndex{
		backing: backing,
		binary:  binary,
	}, nil
}

func (b *faissBinaryIndex) add(vecs *vectorSet) error {
	// add the binaryData from the vectorSet to the binary index
	return b.binary.Add(vecs.binaryData)
}

func (b *faissBinaryIndex) close() {
	b.binary.Close()
	if b.backing != nil {
		b.backing.Close()
	}
}

func (b *faissBinaryIndex) dim() int {
	return b.binary.D()
}

func (b *faissBinaryIndex) metricType() int {
	if b.backing != nil {
		return b.backing.MetricType()
	}
	return b.binary.MetricType()
}

func (b *faissBinaryIndex) ntotal() int64 {
	return b.binary.Ntotal()
}

func (b *faissBinaryIndex) reconstructBatch(vecIDs []int64, prealloc []float32) ([]float32, error) {
	// binary indexes do not support reconstruction
	return nil, errNotSupported
}

func (b *faissBinaryIndex) search(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	// search the binary index with oversampling and then do a re-ranking on the
	// FAISS index to get the top K results
	// first binarize the query vector if not already done
	qVector.binarize()
	// search the binary index with oversampling to get a larger set of candidate binary IDs for re-ranking
	_, binIDs, err := b.binary.SearchWithOptions(qVector.binaryData, binaryOversampleValue*k,
		selector, params)
	if err != nil {
		return nil, nil, err
	}
	var scores []float32
	var labels []int64
	// if we have a backing index for re-ranking, compute the distances/scores for the
	// retrieved binary IDs and then get the top K results based on those distances/scores.
	if b.backing != nil {
		distances, err := b.backing.DistCompute(qVector.floatData, binIDs)
		if err != nil {
			return nil, nil, err
		}
		// quick select algorithm for inplace partial sorting to get top K results
		// based on distances/scores
		scores, labels = topNIDsByDistance(distances, binIDs, int(k))
	} else {
		// if we don't have a backing index for re-ranking, we return error since we cannot return meaningful
		// scores without a backing index to compute distances/scores for the retrieved binary IDs.
		return nil, nil, errNotSupported
	}
	return scores, labels, nil
}

func (b *faissBinaryIndex) serialize() ([]byte, error) {
	return faiss.WriteBinaryIndexIntoBuffer(b.binary)
}

func (b *faissBinaryIndex) size() uint64 {
	sizeInBytes := b.binary.Size()
	if b.backing != nil {
		sizeInBytes += b.backing.Size()
	}
	return sizeInBytes
}

// -----------------------------------------------------------------
// casting methods to access index-specific operations below
// -----------------------------------------------------------------
func (b *faissBinaryIndex) castIVF() faissIndexIVF {
	if b.binary.IsIVFIndex() {
		// return b itself, as the IVF interface is implemented by the same
		// struct as the non-IVF interface in go-faiss.
		return b
	}
	// not an IVF index, return nil.
	return nil
}

// Binary indexes don't support SQ, so this always returns nil.
func (b *faissBinaryIndex) castSQ() faissIndexSQ {
	return nil
}

// -----------------------------------------------------------------
// IVF-Index specific operations
// -----------------------------------------------------------------

func (b *faissBinaryIndex) centroidCardinalities(limit int, descending bool) ([]uint64, [][]float32, error) {
	cardinalites, bCentroids, err := b.binary.ObtainKCentroidCardinalitiesFromIVFIndex(limit, descending)
	if err != nil {
		return nil, nil, err
	}
	centroids := make([][]float32, len(bCentroids))
	for i := range bCentroids {
		centroids[i] = make([]float32, len(bCentroids[i]))
		for j := range bCentroids[i] {
			centroids[i][j] = float32(bCentroids[i][j])
		}
	}
	return cardinalites, centroids, nil
}

func (b *faissBinaryIndex) clusterVectorCounts(sel faiss.Selector, nlist int) ([]int64, error) {
	return b.binary.ObtainClusterVectorCountsFromIVFIndex(sel, nlist)
}

func (b *faissBinaryIndex) ivfParams() (nprobe, nlist int) {
	return b.binary.IVFParams()
}

func (b *faissBinaryIndex) searchQuantizer(qVector *vectorSet, centroidSelector faiss.Selector, centroidCount int64) ([]int64, []float32, error) {
	// binarize the query vector if not already done
	qVector.binarize()
	ids, dis, err := b.binary.ObtainClustersWithDistancesFromIVFIndex(qVector.binaryData, centroidSelector, centroidCount)
	if err != nil {
		return nil, nil, err
	}
	distances := make([]float32, len(dis))
	for i, d := range dis {
		distances[i] = float32(d)
	}
	return ids, distances, nil
}

func (b *faissBinaryIndex) searchClusters(eligibleCentroidIDs []int64, centroidDis []float32,
	centroidsToProbe int, qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	// binarize the query vector if not already done
	qVector.binarize()
	// convert the float distances to binary distances for the binary index search
	binaryCentroidDis := make([]int32, len(centroidDis))
	for i, d := range centroidDis {
		binaryCentroidDis[i] = int32(d)
	}
	// search the binary index without oversampling, since we are already searching a
	// limited number of centroids specified by centroidsToProbe
	_, binIDs, err := b.binary.SearchClustersFromIVFIndex(eligibleCentroidIDs, binaryCentroidDis,
		centroidsToProbe, qVector.binaryData, k, selector, params)
	if err != nil {
		return nil, nil, err
	}
	var scores []float32
	var labels []int64
	// if we have a backing index for re-ranking, compute the distances/scores for the
	// retrieved binary IDs and then get the top K results based on those distances/scores.
	if b.backing != nil {
		// reranking is still necessary since hamming distance has a lot of collisions
		distances, err := b.backing.DistCompute(qVector.floatData, binIDs)
		if err != nil {
			return nil, nil, err
		}
		// quick select algorithm for inplace partial sorting to get top K results
		// based on distances/scores
		scores, labels = topNIDsByDistance(distances, binIDs, int(k))
	} else {
		// if we don't have a backing index for re-ranking, we return error since we cannot return meaningful
		// scores without a backing index to compute distances/scores for the retrieved binary IDs.
		return nil, nil, errNotSupported
	}
	return scores, labels, nil
}

func (b *faissBinaryIndex) setDirectMap(directMapType int) error {
	return b.binary.SetDirectMap(directMapType)
}

func (b *faissBinaryIndex) setNProbe(nprobe int32) {
	b.binary.SetNProbe(nprobe)
}

func (b *faissBinaryIndex) trainAndAdd(trainingData *vectorSet, vecsToAdd *vectorSet) error {
	err := b.binary.Train(trainingData.binaryData)
	if err != nil {
		return err
	}
	return b.add(vecsToAdd)
}

func (b *faissBinaryIndex) setQuantizers(centroidIndex faissIndexIVF) error {
	// not supported for binary indexes, return error
	return errNotSupported
}

func (b *faissBinaryIndex) mergeFrom(other faissIndex, offset int64) error {
	// merging is not supported for binary indexes, return error
	return errNotSupported
}
