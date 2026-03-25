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
// Faiss GPU Float32 Index
// ---------------------------------
// faissGPUFloat32Index wraps a CPU float32 index alongside a GPU index.
// The GPU is used for unfiltered searches (no selector), while all
// other operations (filtered searches, IVF cluster searches, SQ/IVF
// operations, serialization, etc.) are delegated to the CPU index.
type faissGPUFloat32Index struct {
	cpuIdx *faiss.IndexImpl
	gpuIdx *faiss.GPUIndexImpl
}

func newFaissGPUFloat32Index(cpuIdx *faiss.IndexImpl) (faissIndex, error) {
	if cpuIdx == nil {
		return nil, ErrNilIndex
	}
	gpuIdx, err := faiss.CloneToGPU(cpuIdx)
	if err != nil {
		return nil, err
	}
	return &faissGPUFloat32Index{
		cpuIdx: cpuIdx,
		gpuIdx: gpuIdx,
	}, nil
}

func (f *faissGPUFloat32Index) add(vecs *vectorSet) error {
	err := f.gpuIdx.Add(vecs.floatData)
	if err != nil {
		return err
	}
	return f.syncGPUToCPU()
}

func (f *faissGPUFloat32Index) close() {
	f.gpuIdx.Close()
	f.cpuIdx.Close()
}

func (f *faissGPUFloat32Index) dim() int {
	return f.cpuIdx.D()
}

func (f *faissGPUFloat32Index) metricType() int {
	return f.cpuIdx.MetricType()
}

func (f *faissGPUFloat32Index) searchWithoutIDs(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	if selector == nil && len(params) == 0 {
		// no selector and no params, use GPU for unfiltered search
		return f.gpuIdx.Search(qVector.floatData, k)
	}
	// fall back to CPU for filtered search since GPU does not support selectors
	return f.cpuIdx.SearchWithoutIDs(qVector.floatData, k, selector, params)
}

func (f *faissGPUFloat32Index) searchWithIDs(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	// GPU does not support selector-based search, use CPU
	return f.cpuIdx.SearchWithIDs(qVector.floatData, k, selector, params)
}

func (f *faissGPUFloat32Index) serialize() ([]byte, error) {
	return faiss.WriteIndexIntoBuffer(f.cpuIdx)
}

func (f *faissGPUFloat32Index) size() uint64 {
	return f.cpuIdx.Size()
}

// -----------------------------------------------------------------
// casting methods to access index-specific operations below
// -----------------------------------------------------------------
func (f *faissGPUFloat32Index) castIVF() faissIndexIVF {
	if f.cpuIdx.IsIVFIndex() {
		return f
	}
	return nil
}

func (f *faissGPUFloat32Index) castSQ() faissIndexSQ {
	if f.cpuIdx.IsSQIndex() {
		return f
	}
	return nil
}

// -----------------------------------------------------------------
// IVF-Index specific operations (delegate to CPU index)
// -----------------------------------------------------------------
func (f *faissGPUFloat32Index) clusterVectorCounts(sel faiss.Selector, nlist int) ([]int64, error) {
	return f.cpuIdx.ObtainClusterVectorCountsFromIVFIndex(sel, nlist)
}

func (f *faissGPUFloat32Index) centroidCardinalities(limit int, descending bool) ([]uint64, [][]float32, error) {
	return f.cpuIdx.ObtainKCentroidCardinalitiesFromIVFIndex(limit, descending)
}

func (f *faissGPUFloat32Index) ivfParams() (nprobe, nlist int) {
	return f.cpuIdx.IVFParams()
}

func (f *faissGPUFloat32Index) searchQuantizer(qVector *vectorSet, centroidSelector faiss.Selector, centroidCount int64) ([]int64, []float32, error) {
	return f.cpuIdx.ObtainClustersWithDistancesFromIVFIndex(qVector.floatData, centroidSelector, centroidCount)
}

func (f *faissGPUFloat32Index) searchClusters(eligibleCentroidIDs []int64, centroidDis []float32,
	centroidsToProbe int, qVecSet *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	return f.cpuIdx.SearchClustersFromIVFIndex(eligibleCentroidIDs, centroidDis, centroidsToProbe, qVecSet.floatData, k, selector, params)
}

func (f *faissGPUFloat32Index) setDirectMap(directMapType int) error {
	return f.cpuIdx.SetDirectMap(directMapType)
}

func (f *faissGPUFloat32Index) setNProbe(nprobe int32) {
	f.cpuIdx.SetNProbe(nprobe)
}

func (f *faissGPUFloat32Index) train(trainingData *vectorSet) error {
	err := f.gpuIdx.Train(trainingData.floatData)
	if err != nil {
		return err
	}
	return f.syncGPUToCPU()
}

// syncGPUToCPU clones the current GPU index state back to the CPU index,
// replacing the old CPU index.
func (f *faissGPUFloat32Index) syncGPUToCPU() error {
	cpuIdx, err := faiss.CloneToCPU(f.gpuIdx)
	if err != nil {
		return err
	}
	f.cpuIdx.Close()
	f.cpuIdx = cpuIdx
	return nil
}
