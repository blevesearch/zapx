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
	cpuIdx  *faiss.IndexImpl
	gpuIdx  *faiss.GPUIndexImpl
	batcher *requestBatcher
}

// if cloning to GPU fails, we still allow searches to happen on CPU only
func newFaissGPUFloat32Index(cpuIdx *faiss.IndexImpl) (faissIndex, error) {
	if cpuIdx == nil {
		return nil, ErrNilIndex
	}
	gpuIdx, _ := faiss.CloneToGPU(cpuIdx)
	f := &faissGPUFloat32Index{
		cpuIdx: cpuIdx,
		gpuIdx: gpuIdx,
	}
	if gpuIdx != nil {
		f.batcher = newRequestBatcher(f)
	}
	return f, nil
}

// attempt to add the vectors to the GPU index. If it fails,
// fallback to the CPU index
func (f *faissGPUFloat32Index) add(vecs *vectorSet) error {
	if f.gpuIdx == nil {
		return f.cpuIdx.Add(vecs.floatData)
	}

	err := f.gpuIdx.Add(vecs.floatData)
	if err != nil {
		f.gpuIdx.Close()
		f.gpuIdx = nil
		f.stopBatcher()
		return f.cpuIdx.Add(vecs.floatData)
	}

	err = f.syncGPUToCPU()
	if err != nil {
		f.gpuIdx.Close()
		f.gpuIdx = nil
		f.stopBatcher()
		return f.cpuIdx.Add(vecs.floatData)
	}

	return nil
}

func (f *faissGPUFloat32Index) close() {
	f.stopBatcher()
	if f.gpuIdx != nil {
		f.gpuIdx.Close()
	}
	f.cpuIdx.Close()
}

func (f *faissGPUFloat32Index) stopBatcher() {
	if f.batcher != nil {
		f.batcher.stop()
		f.batcher = nil
	}
}

func (f *faissGPUFloat32Index) dim() int {
	return f.cpuIdx.D()
}

func (f *faissGPUFloat32Index) metricType() int {
	return f.cpuIdx.MetricType()
}

func (f *faissGPUFloat32Index) searchWithoutIDs(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	if f.batcher != nil && selector == nil && len(params) == 0 {
		// no selector and no params, use the batcher to batch GPU searches
		return f.batcher.search(qVector, k)
	}
	// fall back to CPU for filtered search since GPU does not support selectors
	return f.cpuIdx.SearchWithoutIDs(qVector.floatData, k, selector, params)
}

func (f *faissGPUFloat32Index) batchSearch(qVector *vectorSet, k int64) ([]float32, []int64, error) {
	return f.gpuIdx.Search(qVector.floatData, k)
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

// attempt to train and add the vectors to the GPU index. If it fails,
// fallback to the CPU index
func (f *faissGPUFloat32Index) trainAndAdd(trainingData *vectorSet, vecsToAdd *vectorSet) error {
	if f.gpuIdx == nil {
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err := f.gpuIdx.Train(trainingData.floatData)
	if err != nil {
		f.gpuIdx.Close()
		f.gpuIdx = nil
		f.stopBatcher()
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err = f.gpuIdx.Add(trainingData.floatData)
	if err != nil {
		f.gpuIdx.Close()
		f.gpuIdx = nil
		f.stopBatcher()
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err = f.syncGPUToCPU()
	if err != nil {
		f.gpuIdx.Close()
		f.gpuIdx = nil
		f.stopBatcher()
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	return nil
}

// this function trains and adds the vectors on the CPU index only
func (f *faissGPUFloat32Index) trainAndAddCPU(trainingData *vectorSet, vecsToAdd *vectorSet) error {
	err := f.cpuIdx.Train(trainingData.floatData)
	if err != nil {
		return err
	}

	return f.cpuIdx.Add(vecsToAdd.floatData)
}

// syncGPUToCPU clones the current GPU index state back to the CPU index,
// replacing the old CPU index.
func (f *faissGPUFloat32Index) syncGPUToCPU() error {
	if f.gpuIdx == nil {
		return nil
	}

	cpuIdx, err := faiss.CloneToCPU(f.gpuIdx)
	if err != nil {
		return err
	}

	oldCPUIdx := f.cpuIdx
	f.cpuIdx = cpuIdx
	oldCPUIdx.Close()
	return nil
}
