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
	"sync"

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

	// mu protects gpuIdx, batcher, and closed.
	mu      sync.RWMutex
	gpuIdx  *faiss.GPUIndexImpl
	batcher *requestBatcher
	closed  bool
}

// newFaissGPUFloat32Index returns immediately using CPU-only search while the
// GPU clone is performed asynchronously in the background.
func newFaissGPUFloat32Index(cpuIdx *faiss.IndexImpl) (faissIndex, error) {
	if cpuIdx == nil {
		return nil, ErrNilIndex
	}
	f := &faissGPUFloat32Index{cpuIdx: cpuIdx}
	go f.initGPU()
	return f, nil
}

// initGPU clones the CPU index to the GPU and sets up the request batcher.
// If the index has already been closed by the time the clone finishes, the
// newly created GPU index is immediately released.
func (f *faissGPUFloat32Index) initGPU() {
	gpuIdx, err := faiss.CloneToGPU(f.cpuIdx)
	if err != nil || gpuIdx == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		gpuIdx.Close()
		return
	}
	f.gpuIdx = gpuIdx
	f.batcher = newRequestBatcher(f)
}

// attempt to add the vectors to the GPU index. If it fails,
// fallback to the CPU index
func (f *faissGPUFloat32Index) add(vecs *vectorSet) error {
	f.mu.Lock()
	gpuIdx := f.gpuIdx
	f.mu.Unlock()

	if gpuIdx == nil {
		return f.cpuIdx.Add(vecs.floatData)
	}

	err := gpuIdx.Add(vecs.floatData)
	if err != nil {
		f.teardownGPU()
		return f.cpuIdx.Add(vecs.floatData)
	}

	err = f.syncGPUToCPU()
	if err != nil {
		f.teardownGPU()
		return f.cpuIdx.Add(vecs.floatData)
	}

	return nil
}

func (f *faissGPUFloat32Index) close() {
	f.mu.Lock()
	f.closed = true
	batcher := f.batcher
	f.batcher = nil
	gpuIdx := f.gpuIdx
	f.gpuIdx = nil
	f.mu.Unlock()

	// stop the batcher outside the lock — its final flush calls batchSearch
	// which needs the RLock.
	if batcher != nil {
		batcher.stop()
	}
	if gpuIdx != nil {
		gpuIdx.Close()
	}
	f.cpuIdx.Close()
}

// teardownGPU extracts and nils the GPU index and batcher under the lock,
// then stops the batcher and closes the GPU index outside the lock to
// avoid deadlocking with batchSearch's RLock.
func (f *faissGPUFloat32Index) teardownGPU() {
	f.mu.Lock()
	batcher := f.batcher
	f.batcher = nil
	gpuIdx := f.gpuIdx
	f.gpuIdx = nil
	f.mu.Unlock()

	if batcher != nil {
		batcher.stop()
	}
	if gpuIdx != nil {
		gpuIdx.Close()
	}
}

func (f *faissGPUFloat32Index) dim() int {
	return f.cpuIdx.D()
}

func (f *faissGPUFloat32Index) metricType() int {
	return f.cpuIdx.MetricType()
}

func (f *faissGPUFloat32Index) searchWithoutIDs(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	if selector == nil && len(params) == 0 {
		f.mu.RLock()
		batcher := f.batcher
		f.mu.RUnlock()
		if batcher != nil {
			return batcher.search(qVector, k)
		}
	}
	// GPU not ready or filtered search — fall back to CPU
	return f.cpuIdx.SearchWithoutIDs(qVector.floatData, k, selector, params)
}

// batchSearch is called from the coalesce queue's monitor goroutine.
// It holds the RLock for the duration of the GPU search to prevent close()
// from tearing down the GPU index while a search is in flight.
// If the GPU index was torn down, it falls back to the CPU index.
func (f *faissGPUFloat32Index) batchSearch(qVector *vectorSet, k int64) ([]float32, []int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.gpuIdx != nil {
		return f.gpuIdx.Search(qVector.floatData, k)
	}
	return f.cpuIdx.SearchWithoutIDs(qVector.floatData, k, nil, nil)
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
	f.mu.Lock()
	gpuIdx := f.gpuIdx
	f.mu.Unlock()

	if gpuIdx == nil {
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err := gpuIdx.Train(trainingData.floatData)
	if err != nil {
		f.teardownGPU()
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err = gpuIdx.Add(trainingData.floatData)
	if err != nil {
		f.teardownGPU()
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err = f.syncGPUToCPU()
	if err != nil {
		f.teardownGPU()
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
	f.mu.RLock()
	gpuIdx := f.gpuIdx
	f.mu.RUnlock()

	if gpuIdx == nil {
		return nil
	}

	cpuIdx, err := faiss.CloneToCPU(gpuIdx)
	if err != nil {
		return err
	}

	oldCPUIdx := f.cpuIdx
	f.cpuIdx = cpuIdx
	oldCPUIdx.Close()
	return nil
}
