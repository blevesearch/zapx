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

	// doneCh is closed when initGPU completes.
	doneCh chan struct{}

	// mu is only used to coordinate the async initGPU with searchWithoutIDs,
	// which may read gpuIdx/batcher before initGPU completes.
	mu      sync.RWMutex
	gpuIdx  *faiss.GPUIndexImpl
	batcher *requestBatcher
}

// newFaissGPUFloat32Index creates a GPU-backed float32 index. The GPU clone is
// always performed asynchronously; searchWithoutIDs falls back to CPU until it
// completes. All other GPU-operating methods block on doneCh before proceeding.
func newFaissGPUFloat32Index(cpuIdx *faiss.IndexImpl) (faissIndex, error) {
	if cpuIdx == nil {
		return nil, ErrNilIndex
	}
	f := &faissGPUFloat32Index{
		cpuIdx: cpuIdx,
		doneCh: make(chan struct{}),
	}
	go f.initGPU()
	return f, nil
}

// waitGPU blocks until initGPU has completed. Safe to call multiple times
// since reading from a closed channel returns immediately.
func (f *faissGPUFloat32Index) waitGPU() {
	<-f.doneCh
}

// initGPU clones the CPU index to the GPU and sets up the request batcher.
// It always closes doneCh when it returns, signalling completion to waiters.
func (f *faissGPUFloat32Index) initGPU() {
	defer close(f.doneCh)
	gpuIdx, err := faiss.CloneToGPU(f.cpuIdx)
	if err != nil || gpuIdx == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gpuIdx = gpuIdx
	f.batcher = newRequestBatcher(f)
}

// attempt to add the vectors to the GPU index. If it fails,
// fallback to the CPU index
func (f *faissGPUFloat32Index) add(vecs *vectorSet) error {
	f.waitGPU()
	if f.gpuIdx == nil {
		return f.cpuIdx.Add(vecs.floatData)
	}

	err := f.gpuIdx.Add(vecs.floatData)
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
	f.waitGPU()
	f.teardownGPU()
	f.cpuIdx.Close()
}

// teardownGPU stops the batcher first (while gpuIdx is still live so that
// the final flush can complete on the GPU), then nils and closes the GPU index.
func (f *faissGPUFloat32Index) teardownGPU() {
	f.waitGPU()
	batcher := f.batcher
	f.batcher = nil
	if batcher != nil {
		batcher.stop()
	}

	gpuIdx := f.gpuIdx
	f.gpuIdx = nil
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
		if f.batcher != nil && f.gpuIdx != nil {
			distances, ids, err := f.batcher.search(qVector, k)
			f.mu.RUnlock()
			return distances, ids, err
		}
		f.mu.RUnlock()
	}
	// GPU not ready, filtered search, or non-empty params — fall back to CPU
	return f.cpuIdx.SearchWithoutIDs(qVector.floatData, k, selector, params)
}

// batchSearch is called from the coalesce queue's monitor goroutine.
// The caller (searchWithoutIDs) holds the RLock for the duration, guaranteeing
// gpuIdx is non-nil and cannot be torn down while this runs.
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
	f.waitGPU()
	if f.gpuIdx != nil {
		f.gpuIdx.SetNProbe(nprobe)
	}
}

// attempt to train and add the vectors to the GPU index. If it fails,
// fallback to the CPU index
func (f *faissGPUFloat32Index) trainAndAdd(trainingData *vectorSet, vecsToAdd *vectorSet) error {
	f.waitGPU()
	if f.gpuIdx == nil {
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err := f.gpuIdx.Train(trainingData.floatData)
	if err != nil {
		f.teardownGPU()
		return f.trainAndAddCPU(trainingData, vecsToAdd)
	}

	err = f.gpuIdx.Add(vecsToAdd.floatData)
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
