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
	"sync"
	"time"

	faiss "github.com/blevesearch/go-faiss"
)

const (
	DefaultBatchExecutionDelay = time.Duration(100 * time.Millisecond)
	DefaultMaxBatchSize        = 512
)

type GPUIndexSearchCallbackFunc func(qVector []float32, k int64) ([]float32, []int64, error)

// ----------------------------------------------------------------
// GPU Index
// ----------------------------------------------------------------

type requestBatch struct {
	dims      int
	vectors   []float32
	k         []int64
	respChans []chan *gpuResponse
}

func newRequestBatch(dims int) *requestBatch {
	return &requestBatch{
		dims:      dims,
		vectors:   make([]float32, 0, DefaultMaxBatchSize*dims),
		k:         make([]int64, 0, DefaultMaxBatchSize),
		respChans: make([]chan *gpuResponse, 0, DefaultMaxBatchSize),
	}
}

func (r *requestBatch) add(vector []float32, k int64, respChan chan *gpuResponse) {
	r.vectors = append(r.vectors, vector...)
	r.k = append(r.k, k)
	r.respChans = append(r.respChans, respChan)
}

func (r *requestBatch) reset() {
	r.vectors = r.vectors[:0]
	r.k = r.k[:0]
	r.respChans = r.respChans[:0]
}

func (r *requestBatch) count() int {
	return len(r.k)
}

type gpuRequest struct {
	vector   []float32
	k        int64
	respChan chan *gpuResponse
}

type gpuResponse struct {
	distances []float32
	labels    []int64
	err       error
}

type batchWorker struct {
	batch    *requestBatch
	ticker   *time.Ticker
	searchFn GPUIndexSearchCallbackFunc
	reqChan  chan *gpuRequest
	closeCh  chan struct{}
	doneCh   chan struct{}
	mu       sync.Mutex
	pending  []*gpuRequest
}

func newBatchWorker(dims int, searchFn GPUIndexSearchCallbackFunc) *batchWorker {
	return &batchWorker{
		batch:    newRequestBatch(dims),
		ticker:   time.NewTicker(DefaultBatchExecutionDelay),
		searchFn: searchFn,
		reqChan:  make(chan *gpuRequest, DefaultMaxBatchSize),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
		pending:  make([]*gpuRequest, 0, DefaultMaxBatchSize),
	}
}

func (b *batchWorker) monitor() {
	for {
		select {
		case <-b.ticker.C:
			b.executeBatch()
		case req := <-b.reqChan:
			b.mu.Lock()
			b.pending = append(b.pending, req)
			shouldExecute := len(b.pending) >= DefaultMaxBatchSize
			b.mu.Unlock()
			if shouldExecute {
				b.executeBatch()
			}
		case <-b.closeCh:
			// Flush pending batch and exit
			b.executeBatch()
			b.ticker.Stop()
			close(b.doneCh)
			return
		}
	}
}

func (b *batchWorker) executeBatch() {
	b.mu.Lock()
	if len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	b.batch.reset()
	maxK := int64(0)
	for _, req := range b.pending {
		b.batch.add(req.vector, req.k, req.respChan)
		if req.k > maxK {
			maxK = req.k
		}
	}
	b.pending = b.pending[:0]
	b.mu.Unlock()
	distances, labels, err := b.searchFn(b.batch.vectors, maxK)
	var distOffset, labelOffset int64
	for i := 0; i < b.batch.count(); i++ {
		k := b.batch.k[i]
		respDistances := distances[distOffset : distOffset+k]
		respLabels := labels[labelOffset : labelOffset+k]
		b.batch.respChans[i] <- &gpuResponse{
			distances: respDistances,
			labels:    respLabels,
			err:       err,
		}
		distOffset += maxK
		labelOffset += maxK
	}
}

type GPUIndex struct {
	gpuIndex *faiss.GPUIndexImpl
	worker   *batchWorker
}

func NewGPUIndex(cpuIndex *faiss.IndexImpl) (*GPUIndex, error) {
	gpuIndex, err := faiss.CloneToGPU(cpuIndex)
	if err != nil {
		return nil, err
	}
	dims := gpuIndex.D()
	rv := &GPUIndex{
		gpuIndex: gpuIndex,
		worker:   newBatchWorker(dims, gpuIndex.Search),
	}
	go rv.worker.monitor()
	return rv, nil
}

func (g *GPUIndex) Search(qVector []float32, k int64) ([]float32, []int64, error) {
	respChan := make(chan *gpuResponse, 1)
	req := &gpuRequest{
		vector:   qVector,
		k:        k,
		respChan: respChan,
	}
	g.worker.reqChan <- req
	resp := <-respChan
	return resp.distances, resp.labels, resp.err
}

func (g *GPUIndex) Close() {
	// Signal worker to flush pending and exit
	close(g.worker.closeCh)
	// Wait for worker goroutine to complete
	<-g.worker.doneCh
	// Safe to close GPU index
	g.gpuIndex.Close()
}

func (g *GPUIndex) Size() uint64 {
	return g.gpuIndex.Size()
}
