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
	"errors"
	"sync"
	"testing"
)

// mock faissQueryBatch implementation
type mockFaissIndex struct {
	mu          sync.Mutex
	searchFunc  func(qVector *vectorSet, k int64) ([]float32, []int64, error)
	searchCalls int
}

func (m *mockFaissIndex) batchSearch(qVector *vectorSet, k int64) ([]float32, []int64, error) {
	m.mu.Lock()
	m.searchCalls++
	m.mu.Unlock()
	if m.searchFunc != nil {
		return m.searchFunc(qVector, k)
	}
	distances := make([]float32, int64(qVector.nvecs)*k)
	ids := make([]int64, int64(qVector.nvecs)*k)
	for i := 0; i < qVector.nvecs; i++ {
		for j := int64(0); j < k; j++ {
			offset := int64(i)*k + j
			distances[offset] = float32(i)*10 + float32(j)
			ids[offset] = int64(i)*100 + j
		}
	}
	return distances, ids, nil
}

func makeVectorSet(dim int, nvecs int) *vectorSet {
	data := make([]float32, dim*nvecs)
	for i := range data {
		data[i] = float32(i)
	}
	vs, _ := newVectorSet(dim, data)
	return vs
}

// --- requestBatcher tests ---

func TestNewRequestBatcher(t *testing.T) {
	idx := &mockFaissIndex{}
	b := newRequestBatcher(idx)
	if b == nil {
		t.Fatal("expected non-nil batcher")
	}
	if b.cq == nil {
		t.Fatal("expected non-nil coalesce queue")
	}
	b.stop()
}

func TestRequestBatcherSearch(t *testing.T) {
	idx := &mockFaissIndex{}
	b := newRequestBatcher(idx)
	defer b.stop()

	qv := makeVectorSet(3, 1)
	distances, ids, err := b.search(qv, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(distances) != 2 {
		t.Fatalf("expected 2 distances, got %d", len(distances))
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
}

func TestRequestBatcherSearchAfterStop(t *testing.T) {
	idx := &mockFaissIndex{}
	b := newRequestBatcher(idx)
	b.stop()

	qv := makeVectorSet(3, 1)
	_, _, err := b.search(qv, 2)
	if !errors.Is(err, errBatcherStopped) {
		t.Fatalf("expected errBatcherStopped, got %v", err)
	}
}

func TestRequestBatcherConcurrentSearches(t *testing.T) {
	idx := &mockFaissIndex{}
	b := newRequestBatcher(idx)
	defer b.stop()

	var wg sync.WaitGroup
	numSearches := 50
	wg.Add(numSearches)
	errs := make([]error, numSearches)
	for i := 0; i < numSearches; i++ {
		go func(idx int) {
			defer wg.Done()
			qv := makeVectorSet(3, 1)
			dists, ids, err := b.search(qv, 2)
			if err != nil {
				errs[idx] = err
				return
			}
			if len(dists) != 2 || len(ids) != 2 {
				errs[idx] = errors.New("unexpected result length")
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("search %d failed: %v", i, err)
		}
	}
}

func TestRequestBatcherConcurrentSearchesDifferentK(t *testing.T) {
	idx := &mockFaissIndex{}
	b := newRequestBatcher(idx)
	defer b.stop()

	var wg sync.WaitGroup
	numSearches := 20
	wg.Add(numSearches)
	errs := make([]error, numSearches)
	for i := 0; i < numSearches; i++ {
		go func(idx int) {
			defer wg.Done()
			k := int64(idx%3) + 1
			qv := makeVectorSet(3, 1)
			dists, ids, err := b.search(qv, k)
			if err != nil {
				errs[idx] = err
				return
			}
			if int64(len(dists)) != k || int64(len(ids)) != k {
				errs[idx] = errors.New("unexpected result length for k")
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("search %d failed: %v", i, err)
		}
	}
}

// --- batchRequest tests ---

func TestNewBatchRequest(t *testing.T) {
	qv := makeVectorSet(3, 1)
	req, respCh := newBatchRequest(qv, 5)
	if req == nil {
		t.Fatal("expected non-nil request")
	}
	if req.k != 5 {
		t.Fatalf("expected k=5, got %d", req.k)
	}
	if req.qVector != qv {
		t.Fatal("expected same qVector reference")
	}
	if len(req.respCh) != 1 {
		t.Fatalf("expected 1 response channel, got %d", len(req.respCh))
	}
	if respCh == nil {
		t.Fatal("expected non-nil response channel")
	}
}

func TestBatchRequestCanMerge(t *testing.T) {
	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)

	r1, _ := newBatchRequest(qv1, 5)
	r2, _ := newBatchRequest(qv2, 5)
	r3, _ := newBatchRequest(qv2, 10)

	if !r1.canMerge(r2) {
		t.Fatal("expected requests with same k to be mergeable")
	}
	if r1.canMerge(r3) {
		t.Fatal("expected requests with different k to not be mergeable")
	}
}

func TestBatchRequestMergeWith(t *testing.T) {
	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)

	r1, ch1 := newBatchRequest(qv1, 5)
	r2, ch2 := newBatchRequest(qv2, 5)
	r1.mergeWith(r2)

	if r1.qVector.nvecs != 2 {
		t.Fatalf("expected 2 vectors after merge, got %d", r1.qVector.nvecs)
	}
	if len(r1.respCh) != 2 {
		t.Fatalf("expected 2 response channels after merge, got %d", len(r1.respCh))
	}
	if r1.respCh[0] != ch1 || r1.respCh[1] != ch2 {
		t.Fatal("response channels not correctly merged")
	}
}

func TestBatchRequestSendResponseSuccess(t *testing.T) {
	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)

	r1, ch1 := newBatchRequest(qv1, 2)
	r2, ch2 := newBatchRequest(qv2, 2)
	r1.mergeWith(r2)

	distances := []float32{1.0, 2.0, 3.0, 4.0}
	ids := []int64{10, 20, 30, 40}
	r1.sendResponse(distances, ids, nil)

	resp1 := <-ch1
	if resp1.err != nil {
		t.Fatalf("unexpected error: %v", resp1.err)
	}
	if len(resp1.distances) != 2 || resp1.distances[0] != 1.0 || resp1.distances[1] != 2.0 {
		t.Fatalf("unexpected distances for resp1: %v", resp1.distances)
	}
	if len(resp1.ids) != 2 || resp1.ids[0] != 10 || resp1.ids[1] != 20 {
		t.Fatalf("unexpected ids for resp1: %v", resp1.ids)
	}

	resp2 := <-ch2
	if resp2.err != nil {
		t.Fatalf("unexpected error: %v", resp2.err)
	}
	if len(resp2.distances) != 2 || resp2.distances[0] != 3.0 || resp2.distances[1] != 4.0 {
		t.Fatalf("unexpected distances for resp2: %v", resp2.distances)
	}
	if len(resp2.ids) != 2 || resp2.ids[0] != 30 || resp2.ids[1] != 40 {
		t.Fatalf("unexpected ids for resp2: %v", resp2.ids)
	}
}

func TestBatchRequestSendResponseError(t *testing.T) {
	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)

	r1, ch1 := newBatchRequest(qv1, 2)
	r2, ch2 := newBatchRequest(qv2, 2)
	r1.mergeWith(r2)

	testErr := errors.New("search failed")
	r1.sendResponse(nil, nil, testErr)

	resp1 := <-ch1
	if !errors.Is(resp1.err, testErr) {
		t.Fatalf("expected testErr, got %v", resp1.err)
	}
	resp2 := <-ch2
	if !errors.Is(resp2.err, testErr) {
		t.Fatalf("expected testErr, got %v", resp2.err)
	}
}

func TestBatchRequestSendResponseSingleRequest(t *testing.T) {
	qv := makeVectorSet(3, 1)
	req, ch := newBatchRequest(qv, 3)

	distances := []float32{1.0, 2.0, 3.0}
	ids := []int64{10, 20, 30}
	req.sendResponse(distances, ids, nil)

	resp := <-ch
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if len(resp.distances) != 3 {
		t.Fatalf("expected 3 distances, got %d", len(resp.distances))
	}
	if len(resp.ids) != 3 {
		t.Fatalf("expected 3 ids, got %d", len(resp.ids))
	}
}

// --- batchResponse tests ---

func TestNewBatchResponse(t *testing.T) {
	distances := []float32{1.0, 2.0}
	ids := []int64{10, 20}
	resp := newBatchResponse(distances, ids, nil)
	if resp.err != nil {
		t.Fatalf("expected nil error, got %v", resp.err)
	}
	if len(resp.distances) != 2 {
		t.Fatalf("expected 2 distances, got %d", len(resp.distances))
	}
	if len(resp.ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(resp.ids))
	}

	testErr := errors.New("fail")
	respErr := newBatchResponse(nil, nil, testErr)
	if !errors.Is(respErr.err, testErr) {
		t.Fatalf("expected error, got %v", respErr.err)
	}
}

// --- batchManager tests ---

func TestBatchManagerGetPut(t *testing.T) {
	bm := newBatchManager()
	batch := bm.getBatch()
	if batch == nil {
		t.Fatal("expected non-nil batch")
	}
	if len(batch) != 0 {
		t.Fatalf("expected empty batch, got len %d", len(batch))
	}
	if cap(batch) < 16 {
		t.Fatalf("expected capacity >= 16, got %d", cap(batch))
	}

	qv := makeVectorSet(3, 1)
	req, _ := newBatchRequest(qv, 2)
	batch = append(batch, req)
	bm.putBatch(batch)

	batch2 := bm.getBatch()
	if len(batch2) != 0 {
		t.Fatalf("expected empty batch after get, got len %d", len(batch2))
	}
}

// --- coalesceQueue tests ---

func TestCoalesceQueueBasic(t *testing.T) {
	idx := &mockFaissIndex{}
	q := newCoalesceQueue(idx)
	defer q.stop()

	qv := makeVectorSet(3, 1)
	req, ch := newBatchRequest(qv, 2)
	q.enqueueCh <- req
	resp := <-ch
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if len(resp.distances) != 2 {
		t.Fatalf("expected 2 distances, got %d", len(resp.distances))
	}
}

func TestCoalesceQueueStopIdempotent(t *testing.T) {
	idx := &mockFaissIndex{}
	q := newCoalesceQueue(idx)
	q.stop()
	q.stop() // should not panic
}

func TestCoalesceQueueCoalescesMergeable(t *testing.T) {
	idx := &mockFaissIndex{}
	bm := newBatchManager()
	q := &coalesceQueue{
		idx:          idx,
		batchManager: bm,
	}

	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)
	r1, _ := newBatchRequest(qv1, 5)
	r2, _ := newBatchRequest(qv2, 5)

	// clone r1's vector as coalesce would
	r1.qVector = r1.qVector.clone()
	batch := bm.getBatch()
	batch = append(batch, r1)

	result := q.coalesce(batch, r2)
	if len(result) != 1 {
		t.Fatalf("expected 1 coalesced entry, got %d", len(result))
	}
	if result[0].qVector.nvecs != 2 {
		t.Fatalf("expected 2 vectors after coalesce, got %d", result[0].qVector.nvecs)
	}
}

func TestCoalesceQueueCoalesceNotMergeable(t *testing.T) {
	idx := &mockFaissIndex{}
	bm := newBatchManager()
	q := &coalesceQueue{
		idx:          idx,
		batchManager: bm,
	}

	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)
	r1, _ := newBatchRequest(qv1, 5)
	r2, _ := newBatchRequest(qv2, 10)

	r1.qVector = r1.qVector.clone()
	batch := bm.getBatch()
	batch = append(batch, r1)

	result := q.coalesce(batch, r2)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries when not mergeable, got %d", len(result))
	}
}

func TestCoalesceQueueCoalesceNilQueue(t *testing.T) {
	idx := &mockFaissIndex{}
	bm := newBatchManager()
	q := &coalesceQueue{
		idx:          idx,
		batchManager: bm,
	}

	qv := makeVectorSet(3, 1)
	r, _ := newBatchRequest(qv, 5)

	result := q.coalesce(nil, r)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
}

func TestCoalesceQueueExecuteBatch(t *testing.T) {
	idx := &mockFaissIndex{}
	bm := newBatchManager()
	q := &coalesceQueue{
		idx:          idx,
		batchManager: bm,
	}

	qv := makeVectorSet(3, 1)
	req, ch := newBatchRequest(qv, 2)
	batch := bm.getBatch()
	batch = append(batch, req)
	q.executeBatch(batch)

	resp := <-ch
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}
	if len(resp.distances) != 2 {
		t.Fatalf("expected 2 distances, got %d", len(resp.distances))
	}
}

func TestCoalesceQueueExecuteBatchWithError(t *testing.T) {
	testErr := errors.New("index error")
	idx := &mockFaissIndex{
		searchFunc: func(qVector *vectorSet, k int64) ([]float32, []int64, error) {
			return nil, nil, testErr
		},
	}
	bm := newBatchManager()
	q := &coalesceQueue{
		idx:          idx,
		batchManager: bm,
	}

	qv := makeVectorSet(3, 1)
	req, ch := newBatchRequest(qv, 2)
	batch := bm.getBatch()
	batch = append(batch, req)
	q.executeBatch(batch)

	resp := <-ch
	if !errors.Is(resp.err, testErr) {
		t.Fatalf("expected testErr, got %v", resp.err)
	}
}

func TestCoalesceQueueStopWithPendingRequests(t *testing.T) {
	idx := &mockFaissIndex{}
	q := newCoalesceQueue(idx)

	qv := makeVectorSet(3, 1)
	req, ch := newBatchRequest(qv, 2)
	q.enqueueCh <- req

	resp := <-ch
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}

	q.stop()
}

func TestSearchWithErrorFromIndex(t *testing.T) {
	testErr := errors.New("faiss error")
	idx := &mockFaissIndex{
		searchFunc: func(qVector *vectorSet, k int64) ([]float32, []int64, error) {
			return nil, nil, testErr
		},
	}
	b := newRequestBatcher(idx)
	defer b.stop()

	qv := makeVectorSet(3, 1)
	_, _, err := b.search(qv, 2)
	if !errors.Is(err, testErr) {
		t.Fatalf("expected faiss error, got %v", err)
	}
}

func TestErrBatcherStopped(t *testing.T) {
	if errBatcherStopped.Error() != "batcher has been stopped" {
		t.Fatalf("unexpected error message: %s", errBatcherStopped.Error())
	}
}

func TestCoalesceQueueExecuteBatchMultipleRequests(t *testing.T) {
	idx := &mockFaissIndex{}
	bm := newBatchManager()
	q := &coalesceQueue{
		idx:          idx,
		batchManager: bm,
	}

	qv1 := makeVectorSet(3, 1)
	qv2 := makeVectorSet(3, 1)
	req1, ch1 := newBatchRequest(qv1, 2)
	req2, ch2 := newBatchRequest(qv2, 3)

	batch := bm.getBatch()
	batch = append(batch, req1, req2)
	q.executeBatch(batch)

	resp1 := <-ch1
	if resp1.err != nil {
		t.Fatalf("unexpected error for req1: %v", resp1.err)
	}
	if len(resp1.distances) != 2 {
		t.Fatalf("expected 2 distances for req1, got %d", len(resp1.distances))
	}

	resp2 := <-ch2
	if resp2.err != nil {
		t.Fatalf("unexpected error for req2: %v", resp2.err)
	}
	if len(resp2.distances) != 3 {
		t.Fatalf("expected 3 distances for req2, got %d", len(resp2.distances))
	}
}

func TestFillerDrainsOnStop(t *testing.T) {
	blockCh := make(chan struct{})
	idx := &mockFaissIndex{
		searchFunc: func(qVector *vectorSet, k int64) ([]float32, []int64, error) {
			<-blockCh
			distances := make([]float32, int64(qVector.nvecs)*k)
			ids := make([]int64, int64(qVector.nvecs)*k)
			return distances, ids, nil
		},
	}
	b := newRequestBatcher(idx)

	qv1 := makeVectorSet(3, 1)
	req1, ch1 := newBatchRequest(qv1, 2)
	b.cq.enqueueCh <- req1

	qv2 := makeVectorSet(3, 1)
	req2, ch2 := newBatchRequest(qv2, 2)
	b.cq.enqueueCh <- req2

	close(blockCh)

	go b.stop()

	resp1 := <-ch1
	if resp1.err != nil {
		t.Fatalf("unexpected error: %v", resp1.err)
	}
	resp2 := <-ch2
	if resp2.err != nil {
		t.Fatalf("unexpected error: %v", resp2.err)
	}
}
