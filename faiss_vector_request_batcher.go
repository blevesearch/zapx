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
	"time"
)

var (
	// adaptive batching parameters - these can be tuned based on the expected workload and latency requirements.

	// MinLatencyBudget is the minimum amount of time that the batcher will wait before flushing a batch of requests.
	MinLatencyBudget time.Duration = 10 * time.Millisecond
	// MaxLatencyBudget is the maximum amount of time that the batcher will wait before flushing a batch of requests.
	MaxLatencyBudget time.Duration = 250 * time.Millisecond
)

var (
	errBatcherStopped error = errors.New("batcher has been stopped")
)

// The requestBatcher is responsible for batching search requests to a Faiss index.
// It will accumulate incoming search requests and execute them in batches to improve performance.
// The batcher will use the provided Faiss index to perform the searches, and it
// will manage the batching logic, including timing and concurrency control.
type requestBatcher struct {
	// the coalesce queue that manages the batching of incoming search requests.
	cq *coalesceQueue
}

func newRequestBatcher(idx faissIndexBatch) *requestBatcher {
	b := &requestBatcher{
		cq: newCoalesceQueue(idx),
	}
	return b
}

func (b *requestBatcher) search(qVector *vectorSet, k int64) ([]float32, []int64, error) {
	// create a new batch request for this search query.
	req, respCh := newBatchRequest(qVector, k)
	// check if the batcher has been stopped before processing the search request.
	select {
	case b.cq.enqueueCh <- req:
	case <-b.cq.stopCh:
		return nil, nil, errBatcherStopped
	}
	// wait for the search results to be sent back through the response channel,
	// and return those results to the caller.
	resp := <-respCh
	return resp.distances, resp.ids, resp.err
}

func (b *requestBatcher) stop() {
	b.cq.stop()
}

// --------------------------------------------------
// batch request
// --------------------------------------------------

type batchRequest struct {
	qVector *vectorSet
	k       int64
	respCh  []chan *batchResponse
}

func newBatchRequest(qVector *vectorSet, k int64) (*batchRequest, chan *batchResponse) {
	// response channel for sending the search results back to the requester.
	respChan := make(chan *batchResponse, 1)
	return &batchRequest{
		qVector: qVector,
		k:       k,
		respCh:  []chan *batchResponse{respChan},
	}, respChan
}

func (r *batchRequest) canMerge(other *batchRequest) bool {
	// for now, we can only merge requests that have the same k value,
	// since the Faiss search API requires a single k value for each search.
	return r.k == other.k
}

func (r *batchRequest) mergeWith(other *batchRequest) {
	if !r.canMerge(other) {
		return
	}
	// since we are merging the query vectors of two requests, we need to clone
	// the original query vector to avoid mutating the original request's query vector when we merge.
	r.qVector = r.qVector.clone()
	// merge the query vectors of the two requests by concatenating them together.
	r.qVector.mergeWith(other.qVector)
	// append the response channels from the other request to this request, so that when the search results are ready,
	// we can send the results back to all requesters that were merged into this batch.
	r.respCh = append(r.respCh, other.respCh...)
}

func (r *batchRequest) sendResponse(distances []float32, ids []int64, err error) {
	// we may have multiple batches merged together, so we need segregate the results for each original request
	// and send them back to the appropriate response channels.
	if err != nil {
		// if there was an error during the search, send the error back to all requesters in this batch.
		for _, respCh := range r.respCh {
			respCh <- newBatchResponse(nil, nil, err)
			close(respCh)
		}
		return
	}
	// if the search was successful, we need to split the combined results back into individual responses for each original request.
	for i, respCh := range r.respCh {
		offset := int64(i) * r.k
		// calculate the start and end indices for the results corresponding to this response channel.
		curDistances := distances[offset : offset+r.k]
		curIDs := ids[offset : offset+r.k]
		// send the results back to the requester through the response channel.
		respCh <- newBatchResponse(curDistances, curIDs, nil)
		// close the response channel to signal that the response has been sent and there will be no more data.
		close(respCh)
	}
}

// --------------------------------------------------
// batch response
// --------------------------------------------------

type batchResponse struct {
	distances []float32
	ids       []int64
	err       error
}

func newBatchResponse(distances []float32, ids []int64, err error) *batchResponse {
	return &batchResponse{
		distances: distances,
		ids:       ids,
		err:       err,
	}
}

// --------------------------------------------------
// coalesceQueue
// --------------------------------------------------

type coalesceQueue struct {
	// the Faiss index that this coalesce queue will execute search requests against.
	idx faissIndexBatch
	// channel for enqueuing new batch requests into the queue.
	enqueueCh chan *batchRequest
	// safeguard to ensure that the stop() method is thread-safe and can only be called once, preventing multiple close operations on the stopCh.
	stopOnce sync.Once
	// channel for signaling the batcher to stop processing requests and shut down.
	stopCh chan struct{}
	// queue of pending batch requests that are waiting to be processed.
	queue []*batchRequest
	// timer for automatically triggering the execution of a batch of requests when the earliest deadline is reached.
	timer *time.Timer
	// timestamp of the last time the queue was flushed.
	lastFlush time.Time
}

func newCoalesceQueue(idx faissIndexBatch) *coalesceQueue {
	rv := &coalesceQueue{
		idx:       idx,
		enqueueCh: make(chan *batchRequest),
		stopCh:    make(chan struct{}),
	}
	go rv.monitor()
	return rv
}

func (q *coalesceQueue) stop() {
	q.stopOnce.Do(func() {
		close(q.stopCh)
	})
}

func (q *coalesceQueue) monitor() {
	var dequeueTimer <-chan time.Time
	for {
		select {
		case req := <-q.enqueueCh:
			if len(q.queue) == 0 {
				dequeueTimer = q.startTimer()
			}
			q.enqueue(req)
		case <-dequeueTimer:
			// when the timer fires, it means that at least one batch request in the queue has
			// reached its deadline and needs to be executed.
			q.flush()
			// the queue is flushed after processing, so set the dequeue timer to nil until we have
			// new requests in the queue that can trigger a new timer.
			dequeueTimer = nil
		case <-q.stopCh:
			// flush the queue one last time before stopping, to ensure that any pending requests are processed.
			q.flush()
			dequeueTimer = nil
			return
		}
	}
}

func (q *coalesceQueue) startTimer() <-chan time.Time {
	// Calculate adaptive budget when this is the first request after a flush
	var budget time.Duration
	// empty queue.
	if !q.lastFlush.IsZero() {
		// Calculate time elapsed since last flush
		timeSinceLastFlush := time.Since(q.lastFlush)
		// Adaptive strategy:
		// - If requests arrive quickly (small timeSinceLastFlush), use higher budget to batch more
		// - If requests arrive slowly (large timeSinceLastFlush), use lower budget to reduce latency
		budget = max(MaxLatencyBudget-timeSinceLastFlush, MinLatencyBudget)
	} else {
		// First request ever, use maximum budget
		budget = MaxLatencyBudget
	}
	if q.timer != nil {
		q.timer.Reset(budget)
	} else {
		q.timer = time.NewTimer(budget)
	}
	return q.timer.C
}

func (q *coalesceQueue) enqueue(req *batchRequest) {
	// Try to find an existing batch request in the queue that can be merged with this new request.
	for _, pendingReq := range q.queue {
		if pendingReq.canMerge(req) {
			// if we find a compatible request, merge this new request into the existing batch request and return.
			pendingReq.mergeWith(req)
			return
		}
	}
	// if we didn't find any compatible request to merge with, add this new request to the end of the queue.
	q.queue = append(q.queue, req)
}

func (q *coalesceQueue) flush() {
	// execute all the requests in the queue, and empty the queue afterwards.
	for _, req := range q.queue {
		distances, ids, err := q.idx.batchSearch(req.qVector, req.k)
		req.sendResponse(distances, ids, err)
	}
	// update the last flush time to now, since we just processed a batch of requests.
	q.lastFlush = time.Now()
	if q.timer != nil {
		q.timer.Stop()
	}
	// clear the queue after processing all the requests.
	q.queue = q.queue[:0]
}
