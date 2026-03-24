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
	"time"
)

var (
	// the latency budget for processing a batch of search requests.
	// This is the maximum amount of time that the batcher will wait before
	// executing a batch of search requests.
	// - Higher the latency budget, higher the throughput of the batcher, but also higher the latency for individual search requests.
	// - Lower the latency budget, lower the throughput of the batcher, but also lower the latency for individual search requests.
	LatencyBudget time.Duration = 250 * time.Millisecond
)

var (
	errBatcherStopped error = errors.New("batcher has been stopped")
)

// The requestBatcher interface defines the contract for a component that batches search requests to a Faiss index.
type requestBatcher interface {
	// search executes a search request against the Faiss index,
	// potentially batching it with other requests for improved performance.
	search(qVector *vectorSet, k int64) ([]float32, []int64, error)
	// stop signals the batcher to stop processing requests and clean up any resources.
	stop()
}

// The requestBatcher is responsible for batching search requests to a Faiss index.
// It will accumulate incoming search requests and execute them in batches to improve performance.
// The batcher will use the provided Faiss index to perform the searches, and it
// will manage the batching logic, including timing and concurrency control.
type batcher struct {
	// the coalesce queue that manages the batching of incoming search requests.
	cq *coalesceQueue
}

func newRequestBatcher(idx faissIndex) requestBatcher {
	b := &batcher{
		cq: newCoalesceQueue(idx),
	}
	return b
}

func (b *batcher) search(qVector *vectorSet, k int64) ([]float32, []int64, error) {
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

func (b *batcher) stop() {
	b.cq.stop()
}

// --------------------------------------------------
// batch request
// --------------------------------------------------

type batchRequest struct {
	qVector  *vectorSet
	k        int64
	respCh   []chan *batchResponse
	deadline time.Time
}

func newBatchRequest(qVector *vectorSet, k int64) (*batchRequest, chan *batchResponse) {
	// response channel for sending the search results back to the requester.
	respChan := make(chan *batchResponse, 1)
	return &batchRequest{
		qVector:  qVector,
		k:        k,
		respCh:   []chan *batchResponse{respChan},
		deadline: time.Now().Add(LatencyBudget),
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
	// merge the query vectors of the two requests by concatenating them together.
	r.qVector.mergeWith(other.qVector)
	// append the response channels from the other request to this request, so that when the search results are ready,
	// we can send the results back to all requesters that were merged into this batch.
	r.respCh = append(r.respCh, other.respCh...)
	// retain the earliest deadline among the merged requests, so that the batch will be executed within the latency budget of all merged requests.
	if other.deadline.Before(r.deadline) {
		r.deadline = other.deadline
	}
}

func (r *batchRequest) sendResponse(distances []float32, ids []int64, err error) {
	// we may have multiple batches merged together, so we need segregate the results for each original request
	// and send them back to the appropriate response channels.
	for i, respCh := range r.respCh {
		offset := int64(i) * r.k
		// calculate the start and end indices for the results corresponding to this response channel.
		curDistances := distances[offset : offset+r.k]
		curIDs := ids[offset : offset+r.k]
		// send the results back to the requester through the response channel.
		respCh <- newBatchResponse(curDistances, curIDs, err)
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
	idx faissIndex
	// channel for enqueuing new batch requests into the queue.
	enqueueCh chan *batchRequest
	// channel for signaling the batcher to stop processing requests and shut down.
	stopCh chan struct{}
	// queue of pending batch requests that are waiting to be processed.
	queue []*batchRequest
	// timer for automatically triggering the execution of a batch of requests when the earliest deadline is reached.
	timer *time.Timer
}

func newCoalesceQueue(idx faissIndex) *coalesceQueue {
	rv := &coalesceQueue{
		idx:       idx,
		queue:     make([]*batchRequest, 0),
		enqueueCh: make(chan *batchRequest),
		stopCh:    make(chan struct{}),
	}
	go rv.monitor()
	return rv
}

func (q *coalesceQueue) stop() {
	close(q.stopCh)
	if q.timer != nil {
		q.timer.Stop()
	}
}

func (q *coalesceQueue) monitor() {
	var dequeueTimer <-chan time.Time
	for {
		select {
		case req := <-q.enqueueCh:
			// enqueue the new batch request into the coalesce queue.
			q.enqueue(req)
			// reset the dequeue timer after adding a new request.
			dequeueTimer = q.resetTimer()
		case <-dequeueTimer:
			// when the timer fires, it means that at least one batch request in the queue has
			// reached its deadline and needs to be executed.
			q.flush()
			// the queue is flushed after processing, so we need to reset the timer for the next batch of requests in the queue (if any).
			dequeueTimer = q.resetTimer()
		case <-q.stopCh:
			// flush the queue one last time before stopping, to ensure that any pending requests are processed.
			q.flush()
			return
		}
	}
}

func (q *coalesceQueue) enqueue(req *batchRequest) {
	// since we are adding a new request to the queue, we need to reset the timer to ensure that the
	// batch will be executed within the latency budget of all requests in the queue.
	// try to find an existing batch request in the queue that can be merged with this new request.
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

func (q *coalesceQueue) resetTimer() <-chan time.Time {
	if len(q.queue) == 0 {
		return nil
	}
	// find earliest deadline
	earliest := q.queue[0].deadline
	for _, r := range q.queue[1:] {
		if r.deadline.Before(earliest) {
			earliest = r.deadline
		}
	}
	// calculate the duration until the earliest deadline, and reset the timer to trigger at that time.
	d := time.Until(earliest)
	if d < 0 {
		d = 0
	}
	if q.timer != nil {
		q.timer.Reset(d)
	} else {
		q.timer = time.NewTimer(d)
	}
	return q.timer.C
}

func (q *coalesceQueue) flush() {
	// execute all the requests in the queue, and empty the queue afterwards.
	for _, req := range q.queue {
		distances, ids, err := q.idx.searchWithoutIDs(req.qVector, req.k, nil, nil)
		req.sendResponse(distances, ids, err)
	}
	// clear the queue after processing all the requests.
	q.queue = q.queue[:0]
}
