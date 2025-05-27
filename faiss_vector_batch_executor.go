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
	"encoding/json"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	faiss "github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// batchKey represents a unique combination of k and params for batching
type batchKey struct {
	k      int64
	params string // string representation of params for comparison
}

// batchRequest represents a single vector search request in a batch
type batchRequest struct {
	qVector []float32
	result  chan batchResult
}

// batchGroup represents a group of requests with the same k and params
type batchGroup struct {
	requests           []batchRequest
	vecIndex           *faiss.IndexImpl
	vecDocIDMap        map[int64]uint32
	vectorIDsToExclude []int64
}

// batchExecutor manages batched vector search requests
type batchExecutor struct {
	batchDelay time.Duration

	m      sync.RWMutex
	groups map[batchKey]*batchGroup
}

func newBatchExecutor(options *segment.InterpretVectorIndexOptions) *batchExecutor {
	batchDelay := segment.DefaultBatchExecutionDelay
	if options != nil && options.BatchExecutionDelay > 0 {
		batchDelay = options.BatchExecutionDelay
	}

	return &batchExecutor{
		batchDelay: batchDelay,
		groups:     make(map[batchKey]*batchGroup),
	}
}

type batchResult struct {
	pl  segment.VecPostingsList
	err error
}

func (be *batchExecutor) close() {
	be.m.Lock()
	defer be.m.Unlock()

	for key, group := range be.groups {
		for _, req := range group.requests {
			close(req.result)
		}
		delete(be.groups, key)
	}
}

// queueRequest adds a vector search request to the appropriate batch group
func (be *batchExecutor) queueRequest(qVector []float32, k int64, params json.RawMessage,
	vecIndex *faiss.IndexImpl, vecDocIDMap map[int64]uint32,
	vectorIDsToExclude []int64) <-chan batchResult {

	// Create a channel for the result
	resultCh := make(chan batchResult, 1)

	// Create batch key
	key := batchKey{
		k:      k,
		params: string(params),
	}

	be.m.Lock()
	defer be.m.Unlock()

	// Get or create batch group
	group, exists := be.groups[key]
	if !exists {
		group = &batchGroup{
			requests:           make([]batchRequest, 0),
			vecIndex:           vecIndex,
			vecDocIDMap:        vecDocIDMap,
			vectorIDsToExclude: vectorIDsToExclude,
		}
		be.groups[key] = group
	}

	// Add request to group
	group.requests = append(group.requests, batchRequest{
		qVector: qVector,
		result:  resultCh,
	})

	// If this is the first request in the group, start a timer to process the batch
	if len(group.requests) == 1 {
		be.processBatchAfterDelay(key, be.batchDelay)
	}

	return resultCh
}

// processBatchAfterDelay waits for the specified delay and then processes the batch
func (be *batchExecutor) processBatchAfterDelay(key batchKey, delay time.Duration) {
	time.AfterFunc(delay, func() {
		be.m.Lock()
		group, exists := be.groups[key]
		if !exists {
			be.m.Unlock()
			return
		}

		// Remove the group from the map before processing
		delete(be.groups, key)
		be.m.Unlock()

		// Process the batch
		be.processBatch(key, group)
	})
}

// processBatch executes a batch of vector search requests
func (be *batchExecutor) processBatch(key batchKey, group *batchGroup) {
	if len(group.requests) == 0 {
		return
	}

	// Prepare vectors for batch search
	dim := group.vecIndex.D()
	vecs := make([]float32, len(group.requests)*dim)
	for i, req := range group.requests {
		copy(vecs[i*dim:(i+1)*dim], req.qVector)
	}

	// Execute batch search
	scores, ids, err := group.vecIndex.SearchWithoutIDs(vecs, key.k, group.vectorIDsToExclude,
		json.RawMessage(key.params))
	if err != nil {
		// Send error to all channels
		for _, req := range group.requests {
			req.result <- batchResult{
				err: err,
			}
			close(req.result)
		}
		return
	}

	// Calculate number of results per request
	resultsPerRequest := int(key.k)
	totalResults := len(scores)

	// Process results and send to respective channels
	for i := range group.requests {
		pl := &VecPostingsList{
			postings: roaring64.New(),
		}

		// Calculate start and end indices for this request's results
		startIdx := i * resultsPerRequest
		endIdx := startIdx + resultsPerRequest
		if endIdx > totalResults {
			endIdx = totalResults
		}

		// Get this request's results
		currScores := scores[startIdx:endIdx]
		currIDs := ids[startIdx:endIdx]

		// Add results to postings list
		for j := 0; j < len(currIDs); j++ {
			vecID := currIDs[j]
			if docID, ok := group.vecDocIDMap[vecID]; ok {
				code := getVectorCode(docID, currScores[j])
				pl.postings.Add(code)
			}
		}

		// Send result to channel
		group.requests[i].result <- batchResult{
			pl: pl,
		}
		close(group.requests[i].result)
	}
}
