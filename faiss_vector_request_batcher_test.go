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
	"testing"
	"time"

	faiss "github.com/blevesearch/go-faiss"
)

// --------------------------
// fake Faiss index
// --------------------------
// placeholder faiss index implementation for testing the request batcher.
// It allows us to simulate search latency and return predefined distances and IDs for
// search requests, without needing a real Faiss index or vector data.
type fakeFaissIndex struct {
	searchDelay time.Duration
}

func (f *fakeFaissIndex) search(vecs *vectorSet, k int64) ([]float32, []int64, error) {
	if f.searchDelay > 0 {
		time.Sleep(f.searchDelay)
	}
	// generate K random distances and IDs for testing purposes, for each vector in the input vector set.
	resultSize := vecs.nvecs * int(k)
	d := make([]float32, resultSize)
	i := make([]int64, resultSize)
	for j := 0; j < resultSize; j++ {
		d[j] = float32(j)
		i[j] = int64(j)
	}
	return d, i, nil
}

func (f *fakeFaissIndex) batchSearch(vecs *vectorSet, k int64) ([]float32, []int64, error) {
	return f.search(vecs, k)
}

func (f *fakeFaissIndex) searchWithoutIDs(vecs *vectorSet, k int64, _ faiss.Selector, _ json.RawMessage) ([]float32, []int64, error) {
	return f.search(vecs, k)
}

func (f *fakeFaissIndex) searchWithIDs(vecs *vectorSet, k int64, _ faiss.Selector, _ json.RawMessage) ([]float32, []int64, error) {
	return f.search(vecs, k)
}

func (f *fakeFaissIndex) add(vecs *vectorSet) error {
	return nil
}

func (f *fakeFaissIndex) close() {}

func (f *fakeFaissIndex) dim() int {
	return 0
}

func (f *fakeFaissIndex) metricType() int {
	return 0
}

func (f *fakeFaissIndex) serialize() ([]byte, error) {
	return nil, nil
}

func (f *fakeFaissIndex) size() uint64 {
	return 0
}

func (f *fakeFaissIndex) castIVF() faissIndexIVF {
	return nil
}

func (f *fakeFaissIndex) castSQ() faissIndexSQ {
	return nil
}

func newFakeFaissIndex(searchDelay time.Duration) *fakeFaissIndex {
	return &fakeFaissIndex{
		searchDelay: searchDelay,
	}
}

// --------------------------------------------------

func dummyVectorSet(dim int) (*vectorSet, error) {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = float32(i)
	}
	return newVectorSet(dim, vec)
}

func BenchmarkBatchedSearch(b *testing.B) {
	// config
	dims := 512
	k := 10
	delay := 50 * time.Millisecond
	// impl
	idx := newFakeFaissIndex(delay)
	rb := newRequestBatcher(idx)
	b.Cleanup(rb.stop)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			vecs, err := dummyVectorSet(dims)
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
			_, _, err = rb.search(vecs, int64(k))
			if err != nil {
				b.Fatalf("unexpected error: %v", err)
			}
		}
	})
}
