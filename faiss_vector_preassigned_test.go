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
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

const (
	preassignedTestDims = 8
	// comfortably past ivfThreshold so both the trained index and the merged
	// segment clear canFastMerge's bar and an IVF index is actually built.
	preassignedTestVecs = 1400
)

// preassignedTestDocs builds numDocs documents with deterministic pseudo-random
// vectors, offset by seed so that different calls produce different data.
func preassignedTestDocs(t *testing.T, numDocs int, seed int64, optimizeFor string) []index.Document {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	docs := make([]index.Document, 0, numDocs)
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, preassignedTestDims)
		for j := range vec {
			vec[j] = rng.Float32() * 100
		}
		id := docIDForSeed(seed, i)
		docs = append(docs, newVecStubDocument(id, []index.Field{
			newStubFieldSplitString("_id", nil, id, true, false, false),
			newStubFieldVecWithOptimization("vec", vec, preassignedTestDims,
				index.EuclideanDistance, index.IndexField, optimizeFor),
		}))
	}
	return docs
}

func docIDForSeed(seed int64, i int) string {
	return string(rune('a'+int(seed)%26)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// persistAndOpen writes docs out as a segment at path and opens it.
func persistAndOpen(t *testing.T, plugin *ZapPlugin, docs []index.Document,
	path string, config map[string]interface{}) segment.Segment {
	t.Helper()
	newSeg, _, err := plugin.NewUsing(docs, config)
	if err != nil {
		t.Fatalf("building segment at %s: %v", path, err)
	}
	unpersisted, ok := newSeg.(segment.UnpersistedSegment)
	if !ok {
		t.Fatalf("segment at %s is not persistable", path)
	}
	if err := unpersisted.Persist(path); err != nil {
		t.Fatalf("persisting segment at %s: %v", path, err)
	}
	onDisk, err := plugin.OpenUsing(path, config)
	if err != nil {
		t.Fatalf("opening segment at %s: %v", path, err)
	}
	t.Cleanup(func() {
		_ = onDisk.Close()
		_ = os.RemoveAll(path)
	})
	return onDisk
}

// vecResults drains a postings list into a docNum -> score map.
func vecResults(t *testing.T, pl segment.VecPostingsList) map[uint64]float32 {
	t.Helper()
	rv := make(map[uint64]float32)
	itr := pl.Iterator(nil)
	for {
		next, err := itr.Next()
		if err != nil {
			t.Fatalf("iterating postings: %v", err)
		}
		if next == nil {
			return rv
		}
		rv[next.Number()] = next.Score()
	}
}

// TestPreassignedCentroidSearch covers the two phase search end to end: a
// trained index ranks the centroids once, a segment that was fast merged
// against that trained index reuses the ranking, and the results are the same
// ones an ordinary search would have produced.
func TestPreassignedCentroidSearch(t *testing.T) {
	dir := t.TempDir()
	plugin := &ZapPlugin{}

	// The trained index: a segment built from a sample of the data, whose
	// coarse quantizer every fast merged segment will clone.
	trainedSeg := persistAndOpen(t, plugin,
		preassignedTestDocs(t, preassignedTestVecs, 1, index.IndexOptimizedForMemoryEfficient),
		filepath.Join(dir, "trained"), nil)

	trained, ok := trainedSeg.(segment.TrainedSegment)
	if !ok {
		t.Fatal("trained segment does not implement segment.TrainedSegment")
	}

	// Two data segments, to be merged into one against the trained index.
	dataSeg1 := persistAndOpen(t, plugin,
		preassignedTestDocs(t, preassignedTestVecs, 2, index.IndexOptimizedForMemoryEfficient),
		filepath.Join(dir, "data-1"), nil)
	dataSeg2 := persistAndOpen(t, plugin,
		preassignedTestDocs(t, preassignedTestVecs, 3, index.IndexOptimizedForMemoryEfficient),
		filepath.Join(dir, "data-2"), nil)

	// Wiring the trained index in via the callback is what makes the merge take
	// the fast path, which clones the trained quantizer into the output.
	mergeConfig := map[string]interface{}{
		index.TrainedIndexCallback: index.TrainedIndexCallbackFn(
			func(field string) (interface{}, error) {
				return trained.GetCoarseQuantizer(field)
			}),
	}

	mergedPath := filepath.Join(dir, "merged")
	_, _, err := plugin.MergeUsing([]segment.Segment{dataSeg1, dataSeg2},
		[]*roaring.Bitmap{nil, nil}, mergedPath, nil, nil, mergeConfig)
	if err != nil {
		t.Fatalf("fast merge failed: %v", err)
	}
	mergedSeg, err := plugin.OpenUsing(mergedPath, nil)
	if err != nil {
		t.Fatalf("opening merged segment: %v", err)
	}
	defer func() {
		_ = mergedSeg.Close()
		_ = os.RemoveAll(mergedPath)
	}()

	// ---------------------------------------------------------------
	// Phase one: rank the centroids once, against the trained index.
	// ---------------------------------------------------------------
	queryVec := make([]float32, preassignedTestDims)
	rng := rand.New(rand.NewSource(99))
	for i := range queryVec {
		queryVec[i] = rng.Float32() * 100
	}

	pre, err := trained.SearchCentroids("vec", queryVec)
	if err != nil {
		t.Fatalf("phase one centroid search failed: %v", err)
	}
	if !pre.Valid() {
		t.Fatal("expected a usable centroid ranking from the trained index")
	}
	if pre.Nprobe <= 0 {
		t.Fatalf("expected a positive nprobe from the trained index, got %d", pre.Nprobe)
	}
	// The ranking must be ordered by increasing distance, since phase two
	// probes a prefix of it.
	for i := 1; i < len(pre.Distances[:len(pre.IDs)]); i++ {
		if pre.Distances[i] < pre.Distances[i-1] {
			t.Fatalf("centroid ranking is not ordered by distance at %d: %v < %v",
				i, pre.Distances[i], pre.Distances[i-1])
		}
	}

	const k = 20

	t.Run("fast merged segment reuses the ranking", func(t *testing.T) {
		vecSeg, ok := mergedSeg.(segment.VectorSegment)
		if !ok {
			t.Fatal("merged segment is not a vector segment")
		}
		vecIndex, err := vecSeg.InterpretVectorIndex("vec", nil)
		if err != nil {
			t.Fatalf("interpreting vector index: %v", err)
		}
		defer vecIndex.Close()

		pvi, ok := vecIndex.(segment.PreassignedVectorIndex)
		if !ok {
			t.Fatal("vector index does not implement segment.PreassignedVectorIndex")
		}
		shares, err := pvi.SharesCentroidLayout(pre)
		if err != nil {
			t.Fatalf("layout check failed: %v", err)
		}
		if !shares {
			t.Fatal("a fast merged segment must share the trained centroid layout")
		}

		want, err := vecIndex.Search(queryVec, k, nil)
		if err != nil {
			t.Fatalf("ordinary search failed: %v", err)
		}
		got, err := pvi.SearchPreassigned(queryVec, k, pre, nil)
		if err != nil {
			t.Fatalf("pre-assigned search failed: %v", err)
		}
		assertSameResults(t, vecResults(t, want), vecResults(t, got))
	})

	t.Run("independently built segment falls back", func(t *testing.T) {
		// dataSeg1 was clustered on its own data, so its list numbering has
		// nothing to do with the trained index's.
		vecSeg, ok := dataSeg1.(segment.VectorSegment)
		if !ok {
			t.Fatal("data segment is not a vector segment")
		}
		vecIndex, err := vecSeg.InterpretVectorIndex("vec", nil)
		if err != nil {
			t.Fatalf("interpreting vector index: %v", err)
		}
		defer vecIndex.Close()

		pvi, ok := vecIndex.(segment.PreassignedVectorIndex)
		if !ok {
			t.Fatal("vector index does not implement segment.PreassignedVectorIndex")
		}
		shares, err := pvi.SharesCentroidLayout(pre)
		if err != nil {
			t.Fatalf("layout check failed: %v", err)
		}
		if shares {
			t.Fatal("an independently clustered segment must not claim the trained layout")
		}

		// Passing pre anyway has to be harmless: the index falls back and
		// returns exactly what an ordinary search would.
		want, err := vecIndex.Search(queryVec, k, nil)
		if err != nil {
			t.Fatalf("ordinary search failed: %v", err)
		}
		got, err := pvi.SearchPreassigned(queryVec, k, pre, nil)
		if err != nil {
			t.Fatalf("pre-assigned search failed: %v", err)
		}
		assertSameResults(t, vecResults(t, want), vecResults(t, got))
	})

	t.Run("filtered search reuses the ranking", func(t *testing.T) {
		vecSeg, ok := mergedSeg.(segment.VectorSegment)
		if !ok {
			t.Fatal("merged segment is not a vector segment")
		}
		vecIndex, err := vecSeg.InterpretVectorIndex("vec", nil)
		if err != nil {
			t.Fatalf("interpreting vector index: %v", err)
		}
		defer vecIndex.Close()

		pvi := vecIndex.(segment.PreassignedVectorIndex)

		// every third document is eligible
		eligible := make([]uint64, 0, preassignedTestVecs)
		for i := uint64(0); i < uint64(2*preassignedTestVecs); i += 3 {
			eligible = append(eligible, i)
		}
		list := &stubEligibleDocumentList{ids: eligible}

		want, err := vecIndex.SearchWithFilter(queryVec, k, list, nil)
		if err != nil {
			t.Fatalf("ordinary filtered search failed: %v", err)
		}
		got, err := pvi.SearchWithFilterPreassigned(queryVec, k, list, pre, nil)
		if err != nil {
			t.Fatalf("pre-assigned filtered search failed: %v", err)
		}
		assertSameResults(t, vecResults(t, want), vecResults(t, got))
	})
}

// assertSameResults requires the two searches to agree exactly. They probe the
// same clusters in the same order over the same data, so anything else means
// the pre-assigned path diverged.
func assertSameResults(t *testing.T, want, got map[uint64]float32) {
	t.Helper()
	if len(want) == 0 {
		t.Fatal("baseline search returned no results, the comparison would be vacuous")
	}
	if len(want) != len(got) {
		t.Fatalf("result count differs: ordinary search %d, pre-assigned %d",
			len(want), len(got))
	}
	for docNum, wantScore := range want {
		gotScore, ok := got[docNum]
		if !ok {
			t.Fatalf("doc %d present in the ordinary search but missing from the pre-assigned one",
				docNum)
		}
		if gotScore != wantScore {
			t.Fatalf("doc %d scored %v by the ordinary search but %v by the pre-assigned one",
				docNum, wantScore, gotScore)
		}
	}
}

// ---------------------------------------------------------------------------

// stubEligibleDocumentList is a minimal index.EligibleDocumentList over a
// sorted slice of local doc numbers.
type stubEligibleDocumentList struct {
	ids []uint64
}

func (s *stubEligibleDocumentList) Count() uint64 { return uint64(len(s.ids)) }

func (s *stubEligibleDocumentList) Iterator() index.EligibleDocumentIterator {
	return &stubEligibleDocumentIterator{ids: s.ids}
}

type stubEligibleDocumentIterator struct {
	ids []uint64
	pos int
}

func (s *stubEligibleDocumentIterator) Next() (uint64, bool) {
	if s.pos >= len(s.ids) {
		return 0, false
	}
	id := s.ids[s.pos]
	s.pos++
	return id, true
}
