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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// Benchmarks for the two phase vector search.
//
// What the change trades: an ordinary query runs one coarse quantizer search
// per segment, each costing roughly nlist * d distance computations. The two
// phase path runs exactly one, against the trained index, and every segment
// that shares its centroid layout reuses the result. So the expected saving
// grows with the segment count and with nlist * d, and shrinks as the work
// inside the probed clusters (nprobe * nvecs/nlist * d) comes to dominate.
//
// Both benchmarks below do the same total amount of cluster probing over the
// same data and return the same documents; only the quantizer work differs.

type benchCorpus struct {
	trained  segment.TrainedSegment
	segments []segment.Segment
	queries  [][]float32
	dims     int
	nlist    int
}

// buildBenchCorpus trains an index with nlist centroids and then writes
// numSegments segments, each fast merged against it so they all share its
// centroid layout.
func buildBenchCorpus(b *testing.B, dims, nlist, trainVecs, segVecs, numSegments int) *benchCorpus {
	b.Helper()
	dir := b.TempDir()
	plugin := &ZapPlugin{}

	docs := func(n int, seed int64) []index.Document {
		rng := rand.New(rand.NewSource(seed))
		rv := make([]index.Document, 0, n)
		for i := 0; i < n; i++ {
			vec := make([]float32, dims)
			for j := range vec {
				vec[j] = rng.Float32()
			}
			id := fmt.Sprintf("d%d-%d", seed, i)
			rv = append(rv, newVecStubDocument(id, []index.Field{
				newStubFieldSplitString("_id", nil, id, true, false, false),
				newStubFieldVecWithOptimization("vec", vec, dims,
					index.EuclideanDistance, index.IndexField,
					index.IndexOptimizedForMemoryEfficient),
			}))
		}
		return rv
	}

	open := func(d []index.Document, path string, cfg map[string]interface{}) segment.Segment {
		newSeg, _, err := plugin.NewUsing(d, cfg)
		if err != nil {
			b.Fatalf("building %s: %v", path, err)
		}
		if err := newSeg.(segment.UnpersistedSegment).Persist(path); err != nil {
			b.Fatalf("persisting %s: %v", path, err)
		}
		onDisk, err := plugin.OpenUsing(path, cfg)
		if err != nil {
			b.Fatalf("opening %s: %v", path, err)
		}
		b.Cleanup(func() {
			_ = onDisk.Close()
			_ = os.RemoveAll(path)
		})
		return onDisk
	}

	// The trained index, pinned to the requested centroid count the same way
	// the production training path does it.
	trainCfg := map[string]interface{}{
		index.TrainingKey: &index.TrainingParams{NumCentroids: nlist},
	}
	trainedSeg := open(docs(trainVecs, 1), filepath.Join(dir, "trained"), trainCfg)
	trained, ok := trainedSeg.(segment.TrainedSegment)
	if !ok {
		b.Fatal("trained segment does not implement segment.TrainedSegment")
	}

	mergeCfg := map[string]interface{}{
		index.TrainedIndexCallback: index.TrainedIndexCallbackFn(
			func(field string) (interface{}, error) {
				return trained.GetCoarseQuantizer(field)
			}),
	}

	// Each output segment is the fast merge of two inputs, which is what gives
	// it a clone of the trained quantizer.
	segments := make([]segment.Segment, 0, numSegments)
	for s := 0; s < numSegments; s++ {
		inA := open(docs(segVecs/2, int64(100+2*s)), filepath.Join(dir, fmt.Sprintf("in-%d-a", s)), nil)
		inB := open(docs(segVecs/2, int64(101+2*s)), filepath.Join(dir, fmt.Sprintf("in-%d-b", s)), nil)

		mergedPath := filepath.Join(dir, fmt.Sprintf("merged-%d", s))
		_, _, err := plugin.MergeUsing([]segment.Segment{inA, inB},
			[]*roaring.Bitmap{nil, nil}, mergedPath, nil, nil, mergeCfg)
		if err != nil {
			b.Fatalf("fast merge %d failed: %v", s, err)
		}
		mergedSeg, err := plugin.OpenUsing(mergedPath, nil)
		if err != nil {
			b.Fatalf("opening merged segment %d: %v", s, err)
		}
		b.Cleanup(func() {
			_ = mergedSeg.Close()
			_ = os.RemoveAll(mergedPath)
		})
		segments = append(segments, mergedSeg)
	}

	// a fixed pool of query vectors, cycled through so no single query's
	// cluster distribution dominates the measurement
	rng := rand.New(rand.NewSource(7))
	queries := make([][]float32, 64)
	for i := range queries {
		q := make([]float32, dims)
		for j := range q {
			q[j] = rng.Float32()
		}
		queries[i] = q
	}

	return &benchCorpus{
		trained:  trained,
		segments: segments,
		dims:     dims,
		nlist:    nlist,
		queries:  queries,
	}
}

// vectorIndexes opens a wrapper per segment, as a real query does once.
func (c *benchCorpus) vectorIndexes(b *testing.B) []segment.VectorIndex {
	b.Helper()
	rv := make([]segment.VectorIndex, 0, len(c.segments))
	for _, s := range c.segments {
		vi, err := s.(segment.VectorSegment).InterpretVectorIndex("vec", nil)
		if err != nil {
			b.Fatalf("interpreting vector index: %v", err)
		}
		b.Cleanup(vi.Close)
		rv = append(rv, vi)
	}
	return rv
}

const benchK = 10

// benchCases are sized to bracket realistic deployments: the segment count is
// what the saving scales with, so it is the dimension varied most.
var benchCases = []struct {
	name        string
	dims        int
	nlist       int
	trainVecs   int
	segVecs     int
	numSegments int
}{
	// names avoid '/' so that -bench patterns can select a single case;
	// go test splits the pattern on '/' to address sub-benchmark levels.
	//
	// nlist is the variable that matters most. A trained index derives it from
	// the whole corpus, while each segment holds only a slice of that corpus,
	// so a production index has a large nlist against comparatively few
	// vectors per segment - meaning a full size quantizer search in front of a
	// short inverted list scan. The high nlist cases below are that regime;
	// the nlist=256 cases are the opposite one, where the list scan dominates
	// and there is little quantizer work to save.
	{"d128_nlist256_seg4", 128, 256, 25600, 12800, 4},
	{"d128_nlist256_seg16", 128, 256, 25600, 12800, 16},
	{"d128_nlist1024_seg16", 128, 1024, 40960, 12800, 16},
	{"d128_nlist4096_seg16", 128, 4096, 81920, 12800, 16},
	{"d768_nlist256_seg16", 768, 256, 25600, 12800, 16},
	{"d768_nlist4096_seg16", 768, 4096, 81920, 12800, 16},
}

// BenchmarkVectorSearchPerSegmentQuantizer is the baseline: every segment runs
// its own coarse quantizer search.
func BenchmarkVectorSearchPerSegmentQuantizer(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			c := buildBenchCorpus(b, tc.dims, tc.nlist, tc.trainVecs, tc.segVecs, tc.numSegments)
			indexes := c.vectorIndexes(b)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q := c.queries[i%len(c.queries)]
				for _, vi := range indexes {
					if _, err := vi.Search(q, benchK, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkVectorSearchPreassigned is the two phase path: one coarse quantizer
// search against the trained index, reused by every segment.
func BenchmarkVectorSearchPreassigned(b *testing.B) {
	for _, tc := range benchCases {
		b.Run(tc.name, func(b *testing.B) {
			c := buildBenchCorpus(b, tc.dims, tc.nlist, tc.trainVecs, tc.segVecs, tc.numSegments)
			indexes := c.vectorIndexes(b)

			// Confirm the fast path is actually the one being measured;
			// otherwise this would silently benchmark the fallback.
			pre, err := c.trained.SearchCentroids("vec", c.queries[0])
			if err != nil || !pre.Valid() {
				b.Fatalf("phase one produced no ranking: %v", err)
			}
			for i, vi := range indexes {
				shares, err := vi.(segment.PreassignedVectorIndex).SharesCentroidLayout(pre)
				if err != nil || !shares {
					b.Fatalf("segment %d does not share the trained layout (err %v)", i, err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q := c.queries[i%len(c.queries)]
				// phase one, once per query
				pre, err := c.trained.SearchCentroids("vec", q)
				if err != nil {
					b.Fatal(err)
				}
				// phase two, per segment
				for _, vi := range indexes {
					_, err := vi.(segment.PreassignedVectorIndex).
						SearchPreassigned(q, benchK, pre, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkCoarseQuantizerShare breaks a query down into its two parts, so the
// ceiling on what the two phase search can save is visible directly:
//
//	Phase1  - one coarse quantizer search against the trained index, which is
//	          the work the two phase path performs once instead of per segment.
//	Segment - one ordinary single-segment search, quantizer plus list scan,
//	          which is what the baseline pays per segment.
//
// The saving available to a query over M segments is (M-1) * Phase1, so
// Phase1/Segment is what decides whether any of this is worth it.
func BenchmarkCoarseQuantizerShare(b *testing.B) {
	for _, tc := range benchCases {
		// one segment is enough: this benchmark is about the per-query split,
		// not about how it scales with segment count.
		c := buildBenchCorpus(b, tc.dims, tc.nlist, tc.trainVecs, tc.segVecs, 1)

		b.Run(tc.name+"/Phase1", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := c.trained.SearchCentroids("vec", c.queries[i%len(c.queries)]); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/Segment", func(b *testing.B) {
			vi := c.vectorIndexes(b)[0]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := vi.Search(c.queries[i%len(c.queries)], benchK, nil); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/SegmentPreassigned", func(b *testing.B) {
			vi := c.vectorIndexes(b)[0].(segment.PreassignedVectorIndex)
			pre, err := c.trained.SearchCentroids("vec", c.queries[0])
			if err != nil || !pre.Valid() {
				b.Fatalf("phase one produced no ranking: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := vi.SearchPreassigned(c.queries[i%len(c.queries)], benchK, pre, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
