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

package zap

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	index "github.com/blevesearch/bleve_index_api"
)

// Neither zapx nor bleve had a postings-scan benchmark before this format
// change, which made "is the inner loop actually faster" unanswerable. These
// measure the thing the block format exists to speed up: walking a term's
// postings and reading the frequency and norm of each hit.

const benchNumDocs = 50000

// benchTermSpec names a term and how often it appears, spanning the range that
// matters: one in every document (deltas of 1, so the doc block packs at zero
// bits), a mid-frequency term, and a rare one.
var benchTermSpecs = []struct {
	term string
	mod  int
}{
	{"bench_all", 1},
	{"bench_10", 10},
	{"bench_1000", 1000},
}

var (
	benchSegOnce sync.Once
	benchSeg     *SegmentBase
)

func benchSegment(tb testing.TB) *SegmentBase {
	benchSegOnce.Do(func() {
		results := make([]index.Document, 0, benchNumDocs)
		for i := 0; i < benchNumDocs; i++ {
			tokens := make([]string, 0, 8)
			for _, spec := range benchTermSpecs {
				if i%spec.mod == 0 {
					tokens = append(tokens, spec.term)
				}
			}
			tokens = append(tokens, fmt.Sprintf("u%d", i))
			for p := 0; p < i%23; p++ {
				tokens = append(tokens, "pad")
			}
			f := newStubFieldSplitString("body", nil, strings.Join(tokens, " "), false, false, false)
			f.options = index.IndexField
			for _, tf := range f.analyzedFreqs {
				tf.Locations = nil
			}
			results = append(results, newStubDocument(fmt.Sprintf("d%d", i), []*stubField{
				newStubFieldSplitString("_id", nil, fmt.Sprintf("d%d", i), true, false, false),
				f,
			}, "_all"))
		}
		seg, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
		if err != nil {
			panic(err)
		}
		benchSeg = seg.(*SegmentBase)
	})
	return benchSeg
}

func BenchmarkPostingsNext(b *testing.B) {
	sb := benchSegment(b)
	dict, err := sb.dictionary("body")
	if err != nil {
		b.Fatal(err)
	}
	for _, spec := range benchTermSpecs {
		b.Run(spec.term, func(b *testing.B) {
			var pl *PostingsList
			var itr *PostingsIterator
			var hits int
			for b.Loop() {
				pl, err = dict.postingsList([]byte(spec.term), nil, pl)
				if err != nil {
					b.Fatal(err)
				}
				it := pl.Iterator(true, true, false, itr)
				itr = it.(*PostingsIterator)
				for {
					p, err := itr.Next()
					if err != nil {
						b.Fatal(err)
					}
					if p == nil {
						break
					}
					sinkFloat += p.Norm() * float64(p.Frequency())
					hits++
				}
			}
			b.ReportMetric(float64(hits)/float64(b.N), "hits/op")
		})
	}
}

// BenchmarkPostingsAdvance is the conjunction-style access pattern: jump
// forward by a stride rather than walking every posting. Large strides are what
// the skip list is for.
func BenchmarkPostingsAdvance(b *testing.B) {
	sb := benchSegment(b)
	dict, err := sb.dictionary("body")
	if err != nil {
		b.Fatal(err)
	}
	for _, stride := range []uint64{16, 256, 4096} {
		b.Run(fmt.Sprintf("all/stride%d", stride), func(b *testing.B) {
			var pl *PostingsList
			var itr *PostingsIterator
			for b.Loop() {
				pl, err = dict.postingsList([]byte("bench_all"), nil, pl)
				if err != nil {
					b.Fatal(err)
				}
				it := pl.Iterator(true, true, false, itr)
				itr = it.(*PostingsIterator)
				var target uint64
				for {
					p, err := itr.Advance(target)
					if err != nil {
						b.Fatal(err)
					}
					if p == nil {
						break
					}
					sinkFloat += p.Norm()
					target = p.Number() + stride
				}
			}
		})
	}
}

// benchDeletionSpecs names two deletion shapes because they stress
// positionAtLive's per-block delMask differently: scattered deletions mainly
// exercise replacing isDeleted's three interface calls with a bit test; a
// deleted run exercises the bigger win -- skipping a whole block with
// nothing live left in it via the cross-block loop, instead of bouncing back
// out to the caller once per deleted doc.
var benchDeletionSpecs = []struct {
	name   string
	except func(numDocs int) *roaring.Bitmap
}{
	// Every 11th doc: 11 shares no factor with any benchTermSpecs mod (1, 10,
	// 1000), so this lands as a partial, non-degenerate overlap against all
	// three terms instead of deleting one of them outright.
	{"scattered9pct", func(numDocs int) *roaring.Bitmap {
		bm := roaring.New()
		for d := 0; d < numDocs; d += 11 {
			bm.Add(uint32(d))
		}
		return bm
	}},
	{"run20pct", func(numDocs int) *roaring.Bitmap {
		bm := roaring.New()
		bm.AddRange(0, uint64(numDocs/5))
		return bm
	}},
}

func BenchmarkPostingsNextWithDeletions(b *testing.B) {
	sb := benchSegment(b)
	dict, err := sb.dictionary("body")
	if err != nil {
		b.Fatal(err)
	}
	for _, spec := range benchTermSpecs {
		for _, del := range benchDeletionSpecs {
			except := del.except(benchNumDocs)
			b.Run(spec.term+"/"+del.name, func(b *testing.B) {
				var pl *PostingsList
				var itr *PostingsIterator
				var hits int
				for b.Loop() {
					pl, err = dict.postingsList([]byte(spec.term), except, pl)
					if err != nil {
						b.Fatal(err)
					}
					it := pl.Iterator(true, true, false, itr)
					itr = it.(*PostingsIterator)
					for {
						p, err := itr.Next()
						if err != nil {
							b.Fatal(err)
						}
						if p == nil {
							break
						}
						sinkFloat += p.Norm() * float64(p.Frequency())
						hits++
					}
				}
				b.ReportMetric(float64(hits)/float64(b.N), "hits/op")
			})
		}
	}
}

// BenchmarkPostingsAdvanceWithDeletions is BenchmarkPostingsAdvance's
// counterpart with a deletion set applied, isolating the same stride-based
// access pattern conjunctions use.
func BenchmarkPostingsAdvanceWithDeletions(b *testing.B) {
	sb := benchSegment(b)
	dict, err := sb.dictionary("body")
	if err != nil {
		b.Fatal(err)
	}
	for _, del := range benchDeletionSpecs {
		except := del.except(benchNumDocs)
		for _, stride := range []uint64{16, 256, 4096} {
			b.Run(fmt.Sprintf("all/%s/stride%d", del.name, stride), func(b *testing.B) {
				var pl *PostingsList
				var itr *PostingsIterator
				for b.Loop() {
					pl, err = dict.postingsList([]byte("bench_all"), except, pl)
					if err != nil {
						b.Fatal(err)
					}
					it := pl.Iterator(true, true, false, itr)
					itr = it.(*PostingsIterator)
					var target uint64
					for {
						p, err := itr.Advance(target)
						if err != nil {
							b.Fatal(err)
						}
						if p == nil {
							break
						}
						sinkFloat += p.Norm()
						target = p.Number() + stride
					}
				}
			})
		}
	}
}

var sinkFloat float64

// BenchmarkSegmentBuild and BenchmarkSegmentMerge cover the write path: block
// packing replaces the chunked varint encoders, and field norms move from one
// varint per posting to one byte per document.
func BenchmarkSegmentBuild(b *testing.B) {
	results := benchDocuments(10000)
	b.ResetTimer()
	for b.Loop() {
		_, _, err := zapPlugin.newWithChunkMode(results, DefaultChunkMode, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSegmentMerge(b *testing.B) {
	var sbs []*SegmentBase
	for s := 0; s < 4; s++ {
		seg, _, err := zapPlugin.newWithChunkMode(benchDocuments(5000), DefaultChunkMode, nil)
		if err != nil {
			b.Fatal(err)
		}
		sbs = append(sbs, seg.(*SegmentBase))
	}
	drops := make([]*roaring.Bitmap, len(sbs))
	path := getTempPath("zap-bench-merge.zap")

	b.ResetTimer()
	for b.Loop() {
		_ = os.RemoveAll(path)
		_, _, err := mergeSegmentBases(sbs, drops, path, DefaultChunkMode, nil, nil, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = os.RemoveAll(path)
}

func benchDocuments(n int) []index.Document {
	results := make([]index.Document, 0, n)
	for i := 0; i < n; i++ {
		tokens := make([]string, 0, 8)
		for _, spec := range benchTermSpecs {
			if i%spec.mod == 0 {
				tokens = append(tokens, spec.term)
			}
		}
		tokens = append(tokens, fmt.Sprintf("u%d", i))
		for p := 0; p < i%23; p++ {
			tokens = append(tokens, "pad")
		}
		f := newStubFieldSplitString("body", nil, strings.Join(tokens, " "), false, false, false)
		f.options = index.IndexField
		for _, tf := range f.analyzedFreqs {
			tf.Locations = nil
		}
		results = append(results, newStubDocument(fmt.Sprintf("d%d", i), []*stubField{
			newStubFieldSplitString("_id", nil, fmt.Sprintf("d%d", i), true, false, false),
			f,
		}, "_all"))
	}
	return results
}

// BenchmarkPostingsIteratorConstructOnly measures the pure cost of building
// a fresh iterator and never touching it -- the "far more iterators than it
// decodes blocks" case the blockCursor.buf pointer comment describes: a term
// that gets pruned or found absent in a segment before anything is ever
// decoded.
func BenchmarkPostingsIteratorConstructOnly(b *testing.B) {
	sb := benchSegment(b)
	dict, err := sb.dictionary("body")
	if err != nil {
		b.Fatal(err)
	}
	pl, err := dict.postingsList([]byte("bench_all"), nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		itr := pl.Iterator(true, true, false, nil)
		if itr == nil {
			b.Fatal("nil iterator")
		}
	}
}

// BenchmarkPostingsIteratorMixed simulates a multi-segment query for a term
// that only really matters in a fraction of the segments it's looked up in:
// most constructed iterators are discarded unused, a few are fully scanned.
func BenchmarkPostingsIteratorMixed(b *testing.B) {
	sb := benchSegment(b)
	dict, err := sb.dictionary("body")
	if err != nil {
		b.Fatal(err)
	}
	pl, err := dict.postingsList([]byte("bench_all"), nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	const fanout = 10 // 1 in 10 iterators actually gets scanned
	i := 0
	for b.Loop() {
		itr := pl.Iterator(true, true, false, nil)
		if i%fanout == 0 {
			for {
				p, err := itr.Next()
				if err != nil {
					b.Fatal(err)
				}
				if p == nil {
					break
				}
				sinkFloat += p.Norm()
			}
		}
		i++
	}
}
