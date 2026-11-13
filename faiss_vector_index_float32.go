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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"

	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
)

var reflectStaticSizeFloat32Index uint64

func init() {
	var f faissFloat32Index
	reflectStaticSizeFloat32Index = uint64(reflect.TypeOf(f).Size())
}

// ---------------------------------
// Faiss Float32 Index
// ---------------------------------
type faissFloat32Index struct {
	idx *faiss.IndexImpl
	// idxBytes holds the original serialized index bytes to prevent GC.
	idxBytes []byte
	params   *faissIndexParams
}

func newFaissFloat32Index(idx *faiss.IndexImpl, params *faissIndexParams) (faissIndex, error) {
	if idx == nil {
		return nil, errNilIndex
	}
	if params == nil {
		return nil, errNilParams
	}
	return &faissFloat32Index{
		idx:    idx,
		params: params,
	}, nil
}

func newFaissFloat32IndexFromBytes(idxBytes []byte, params *faissIndexParams) (faissIndex, error) {
	if idxBytes == nil {
		return nil, errNilIndex
	}

	if params == nil {
		return nil, errNilParams
	}

	idx, err := faiss.ReadIndexFromBuffer(idxBytes, params.ioFlags)
	if err != nil {
		return nil, err
	}

	return &faissFloat32Index{
		idx:      idx,
		idxBytes: idxBytes,
		params:   params,
	}, nil
}

func (f *faissFloat32Index) add(vecs *vectorSet) error {
	return f.idx.Add(vecs.floatData)
}

func (f *faissFloat32Index) close() {
	f.idx.Close()
	f.idxBytes = nil
}

func (f *faissFloat32Index) dim() int {
	return f.idx.D()
}

func (f *faissFloat32Index) metricType() int {
	return f.idx.MetricType()
}

func (f *faissFloat32Index) ntotal() int64 {
	return f.idx.Ntotal()
}

func (f *faissFloat32Index) reconstructBatch(vecIDs []int64, prealloc []float32) ([]float32, error) {
	return f.idx.ReconstructBatch(vecIDs, prealloc)
}

func (f *faissFloat32Index) search(qVector *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	return f.idx.SearchWithOptions(qVector.floatData, k, selector, params)
}

func (f *faissFloat32Index) write(buf []byte, w *FileWriter) error {
	// A writer callback may change the payload's length, and is not known to
	// be decomposable across chunks, so the whole index has to exist before
	// its length prefix can be written. That case keeps the buffered path.
	if w.processor != nil {
		return f.writeBuffered(buf, w)
	}

	// Ask faiss for the exact serialized size up front
	size, err := faiss.IndexSerializedSize(f.idx)
	if err != nil {
		return err
	}
	atomic.AddUint64(&f.params.stats.TotVecSectionFloatIndexBytesWritten, size)

	// write the length of the serialized vector index bytes
	n := binary.PutUvarint(buf, size)
	_, err = w.Write(buf[:n])
	if err != nil {
		return err
	}

	written, err := faiss.WriteIndexChunked(f.idx, 0, func(chunk []byte) error {
		_, werr := w.Write(chunk)
		return werr
	})
	if err != nil {
		return err
	}
	if written != size {
		// The length prefix is already out, so a disagreement here would
		// leave a segment that reads back as garbage. Fail loudly instead.
		return fmt.Errorf("faiss wrote %d index bytes against a declared size of %d",
			written, size)
	}
	return nil
}

// writeBuffered serializes the index into memory and writes it out in one
// piece. Used when a writer callback is in play, since the post-callback
// length cannot be known before the callback has seen everything.
func (f *faissFloat32Index) writeBuffered(buf []byte, w *FileWriter) error {
	idxBytes, err := faiss.WriteIndexIntoBuffer(f.idx)
	if err != nil {
		return err
	}
	idxBytes = w.process(idxBytes)
	atomic.AddUint64(&f.params.stats.TotVecSectionFloatIndexBytesWritten, uint64(len(idxBytes)))
	// write the length of the serialized vector index bytes
	n := binary.PutUvarint(buf, uint64(len(idxBytes)))
	_, err = w.Write(buf[:n])
	if err != nil {
		return err
	}

	_, err = w.Write(idxBytes)
	if err != nil {
		return err
	}
	return nil
}

func (f *faissFloat32Index) size() uint64 {
	return reflectStaticSizeFloat32Index +
		f.params.size() +
		f.idx.Size()
}

// -----------------------------------------------------------------
// casting methods to access index-specific operations below
// -----------------------------------------------------------------
func (f *faissFloat32Index) castIVF() faissIndexIVF {
	if f.idx.IsIVFIndex() {
		// return f itself, as the IVF interface is implemented by the same
		// struct as the non-IVF interface in go-faiss.
		return f
	}
	// not an IVF index, return nil.
	return nil
}

// -----------------------------------------------------------------
// IVF-Index specific operations
// -----------------------------------------------------------------
func (f *faissFloat32Index) clusterVectorCounts(sel faiss.Selector, nlist int) ([]int64, error) {
	return f.idx.ObtainClusterVectorCountsFromIVFIndex(sel, nlist)
}

func (f *faissFloat32Index) centroidCardinalities(limit int, descending bool) ([]uint64, [][]float32, error) {
	return f.idx.ObtainKCentroidCardinalitiesFromIVFIndex(limit, descending)
}

func (f *faissFloat32Index) ivfParams() (nprobe, nlist int) {
	return f.idx.IVFParams()
}

func (f *faissFloat32Index) searchQuantizer(qVector *vectorSet, centroidSelector faiss.Selector, centroidCount int64) ([]int64, []float32, error) {
	return f.idx.ObtainClustersWithDistancesFromIVFIndex(qVector.floatData, centroidSelector, centroidCount)
}

func (f *faissFloat32Index) searchClusters(eligibleCentroidIDs []int64, centroidDis []float32,
	centroidsToProbe int, qVecSet *vectorSet, k int64, selector faiss.Selector, params json.RawMessage) ([]float32, []int64, error) {
	return f.idx.SearchClustersFromIVFIndex(eligibleCentroidIDs, centroidDis, centroidsToProbe, qVecSet.floatData, k, selector, params)
}

func (f *faissFloat32Index) setDirectMap(directMapType int) error {
	return f.idx.SetDirectMap(directMapType)
}

func (f *faissFloat32Index) setNProbe(nprobe int32) {
	f.idx.SetNProbe(nprobe)
}

func (f *faissFloat32Index) train(trainingData *vectorSet) error {
	return f.idx.Train(trainingData.floatData)
}

func (f *faissFloat32Index) setQuantizers(trainedIndex faissIndexIVF) error {
	centroidFaissIndex, ok := trainedIndex.(*faissFloat32Index)
	if !ok {
		// if not a float32 trained index, we cannot set it as the quantizer
		// for the current index, return an error.
		return errNotSupported
	}
	return f.idx.SetQuantizers(centroidFaissIndex.idx)
}

func (f *faissFloat32Index) isMergeable() bool {
	switch f.params.optimization {
	case index.IndexOptimizedForLatency, index.IndexOptimizedForRecall:
		if f.params.fastMerge {
			return f.params.numVecs >= ivfThreshold
		}
		return f.params.numVecs >= ivfSq8Threshold
	case index.IndexOptimizedForMemoryEfficient, index.IndexIVFRaBitQ:
		return f.params.numVecs >= ivfThreshold
	default:
		return false
	}
}

func (f *faissFloat32Index) mergeFrom(other faissIndex, offset int64) error {
	o, ok := other.(*faissFloat32Index)
	if !ok || !o.isMergeable() || !f.idx.CanMergeFrom(o.idx) {
		return errIndexNotMergeable
	}
	return f.idx.MergeFrom(o.idx, offset)
}
