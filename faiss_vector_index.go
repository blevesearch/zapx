//  Copyright (c) 2024 Couchbase, Inc.
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

	"github.com/blevesearch/go-faiss"
)

// Abstract interface for Faiss vector indices, which are returned by the go-faiss library.
type faissIndex interface {
	// adds the given vectors to the index.
	add(vecs *vectorSet) error
	// searializes the index into a byte slice,
	// which can be stored or transmitted.
	serialize() ([]byte, error)
	// closes the index and releases any associated resources.
	close()
	// returns the size of the index in bytes.
	size() uint64
	// returns the underlying IVF index if this is an IVF index,
	// and a boolean indicating whether the cast was successful.
	castIVF() (index faissIndexIVF, ok bool)
	// returns the underlying SQ index if this is an SQ index,
	// and a boolean indicating whether the cast was successful.
	castSQ() (index faissIndexSQ, ok bool)
}

// Interface for Faiss vector indices that support reconstruction of vectors based on their IDs.
type faissReconstructIndex interface {
	faissIndex
	// reconstructs the vectors corresponding to the given vector IDs.
	// This is essential for merge operations, where we need to reconstruct
	// vectors from existing indexes to add them to the new merged index.
	reconstruct(vecIds []int64) ([]float32, error)
}

// Interface for IVF-specific operations on Faiss vector indices.
type faissIndexIVF interface {
	// sets the direct map type for the IVF index. The direct map is essential for
	// reconstructing vectors based on their sequential vector IDs in future merges.
	setDirectMap(directMapType int) error
	// sets the number of probes (nprobe) for the IVF index. nprobe determines how many
	// inverted lists are probed during search, and is a key parameter that controls the
	// trade-off between search accuracy and latency.
	setNProbe(nprobe int32)
	// trains the IVF index on the provided training data. This step is necessary to
	// perform k-means clustering to partition the data space of the index, which enables
	// efficient non-exhaustive search during query time.
	train(trainingData *vectorSet) error
}

type faissIndexSQ interface {
	// trains the SQ index on the provided training data. This step is necessary to
	// perform quantization of the vector space, which enables efficient storage and
	// search of high-dimensional vectors.
	train(trainingData *vectorSet) error
}

// ---------------------------------
// CPU Faiss Float32 Index
// ---------------------------------
type faissFloat32Index struct {
	idx *faiss.IndexImpl
}

func newFaissFloat32Index(idx *faiss.IndexImpl) (index faissIndex, err error) {
	return &faissFloat32Index{
		idx: idx,
	}, nil
}

func (f *faissFloat32Index) castIVF() (index faissIndexIVF, ok bool) {
	if f.idx.IsIVFIndex() {
		// return f itself, as the IVF interface is implemented by the same
		// struct as the non-IVF interface in go-faiss.
		return f, true
	}
	// not an IVF index, return nil and false.
	return nil, false
}

func (f *faissFloat32Index) castSQ() (index faissIndexSQ, ok bool) {
	if f.idx.IsSQIndex() {
		// return f itself, as the SQ interface is implemented by the same
		// struct as the non-SQ interface in go-faiss.
		return f, true
	}
	// not an SQ index, return nil and false.
	return nil, false
}

func (f *faissFloat32Index) close() {
	if f.idx != nil {
		f.idx.Close()
	}
}

func (f *faissFloat32Index) setDirectMap(directMapType int) error {
	return f.idx.SetDirectMap(directMapType)
}

func (f *faissFloat32Index) setNProbe(nprobe int32) {
	f.idx.SetNProbe(nprobe)
}

func (f *faissFloat32Index) add(vecs *vectorSet) error {
	// add the floatData from the vectorSet to the index
	return f.idx.Add(vecs.floatData)
}

func (f *faissFloat32Index) train(trainingData *vectorSet) error {
	// train the index using the floatData from the vectorSet
	return f.idx.Train(trainingData.floatData)
}

func (f *faissFloat32Index) serialize() ([]byte, error) {
	// serialize the index into a byte slice using go-faiss's WriteIndexToBuffer method
	return faiss.WriteIndexIntoBuffer(f.idx)
}

func (f *faissFloat32Index) size() uint64 {
	if f.idx == nil {
		return 0
	}
	return f.idx.Size()
}

// ---------------------------------
// CPU Faiss Binary Index
// ---------------------------------
type faissBinaryIndex struct {
	backing *faiss.IndexImpl
	binary  *faiss.BinaryIndexImpl
}

func newFaissBinaryIndex(binary *faiss.BinaryIndexImpl, backing *faiss.IndexImpl) (index faissIndex, err error) {
	return &faissBinaryIndex{
		backing: backing,
		binary:  binary,
	}, nil
}

func (b *faissBinaryIndex) castIVF() (index faissIndexIVF, ok bool) {
	if b.binary.IsIVFIndex() {
		// return b itself, as the IVF interface is implemented by the same
		// struct as the non-IVF interface in go-faiss.
		return b, true
	}
	// not an IVF index, return nil and false.
	return nil, false
}

// Binary indexes don't support SQ, so this always returns nil and false.
func (b *faissBinaryIndex) castSQ() (index faissIndexSQ, ok bool) {
	return nil, false
}

func (b *faissBinaryIndex) close() {
	if b.binary != nil {
		b.binary.Close()
	}
	if b.backing != nil {
		b.backing.Close()
	}
}

func (b *faissBinaryIndex) setDirectMap(directMapType int) error {
	return b.binary.SetDirectMap(directMapType)
}

func (b *faissBinaryIndex) setNProbe(nprobe int32) {
	b.binary.SetNProbe(nprobe)
}

func (b *faissBinaryIndex) add(vecs *vectorSet) error {
	// add the binaryData from the vectorSet to the binary index
	return b.binary.Add(vecs.binaryData)
}

func (b *faissBinaryIndex) train(trainingData *vectorSet) error {
	// train the binary index using the binaryData from the vectorSet
	return b.binary.Train(trainingData.binaryData)
}

func (b *faissBinaryIndex) serialize() ([]byte, error) {
	// serialize the binary index into a byte slice using go-faiss's WriteIndexToBuffer method
	return faiss.WriteBinaryIndexIntoBuffer(b.binary)
}

func (b *faissBinaryIndex) size() uint64 {
	var sizeInBytes uint64
	if b.binary != nil {
		sizeInBytes += b.binary.Size()
	}
	if b.backing != nil {
		sizeInBytes += b.backing.Size()
	}
	return sizeInBytes
}

// ----------------------------------
// vectorSet
// ----------------------------------
type vectorSet struct {
	// dimensionality of each vector
	dim int
	// number of vectors represented
	nvecs int
	// float vectors stored in row-major format,
	// i.e. for N vectors of D dimensions,
	// the length of this slice is N*D,
	floatData []float32
	// row-major binary representation of the float vectors,
	// where each bit represents the sign bit
	// of the corresponding float value.
	binaryData []uint8
}

func newVectorSet(dim int, data []float32) (*vectorSet, error) {
	if len(data) != dim || dim <= 0 {
		return nil, fmt.Errorf("invalid vector data: dims %d, data length %d", dim, len(data))
	}
	nvecs := len(data) / dim
	return &vectorSet{
		dim:       dim,
		nvecs:     nvecs,
		floatData: data,
	}, nil
}

// converts float32 vectors into binary format based on the sign bit
// of the float32 values.
func convertToBinary(vecs []float32, dims int) []uint8 {
	nvecs := len(vecs) / dims
	packed := make([]uint8, 0, nvecs*(dims+7)/8)
	var cur uint8
	var count int
	for i := 0; i < nvecs; i++ {
		count = 0
		for j := 0; j < dims; j++ {
			value := vecs[i*dims+j]
			// Apply the threshold: convert the float32 to 1 or 0 based on threshold
			if value >= 0.0 {
				// Shift the bit into the correct position in the byte
				cur |= (1 << (7 - count))
			}
			count++
			// When we have 8 bits, store the byte and reset for the next byte
			if count == 8 {
				packed = append(packed, cur)
				cur = 0
				count = 0
			}
		}
		// If there are any remaining bits, pack them into a byte and append
		if count > 0 {
			cur <<= (8 - count)
			packed = append(packed, cur)
		}
	}
	return packed
}

func (v *vectorSet) binarize() {
	// if binaryData is already populated, no need to convert again
	if v.binaryData != nil {
		return
	}
	// convert the floatData to binary format and store in binaryData
	v.binaryData = convertToBinary(v.floatData, v.dim)
}
