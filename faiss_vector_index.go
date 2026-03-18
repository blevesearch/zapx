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

import "github.com/blevesearch/go-faiss"

// Abstract interface for Faiss vector indices, which are returned by the go-faiss library.
type FaissVectorIndex interface {
	// returns the underlying IVF index if this is an IVF index,
	// and a boolean indicating whether the cast was successful.
	castIVF() (index FaissVectorIndexIVF, ok bool)
}

type FaissVectorIndexIVF interface {
}

// ---------------------------------
// CPU Faiss Float32 Index
// ---------------------------------
type faissFloat32Index struct {
	idx *faiss.IndexImpl
}

func newFaissFloat32Index(idx *faiss.IndexImpl) (index FaissVectorIndex, err error) {
	return &faissFloat32Index{
		idx: idx,
	}, nil
}

func (f *faissFloat32Index) castIVF() (index FaissVectorIndexIVF, ok bool) {
	if f.idx.IsIVFIndex() {
		// return f itself, as the IVF interface is implemented by the same
		// struct as the non-IVF interface in go-faiss.
		return f, true
	}
	// not an IVF index, return nil and false.
	return nil, false
}

// ---------------------------------
// CPU Faiss Binary Index
// ---------------------------------
type faissBinaryIndex struct {
	backing *faiss.IndexImpl
	binary  *faiss.BinaryIndexImpl
}

func newFaissBinaryIndex(backing *faiss.IndexImpl, binary *faiss.BinaryIndexImpl) (index FaissVectorIndex, err error) {
	return &faissBinaryIndex{
		backing: backing,
		binary:  binary,
	}, nil
}

func (b *faissBinaryIndex) castIVF() (index FaissVectorIndexIVF, ok bool) {
	if b.binary.IsIVFIndex() {
		// return b itself, as the IVF interface is implemented by the same
		// struct as the non-IVF interface in go-faiss.
		return b, true
	}
	// not an IVF index, return nil and false.
	return nil, false
}
