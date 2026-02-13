//  Copyright (c) 2023 Couchbase, Inc.
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
	"fmt"
	"math"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

func init() {
	registerSegmentSection(SectionFaissVectorIndex, &faissVectorIndexSection{})
	invertedTextIndexSectionExclusionChecks = append(invertedTextIndexSectionExclusionChecks, func(field index.Field) bool {
		_, ok := field.(index.VectorField)
		return ok
	})
	faiss.SetOMPThreads(defaultFaissOMPThreads)
}

const (
	// Set the default number of OMP threads to be used by FAISS
	// to 1 since openMP does not support goroutine based threading well.
	defaultFaissOMPThreads = 1
	// Divide the estimated nprobe with this value to optimize
	// for latency.
	nprobeLatencyOptimization = 2
)

// Vector index types currently supported.
const (
	// IndexTypeFlat is a flat index type for exact search.
	IndexTypeFlat = iota
	// IndexTypeIVF is an IVF index type for approximate search.
	IndexTypeIVF
)

// Vector index section implementation types
const (
	FaissFP32Index uint64 = iota
	FaissBIVFIndex
)

type faissVectorIndexSection struct {
}

func (v *faissVectorIndexSection) Process(opaque map[int]resetable, docNum uint32, field index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}
	if vf, ok := field.(index.VectorField); ok {
		vo := v.getVectorIndexOpaque(opaque)
		vo.process(vf, fieldID, docNum)
	}
}

func (v *faissVectorIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) error {
	vo := v.getVectorIndexOpaque(opaque)
	return vo.writeVectorIndexes(w)
}

func (v *faissVectorIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	vo := v.getVectorIndexOpaque(opaque)
	return vo.fieldAddrs[uint16(fieldID)]
}

// vecIndexInfo contains information specific to a vector index,
// including metadata and the faiss index pointer itself.
type vecIndexInfo struct {
	startOffset       int
	indexSize         uint64
	vecIds            []int64
	indexOptimizedFor string
	indexType         uint64
	index             *faissIndex
}

// Merge merges vector indexes from multiple segments into a single index.
func (v *faissVectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
	vo := v.getVectorIndexOpaque(opaque)
	// preallocating the space over here, if there are too many fields
	// in the segment this will help by avoiding multiple allocation
	// calls.
	// the segments with valid vector sections in them
	vecSegs := make([]*SegmentBase, 0, len(segments))
	// vector index information from those segments
	indexes := make([]*vecIndexInfo, 0, len(segments))
	// mapping from vector IDs to docIDs across segments
	vecToDocID := make([]uint64, 0, len(segments))
	// for every field, gather the vector indexes from the segments
	// that have them, merge them and write them out to the writer.
	for fieldID, fieldName := range fieldsInv {
		// continue if field is not required to be indexed
		if !vo.fieldsOptions[fieldName].IsIndexed() {
			continue
		}
		indexes = indexes[:0] // resizing the slices
		vecSegs = vecSegs[:0]
		vecToDocID = vecToDocID[:0]
		for segI, sb := range segments {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}
			if _, ok := sb.fieldsMap[fieldName]; !ok {
				continue
			}
			// check if the section address is a valid one for "fieldName" in the
			// segment sb. the local fieldID (fetched by the fieldsMap of the sb)
			// is to be used while consulting the fieldsSectionsMap
			pos := int(sb.fieldsSectionsMap[sb.fieldsMap[fieldName]-1][SectionFaissVectorIndex])
			if pos == 0 {
				continue
			}
			// loading doc values - adhering to the sections format. never
			// valid values for vector section
			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// read the vector index optimization type represented as an int
			indexOptimizationTypeInt, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// read the number of vectors
			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// read the length of the vector to docID map (unused for now)
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// track the valid vectors to be reconstructed for this segment
			// during the merge operation.
			newIndexInfo := &vecIndexInfo{
				indexOptimizedFor: index.VectorIndexOptimizationsReverseLookup[int(indexOptimizationTypeInt)],
				vecIds:            make([]int64, 0, numVecs),
				index:             &faissIndex{},
			}
			for vecID := 0; vecID < int(numVecs); vecID++ {
				docID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n
				// check if this docID is dropped in the new segment
				newDocID := newDocNumsIn[segI][uint32(docID)]
				if newDocID != docDropped {
					// valid docID, track the mapping
					vecToDocID = append(vecToDocID, newDocID)
					// if the remapped doc ID is valid, track it
					// as part of vecs to be reconstructed (for larger indexes).
					// This accounts only for valid vector IDs, so deleted
					// ones won't be reconstructed in the final index.
					newIndexInfo.vecIds = append(newIndexInfo.vecIds, int64(vecID))
				}
			}
			if len(newIndexInfo.vecIds) == 0 {
				// no valid vectors to be merged from this segment
				continue
			}
			// read the type of vector index
			indexType, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// read the size of the vector index
			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// record the start offset and size of the vector index
			newIndexInfo.startOffset = pos
			newIndexInfo.indexSize = indexSize
			newIndexInfo.indexType = indexType
			vecSegs = append(vecSegs, sb)
			indexes = append(indexes, newIndexInfo)
			pos += int(indexSize)
		}
		// continue if there are absolutely no valid vectors present in the segment
		// for this field and crucially don't store the section start offset in it
		if len(indexes) == 0 || len(vecToDocID) == 0 {
			continue
		}
		err := vo.flushSectionMetadata(fieldID, w, vecToDocID, indexes)
		if err != nil {
			return err
		}
		err = vo.mergeAndWriteVectorIndexes(vecSegs, indexes, w, closeCh)
		if err != nil {
			return err
		}
	}
	return nil
}

func (v *vectorIndexOpaque) flushSectionMetadata(fieldID int, w *CountHashWriter,
	vecToDocID []uint64, indexes []*vecIndexInfo) error {
	tempBuf := v.grabBuf(binary.MaxVarintLen64)
	fieldStart := w.Count()
	// marking the fact that for vector index, doc values are not valid by
	// storing fieldNotUninverted values.
	n := binary.PutUvarint(tempBuf, fieldNotUninverted)
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	n = binary.PutUvarint(tempBuf, fieldNotUninverted)
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	// write the index optimization type
	n = binary.PutUvarint(tempBuf, uint64(index.SupportedVectorIndexOptimizations[indexes[0].indexOptimizedFor]))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	// write the number of vectors
	n = binary.PutUvarint(tempBuf, uint64(len(vecToDocID)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	// write the size of the vector to docID map (unused for now)
	n = binary.PutUvarint(tempBuf, 0)
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	// write the vecID -> docID mapping
	for _, docID := range vecToDocID {
		// write the docID, with vecID being implicit from the order of addition
		// i.e., 0 to N-1
		n = binary.PutUvarint(tempBuf, docID)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
	}
	// record the fieldStart value for this section.
	v.fieldAddrs[uint16(fieldID)] = fieldStart
	return nil
}

// calculateNprobe calculates the nprobe count given nlist (number of centroids)
// based on the metric the index is optimized for.
func calculateNprobe(nlist int, indexOptimizedFor string) int32 {
	nprobe := int32(math.Sqrt(float64(nlist)))
	if indexOptimizedFor == index.IndexOptimizedForLatency {
		nprobe /= nprobeLatencyOptimization
		if nprobe < 1 {
			nprobe = 1
		}
	}
	return nprobe
}

// todo: naive implementation. need to keep in mind the perf implications and improve on this.
// perhaps, parallelized merging can help speed things up over here.
func (v *vectorIndexOpaque) mergeAndWriteVectorIndexes(sbs []*SegmentBase,
	vecIndexes []*vecIndexInfo, w *CountHashWriter, closeCh chan struct{}) error {
	// safe to assume that all the indexes are of the same config values, given
	// that they are extracted from the field mapping info.
	var dims, metric, indexDataCap, reconsCap int
	var indexOptimizedFor string
	var indexType uint64
	var validMerge bool
	for segI, segBase := range sbs {
		// Considering merge operations on vector indexes are expensive, it is
		// worth including an early exit if the merge is aborted, saving us
		// the resource spikes, even if temporary.
		if isClosed(closeCh) {
			freeReconstructedIndexes(vecIndexes)
			return seg.ErrClosed
		}
		// track which index we are currently processing
		currVecIndex := vecIndexes[segI]
		currNumVecs := len(currVecIndex.vecIds)
		// if no valid vectors for this index, don't bring it into memory
		if currNumVecs == 0 {
			continue
		}
		// read the serialized index bytes
		indexBytes := segBase.mem[currVecIndex.startOffset : currVecIndex.startOffset+int(currVecIndex.indexSize)]
		// reconstruct the faiss index from the bytes
		faissIndex, err := faiss.ReadIndexFromBuffer(indexBytes, faissIOFlags)
		if err != nil {
			freeReconstructedIndexes(vecIndexes)
			return err
		}
		// set the dims and metric values from the constructed index.
		dims = faissIndex.D()
		// at least one valid index to be merged, mark the merge as valid.
		validMerge = true
		metric = faissIndex.MetricType()
		indexOptimizedFor = currVecIndex.indexOptimizedFor
		indexType = currVecIndex.indexType
		// update trackers for buffer capacities
		indexReconsLen := len(currVecIndex.vecIds) * dims
		if indexReconsLen > reconsCap {
			reconsCap = indexReconsLen
		}
		indexDataCap += indexReconsLen
		// update the index currently being processed to store the faiss index.
		currVecIndex.index.fIndex = faissIndex
	}
	// not a valid merge operation as there are no valid indexes to merge.
	if !validMerge {
		return nil
	}
	// merging of indexes with reconstruction method.
	// the vecIds in each index contain only the valid vectors,
	// so we reconstruct only those.
	indexData := make([]float32, 0, indexDataCap)
	// reusable buffer for reconstruction
	recons := make([]float32, 0, reconsCap)
	// total number of vectors in the vector index after merge
	// used to determine the index type to be created.
	nvecs := 0
	for _, currVecIndex := range vecIndexes {
		if isClosed(closeCh) {
			freeReconstructedIndexes(vecIndexes)
			return seg.ErrClosed
		}
		currNumVecs := len(currVecIndex.vecIds)
		currFaissIndex := currVecIndex.index.fIndex
		// reconstruct the vectors only if present, it could be that
		// some of the indexes had all of their vectors updated/deleted.
		if currNumVecs > 0 && currFaissIndex != nil {
			neededReconsLen := currNumVecs * dims
			recons = recons[:neededReconsLen]
			var err error
			recons, err = currFaissIndex.ReconstructBatch(currVecIndex.vecIds, recons)
			if err != nil {
				freeReconstructedIndexes(vecIndexes)
				return err
			}
			indexData = append(indexData, recons...)
			nvecs += currNumVecs
		}
	}
	// if no valid vectors after merge, nothing to do
	if nvecs == 0 {
		// no valid vectors for this index, so we don't even have to
		// record it in the section
		freeReconstructedIndexes(vecIndexes)
		return nil
	}
	// freeing the reconstructed indexes immediately - waiting till the end
	// to do the same is not needed because the following operations don't need
	// the reconstructed ones anymore and doing so will hold up memory which can
	// be detrimental while creating indexes during introduction.
	freeReconstructedIndexes(vecIndexes)
	// create the faiss index to hold the merged data, and add the
	// reconstructed vectors into it.
	fIndexBytes, err := makeFaissFloatIndex(indexData, metric, indexOptimizedFor,
		dims, nvecs)
	if err != nil {
		return err
	}

	// get a temporary buffer for writing out the index
	tempBuf := v.grabBuf(binary.MaxVarintLen64)
	// write the type of the vector index
	n := binary.PutUvarint(tempBuf, uint64(indexType))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	// write the length of the serialized vector index bytes
	n = binary.PutUvarint(tempBuf, uint64(len(fIndexBytes)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	// write the vector index data
	_, err = w.Write(fIndexBytes)

	if indexType == FaissBIVFIndex {
		// create the binary index to hold the merged data, and
		// add the reconstructed vectors into it.
		bIndexBytes, err := makeFaissBinaryIndex(indexData, indexOptimizedFor,
			dims, nvecs)
		if err != nil {
			return err
		}

		// write the length of the serialized binary vector index bytes
		n = binary.PutUvarint(tempBuf, uint64(len(bIndexBytes)))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the binary vector index data
		_, err = w.Write(bIndexBytes)
		if err != nil {
			return err
		}
	}

	return err
}

// returns the serialized faiss index bytes for the given vector data and index config.
func makeFaissFloatIndex(vecs []float32, metric int, indexOptimizedFor string,
	dims int, nvecs int) ([]byte, error) {

	nlist := determineCentroids(nvecs)
	description, indexClass := determineFloatIndexToUse(nvecs, nlist, indexOptimizedFor)

	index, err := faiss.IndexFactory(dims, description, metric)
	if err != nil {
		return nil, err
	}
	// ensure the faiss index is closed after use
	defer index.Close()

	// if we are using an IVF index, set the direct map and train it
	if indexClass == IndexTypeIVF {
		// the direct map maintained in the IVF index is essential for the
		// reconstruction of vectors based on the sequential vector IDs in the
		// future merges use direct map type 1 -> array based direct map, since
		// we have sequential vector IDs starting from 0 to N-1.
		err = index.SetDirectMap(1)
		if err != nil {
			return nil, err
		}

		// calculate nprobe using a heuristic.
		nprobe := calculateNprobe(nlist, indexOptimizedFor)
		index.SetNProbe(nprobe)

		// train the vector index, essentially performs k-means clustering to partition
		// the data space of indexData such that during the search time, we probe
		// only a subset of vectors -> non-exhaustive search. could be a time
		// consuming step when the indexData is large.
		err = index.Train(vecs)
		if err != nil {
			return nil, err
		}
	}
	// add the vectors to the index using sequential vector IDs starting
	// from 0 to N-1
	err = index.Add(vecs)
	if err != nil {
		return nil, err
	}

	// serialize the merged index into a byte slice, and write it out
	indexBytes, err := faiss.WriteIndexIntoBuffer(index)
	if err != nil {
		return nil, err
	}

	return indexBytes, nil
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

// returns the index description string and index type constant for the binary
// index to be created based on the number of vectors and centroids.
func determineBinaryIndexToUse(nvecs, nlist int) (string, int) {
	switch {
	case nvecs >= 1000:
		return fmt.Sprintf("BIVF%d", nlist), IndexTypeIVF
	default:
		return "BFlat", IndexTypeFlat
	}
}

// returns the index type constant for the vector index to be created based on the
// index optimization type specified in the field mapping.
func determineIndexTypeFromOptimization(indexOptimizedFor string) uint64 {
	if indexOptimizedFor == index.IndexOptimizedWithBivfFlat {
		return FaissBIVFIndex
	}
	return FaissFP32Index
}

// returns the serialized faiss binary index bytes for the given vector data and index config.
func makeFaissBinaryIndex(vecs []float32, indexOptimizedFor string, dims int,
	nvecs int) ([]byte, error) {

	nlist := determineCentroids(nvecs)
	description, indexClass := determineBinaryIndexToUse(nvecs, nlist)

	index, err := faiss.BinaryIndexFactory(dims, description)
	if err != nil {
		return nil, err
	}
	// ensure the faiss index is closed after use
	defer index.Close()

	// convert the float32 vectors into binary format
	bvecs := convertToBinary(vecs, dims)

	// if we are using an IVF index, set the direct map and train it
	if indexClass == IndexTypeIVF {
		// the direct map maintained in the IVF index is essential for the
		// reconstruction of vectors based on the sequential vector IDs in the
		// future merges use direct map type 1 -> array based direct map, since
		// we have sequential vector IDs starting from 0 to N-1.
		err = index.SetDirectMap(1)
		if err != nil {
			return nil, err
		}

		// calculate nprobe using a heuristic.
		nprobe := calculateNprobe(nlist, indexOptimizedFor)
		index.SetNProbe(nprobe)

		// train the vector index, essentially performs k-means clustering to partition
		// the data space of indexData such that during the search time, we probe
		// only a subset of vectors -> non-exhaustive search. could be a time
		// consuming step when the indexData is large.
		err = index.Train(bvecs)
		if err != nil {
			return nil, err
		}
	}

	err = index.Add(bvecs)
	if err != nil {
		return nil, err
	}

	// serialize the merged binary index into a byte slice, and write it out
	mergedBIndexBytes, err := faiss.WriteBinaryIndexIntoBuffer(index)
	if err != nil {
		return nil, err
	}

	return mergedBIndexBytes, nil
}

// freeReconstructedIndexes closes all faiss indexes in the provided slice.
func freeReconstructedIndexes(indexes []*vecIndexInfo) {
	for _, entry := range indexes {
		if entry.index != nil {
			entry.index.close()
		}
	}
}

// grabBuf returns a reusable buffer of the given size, allocating a new one if needed.
func (v *vectorIndexOpaque) grabBuf(size int) []byte {
	buf := v.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		v.tmp0 = buf
	}
	return buf[:size]
}

// determineCentroids determines the number of centroids to use for an IVF index.
func determineCentroids(nvecs int) int {
	var nlist int
	switch {
	case nvecs >= 200000:
		nlist = int(4 * math.Sqrt(float64(nvecs)))
	case nvecs >= 1000:
		// 100 points per cluster is a reasonable default, considering the default
		// minimum and maximum points per cluster is 39 and 256 respectively.
		// Since it's a recommendation to have a minimum of 10 clusters, 1000(100 * 10)
		// was chosen as the lower threshold.
		nlist = nvecs / 100
	}
	return nlist
}

// determineIndexToUse returns a description string for the index and quantizer type,
// and an index type constant.
func determineFloatIndexToUse(nvecs, nlist int, indexOptimizedFor string) (string, int) {
	switch indexOptimizedFor {
	case index.IndexOptimizedWithBivfFlat:
		return "Flat", IndexTypeFlat
	case index.IndexOptimizedForMemoryEfficient:
		switch {
		case nvecs >= 1000:
			return fmt.Sprintf("IVF%d,SQ4", nlist), IndexTypeIVF
		default:
			return "Flat", IndexTypeFlat
		}
	default:
		switch {
		case nvecs >= 10000:
			return fmt.Sprintf("IVF%d,SQ8", nlist), IndexTypeIVF
		case nvecs >= 1000:
			return fmt.Sprintf("IVF%d,Flat", nlist), IndexTypeIVF
		default:
			return "Flat", IndexTypeFlat
		}
	}
}

func (vo *vectorIndexOpaque) writeVectorIndexes(w *CountHashWriter) error {
	// for every fieldID, contents to store over here are:
	//    1. the serialized representation of the dense vector index.
	//    2. its constituent metadata like:
	//        a. number of vectors
	//        b. dimension of vectors
	//        c. distance metric
	//        d. index optimization type
	//        e. vectorID -> docID mapping
	tempBuf := vo.grabBuf(binary.MaxVarintLen64)
	for fieldID, content := range vo.fieldVectorIndex {
		// number of vectors to be indexed for this field
		nvecs := len(content.vecDocIDs)
		// Set the faiss metric type (default is Euclidean Distance or l2_norm)
		metric := faiss.MetricL2
		if content.metric == index.InnerProduct || content.metric == index.CosineSimilarity {
			// use the same FAISS metric for inner product and cosine similarity
			metric = faiss.MetricInnerProduct
		}
		fIndexBytes, err := makeFaissFloatIndex(content.vectors, metric, content.optimizedFor,
			len(content.vecDocIDs), nvecs)
		if err != nil {
			return err
		}

		indexType := determineIndexTypeFromOptimization(content.optimizedFor)

		// record the fieldStart value for this section.
		fieldStart := w.Count()
		// writing out two offset values to indicate that the current field's
		// vector section doesn't have valid doc value content within it.
		n := binary.PutUvarint(tempBuf, fieldNotUninverted)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		n = binary.PutUvarint(tempBuf, fieldNotUninverted)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the index optimization type
		n = binary.PutUvarint(tempBuf, uint64(index.SupportedVectorIndexOptimizations[content.optimizedFor]))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the number of vectors
		n = binary.PutUvarint(tempBuf, uint64(nvecs))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the size of the vector to docID map (unused for now)
		n = binary.PutUvarint(tempBuf, 0)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the vecID -> docID mapping
		for _, docID := range content.vecDocIDs {
			// write docIDs associated with every vector, with vecID being
			// implicit from the order of addition, i.e., 0 to N-1
			n = binary.PutUvarint(tempBuf, uint64(docID))
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return err
			}
		}
		// write the type of the vector index
		n = binary.PutUvarint(tempBuf, uint64(indexType))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the length of the serialized vector index bytes
		n = binary.PutUvarint(tempBuf, uint64(len(fIndexBytes)))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the vector index data
		_, err = w.Write(fIndexBytes)
		if err != nil {
			return err
		}
		if indexType == FaissBIVFIndex {
			bIndexBytes, err := makeFaissBinaryIndex(content.vectors, content.optimizedFor,
				content.dimension, nvecs)
			if err != nil {
				return err
			}

			// write the length of the serialized binary vector index bytes
			n = binary.PutUvarint(tempBuf, uint64(len(bIndexBytes)))
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return err
			}

			// write the binary vector index data
			_, err = w.Write(bIndexBytes)
			if err != nil {
				return err
			}

		}
		// accounts for whatever data has been written out to the writer.
		vo.incrementBytesWritten(uint64(w.Count() - fieldStart))
		vo.fieldAddrs[fieldID] = fieldStart
	}
	return nil
}

func (vo *vectorIndexOpaque) process(field index.VectorField, fieldID uint16, docNum uint32) {
	if fieldID == math.MaxUint16 {
		// doc processing checkpoint - no action needed
		return
	}
	vec := field.Vector()
	dim := field.Dims()
	metric := field.Similarity()
	indexOptimizedFor := field.IndexOptimizedFor()
	// caller is supposed to make sure len(vec) is a multiple of dim.
	// Not double checking it here to avoid the overhead.
	// This accounts for multi-vector fields, where a field can have
	// multiple vectors associated with it. In this case we process all
	// vectors associated with the field as separate vectors.
	numVectors := len(vec) / dim
	for i := 0; i < numVectors; i++ {
		vector := vec[i*dim : (i+1)*dim]
		// check if we have content for this fieldID already
		content, ok := vo.fieldVectorIndex[fieldID]
		if !ok {
			// create an entry for this fieldID as this is the first time
			// we are seeing this field
			content = &vectorIndexContent{
				dimension:    dim,
				metric:       metric,
				optimizedFor: indexOptimizedFor,
				vectors:      make([]float32, 0, dim*numVectors),
				vecDocIDs:    make([]uint32, 0, numVectors),
			}
			vo.fieldVectorIndex[fieldID] = content
		}
		// track the vector data and docIDs
		content.vectors = append(content.vectors, vector...)
		content.vecDocIDs = append(content.vecDocIDs, docNum)
	}
}

func (v *faissVectorIndexSection) getVectorIndexOpaque(opaque map[int]resetable) *vectorIndexOpaque {
	if _, ok := opaque[SectionFaissVectorIndex]; !ok {
		opaque[SectionFaissVectorIndex] = v.InitOpaque(nil)
	}
	return opaque[SectionFaissVectorIndex].(*vectorIndexOpaque)
}

func (v *faissVectorIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &vectorIndexOpaque{
		fieldAddrs:       make(map[uint16]int),
		fieldVectorIndex: make(map[uint16]*vectorIndexContent),
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

// vectorIndexContent contains the information required to create a vector index for a vector field.
type vectorIndexContent struct {
	// vectors stores flattened vectors in a row-major order
	vectors []float32
	// vecDocIDs corresponding to each vector
	vecDocIDs []uint32
	// dimension is the dimension of all vectors
	dimension int
	// metric is the distance metric to be used
	metric string
	// optimizedFor is the optimization type for the index
	optimizedFor string
}

// vectorIndexOpaque holds the internal state for vector index processing.
type vectorIndexOpaque struct {
	bytesWritten uint64
	// fieldAddrs maps fieldID to the address of its vector section
	fieldAddrs map[uint16]int
	// fieldVectorIndex maps fieldID to its vector index content
	fieldVectorIndex map[uint16]*vectorIndexContent
	// fieldsOptions contains field indexing options
	fieldsOptions map[string]index.FieldIndexingOptions
	// tmp0 is a reusable buffer
	tmp0 []byte
}

func (vo *vectorIndexOpaque) incrementBytesWritten(val uint64) {
	atomic.AddUint64(&vo.bytesWritten, val)
}

func (vo *vectorIndexOpaque) BytesWritten() uint64 {
	return atomic.LoadUint64(&vo.bytesWritten)
}

func (vo *vectorIndexOpaque) BytesRead() uint64 {
	return 0
}

func (vo *vectorIndexOpaque) ResetBytesRead(uint64) {
}

// Reset clears all state in the vectorIndexOpaque for reuse.
func (vo *vectorIndexOpaque) Reset() error {
	clear(vo.fieldAddrs)
	clear(vo.fieldVectorIndex)
	vo.tmp0 = vo.tmp0[:0]
	vo.fieldsOptions = nil
	atomic.StoreUint64(&vo.bytesWritten, 0)
	return nil
}

func (v *vectorIndexOpaque) Set(key string, val interface{}) {
	switch key {
	case "fieldsOptions":
		v.fieldsOptions = val.(map[string]index.FieldIndexingOptions)
	}
}
