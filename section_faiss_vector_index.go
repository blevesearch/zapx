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
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

const defaultFaissOMPThreads = 1

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	registerSegmentSection(SectionFaissVectorIndex, &faissVectorIndexSection{})
	invertedTextIndexSectionExclusionChecks = append(invertedTextIndexSectionExclusionChecks, func(field index.Field) bool {
		_, ok := field.(index.VectorField)
		return ok
	})
	faiss.SetOMPThreads(defaultFaissOMPThreads)
}

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

// information specific to a vector index - (including metadata and
// the index pointer itself)
type vecIndexInfo struct {
	startOffset       int
	indexSize         uint64
	vecIds            []int64
	indexOptimizedFor string
	index             *faiss.IndexImpl
}

// keep in mind with respect to update and delete operations with respect to vectors
func (v *faissVectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
	vo := v.getVectorIndexOpaque(opaque)

	// the segments with valid vector sections in them
	// preallocating the space over here, if there are too many fields
	// in the segment this will help by avoiding multiple allocation
	// calls.
	vecSegs := make([]*SegmentBase, 0, len(segments))
	indexes := make([]*vecIndexInfo, 0, len(segments))
	vecToDocID := make([]uint64, 0, len(segments))

	for fieldID, fieldName := range fieldsInv {
		indexes = indexes[:0] // resizing the slices
		vecSegs = vecSegs[:0]
		vecToDocID = vecToDocID[:0]

		// todo: would parallely fetching the following stuff from segments
		// be beneficial in terms of perf?
		for segI, sb := range segments {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}
			if _, ok := sb.fieldsMap[fieldName]; !ok {
				continue
			}
			// early exit if field is not required to be indexed
			if !vo.fieldsOptions[fieldName].IsIndexed() {
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

			// read the vector optimized for value
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			// the index optimization type represented as an int
			indexOptimizationTypeInt, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

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
			}
			for i := 0; i < int(numVecs); i++ {
				docID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n
				// check if this docID is dropped in the new segment
				newDocID := newDocNumsIn[segI][uint32(docID)]
				if newDocID != docDropped {
					// valid docID, track the mapping
					vecToDocID = append(vecToDocID, newDocID)
					// if the remapped doc ID is valid, track it
					// as part of vecs to be reconstructed (for larger indexes).
					// this would account only the valid vector IDs, so the deleted
					// ones won't be reconstructed in the final index.
					newIndexInfo.vecIds = append(newIndexInfo.vecIds, int64(i))
				}
			}

			// read the type of vector index (unused for now)
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// read the size of the vector index
			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			// record the start offset and size of the vector index
			newIndexInfo.startOffset = pos
			newIndexInfo.indexSize = indexSize
			vecSegs = append(vecSegs, sb)
			indexes = append(indexes, newIndexInfo)
			pos += int(indexSize)
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

	// early exit if there are absolutely no valid vectors present in the segment
	// and crucially don't store the section start offset in it
	if len(indexes) == 0 || len(vecToDocID) == 0 {
		return nil
	}
	fieldStart := w.Count()
	// marking the fact that for vector index, doc values isn't valid by
	// storing fieldNotUniverted values.
	n := binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return err
	}
	n = binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

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

	for _, docID := range vecToDocID {
		// write the docID, with vecID being implicit from the order of addition
		// i.e., 0 to N-1
		n = binary.PutUvarint(tempBuf, docID)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
	}

	v.fieldAddrs[uint16(fieldID)] = fieldStart
	return nil
}

func (v *vectorIndexOpaque) flushVectorIndex(indexBytes []byte, w *CountHashWriter) error {
	tempBuf := v.grabBuf(binary.MaxVarintLen64)

	// write the type of the vector index (unused for now)
	n := binary.PutUvarint(tempBuf, 0)
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	n = binary.PutUvarint(tempBuf, uint64(len(indexBytes)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	// write the vector index data
	_, err = w.Write(indexBytes)
	return err
}

// Divide the estimated nprobe with this value to optimize
// for latency.
const nprobeLatencyOptimization = 2

// Calculates the nprobe count, given nlist(number of centroids) based on
// the metric the index is optimized for.
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
	var dims, metric int
	var indexOptimizedFor string

	var validMerge bool
	var indexDataCap, reconsCap int
	for segI, segBase := range sbs {
		// Considering merge operations on vector indexes are expensive, it is
		// worth including an early exit if the merge is aborted, saving us
		// the resource spikes, even if temporary.
		if isClosed(closeCh) {
			freeReconstructedIndexes(vecIndexes)
			return seg.ErrClosed
		}
		if len(vecIndexes[segI].vecIds) == 0 {
			// no valid vectors for this index, don't bring it into memory
			continue
		}

		// read the index bytes. todo: parallelize this
		indexBytes := segBase.mem[vecIndexes[segI].startOffset : vecIndexes[segI].startOffset+int(vecIndexes[segI].indexSize)]
		index, err := faiss.ReadIndexFromBuffer(indexBytes, faissIOFlags)
		if err != nil {
			freeReconstructedIndexes(vecIndexes)
			return err
		}
		if len(vecIndexes[segI].vecIds) > 0 {
			indexReconsLen := len(vecIndexes[segI].vecIds) * index.D()
			if indexReconsLen > reconsCap {
				reconsCap = indexReconsLen
			}
			indexDataCap += indexReconsLen
		}
		vecIndexes[segI].index = index

		validMerge = true
		// set the dims and metric values from the constructed index.
		dims = index.D()
		metric = int(index.MetricType())
		indexOptimizedFor = vecIndexes[segI].indexOptimizedFor
	}

	// not a valid merge operation as there are no valid indexes to merge.
	if !validMerge {
		return nil
	}
	// merging of indexes with reconstruction method.
	// the indexes[i].vecIds has only the valid vecs of this vector
	// index present in it, so we'd be reconstructing only those.
	indexData := make([]float32, 0, indexDataCap)
	// reusable buffer for reconstruction
	recons := make([]float32, 0, reconsCap)
	var err error
	for i := 0; i < len(vecIndexes); i++ {
		if isClosed(closeCh) {
			freeReconstructedIndexes(vecIndexes)
			return seg.ErrClosed
		}

		// reconstruct the vectors only if present, it could be that
		// some of the indexes had all of their vectors updated/deleted.
		if len(vecIndexes[i].vecIds) > 0 {
			neededReconsLen := len(vecIndexes[i].vecIds) * dims
			recons = recons[:neededReconsLen]
			// todo: parallelize reconstruction
			recons, err = vecIndexes[i].index.ReconstructBatch(vecIndexes[i].vecIds, recons)
			if err != nil {
				freeReconstructedIndexes(vecIndexes)
				return err
			}
			indexData = append(indexData, recons...)
		}
	}

	if len(indexData) == 0 {
		// no valid vectors for this index, so we don't even have to
		// record it in the section
		freeReconstructedIndexes(vecIndexes)
		return nil
	}

	nvecs := len(indexData) / dims

	// index type to be created after merge based on the number of vectors
	// in indexData added into the index.
	nlist := determineCentroids(nvecs)
	indexDescription, indexClass := determineIndexToUse(nvecs, nlist, indexOptimizedFor)

	// freeing the reconstructed indexes immediately - waiting till the end
	// to do the same is not needed because the following operations don't need
	// the reconstructed ones anymore and doing so will hold up memory which can
	// be detrimental while creating indexes during introduction.
	freeReconstructedIndexes(vecIndexes)

	faissIndex, err := faiss.IndexFactory(dims, indexDescription, metric)
	if err != nil {
		return err
	}
	defer faissIndex.Close()

	if indexClass == IndexTypeIVF {
		// the direct map maintained in the IVF index is essential for the
		// reconstruction of vectors based on the sequential vector IDs in the future merges
		err = faissIndex.SetDirectMap(1)
		if err != nil {
			return err
		}

		nprobe := calculateNprobe(nlist, indexOptimizedFor)
		faissIndex.SetNProbe(nprobe)

		// train the vector index, essentially performs k-means clustering to partition
		// the data space of indexData such that during the search time, we probe
		// only a subset of vectors -> non-exhaustive search. could be a time
		// consuming step when the indexData is large.
		err = faissIndex.Train(indexData)
		if err != nil {
			return err
		}
	}

	err = faissIndex.Add(indexData)
	if err != nil {
		return err
	}

	mergedIndexBytes, err := faiss.WriteIndexIntoBuffer(faissIndex)
	if err != nil {
		return err
	}

	return v.flushVectorIndex(mergedIndexBytes, w)
}

func freeReconstructedIndexes(indexes []*vecIndexInfo) {
	for _, entry := range indexes {
		if entry.index != nil {
			entry.index.Close()
		}
	}
}

// todo: is it possible to merge this resuable stuff with the interim's tmp0?
func (v *vectorIndexOpaque) grabBuf(size int) []byte {
	buf := v.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		v.tmp0 = buf
	}
	return buf[0:size]
}

// Determines the number of centroids to use for an IVF index.
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

const (
	IndexTypeFlat = iota
	IndexTypeIVF
)

// Returns a description string for the index and quantizer type
// and an index type.
func determineIndexToUse(nvecs, nlist int, indexOptimizedFor string) (string, int) {
	if indexOptimizedFor == index.IndexOptimizedForMemoryEfficient {
		switch {
		case nvecs >= 1000:
			return fmt.Sprintf("IVF%d,SQ4", nlist), IndexTypeIVF
		default:
			return "Flat", IndexTypeFlat
		}
	}

	switch {
	case nvecs >= 10000:
		return fmt.Sprintf("IVF%d,SQ8", nlist), IndexTypeIVF
	case nvecs >= 1000:
		return fmt.Sprintf("IVF%d,Flat", nlist), IndexTypeIVF
	default:
		return "Flat", IndexTypeFlat
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
		// calculate the capacity of the vecs and ids slices
		// to avoid multiple allocations.
		// number of vectors to be indexed for this field
		nvecs := len(content.vectors)
		// dimension of each vector
		dims := int(content.dimension)
		// flatten the vectors into a single slice of size numVectors * dims
		vecs := make([]float32, 0, nvecs*dims)
		for _, vecInfo := range content.vectors {
			vecs = append(vecs, vecInfo.vector...)
		}
		// Set the faiss metric type (default is Euclidean Distance or l2_norm)
		var metric = faiss.MetricL2
		if content.metric == index.InnerProduct || content.metric == index.CosineSimilarity {
			// use the same FAISS metric for inner product and cosine similarity
			metric = faiss.MetricInnerProduct
		}
		nlist := determineCentroids(nvecs)
		indexDescription, indexClass := determineIndexToUse(nvecs, nlist, content.optimizedFor)
		faissIndex, err := faiss.IndexFactory(dims, indexDescription, metric)
		if err != nil {
			return err
		}
		defer faissIndex.Close()
		if indexClass == IndexTypeIVF {
			// set the direct map to reconstruct vectors based on vector IDs
			// in future merges, we keep 1 as the direct map type to use an
			// array based direct map, since we have sequential vector IDs starting
			// from 0 to N-1.
			err = faissIndex.SetDirectMap(1)
			if err != nil {
				return err
			}
			// calculate nprobe using a heuristic.
			nprobe := calculateNprobe(nlist, content.optimizedFor)
			// set nprobe value
			faissIndex.SetNProbe(nprobe)
			// train the index with the vectors
			err = faissIndex.Train(vecs)
			if err != nil {
				return err
			}
		}
		// add the vectors to the index using sequential vector IDs starting
		// from 0 to N-1
		err = faissIndex.Add(vecs)
		if err != nil {
			return err
		}
		// record the fieldStart value for this section.
		fieldStart := w.Count()
		// writing out two offset values to indicate that the current field's
		// vector section doesn't have valid doc value content within it.
		n := binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		n = binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
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
		n = binary.PutUvarint(tempBuf, uint64(faissIndex.Ntotal()))
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
		for _, vecEntry := range content.vectors {
			// write docIDs associated with every vector, with vecID being
			// implicit from the order of addition, i.e., 0 to N-1
			n = binary.PutUvarint(tempBuf, uint64(vecEntry.docID))
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return err
			}

			n = binary.PutUvarint(tempBuf, uint64(docID))
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return err
			}
		}
		// serialize the built index into a byte slice
		buf, err := faiss.WriteIndexIntoBuffer(faissIndex)
		if err != nil {
			return err
		}
		// write the type of the vector index (unused for now)
		n = binary.PutUvarint(tempBuf, 0)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the index bytes and its length
		n = binary.PutUvarint(tempBuf, uint64(len(buf)))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}
		// write the vector index data
		_, err = w.Write(buf)
		if err != nil {
			return err
		}
		// accounts for whatever data has been written out to the writer.
		vo.incrementBytesWritten(uint64(w.Count() - fieldStart))
		vo.fieldAddrs[fieldID] = fieldStart
	}
	return nil
}

func (vo *vectorIndexOpaque) process(field index.VectorField, fieldID uint16, docNum uint32) {
	if fieldID == math.MaxUint16 {
		// doc processing checkpoint. currently nothing to do
		return
	}
	vec := field.Vector()
	dim := field.Dims()
	metric := field.Similarity()
	indexOptimizedFor := field.IndexOptimizedFor()
	// caller is supposed to make sure len(vec) is a multiple of dim.
	// Not double checking it here to avoid the overhead.
	// this is to account for multi-vector fields, where a field can have
	// multiple vectors associated with it. In this case we process all the
	// vectors associated with the field as separate vectors.
	numVectors := len(vec) / dim
	var content *vectorIndexContent
	var ok bool
	for i := 0; i < numVectors; i++ {
		vector := vec[i*dim : (i+1)*dim]
		// check if we have content for this fieldID already
		if content, ok = vo.fieldVectorIndex[fieldID]; !ok {
			// create an entry for this fieldID as this is the first time we
			// are seeing this field
			content = &vectorIndexContent{
				dimension:    uint16(dim),
				metric:       metric,
				optimizedFor: indexOptimizedFor,
			}
			vo.fieldVectorIndex[fieldID] = content
		}
		// add an entry to the index content's vectors, supplying both the
		// the vector and the docNum for this vector within the segment
		content.vectors = append(content.vectors, &vectorEntry{
			vector: vector,
			docID:  docNum,
		})
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

// the information required to create a vector index for a vector field
type vectorIndexContent struct {
	// vectorID -> vectorEntry
	vectors []*vectorEntry
	// dimension of all the vectors
	dimension uint16
	// distance metric to be used
	metric string
	// optimization type for the index
	optimizedFor string
}

// vector entry captures the vector and its
// associated document ID within the segment.
type vectorEntry struct {
	vector []float32
	docID  uint32
}

type vectorIndexOpaque struct {
	bytesWritten uint64

	// maps the field to the address of its vector section
	fieldAddrs map[uint16]int

	// maps the fieldID of a vector field to its vector index content
	fieldVectorIndex map[uint16]*vectorIndexContent

	// field indexing options
	fieldsOptions map[string]index.FieldIndexingOptions

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

// cleanup stuff over here for reusability
func (vo *vectorIndexOpaque) Reset() (err error) {
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
