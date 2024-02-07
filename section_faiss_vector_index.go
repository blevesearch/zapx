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

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	registerSegmentSection(SectionFaissVectorIndex, &faissVectorIndexSection{})
	isFieldNotApplicableToInvertedTextSection = func(field index.Field) bool {
		_, ok := field.(index.VectorField)
		return ok
	}
}

type faissVectorIndexSection struct {
}

func (v *faissVectorIndexSection) Process(opaque map[int]resetable, docNum uint32, field index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}

	if vf, ok := field.(index.VectorField); ok {
		vo := v.getvectorIndexOpaque(opaque)
		vo.process(vf, fieldID, docNum)
	}
}

func (v *faissVectorIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {
	vo := v.getvectorIndexOpaque(opaque)
	vo.writeVectorIndexes(w)
	return 0, nil
}

func (v *faissVectorIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	vo := v.getvectorIndexOpaque(opaque)
	return vo.fieldAddrs[uint16(fieldID)]
}

// metadata corresponding to a serialized vector index
type vecIndexMeta struct {
	startOffset       int
	indexSize         uint64
	vecIds            []int64
	indexOptimizedFor string
}

// keep in mind with respect to update and delete operations with resepct to vectors
func (v *faissVectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
	vo := v.getvectorIndexOpaque(opaque)

	// the segments with valid vector sections in them
	// preallocating the space over here, if there are too many fields
	// in the segment this will help by avoiding multiple allocation
	// calls.
	vecSegs := make([]*SegmentBase, 0, len(segments))
	indexes := make([]*vecIndexMeta, 0, len(segments))

	for fieldID, fieldName := range fieldsInv {
		indexes = indexes[:0] // resizing the slices
		vecSegs = vecSegs[:0]
		vecToDocID := make(map[int64]uint64)

		// todo: would parallely fetching the following stuff from segments
		// be beneficial in terms of perf?
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

			// the index optimization type represented as an int
			indexOptimizationTypeInt, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			vecSegs = append(vecSegs, sb)
			indexes = append(indexes, &vecIndexMeta{
				vecIds:            make([]int64, 0, numVecs),
				indexOptimizedFor: index.VectorIndexOptimizationsReverseLookup[int(indexOptimizationTypeInt)],
			})

			curIdx := len(indexes) - 1
			for i := 0; i < int(numVecs); i++ {
				vecID, n := binary.Varint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				docID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				// remap the docID from the old segment to the new document nos.
				// provided. furthermore, also drop the now-invalid doc nums
				// of that segment
				var vecIDNotDeleted bool // indicates if the vector ID was not deleted.
				var newDocID uint64      // new docID in the new segment
				if newDocNumsIn[segI][uint32(docID)] != docDropped {
					newDocID = newDocNumsIn[segI][uint32(docID)]
					vecIDNotDeleted = true
				}
				// if the remapped doc ID is valid, track it
				// as part of vecs to be reconstructed (for larger indexes).
				// this would account only the valid vector IDs, so the deleted
				// ones won't be reconstructed in the final index.
				if vecIDNotDeleted {
					vecToDocID[vecID] = newDocID
					indexes[curIdx].vecIds = append(indexes[curIdx].vecIds, vecID)
				}
			}

			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			indexes[curIdx].startOffset = pos
			indexes[curIdx].indexSize = indexSize
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
	vecToDocID map[int64]uint64, indexes []*vecIndexMeta) error {
	tempBuf := v.grabBuf(binary.MaxVarintLen64)
	if len(indexes) == 0 {
		fmt.Println("its nil")
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

	// write the number of unique vectors
	n = binary.PutUvarint(tempBuf, uint64(len(vecToDocID)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	for vecID, docID := range vecToDocID {
		// write the vecID
		n = binary.PutVarint(tempBuf, vecID)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return err
		}

		// write the docID
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

	n := binary.PutUvarint(tempBuf, uint64(len(indexBytes)))
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	// write the vector index data
	_, err = w.Write(indexBytes)
	if err != nil {
		return err
	}

	return nil
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
	indexes []*vecIndexMeta, w *CountHashWriter, closeCh chan struct{}) error {

	vecIndexes := make([]*faiss.IndexImpl, 0, len(sbs))
	reconsCap := 0
	for segI, segBase := range sbs {
		// Considering merge operations on vector indexes are expensive, it is
		// worth including an early exit if the merge is aborted, saving us
		// the resource spikes, even if temporary.
		if isClosed(closeCh) {
			freeReconstructedIndexes(vecIndexes)
			return seg.ErrClosed
		}
		// read the index bytes. todo: parallelize this
		indexBytes := segBase.mem[indexes[segI].startOffset : indexes[segI].startOffset+int(indexes[segI].indexSize)]
		index, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
		if err != nil {
			freeReconstructedIndexes(vecIndexes)
			return err
		}
		indexReconsLen := len(indexes[segI].vecIds) * index.D()
		if indexReconsLen > reconsCap {
			reconsCap = indexReconsLen
		}
		vecIndexes = append(vecIndexes, index)
	}

	// no vector indexes to merge
	if len(vecIndexes) == 0 {
		return nil
	}

	var mergedIndexBytes []byte

	// capacities for the finalVecIDs and indexData slices
	// to avoid multiple allocations, via append.
	finalVecIDCap := len(indexes[0].vecIds) * len(vecIndexes)
	indexDataCap := finalVecIDCap * vecIndexes[0].D()

	finalVecIDs := make([]int64, 0, finalVecIDCap)
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
		if len(indexes[i].vecIds) > 0 {
			neededReconsLen := len(indexes[i].vecIds) * vecIndexes[i].D()
			recons = recons[:neededReconsLen]
			// todo: parallelize reconstruction
			recons, err = vecIndexes[i].ReconstructBatch(indexes[i].vecIds, recons)
			if err != nil {
				freeReconstructedIndexes(vecIndexes)
				return err
			}
			indexData = append(indexData, recons...)
			// Adding vector IDs in the same order as the vectors
			finalVecIDs = append(finalVecIDs, indexes[i].vecIds...)
		}
	}

	if len(indexData) == 0 {
		// no valid vectors for this index, so we don't even have to
		// record it in the section
		freeReconstructedIndexes(vecIndexes)
		return nil
	}

	nvecs := len(finalVecIDs)

	// index type to be created after merge based on the number of vectors in
	// indexData added into the index.
	nlist := determineCentroids(nvecs)
	indexDescription, indexClass := determineIndexToUse(nvecs, nlist)

	// safe to assume that all the indexes are of the same config values, given
	// that they are extracted from the field mapping info.
	dims := vecIndexes[0].D()
	metric := vecIndexes[0].MetricType()
	indexOptimizedFor := indexes[0].indexOptimizedFor

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
		// reconstruction of vectors based on vector IDs in the future merges.
		// the AddWithIDs API also needs a direct map to be set before using.
		err = faissIndex.SetDirectMap(2)
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

	err = faissIndex.AddWithIDs(indexData, finalVecIDs)
	if err != nil {
		return err
	}

	mergedIndexBytes, err = faiss.WriteIndexIntoBuffer(faissIndex)
	if err != nil {
		return err
	}

	err = v.flushVectorIndex(mergedIndexBytes, w)
	if err != nil {
		return err
	}

	return nil
}

// todo: can be parallelized.
func freeReconstructedIndexes(indexes []*faiss.IndexImpl) {
	for _, index := range indexes {
		index.Close()
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
	// At 1M vectors, nlist = 4k gave a reasonably high recall with the right nprobe,
	// whereas 1M/100 = 10000 centroids would increase training time without
	// corresponding increase in recall
	case nvecs >= 1000000:
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
func determineIndexToUse(nvecs, nlist int) (string, int) {
	switch {
	case nvecs >= 10000:
		return fmt.Sprintf("IVF%d,SQ8", nlist), IndexTypeIVF
	case nvecs >= 1000:
		return fmt.Sprintf("IVF%d,Flat", nlist), IndexTypeIVF
	default:
		return "IDMap2,Flat", IndexTypeFlat
	}
}

func (vo *vectorIndexOpaque) writeVectorIndexes(w *CountHashWriter) (offset uint64, err error) {
	// for every fieldID, contents to store over here are:
	//    1. the serialized representation of the dense vector index.
	//    2. its constituent vectorID -> {docID} mapping.
	tempBuf := vo.grabBuf(binary.MaxVarintLen64)
	for fieldID, content := range vo.vecFieldMap {
		// calculate the capacity of the vecs and ids slices
		// to avoid multiple allocations.
		vecs := make([]float32, 0, uint16(len(content.vecs))*content.dim)
		ids := make([]int64, 0, len(content.vecs))
		for hash, vecInfo := range content.vecs {
			vecs = append(vecs, vecInfo.vec...)
			ids = append(ids, int64(hash))
		}

		var metric = faiss.MetricL2
		if content.metric == index.CosineSimilarity {
			metric = faiss.MetricInnerProduct
		}

		nvecs := len(ids)
		nlist := determineCentroids(nvecs)
		indexDescription, indexClass := determineIndexToUse(nvecs, nlist)
		faissIndex, err := faiss.IndexFactory(int(content.dim), indexDescription, metric)
		if err != nil {
			return 0, err
		}

		defer faissIndex.Close()

		if indexClass == IndexTypeIVF {
			err = faissIndex.SetDirectMap(2)
			if err != nil {
				return 0, err
			}

			nprobe := calculateNprobe(nlist, content.indexOptimizedFor)
			faissIndex.SetNProbe(nprobe)

			err = faissIndex.Train(vecs)
			if err != nil {
				return 0, err
			}
		}

		err = faissIndex.AddWithIDs(vecs, ids)
		if err != nil {
			return 0, err
		}

		// serialize the built index into a byte slice
		buf, err := faiss.WriteIndexIntoBuffer(faissIndex)
		if err != nil {
			return 0, err
		}

		fieldStart := w.Count()
		// writing out two offset values to indicate that the current field's
		// vector section doesn't have valid doc value content within it.
		n := binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}
		n = binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}

		n = binary.PutUvarint(tempBuf, uint64(index.SupportedVectorIndexOptimizations[content.indexOptimizedFor]))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}

		// write the number of unique vectors
		n = binary.PutUvarint(tempBuf, uint64(faissIndex.Ntotal()))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}

		// fixme: this can cause a write amplification. need to improve this.
		// todo: might need to a reformating to optimize according to mmap needs.
		// reformating idea: storing all the IDs mapping towards the end of the
		// section would be help avoiding in paging in this data as part of a page
		// (which is to load a non-cacheable info like index). this could help the
		// paging costs
		for vecID, _ := range content.vecs {
			docID := vo.vecIDMap[vecID].docID
			// write the vecID
			n = binary.PutVarint(tempBuf, vecID)
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return 0, err
			}

			n = binary.PutUvarint(tempBuf, uint64(docID))
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return 0, err
			}
		}

		// record the fieldStart value for this section.
		// write the vecID -> docID mapping
		// write the index bytes and its length
		n = binary.PutUvarint(tempBuf, uint64(len(buf)))
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}

		// write the vector index data
		_, err = w.Write(buf)
		if err != nil {
			return 0, err
		}

		// accounts for whatever data has been written out to the writer.
		vo.incrementBytesWritten(uint64(w.Count() - fieldStart))
		vo.fieldAddrs[fieldID] = fieldStart
	}
	return 0, nil
}

func (vo *vectorIndexOpaque) process(field index.VectorField, fieldID uint16, docNum uint32) {
	if !vo.init {
		vo.realloc()
		vo.init = true
	}
	if fieldID == math.MaxUint16 {
		// doc processing checkpoint. currently nothing to do
		return
	}

	//process field
	vec := field.Vector()
	dim := field.Dims()
	metric := field.Similarity()
	indexOptimizedFor := field.IndexOptimizedFor()

	// caller is supposed to make sure len(vec) is a multiple of dim.
	// Not double checking it here to avoid the overhead.
	numSubVecs := len(vec) / dim
	for i := 0; i < numSubVecs; i++ {
		subVec := vec[i*dim : (i+1)*dim]

		// NOTE: currently, indexing only unique vectors.
		subVecHash := hashCode(subVec)
		if _, ok := vo.vecIDMap[subVecHash]; !ok {
			vo.vecIDMap[subVecHash] = &vecInfo{
				docID: docNum,
			}
		}

		// tracking the unique vectors for every field which will be used later
		// to construct the vector index.
		if _, ok := vo.vecFieldMap[fieldID]; !ok {
			vo.vecFieldMap[fieldID] = &indexContent{
				vecs: map[int64]*vecInfo{
					subVecHash: &vecInfo{
						vec: subVec,
					},
				},
				dim:               uint16(dim),
				metric:            metric,
				indexOptimizedFor: indexOptimizedFor,
			}
		} else {
			vo.vecFieldMap[fieldID].vecs[subVecHash] = &vecInfo{
				vec: subVec,
			}
		}
	}
}

// todo: better hash function?
// keep the perf aspects in mind with respect to the hash function.
// Uses a time based seed to prevent 2 identical vectors in different
// segments from having the same hash (which otherwise could cause an
// issue when merging those segments)
func hashCode(a []float32) int64 {
	var rv, sum int64
	for _, v := range a {
		// Weighing each element of the vector differently to minimise chance
		// of collisions between non identical vectors.
		sum = int64(math.Float32bits(v)) + sum*31
	}

	// Similar to getVectorCode(), this uses the first 32 bits for the vector sum
	// and the last 32 for a random 32-bit int to ensure identical vectors have
	// unique hashes.
	rv = sum<<32 | int64(rand.Int31())
	return rv
}

func (v *faissVectorIndexSection) getvectorIndexOpaque(opaque map[int]resetable) *vectorIndexOpaque {
	if _, ok := opaque[SectionFaissVectorIndex]; !ok {
		opaque[SectionFaissVectorIndex] = v.InitOpaque(nil)
	}
	return opaque[SectionFaissVectorIndex].(*vectorIndexOpaque)
}

func (v *faissVectorIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &vectorIndexOpaque{
		fieldAddrs:  make(map[uint16]int),
		vecIDMap:    make(map[int64]*vecInfo),
		vecFieldMap: make(map[uint16]*indexContent),
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

type indexContent struct {
	vecs              map[int64]*vecInfo
	dim               uint16
	metric            string
	indexOptimizedFor string
}

type vecInfo struct {
	vec   []float32
	docID uint32
}

type vectorIndexOpaque struct {
	init bool

	bytesWritten uint64

	lastNumVecs   int
	lastNumFields int

	// maps the field to the address of its vector section
	fieldAddrs map[uint16]int

	// maps the vecID to basic info involved around it such as
	// the docID its present in and the vector itself
	vecIDMap map[int64]*vecInfo
	// maps the field to information necessary for its vector
	// index to be build.
	vecFieldMap map[uint16]*indexContent

	tmp0 []byte
}

func (v *vectorIndexOpaque) realloc() {
	// when an opaque instance is reused, the two maps are pre-allocated
	// with space before they were reset. this can be useful in continuous
	// mutation scenarios, where the batch sizes are more or less same.
	v.vecFieldMap = make(map[uint16]*indexContent, v.lastNumFields)
	v.vecIDMap = make(map[int64]*vecInfo, v.lastNumVecs)
	v.fieldAddrs = make(map[uint16]int, v.lastNumFields)
}

func (v *vectorIndexOpaque) incrementBytesWritten(val uint64) {
	atomic.AddUint64(&v.bytesWritten, val)
}

func (v *vectorIndexOpaque) BytesWritten() uint64 {
	return atomic.LoadUint64(&v.bytesWritten)
}

func (v *vectorIndexOpaque) BytesRead() uint64 {
	return 0
}

func (v *vectorIndexOpaque) ResetBytesRead(uint64) {
}

// cleanup stuff over here for reusability
func (v *vectorIndexOpaque) Reset() (err error) {
	// tracking the number of vecs and fields processed and tracked in this
	// opaque, for better allocations of the maps
	v.lastNumVecs = len(v.vecIDMap)
	v.lastNumFields = len(v.vecFieldMap)

	v.init = false
	v.fieldAddrs = nil
	v.vecFieldMap = nil
	v.vecIDMap = nil
	v.tmp0 = v.tmp0[:0]

	atomic.StoreUint64(&v.bytesWritten, 0)

	return nil
}

func (v *vectorIndexOpaque) Set(key string, val interface{}) {
}
