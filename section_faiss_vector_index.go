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
	"math"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
	seg "github.com/blevesearch/scorch_segment_api/v2"
	"golang.org/x/exp/maps"
)

func init() {
	registerSegmentSection(SectionFaissVectorIndex, &faissVectorIndexSection{})
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
	startOffset int
	indexSize   uint64
	vecIds      []int64
	deleted     []int64
}

type vecIndexData struct {
	index *faiss.IndexImpl
	meta  *vecIndexMeta
}

func remapDocIDs(oldIDs *roaring.Bitmap, newIDs []uint64) *roaring.Bitmap {
	newBitmap := roaring.NewBitmap()

	for _, oldID := range oldIDs.ToArray() {
		if newIDs[oldID] != docDropped {
			newBitmap.Add(uint32(newIDs[oldID]))
		}
	}
	return newBitmap
}

// keep in mind with respect to update and delete opeartions with resepct to vectors/
// leverage bitmaps stored
func (v *faissVectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
	vo := v.getvectorIndexOpaque(opaque)

	for fieldID, fieldName := range fieldsInv {

		indexes := make([]*vecIndexMeta, len(segments))
		vecToDocID := make(map[int64]*roaring.Bitmap)

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

			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			indexes[segI] = &vecIndexMeta{
				startOffset: pos,
				indexSize:   indexSize,
			}

			pos += int(indexSize)

			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			indexes[segI].vecIds = make([]int64, 0, numVecs)

			for i := 0; i < int(numVecs); i++ {
				vecID, n := binary.Varint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				numDocs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				bitMap := roaring.NewBitmap()
				if numDocs == 1 {
					docID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
					pos += n
					bitMap.Add(uint32(docID))
				} else {
					bitMapLen, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
					pos += n

					roaringBytes := sb.mem[pos : pos+int(bitMapLen)]
					pos += int(bitMapLen)

					_, err := bitMap.FromBuffer(roaringBytes)
					if err != nil {
						return err
					}
				}

				// remap the docIDs from the old segment to the new document nos.
				// provided. furthermore, this function also drops the invalid doc nums
				// of that segment in the resulting bitmap
				bitMap = remapDocIDs(bitMap, newDocNumsIn[segI])
				if vecToDocID[vecID] == nil {
					// if the remapped bitmap has valid docs as entries, track it
					// as part of vecs to be reconstructed (for larger indexes).
					// otherwise, since there are no docs present for this vecID,
					// delete it from the specific vector index later on.
					if bitMap.GetCardinality() > 0 {
						vecToDocID[vecID] = bitMap
						indexes[segI].vecIds = append(indexes[segI].vecIds, vecID)
					} else {
						indexes[segI].deleted = append(indexes[segI].deleted, vecID)
					}
				} else {
					vecToDocID[vecID].Or(bitMap)
				}
			}
		}
		err := vo.mergeAndWriteVectorIndexes(fieldID, segments, vecToDocID, indexes, w, closeCh)
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *vectorIndexOpaque) flushVectorSection(vecToDocID map[int64]*roaring.Bitmap,
	serializedIndex []byte, w *CountHashWriter) (int, error) {
	tempBuf := v.grabBuf(binary.MaxVarintLen64)

	fieldStart := w.Count()
	// marking the fact that for vector index, doc values isn't valid by
	// storing fieldNotUniverted values.
	n := binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return 0, err
	}
	n = binary.PutUvarint(tempBuf, uint64(fieldNotUninverted))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return 0, err
	}

	n = binary.PutUvarint(tempBuf, uint64(len(serializedIndex)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return 0, err
	}

	// write the vector index data
	_, err = w.Write(serializedIndex)
	if err != nil {
		return 0, err
	}

	// write the number of unique vectors
	n = binary.PutUvarint(tempBuf, uint64(len(vecToDocID)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return 0, err
	}

	for vecID, docIDs := range vecToDocID {
		// write the vecID
		n = binary.PutVarint(tempBuf, vecID)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}

		numDocs := docIDs.GetCardinality()
		n = binary.PutUvarint(tempBuf, numDocs)
		_, err = w.Write(tempBuf[:n])
		if err != nil {
			return 0, err
		}

		// an optimization to avoid using the bitmaps if there is only 1 doc
		// with the vecID.
		if numDocs == 1 {
			n = binary.PutUvarint(tempBuf, uint64(docIDs.Minimum()))
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return 0, err
			}
			continue
		}

		// write the docIDs
		_, err = writeRoaringWithLen(docIDs, w, tempBuf)
		if err != nil {
			return 0, err
		}
	}

	return fieldStart, nil
}

// Func which returns if reconstruction is required.
// Won't be required if multiple flat indexes are being combined into a larger
// flat index.
func reconstructionRequired(isNewIndexIVF bool, indexes []*vecIndexData) bool {
	// Always required for IVF indexes.
	if isNewIndexIVF {
		return true
	}

	// if any existing index is not flat, all need to be reconstructed.
	for _, index := range indexes {
		_, isExistingIndexIVF := getIndexType(len(index.meta.vecIds) +
			len(index.meta.deleted))
		if isExistingIndexIVF {
			return true
		}
	}

	return false
}

func removeDeletedVectors(index *faiss.IndexImpl, ids []int64) error {
	sel, err := faiss.NewIDSelectorBatch(ids)
	if err != nil {
		return err
	}

	defer sel.Delete()
	_, err = index.RemoveIDs(sel)
	if err != nil {
		return err
	}
	return nil
}

// todo: naive implementation. need to keep in mind the perf implications and improve on this.
// perhaps, parallelized merging can help speed things up over here.
func (v *vectorIndexOpaque) mergeAndWriteVectorIndexes(fieldID int, sbs []*SegmentBase,
	vecToDocID map[int64]*roaring.Bitmap, indexes []*vecIndexMeta, w *CountHashWriter, closeCh chan struct{}) error {

	var vecIndexes []*vecIndexData
	for segI, seg := range sbs {
		// read the index bytes. todo: parallelize this
		if indexes[segI] == nil {
			continue
		}
		indexBytes := seg.mem[indexes[segI].startOffset : indexes[segI].startOffset+int(indexes[segI].indexSize)]
		index, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
		if err != nil {
			freeReconstructedIndexes(vecIndexes)
			return err
		}
		vecIndexes = append(vecIndexes, &vecIndexData{index: index, meta: indexes[segI]})
	}

	// no vector indexes to merge
	if vecIndexes == nil {
		return nil
	}

	defer freeReconstructedIndexes(vecIndexes)

	var mergedIndexBytes []byte
	// index type to be created after merge.
	indexType, isIVF := getIndexType(len(vecToDocID))

	if reconstructionRequired(isIVF, vecIndexes) {
		// merging of indexes with reconstruction
		// method. the meta.indexes[i].vecIds is such that it has only the valid vecs
		// of this vector index present in it, so we'd be reconstructed only the
		// valid ones.
		var indexData []float32
		for i := 0; i < len(vecIndexes); i++ {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}

			// reconstruct the vectors only if present, it could be that
			// some of the indexes had all of their vectors updated/deleted.
			if len(vecIndexes[i].meta.vecIds) > 0 {
				// todo: parallelize reconstruction
				recons, err := vecIndexes[i].index.ReconstructBatch(int64(len(vecIndexes[i].meta.vecIds)),
					vecIndexes[i].meta.vecIds)
				if err != nil {
					return err
				}
				indexData = append(indexData, recons...)
			}
		}

		if len(indexData) == 0 {
			// no valid vectors for this index, so we don't even have to
			// record it in the section
			return nil
		}

		// safe to assume that all the indexes are of the same config values, given
		// that they are extracted from the field mapping info.
		dims := vecIndexes[0].index.D()
		metric := vecIndexes[0].index.MetricType()
		finalVecIDs := maps.Keys(vecToDocID)

		index, err := faiss.IndexFactory(dims, indexType, metric)
		if err != nil {
			return err
		}
		defer index.Close()

		if isIVF {
			// the direct map maintained in the IVF index is essential for the
			// reconstruction of vectors based on vector IDs in the future merges.
			// the AddWithIDs API also needs a direct map to be set before using.
			err = index.SetDirectMap(2)
			if err != nil {
				return err
			}

			// train the vector index, essentially performs k-means clustering to partition
			// the data space of indexData such that during the search time, we probe
			// only a subset of vectors -> non-exhaustive search. could be a time
			// consuming step when the indexData is large.
			err = index.Train(indexData)
			if err != nil {
				return err
			}
		}

		err = index.AddWithIDs(indexData, finalVecIDs)
		if err != nil {
			return err
		}

		mergedIndexBytes, err = faiss.WriteIndexIntoBuffer(index)
		if err != nil {
			return err
		}
	} else {
		var err error
		if len(vecIndexes[0].meta.deleted) > 0 {
			err = removeDeletedVectors(vecIndexes[0].index, vecIndexes[0].meta.deleted)
			if err != nil {
				return err
			}
		}
		for i := 1; i < len(vecIndexes); i++ {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}
			if len(vecIndexes[i].meta.deleted) > 0 {
				err = removeDeletedVectors(vecIndexes[i].index, vecIndexes[i].meta.deleted)
				if err != nil {
					return err
				}
			}
			err = vecIndexes[0].index.MergeFrom(vecIndexes[i].index, 0)
			if err != nil {
				return err
			}
		}

		mergedIndexBytes, err = faiss.WriteIndexIntoBuffer(vecIndexes[0].index)
		if err != nil {
			return err
		}
	}
	fieldStart, err := v.flushVectorSection(vecToDocID, mergedIndexBytes, w)
	if err != nil {
		return err
	}
	v.fieldAddrs[uint16(fieldID)] = fieldStart

	return nil
}

// todo: can be parallelized.
func freeReconstructedIndexes(indexes []*vecIndexData) {
	for _, vecIndex := range indexes {
		vecIndex.index.Close()
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

func getIndexType(nVecs int) (string, bool) {
	switch {
	case nVecs > 10000:
		return "IVF100,SQ8", true
	// default minimum number of training points per cluster is 39
	case nVecs >= 390:
		return "IVF10,Flat", true
	default:
		return "IDMap2,Flat", false
	}
}

func (vo *vectorIndexOpaque) writeVectorIndexes(w *CountHashWriter) (offset uint64, err error) {
	// for every fieldID, contents to store over here are:
	//    1. the serialized representation of the dense vector index.
	//    2. its constituent vectorID -> {docID} mapping. perhaps a bitmap is enough.
	tempBuf := vo.grabBuf(binary.MaxVarintLen64)
	for fieldID, content := range vo.vecFieldMap {

		var vecs []float32
		var ids []int64
		for hash, vecInfo := range content.vecs {
			vecs = append(vecs, vecInfo.vec...)
			ids = append(ids, int64(hash))
		}

		var metric = faiss.MetricL2
		if content.metric == index.CosineSimilarity {
			metric = faiss.MetricInnerProduct
		}

		indexType, isIVF := getIndexType(len(ids))
		// create an index - currently keeping it as flat for faster data ingest
		index, err := faiss.IndexFactory(int(content.dim), indexType, metric)
		if err != nil {
			return 0, err
		}

		defer index.Close()

		if isIVF {
			err = index.SetDirectMap(2)
			if err != nil {
				return 0, err
			}

			err = index.Train(vecs)
			if err != nil {
				return 0, err
			}
		}

		err = index.AddWithIDs(vecs, ids)
		if err != nil {
			return 0, err
		}

		// serialize the built index into a byte slice
		buf, err := faiss.WriteIndexIntoBuffer(index)
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

		// write the number of unique vectors
		n = binary.PutUvarint(tempBuf, uint64(index.Ntotal()))
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
			docIDs := vo.vecIDMap[vecID].docIDs
			// write the vecID
			n = binary.PutVarint(tempBuf, vecID)
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return 0, err
			}

			numDocs := docIDs.GetCardinality()
			n = binary.PutUvarint(tempBuf, numDocs)
			_, err = w.Write(tempBuf[:n])
			if err != nil {
				return 0, err
			}

			// an optimization to avoid using the bitmaps if there is only 1 doc
			// with the vecID.
			if numDocs == 1 {
				n = binary.PutUvarint(tempBuf, uint64(docIDs.Minimum()))
				_, err = w.Write(tempBuf[:n])
				if err != nil {
					return 0, err
				}
				continue
			}

			// write the docIDs
			_, err = writeRoaringWithLen(docIDs, w, tempBuf)
			if err != nil {
				return 0, err
			}
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

	// caller is supposed to make sure len(vec) is a multiple of dim.
	// Not double checking it here to avoid the overhead.
	numSubVecs := len(vec) / dim
	for i := 0; i < numSubVecs; i++ {
		subVec := vec[i*dim : (i+1)*dim]

		// NOTE: currently, indexing only unique vectors.
		subVecHash := hashCode(subVec)
		if _, ok := vo.vecIDMap[subVecHash]; !ok {
			vo.vecIDMap[subVecHash] = vecInfo{
				docIDs: roaring.NewBitmap(),
			}
		}
		// add the docID to the bitmap
		vo.vecIDMap[subVecHash].docIDs.Add(docNum)

		// tracking the unique vectors for every field which will be used later
		// to construct the vector index.
		if _, ok := vo.vecFieldMap[fieldID]; !ok {
			vo.vecFieldMap[fieldID] = indexContent{
				vecs: map[int64]vecInfo{
					subVecHash: {
						vec: subVec,
					},
				},
				dim:    uint16(dim),
				metric: metric,
			}
		} else {
			vo.vecFieldMap[fieldID].vecs[subVecHash] = vecInfo{
				vec: subVec,
			}
		}
	}
}

// todo: better hash function?
// keep the perf aspects in mind with respect to the hash function.
// random seed based hash golang.
func hashCode(a []float32) int64 {
	var rv int64
	for _, v := range a {
		rv = int64(math.Float32bits(v)) + (31 * rv)
	}

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
		vecIDMap:    make(map[int64]vecInfo),
		vecFieldMap: make(map[uint16]indexContent),
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

type indexContent struct {
	vecs   map[int64]vecInfo
	dim    uint16
	metric string
}

type vecInfo struct {
	vec    []float32
	docIDs *roaring.Bitmap
}

type vectorIndexOpaque struct {
	init bool

	bytesWritten uint64

	lastNumVecs   int
	lastNumFields int

	// maps the field to the address of its vector section
	fieldAddrs map[uint16]int

	// maps the vecID to basic info involved around it such as
	// the docIDs its present in and the vector itself
	vecIDMap map[int64]vecInfo
	// maps the field to information necessary for its vector
	// index to be build.
	vecFieldMap map[uint16]indexContent

	tmp0 []byte
}

func (v *vectorIndexOpaque) realloc() {
	// when an opaque instance is reused, the two maps are pre-allocated
	// with space before they were reset. this can be useful in continuous
	// mutation scenarios, where the batch sizes are more or less same.
	v.vecFieldMap = make(map[uint16]indexContent, v.lastNumFields)
	v.vecIDMap = make(map[int64]vecInfo, v.lastNumVecs)
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
