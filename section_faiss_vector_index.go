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

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
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

LOOP:
	for fieldID, fieldName := range fieldsInv {

		var indexes []vecIndexMeta
		vecToDocID := make(map[int64]*roaring.Bitmap)

		// todo: would parallely fetching the following stuff from segments
		// be beneficial in terms of perf?
		for segI, sb := range segments {
			if isClosed(closeCh) {
				return fmt.Errorf("merging of vector sections aborted")
			}
			if _, ok := sb.fieldsMap[fieldName]; !ok {
				continue
			}

			// check if the section address is a valid one for "fieldName" in the
			// segment sb. the local fieldID (fetched by the fieldsMap of the sb)
			// is to be used while consulting the fieldsSectionsMap
			pos := int(sb.fieldsSectionsMap[sb.fieldsMap[fieldName]-1][SectionFaissVectorIndex])
			if pos == 0 {
				continue LOOP
			}

			// loading doc values - adhering to the sections format. never
			// valid values for vector section
			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			indexSize, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			indexes = append(indexes, vecIndexMeta{
				startOffset: pos,
				indexSize:   indexSize,
			})

			pos += int(indexSize)

			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			indexes[len(indexes)-1].vecIds = make([]int64, 0, numVecs)

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
				// provided.
				bitMap = remapDocIDs(bitMap, newDocNumsIn[segI])
				if vecToDocID[vecID] == nil {
					// if there are some tombstone entries in the docIDs, marked
					// in the drops[ith-segment] bitmap, don't include them in the
					// final bitmap.
					if drops[segI] != nil && !drops[segI].IsEmpty() {
						vecToDocID[vecID] = roaring.AndNot(bitMap, drops[segI])
					} else {
						vecToDocID[vecID] = bitMap
					}
					indexes[len(indexes)-1].vecIds = append(indexes[len(indexes)-1].vecIds, vecID)
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

// todo: naive implementation. need to keep in mind the perf implications and improve on this.
// perhaps, parallelized merging can help speed things up over here.
func (v *vectorIndexOpaque) mergeAndWriteVectorIndexes(fieldID int, sbs []*SegmentBase,
	vecToDocID map[int64]*roaring.Bitmap, indexes []vecIndexMeta, w *CountHashWriter, closeCh chan struct{}) error {

	var vecIndexes []*faiss.IndexImpl
	for segI, seg := range sbs {
		// read the index bytes. todo: parallelize this
		indexBytes := seg.mem[indexes[segI].startOffset : indexes[segI].startOffset+int(indexes[segI].indexSize)]
		index, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
		if err != nil {
			freeReconstructedIndexes(vecIndexes)
			return err
		}
		vecIndexes = append(vecIndexes, index)
	}

	defer freeReconstructedIndexes(vecIndexes)

	var mergedIndexBytes []byte
	indexType, isIVF := getIndexType(len(vecToDocID))
	if isIVF {
		// merging of more complex index types (for eg ivf family) with reconstruction
		// method.
		var indexData []float32
		for i := 0; i < len(vecIndexes); i++ {
			if isClosed(closeCh) {
				return fmt.Errorf("merging of vector sections aborted")
			}
			// todo: parallelize reconstruction
			recons, err := vecIndexes[i].ReconstructBatch(int64(len(indexes[i].vecIds)), indexes[i].vecIds)
			if err != nil {
				return err
			}
			indexData = append(indexData, recons...)
		}

		// safe to assume that all the indexes are of the same config values, given
		// that they are extracted from the field mapping info.
		dims := vecIndexes[0].D()
		metric := vecIndexes[0].MetricType()
		finalVecIDs := maps.Keys(vecToDocID)

		index, err := faiss.IndexFactory(dims, indexType, metric)
		if err != nil {
			return err
		}
		defer index.Close()

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

		err = index.AddWithIDs(indexData, finalVecIDs)
		if err != nil {
			return err
		}

		mergedIndexBytes, err = faiss.WriteIndexIntoBuffer(index)
		if err != nil {
			return err
		}
	} else {
		// todo: ivf -> flat index when there were huge number of vector deletes for this field
		var err error
		for i := 1; i < len(vecIndexes); i++ {
			if isClosed(closeCh) {
				return fmt.Errorf("merging of vector sections aborted")
			}
			err := vecIndexes[0].MergeFrom(vecIndexes[i], 0)
			if err != nil {
				return err
			}
		}

		mergedIndexBytes, err = faiss.WriteIndexIntoBuffer(vecIndexes[0])
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
func freeReconstructedIndexes(vecIndexes []*faiss.IndexImpl) {
	for _, index := range vecIndexes {
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
		}

		err = index.Train(vecs)
		if err != nil {
			return 0, err
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
		vo.init = true
		vo.allocateSpace()
	}
	if fieldID == math.MaxUint16 {
		// doc processing checkpoint. currently nothing to do
		return
	}

	//process field
	vec := field.Vector()
	dim := field.Dims()
	metric := field.Similarity()

	if vec != nil {
		// NOTE: currently, indexing only unique vectors.
		vecHash := hashCode(vec)
		if _, ok := vo.vecIDMap[vecHash]; !ok {
			vo.vecIDMap[vecHash] = vecInfo{
				docIDs: roaring.NewBitmap(),
			}
		}
		// add the docID to the bitmap
		vo.vecIDMap[vecHash].docIDs.Add(docNum)

		// tracking the unique vectors for every field which will be used later
		// to construct the vector index.
		if _, ok := vo.vecFieldMap[fieldID]; !ok {
			vo.vecFieldMap[fieldID] = indexContent{
				vecs: map[int64]vecInfo{
					vecHash: {
						vec: vec,
					},
				},
				dim:    uint16(dim),
				metric: metric,
			}
		} else {
			vo.vecFieldMap[fieldID].vecs[vecHash] = vecInfo{
				vec: vec,
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

func (v *vectorIndexOpaque) allocateSpace() {
	// todo: allocate the space for the opaque contents if possible.
	// basically to avoid too many heap allocs and also reuse things
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

// todo: document the data structures involved in vector section.
type vectorIndexOpaque struct {
	init bool

	bytesWritten uint64

	fieldAddrs map[uint16]int

	vecIDMap    map[int64]vecInfo
	vecFieldMap map[uint16]indexContent

	tmp0 []byte
}

func (v *vectorIndexOpaque) incrementBytesWritten(val uint64) {
	v.bytesWritten += val
}

func (v *vectorIndexOpaque) BytesWritten() uint64 {
	return v.bytesWritten
}

func (v *vectorIndexOpaque) BytesRead() uint64 {
	return 0
}

func (v *vectorIndexOpaque) ResetBytesRead(uint64) {}

func (vo *vectorIndexOpaque) Reset() (err error) {
	// cleanup stuff over here

	return nil
}
func (v *vectorIndexOpaque) Set(key string, val interface{}) {

}
