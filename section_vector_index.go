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
)

func init() {
	segmentSections[sectionVectorIndex] = &vectorIndexSection{}
}

type vectorIndexSection struct {
}

func (v *vectorIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}

	if vf, ok := field.(index.VectorField); ok {
		vo := v.getvectorIndexOpaque(opaque)
		vo.process(vf, fieldID, docNum)
	}
}

func (v *vectorIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {
	vo := v.getvectorIndexOpaque(opaque)
	vo.writeVectorIndexes(w)
	return 0, nil
}

func (v *vectorIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	vo := v.getvectorIndexOpaque(opaque)
	return vo.fieldAddrs[uint16(fieldID)]
}

// metadata corresponding to a serialized vector index
type vecIndexMeta struct {
	startOffset int
	indexSize   uint64
}

func remapDocIDs(oldIDs *roaring.Bitmap, newIDs []uint64) *roaring.Bitmap {
	newBitmap := roaring.NewBitmap()
	for _, oldID := range oldIDs.ToArray() {
		newBitmap.Add(uint32(newIDs[oldID]))
	}
	return newBitmap
}

// keep in mind with respect to update and delete opeartions with resepct to vectors/
// leverage bitmaps stored
func (v *vectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
	vo := v.getvectorIndexOpaque(opaque)

LOOP:
	for fieldID, _ := range fieldsInv {
		// need to add a check wrt field type
		if isClosed(closeCh) {
			return fmt.Errorf("merging of vector sections aborted")
		}

		var indexes []vecIndexMeta
		vecToDocID := make(map[uint64]*roaring.Bitmap)
		for segI, sb := range segments {

			pos := int(sb.fieldsSectionsMap[fieldID][sectionVectorIndex])
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
			// don't read the index bytes just yet, perhaps?
			pos += int(indexSize)

			numVecs, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n

			for i := 0; i < int(numVecs); i++ {
				vecID, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				bitMapLen, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
				pos += n

				roaringBytes := sb.mem[pos : pos+int(bitMapLen)]
				pos += int(bitMapLen)

				bitMap := roaring.NewBitmap()
				_, err := bitMap.FromBuffer(roaringBytes)
				if err != nil {
					return err
				}

				bitMap = remapDocIDs(bitMap, newDocNumsIn[segI])
				if vecToDocID[vecID] == nil {
					if drops[segI] != nil && !drops[segI].IsEmpty() {
						vecToDocID[vecID] = roaring.AndNot(bitMap, drops[segI])
					} else {
						vecToDocID[vecID] = bitMap
					}
				} else {
					vecToDocID[vecID].Or(bitMap)
				}
			}
		}
		vo.mergeAndWriteVectorIndexes(fieldID, segments, vecToDocID, indexes, w)
	}

	// return err
	return nil
}

// todo: naive implementation. need to keep in mind the perf implications and improve on this.
// perhaps, parallelized merging can help speed things up over here.
func (v *vectorIndexOpaque) mergeAndWriteVectorIndexes(fieldID int, sbs []*SegmentBase,
	vecToDocID map[uint64]*roaring.Bitmap, indexes []vecIndexMeta, w *CountHashWriter) error {
	if len(vecToDocID) >= 100000 {
		// merging of more complex index types (for eg ivf family) with reconstruction
		// method.
		return fmt.Errorf("to be implemented")
	}

	var vecIndexes []*faiss.IndexImpl
	for segI, seg := range sbs {
		// read the index bytes
		indexBytes := seg.mem[indexes[segI].startOffset : indexes[segI].startOffset+int(indexes[segI].indexSize)]
		index, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
		if err != nil {
			return err
		}
		vecIndexes = append(vecIndexes, index)
	}

	for i := 1; i < len(vecIndexes); i++ {
		err := vecIndexes[0].MergeFrom(vecIndexes[i], 0)
		if err != nil {
			return err
		}
	}

	mergedIndexBytes, err := faiss.WriteIndexIntoBuffer(vecIndexes[0])
	if err != nil {
		return err
	}

	tempBuf := v.grabBuf(binary.MaxVarintLen64)
	// start writing out the merged info to writer
	fieldStart := w.Count()
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

	n = binary.PutUvarint(tempBuf, uint64(len(mergedIndexBytes)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	// write the vector index data
	_, err = w.Write(mergedIndexBytes)
	if err != nil {
		return err
	}

	// write the number of unique vectors
	n = binary.PutUvarint(tempBuf, uint64(len(vecToDocID)))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return err
	}

	for vecID, docIDs := range vecToDocID {
		// write the vecID
		_, err := writeUvarints(w, uint64(vecID))
		if err != nil {
			return err
		}

		// write the docIDs
		_, err = writeRoaringWithLen(docIDs, w, tempBuf)
		if err != nil {
			return err
		}
	}

	v.fieldAddrs[uint16(fieldID)] = fieldStart
	return nil
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

		// create an index, its always a flat for now, because each batch size
		// won't have too many vectors (in order for >100K). todo: will need to revisit
		// this logic - creating based on configured batch size in scorch.
		index, err := faiss.IndexFactory(int(content.dim), "IDMap2,Flat", faiss.MetricInnerProduct)
		if err != nil {
			return 0, err
		}

		err = index.Train(vecs)
		if err != nil {
			return 0, err
		}

		index.AddWithIDs(vecs, ids)
		if err != nil {
			return 0, err
		}

		// serialize the built index into a byte slice
		buf, err := faiss.WriteIndexIntoBuffer(index)
		if err != nil {
			return 0, err
		}

		fieldStart := w.Count()
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
		n = binary.PutUvarint(tempBuf, uint64(len(content.vecs)))
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
			_, err := writeUvarints(w, uint64(vecID))
			if err != nil {
				return 0, err
			}

			// write the docIDs
			_, err = writeRoaringWithLen(docIDs, w, tempBuf)
			if err != nil {
				return 0, err
			}
		}

		vo.fieldAddrs[fieldID] = fieldStart
	}
	return 0, nil
}

func (vo *vectorIndexOpaque) process(field index.VectorField, fieldID uint16, docNum uint64) {
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
		vo.vecIDMap[vecHash].docIDs.Add(uint32(docNum))

		if _, ok := vo.vecFieldMap[fieldID]; !ok {
			vo.vecFieldMap[fieldID] = indexContent{
				vecs: map[uint32]vecInfo{
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
func hashCode(a []float32) uint32 {
	var rv uint32
	for _, v := range a {
		rv = rv ^ math.Float32bits(v)
	}

	return rv
}

func (v *vectorIndexOpaque) allocateSpace() {
	// todo: allocate the space for the opaque contents if possible.
	// basically to avoid too many heap allocs and also reuse things
}

func (v *vectorIndexSection) getvectorIndexOpaque(opaque map[int]resetable) *vectorIndexOpaque {
	if _, ok := opaque[sectionVectorIndex]; !ok {
		opaque[sectionVectorIndex] = v.InitOpaque(nil)
	}
	return opaque[sectionVectorIndex].(*vectorIndexOpaque)
}

func (v *vectorIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &vectorIndexOpaque{
		fieldAddrs:  make(map[uint16]int),
		vecIDMap:    make(map[uint32]vecInfo),
		vecFieldMap: make(map[uint16]indexContent),
	}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

type indexContent struct {
	vecs   map[uint32]vecInfo
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

	fieldAddrs map[uint16]int

	vecIDMap    map[uint32]vecInfo
	vecFieldMap map[uint16]indexContent

	tmp0 []byte
}

func (vo *vectorIndexOpaque) Reset() (err error) {
	// cleanup stuff over here

	return nil
}
func (v *vectorIndexOpaque) Set(key string, val interface{}) {

}
