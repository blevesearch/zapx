//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"
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

// keep in mind with respect to update and delete opeartions with resepct to vectors/
// leverage bitmaps stored
func (v *vectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
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
		index, err := faiss.IndexFactory(int(content.dim), "IDMap2,Flat", faiss.MetricL2)
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
