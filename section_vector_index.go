package zap

// keep a build tag for this file.

import (
	"math"

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	faiss "github.com/blevesearch/go-faiss"
)

type vectorIndexSection struct {
}

func (v *vectorIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}

	if vf, ok := field.(index.DenseVectorField); ok {
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

	return -1
}

func (v *vectorIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {

	return nil
}

// todo: is it possible to merge this resuable stuff with the interim's tmp0?
func (v *vectorIndexOpaque) grabBuf(size int) []byte {
	return nil
}

func (vo *vectorIndexOpaque) writeVectorIndexes(w *CountHashWriter) (offset uint64, err error) {
	// for every fieldID, contents to store over here are:
	//    1. the serialized representation of the dense vector index.
	//    2. its constituent vectorIDs -> docID mapping. perhaps a bitmap is enough.

	for _, content := range vo.vecFieldMap {

		var vecs []float32
		for _, vec := range content.vecs {
			vecs = append(vecs, vec...)
		}

		// create an index
		index, err := faiss.IndexFactory(int(content.dim), "Flat", faiss.MetricL2)
		if err != nil {
			return 0, err
		}

		err = index.Train(vecs)
		if err != nil {
			return 0, err
		}

		// todo: this must be add_with_ids
		index.Add(vecs)
		if err != nil {
			return 0, err
		}

		// serialize the built index into a byte slice
		buf, err := faiss.WriteIndexIntoBuffer(index)
		if err != nil {
			return 0, err
		}
		// record the fieldStart value for this section.
		// write the vecID -> docID mapping
		w.Write(buf)

	}
	return 0, nil
}

func (vo *vectorIndexOpaque) process(field index.DenseVectorField, fieldID uint16, docNum uint64) {
	if !vo.init {
		vo.init = true
		vo.allocateSpace()
	}
	if fieldID == math.MaxUint16 {
		// doc processing checkpoint. currently nothing to do
		return
	}

	//process field

	vec, dim, metric := field.DenseVector()
	if vec != nil {

		// NOTE: currently, indexing only unique vectors.
		vecHash := hashCode(vec)
		if _, ok := vo.vecIDMap[vecHash]; !ok {
			vo.vecIDMap[vecHash] = vecInfo{
				vecID:  uint64(len(vo.vecIDMap)),
				docIDs: roaring.NewBitmap(),
			}
		}
		// add the docID to the bitmap
		vo.vecIDMap[vecHash].docIDs.Add(uint32(docNum))

		vecKey := vo.vecIDMap[vecHash].vecID
		if index, ok := vo.vecFieldMap[fieldID]; !ok {
			vo.vecFieldMap[fieldID] = indexContent{
				vecs: map[uint64][]float32{
					vecKey: vec,
				},
				dim:    uint16(dim),
				metric: metric,
			}
		} else {
			index.vecs[vecKey] = vec
		}
	}
}

// todo: better hash function?
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

// revisit this function's purpose etc.
func (v *vectorIndexOpaque) getOrDefineField(fieldName string) int {
	return -1
}

func (v *vectorIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &vectorIndexOpaque{}
	for k, v := range args {
		rv.Set(k, v)
	}

	return rv
}

type indexContent struct {
	vecs   map[uint64][]float32
	dim    uint16
	metric string
}

type vecInfo struct {
	vecID  uint64
	docIDs *roaring.Bitmap
}

type vectorIndexOpaque struct {
	results []index.Document
	init    bool

	vecIDMap    map[uint32]vecInfo
	vecFieldMap map[uint16]indexContent
}

func (vo *vectorIndexOpaque) Reset() (err error) {
	// cleanup stuff over here

	return nil
}
func (v *vectorIndexOpaque) Set(key string, val interface{}) {

}
