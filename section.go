package zap

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
)

type section interface {
	Process(opaque map[int]resetable, docNum uint64, f index.Field, fieldID uint16)

	Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error)

	AddrForField(opaque map[int]resetable, fieldID int) int

	Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
		newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error

	InitOpaque() resetable
}

const (
	sectionInvertedIndex = iota
	sectionNumericRangeIndex
)

var segmentSections = map[uint16]section{
	sectionInvertedIndex:     &invertedIndexSection{},
	sectionNumericRangeIndex: &numericRangeIndexSection{},
}

type invertedIndexSection struct {
}

func (i *invertedIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {

}

func (i *invertedIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {
	return 0, nil
}

func (i *invertedIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	return 0
}

func (i *invertedIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {
	return nil
}

func (i *invertedIndexSection) InitOpaque() resetable {
	return nil
}

type numericRangeIndexSection struct {
}

func (n *numericRangeIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {
	if nf, ok := field.(index.NumericField); ok {
		nfv, _ := nf.Number()
		n.process(opaque, docNum, nfv, fieldID)
	}
}

func (n *numericRangeIndexSection) process(opaque map[int]resetable, docNum uint64, val float64, fieldID uint16) {
	nro := n.getNumericRangeOpaque(opaque)
	nro.byField[int(fieldID)] = AddNumericValue(val, docNum, nro.byField[int(fieldID)])
}

func (n *numericRangeIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (int64, error) {
	nro := n.getNumericRangeOpaque(opaque)

	// for each field
	for fieldId, nrNodes := range nro.byField {

		// for each value in field
		for _, node := range nrNodes {

			// write out bitmap of doc nums having this value

			// FIXME support reusing buffer?
			var err error

			// record address of this bitmap for later use
			node.addr = w.Count()
			_, err = writeRoaringWithLen(node.docs, w, make([]byte, binary.MaxVarintLen64))
			if err != nil {
				return 0, err
			}
		}

		fieldStart := w.Count()
		// now write the number of values we have
		writeUvarints(w, uint64(len(nrNodes)))
		// now write out each of the values
		for _, node := range nrNodes {
			// FIXME must write in little endian to use unsafe slice conversion on read-side
			binary.Write(w, binary.LittleEndian, node.val)
		}
		// now write out each of the addresses
		for _, node := range nrNodes {
			// FIXME must write in little endian to use unsafe slice conversion on read-side
			binary.Write(w, binary.LittleEndian, uint64(node.addr))
		}
		nro.fieldAddrs[fieldId] = fieldStart
	}

	return 0, nil
}

func (n *numericRangeIndexSection) getNumericRangeOpaque(opaque map[int]resetable) *numericRangeOpaque {
	if _, ok := opaque[sectionNumericRangeIndex]; !ok {
		opaque[sectionNumericRangeIndex] = n.InitOpaque()
	}
	return opaque[sectionNumericRangeIndex].(*numericRangeOpaque)
}

func (n *numericRangeIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	nro := n.getNumericRangeOpaque(opaque)
	return nro.fieldAddrs[fieldID]
}

// FIXME need to check the closeCh sometime
func (n *numericRangeIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {

	// try to merge field at a time?
	for fieldID := range fieldsInv {

		// iterate each segment
		for segI, sb := range segments {
			fieldNumericSection := sb.fieldsSectionsMap[fieldID][sectionNumericRangeIndex]

			// read how many values are in this field
			numNumericValues, sz := binary.Uvarint(sb.mem[fieldNumericSection : fieldNumericSection+binary.MaxVarintLen64])
			pos := fieldNumericSection + uint64(sz)
			floatData, err := ByteSliceToFloat6464Slice(sb.mem[pos : pos+(numNumericValues*8)])
			if err != nil {
				return err
			}

			floatOffsets := pos + (numNumericValues * 8)
			floatOffsetsSlice, err := ByteSliceToUint64Slice(sb.mem[floatOffsets : floatOffsets+(numNumericValues*8)])
			if err != nil {
				return err
			}

			// now walk each float val
			for walkIndex := 0; walkIndex < int(numNumericValues); walkIndex++ {
				valueBitmapOffset := floatOffsetsSlice[walkIndex]

				// load/read roaring for this value

				bitSetLen, read := binary.Uvarint(sb.mem[valueBitmapOffset : valueBitmapOffset+binary.MaxVarintLen64])
				bitSetStart := valueBitmapOffset + uint64(read)

				roaringBytes := sb.mem[bitSetStart : bitSetStart+bitSetLen]

				// fixme reuse roaring bitmap in this loop
				postings := roaring.New()
				_, err := postings.FromBuffer(roaringBytes)
				if err != nil {
					return fmt.Errorf("error loading roaring bitmap: %v", err)
				}

				// now iterate it
				itr := postings.Iterator()
				for itr.HasNext() {
					segmentLocalDocNum := itr.Next()
					n.process(opaque, newDocNumsIn[segI][segmentLocalDocNum], floatData[walkIndex], uint16(fieldID))
				}
			}
		}
	}

	// now we have merged all the numeric data, we can start writing
	_, err := n.Persist(opaque, w)

	return err
}

func (n *numericRangeIndexSection) InitOpaque() resetable {
	return &numericRangeOpaque{
		byField:    map[int]nrNodes{},
		fieldAddrs: map[int]int{},
	}
}

type numericRangeOpaque struct {
	byField    map[int]nrNodes
	fieldAddrs map[int]int
}

func (n *numericRangeOpaque) Reset() {
	n.byField = map[int]nrNodes{}
	n.fieldAddrs = map[int]int{}
}
