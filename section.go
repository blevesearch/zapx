package zap

import (
	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
)

type section interface {
	Process(opaque map[int]resetable, docNum uint64, f index.Field, fieldID uint16)

	Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error)

	AddrForField(opaque map[int]resetable, fieldID int) int

	Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
		newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error

	InitOpaque(args map[string]interface{}) resetable
}

type resetable interface {
	Reset() error
	Set(key string, value interface{})
}

const (
	sectionInvertedIndex = iota
	sectionNumericRangeIndex
)

var segmentSections = map[uint16]section{
	sectionInvertedIndex:     &invertedIndexSection{},
	sectionNumericRangeIndex: &numericRangeIndexSection{},
}
