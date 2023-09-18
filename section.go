package zap

import (
	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
)

type section interface {
	// process is essentially parsing of a specific field's content in a specific
	// document. any tracking of processed data *specific to this section* should
	// be done in opaque which will be passed to the Persist() API.
	Process(opaque map[int]resetable, docNum uint64, f index.Field, fieldID uint16)

	// flush the processed data in the opaque to the writer.
	Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error)

	// this API is used to fetch the file offset of the field for this section.
	// this is used during search time to parse the section, and fetch results
	// for the specific "index" thats part of the section.
	AddrForField(opaque map[int]resetable, fieldID int) int

	// for every field in the fieldsInv (relevant to this section) merge the section
	// contents from all the segments into a single section data for the field.
	// as part of the merge API, write the merged data to the writer and also track
	// the starting offset of this newly merged section data.
	Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
		newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error

	// opaque is used to track the data specific to this section. its not visible
	// to the other sections and is only visible and freely modifiable by this specifc
	// section.
	InitOpaque(args map[string]interface{}) resetable
}

type resetable interface {
	Reset() error
	Set(key string, value interface{})
}

const (
	sectionInvertedIndex = iota
)

var segmentSections = map[uint16]section{
	sectionInvertedIndex: &invertedTextIndexSection{},
}
