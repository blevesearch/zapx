package zap

import (
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	faiss "github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func TestCentroidSegmentMerge(t *testing.T) {
	// get the centroid index from the path
	filepaths := []string{
		"/Users/thejas.orkombu/fts/builds/totoro/ns_server/data/n_0/data/@fts/bucket-1.scope-1.index_45108fa92e0430b7_162cc76c.pindex/store/0000000017a9.zap",
		"/Users/thejas.orkombu/fts/builds/totoro/ns_server/data/n_0/data/@fts/bucket-1.scope-1.index_45108fa92e0430b7_162cc76c.pindex/store/000000001118.zap",
		"/Users/thejas.orkombu/fts/builds/totoro/ns_server/data/n_0/data/@fts/bucket-1.scope-1.index_45108fa92e0430b7_162cc76c.pindex/store/00000000070d.zap",
	}

	centroidIndexPath := "/Users/thejas.orkombu/fts/builds/totoro/ns_server/data/n_0/data/@fts/bucket-1.scope-1.index_45108fa92e0430b7_162cc76c.pindex/store/centroid_index"

	centroidIndex, err := zapPlugin.OpenEx(centroidIndexPath, nil)
	if err != nil {
		t.Fatal(err)
	}

	callback := func(field string) (*faiss.IndexImpl, error) {
		centroidIndexSegment, ok := centroidIndex.(segment.CentroidIndexSegment)
		if !ok {
			return nil, fmt.Errorf("segment is not a centroid index segment")
		}
		return centroidIndexSegment.GetCoarseQuantizer(field)
	}

	var segs []segment.Segment
	for _, filepath := range filepaths {
		seg, err := zapPlugin.OpenEx(filepath, nil)
		if err != nil {
			t.Fatal(err)
		}

		segs = append(segs, seg)
	}

	config := map[string]interface{}{
		"getCentroidIndexCallback": callback,
	}
	_, _, err = zapPlugin.MergeEx(segs, []*roaring.Bitmap{nil, nil, nil}, "./merged_segment", nil, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	// path := "./centroid_index"
	// zapPlugin := &ZapPlugin{}
	// seg, err := zapPlugin.OpenEx(path, nil)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// config := map[string]interface{}{
	// 	"getCentroidIndexCallback": func(field string) (*faiss.IndexImpl, error) {
	// 		centroidIndexSegment, ok := seg.(segment.CentroidIndexSegment)
	// 		if !ok {
	// 			return nil, fmt.Errorf("segment is not a centroid index segment", seg != nil)
	// 		}
	// 		return centroidIndexSegment.GetCoarseQuantizer(field)
	// 	},
	// }

	// new IVF segment

	// MergeEx(path, config)

	// create a new segment
	// the faiss index should be used a template for the Merge API()
}
