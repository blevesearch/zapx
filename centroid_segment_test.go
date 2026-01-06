package zap

import (
	"fmt"
	"testing"

	faiss "github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func TestCentroidSegment(t *testing.T) {
	// get the centroid index from the path
	path := "./centroid_index"
	zapPlugin := &ZapPlugin{}
	seg, err := zapPlugin.OpenEx(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	config := map[string]interface{}{
		"getCentroidIndexCallback": func(field string) (*faiss.IndexImpl, error) {
			centroidIndexSegment, ok := seg.(segment.CentroidIndexSegment)
			if !ok {
				return nil, fmt.Errorf("segment is not a centroid index segment", seg != nil)
			}
			return centroidIndexSegment.GetCoarseQuantizer(field)
		},
	}

	// new IVF segment

	// MergeEx(path, config)

	// create a new segment
	// the faiss index should be used a template for the Merge API()
}
