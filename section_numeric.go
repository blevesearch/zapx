package zap

import (
	"github.com/RoaringBitmap/roaring"
	"sort"
)

type nrNode struct {
	val float64
	docs *roaring.Bitmap

	addr int
}

type nrNodes []*nrNode

func (n nrNodes) Len() int {
	return len(n)
}

func (n nrNodes) Less(i, j int) bool {
	return n[i].val < n[j].val
}

func (n nrNodes) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func AddNumericValue(f float64, docNum uint64, nr nrNodes) nrNodes {
	idx := sort.Search(len(nr), func(i int) bool {
		return nr[i].val >= f
	})
	if idx < len(nr) && nr[idx].val == f {
		nr[idx].docs.Add(uint32(docNum))
	} else {
		// cheating for now, just appending to the end and resorting
		bs := roaring.New()
		bs.Add(uint32(docNum))
		rv := append(nr, &nrNode{
			val: f,
			docs: bs,
		})
		// full resort
		sort.Sort(nr)
		return rv
	}
	return nr
}
