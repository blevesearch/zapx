package zap

import (
	"math"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func getStubDocScores(k int) (ids []uint64, scores []float32, err error) {
	for i := 1; i <= k; i++ {
		ids = append(ids, uint64(i))
		scores = append(scores, float32((2*i+3)/200))
	}
	return ids, scores, nil
}

func TestVecPostingsIterator(t *testing.T) {

	vecPL := &VecPostingsList{
		postings: roaring64.New(),
	}

	ids, scores, err := getStubDocScores(10)
	if err != nil {
		t.Fatal(err)
	}
	docIDs := make(map[uint64]float32)

	for i, id := range ids {
		code := uint64(id)<<31 | uint64(math.Float32bits(scores[i]))
		vecPL.postings.Add(code)
		docIDs[id] = scores[i]
	}

	iter := vecPL.Iterator(nil)
	for i := 0; true; i++ {
		vp, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if vp == nil {
			break
		}
		if vp.Number() != ids[i] {
			t.Fatalf("expected %d, got %d", ids[i], vp.Number())
		}
		if vp.Score() != scores[i] {
			t.Fatalf("expected %f, got %f", scores[i], vp.Score())
		}
	}
}
