// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"math/rand"
	"testing"
)

// reusable scratch buffers for decodePFORBlock
var testDst = make([]uint64, pforBlockSize)
var testExcIdx = make([]int, pforBlockSize)
var testExcVal = make([]uint64, pforBlockSize)

func decodeFull(data []byte) ([]uint64, int) {
	return decodePFORBlock(data, testDst, testExcIdx, testExcVal)
}

func TestPFORConstantBlock(t *testing.T) {
	for _, constVal := range []uint64{0, 1, 42, 255} {
		vals := make([]uint64, pforBlockSize)
		for i := range vals {
			vals[i] = constVal
		}
		enc := encodePFORBlock(vals)
		// constant block: count(1) + bw=0(1) + constVal(1) = 3 bytes
		if len(enc) != 3 {
			t.Errorf("constVal=%d: expected 3-byte encoding, got %d bytes", constVal, len(enc))
		}
		got, n := decodeFull(enc)
		if n != len(enc) {
			t.Errorf("constVal=%d: bytesConsumed=%d, want %d", constVal, n, len(enc))
		}
		if len(got) != pforBlockSize {
			t.Fatalf("constVal=%d: decoded %d values, want %d", constVal, len(got), pforBlockSize)
		}
		for i, v := range got {
			if v != constVal {
				t.Errorf("constVal=%d: got[%d]=%d", constVal, i, v)
			}
		}
	}
}

func TestPFORFullBlockNoExceptions(t *testing.T) {
	// All values fit in bw bits, no exceptions.
	vals := make([]uint64, pforBlockSize)
	for i := range vals {
		vals[i] = uint64(i % 16) // 4-bit values
	}
	enc := encodePFORBlock(vals)
	got, n := decodeFull(enc)
	if n != len(enc) {
		t.Fatalf("bytesConsumed=%d, want %d", n, len(enc))
	}
	if len(got) != pforBlockSize {
		t.Fatalf("decoded %d values, want %d", len(got), pforBlockSize)
	}
	for i, v := range got {
		if v != vals[i] {
			t.Errorf("got[%d]=%d, want %d", i, v, vals[i])
		}
	}
}

func TestPFORFullBlockWithExceptions(t *testing.T) {
	// Mix of small values and a few large outliers (exceptions).
	vals := make([]uint64, pforBlockSize)
	for i := range vals {
		vals[i] = 3
	}
	vals[0] = 1000   // exception at pos 0
	vals[127] = 9999 // exception near middle
	vals[255] = 500  // exception at last position

	enc := encodePFORBlock(vals)
	got, n := decodeFull(enc)
	if n != len(enc) {
		t.Fatalf("bytesConsumed=%d, want %d", n, len(enc))
	}
	if len(got) != pforBlockSize {
		t.Fatalf("decoded %d values, want %d", len(got), pforBlockSize)
	}
	for i, v := range got {
		if v != vals[i] {
			t.Errorf("got[%d]=%d, want %d", i, v, vals[i])
		}
	}
}

func TestPFORPartialBlock(t *testing.T) {
	// Fewer than pforBlockSize values.
	for _, n := range []int{1, 7, 31, 100, 255} {
		vals := make([]uint64, n)
		for i := range vals {
			vals[i] = uint64(i + 1)
		}
		enc := encodePFORBlock(vals)
		got, consumed := decodeFull(enc)
		if consumed != len(enc) {
			t.Errorf("n=%d: bytesConsumed=%d, want %d", n, consumed, len(enc))
		}
		if len(got) != n {
			t.Fatalf("n=%d: decoded %d values, want %d", n, len(got), n)
		}
		for i, v := range got {
			if v != vals[i] {
				t.Errorf("n=%d: got[%d]=%d, want %d", n, i, v, vals[i])
			}
		}
	}
}

func TestPFORTruncatedDataReturnsNil(t *testing.T) {
	vals := make([]uint64, pforBlockSize)
	for i := range vals {
		vals[i] = uint64(i)
	}
	enc := encodePFORBlock(vals)

	// Truncate to various lengths and verify no crash, nil return.
	for trunc := 0; trunc < len(enc); trunc++ {
		got, _ := decodeFull(enc[:trunc])
		if trunc < 3 && got != nil {
			t.Errorf("trunc=%d: expected nil for very short input, got %v", trunc, got)
		}
	}
}

func TestPFOROptimalBitWidth(t *testing.T) {
	// All same value → bw should prefer the constant-block path (bw=0 is handled
	// by the encoder before calling pforOptimalBitWidth, but for general inputs
	// with mixed values bw should be the tightest fit).
	vals := []uint64{1, 2, 3, 4}
	bw := pforOptimalBitWidth(vals)
	if bw < 2 || bw > 32 {
		t.Errorf("unexpected bw=%d for vals %v", bw, vals)
	}
	// All zeros (except one outlier) → low bw with one exception is preferred
	// over bw=32 for all.
	big := make([]uint64, 128)
	big[0] = 1 << 31 // large outlier
	bw2 := pforOptimalBitWidth(big)
	// With 127 zeros and one 31-bit value, 1-bit width + 1 exception is cheaper
	// than 31-bit width for all 128 values.
	if bw2 >= 31 {
		t.Errorf("expected small bw for mostly-zero slice, got %d", bw2)
	}
}

func TestPFORRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 200; iter++ {
		n := rng.Intn(pforBlockSize) + 1
		vals := make([]uint64, n)
		for i := range vals {
			// Mix of small and occasionally large values.
			if rng.Intn(20) == 0 {
				vals[i] = uint64(rng.Uint32())
			} else {
				vals[i] = uint64(rng.Intn(256))
			}
		}
		enc := encodePFORBlock(vals)
		got, consumed := decodeFull(enc)
		if consumed != len(enc) {
			t.Fatalf("iter=%d n=%d: bytesConsumed=%d, want %d", iter, n, consumed, len(enc))
		}
		if len(got) != n {
			t.Fatalf("iter=%d n=%d: decoded %d values", iter, n, len(got))
		}
		for i, v := range got {
			if v != vals[i] {
				t.Fatalf("iter=%d n=%d i=%d: got %d want %d", iter, n, i, v, vals[i])
			}
		}
	}
}
