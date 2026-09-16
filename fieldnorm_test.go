//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"math"
	"testing"
)

func TestFieldNormTableStrictlyIncreasing(t *testing.T) {
	for id := 1; id < 256; id++ {
		if fieldNormTable[id] <= fieldNormTable[id-1] {
			t.Fatalf("fieldNormTable not strictly increasing at id %d: %d <= %d",
				id, fieldNormTable[id], fieldNormTable[id-1])
		}
	}
}

func TestFieldNormTableKnownValues(t *testing.T) {
	// These pin the quantization to tantivy's, so a segment written here and a
	// segment written there agree on what a norm id means.
	tests := []struct {
		id   uint8
		want uint32
	}{
		{0, 0}, {1, 1}, {23, 23}, {24, 24}, {31, 31}, {32, 32},
		{39, 39}, {40, 40}, {41, 42}, {255, 2013265944},
	}
	for _, tt := range tests {
		if got := idToFieldNorm(tt.id); got != tt.want {
			t.Errorf("idToFieldNorm(%d) = %d, want %d", tt.id, got, tt.want)
		}
	}
}

func TestFieldNormExactBelow41(t *testing.T) {
	for l := uint32(0); l <= 40; l++ {
		id := fieldNormToID(l)
		if got := idToFieldNorm(id); got != l {
			t.Errorf("length %d round-tripped to %d (id %d), want exact", l, got, id)
		}
	}
}

func TestFieldNormRoundsDown(t *testing.T) {
	for l := uint32(0); l < 100000; l++ {
		id := fieldNormToID(l)
		stored := idToFieldNorm(id)
		if stored > l {
			t.Fatalf("length %d quantized up to %d", l, stored)
		}
		if id < 255 && idToFieldNorm(id+1) <= l {
			t.Fatalf("length %d quantized to id %d, but id %d also fits", l, id, id+1)
		}
		// Relative error above the identity range is bounded by the 3-bit
		// mantissa.
		if l > 40 && float64(l-stored)/float64(l) > 1.0/8.0 {
			t.Fatalf("length %d quantized to %d, relative error exceeds 1/8", l, stored)
		}
	}
}

func TestFieldNormSaturates(t *testing.T) {
	if got := fieldNormToID(math.MaxUint32); got != 255 {
		t.Errorf("fieldNormToID(MaxUint32) = %d, want 255", got)
	}
}

func TestNormFactorFromID(t *testing.T) {
	if !math.IsInf(normFactorFromID(0), 1) {
		t.Errorf("normFactorFromID(0) = %v, want +Inf", normFactorFromID(0))
	}
	for id := 1; id < 256; id++ {
		want := float64(float32(1.0 / math.Sqrt(float64(fieldNormTable[id]))))
		if got := normFactorFromID(uint8(id)); got != want {
			t.Errorf("normFactorFromID(%d) = %v, want %v", id, got, want)
		}
	}
}
