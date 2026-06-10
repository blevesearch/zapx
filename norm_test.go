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
	"math"
	"testing"
)

// TestEncodeNormByteRoundTrip verifies that encodeNormByte / normDecodeSmallFloat
// satisfy the SmallFloat contract: decoding an encoded field length gives back an
// approximation within the expected relative error bound.
//
// SmallFloat uses a 3-bit mantissa and 5-bit exponent (Lucene SmallFloat 3/15
// variant), so the maximum relative error per quantisation step is ≤ 12.5%
// (1/8 = one mantissa-bit width).  We accept up to 13% to allow rounding.
func TestEncodeNormByteRoundTrip(t *testing.T) {
	const maxRelErr = 0.13

	testCases := []uint32{
		1, 2, 3, 4, 5, 7, 10, 15, 20, 50, 100, 200, 500, 1000, 2000,
	}
	for _, fieldLen := range testCases {
		b := encodeNormByte(fieldLen)
		decoded := normDecodeSmallFloat(b)

		// Both values must be positive.
		if decoded == 0 {
			t.Errorf("fieldLen=%d: decoded to 0", fieldLen)
			continue
		}
		// Relative error = |decoded - fieldLen| / fieldLen.
		relErr := math.Abs(float64(decoded)-float64(fieldLen)) / float64(fieldLen)
		if relErr > maxRelErr {
			t.Errorf("fieldLen=%d: encoded=0x%02x decoded=%d relErr=%.2f%% (max %.0f%%)",
				fieldLen, b, decoded, relErr*100, maxRelErr*100)
		}
	}
}

// TestEncodeNormByteMonotone verifies that encodeNormByte is non-decreasing:
// a longer field always encodes to a byte ≥ the shorter field's byte.
// This is required for normDecodeTable ordering to be consistent.
func TestEncodeNormByteMonotone(t *testing.T) {
	prev := encodeNormByte(1)
	for fieldLen := uint32(2); fieldLen <= 2000; fieldLen++ {
		cur := encodeNormByte(fieldLen)
		if cur < prev {
			t.Errorf("encodeNormByte not monotone: fieldLen=%d gives 0x%02x < prev 0x%02x",
				fieldLen, cur, prev)
		}
		prev = cur
	}
}

// TestNormDecodeTableConsistency verifies that normDecodeTable[b] matches the
// scalar normDecodeSmallFloat(b) for all 256 byte values.
func TestNormDecodeTableConsistency(t *testing.T) {
	for b := 0; b < 256; b++ {
		tableVal := normDecodeTable[b]
		scalarVal := normDecodeSmallFloat(uint8(b))
		if tableVal != scalarVal {
			t.Errorf("byte 0x%02x: table=%d scalar=%d", b, tableVal, scalarVal)
		}
	}
}

// TestEncodeNormByteZeroFieldLen ensures fieldLen=0 doesn't panic; the encoding
// may return any byte value since fieldLen=0 is not a valid document length.
func TestEncodeNormByteZeroFieldLen(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("encodeNormByte(0) panicked: %v", r)
		}
	}()
	_ = encodeNormByte(0)
}
