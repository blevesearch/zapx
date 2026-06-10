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

import "math"

// normDecodeTable maps a SmallFloat-encoded byte (0–255) to the approximate
// field length as a uint32.  Index: encoded byte.  Value: approx fieldLen.
//
// Usage at query time:
//
//	normBits  = uint64(normDecodeTable[normByte])
//	p.norm    = math.Float32frombits(uint32(normBits))  // type-pun, not cast
//	p.Norm()  = 1/sqrt(float64(normBits)) ≈ 1/sqrt(fieldLen)
var normDecodeTable [256]uint32

func init() {
	for i := 0; i < 256; i++ {
		normDecodeTable[i] = normDecodeSmallFloat(uint8(i))
	}
}

// encodeNormByte encodes a field length into a 1-byte SmallFloat value.
// Based on Lucene's SmallFloat.floatToByte315: 3-bit mantissa, 5-bit exponent.
// Relative error ≤ ~12.5%; covers fieldLen 1–~2M.
func encodeNormByte(fieldLen uint32) uint8 {
	if fieldLen == 0 {
		return 0
	}
	bits := math.Float32bits(float32(fieldLen))
	mantissa := int((bits >> 20) & 0x7)
	biasedExp := int((bits >> 23) & 0xFF)
	exp := biasedExp - 117
	if exp <= 0 {
		return uint8(mantissa)
	}
	if exp >= 32 {
		return 0xFF
	}
	return uint8((exp << 3) | mantissa)
}

// normDecodeSmallFloat is the scalar decoder used to build normDecodeTable.
func normDecodeSmallFloat(b uint8) uint32 {
	if b == 0 {
		return 0
	}
	mantissa := float64(b&0x7)/8.0 + 1.0
	exp := int(b>>3) - 10
	v := math.Ldexp(mantissa, exp)
	if v < 1 {
		return 1
	}
	return uint32(v + 0.5)
}
