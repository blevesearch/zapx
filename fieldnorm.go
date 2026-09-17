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
	"sort"
)

// The field norm is the number of tokens in the field that is indexed.
// Typically this value is a uint32 (integer length, always positive).
// At search time, we need the reciprocal square root of this (1 / sqrt(norm))
// which we refer to here as the norm factor.
//
// One key thing to note is that the final BM25/TF-IDF score is robust to
// perturbations in the norm/norm factor. Search engines take advantage of this
// by trying to "quantize" the norm to 8 bits rather than 32 bits usually needed.
// This quantized form is called the norm ID.
//
// We store a precomputed table `fieldNormTable` that maps the norm ID to the
// field length it represents so that it's just a simple lookup. There are two
// ways the norm ID represents numbers:
// 1. If the norm ID has a value < 24, the norm ID stores the raw length
// 2. If the norm ID >= 24, we store the difference between the ID and 24
// 	  in a custom floating point type that fits in the 8 bits: the lowest 3
//    bits are used for the mantissa, the highest 5 are the exponent.
//
// The way the data is encoded ensures that norm values up until 40 is represented
// exactly, and values above 40 have a relative error bounded by 12.5%.
//
// We also store an additional precomputed table `fieldNormFactor` that directly
// gives us the norm factor for a norm ID.

// identityPart is the number of leading ids that decode to themselves before
// the exponential part takes over.
const identityPart = 24

// fieldNormTable maps a norm id to the field length it represents.  It is
// strictly increasing, which is what lets fieldNormToID binary-search it.
var fieldNormTable [256]uint32

// fieldNormFactor maps a norm id to 1/sqrt(length), the value the scorer wants.
// fieldNormFactor[0] is +Inf, matching the behaviour of a zero-length field in
// earlier zap versions.
var fieldNormFactor [256]float32

func init() {
	for id := 0; id < 256; id++ {
		fieldNormTable[id] = decodeFieldNormID(uint8(id))
		fieldNormFactor[id] = float32(1.0 / math.Sqrt(float64(fieldNormTable[id])))
	}
}

// decodeExpPart decodes the exponential portion of a norm id: the low 3 bits
// are the mantissa, the high 5 are the exponent.  A zero exponent means the
// mantissa is the value verbatim; otherwise the implicit leading 1 is restored
// and the result shifted.
func decodeExpPart(b uint8) uint32 {
	bits := uint32(b & 0x07)
	shift := b >> 3
	if shift == 0 {
		return bits
	}
	return (bits | 8) << (shift - 1)
}

func decodeFieldNormID(id uint8) uint32 {
	if id < identityPart {
		return uint32(id)
	}
	return identityPart + decodeExpPart(id-identityPart)
}

// ---------------------------------------------------------------
// Functions below use the precomputed tables to get norm, norm ID
// and norm factor

// idToFieldNorm returns the field length a norm id stands for.
func idToFieldNorm(id uint8) uint32 {
	return fieldNormTable[id]
}

// fieldNormToID quantizes a field length to the largest norm id whose length is
// less than or equal to it, saturating at 255.
func fieldNormToID(fieldNorm uint32) uint8 {
	// sort.Search gives the first index whose value exceeds fieldNorm; the id
	// we want is the one before it.  Index 0 holds length 0, so the result of
	// the search is always at least 1 and the subtraction is safe.
	return uint8(sort.Search(256, func(i int) bool {
		return fieldNormTable[i] > fieldNorm
	}) - 1)
}

// normFactorFromID returns the normalization factor 1/sqrt(fieldLength) for a
// norm id, as one L1-resident table lookup.
func normFactorFromID(id uint8) float64 {
	return float64(fieldNormFactor[id])
}
