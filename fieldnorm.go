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

// A field norm is the number of tokens the analyzer emitted for one field of
// one document -- the "dl" term in BM25.  It is stored as a single quantized
// byte per document per field, in a dense column shared by every term of the
// field, rather than being repeated alongside every posting.
//
// The quantization is the scheme Lucene and tantivy both use: a 3-bit mantissa
// with an implicit leading 1 and a 5-bit exponent, biased so that the first 41
// lengths are represented exactly.  Properties worth knowing:
//
//   - lengths 0..=40 represented exactly, so short fields (titles, names, tags),
//     where length normalization matters most, lose nothing;
//   - the mapping is monotonic and rounds *down*, so a stored length is never
//     larger than the true one;
//   - relative error above 40 is bounded by 1/8;
//   - it saturates at fieldNormTable[255].

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
