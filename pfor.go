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

import "encoding/binary"

// pforBlockSize is the number of docNum slots covered by each PFOR block.
// Block k covers docNums [k*pforBlockSize, (k+1)*pforBlockSize).
// A block may contain fewer than pforBlockSize values if the posting list is sparse
// in that range; the actual count is stored in the block header.
const pforBlockSize = 256

// encodePFORBlock encodes a slice of uint64 values into a PFOR block.
// len(vals) must be in [1, pforBlockSize].
//
// Block format:
//
//	uint8   count        1..255; 0 encodes pforBlockSize (256)
//	uint8   bitWidth     0 = constant; 1..32 = packed bits per value
//	if bitWidth == 0:
//	  uint8  constVal    constant value for all entries (fits in one byte)
//	else:
//	  uint8  nExcepts
//	  uint8[nExcepts]   exceptIdx    positions (0..count-1) of exception values
//	  uint32[nExcepts]  exceptVal    LE uint32; full value at each exception position
//	  byte[(count*bitWidth+7)/8]  packedBits   LSB-first; exception positions packed as 0
func encodePFORBlock(vals []uint64) []byte {
	n := len(vals)
	if n == 0 {
		return nil
	}

	countByte := uint8(n & 0xFF) // 0 encodes as pforBlockSize

	// Fast path: all values are the same and fit in one byte.
	first := vals[0]
	allSame := true
	for i := 1; i < n; i++ {
		if vals[i] != first {
			allSame = false
			break
		}
	}
	if allSame && first <= 0xFF {
		return []byte{countByte, 0, uint8(first)}
	}

	// Find the bit width that minimises total encoded size.
	bw := pforOptimalBitWidth(vals[:n])
	var threshold uint64
	if bw < 64 {
		threshold = (uint64(1) << bw) - 1
	} else {
		threshold = ^uint64(0)
	}

	// Collect exceptions: values that don't fit in bw bits.
	var exceptIdx []uint8
	var exceptVal []uint32
	for i, v := range vals[:n] {
		if v > threshold {
			exceptIdx = append(exceptIdx, uint8(i))
			exceptVal = append(exceptVal, uint32(v))
		}
	}

	// Pack bw bits per value; exception positions are packed as 0.
	packBytes := (n*bw + 7) / 8
	packed := make([]byte, packBytes)
	bitPos := 0
	for _, v := range vals[:n] {
		vv := v
		if vv > threshold {
			vv = 0
		}
		for b := 0; b < bw; b++ {
			if (vv>>b)&1 == 1 {
				packed[bitPos>>3] |= 1 << (bitPos & 7)
			}
			bitPos++
		}
	}

	buf := make([]byte, 0, 3+len(exceptIdx)+len(exceptVal)*4+packBytes)
	buf = append(buf, countByte, uint8(bw), uint8(len(exceptIdx)))
	buf = append(buf, exceptIdx...)
	for _, ev := range exceptVal {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], ev)
		buf = append(buf, tmp[:]...)
	}
	buf = append(buf, packed...)
	return buf
}

// pforOptimalBitWidth finds the bit width in [1, 32] that minimises the
// encoded block size (packed bits + exceptions overhead).
func pforOptimalBitWidth(vals []uint64) int {
	n := len(vals)
	// Upper bound: bw=32, no exceptions — 3-byte header + n*4 bytes.
	bestSize := 3 + n*4
	bestBW := 32
	for bw := 1; bw <= 32; bw++ {
		var thresh uint64
		if bw < 64 {
			thresh = (uint64(1) << bw) - 1
		} else {
			thresh = ^uint64(0)
		}
		nExcepts := 0
		for _, v := range vals {
			if v > thresh {
				nExcepts++
			}
		}
		// 3 = header (count + bitWidth + nExcepts)
		// nExcepts * 5 = 1 index byte + 4 value bytes per exception
		size := 3 + (n*bw+7)/8 + nExcepts*5
		if size < bestSize {
			bestSize = size
			bestBW = bw
		}
	}
	return bestBW
}

// decodePFORBlock decodes a PFOR block from data into dst.
// dst is a caller-provided buffer (capacity pforBlockSize) reused across calls.
// Returns the updated slice (may alias dst) and bytes consumed.
// Returns (nil, 0) on any truncation or format error.
func decodePFORBlock(data []byte, dst []uint64) (vals []uint64, bytesConsumed int) {
	if len(data) < 2 {
		return nil, 0
	}

	count := int(data[0])
	if count == 0 {
		count = pforBlockSize
	}
	bw := int(data[1])
	pos := 2

	// allocBuf returns dst reused if large enough, else a fresh slice.
	allocBuf := func(n int) []uint64 {
		if cap(dst) >= n {
			return dst[:n]
		}
		return make([]uint64, n)
	}

	if bw == 0 {
		// Constant block: single byte holds the constant value.
		if pos >= len(data) {
			return nil, 0
		}
		constVal := uint64(data[pos])
		pos++
		out := allocBuf(count)
		for i := range out {
			out[i] = constVal
		}
		return out, pos
	}

	// Read exception count.
	if pos >= len(data) {
		return nil, 0
	}
	nExcepts := int(data[pos])
	pos++

	// Read exception indices.
	if pos+nExcepts > len(data) {
		return nil, 0
	}
	exceptIdxs := make([]int, nExcepts)
	for i := 0; i < nExcepts; i++ {
		exceptIdxs[i] = int(data[pos])
		pos++
	}

	// Read exception values (LE uint32).
	if pos+nExcepts*4 > len(data) {
		return nil, 0
	}
	exceptVals := make([]uint64, nExcepts)
	for i := 0; i < nExcepts; i++ {
		exceptVals[i] = uint64(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	}

	// Read and unpack bw bits per value (LSB-first within each byte).
	packBytes := (count*bw + 7) / 8
	if pos+packBytes > len(data) {
		return nil, 0
	}
	packed := data[pos : pos+packBytes]
	pos += packBytes

	out := allocBuf(count)
	mask := uint64((1 << bw) - 1)
	bitPos := 0
	for i := 0; i < count; i++ {
		var v uint64
		for b := 0; b < bw; b++ {
			if (packed[bitPos>>3]>>(bitPos&7))&1 == 1 {
				v |= 1 << b
			}
			bitPos++
		}
		out[i] = v & mask
	}

	// Patch exception positions with their full values.
	for i, idx := range exceptIdxs {
		if idx < count {
			out[idx] = exceptVals[i]
		}
	}
	return out, pos
}
