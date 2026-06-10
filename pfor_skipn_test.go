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
	"encoding/binary"
	"testing"
)

// TestSkipNPFORMode verifies that SkipN advances pforPos by exactly n
// positions and that the next readUvarint returns the correct value.
func TestSkipNPFORMode(t *testing.T) {
	const n = pforBlockSize
	vals := make([]uint64, n)
	for i := range vals {
		vals[i] = uint64(i*3 + 7) // arbitrary distinct values
	}

	d := &chunkedIntDecoder{}
	d.SetPFORMode(true)
	d.pforDecoded = append(d.pforDecoded[:0], vals...)

	// SkipN(5): next read must return vals[5].
	d.SkipN(5)
	if d.pforPos != 5 {
		t.Fatalf("pforPos after SkipN(5): got %d, want 5", d.pforPos)
	}
	v, err := d.readUvarint()
	if err != nil || v != vals[5] {
		t.Fatalf("readUvarint after SkipN(5): got (%d, %v), want (%d, nil)", v, err, vals[5])
	}

	// SkipN(0) must be a no-op.
	before := d.pforPos
	d.SkipN(0)
	if d.pforPos != before {
		t.Fatalf("SkipN(0) moved pforPos: %d → %d", before, d.pforPos)
	}

	// Skip all remaining values except the last, then read the last.
	remaining := len(d.pforDecoded) - d.pforPos - 1
	d.SkipN(remaining)
	last, err := d.readUvarint()
	if err != nil || last != vals[n-1] {
		t.Fatalf("last value: got (%d, %v), want (%d, nil)", last, err, vals[n-1])
	}
}

// TestSkipNVarintMode verifies SkipN falls back to n SkipUvarint calls in
// non-PFOR mode and leaves the reader positioned at the right entry.
func TestSkipNVarintMode(t *testing.T) {
	vals := []uint64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	var buf [binary.MaxVarintLen64 * 10]byte
	pos := 0
	for _, v := range vals {
		pos += binary.PutUvarint(buf[pos:], v)
	}

	d := &chunkedIntDecoder{}
	d.r = newMemUvarintReader(buf[:pos])

	// SkipN(3): next read must return vals[3] = 40.
	d.SkipN(3)
	v, err := d.readUvarint()
	if err != nil || v != vals[3] {
		t.Fatalf("after SkipN(3): got (%d, %v), want (%d, nil)", v, err, vals[3])
	}
}
