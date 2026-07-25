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

import "testing"

// TestReleaseChunkedIntDecoderResetsPFORMode pins the invariant that makes the
// decoder pool safe.
//
// Decoders are pooled without regard to which slot they served. A
// freqNormReader has SetPFORMode() called on it; a locReader never does and
// relies on pforMode being false. So a decoder released from a freqNormReader
// and handed back out as a locReader must not still be in PFOR mode, or it will
// try to PFOR-decode the location stream and fail with "pfor: read past end of
// block".
//
// This could not happen before pooling, because reuse was 1:1 with the same slot
// of the same PostingsIterator.
func TestReleaseChunkedIntDecoderResetsPFORMode(t *testing.T) {
	d := &chunkedIntDecoder{}
	d.SetPFORMode(true) // allocates the scratch buffers
	d.pforPos = 7
	d.pforDecoded = append(d.pforDecoded, 1, 2, 3)
	d.chunkOffsets = append(d.chunkOffsets, 10, 20)
	d.startOffset = 99
	d.dataStartOffset = 123
	d.bytesRead = 456
	d.data = []byte{1, 2, 3}
	d.curChunkBytes = []byte{4, 5}

	decodedCap := cap(d.pforDecoded)
	excIdxCap := cap(d.pforExcIdx)
	excValCap := cap(d.pforExcVal)

	releaseChunkedIntDecoder(d)

	// State that would corrupt the next user must be cleared.
	if d.pforMode {
		t.Error("pforMode still true after release — a locReader reusing this " +
			"decoder would PFOR-decode the location stream")
	}
	if d.pforPos != 0 {
		t.Errorf("pforPos = %d after release, want 0", d.pforPos)
	}
	if len(d.pforDecoded) != 0 {
		t.Errorf("pforDecoded len = %d after release, want 0", len(d.pforDecoded))
	}
	if len(d.chunkOffsets) != 0 {
		t.Errorf("chunkOffsets len = %d after release, want 0", len(d.chunkOffsets))
	}
	if d.startOffset != 0 || d.dataStartOffset != 0 || d.bytesRead != 0 {
		t.Errorf("offsets/bytesRead not reset: startOffset=%d dataStartOffset=%d bytesRead=%d",
			d.startOffset, d.dataStartOffset, d.bytesRead)
	}
	// References into segment memory must be dropped so a pooled decoder never
	// pins an mmap'd region.
	if d.data != nil || d.curChunkBytes != nil || d.fr != nil || d.r != nil {
		t.Error("release left a reference to segment memory or a reader")
	}

	// The whole point of pooling is retaining the 6KB of scratch capacity.
	if cap(d.pforDecoded) != decodedCap || cap(d.pforExcIdx) != excIdxCap ||
		cap(d.pforExcVal) != excValCap {
		t.Errorf("release dropped scratch capacity: decoded %d->%d, excIdx %d->%d, excVal %d->%d",
			decodedCap, cap(d.pforDecoded), excIdxCap, cap(d.pforExcIdx),
			excValCap, cap(d.pforExcVal))
	}
	if cap(d.pforDecoded) < pforBlockSize {
		t.Errorf("pforDecoded cap = %d, want >= %d", cap(d.pforDecoded), pforBlockSize)
	}
}

// TestPostingsIteratorReleaseIsIdempotent verifies Release() can be called more
// than once without handing the same decoder to the pool twice, which would let
// two users share it.
func TestPostingsIteratorReleaseIsIdempotent(t *testing.T) {
	d1 := &chunkedIntDecoder{}
	d2 := &chunkedIntDecoder{}
	itr := &PostingsIterator{freqNormReader: d1, locReader: d2}

	itr.Release()
	if itr.freqNormReader != nil || itr.locReader != nil {
		t.Fatal("Release did not detach the decoders")
	}
	itr.Release() // must be a no-op, not a double Put

	var nilItr *PostingsIterator
	nilItr.Release() // must not panic
}
