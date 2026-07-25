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
	"encoding/binary"
	"testing"
)

// dirOffset is where the encoded directory starts inside the test buffer. It must
// be non-zero: termNotEncoded == 0, so a decoder built at offset 0 treats the
// term as having no encoded postings and skips the directory entirely.
const dirOffset = 1

// buildChunkDir encodes a chunk-offset directory the way intcoder.Write does: a
// uvarint count followed by one uvarint per chunk holding the cumulative END
// offset of that chunk's bytes. Returns the buffer and where chunk data begins.
func buildChunkDir(t *testing.T, ends []uint64) ([]byte, uint64) {
	t.Helper()

	buf := make([]byte, dirOffset, binary.MaxVarintLen64*(len(ends)+1)+dirOffset)
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(ends)))
	buf = append(buf, tmp[:n]...)
	for _, e := range ends {
		n = binary.PutUvarint(tmp, e)
		buf = append(buf, tmp[:n]...)
	}
	return buf, uint64(len(buf))
}

// TestChunkDirEmptyAndNotEncoded covers the two degenerate constructions.
func TestChunkDirEmptyAndNotEncoded(t *testing.T) {
	d := newChunkedIntDecoder([]byte{0x00}, termNotEncoded, nil, nil)
	if len(d.chunkOffsets) != 0 {
		t.Errorf("termNotEncoded: %d offsets, want 0", len(d.chunkOffsets))
	}
	if d.dataStartOffset != termNotEncoded {
		t.Errorf("termNotEncoded: dataStartOffset = %d, want %d",
			d.dataStartOffset, uint64(termNotEncoded))
	}

	buf, _ := buildChunkDir(t, nil)
	d = newChunkedIntDecoder(buf, dirOffset, nil, nil)
	if err := d.loadChunk(0); err == nil {
		t.Error("loadChunk(0) on a zero-chunk directory should fail")
	}
}

// TestChunkDirReuseTruncatesStaleOffsets guards the pooling interaction: a
// decoder reused for a term with fewer chunks must not expose the previous
// term's trailing offsets, which would produce a valid-looking but wrong chunk
// range instead of an error.
func TestChunkDirReuseTruncatesStaleOffsets(t *testing.T) {
	longBuf, _ := buildChunkDir(t, []uint64{10, 20, 30, 40, 50, 60})
	shortBuf, _ := buildChunkDir(t, []uint64{5, 9})

	d := newChunkedIntDecoder(longBuf, dirOffset, nil, nil)
	if len(d.chunkOffsets) != 6 {
		t.Fatalf("setup: %d offsets, want 6", len(d.chunkOffsets))
	}

	d = newChunkedIntDecoder(shortBuf, dirOffset, d, nil)
	if len(d.chunkOffsets) != 2 {
		t.Fatalf("after reuse: %d offsets, want 2 — stale entries are still visible",
			len(d.chunkOffsets))
	}
	if d.chunkOffsets[0] != 5 || d.chunkOffsets[1] != 9 {
		t.Errorf("after reuse: chunkOffsets = %v, want [5 9]", d.chunkOffsets)
	}

	// Same via the pool.
	releaseChunkedIntDecoder(d)
	d2 := newChunkedIntDecoder(shortBuf, dirOffset, nil, nil)
	if len(d2.chunkOffsets) != 2 || d2.chunkOffsets[0] != 5 || d2.chunkOffsets[1] != 9 {
		t.Errorf("after release/reacquire: chunkOffsets = %v, want [5 9]", d2.chunkOffsets)
	}
}
