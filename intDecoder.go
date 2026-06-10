//  Copyright (c) 2019 Couchbase, Inc.
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
	"fmt"
)

type chunkedIntDecoder struct {
	startOffset     uint64
	dataStartOffset uint64
	chunkOffsets    []uint64
	curChunkBytes   []byte
	data            []byte
	r               *memUvarintReader
	fr              *FileReader

	bytesRead uint64

	// pforMode enables PFOR block decoding (§4).
	// When true, loadChunk() decodes a PFOR block into pforDecoded and
	// readUvarint() returns values from the decoded array sequentially.
	pforMode    bool
	pforDecoded []uint64
	pforPos     int
}

// newChunkedIntDecoder expects an optional or reset chunkedIntDecoder for better reuse.
func newChunkedIntDecoder(buf []byte, offset uint64, rv *chunkedIntDecoder, fr *FileReader) *chunkedIntDecoder {
	if rv == nil {
		rv = &chunkedIntDecoder{startOffset: offset, data: buf}
	} else {
		rv.startOffset = offset
		rv.data = buf
	}

	var n, numChunks uint64
	var read int
	if offset == termNotEncoded {
		numChunks = 0
	} else {
		numChunks, read = binary.Uvarint(buf[offset+n : offset+n+binary.MaxVarintLen64])
	}

	n += uint64(read)
	if cap(rv.chunkOffsets) >= int(numChunks) {
		rv.chunkOffsets = rv.chunkOffsets[:int(numChunks)]
	} else {
		rv.chunkOffsets = make([]uint64, int(numChunks))
	}
	for i := 0; i < int(numChunks); i++ {
		rv.chunkOffsets[i], read = binary.Uvarint(buf[offset+n : offset+n+binary.MaxVarintLen64])
		n += uint64(read)
	}
	rv.bytesRead += n
	rv.dataStartOffset = offset + n
	rv.fr = fr
	return rv
}

// SetPFORMode enables PFOR block decoding for this decoder.
// Must be called right after newChunkedIntDecoder, before loadChunk.
func (d *chunkedIntDecoder) SetPFORMode(enabled bool) {
	d.pforMode = enabled
}

// A util function which fetches the query time
// specific bytes encoded by intcoder (for eg the
// freqNorm and location details of a term in document)
// the loadChunk retrieves the next chunk and the
// number of bytes retrieve in that operation is accounted
func (d *chunkedIntDecoder) getBytesRead() uint64 {
	return d.bytesRead
}

func (d *chunkedIntDecoder) loadChunk(chunk int) error {
	if d.startOffset == termNotEncoded {
		if d.pforMode {
			d.pforDecoded = d.pforDecoded[:0]
			d.pforPos = 0
		} else {
			d.r = newMemUvarintReader([]byte(nil))
		}
		return nil
	}

	if chunk >= len(d.chunkOffsets) {
		return fmt.Errorf("tried to load freq chunk that doesn't exist %d/(%d)",
			chunk, len(d.chunkOffsets))
	}

	end, start := d.dataStartOffset, d.dataStartOffset
	s, e := readChunkBoundary(chunk, d.chunkOffsets)
	start += s
	end += e

	var err error
	d.curChunkBytes, err = d.fr.process(d.data[start:end])
	if err != nil {
		return fmt.Errorf("error processing chunk %d: %w", chunk, err)
	}
	d.bytesRead += end - start

	if d.pforMode {
		decoded, _ := decodePFORBlock(d.curChunkBytes)
		d.pforDecoded = decoded
		d.pforPos = 0
		// Reset the varint reader to empty so isNil() etc. work consistently
		// even though PFOR doesn't use it.
		if d.r == nil {
			d.r = newMemUvarintReader([]byte(nil))
		} else {
			d.r.Reset([]byte(nil))
		}
	} else {
		if d.r == nil {
			d.r = newMemUvarintReader(d.curChunkBytes)
		} else {
			d.r.Reset(d.curChunkBytes)
		}
	}

	return nil
}

func (d *chunkedIntDecoder) reset() {
	d.startOffset = 0
	d.dataStartOffset = 0
	d.chunkOffsets = d.chunkOffsets[:0]
	d.curChunkBytes = d.curChunkBytes[:0]
	d.bytesRead = 0
	d.data = d.data[:0]
	if d.r != nil {
		d.r.Reset([]byte(nil))
	}
	d.pforDecoded = d.pforDecoded[:0]
	d.pforPos = 0
	// pforMode is intentionally NOT cleared here — the caller (posting.go) always
	// calls SetPFORMode immediately after newChunkedIntDecoder to set the correct mode.
}

func (d *chunkedIntDecoder) isNil() bool {
	if d.pforMode {
		// True when no values have been loaded OR all have been consumed.
		// Covers both initial state and the reuse-across-terms case where
		// pforDecoded still holds stale data from the previous term.
		return d.pforPos >= len(d.pforDecoded)
	}
	return d.curChunkBytes == nil || len(d.curChunkBytes) == 0
}

func (d *chunkedIntDecoder) readUvarint() (uint64, error) {
	if d.pforMode {
		if d.pforPos >= len(d.pforDecoded) {
			return 0, fmt.Errorf("pfor: read past end of block (pos=%d len=%d)", d.pforPos, len(d.pforDecoded))
		}
		v := d.pforDecoded[d.pforPos]
		d.pforPos++
		return v, nil
	}
	return d.r.ReadUvarint()
}

func (d *chunkedIntDecoder) readBytes(start, end int) []byte {
	return d.curChunkBytes[start:end]
}

func (d *chunkedIntDecoder) SkipUvarint() {
	if d.pforMode {
		if d.pforPos < len(d.pforDecoded) {
			d.pforPos++
		}
		return
	}
	d.r.SkipUvarint()
}

func (d *chunkedIntDecoder) SkipBytes(count int) {
	d.r.SkipBytes(count)
}

func (d *chunkedIntDecoder) Len() int {
	if d.pforMode {
		return len(d.pforDecoded) - d.pforPos
	}
	return d.r.Len()
}

func (d *chunkedIntDecoder) remainingLen() int {
	if d.pforMode {
		// Return current position as a "bytes consumed" marker.
		// nextBytes() in PFOR mode reads this before/after and uses the
		// difference only to identify the byte range — but for PFOR it
		// reconstructs the varint bytes directly from the decoded value,
		// so this value is never used to index into curChunkBytes.
		return d.pforPos
	}
	return len(d.curChunkBytes) - d.r.Len()
}
