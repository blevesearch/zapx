//  Copyright (c) 2017 Couchbase, Inc.
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
	"bytes"
	"testing"
)

func TestChunkedContentCoder(t *testing.T) {
	tests := []struct {
		maxDocNum uint64
		chunkSize uint64
		docNums   []uint64
		vals      [][]byte
		expected  []byte
	}{
		{
			maxDocNum: 0,
			chunkSize: 1,
			docNums:   []uint64{0},
			vals:      [][]byte{[]byte("bleve")},
			// 1 chunk, chunk-0 length 11(b), value
			expected: []byte{
				0x5, 0x10, 0x62, 0x6c, 0x65, 0x76, 0x65, // compressed value - "bleve"
				0x7,                                    // chunk offset length - 7 bytes
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // length of offset entries - 1 entry
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // number of chunks - 1 chunk
			},
		},
		{
			maxDocNum: 0,
			chunkSize: 2,
			docNums:   []uint64{0},
			vals:      [][]byte{[]byte("bleve")},
			// 1 chunk, chunk-0 length 11(b), value
			expected: []byte{
				0x1, 0x0, 0x5, 0x5, 0x10, 0x62, 0x6c, 0x65, 0x76, 0x65, // meta + compressed value - "bleve"
				0xa,                                    // chunk offset length - 10 bytes
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // length of offset entries - 1 entry
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // number of chunks - 1 chunk
			},
		},
		{
			maxDocNum: 1,
			chunkSize: 1,
			docNums:   []uint64{0, 1},
			vals: [][]byte{
				[]byte("upside"),
				[]byte("scorch"),
			},

			expected: []byte{
				0x6, 0x14, 0x75, 0x70, 0x73, 0x69, 0x64, 0x65, // compressed value - "upside"
				0x6, 0x14, 0x73, 0x63, 0x6f, 0x72, 0x63, 0x68, // compressed value - "scorch"
				0x8, 0x10, // chunk offset lengths - 8 and 16 bytes
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, // length of offset entries - 2 entries
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, // number of chunks - 2 chunks
			},
		},
		{
			maxDocNum: 1,
			chunkSize: 2,
			docNums:   []uint64{0, 1},
			vals: [][]byte{
				[]byte("upside"),
				[]byte("scorch"),
			},

			expected: []byte{
				0x2,      // meta - 2 documents in chunk
				0x0, 0x6, // meta - docNum 0, offset 6
				0x1, 0xc, // meta - docNum 1, offset 12
				0xc, 0x2c, 0x75, 0x70, 0x73, 0x69, 0x64, 0x65, 0x73, 0x63, 0x6f, 0x72, 0x63, 0x68, // compressed value - "upsidescorch"
				0x13,                                   // chunk offset length - 19 bytes
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // length of offset entries - 1 entry
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, // number of chunks - 1 chunk
			},
		},
	}

	for _, test := range tests {
		var actual bytes.Buffer
		cic := newChunkedContentCoder(test.chunkSize, test.maxDocNum, &actual, false, false)
		for i, docNum := range test.docNums {
			err := cic.Add(docNum, test.vals[i])
			if err != nil {
				t.Fatalf("error adding to intcoder: %v", err)
			}
		}
		_ = cic.Close()
		_, err := cic.Write()
		if err != nil {
			t.Fatalf("error writing: %v", err)
		}

		if !bytes.Equal(test.expected, actual.Bytes()) {
			t.Errorf("got:%s, expected:%s", actual.String(), string(test.expected))
		}
	}
}

func TestChunkedContentCoders(t *testing.T) {
	tests := []struct {
		maxDocNum  uint64
		chunkSize  uint64
		skipSnappy bool
		docNums    []uint64
		vals       [][]byte
	}{
		{
			maxDocNum:  5,
			chunkSize:  1,
			skipSnappy: false,
			docNums:    []uint64{0, 1, 2, 3, 4, 5},
			vals: [][]byte{
				[]byte("scorch"),
				[]byte("does"),
				[]byte("better"),
				[]byte("than"),
				[]byte("upside"),
				[]byte("down"),
			},
		},
		{
			maxDocNum:  5,
			chunkSize:  2,
			skipSnappy: false,
			docNums:    []uint64{0, 1, 2, 3, 4, 5},
			vals: [][]byte{
				[]byte("scorch"),
				[]byte("does"),
				[]byte("better"),
				[]byte("than"),
				[]byte("upside"),
				[]byte("down"),
			},
		},
		{
			maxDocNum:  5,
			chunkSize:  1,
			skipSnappy: true,
			docNums:    []uint64{0, 1, 2, 3, 4, 5},
			vals: [][]byte{
				[]byte("scorch"),
				[]byte("does"),
				[]byte("better"),
				[]byte("than"),
				[]byte("upside"),
				[]byte("down"),
			},
		},
		{
			maxDocNum:  5,
			chunkSize:  2,
			skipSnappy: true,
			docNums:    []uint64{0, 1, 2, 3, 4, 5},
			vals: [][]byte{
				[]byte("scorch"),
				[]byte("does"),
				[]byte("better"),
				[]byte("than"),
				[]byte("upside"),
				[]byte("down"),
			},
		},
	}

	for _, test := range tests {
		var actual1, actual2 bytes.Buffer
		// chunkedContentCoder that writes out at the end
		cic1 := newChunkedContentCoder(test.chunkSize, test.maxDocNum, &actual1, false, false)
		// chunkedContentCoder that writes out in chunks
		cic2 := newChunkedContentCoder(test.chunkSize, test.maxDocNum, &actual2, true, false)

		for i, docNum := range test.docNums {
			err := cic1.Add(docNum, test.vals[i])
			if err != nil {
				t.Fatalf("error adding to intcoder: %v", err)
			}
			err = cic2.Add(docNum, test.vals[i])
			if err != nil {
				t.Fatalf("error adding to intcoder: %v", err)
			}
		}
		_ = cic1.Close()
		_ = cic2.Close()

		_, err := cic1.Write()
		if err != nil {
			t.Fatalf("error writing: %v", err)
		}
		_, err = cic2.Write()
		if err != nil {
			t.Fatalf("error writing: %v", err)
		}

		if !bytes.Equal(actual1.Bytes(), actual2.Bytes()) {
			t.Errorf("%s != %s", actual1.String(), actual2.String())
		}
	}
}
