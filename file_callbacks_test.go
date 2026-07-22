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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

package zap

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
)

func initFileCallbacks(t *testing.T) {
	key := make([]byte, 32)
	keyId := "test-key-id"

	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate random key: %s", err.Error())
	}

	WriterHook := func(context []byte) (string, func(data []byte) []byte, error) {

		if context == nil {
			return "", func(data []byte) []byte {
				return data
			}, nil
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			return "", nil, fmt.Errorf("Failed to create AES cipher: %s", err.Error())
		}

		aesgcm, err := cipher.NewGCM(block)
		if err != nil {
			return "", nil, fmt.Errorf("Failed to create AES GCM: %s", err.Error())
		}

		nonce := make([]byte, 12)
		if _, err := rand.Read(nonce); err != nil {
			return "", nil, fmt.Errorf("Failed to generate random nonce: %s", err.Error())
		}

		writerCallback := func(data []byte) []byte {
			ciphertext := aesgcm.Seal(nil, nonce, data, nil)
			result := append(ciphertext, nonce...)

			for i := len(nonce) - 1; i >= 0; i-- {
				if nonce[i] < 255 {
					nonce[i]++
					break
				}
				nonce[i] = 0
			}
			return result
		}

		return keyId, writerCallback, nil
	}

	ReaderHook := func(id string, context []byte) (func(data []byte) ([]byte, error), error) {

		if id == "" {
			return func(data []byte) ([]byte, error) {
				return data, nil
			}, nil
		}

		if id != keyId {
			return nil, fmt.Errorf("unknown callback ID: %s", id)
		}

		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, fmt.Errorf("Failed to create AES cipher: %s", err.Error())
		}

		aesgcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("Failed to create AES GCM: %s", err.Error())
		}

		readerCallback := func(data []byte) ([]byte, error) {

			if len(data) < 12 {
				return nil, fmt.Errorf("ciphertext too short")
			}

			nonce := data[len(data)-12:]
			ciphertext := data[:len(data)-12]
			plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt data: %w", err)
			}

			return plaintext, nil
		}

		return readerCallback, nil
	}

	prevWriterHook := index.WriterHook
	prevReaderHook := index.ReaderHook

	index.WriterHook = WriterHook
	index.ReaderHook = ReaderHook

	t.Cleanup(func() {
		index.WriterHook = prevWriterHook
		index.ReaderHook = prevReaderHook
	})
}

// TestWriteReadUint64Array round-trips uint64 arrays through
// WriteUint64Array and ReadUint64Array.
func TestWriteReadUint64Array(t *testing.T) {
	arrays := [][]uint64{
		nil,
		{},
		{0},
		{42},
		{math.MaxUint64},
		{7, 3, 9, 0, math.MaxUint64, 1 << 33},
	}
	longArr := make([]uint64, 1000)
	for i := range longArr {
		longArr[i] = uint64(i) * 3
	}
	arrays = append(arrays, longArr)

	callbackContext := []byte("test-uint64-array")

	// vary the number of bytes written before the arrays so every padding
	// length gets exercised
	for prefix := 0; prefix < 8; prefix++ {
		var buf bytes.Buffer
		fw, err := NewFileWriter(NewCountHashWriter(&buf), callbackContext)
		if err != nil {
			t.Fatal(err)
		}

		if prefix > 0 {
			if _, err = fw.Write(make([]byte, prefix)); err != nil {
				t.Fatal(err)
			}
		}

		writtenLens := make([]int, len(arrays))
		for i, arr := range arrays {
			writtenLens[i], err = fw.WriteUint64Array(arr)
			if err != nil {
				t.Fatal(err)
			}
		}

		fr, err := NewFileReader(fw.id, callbackContext)
		if err != nil {
			t.Fatal(err)
		}

		data := buf.Bytes()
		pos := uint64(prefix)
		for i, arr := range arrays {
			// the payload must begin on an 8-byte boundary in the file
			bufLen, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			pad := data[pos+uint64(n)]
			payloadStart := pos + uint64(n) + 1 + uint64(pad)
			if bufLen > 0 && payloadStart%8 != 0 {
				t.Fatalf("prefix %d array %d: payload starts at unaligned offset %d",
					prefix, i, payloadStart)
			}

			vals, mem, shift, err := fr.ReadUint64Array(data[pos:])
			if err != nil {
				t.Fatal(err)
			}
			if shift != uint64(writtenLens[i]) {
				t.Fatalf("prefix %d array %d: wrote %d bytes but read consumed %d",
					prefix, i, writtenLens[i], shift)
			}
			if len(vals) != len(arr) {
				t.Fatalf("prefix %d array %d: expected %d values, got %d",
					prefix, i, len(arr), len(vals))
			}
			for j := range arr {
				if vals[j] != arr[j] {
					t.Fatalf("prefix %d array %d: expected %d at index %d, got %d",
						prefix, i, arr[j], j, vals[j])
				}
			}

			if len(arr) > 0 {
				if fw.processor == nil && isLittleEndian && mem == nil {
					t.Fatalf("prefix %d array %d: expected zero-copy read, got a decoded copy",
						prefix, i)
				}
				if fw.processor != nil && mem != nil {
					t.Fatalf("prefix %d array %d: expected a decoded copy with a writer callback, got a zero-copy view",
						prefix, i)
				}
			}

			pos += shift
		}

		if pos != uint64(len(data)) {
			t.Fatalf("prefix %d: %d bytes left unconsumed", prefix, uint64(len(data))-pos)
		}
	}
}

func TestWriteReadUint32Array(t *testing.T) {
	arrays := [][]uint32{
		nil,
		{},
		{0},
		{42},
		{math.MaxUint32},
		{7, 3, 9, 0, math.MaxUint32, 1 << 20},
	}
	longArr := make([]uint32, 1000)
	for i := range longArr {
		longArr[i] = uint32(i) * 3
	}
	arrays = append(arrays, longArr)

	callbackContext := []byte("test-uint32-array")

	// vary the number of bytes written before the arrays so every padding
	// length gets exercised (uint32 arrays align to a 4-byte boundary)
	for prefix := 0; prefix < 4; prefix++ {
		var buf bytes.Buffer
		fw, err := NewFileWriter(NewCountHashWriter(&buf), callbackContext)
		if err != nil {
			t.Fatal(err)
		}

		if prefix > 0 {
			if _, err = fw.Write(make([]byte, prefix)); err != nil {
				t.Fatal(err)
			}
		}

		writtenLens := make([]int, len(arrays))
		for i, arr := range arrays {
			writtenLens[i], err = fw.WriteUint32Array(arr)
			if err != nil {
				t.Fatal(err)
			}
		}

		fr, err := NewFileReader(fw.id, callbackContext)
		if err != nil {
			t.Fatal(err)
		}

		data := buf.Bytes()
		pos := uint64(prefix)
		for i, arr := range arrays {
			// the payload must begin on a 4-byte boundary in the file
			bufLen, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			pad := data[pos+uint64(n)]
			payloadStart := pos + uint64(n) + 1 + uint64(pad)
			if bufLen > 0 && payloadStart%4 != 0 {
				t.Fatalf("prefix %d array %d: payload starts at unaligned offset %d",
					prefix, i, payloadStart)
			}

			vals, mem, shift, err := fr.ReadUint32Array(data[pos:])
			if err != nil {
				t.Fatal(err)
			}
			if shift != uint64(writtenLens[i]) {
				t.Fatalf("prefix %d array %d: wrote %d bytes but read consumed %d",
					prefix, i, writtenLens[i], shift)
			}
			if len(vals) != len(arr) {
				t.Fatalf("prefix %d array %d: expected %d values, got %d",
					prefix, i, len(arr), len(vals))
			}
			for j := range arr {
				if vals[j] != arr[j] {
					t.Fatalf("prefix %d array %d: expected %d at index %d, got %d",
						prefix, i, arr[j], j, vals[j])
				}
			}

			if len(arr) > 0 {
				if fw.processor == nil && isLittleEndian && mem == nil {
					t.Fatalf("prefix %d array %d: expected zero-copy read, got a decoded copy",
						prefix, i)
				}
				if fw.processor != nil && mem != nil {
					t.Fatalf("prefix %d array %d: expected a decoded copy with a writer callback, got a zero-copy view",
						prefix, i)
				}
			}

			pos += shift
		}

		if pos != uint64(len(data)) {
			t.Fatalf("prefix %d: %d bytes left unconsumed", prefix, uint64(len(data))-pos)
		}
	}
}

// Initializes encryption related file callbacks and
// runs all file I/O related tests
func TestFileCallbacks(t *testing.T) {
	initFileCallbacks(t)

	TestOpen(t)
	TestOpenMulti(t)
	TestOpenMultiWithTwoChunks(t)
	TestSegmentVisitableDocValueFieldsList(t)
	TestSegmentDocsWithNonOverlappingFields(t)
	TestMergedSegmentDocsWithNonOverlappingFields(t)

	TestChunkedContentCoder(t)
	TestChunkedContentCoders(t)

	TestDictionary(t)
	TestDictionaryError(t)
	TestDictionaryBug1156(t)

	TestEnumerator(t)

	TestChunkIntCoder(t)
	TestChunkLengthToOffsets(t)
	TestChunkReadBoundaryFromOffsets(t)

	TestMerge(t)
	TestMergeWithEmptySegment(t)
	TestMergeWithEmptySegments(t)
	TestMergeWithEmptySegmentFirst(t)
	TestMergeWithEmptySegmentsFirst(t)
	TestMergeAndDrop(t)
	TestMergeAndDropAllFromOneSegment(t)
	TestMergeWithUpdates(t)
	TestMergeWithUpdatesOnManySegments(t)
	TestMergeWithUpdatesOnOneDoc(t)
	TestMergeBytesWritten(t)
	TestUnder32Bits(t)

	TestSynonymSegment(t)

	TestRoaringSizes(t)

	TestWriteReadUint64Array(t)
	TestWriteReadUint32Array(t)

	TestGeoIndexSectionRoundTrip(t)
	TestGeoIndexMerge(t)
}
