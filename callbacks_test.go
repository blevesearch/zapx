//  Copyright (c) 2025 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package zap

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"testing"
)

func initFileCallbacks(t *testing.T) {
	key := make([]byte, 32)
	keyId := "test-key-id"

	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate random key: %v", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("Failed to create AES cipher: %v", err)
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("Failed to create AES GCM: %v", err)
	}

	CounterGetter = func() ([]byte, error) {
		counter := make([]byte, 12)
		if _, err := rand.Read(counter); err != nil {
			return nil, err
		}
		return counter, nil
	}

	writerCallback := func(data, counter []byte) ([]byte, error) {
		ciphertext := aesgcm.Seal(nil, counter, data, nil)
		result := append(ciphertext, counter...)
		return result, nil
	}

	readerCallback := func(data []byte) ([]byte, error) {
		if len(data) < 12 {
			return nil, fmt.Errorf("ciphertext too short")
		}

		counter := data[len(data)-12:]
		ciphertext := data[:len(data)-12]
		plaintext, err := aesgcm.Open(nil, counter, ciphertext, nil)
		if err != nil {
			return nil, err
		}
		return plaintext, nil
	}

	WriterCallbackGetter = func() (string, func(data []byte, counter []byte) ([]byte, error), error) {
		return keyId, writerCallback, nil
	}

	ReaderCallbackGetter = func(id string) (func(data []byte) ([]byte, error), error) {
		if id != keyId {
			return nil, fmt.Errorf("unknown callback ID: %s", id)
		}
		return readerCallback, nil
	}
}

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

	TestVecPostingsIterator(t)
	TestVectorSegment(t)
	TestHashCode(t)
	TestPersistedVectorSegment(t)
	TestValidVectorMerge(t)

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
}
