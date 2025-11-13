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

	crypto "github.com/couchbase/gocbcrypto"
)

func initFileCallbacks(t *testing.T) {
	encryptionKey := make([]byte, 32)
	if _, err := rand.Read(encryptionKey); err != nil {
		panic("failed to generate AES key: " + err.Error())
	}

	key := make([]byte, 32)
	keyId := "test-key-id"
	label := []byte("test-label")

	if _, err := rand.Read(key); err != nil {
		panic("Failed to generate random key: " + err.Error())
	}

	WriterHook = func(context []byte) (string, func(data []byte) []byte, error) {

		derivedKey := make([]byte, 32)
		derivedKey, err := crypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA2-256", "")
		if err != nil {
			return "", nil, err
		}

		block, err := aes.NewCipher(derivedKey)
		if err != nil {
			panic("Failed to create AES cipher: " + err.Error())
		}

		aesgcm, err := cipher.NewGCM(block)
		if err != nil {
			panic("Failed to create AES GCM: " + err.Error())
		}

		nonce := make([]byte, 12)
		if _, err := rand.Read(nonce); err != nil {
			panic("Failed to generate random nonce: " + err.Error())
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

	ReaderHook = func(id string, context []byte) (func(data []byte) ([]byte, error), error) {
		if id != keyId {
			return nil, fmt.Errorf("unknown callback ID: %s", id)
		}

		derivedKey := make([]byte, 32)
		derivedKey, err := crypto.OpenSSLKBKDFDeriveKey(key, label, context, derivedKey, "SHA2-256", "")
		if err != nil {
			return nil, err
		}

		block, err := aes.NewCipher(derivedKey)
		if err != nil {
			panic("Failed to create AES cipher: " + err.Error())
		}

		aesgcm, err := cipher.NewGCM(block)
		if err != nil {
			panic("Failed to create AES GCM: " + err.Error())
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
