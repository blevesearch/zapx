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
	"encoding/binary"
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
)

// This file provides a mechanism for users of zap to provide callbacks
// that can process data before it is written to disk, and after it is read
// from disk.  This can be used for things like encryption, compression, etc.

// The user is responsible for ensuring that the writer and reader callbacks
// are compatible with each other, and that any state needed by the callbacks
// is managed appropriately.  For example, if the writer callback uses a
// unique key or nonce per write, the reader callback must be able to
// determine the correct key or nonce to use for each read.

// The callbacks are identified by an id string, which is returned by the
// WriterCallbackGetter. The same id string is passed to the ReaderCallbackGetter
// when creating a reader.  This allows the reader to determine which
// callback to use for a given file.

// An example implementation using AES-GCM encryption is provided in
// file_callbacks_test.go within initFileCallbacks().

// the default id to use for file callbacks that indicates a no op
const DefaultFileCallbackId = ""

// FileWriter wraps a CountHashWriter and applies a user provided
// writer callback to the data being written.
type FileWriter struct {
	id        string
	c         *CountHashWriter
	tmp       []byte
	processor func(data []byte) []byte
}

// creates an empty FileWriter with no callback. Used
// when we are writing data that is not going to be persisted
func NewFileWriterEmpty(c *CountHashWriter) *FileWriter {
	rv := &FileWriter{
		c: c,
	}

	return rv
}

// NewFileWriter creates a FileWriter with the provided CountHashWriter and applies
// the writer callback identified by the context.
func NewFileWriter(c *CountHashWriter, context []byte) (*FileWriter, error) {
	rv := &FileWriter{
		c: c,
	}

	if index.WriterHook != nil {
		var err error
		rv.id, rv.processor, err = index.WriterHook(context)
		if err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func (w *FileWriter) Write(data []byte) (int, error) {
	return w.c.Write(data)
}

// process applies the writer callback to the data, if one is set
func (w *FileWriter) process(data []byte) []byte {
	if w.processor != nil {
		return w.processor(data)
	}
	return data
}

func (w *FileWriter) Count() int {
	return w.c.Count()
}

func (w *FileWriter) Sum32() uint32 {
	return w.c.Sum32()
}

func (w *FileWriter) grabBuf(size int) []byte {
	if cap(w.tmp) < size {
		w.tmp = make([]byte, size)
	}
	return w.tmp[:size]
}

func (w *FileWriter) WriteArray(arr []byte) (int, error) {
	arr = w.process(arr)
	numBuf := w.grabBuf(binary.MaxVarintLen64)

	n := binary.PutUvarint(numBuf, uint64(len(arr)))
	_, err := w.Write(numBuf[:n])
	if err != nil {
		return 0, err
	}

	return w.Write(arr)
}

func (w *FileWriter) WriteArrayWithOffsets(arr [][]byte) (int, error) {
	offsets := make([]byte, len(arr)*8)
	buf := make([]byte, 0)
	numBuf := w.grabBuf(binary.MaxVarintLen64)

	for i, a := range arr {
		a = w.process(a)
		buf = append(buf, a...)
		binary.BigEndian.PutUint64(offsets[i*8:(i+1)*8], uint64(len(buf)))
	}

	offsets = w.process(offsets)
	n := binary.PutUvarint(numBuf, uint64(len(offsets)))
	_, err := w.Write(numBuf[:n])
	if err != nil {
		return 0, err
	}

	_, err = w.Write(offsets)
	if err != nil {
		return 0, err
	}

	n = binary.PutUvarint(numBuf, uint64(len(buf)))
	_, err = w.Write(numBuf[:n])
	if err != nil {
		return 0, err
	}

	return w.Write(buf)
}

// FileReader wraps a reader callback to be applied to data read from a file.
type FileReader struct {
	id        string
	processor func(data []byte) ([]byte, error)
}

// NewFileReader creates a FileReader with the reader callback identified by the context.
// The id is used to identify which callback to use when reading data.
func NewFileReader(id string, context []byte) (*FileReader, error) {
	rv := &FileReader{
		id: id,
	}

	if index.ReaderHook != nil {
		var err error
		rv.processor, err = index.ReaderHook(id, context)
		if err != nil {
			return nil, err
		}
	} else if id != DefaultFileCallbackId {
		return nil, fmt.Errorf("reader callback id %s provided but no ReaderHook is set", id)
	}

	return rv, nil
}

// process applies the reader callback to the data, if one is set
func (r *FileReader) process(data []byte) ([]byte, error) {
	if r.processor != nil {
		return r.processor(data)
	}
	return data, nil
}

func (r *FileReader) ReadArray(data []byte) ([]byte, uint64, error) {
	var pos uint64

	bufLen, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if bufLen < 0 {
		return nil, 0, fmt.Errorf("read array length is less than 0")
	} else if bufLen == 0 {
		return nil, pos, nil
	}

	buf, err := r.process(data[pos : pos+bufLen])
	if err != nil {
		return nil, 0, err
	}
	pos += bufLen

	return buf, pos, nil
}

func (r *FileReader) ReadArrayWithOffsets(data []byte) ([][]byte, uint64, error) {
	var pos uint64

	buf, shift, err := r.ReadArray(data[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += shift

	offsets := make([]uint64, len(buf)/8)
	for i := 0; i < len(offsets); i++ {
		offsets[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	dataLen, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if dataLen == 0 {
		return nil, 0, fmt.Errorf("read array length is 0")
	}
	rawData := data[pos : pos+dataLen]
	pos += dataLen

	arr := make([][]byte, len(offsets))
	for i := range offsets {
		var start uint64
		if i > 0 {
			start = offsets[i-1]
		}
		end := offsets[i]
		arr[i], err = r.process(rawData[start:end])
		if err != nil {
			return nil, 0, err
		}
	}

	return arr, pos, nil
}
