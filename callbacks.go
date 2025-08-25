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

package zap

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

// Additionaly, if the writer callback needs a unique counter or nonce
// per write, the CounterGetter can be used to provide that.  The counter
// is passed to the writer callback along with the data to be written.
// The counter is not passed to the reader callback, as it is assumed that
// the reader callback can determine the correct counter to use based
// on the data being read.

// An example implementation using AES-GCM is provided in callbacks_test.go
// within initFileCallbacks.

// Default no-op implementation. Is called before writing any user data to a file.
var WriterCallbackGetter = func() (string, func(data []byte, _ []byte) ([]byte, error), error) {
	return "", func(data []byte, _ []byte) ([]byte, error) {
		return data, nil
	}, nil
}

// Default no-op implementation. Is called after reading any user data from a file.
var ReaderCallbackGetter = func(string) (func(data []byte) ([]byte, error), error) {
	return func(data []byte) ([]byte, error) {
		return data, nil
	}, nil
}

// Default no-op implementation. Is called once per write call if a unique counter is
// needed by the writer callback.
var CounterGetter = func() ([]byte, error) {
	return nil, nil
}

// fileWriter wraps a CountHashWriter and applies a user provided
// writer callback to the data being written.
type fileWriter struct {
	writerCB func(data []byte, counter []byte) ([]byte, error)
	counter  []byte
	id       string
	c        *CountHashWriter
}

func NewFileWriter(c *CountHashWriter) (*fileWriter, error) {
	var err error
	rv := &fileWriter{c: c}
	rv.id, rv.writerCB, err = WriterCallbackGetter()
	if err != nil {
		return nil, err
	}
	rv.counter, err = CounterGetter()
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (w *fileWriter) Write(data []byte) (int, error) {
	return w.c.Write(data)
}

// process applies the writer callback to the data, if one is set
// and increments the counter if one is set.
func (w *fileWriter) process(data []byte) ([]byte, error) {
	if w.writerCB != nil {
		w.incrementCounter()
		return w.writerCB(data, w.counter)
	}
	return data, nil
}

func (w *fileWriter) incrementCounter() {
	if w.counter != nil {
		for i := len(w.counter) - 1; i >= 0; i-- {
			if w.counter[i] < 255 {
				w.counter[i]++
				return
			}
			w.counter[i] = 0
		}
	}
}

func (w *fileWriter) Count() int {
	return w.c.Count()
}

func (w *fileWriter) Sum32() uint32 {
	return w.c.Sum32()
}

// fileReader wraps a reader callback to be applied to data read from a file.
type fileReader struct {
	callback func(data []byte) ([]byte, error)
	id       string
}

func NewFileReader(id string) (*fileReader, error) {
	var err error
	rv := &fileReader{id: id}
	rv.callback, err = ReaderCallbackGetter(id)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

// process applies the reader callback to the data, if one is set
func (r *fileReader) process(data []byte) ([]byte, error) {
	if r.callback != nil {
		return r.callback(data)
	}
	return data, nil
}
