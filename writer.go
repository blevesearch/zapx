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

type fileWriter struct {
	writerCB func(data []byte) ([]byte, error)
	id       string
	c        *CountHashWriter
}

var WriterCallbackGetter = func() (string, func(data []byte) ([]byte, error)) {
	return "", func(data []byte) ([]byte, error) {
		return data, nil
	}
}

var ReaderCallbackGetter = func(string) func(data []byte) ([]byte, error) {
	return func(data []byte) ([]byte, error) {
		return data, nil
	}
}

func NewFileWriter(c *CountHashWriter) *fileWriter {
	rv := &fileWriter{c: c}
	rv.id, rv.writerCB = WriterCallbackGetter()

	return rv
}

func (w *fileWriter) Write(data []byte) (int, error) {
	return w.c.Write(data)
}

func (w *fileWriter) process(data []byte) ([]byte, error) {
	if w.writerCB != nil {
		return w.writerCB(data)
	}
	return data, nil
}

func (w *fileWriter) Count() int {
	return w.c.Count()
}

func (w *fileWriter) Sum32() uint32 {
	return w.c.Sum32()
}

type fileReader struct {
	callback func(data []byte) ([]byte, error)
	id       string
}

func NewFileReader(id string) *fileReader {
	rv := &fileReader{id: id}
	rv.callback = ReaderCallbackGetter(id)

	return rv
}

func (r *fileReader) process(data []byte) ([]byte, error) {
	if r.callback != nil {
		return r.callback(data)
	}
	return data, nil
}
