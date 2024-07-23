//  Copyright (c) 2024 Couchbase, Inc.
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

//go:build vectors && windows
// +build vectors,windows

package zap

import (
	"runtime"
	"sync"

	faiss "github.com/blevesearch/go-faiss"
)

const faissIOFlags = faiss.IOFlagReadOnly

// This is a workaround for the issue that the faiss library does not support
// concurrent training on Windows. The training is serialized by using a mutex.
// Details:
//  1. Faiss uses openBLAS for matrix operations, and openBLAS can be built in either
//     single-threaded mode or multi-threaded mode.
//  2. Single threaded openBLAS is not performant, and can lead to an exponential increase
//     in training time, especially during segment merge.
//  3. openBLAS must hence be built in multi-threaded mode to be performant, but this
//     makes openBLAS to be not thread-safe on windows, where one of its backing pthreads
//     can get leaked.
var trainMutex sync.Mutex

func trainFaissIndex(index *faiss.IndexImpl, indexData []float32) error {
	runtime.LockOSThread()
	trainMutex.Lock()
	defer func() {
		trainMutex.Unlock()
		runtime.UnlockOSThread()
	}
	return index.Train(indexData)
}
