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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"testing"

	"github.com/blevesearch/go-faiss"
)

func TestNumGPUs(t *testing.T) {
	_, err := faiss.NumGPUs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetDeviceID(t *testing.T) {
	id := GetDeviceID()
	if NumGPUs == 0 {
		if id != -1 {
			t.Fatalf("expected -1 device ID when no GPUs available, got: %d", id)
		}
	} else if id < 0 || id >= NumGPUs {
		t.Fatalf("expected device ID between 0 and %d, got: %d", NumGPUs-1, id)
	}
}
