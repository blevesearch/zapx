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

//go:build vectors
// +build vectors

package zap

import (
	"encoding/binary"
	"fmt"
)

// writeVectorMetadata writes vector metadata and mappings to the writer.
func writeVectorMetadata(w *CountHashWriter, fieldNotUninverted uint64,
	optimizationType uint64, numVecs int) error {
	tempBuf := make([]byte, binary.MaxVarintLen64)

	// Write fieldNotUninverted (twice for compatibility)
	n := binary.PutUvarint(tempBuf, fieldNotUninverted)
	_, err := w.Write(tempBuf[:n])
	if err != nil {
		return fmt.Errorf("failed to write fieldNotUninverted: %w", err)
	}
	n = binary.PutUvarint(tempBuf, fieldNotUninverted)
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return fmt.Errorf("failed to write fieldNotUninverted: %w", err)
	}

	// Write optimization type
	n = binary.PutUvarint(tempBuf, optimizationType)
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return fmt.Errorf("failed to write optimization type: %w", err)
	}

	// Write number of unique vectors
	n = binary.PutUvarint(tempBuf, uint64(numVecs))
	_, err = w.Write(tempBuf[:n])
	if err != nil {
		return fmt.Errorf("failed to write number of unique vectors: %w", err)
	}

	return nil
}
