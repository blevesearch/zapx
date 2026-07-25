//  Copyright (c) 2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//              http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"errors"
	"fmt"
)

// LegacyChunkMode was the original chunk mode (always chunk size 1024)
// this mode is still used for chunking doc values.
var LegacyChunkMode uint32 = 1024

// DefaultChunkMode is the most recent improvement to chunking and should
// be used by default.  v18: PFOR block encoding (mode 1028).
var DefaultChunkMode uint32 = 1028

// PFORChunkMode signals PFOR block encoding for the freq stream (v18+), with a
// cardinality-adaptive chunk size (§75): a term matching no more than
// pforBlockSize documents gets ONE chunk spanning the segment, rather than one
// chunk per 256-docNum window.
//
// The window-per-256-docNums rule this replaces sized chunks by the SEGMENT, not
// by the term, so the chunk-offset directory held ceil(maxDocs/256) entries
// however few documents the term matched. Measured on the §26 entity corpus
// (6 keyword-ID fields, 500k docs, 15 segments, ~1-4 matches per field/segment):
//
//	per query: 31 posting iterators, 272 directory entries each = 8,440 entries,
//	           indexing 9 BYTES of freq data per list
//
// The directory was ~30x the payload it described, and every entry was decoded
// before the first posting could be read. Dense terms were never the problem:
// TermTier1 (DF 100%) has 131 entries indexing 33,327 bytes, 0.4% overhead.
//
// Note the mode number moved 1027 -> 1028 and 1027 was REMOVED rather than
// redefined. chunkMode is persisted per segment and both the reader
// (Dictionary.postingsList) and the writers re-derive chunkSize from it plus the
// cardinality, so redefining 1027 in place would make an existing prototype-v18
// segment silently misread — a low-cardinality term taken as one chunk out of a
// directory that actually holds hundreds, returning whatever bytes live there.
// Dropping 1027 instead means such a segment fails loudly on its first postings
// read with "unknown chunk mode". v18 is unreleased, so there is no compatibility
// obligation, only an obligation not to corrupt quietly.
var PFORChunkMode uint32 = 1028

var ErrChunkSizeZero = errors.New("chunk size is zero")

// getChunkSize returns the chunk size for the given chunkMode, cardinality, and
// maxDocs.
//
// In error cases, the returned chunk size will be 0. Caller can differentiate
// between a valid chunk size of 0 and an error by checking for ErrChunkSizeZero.
func getChunkSize(chunkMode uint32, cardinality uint64, maxDocs uint64) (uint64, error) {
	switch {
	case chunkMode == 0:
		return 0, ErrChunkSizeZero

	// any chunkMode <= 1024 will always chunk with chunkSize=chunkMode
	case chunkMode <= 1024:
		// legacy chunk size
		return uint64(chunkMode), nil

	case chunkMode == 1025:
		// attempt at simple improvement
		// theory - the point of chunking is to put a bound on the maximum number of
		// calls to Next() needed to find a random document.  ie, you should be able
		// to do one jump to the correct chunk, and then walk through at most
		// chunk-size items
		// previously 1024 was chosen as the chunk size, but this is particularly
		// wasteful for low cardinality terms.  the observation is that if there
		// are less than 1024 items, why not put them all in one chunk,
		// this way you'll still achieve the same goal of visiting at most
		// chunk-size items.
		// no attempt is made to tweak any other case
		if cardinality <= 1024 {
			if maxDocs == 0 {
				return 0, ErrChunkSizeZero
			}
			return maxDocs, nil
		}
		return 1024, nil

	case chunkMode == 1026:
		// improve upon the ideas tested in chunkMode 1025
		// the observation that the fewest number of dense chunks is the most
		// desirable layout, given the built-in assumptions of chunking
		// (that we want to put an upper-bound on the number of items you must
		//  walk over without skipping, currently tuned to 1024)
		//
		// 1.  compute the number of chunks needed (max 1024/chunk)
		// 2.  convert to chunkSize, dividing into maxDocs
		numChunks := (cardinality / 1024) + 1
		chunkSize := maxDocs / numChunks
		if chunkSize == 0 {
			return 0, ErrChunkSizeZero
		}
		return chunkSize, nil

	// NOTE: mode 1027 (PFOR with unconditional 256-docNum windows) was removed in
	// favour of 1028 below; see PFORChunkMode. A segment written by an earlier
	// prototype build reaches the default case and fails loudly.

	case chunkMode == 1028:
		// PFOR block encoding, cardinality-adaptive (§75).
		//
		// A term matching no more than one block's worth of documents gets a
		// single chunk spanning the segment, so its chunk-offset directory holds
		// one entry instead of ceil(maxDocs/256). The block header stores the
		// entry count (1..255, with 0 meaning 256), and the v18 freq stream holds
		// exactly one value per document — norms live in SectionNormColumn — so
		// cardinality <= pforBlockSize guarantees the single block stays within
		// the count the header can represent.
		//
		// Above the threshold, chunks are docNum-aligned windows, which is what
		// lets a chunk index be derived from a docNum as docNum/chunkSize.
		if cardinality <= pforBlockSize {
			if maxDocs == 0 {
				return 0, ErrChunkSizeZero
			}
			return maxDocs, nil
		}
		return pforBlockSize, nil
	}
	return 0, fmt.Errorf("unknown chunk mode %d", chunkMode)
}
