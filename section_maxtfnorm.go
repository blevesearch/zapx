// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"encoding/binary"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
)

func init() {
	registerSegmentSection(SectionMaxTFNorm, &maxTFNormSection{})
}

// On-disk layout of one field's MaxTFNorm sidecar block — two parallel
// fixed-width columns rather than interleaved rows:
//
//	[normColumnHeaderSize]  two uvarint(fieldNotUninverted), so loadDvReaders
//	                        skips the block safely
//	[4]                     LE uint32 entryCount
//	[8 × entryCount]        offsets column: LE uint64 postingsOffset,
//	                        ascending — the binary search key
//	[8 × entryCount]        values column: LE uint32 maxFreq | LE uint32
//	                        minFieldLen, at the same index as its offset
//
// Both columns are fixed width, so query time binary-searches the mmap'd bytes
// in place: no decode pass, no per-field heap allocation, nothing cached.
//
// Columnar rather than row-based because the search only ever probes offsets.
// At an 8-byte stride a 64-byte cache line holds 8 candidate offsets instead of
// the 4 a 16-byte row stride would give, so the tail of the search — where
// probes converge and locality actually pays — touches half as many lines. The
// matching (maxFreq, minFieldLen) pair is then a single indexed load from the
// values column, touching exactly one line, only on a hit.
const (
	maxTFNormOffsetSize  = 8 // LE uint64 postingsOffset
	maxTFNormValueSize   = 8 // LE uint32 maxFreq + LE uint32 minFieldLen
	maxTFNormCountSize   = 4
	maxTFNormColumnsFrom = normColumnHeaderSize + maxTFNormCountSize
)

// maxTFNormSection stores per-term maxFreq and minFieldLen values collected at
// segment flush time so that WAND/MaxScore pruning can compute an O(1) upper
// bound for MaxTFNorm without a cold-start scan.
type maxTFNormSection struct{}

// maxTFNormSidecarEntry holds the data collected for a single (field, term) at flush time.
type maxTFNormSidecarEntry struct {
	postingsOffset uint64
	maxFreq        uint64
	minFieldLen    uint32 // shortest field length in the posting list = highest norm
}

// maxTFNormOpaque holds per-flush state for the maxTFNorm section.
type maxTFNormOpaque struct {
	numDocs      uint64
	fieldEntries map[int][]maxTFNormSidecarEntry // fieldID → unsorted entries (sorted on Persist)
	fieldAddrs   map[int]int                     // fieldID → byte offset in file
}

func (m *maxTFNormOpaque) Reset() error {
	m.numDocs = 0
	m.fieldEntries = make(map[int][]maxTFNormSidecarEntry)
	m.fieldAddrs = make(map[int]int)
	return nil
}

func (m *maxTFNormOpaque) Set(key string, val interface{}) {
	switch key {
	case "results":
		if results, ok := val.([]index.Document); ok {
			m.numDocs = uint64(len(results))
		}
	case "numDocs":
		if nd, ok := val.(uint64); ok {
			m.numDocs = nd
		}
	}
}

// addEntry records the max freq / min field length for a single (fieldID, term)
// pair.  minFieldLen is the shortest analyzed field length over the term's
// posting list, i.e. the doc with the highest norm (= 1/sqrt(minFieldLen)).
// postingsOffset is the FST value for this term (general encoding only).
func (m *maxTFNormOpaque) addEntry(fieldID int, postingsOffset uint64, maxFreq uint64, minFieldLen uint32) {
	m.fieldEntries[fieldID] = append(m.fieldEntries[fieldID], maxTFNormSidecarEntry{
		postingsOffset: postingsOffset,
		maxFreq:        maxFreq,
		minFieldLen:    minFieldLen,
	})
}

// Process is a no-op: the data is collected in writeDicts, not per-doc.
func (ms *maxTFNormSection) Process(opaque map[int]resetable, docNum uint32, f index.Field, fieldID uint16) {
}

// Persist writes each field's entry list to w in sorted order.
func (ms *maxTFNormSection) Persist(opaque map[int]resetable, w *FileWriter) error {
	mto := ms.getMaxTFNormOpaque(opaque)
	return mto.writeEntries(w)
}

// AddrForField returns the file offset for the given field's entry block,
// or 0 if no entries were recorded for that field.
func (ms *maxTFNormSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	mto := ms.getMaxTFNormOpaque(opaque)
	return mto.fieldAddrs[fieldID]
}

// Merge writes the MaxTFNorm sidecar for the merged segment.
// Per-term (maxFreq, minFieldLen) entries are accumulated into opaque by
// mergeAndPersistInvertedSection; Merge here serialises them and records
// their file offsets so AddrForField returns correct addresses to the
// segment footer's fieldsSectionsMap.
func (ms *maxTFNormSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64,
	w *FileWriter, closeCh chan struct{}) error {
	mto := ms.getMaxTFNormOpaque(opaque)
	return mto.writeEntries(w)
}

// InitOpaque creates the per-flush opaque for the maxTFNorm section.
func (ms *maxTFNormSection) InitOpaque(args map[string]interface{}) resetable {
	mto := &maxTFNormOpaque{
		fieldEntries: make(map[int][]maxTFNormSidecarEntry),
		fieldAddrs:   make(map[int]int),
	}
	for k, v := range args {
		mto.Set(k, v)
	}
	return mto
}

func (ms *maxTFNormSection) getMaxTFNormOpaque(opaque map[int]resetable) *maxTFNormOpaque {
	if _, ok := opaque[SectionMaxTFNorm]; !ok {
		opaque[SectionMaxTFNorm] = ms.InitOpaque(nil)
	}
	return opaque[SectionMaxTFNorm].(*maxTFNormOpaque)
}

// writeEntries serialises all field entries to w in fieldID order, using the
// two-column layout documented at the top of this file.
func (mto *maxTFNormOpaque) writeEntries(w *FileWriter) error {
	// collect and sort field IDs for deterministic output
	fieldIDs := make([]int, 0, len(mto.fieldEntries))
	for fid := range mto.fieldEntries {
		fieldIDs = append(fieldIDs, fid)
	}
	sort.Ints(fieldIDs)

	var headerBuf [normColumnHeaderSize]byte
	n := binary.PutUvarint(headerBuf[:], fieldNotUninverted)
	binary.PutUvarint(headerBuf[n:], fieldNotUninverted)

	var buf [maxTFNormOffsetSize]byte

	for _, fid := range fieldIDs {
		entries := mto.fieldEntries[fid]
		if len(entries) == 0 {
			continue
		}

		// sort by postingsOffset for binary search at query time
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].postingsOffset < entries[j].postingsOffset
		})

		addr := w.Count()
		if _, err := w.Write(headerBuf[:]); err != nil {
			return err
		}

		// write entry count so the reader knows the record array's extent
		binary.LittleEndian.PutUint32(buf[:], uint32(len(entries)))
		if _, err := w.Write(buf[:maxTFNormCountSize]); err != nil {
			return err
		}

		// offsets column first, so the binary search reads a contiguous run
		for _, e := range entries {
			binary.LittleEndian.PutUint64(buf[:], e.postingsOffset)
			if _, err := w.Write(buf[:]); err != nil {
				return err
			}
		}

		// values column, parallel to the offsets column by index
		for _, e := range entries {
			maxFreq := e.maxFreq
			if maxFreq > math.MaxUint32 {
				maxFreq = math.MaxUint32 // upper bound only; unreachable in practice
			}
			binary.LittleEndian.PutUint32(buf[0:], uint32(maxFreq))
			binary.LittleEndian.PutUint32(buf[4:], e.minFieldLen)
			if _, err := w.Write(buf[:]); err != nil {
				return err
			}
		}

		mto.fieldAddrs[fid] = addr
	}

	return nil
}

// ----------------------------------------------------------------------------
// Read-time helpers on SegmentBase
// ----------------------------------------------------------------------------

// maxTFNormColumns returns the field's two sidecar columns as slices of sb.mem
// — no copy, no decode.  offsets is the ascending binary-search key column and
// values holds the matching (maxFreq, minFieldLen) pairs at the same index.
// Returns nil, nil when the field has no MaxTFNorm section or the block is
// truncated.
func (sb *SegmentBase) maxTFNormColumns(fieldID uint16) (offsets, values []byte) {
	fid := int(fieldID)
	if fid >= len(sb.fieldsSectionsMap) {
		return nil, nil
	}
	sectionMap := sb.fieldsSectionsMap[fid]
	if SectionMaxTFNorm >= len(sectionMap) {
		return nil, nil
	}
	addr := sectionMap[SectionMaxTFNorm]
	if addr == 0 {
		return nil, nil
	}

	mem := sb.mem
	offsetsStart := addr + maxTFNormColumnsFrom
	if offsetsStart > uint64(len(mem)) {
		return nil, nil
	}
	count := uint64(binary.LittleEndian.Uint32(mem[addr+normColumnHeaderSize:]))
	if count == 0 {
		return nil, nil
	}
	valuesStart := offsetsStart + count*maxTFNormOffsetSize
	valuesEnd := valuesStart + count*maxTFNormValueSize
	if valuesEnd > uint64(len(mem)) {
		return nil, nil
	}
	return mem[offsetsStart:valuesStart], mem[valuesStart:valuesEnd]
}

// lookupMaxTFNorm returns the precomputed (maxFreq, minFieldLen) for the given
// field/postingsOffset by binary-searching the sidecar records in place.
// minFieldLen is the shortest field length over the posting list, i.e. the
// position of the highest norm; callers convert it with normFromFieldLen.
// Returns ok=false if the MaxTFNorm section is absent or the term is not found.
func (sb *SegmentBase) lookupMaxTFNorm(fieldID uint16, postingsOffset uint64) (maxFreq uint64, minFieldLen uint32, ok bool) {
	offsets, values := sb.maxTFNormColumns(fieldID)
	if len(offsets) == 0 {
		return 0, 0, false
	}

	lo, hi := 0, len(offsets)/maxTFNormOffsetSize-1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		off := binary.LittleEndian.Uint64(offsets[mid*maxTFNormOffsetSize:])
		switch {
		case off < postingsOffset:
			lo = mid + 1
		case off > postingsOffset:
			hi = mid - 1
		default:
			// hit: the pair lives at the same index in the values column
			val := values[mid*maxTFNormValueSize:]
			sb.incrementBytesRead(maxTFNormCountSize + maxTFNormOffsetSize +
				maxTFNormValueSize)
			return uint64(binary.LittleEndian.Uint32(val[0:])),
				binary.LittleEndian.Uint32(val[4:]), true
		}
	}
	return 0, 0, false
}
