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

// maxTFNormSection stores per-term maxFreq and maxNorm values collected at
// segment flush time so that WAND/MaxScore pruning can compute an O(1) upper
// bound for MaxTFNorm without a cold-start scan.
type maxTFNormSection struct{}

// maxTFNormSidecarEntry holds the data collected for a single (field, term) at flush time.
type maxTFNormSidecarEntry struct {
	postingsOffset uint64
	maxFreq        uint64
	maxNormByte    uint8 // encodeNormByte(minFieldLen) where minFieldLen ≈ 1/(maxNorm²)
}

// maxTFNormOpaque holds per-flush state for the maxTFNorm section.
type maxTFNormOpaque struct {
	numDocs      uint64
	fieldEntries map[int][]maxTFNormSidecarEntry // fieldID → unsorted entries (sorted on Persist)
	fieldAddrs   map[int]int              // fieldID → byte offset in file
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

// addEntry records the max freq/norm stats for a single (fieldID, term) pair.
// norm is the raw float32 norm value (= 1/sqrt(fieldLen)).
// postingsOffset is the FST value for this term (general encoding only).
func (m *maxTFNormOpaque) addEntry(fieldID int, postingsOffset uint64, maxFreq uint64, maxNorm float32) {
	nb := normToSmallFloat(maxNorm)
	m.fieldEntries[fieldID] = append(m.fieldEntries[fieldID], maxTFNormSidecarEntry{
		postingsOffset: postingsOffset,
		maxFreq:        maxFreq,
		maxNormByte:    nb,
	})
}

// normToSmallFloat converts a norm (= 1/sqrt(fieldLen)) back to a SmallFloat
// byte encoding of the corresponding fieldLen.  The result is the SMALLEST
// SmallFloat value over all docs in the posting list (i.e. the entry with the
// smallest fieldLen = highest norm), which decodes back to ≈ maxNorm.
func normToSmallFloat(norm float32) uint8 {
	if norm <= 0 {
		return 0
	}
	// norm = 1/sqrt(fieldLen) → fieldLen = 1/(norm²)
	fieldLen := uint32(1.0/(float64(norm)*float64(norm)) + 0.5)
	if fieldLen == 0 {
		fieldLen = 1
	}
	return encodeNormByte(fieldLen)
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

// Merge is a no-op for now: merged segments fall back to the lazy scan path.
// (addr=0 in fieldsSectionsMap → lookupMaxTFNorm returns ok=false → lazy scan.)
func (ms *maxTFNormSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64,
	w *FileWriter, closeCh chan struct{}) error {
	return nil
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

// writeEntries serialises all field entries to w in fieldID order.
// Each field block has:
//   - a normColumnHeaderSize (20-byte) "no DV" prefix for loadDvReaders compatibility
//   - entries sorted by postingsOffset for binary search at read time
//     each entry: uvarint(postingsOffset), uvarint(maxFreq), uint8(maxNormByte)
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

	var buf [binary.MaxVarintLen64]byte

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

		for _, e := range entries {
			// write postingsOffset
			nn := binary.PutUvarint(buf[:], e.postingsOffset)
			if _, err := w.Write(buf[:nn]); err != nil {
				return err
			}
			// write maxFreq
			nn = binary.PutUvarint(buf[:], e.maxFreq)
			if _, err := w.Write(buf[:nn]); err != nil {
				return err
			}
			// write maxNormByte
			if _, err := w.Write([]byte{e.maxNormByte}); err != nil {
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

// loadedMaxTFNormField holds the decoded entry slice for one field, ready for
// binary search.  Stored in SegmentBase.maxTFNormCache (sync.Map).
type loadedMaxTFNormField struct {
	postingsOffsets []uint64
	maxFreqs        []uint64
	maxNormBytes    []uint8
}

// lookupMaxTFNorm returns the precomputed (maxFreq, maxNorm) for the given
// field/postingsOffset, loading and caching the entry slice on first call.
// Returns ok=false if the MaxTFNorm section is absent or the term is not found.
func (sb *SegmentBase) lookupMaxTFNorm(fieldID uint16, postingsOffset uint64) (maxFreq uint64, maxNorm float32, ok bool) {
	fid := int(fieldID)
	if fid >= len(sb.fieldsSectionsMap) {
		return 0, 0, false
	}
	sectionMap := sb.fieldsSectionsMap[fid]
	if SectionMaxTFNorm >= len(sectionMap) {
		return 0, 0, false
	}
	addr := sectionMap[SectionMaxTFNorm]
	if addr == 0 {
		return 0, 0, false
	}

	// Load (or reuse) the decoded slice for this field.
	if v, loaded := sb.maxTFNormCache.Load(fid); loaded {
		lf := v.(*loadedMaxTFNormField)
		return binarySearchMaxTFNorm(lf, postingsOffset)
	}

	// Not yet loaded: decode from memory.
	lf, err := sb.decodeMaxTFNormField(addr)
	if err != nil || lf == nil {
		return 0, 0, false
	}
	// Store; if another goroutine beat us, use their value.
	actual, _ := sb.maxTFNormCache.LoadOrStore(fid, lf)
	return binarySearchMaxTFNorm(actual.(*loadedMaxTFNormField), postingsOffset)
}

// decodeMaxTFNormField reads the on-disk entry list for a field.
func (sb *SegmentBase) decodeMaxTFNormField(addr uint64) (*loadedMaxTFNormField, error) {
	// skip the 20-byte "no DV" header
	pos := addr + normColumnHeaderSize
	if pos >= uint64(len(sb.mem)) {
		return nil, nil
	}

	var lf loadedMaxTFNormField
	mem := sb.mem

	for pos < uint64(len(mem)) {
		// read postingsOffset
		pOff, n := binary.Uvarint(mem[pos:])
		if n <= 0 {
			break
		}
		pos += uint64(n)

		// Safety: if pOff == 0 we've likely hit padding or another section's data.
		// The FST general-encoding postingsOffset is always > 0 for real terms.
		// However 0 is never a valid postingsOffset for the MaxTFNorm section
		// (we only record entries with postingsOffset > 0 in addEntry).
		// A zero here means we've overrun our data; stop.
		if pOff == 0 {
			break
		}

		// read maxFreq
		mf, n := binary.Uvarint(mem[pos:])
		if n <= 0 {
			break
		}
		pos += uint64(n)

		// read maxNormByte
		if pos >= uint64(len(mem)) {
			break
		}
		mnb := mem[pos]
		pos++

		lf.postingsOffsets = append(lf.postingsOffsets, pOff)
		lf.maxFreqs = append(lf.maxFreqs, mf)
		lf.maxNormBytes = append(lf.maxNormBytes, mnb)
	}

	sb.incrementBytesRead(pos - addr)

	if len(lf.postingsOffsets) == 0 {
		return nil, nil
	}
	return &lf, nil
}

// binarySearchMaxTFNorm looks up postingsOffset in the sorted slice.
func binarySearchMaxTFNorm(lf *loadedMaxTFNormField, postingsOffset uint64) (maxFreq uint64, maxNorm float32, ok bool) {
	offs := lf.postingsOffsets
	lo, hi := 0, len(offs)-1
	for lo <= hi {
		mid := (lo + hi) >> 1
		if offs[mid] == postingsOffset {
			mf := lf.maxFreqs[mid]
			mnb := lf.maxNormBytes[mid]
			fl := normDecodeTable[mnb]
			var norm float32
			if fl > 0 {
				norm = float32(1.0 / math.Sqrt(float64(fl)))
			}
			return mf, norm, true
		} else if offs[mid] < postingsOffset {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return 0, 0, false
}
