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
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

// normColumnHeaderSize is the fixed byte count of the "no doc-values" prefix
// written before the raw norm bytes.  All sections must start with two
// uvarint(fieldNotUninverted) values so that loadDvReaders can safely skip
// them without misinterpreting the data.
//
// binary.MaxVarintLen64 = 10; uvarint(math.MaxUint64) always encodes in 10
// bytes, so two writes = 20 bytes total.
const normColumnHeaderSize = 2 * binary.MaxVarintLen64

func init() {
	registerSegmentSection(SectionNormColumn, &normColumnSection{})
}

// normColumnSection stores one SmallFloat-encoded byte per document per field.
// It replaces the per-term normBits varint in the inverted index freq stream
// (v18+), reducing freq-stream width and enabling future columnar access.
type normColumnSection struct{}

func (n *normColumnSection) Process(opaque map[int]resetable, docNum uint32, f index.Field, fieldID uint16) {
	nco := n.getNormColumnOpaque(opaque)
	if fieldID == math.MaxUint16 {
		nco.finalizeDoc(docNum)
		return
	}
	if isFieldExcludedFromInvertedTextIndexSection(f) {
		return
	}
	nco.pendingLens[fieldID] += f.AnalyzedLength()
}

func (n *normColumnSection) Persist(opaque map[int]resetable, w *FileWriter) error {
	nco := n.getNormColumnOpaque(opaque)
	return nco.writeCols(w)
}

func (n *normColumnSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	nco := n.getNormColumnOpaque(opaque)
	return nco.fieldAddrs[fieldID]
}

func (n *normColumnSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64,
	w *FileWriter, closeCh chan struct{}) error {

	nco := n.getNormColumnOpaque(opaque)
	numDocs := nco.numDocs

	for newFieldID, fieldName := range fieldsInv {
		if isClosed(closeCh) {
			return seg.ErrClosed
		}

		destCol := make([]byte, numDocs)
		hasAnyNorm := false

		for segIdx, sb := range segments {
			srcFieldIDPlus1 := sb.fieldsMap[fieldName]
			if srcFieldIDPlus1 == 0 {
				continue
			}
			srcFieldID := srcFieldIDPlus1 - 1
			if int(srcFieldID) >= len(sb.fieldsSectionsMap) {
				continue
			}
			sectionMap := sb.fieldsSectionsMap[srcFieldID]
			if len(sectionMap) <= SectionNormColumn {
				continue
			}
			srcAddr := sectionMap[SectionNormColumn]
			if srcAddr == 0 {
				continue
			}
			// skip the "no DV" header written for loadDvReaders compatibility
			srcNormStart := srcAddr + normColumnHeaderSize
			srcEnd := srcNormStart + sb.numDocs
			if srcEnd > uint64(len(sb.mem)) {
				continue
			}
			srcCol := sb.mem[srcNormStart:srcEnd]

			newDocNums := newDocNumsIn[segIdx]
			drop := drops[segIdx]

			for oldDocNum := uint64(0); oldDocNum < sb.numDocs; oldDocNum++ {
				if drop != nil && drop.Contains(uint32(oldDocNum)) {
					continue
				}
				newDocNum := newDocNums[oldDocNum]
				if newDocNum == docDropped {
					continue
				}
				b := srcCol[oldDocNum]
				if b != 0 {
					destCol[newDocNum] = b
					hasAnyNorm = true
				}
			}
		}

		if !hasAnyNorm {
			continue
		}

		var headerBuf [normColumnHeaderSize]byte
		n := binary.PutUvarint(headerBuf[:], fieldNotUninverted)
		binary.PutUvarint(headerBuf[n:], fieldNotUninverted)

		addr := w.Count()
		_, err := w.Write(headerBuf[:])
		if err != nil {
			return err
		}
		_, err = w.Write(destCol)
		if err != nil {
			return err
		}
		nco.fieldAddrs[newFieldID] = addr
	}

	return nil
}

func (n *normColumnSection) InitOpaque(args map[string]interface{}) resetable {
	nco := &normColumnOpaque{
		pendingLens: make(map[uint16]int),
		colData:     make(map[uint16][]byte),
		fieldAddrs:  make(map[int]int),
	}
	for k, v := range args {
		nco.Set(k, v)
	}
	return nco
}

func (n *normColumnSection) getNormColumnOpaque(opaque map[int]resetable) *normColumnOpaque {
	if _, ok := opaque[SectionNormColumn]; !ok {
		opaque[SectionNormColumn] = n.InitOpaque(nil)
	}
	return opaque[SectionNormColumn].(*normColumnOpaque)
}

// normColumnOpaque holds per-flush or per-merge state for the norm column section.
type normColumnOpaque struct {
	numDocs     uint64
	pendingLens map[uint16]int  // accumulated field lengths for current doc
	colData     map[uint16][]byte // fieldID -> []byte of length numDocs
	fieldAddrs  map[int]int     // fieldID -> byte offset in file
}

func (nco *normColumnOpaque) Reset() error {
	nco.numDocs = 0
	nco.pendingLens = make(map[uint16]int)
	nco.colData = make(map[uint16][]byte)
	nco.fieldAddrs = make(map[int]int)
	return nil
}

func (nco *normColumnOpaque) Set(key string, val interface{}) {
	switch key {
	case "results":
		if results, ok := val.([]index.Document); ok {
			nco.numDocs = uint64(len(results))
		}
	case "numDocs":
		if nd, ok := val.(uint64); ok {
			nco.numDocs = nd
		}
	}
}

// finalizeDoc is called with fieldID=MaxUint16 at the end of each document.
// It encodes all pending field lengths as SmallFloat bytes into colData.
func (nco *normColumnOpaque) finalizeDoc(docNum uint32) {
	for fid, fl := range nco.pendingLens {
		if fl <= 0 {
			continue
		}
		col := nco.colData[fid]
		if col == nil {
			col = make([]byte, nco.numDocs)
			nco.colData[fid] = col
		}
		if int(docNum) < len(col) {
			col[docNum] = encodeNormByte(uint32(fl))
		}
	}
	for k := range nco.pendingLens {
		delete(nco.pendingLens, k)
	}
}

// writeCols writes each field's norm column to w in field-ID order and records offsets.
func (nco *normColumnOpaque) writeCols(w *FileWriter) error {
	// collect and sort field IDs for deterministic output
	fieldIDs := make([]int, 0, len(nco.colData))
	for fid := range nco.colData {
		fieldIDs = append(fieldIDs, int(fid))
	}
	sort.Ints(fieldIDs)

	var headerBuf [normColumnHeaderSize]byte
	n := binary.PutUvarint(headerBuf[:], fieldNotUninverted)
	binary.PutUvarint(headerBuf[n:], fieldNotUninverted)

	for _, fid := range fieldIDs {
		col := nco.colData[uint16(fid)]
		addr := w.Count()
		_, err := w.Write(headerBuf[:])
		if err != nil {
			return err
		}
		_, err = w.Write(col)
		if err != nil {
			return err
		}
		nco.fieldAddrs[fid] = addr
	}
	return nil
}
