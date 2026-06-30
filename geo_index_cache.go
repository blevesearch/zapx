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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
)

type geoIndexCache struct {
	m     sync.RWMutex
	cache map[uint16]*geoCacheEntry

	closeCh  chan struct{}
	isClosed bool
}

var geoMonitorFreq = 1 * time.Second

func newGeoIndexCache() *geoIndexCache {
	return &geoIndexCache{
		cache:   make(map[uint16]*geoCacheEntry),
		closeCh: make(chan struct{}),
	}
}

func (gc *geoIndexCache) Clear() {
	gc.m.Lock()
	if gc.isClosed {
		gc.m.Unlock()
		return
	}
	gc.isClosed = true
	close(gc.closeCh)

	for _, entry := range gc.cache {
		entry.Close()
	}
	gc.cache = nil
	gc.m.Unlock()
}

func (gc *geoIndexCache) loadOrCreate(field uint16, mem []byte, except *roaring.Bitmap, r *FileReader) (*geoCacheEntry, error) {
	gc.m.RLock()
	if gc.isClosed {
		gc.m.RUnlock()
		return nil, nil
	}

	entry, ok := gc.cache[field]
	if ok {
		gc.m.RUnlock()
		return entry.load(), nil
	}
	gc.m.RUnlock()

	gc.m.Lock()
	defer gc.m.Unlock()
	if gc.isClosed {
		return nil, nil
	}

	entry, ok = gc.cache[field]
	if ok {
		return entry.load(), nil
	}

	return gc.createAndCacheLocked(field, mem, except, r)

}

func (gc *geoIndexCache) createAndCacheLocked(field uint16, mem []byte,
	except *roaring.Bitmap, r *FileReader) (*geoCacheEntry, error) {

	var pos uint64
	// Load Num Docs
	numDocs, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)
	if numDocs == 0 {
		return nil, fmt.Errorf("no geo docs found")
	}

	// Load Doc ID to Doc Num mapping
	buf, shift, err := r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	docNums := make([]uint64, numDocs)
	for i := 0; i < int(numDocs); i++ {
		docNums[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load the Document Scores
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	docScores := make([]uint64, numDocs)
	for i := 0; i < int(numDocs); i++ {
		docScores[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Inner Cells
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	innerCells := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		innerCells[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Inner Cell Doc IDs
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	innerDocIDs := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		innerDocIDs[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Cross Cells
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	crossCells := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		crossCells[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load Cross Cell Doc IDs
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	crossDocIDs := make([]uint64, len(buf)/8)
	for i := 0; i < len(buf)/8; i++ {
		crossDocIDs[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	// Load BBox Metadata without expanding the BBox data
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	bBoxesOffsets := make([]uint64, numDocs)
	for i := 0; i < int(numDocs); i++ {
		bBoxesOffsets[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	bBoxesLen, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	bboxMem := mem[pos : pos+bBoxesLen]
	pos += bBoxesLen

	// Load Shape Metadata without expanding the Shape data
	buf, shift, err = r.ReadArray(mem[pos:])
	if err != nil {
		return nil, err
	}
	pos += shift

	shapeOffsets := make([]uint64, numDocs)
	for i := 0; i < int(numDocs); i++ {
		shapeOffsets[i] = binary.BigEndian.Uint64(buf[i*8 : (i+1)*8])
	}

	shapeLen, n := binary.Uvarint(mem[pos : pos+binary.MaxVarintLen64])
	pos += uint64(n)

	shapeMem := mem[pos : pos+shapeLen]
	pos += shapeLen

	rv := newGeoCacheEntry(innerCells, innerDocIDs, crossCells, crossDocIDs, bBoxesOffsets,
		bboxMem, shapeOffsets, shapeMem, numDocs, docNums, docScores, except, r)

	gc.insertLOCKED(field, rv)

	return rv, nil
}

func (gc *geoIndexCache) insertLOCKED(field uint16, entry *geoCacheEntry) {
	if len(gc.cache) == 0 {
		go gc.monitor()
	}

	gc.cache[field] = entry
}

func (gc *geoIndexCache) monitor() {
	ticker := time.NewTicker(geoMonitorFreq)
	defer ticker.Stop()

	for {
		select {
		case <-gc.closeCh:
			return
		case <-ticker.C:
			exit := gc.cleanup()
			if exit {
				return
			}
		}
	}
}

func (gc *geoIndexCache) cleanup() bool {
	gc.m.Lock()

	for field, entry := range gc.cache {
		sample := atomic.LoadUint64(&entry.tracker.sample)
		entry.tracker.add(sample)

		refCount := atomic.LoadInt64(&entry.refs)

		if refCount <= 0 && entry.tracker.avg <= (1-entry.tracker.alpha) {
			atomic.StoreUint64(&entry.tracker.sample, 0)
			delete(gc.cache, field)
			entry.Close()
			continue
		}
		atomic.StoreUint64(&entry.tracker.sample, 0)
	}

	rv := len(gc.cache) == 0
	gc.m.Unlock()
	return rv
}

// geoCacheEntry represents a cached entry for a specific field in the geo index.
type geoCacheEntry struct {
	innerCells  []uint64
	innerDocIDs []uint64

	crossCells  []uint64
	crossDocIDs []uint64

	// contains offsets for each bounding box in the bboxMem slice
	bboxOffsets []uint64
	// contains raw unprocessed bounding box data for on demand processing
	bboxMem []byte

	// contains offsets for each shape in the shapeMem slice
	shapeOffsets []uint64
	// contains raw unprocessed shape data for on demand processing
	shapeMem []byte

	numDocs   uint64
	docNums   []uint64
	docScores []uint64

	tracker *ewma
	refs    int64

	except     *roaring.Bitmap
	fileReader *FileReader
}

func newGeoCacheEntry(innerCells, innerDocIDs, crossCells, crossDocIDs, bboxOffsets []uint64,
	bboxMem []byte, shapeOffsets []uint64, shapeMem []byte, numDocs uint64, docNums []uint64,
	docScores []uint64, except *roaring.Bitmap, r *FileReader) *geoCacheEntry {

	return &geoCacheEntry{
		innerCells:  innerCells,
		innerDocIDs: innerDocIDs,

		crossCells:  crossCells,
		crossDocIDs: crossDocIDs,

		bboxOffsets: bboxOffsets,
		bboxMem:     bboxMem,

		shapeOffsets: shapeOffsets,
		shapeMem:     shapeMem,

		numDocs:   numDocs,
		docNums:   docNums,
		docScores: docScores,

		tracker: &ewma{
			alpha:  0.4,
			sample: 1,
		},
		refs: 1,

		except:     except,
		fileReader: r,
	}
}

func (gce *geoCacheEntry) Close() {
	gce.decRef()
}

func (gce *geoCacheEntry) load() *geoCacheEntry {
	gce.incHits()
	gce.incRef()

	return gce
}

func (gce *geoCacheEntry) incHits() {
	atomic.AddUint64(&gce.tracker.sample, 1)
}

func (gce *geoCacheEntry) incRef() {
	atomic.AddInt64(&gce.refs, 1)
}

func (gce *geoCacheEntry) decRef() {
	atomic.AddInt64(&gce.refs, -1)
}

func (gce *geoCacheEntry) InnerCells() []uint64 {
	return gce.innerCells
}

func (gce *geoCacheEntry) InnerDocIDs() []uint64 {
	return gce.innerDocIDs
}

func (gce *geoCacheEntry) CrossCells() []uint64 {
	return gce.crossCells
}

func (gce *geoCacheEntry) CrossDocIDs() []uint64 {
	return gce.crossDocIDs
}

func (gce *geoCacheEntry) BoundingBox(docNum uint64) ([]byte, error) {
	if docNum >= gce.numDocs {
		return nil, fmt.Errorf("docNum out of range")
	}

	var offsetStart uint64
	if docNum != 0 {
		offsetStart = gce.bboxOffsets[docNum-1]
	}
	offsetEnd := gce.bboxOffsets[docNum]
	if offsetEnd == offsetStart {
		return nil, fmt.Errorf("no bounding box for docNum %d", docNum)
	}

	buf, err := gce.fileReader.process(gce.bboxMem[offsetStart:offsetEnd])
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (gce *geoCacheEntry) Shape(docNum uint64) ([]byte, error) {
	if docNum >= gce.numDocs {
		return nil, fmt.Errorf("docNum out of range")
	}

	var offsetStart uint64
	if docNum != 0 {
		offsetStart = gce.shapeOffsets[docNum-1]
	}
	offsetEnd := gce.shapeOffsets[docNum]
	if offsetEnd == offsetStart {
		return nil, fmt.Errorf("no shape for docNum %d", docNum)
	}

	buf, err := gce.fileReader.process(gce.shapeMem[offsetStart:offsetEnd])
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (gce *geoCacheEntry) DocNums() []uint64 {
	return gce.docNums
}

func (gce *geoCacheEntry) NumDocs() uint64 {
	return gce.numDocs
}

func (gce *geoCacheEntry) DocScores() []uint64 {
	return gce.docScores
}

func (gce *geoCacheEntry) Exclude() *roaring.Bitmap {
	return gce.except
}
