//  Copyright (c) 2020 Couchbase, Inc.
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
	"bufio"
	"fmt"
	"os"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve_index_api/vfs"
	mmap "github.com/blevesearch/mmap-go"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
)

// ZapPlugin implements the Plugin interface of
// the blevesearch/scorch_segment_api pkg
type ZapPlugin struct{}

func (*ZapPlugin) Type() string {
	return Type
}

func (*ZapPlugin) Version() uint32 {
	return Version
}

// OpenVFS opens a segment file through the VFS directory abstraction.
// This implementation uses OpenAt() for direct memory-mapping without temporary files.
func (*ZapPlugin) OpenVFS(dir vfs.Directory, name string) (segment.Segment, error) {
	// 1. Open file for random access from VFS
	rac, err := dir.OpenAt(name)
	if err != nil {
		return nil, fmt.Errorf("VFS openAt %s: %w", name, err)
	}

	// 2. Get file descriptor for mmap
	fd := rac.AsFd()
	if fd == 0 {
		rac.Close()
		return nil, fmt.Errorf("VFS does not provide file descriptor for mmap (not file-backed)")
	}

	// 3. Get the underlying *os.File for mmap
	// FileReaderAtCloser embeds *os.File, so we can type assert to access it
	var f *os.File
	if frac, ok := rac.(*vfs.FileReaderAtCloser); ok {
		f = frac.File
	} else {
		// Fallback: not a FileReaderAtCloser, can't get *os.File for mmap
		rac.Close()
		return nil, fmt.Errorf("VFS ReaderAtCloser is not file-backed, cannot mmap")
	}

	// 4. Memory-map the file
	// First check file size to ensure it's valid for mmap
	fileInfo, err := f.Stat()
	if err != nil {
		rac.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	if fileInfo.Size() == 0 {
		rac.Close()
		return nil, fmt.Errorf("cannot mmap empty file")
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		rac.Close()
		return nil, fmt.Errorf("mmap failed (file size: %d): %w", fileInfo.Size(), err)
	}

	// 5. Create segment with mmap
	rv := &Segment{
		SegmentBase: SegmentBase{
			fieldsMap:      make(map[string]uint16),
			fieldFSTs:      make(map[uint16]*vellum.FST),
			vecIndexCache:  newVectorIndexCache(),
			synIndexCache:  newSynonymIndexCache(),
			fieldDvReaders: make([]map[uint16]*docValueReader, len(segmentSections)),
		},
		f:    f,
		mm:   mm,
		path: name, // Use VFS name instead of filesystem path
		refs: 1,
	}
	rv.SegmentBase.updateSize()

	// 6. Load segment metadata
	if err := rv.loadConfig(); err != nil {
		rv.Close()
		return nil, err
	}

	if err := rv.loadFieldsNew(); err != nil {
		rv.Close()
		return nil, err
	}

	if err := rv.loadDvReaders(); err != nil {
		rv.Close()
		return nil, err
	}

	return rv, nil
}

// MergeVFS merges segments and writes the result through the VFS directory abstraction.
// This implementation writes directly to the VFS writer, avoiding temporary files.
func (*ZapPlugin) MergeVFS(dir vfs.Directory, name string,
	segments []segment.Segment, drops []*roaring.Bitmap,
	closeCh chan struct{}, s segment.StatsReporter) ([][]uint64, uint64, error) {

	// 1. Extract SegmentBases from segments
	segmentBases := make([]*SegmentBase, len(segments))
	for segmenti, seg := range segments {
		switch segmentx := seg.(type) {
		case *Segment:
			segmentBases[segmenti] = &segmentx.SegmentBase
		case *SegmentBase:
			segmentBases[segmenti] = segmentx
		default:
			return nil, 0, fmt.Errorf("unexpected segment type: %T", seg)
		}
	}

	// 2. Create VFS writer for output
	w, err := dir.Create(name)
	if err != nil {
		return nil, 0, fmt.Errorf("VFS create %s: %w", name, err)
	}
	defer w.Close()

	// 3. Buffer the VFS output for performance
	br := bufio.NewWriterSize(w, DefaultFileMergerBufferSize)

	// 4. Wrap writer for counting and stats
	cr := NewCountHashWriterWithStatsReporter(br, s)

	// 5. Perform merge to writer (existing logic)
	newDocNums, numDocs, storedIndexOffset, _, _, sectionsIndexOffset, err :=
		mergeToWriter(segmentBases, drops, DefaultChunkMode, cr, closeCh)
	if err != nil {
		return nil, 0, fmt.Errorf("merge to writer: %w", err)
	}

	// 6. Write footer
	err = persistFooter(numDocs, storedIndexOffset, sectionsIndexOffset, sectionsIndexOffset,
		0, DefaultChunkMode, cr.Sum32(), cr)
	if err != nil {
		return nil, 0, fmt.Errorf("persist footer: %w", err)
	}

	// 7. Flush buffer
	if err := br.Flush(); err != nil {
		return nil, 0, fmt.Errorf("flush buffer: %w", err)
	}

	// 8. Sync to VFS for durability
	if err := w.Sync(); err != nil {
		return nil, 0, fmt.Errorf("VFS sync: %w", err)
	}

	return newDocNums, uint64(cr.Count()), nil
}
