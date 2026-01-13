//  Copyright (c) 2017 Couchbase, Inc.
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

package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"

	zap "github.com/blevesearch/zapx/v17"
	"github.com/golang/snappy"
	"github.com/spf13/cobra"
)

func dumpDocValueResults(data []byte, args []string, field string, id int, fieldEndLoc, fieldStartLoc uint64) error {
	var chunkOffsetsPosition, offset, numChunks uint64

	var fieldName string
	var fieldID uint16
	var fieldDvSize float64
	var fieldChunkCount, fieldDvStart, fieldDvEnd, totalDvSize uint64
	var fieldChunkLens []uint64

	if fieldEndLoc-fieldStartLoc > 16 {
		numChunks = binary.BigEndian.Uint64(data[fieldEndLoc-8 : fieldEndLoc])
		// read the length of chunk offsets
		chunkOffsetsLen := binary.BigEndian.Uint64(data[fieldEndLoc-16 : fieldEndLoc-8])
		// acquire position of chunk offsets
		chunkOffsetsPosition = (fieldEndLoc - 16) - chunkOffsetsLen
	}

	// read the chunk offsets
	chunkLens := make([]uint64, numChunks)
	dvSize := uint64(0)
	for i := 0; i < int(numChunks); i++ {
		length, read := binary.Uvarint(
			data[chunkOffsetsPosition+offset : chunkOffsetsPosition+offset+
				binary.MaxVarintLen64])
		if read <= 0 {
			return fmt.Errorf("Corrupted chunk offset during segment load")
		}

		offset += uint64(read)
		chunkLens[i] = length
		dvSize += length
	}

	totalDvSize += dvSize
	// if no field args are given, then print out the dv locations for all fields
	if len(args) == 1 {
		mbsize := float64(dvSize) / (1024 * 1024)
		fmt.Printf("FieldID: %d '%s' docvalue at %d (%x) numChunks "+
			"%d  diskSize %.6f MB\n", id, field, fieldStartLoc,
			fieldStartLoc, numChunks, mbsize)
		return nil
	}

	// if the field is the requested one for more details,
	// then remember the details
	if field == args[1] {
		fieldDvStart = fieldStartLoc
		fieldDvEnd = fieldEndLoc
		fieldName = field
		fieldID = uint16(id)
		fieldDvSize = float64(dvSize) / (1024 * 1024)
		fieldChunkLens = append(fieldChunkLens, chunkLens...)
		fieldChunkCount = numChunks
	} else {
		return nil
	}

	mbsize := float64(totalDvSize) / (1024 * 1024)
	fmt.Printf("Total Doc Values Size on Disk: %.6f MB\n", mbsize)

	// done with the fields dv locs printing for the given zap file
	if len(args) == 1 {
		return nil
	}

	if fieldName == "" || fieldDvEnd == 0 {
		return fmt.Errorf("No docvalue persisted for given field arg: %s",
			args[1])
	}

	if len(args) == 2 {
		fmt.Printf("FieldID: %d '%s' docvalue at %d (%x) numChunks "+
			"%d  diskSize %.6f MB\n", fieldID, fieldName, fieldDvStart,
			fieldDvStart, fieldChunkCount, fieldDvSize)
		fmt.Printf("Number of docvalue chunks: %d\n", fieldChunkCount)
		return nil
	}

	localDocNum, err := strconv.Atoi(args[2])
	if err != nil {
		return fmt.Errorf("Unable to parse doc number: %v", err)
	}

	if localDocNum >= int(segment.NumDocs()) {
		return fmt.Errorf("Invalid doc number %d (valid 0 - %d)",
			localDocNum, segment.NumDocs()-1)
	}

	// find the chunkNumber where the docValues are stored
	docInChunk := uint64(localDocNum) / uint64(1024)

	if fieldChunkCount < docInChunk {
		return fmt.Errorf("No chunk exists for chunk number: %d for "+
			"localDocNum: %d", docInChunk, localDocNum)
	}

	start, end := readChunkBoundary(int(docInChunk), fieldChunkLens)
	destChunkDataLoc := fieldDvStart + start
	curChunkEnd := fieldDvStart + end

	// read the number of docs reside in the chunk
	var numDocs uint64
	var nr int
	numDocs, nr = binary.Uvarint(
		data[destChunkDataLoc : destChunkDataLoc+binary.MaxVarintLen64])
	if nr <= 0 {
		return fmt.Errorf("Failed to read the chunk")
	}

	chunkMetaLoc := destChunkDataLoc + uint64(nr)
	curChunkHeader := make([]zap.MetaData, int(numDocs))
	offset = uint64(0)
	for i := 0; i < int(numDocs); i++ {
		curChunkHeader[i].DocNum, nr = binary.Uvarint(
			data[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
		offset += uint64(nr)
		curChunkHeader[i].DocDvOffset, nr = binary.Uvarint(
			data[chunkMetaLoc+offset : chunkMetaLoc+offset+binary.MaxVarintLen64])
		offset += uint64(nr)
	}

	compressedDataLoc := chunkMetaLoc + offset
	dataLength := curChunkEnd - compressedDataLoc
	curChunkData := data[compressedDataLoc : compressedDataLoc+dataLength]

	start, end = getDocValueLocs(uint64(localDocNum), curChunkHeader)
	if start == math.MaxUint64 || end == math.MaxUint64 {
		fmt.Printf("No field values found for localDocNum: %d\n",
			localDocNum)
		fmt.Printf("Try docNums present in chunk: %s\n",
			metaDataDocNums(curChunkHeader))
		return nil
	}
	// uncompress the already loaded data
	uncompressed, err := snappy.Decode(nil, curChunkData)
	if err != nil {
		log.Printf("snappy err %+v ", err)
		return err
	}

	var termSeparator byte = 0xff
	var termSeparatorSplitSlice = []byte{termSeparator}

	// pick the terms for the given docNum
	uncompressed = uncompressed[start:end]
	for {
		i := bytes.Index(uncompressed, termSeparatorSplitSlice)
		if i < 0 {
			break
		}

		fmt.Printf(" %s ", uncompressed[0:i])
		uncompressed = uncompressed[i+1:]
	}
	fmt.Printf(" \n ")
	return nil
}

func docValueCmd(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("must specify index file path")
	}

	data := segment.Data()
	// iterate through fields index
	pos := segment.SectionsIndexOffset()
	if pos == 0 {
		// this is the case only for older file formats
		return fmt.Errorf("file format not supported")
	}

	fieldInv, fieldSectionMap, err := getSectionFields(data, pos)
	if err != nil {
		return fmt.Errorf("error while getting fields and sections info %v", err)
	}
	// if no fields are specified then print the docValue offsets for all fields set
	for id, field := range fieldInv {
		fieldStartLoc, fieldEndLoc, err := getDvOffsets(data, id, fieldSectionMap)
		if err != nil {
			return err
		}

		if fieldStartLoc == math.MaxUint64 && len(args) == 1 {
			fmt.Printf("FieldID: %d '%s' docvalue at %d (%x) not "+
				" persisted \n", id, field, fieldStartLoc, fieldStartLoc)
			continue
		}
		err = dumpDocValueResults(data, args, field, id, fieldEndLoc, fieldStartLoc)
		if err != nil {
			return err
		}
	}
	return nil
}

// docvalueCmd represents the docvalue command
var docvalueCmd = &cobra.Command{
	Use:   "docvalue [path] <field> optional <docNum> optional",
	Short: "docvalue prints the docvalue details by field, and docNum",
	Long:  `The docvalue command lets you explore the docValues in order of field and by doc number.`,
	RunE:  docValueCmd,
}

func loadFieldData(data []byte, pos uint64, fieldID uint64, sectionMap map[uint16]uint64, fieldsInv []string) ([]string, error) {
	fieldNameLen, sz := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += uint64(sz)

	fieldName := string(data[pos : pos+fieldNameLen])
	fieldsInv = append(fieldsInv, fieldName)

	pos += fieldNameLen

	fieldNumSections, sz := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += uint64(sz)

	for sectionIdx := uint64(0); sectionIdx < fieldNumSections; sectionIdx++ {
		// read section id
		fieldSectionType := binary.BigEndian.Uint16(data[pos : pos+2])
		pos += 2
		fieldSectionAddr := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		sectionMap[fieldSectionType] = fieldSectionAddr
	}
	return fieldsInv, nil
}

func getSectionFields(data []byte, pos uint64) ([]string, []map[uint16]uint64, error) {
	var fieldID uint64
	var err error
	var fieldSectionMap []map[uint16]uint64
	var fieldInv []string

	// read the number of fields
	numFields, sz := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += uint64(sz)

	for fieldID < numFields {
		sectionMap := make(map[uint16]uint64)
		addr := binary.BigEndian.Uint64(data[pos : pos+8])

		fieldInv, err = loadFieldData(data, addr, fieldID, sectionMap, fieldInv)
		if err != nil {
			return nil, nil, err
		}
		fieldSectionMap = append(fieldSectionMap, sectionMap)
		fieldID++
		pos += 8
	}

	return fieldInv, fieldSectionMap, nil
}

func getDvOffsets(data []byte, id int, fieldSectionMap []map[uint16]uint64) (uint64, uint64, error) {
	var read uint64
	pos := fieldSectionMap[id][zap.SectionInvertedTextIndex]
	fieldStartLoc, n := binary.Uvarint(
		data[pos : pos+binary.MaxVarintLen64])
	if n <= 0 {
		return 0, 0, fmt.Errorf("loadDvIterators: failed to read the "+
			" docvalue offsets for field %d", id)
	}

	read += uint64(n)
	fieldEndLoc, n := binary.Uvarint(
		data[pos+read : pos+read+binary.MaxVarintLen64])
	if n <= 0 {
		return 0, 0, fmt.Errorf("Failed to read the docvalue offset "+
			"end for field %d", id)
	}
	return fieldStartLoc, fieldEndLoc, nil
}

func getDocValueLocs(docNum uint64, metaHeader []zap.MetaData) (uint64, uint64) {
	i := sort.Search(len(metaHeader), func(i int) bool {
		return metaHeader[i].DocNum >= docNum
	})
	if i < len(metaHeader) && metaHeader[i].DocNum == docNum {
		return zap.ReadDocValueBoundary(i, metaHeader)
	}
	return math.MaxUint64, math.MaxUint64
}

func metaDataDocNums(metaHeader []zap.MetaData) string {
	docNums := ""
	for _, meta := range metaHeader {
		docNums += fmt.Sprintf("%d", meta.DocNum) + ", "
	}
	return docNums
}

func readChunkBoundary(chunk int, offsets []uint64) (uint64, uint64) {
	var start uint64
	if chunk > 0 {
		start = offsets[chunk-1]
	}
	return start, offsets[chunk]
}

func init() {
	RootCmd.AddCommand(docvalueCmd)
}
