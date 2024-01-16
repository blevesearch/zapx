//  Copyright (c) 2023 Couchbase, Inc.
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

package cmd

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/blevesearch/go-faiss"
	zap "github.com/blevesearch/zapx/v16"
	"github.com/spf13/cobra"
)

// vectorCmd represent the vector command
// which parses the vector section of a field
var vectorCmd = &cobra.Command{
	Use:   "vector [path] [field] list <vecID> {optional}",
	Short: "prints vector index details for a specified field",
	Long: `The vector command let's you parse a vector section of the specified field and various details about the same.
	1. check whether a vector section exists for the field.
	2. if so, fetch the number of vectors, size of the serialized vector index,
		vector index's config - type, dims, similarity metric.
	3. given a vector ID, get all the local docNums its present in.
	4. reconstruct vector given the vectorID.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		data := segment.Data()
		pos := segment.FieldsIndexOffset()

		if pos == 0 {
			// this is the case only for older file formats
			return fmt.Errorf("file format not supported")
		}

		if len(args) < 2 {
			return fmt.Errorf("must specify field")
		}

		// read the number of fields
		numFields, sz := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(sz)

		var fieldID uint64
		var fieldInv []string

		for fieldID < numFields {
			addr := binary.BigEndian.Uint64(data[pos : pos+8])
			fieldSectionMap := make(map[uint16]uint64)

			fieldInv, err := loadFieldData(data, addr, fieldID, fieldSectionMap, fieldInv)
			if err != nil {
				return fmt.Errorf("error while parsing the field data %v", err)
			}
			if fieldInv[len(fieldInv)-1] == args[1] {
				vectorSectionOffset, ok := fieldSectionMap[uint16(zap.SectionFaissVectorIndex)]
				if !ok {
					return fmt.Errorf("the specified field doesn't have a vector section in it.")
				}
				numVecs, indexSize, vecDocIDMap, index, err := decodeSection(data, vectorSectionOffset)
				if err != nil {
					return fmt.Errorf("error while decoding the vector section for field %v, err: %v", args[1], err)
				}

				switch len(args) {
				case 2:
					metrics := map[int]string{
						faiss.MetricInnerProduct: "inner product",
						faiss.MetricL2:           "l2 distance",
					}
					fmt.Printf("decoded vector section content for field `%v`:\n", args[1])
					fmt.Printf("  number of vectors: %v\n", numVecs)
					fmt.Printf("  size of the serialized vector index: %v\n", indexSize)
					fmt.Printf("  dimensionality of vectors in the index: %v\n", index.D())
					fmt.Printf("  similarity metric used: %v\n", metrics[index.MetricType()])
				case 3:
					if args[2] == "list" {
						fmt.Printf("listing the vector IDs in the index\n")
						for vecID, doc := range vecDocIDMap {
							fmt.Printf("vector with vecID: %v present in doc: %v\n", vecID, doc)
						}
					}
				case 4:
					if vecID, err := strconv.Atoi(args[3]); err == nil {
						vec, err := index.Reconstruct(int64(vecID))
						if err != nil {
							return fmt.Errorf("error while reconstructing vector with ID %v, err: %v", vecID, err)
						}
						fmt.Printf("the reconstructed vector with ID %v is %v\n", vecID, vec)
					}
				default:
					return fmt.Errorf("not enough args")

				}
			}

			fieldID++
			pos += 8
		}
		return nil
	},
}

func decodeSection(data []byte, start uint64) (int, int, map[int64]uint64, *faiss.IndexImpl, error) {
	pos := int(start)
	vecDocIDMap := make(map[int64]uint64)

	// the below loop loads the following:
	// 1. doc values(first 2 iterations) - adhering to the sections format. never
	// valid values for vector section
	// 2. index optimization type.
	for i := 0; i < 3; i++ {
		_, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += n
	}

	// todo: not a good idea to cache the vector index perhaps, since it could be quite huge.
	indexSize, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += n
	indexBytes := data[pos : pos+int(indexSize)]
	pos += int(indexSize)

	// read the number vectors indexed for this field and load the vector to docID mapping.
	// todo: cache the vecID to docIDs mapping for a fieldID
	numVecs, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
	pos += n
	for i := 0; i < int(numVecs); i++ {
		vecID, n := binary.Varint(data[pos : pos+binary.MaxVarintLen64])
		pos += n

		docID, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += n
		vecDocIDMap[vecID] = docID
	}

	vecIndex, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return int(numVecs), int(indexSize), vecDocIDMap, vecIndex, nil
}

func init() {
	RootCmd.AddCommand(vectorCmd)
}
