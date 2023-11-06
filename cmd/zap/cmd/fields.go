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
	"encoding/binary"
	"fmt"

	"github.com/spf13/cobra"
)

// fieldsCmd represents the fields command
var fieldsCmd = &cobra.Command{
	Use:   "fields [path]",
	Short: "fields prints the fields in the specified file",
	Long:  `The fields command lets you print the fields in the specified file.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("must specify index file path")
		}

		data := segment.Data()

		// iterate through fields index
		var fieldInv []string
		pos := segment.FieldsIndexOffset()
		if pos == 0 {
			// this is the case only for older file formats
			return fmt.Errorf("file format not supported?")
		}

		// read the number of fields
		numFields, sz := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(sz)
		var fieldID uint64
		var err error
		var fieldSectionMap []map[uint16]uint64

		for fieldID < numFields {
			sectionMap := make(map[uint16]uint64)
			addr := binary.BigEndian.Uint64(data[pos : pos+8])

			fieldInv, err = loadFieldData(data, addr, fieldID, sectionMap, fieldInv)
			if err != nil {
				return err
			}
			fieldSectionMap = append(fieldSectionMap, sectionMap)
			fieldID++
			pos += 8
		}

		for fieldID, field := range fieldInv {
			for sectionType, sectionAddr := range fieldSectionMap[fieldID] {
				if sectionAddr > 0 {
					switch sectionType {
					case sectionInvertedTextIndex:
						fmt.Printf("field %d '%s' text index starts at %d (%x)\n", fieldID, field, sectionAddr, sectionAddr)
					case sectionFaissVectorIndex:
						fmt.Printf("field %d '%s' vector index starts at %d (%x)\n", fieldID, field, sectionAddr, sectionAddr)
					}
				}
			}
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(fieldsCmd)
}
