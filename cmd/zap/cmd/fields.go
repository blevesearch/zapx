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
	"fmt"

	zap "github.com/blevesearch/zapx/v16"
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
		pos := segment.SectionsIndexOffset()
		if pos == 0 {
			// this is the case only for older file formats
			return fmt.Errorf("file format not supported")
		}

		fieldInv, fieldSectionMap, err := getSectionFields(data, pos)
		if err != nil {
			return fmt.Errorf("error while getting the sections and field info %v", err)
		}
		for fieldID, field := range fieldInv {
			for sectionType, sectionAddr := range fieldSectionMap[fieldID] {
				if sectionAddr > 0 {
					switch sectionType {
					case zap.SectionInvertedTextIndex:
						fmt.Printf("field %d '%s' text index starts at %d (%x)\n", fieldID, field, sectionAddr, sectionAddr)
					case zap.SectionFaissVectorIndex:
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
