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

package cmd

import (
	"encoding/binary"
	"fmt"

	"github.com/spf13/cobra"
)

// edgeListCmd represents the edge command
var edgeListCmd = &cobra.Command{
	Use:   "edgeList",
	Short: "prints the edge list for nested documents",
	Long:  `The edgeList command will print the edge list for nested documents in the segment.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		edgeListAddr, err := segment.EdgeListAddr()
		if err != nil {
			return fmt.Errorf("error getting edge list: %v", err)
		}
		if edgeListAddr == 0 {
			fmt.Println("no edge list present")
			return nil
		}
		data := segment.Data()
		// read edge list
		// pos stores the current read position
		pos := edgeListAddr
		// read number of nested documents which is also the number of edges
		numEdges, read := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		if read <= 0 {
			return fmt.Errorf("error reading number of edges in nested edge list")
		}
		pos += uint64(read)
		// if no edges or no nested documents, return
		if numEdges == 0 {
			fmt.Println("no nested documents present")
			return nil
		}
		// edgeList as a map[node]parent
		edgeList := make(map[uint64]uint64, numEdges)
		for i := uint64(0); i < numEdges; i++ {
			child, read := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			if read <= 0 {
				return fmt.Errorf("error reading child doc id in nested edge list")
			}
			pos += uint64(read)
			parent, read := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			if read <= 0 {
				return fmt.Errorf("error reading parent doc id in nested edge list")
			}
			pos += uint64(read)
			edgeList[child] = parent
		}
		// print number of edges / nested documents
		fmt.Printf("number of edges / nested documents: %d\n", len(edgeList))
		fmt.Printf("child document number -> parent document number\n")
		for child, parent := range edgeList {
			fmt.Printf("%d -> %d\n", child, parent)
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(edgeListCmd)
}
