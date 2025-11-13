//  Copyright (c) 2025 Couchbase, Inc.
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

	"github.com/spf13/cobra"
)

// edgeListCmd represents the edge command
var edgeListCmd = &cobra.Command{
	Use:   "edgeList",
	Short: "prints the edge list for nested documents",
	Long:  `The edgeList command will print the edge list for nested documents in the segment.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		edgeList := segment.EdgeList()
		if len(edgeList) == 0 {
			fmt.Println("no nested documents present")
			return nil
		}
		// print number of edges / nested documents
		fmt.Printf("number of edges / nested documents: %d\n", len(edgeList))
		fmt.Printf("Child Document Number -> Parent Document Number\n")
		for child, parent := range edgeList {
			fmt.Printf("%d -> %d\n", child, parent)
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(edgeListCmd)
}
