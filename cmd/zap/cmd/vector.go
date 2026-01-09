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

	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/go-faiss"
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
		vector index's config - type, dims, similarity metric, optimization type.
	3. list all vectorIDs in the index, and the corresponding document IDs.
	4. reconstruct vector given the vectorID.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("must specify field")
		}
		pos := segment.SectionsIndexOffset()
		if pos == 0 {
			// this is the case only for older file formats
			return fmt.Errorf("file format not supported")
		}
		// get the vector section address for the specified field
		pos, err := segment.VectorAddr(args[1])
		if err != nil {
			return fmt.Errorf("error determining address: %v", err)
		}
		data := segment.Data()
		// read the vector optimization option type
		vecOpt, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		// read the number of vectors indexed
		numVecs, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		// read the length of the vector to docID map (unused for now)
		_, n = binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		// read the vector to docID map
		vecToDocID := make([]uint64, numVecs)
		for vecID := 0; vecID < int(numVecs); vecID++ {
			docID, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			pos += uint64(n)
			vecToDocID[vecID] = docID
		}
		// read the type of the vector index (unused for now)
		_, n = binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		// read the length of the serialized faiss index
		indexSize, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		// read the serialized faiss index bytes
		indexBytes := data[pos : pos+indexSize]
		pos += indexSize
		// construct the faiss index from the buffer
		vecIndex, err := faiss.ReadIndexFromBuffer(indexBytes, faiss.IOFlagReadOnly)
		if err != nil {
			return fmt.Errorf("error reading faiss index from buffer: %v", err)
		}
		metricType := vecIndex.MetricType()
		dims := vecIndex.D()
		optimizationType := index.VectorIndexOptimizationsReverseLookup[int(vecOpt)]

		switch len(args) {
		case 2:
			metrics := map[int]string{
				faiss.MetricL2:           index.EuclideanDistance,
				faiss.MetricInnerProduct: index.InnerProduct,
			}
			fmt.Printf("decoded vector section content for field `%v`:\n", args[1])
			fmt.Printf("  number of vectors: %v\n", numVecs)
			fmt.Printf("  size of the serialized vector index: %v\n", indexSize)
			fmt.Printf("  dimensionality of vectors in the index: %v\n", dims)
			fmt.Printf("  similarity metric used: %v\n", metrics[metricType])
			fmt.Printf("  index optimized for: %v\n", optimizationType)
		case 3:
			if args[2] == "list" {
				fmt.Printf("listing the vector IDs in the index\n")
				for vecID, doc := range vecToDocID {
					fmt.Printf("vector with vecID: %v present in doc: %v\n", vecID, doc)
				}
			}
		case 4:
			vecID, err := strconv.Atoi(args[3])
			if err != nil {
				return fmt.Errorf("error parsing vecID: %v", err)
			}
			vec, err := vecIndex.Reconstruct(int64(vecID))
			if err != nil {
				return fmt.Errorf("error while reconstructing vector with ID %v, err: %v", vecID, err)
			}
			fmt.Printf("the reconstructed vector with ID %v is %v\n", vecID, vec)
		default:
			return fmt.Errorf("not enough args")
		}
		return nil
	},
}

func init() {
	RootCmd.AddCommand(vectorCmd)
}
