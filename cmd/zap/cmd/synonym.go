//  Copyright (c) 2024 Couchbase, Inc.
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

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/blevesearch/vellum"
	"github.com/spf13/cobra"
)

var thesaurusCmd = &cobra.Command{
	Use:   "thesaurus [path] [name]",
	Short: "thesaurus prints the thesaurus with the specified name",
	Long:  `The thesaurus command lets you print the thesaurus with the specified name.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		pos := segment.FieldsIndexOffset()
		if pos == 0 {
			// this is the case only for older file formats
			return fmt.Errorf("file format not supported")
		}
		if len(args) < 2 {
			return fmt.Errorf("must specify thesaurus name")
		}

		pos, err := segment.ThesaurusAddr(args[1])
		if err != nil {
			return fmt.Errorf("error determining address: %v", err)
		}
		fmt.Printf("thesaurus with name %s starts at %d (%x)\n", args[1], pos, pos)

		data := segment.Data()
		vellumLen, read := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(read)
		fmt.Printf("vellum length: %d\n", vellumLen)

		fstBytes := data[pos : pos+vellumLen]
		pos += vellumLen
		fst, err := vellum.Load(fstBytes)
		if err != nil {
			return fmt.Errorf("thesaurus name %s vellum err: %v", args[1], err)
		}
		fmt.Printf("raw vellum data:\n % x\n", fstBytes)

		numSyns, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
		pos += uint64(n)
		if numSyns == 0 {
			return fmt.Errorf("no synonyms found")
		}
		synTermMap := make(map[uint32][]byte, numSyns)
		for i := 0; i < int(numSyns); i++ {
			synID, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			pos += uint64(n)
			termLen, n := binary.Uvarint(data[pos : pos+binary.MaxVarintLen64])
			pos += uint64(n)
			if termLen == 0 {
				return fmt.Errorf("term length is 0")
			}
			term := data[pos : pos+uint64(termLen)]
			pos += uint64(termLen)
			synTermMap[uint32(synID)] = term
		}

		fmt.Printf("termID to term mapping:\n")
		fmt.Printf(" termID\tterm\n")
		for k, v := range synTermMap {
			fmt.Printf("  %d\t%s\n", k, string(v))
		}
		fmt.Printf("thesaurus (term -> [{termID|docNum},...]):\n")
		var totalTerms int
		itr, err := fst.Iterator(nil, nil)
		for err == nil {
			var sl *roaring64.Bitmap
			currTerm, currVal := itr.Current()
			sl, err = readSynonymsList(currVal, data)
			if err != nil {
				return err
			}
			sitr := sl.Iterator()
			printStr := fmt.Sprintf(" %s -> [", currTerm)
			for sitr.HasNext() {
				encodedVal := sitr.Next()
				tID, docNum := decodeSynonym(encodedVal)
				str := fmt.Sprintf("{%d|%d},", tID, docNum)
				printStr += str
			}
			printStr = printStr[:len(printStr)-1] + "]"
			fmt.Printf("%s\n", printStr)
			totalTerms++
			err = itr.Next()
		}
		fmt.Printf("Total terms in thesaurus : %d\n", totalTerms)
		if err != nil && err != vellum.ErrIteratorDone {
			return fmt.Errorf("error iterating thesaurus: %v", err)
		}
		return nil
	},
}

func readSynonymsList(postingsOffset uint64, data []byte) (*roaring64.Bitmap, error) {
	var n uint64
	var read int

	var postingsLen uint64
	postingsLen, read = binary.Uvarint(data[postingsOffset : postingsOffset+binary.MaxVarintLen64])
	n += uint64(read)

	buf := bytes.NewReader(data[postingsOffset+n : postingsOffset+n+postingsLen])
	r := roaring64.NewBitmap()

	_, err := r.ReadFrom(buf)
	if err != nil {
		return nil, fmt.Errorf("error loading roaring bitmap: %v", err)
	}

	return r, nil
}

func decodeSynonym(synonymCode uint64) (synonymID uint32, docID uint32) {
	return uint32(synonymCode >> 32), uint32(synonymCode)
}

func init() {
	RootCmd.AddCommand(thesaurusCmd)
}
