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

	"github.com/blevesearch/vellum"
	zap "github.com/blevesearch/zapx/v18"
	"github.com/spf13/cobra"
)

const termNotEncoded = 0

// exploreCmd represents the explore command
var exploreCmd = &cobra.Command{
	Use:   "explore [path] [field] <term> <docNum>",
	Short: "explores the index by field, then term (optional), and then docNum (optional)",
	Long:  `The explore command lets you explore the index in order of field, then optionally by term, then optionally again by doc number.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("must specify field")
		}

		data := segment.Data()

		addr, err := segment.DictAddr(args[1])
		if err != nil {
			return fmt.Errorf("error determining address: %v", err)
		}
		fmt.Printf("dictionary for field starts at %d (%x)\n", addr, addr)

		vellumLen, read := binary.Uvarint(data[addr : addr+binary.MaxVarintLen64])
		fmt.Printf("vellum length: %d\n", vellumLen)
		fstBytes := data[addr+uint64(read) : addr+uint64(read)+vellumLen]
		fmt.Printf("raw vellum data:\n % x\n", fstBytes)

		if len(args) >= 3 {
			if fstBytes != nil {
				fst, err := vellum.Load(fstBytes)
				if err != nil {
					return fmt.Errorf("dictionary field %s vellum err: %v", args[1], err)
				}
				postingsAddr, exists, err := fst.Get([]byte(args[2]))
				if err != nil {
					return fmt.Errorf("error looking for term : %v", err)
				}
				if exists {
					fmt.Printf("FST val is %d (%x)\n", postingsAddr, postingsAddr)

					info, err := zap.DescribeTermPostings(data, postingsAddr)
					if err != nil {
						return err
					}
					if info.OneHit {
						fmt.Printf("Posting List is 1-hit encoded, docNum: %d\n", info.DocNum)
						return nil
					}

					fmt.Printf("docFreq: %d hasFreqs: %v hasLocs: %v\n",
						info.DocFreq, info.HasFreqs, info.HasLocs)
					fmt.Printf("payload: %d bytes at %d (%x), %d full blocks + %d tail docs\n",
						info.PayloadLen, info.PayloadStart, info.PayloadStart,
						info.NumBlocks, info.TailLen)
					fmt.Printf("skip: %d bytes at %d (%x)\n",
						info.SkipLen, info.SkipStart, info.SkipStart)

					if info.LocsLen > 0 {
						locAddr := info.LocsStart
						fmt.Printf("Loc details at: %d (%x), %d bytes, chunk size %d\n",
							locAddr, locAddr, info.LocsLen, info.LocChunkSize)
						numLChunks, r4 := binary.Uvarint(data[locAddr : locAddr+binary.MaxVarintLen64])
						n := uint64(r4)
						fmt.Printf("there are %d loc chunks\n", numLChunks)

						var locOffsets []uint64
						for j := uint64(0); j < numLChunks; j++ {
							lchunkLen, r4 := binary.Uvarint(data[locAddr+n : locAddr+n+binary.MaxVarintLen64])
							n += uint64(r4)
							locOffsets = append(locOffsets, lchunkLen)
						}

						running2 := locAddr + n
						for k, offset := range locOffsets {
							fmt.Printf("loc chunk: %d, len %d(%x), start at %d (%x) end %d (%x)\n", k, offset, offset, running2, running2, running2+offset, running2+offset)
							running2 += offset
						}
					}

				} else {
					fmt.Printf("dictionary does not contain term '%s'\n", args[2])
				}
			}
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(exploreCmd)
}
