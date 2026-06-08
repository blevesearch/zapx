//  Copyright (c) 2018 Couchbase, Inc.
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

import "github.com/blevesearch/vellum"

var maxItrsPerBasicEnum = 64

type enumerator interface {
	Current() ([]byte, int, uint64)
	GetLowIdxsAndValues() ([]int, []uint64)
	Next() error
	Close() error
}

func newEnumerator(itrs []vellum.Iterator) (enumerator, error) {
	if len(itrs) < maxItrsPerBasicEnum {
		return newBasicEnumerator(itrs, 0)
	} else {
		return newMergeEnumerator(itrs)
	}
}
