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

package zap

import (
	"bytes"

	"github.com/blevesearch/vellum"
)

type mergeEnumerator struct {
	enums      []enumerator
	enumChecks []bool

	lowK    []byte
	lowIdxs []int
	lowCurr int

	values []uint64
}

func newMergeEnumerator(itrs []vellum.Iterator) (enumerator, error) {

	numEnums := len(itrs)/maxItrsPerBasicEnum + 1
	rv := &mergeEnumerator{
		enums:      make([]enumerator, numEnums),
		enumChecks: make([]bool, numEnums),
		lowIdxs:    make([]int, 0, len(itrs)),
		values:     make([]uint64, len(itrs)),
	}

	var err error
	var start, end int
	for i := 0; i < numEnums; i++ {
		start = i * maxItrsPerBasicEnum
		end = (i + 1) * maxItrsPerBasicEnum
		if end > len(itrs) {
			end = len(itrs)
		}
		rv.enums[i], err = newBasicEnumerator(itrs[start:end], i)
		if err != nil {
			return nil, err
		}
	}

	rv.updateMatches(false)
	if rv.lowK == nil && len(rv.lowIdxs) == 0 {
		return rv, vellum.ErrIteratorDone
	}
	return rv, nil
}

func (m *mergeEnumerator) updateMatches(skipEmptyKey bool) {
	m.lowK = nil
	m.lowIdxs = m.lowIdxs[:0]
	m.lowCurr = 0
	var lowIdxs []int

	for _, enum := range m.enums {
		key, _, v := enum.Current()
		if (key == nil && v == 0) || (len(key) == 0 && skipEmptyKey) {
			continue
		}
		cmp := bytes.Compare(key, m.lowK)
		if cmp <= 0 || len(m.lowIdxs) == 0 {
			lowIdxs, _ = enum.GetLowIdxsAndValues()
		}
		if cmp < 0 || len(m.lowIdxs) == 0 {
			m.lowK = key
			m.lowIdxs = m.lowIdxs[:0]
			m.lowIdxs = append(m.lowIdxs, lowIdxs...)
		} else if cmp == 0 {
			m.lowIdxs = append(m.lowIdxs, lowIdxs...)
		}
	}
}

func (m *mergeEnumerator) Current() ([]byte, int, uint64) {
	var i, j int
	if m.lowCurr < len(m.lowIdxs) {
		i = m.lowIdxs[m.lowCurr]
		j = i / maxItrsPerBasicEnum
		return m.enums[j].Current()
	}
	return nil, 0, 0
}

func (m *mergeEnumerator) resetEnumChecks() {
	for i := range m.enumChecks {
		m.enumChecks[i] = false
	}
}

func (m *mergeEnumerator) GetLowIdxsAndValues() ([]int, []uint64) {
	m.values = m.values[:0]
	m.resetEnumChecks()

	for _, idx := range m.lowIdxs {
		i := idx / maxItrsPerBasicEnum

		if !m.enumChecks[i] {
			_, values := m.enums[i].GetLowIdxsAndValues()
			m.values = append(m.values, values...)
			m.enumChecks[i] = true
		}
	}
	return m.lowIdxs, m.values
}

func (m *mergeEnumerator) Next() error {
	m.lowCurr += 1
	if m.lowCurr >= len(m.lowIdxs) {
		m.resetEnumChecks()
		for _, idx := range m.lowIdxs {
			i := idx / maxItrsPerBasicEnum
			m.enumChecks[i] = true
		}

		for i, update := range m.enumChecks {
			if update {
				err := m.enums[i].Next()
				if err != nil && err != vellum.ErrIteratorDone {
					return err
				}
			}
		}
		m.updateMatches(true)
	}
	if m.lowK == nil && len(m.lowIdxs) == 0 {
		return vellum.ErrIteratorDone
	}
	return nil
}

func (m *mergeEnumerator) Close() error {
	var rv error
	for _, enum := range m.enums {
		err := enum.Close()
		if rv == nil {
			rv = err
		}
	}
	return rv
}
