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

package zap

type ewma struct {
	alpha float64
	avg   float64
	// every hit to the cache entry is recorded as part of a sample
	// which will be used to calculate the average in the next cycle of average
	// computation (which is average traffic for the field till now). this is
	// used to track the per second hits to the cache entries.
	sample uint64
}

func (e *ewma) add(val uint64) {
	if e.avg == 0.0 {
		e.avg = float64(val)
	} else {
		// the exponentially weighted moving average
		// X(t) = a.v + (1 - a).X(t-1)
		e.avg = e.alpha*float64(val) + (1-e.alpha)*e.avg
	}
}
