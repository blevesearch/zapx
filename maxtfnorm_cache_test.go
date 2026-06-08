// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"os"
	"testing"
)

// TestMaxTFNormCachePopulated verifies §1: after the first MaxTFNorm call the
// result is stored in invertedCacheEntry.maxTFNormCache so that subsequent
// calls for the same (term, avgDocLen) are pure O(1) map lookups.
func TestMaxTFNormCachePopulated(t *testing.T) {
	tmpPath := getTempPath("scorch_maxtfnorm_cache.zap")
	_ = os.RemoveAll(tmpPath)
	defer os.RemoveAll(tmpPath)

	testSeg, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	if err = PersistSegmentBase(testSeg, tmpPath); err != nil {
		t.Fatal(err)
	}
	seg, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	rawDict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}
	d := rawDict.(*Dictionary)
	const avgDocLen = 2.0

	// Before first call, the cache must not have an entry for "some".
	ce := d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
	if _, ok := ce.getMaxTFNorm("some", float32(avgDocLen)); ok {
		t.Fatal("maxTFNormCache already has 'some' before first call — unexpected")
	}

	v1 := d.MaxTFNorm([]byte("some"), avgDocLen)
	if v1 <= 0 {
		t.Fatalf("MaxTFNorm('some') = %f, want > 0", v1)
	}

	// After first call, the cache must be populated with the computed value.
	cached, ok := ce.getMaxTFNorm("some", float32(avgDocLen))
	if !ok {
		t.Fatal("maxTFNormCache not populated after first MaxTFNorm call")
	}
	if cached != v1 {
		t.Errorf("cached value %f ≠ returned value %f", cached, v1)
	}

	// Different avgDocLen must not collide with the cached entry.
	v2 := d.MaxTFNorm([]byte("some"), avgDocLen*2)
	if _, ok = ce.getMaxTFNorm("some", float32(avgDocLen*2)); !ok {
		t.Fatal("maxTFNormCache not populated for avgDocLen*2")
	}
	// The two cached values must differ (length normalisation is applied).
	if v1 == v2 {
		t.Errorf("expected different MaxTFNorm for different avgDocLen: both returned %f", v1)
	}
}
