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

// TestMaxTFNormUncachedIsStable verifies the two properties that let §14's
// sidecar replace the old per-term maxTFNorm memo outright:
//
//  1. Repeated MaxTFNorm calls return bit-identical values. The bound is
//     recomputed from the mmap'd sidecar every call, with no memo to make
//     successive calls agree, so determinism has to come from the data.
//
//  2. The bound tracks avgDocLength. The sidecar stores raw (maxFreq,
//     minFieldLen) rather than a precomputed float precisely so any
//     avgDocLength can be served — which is why no avgDocLength-keyed cache
//     (and no invalidation when the corpus grows) is needed.
func TestMaxTFNormUncachedIsStable(t *testing.T) {
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

	v1 := d.MaxTFNorm([]byte("some"), avgDocLen)
	if v1 <= 0 {
		t.Fatalf("MaxTFNorm('some') = %f, want > 0", v1)
	}

	// Property 1: stable across repeats, including a cold first call followed
	// by warm ones. Any drift here means the bound depends on hidden state.
	for i := 0; i < 5; i++ {
		if got := d.MaxTFNorm([]byte("some"), avgDocLen); got != v1 {
			t.Fatalf("call %d returned %f, first call returned %f — "+
				"uncached MaxTFNorm must be deterministic", i+2, got, v1)
		}
	}

	// Property 2: a different avgDocLength yields a different bound, served
	// from the same sidecar entry with no invalidation step.
	v2 := d.MaxTFNorm([]byte("some"), avgDocLen*2)
	if v2 <= 0 {
		t.Fatalf("MaxTFNorm('some', avgDocLen*2) = %f, want > 0", v2)
	}
	if v1 == v2 {
		t.Errorf("expected different MaxTFNorm for different avgDocLen: both returned %f", v1)
	}

	// A longer avgDocLength means less length penalty, so the bound must rise.
	if v2 <= v1 {
		t.Errorf("MaxTFNorm should increase with avgDocLength: %f (avgDl=%v) → %f (avgDl=%v)",
			v1, avgDocLen, v2, avgDocLen*2)
	}

	// Interleaving the two avgDocLengths must not perturb either result —
	// there is no shared per-term slot for them to contend over.
	if got := d.MaxTFNorm([]byte("some"), avgDocLen); got != v1 {
		t.Errorf("avgDocLen=%v after avgDocLen=%v returned %f, want %f",
			avgDocLen, avgDocLen*2, got, v1)
	}
	if got := d.MaxTFNorm([]byte("some"), avgDocLen*2); got != v2 {
		t.Errorf("avgDocLen=%v re-query returned %f, want %f", avgDocLen*2, got, v2)
	}

	// Absent term must still return 0.
	if v := d.MaxTFNorm([]byte("zzz_absent"), avgDocLen); v != 0 {
		t.Errorf("MaxTFNorm for absent term = %f, want 0", v)
	}
}
