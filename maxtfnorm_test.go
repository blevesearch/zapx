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
	"math"
	"os"
	"testing"
)

// TestMaxTFNormSidecarAndScan verifies that Dictionary.MaxTFNorm() returns a
// positive value for a known multi-doc term and 0 for an absent term.  The
// §14 sidecar section (if present) and the scan fallback must agree.
func TestMaxTFNormCorrectness(t *testing.T) {
	tmpPath := getTempPath("scorch_maxtfnorm.zap")
	_ = os.RemoveAll(tmpPath)
	defer os.RemoveAll(tmpPath)

	// buildTestSegmentMulti creates two docs (a, b) each with desc="some thing"
	// and other fields.  "some" and "thing" both appear in exactly 2 docs,
	// each with freq=1 and fieldLen=2 (two-token field).
	testSeg, _, err := buildTestSegmentMulti()
	if err != nil {
		t.Fatal(err)
	}
	err = PersistSegmentBase(testSeg, tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	seg, err := zapPlugin.Open(tmpPath)
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer seg.Close()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	// avgDocLength = 2 (both docs have 2-token desc field)
	const avgDocLen = 2.0

	// "some" is in 2 docs, each with freq=1 and fieldLen=2.
	got := dict.(*Dictionary).MaxTFNorm([]byte("some"), avgDocLen)
	if got <= 0 {
		t.Fatalf("MaxTFNorm('some') = %f, want > 0", got)
	}

	// The maximum possible BM25 tfNorm is k1/(1+k1) ≈ 0.545 (no length normalisation).
	maxPossible := float32(wandBM25K1 / (1 + wandBM25K1))
	if got > maxPossible*1.01 { // 1% tolerance for float rounding
		t.Errorf("MaxTFNorm('some') = %f exceeds theoretical max %f", got, maxPossible)
	}

	// Compute the expected value directly using wandTFNorm with SmallFloat-decoded fieldLen.
	// fieldLen=2 goes through encodeNormByte(2) → decode → approximateFieldLen.
	approxFieldLen := float64(normDecodeSmallFloat(encodeNormByte(2)))
	norm := 1.0 / math.Sqrt(approxFieldLen)
	expected := float32(wandTFNorm(1.0, norm, avgDocLen))
	tol := float32(0.001)
	if got < expected-tol || got > expected+tol {
		t.Errorf("MaxTFNorm('some') = %f, want ~%f (±%f)", got, expected, tol)
	}

	// Second call must return cached value (no rescan).
	got2 := dict.(*Dictionary).MaxTFNorm([]byte("some"), avgDocLen)
	if got2 != got {
		t.Errorf("cache: second MaxTFNorm call returned %f, first was %f", got2, got)
	}

	// Absent term must return 0.
	if v := dict.(*Dictionary).MaxTFNorm([]byte("zzz_absent"), avgDocLen); v != 0 {
		t.Errorf("MaxTFNorm for absent term = %f, want 0", v)
	}
}

// TestMaxTFNormZeroAvgDocLen verifies MaxTFNorm returns 0 for avgDocLen <= 0
// (guards against division-by-zero in the BM25 formula).
func TestMaxTFNormZeroAvgDocLen(t *testing.T) {
	tmpPath := getTempPath("scorch_maxtfnorm_zero.zap")
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
		t.Fatalf("error opening segment: %v", err)
	}
	defer seg.Close()

	dict, err := seg.Dictionary("desc")
	if err != nil {
		t.Fatal(err)
	}

	if v := dict.(*Dictionary).MaxTFNorm([]byte("some"), 0); v != 0 {
		t.Errorf("MaxTFNorm with avgDocLen=0 = %f, want 0", v)
	}
	if v := dict.(*Dictionary).MaxTFNorm([]byte("some"), -1); v != 0 {
		t.Errorf("MaxTFNorm with avgDocLen=-1 = %f, want 0", v)
	}
}
