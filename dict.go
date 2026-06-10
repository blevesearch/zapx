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

package zap

import (
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	"github.com/blevesearch/vellum"
)

// BM25 parameters — must match the values in bleve/search/util.go.
const (
	wandBM25K1 = 1.2
	wandBM25B  = 0.75
)

// Dictionary is the zap representation of the term dictionary
type Dictionary struct {
	sb      *SegmentBase
	field   string
	fieldID uint16
	fst     *vellum.FST

	fstReader *vellum.Reader

	bytesRead uint64
}

// represents an immutable, empty dictionary
var emptyDictionary = &Dictionary{}

func (d *Dictionary) Cardinality() int {
	if d.fst != nil {
		return d.fst.Len()
	}
	return 0
}

// PostingsList returns the postings list for the specified term
func (d *Dictionary) PostingsList(term []byte, except *roaring.Bitmap,
	prealloc segment.PostingsList) (segment.PostingsList, error) {
	var preallocPL *PostingsList
	pl, ok := prealloc.(*PostingsList)
	if ok && pl != nil {
		preallocPL = pl
	}
	return d.postingsList(term, except, preallocPL)
}

func (d *Dictionary) postingsList(term []byte, except *roaring.Bitmap, rv *PostingsList) (*PostingsList, error) {
	if d.fstReader == nil {
		if rv == nil || rv == emptyPostingsList {
			return emptyPostingsList, nil
		}
		return d.postingsListInit(rv, except), nil
	}

	// Fast path: avoid FST traversal for terms seen in a previous query.
	var ce *invertedCacheEntry
	if d.sb != nil {
		ce = d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
		if v, ok := ce.termOffsetCache.Load(string(term)); ok {
			return d.postingsListFromOffset(v.(uint64), except, rv)
		}
	}

	postingsOffset, exists, err := d.fstReader.Get(term)

	if err != nil {
		return nil, fmt.Errorf("vellum err: %v", err)
	}
	if !exists {
		if rv == nil || rv == emptyPostingsList {
			return emptyPostingsList, nil
		}
		return d.postingsListInit(rv, except), nil
	}

	if ce != nil {
		ce.termOffsetCache.Store(string(term), postingsOffset)
	}

	return d.postingsListFromOffset(postingsOffset, except, rv)
}

func (d *Dictionary) postingsListFromOffset(postingsOffset uint64, except *roaring.Bitmap, rv *PostingsList) (*PostingsList, error) {
	rv = d.postingsListInit(rv, except)

	err := rv.read(postingsOffset, d)
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (d *Dictionary) postingsListInit(rv *PostingsList, except *roaring.Bitmap) *PostingsList {
	if rv == nil || rv == emptyPostingsList {
		rv = &PostingsList{}
	} else {
		postings := rv.postings
		if postings != nil {
			postings.Clear()
		}

		*rv = PostingsList{} // clear the struct

		rv.postings = postings
	}
	rv.sb = d.sb
	rv.fieldID = d.fieldID
	rv.except = except
	return rv
}

// MaxTFNorm returns the maximum BM25 tf-norm contribution for term in this
// segment, scanning the posting list lazily on the first call and caching
// the result per (term, avgDocLength).
//
// The returned value is:
//
//	max_d( sqrt(freq_d) × k1 / (sqrt(freq_d) + k1×(1−b + b×fl_d/avgDocLength)) )
//
// where fl_d = 1/(norm_d²) is the document field length.
//
// Multiply the result by scorer.IDF() × scorer.QueryWeight() to get the
// per-term score upper bound used by WAND / MaxScore pruning.
//
// Returns 0 if the term is not in this segment.
func (d *Dictionary) MaxTFNorm(term []byte, avgDocLength float64) float32 {
	if d.sb == nil || avgDocLength <= 0 {
		return 0
	}
	cacheEntry := d.sb.invIndexCache.getOrCreateMaxTFNormEntry(d.fieldID)
	avgDocLenF := float32(avgDocLength)

	if v, ok := cacheEntry.getMaxTFNorm(string(term), avgDocLenF); ok {
		return v
	}

	// §14 early-out: use precomputed sidecar if available.
	// termOffsetCache is populated by postingsList() which is called below,
	// but if MaxTFNorm is called after a prior PostingsList lookup it may already
	// be warm.  We check before the scan and again after PostingsList returns.
	if v, loaded := cacheEntry.termOffsetCache.Load(string(term)); loaded {
		postingsOff := v.(uint64)
		if postingsOff&FSTValEncodingMask == FSTValEncodingGeneral {
			if maxFreq, maxNorm, found := d.sb.lookupMaxTFNorm(d.fieldID, postingsOff); found {
				result := float32(wandTFNorm(float64(maxFreq), float64(maxNorm), avgDocLength))
				cacheEntry.setMaxTFNorm(string(term), avgDocLenF, result)
				return result
			}
		}
	}

	// Cache miss: scan the posting list to find max(tfNorm).
	pl, err := d.PostingsList(term, nil, nil)
	if err != nil || pl == emptyPostingsList {
		return 0
	}

	// §14 second early-out: PostingsList() just populated termOffsetCache.
	// If the term uses general encoding, check the precomputed sidecar now.
	if cpl, ok := pl.(*PostingsList); ok && cpl.normBits1Hit == 0 {
		if v, loaded := cacheEntry.termOffsetCache.Load(string(term)); loaded {
			postingsOff := v.(uint64)
			if postingsOff&FSTValEncodingMask == FSTValEncodingGeneral {
				if maxFreq, maxNorm, found := d.sb.lookupMaxTFNorm(d.fieldID, postingsOff); found {
					result := float32(wandTFNorm(float64(maxFreq), float64(maxNorm), avgDocLength))
					cacheEntry.setMaxTFNorm(string(term), avgDocLenF, result)
					return result
				}
			}
		}
	}

	var maxTFNorm float32

	// 1-hit optimisation: only one doc, freq=1, norm stored in FST value.
	if cpl, ok := pl.(*PostingsList); ok && cpl.normBits1Hit != 0 {
		// v18: normBits1Hit stores fieldLen (NormBits1Hit = 1 → fieldLen=1).
		// Norm = 1/sqrt(fieldLen); for fieldLen=1 this gives norm=1.0,
		// which is the correct conservative upper bound.
		norm := float32(1.0 / math.Sqrt(float64(cpl.normBits1Hit)))
		maxTFNorm = float32(wandTFNorm(1, float64(norm), avgDocLength))
	} else {
		iter := pl.Iterator(true, true, false, nil)
		for {
			p, e := iter.Next()
			if p == nil || e != nil {
				break
			}
			v := float32(wandTFNorm(float64(p.Frequency()), p.Norm(), avgDocLength))
			if v > maxTFNorm {
				maxTFNorm = v
			}
		}
	}

	cacheEntry.setMaxTFNorm(string(term), avgDocLenF, maxTFNorm)
	return maxTFNorm
}

// wandTFNorm computes the BM25 tf-norm contribution for a single document.
// freq is raw term frequency; norm is bleve's stored norm (= 1/sqrt(fieldLength)).
func wandTFNorm(freq, norm, avgDocLength float64) float64 {
	tf := math.Sqrt(freq)
	fieldLength := 1.0 / (norm * norm)
	denom := tf + wandBM25K1*(1-wandBM25B+wandBM25B*fieldLength/avgDocLength)
	if denom == 0 {
		return 0
	}
	return tf * wandBM25K1 / denom
}

func (d *Dictionary) Contains(key []byte) (bool, error) {
	if d.fst != nil {
		return d.fst.Contains(key)
	}
	return false, nil
}

// AutomatonIterator returns an iterator which only visits terms
// having the the vellum automaton and start/end key range
func (d *Dictionary) AutomatonIterator(a segment.Automaton,
	startKeyInclusive, endKeyExclusive []byte) segment.DictionaryIterator {
	if d.fst != nil {
		rv := &DictionaryIterator{
			d: d,
		}

		itr, err := d.fst.Search(a, startKeyInclusive, endKeyExclusive)
		if err == nil {
			rv.itr = itr
		} else if err != vellum.ErrIteratorDone {
			rv.err = err
		}

		return rv
	}
	return emptyDictionaryIterator
}

func (d *Dictionary) incrementBytesRead(val uint64) {
	d.bytesRead += val
}

func (d *Dictionary) BytesRead() uint64 {
	return d.bytesRead
}

func (d *Dictionary) ResetBytesRead(val uint64) {
	d.bytesRead = val
}

func (d *Dictionary) BytesWritten() uint64 {
	return 0
}

// DictionaryIterator is an iterator for term dictionary
type DictionaryIterator struct {
	d         *Dictionary
	itr       vellum.Iterator
	err       error
	tmp       PostingsList
	entry     index.DictEntry
	omitCount bool
}

var emptyDictionaryIterator = &DictionaryIterator{}

// Next returns the next entry in the dictionary
func (i *DictionaryIterator) Next() (*index.DictEntry, error) {
	if i.err != nil && i.err != vellum.ErrIteratorDone {
		return nil, i.err
	} else if i.itr == nil || i.err == vellum.ErrIteratorDone {
		return nil, nil
	}
	term, postingsOffset := i.itr.Current()
	if fitr, ok := i.itr.(vellum.FuzzyIterator); ok {
		i.entry.EditDistance = fitr.EditDistance()
	}
	i.entry.Term = string(term)
	if !i.omitCount {
		i.err = i.tmp.read(postingsOffset, i.d)
		if i.err != nil {
			return nil, i.err
		}
		i.entry.Count = i.tmp.Count()
	}
	i.err = i.itr.Next()
	return &i.entry, nil
}
