package zap

import (
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	"reflect"
	"sort"
	"unsafe"
)

func (sb *SegmentBase) InRange(field string, min, max float64, inclusiveMin, inclusiveMax bool, except *roaring.Bitmap) (segment.PostingsList, error) {

	rv := &PostingsList{
		sb: sb,
		except: except,
		postings: roaring.New(),
	}

	fieldIDPlus1 := sb.fieldsMap[field]
	if fieldIDPlus1 > 0 {
		fieldNumericSection := sb.fieldsSectionsMap[fieldIDPlus1-1][sectionNumericRangeIndex]
		if fieldNumericSection > 0 {

			// read how many values are in this field
			numNumericValues, sz := binary.Uvarint(sb.mem[fieldNumericSection:fieldNumericSection+binary.MaxVarintLen64])
			pos := fieldNumericSection+uint64(sz)
			floatData, err := ByteSliceToFloat6464Slice(sb.mem[pos:pos+(numNumericValues*8)])
			if err != nil {
				return nil, err
			}

			floatOffsets := pos + (numNumericValues*8)
			floatOffsetsSlice, err := ByteSliceToUint64Slice(sb.mem[floatOffsets:floatOffsets+(numNumericValues*8)])
			if err != nil {
				return nil, err
			}

			floatIdx := sort.SearchFloat64s(floatData, min)
			for uint64(floatIdx) < numNumericValues {
				floatVal := floatData[floatIdx]

				// handle edge case at start
				if floatVal == min && !inclusiveMin {
					floatIdx++
					continue
				}

				// handle end
				if floatVal == max && !inclusiveMax || floatVal > max {
					break
				}

				// this value is in the desired range, load it's bitset
				bitSetLenStart := floatOffsetsSlice[floatIdx]
				bitSetLen, read := binary.Uvarint(sb.mem[bitSetLenStart : bitSetLenStart+binary.MaxVarintLen64])
				bitSetStart := bitSetLenStart + uint64(read)

				roaringBytes := sb.mem[bitSetStart : bitSetStart+bitSetLen]

				postings := roaring.New()
				_, err := postings.FromBuffer(roaringBytes)
				if err != nil {
					return nil, fmt.Errorf("error loading roaring bitmap: %v", err)
				}

				// or this bitset into the result
				rv.postings.Or(postings)

				floatIdx++
			}
		}
	}

	return rv, nil
}

func ByteSliceToFloat6464Slice(in []byte) ([]float64, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))
	var out []float64
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len / 8
	outHeader.Cap = outHeader.Len

	return out, nil
}

func ByteSliceToUint64Slice(in []byte) ([]uint64, error) {
	inHeader := (*reflect.SliceHeader)(unsafe.Pointer(&in))

	var out []uint64
	outHeader := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	outHeader.Data = inHeader.Data
	outHeader.Len = inHeader.Len / 8
	outHeader.Cap = outHeader.Len

	return out, nil
}
