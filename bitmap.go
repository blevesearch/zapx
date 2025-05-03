package zap

import (
	"github.com/RoaringBitmap/roaring"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	"io"
)

type bitmap roaring.Bitmap

func (*ZapPlugin) NewBitmap() segment.Bitmap {
	return (*bitmap)(roaring.NewBitmap())
}

func (b *bitmap) Add(v uint32) {
	(*roaring.Bitmap)(b).Add(v)
}

func (b *bitmap) AddMany(dat []uint32) {
	(*roaring.Bitmap)(b).AddMany(dat)
}

func (b *bitmap) AddRange(rangeStart, rangeEnd uint64) {
	(*roaring.Bitmap)(b).AddRange(rangeStart, rangeEnd)
}

func (b *bitmap) And(other segment.Bitmap) {
	(*roaring.Bitmap)(b).And((*roaring.Bitmap)(other.(*bitmap)))
}

func (b *bitmap) AndNot(other segment.Bitmap) {
	(*roaring.Bitmap)(b).AndNot((*roaring.Bitmap)(other.(*bitmap)))
}

func (b *bitmap) Clone() segment.Bitmap{
	return (*bitmap)((*roaring.Bitmap)(b).Clone())
}

func (b *bitmap) Contains(v uint32) bool {
	return (*roaring.Bitmap)(b).Contains(v)
}

func (b *bitmap) GetCardinality() uint64 {
	return (*roaring.Bitmap)(b).GetCardinality()
}

func (b *bitmap) GetSizeInBytes() uint64 {
	return (*roaring.Bitmap)(b).GetSizeInBytes()
}

func (b *bitmap) IsEmpty() bool {
	return (*roaring.Bitmap)(b).IsEmpty()
}

func (b *bitmap) Iterator() segment.IntPeekable {
	return (*roaring.Bitmap)(b).Iterator()
}

func (b *bitmap) Or(other segment.Bitmap) {
	(*roaring.Bitmap)(b).Or((*roaring.Bitmap)(other.(*bitmap)))
}

func (b *bitmap) ReadFrom(reader io.Reader) (p int64, err error) {
	return (*roaring.Bitmap)(b).ReadFrom(reader)
}

func (b *bitmap) WriteTo(stream io.Writer) (int64, error) {
	return (*roaring.Bitmap)(b).WriteTo(stream)
}

func (b *bitmap) OrNew(other segment.Bitmap) segment.Bitmap {
	return (*bitmap)(roaring.Or((*roaring.Bitmap)(b), (*roaring.Bitmap)(other.(*bitmap))))
}

func (b *bitmap) AndNew(other segment.Bitmap)  segment.Bitmap {
	return (*bitmap)(roaring.And((*roaring.Bitmap)(b), (*roaring.Bitmap)(other.(*bitmap))))
}

func (b *bitmap) AndNotNew(other segment.Bitmap) segment.Bitmap {
	return (*bitmap)(roaring.AndNot((*roaring.Bitmap)(b), (*roaring.Bitmap)(other.(*bitmap))))
}

func (b *bitmap) HeapOrNew(bitmaps ...segment.Bitmap) segment.Bitmap {
	arg := make([]*roaring.Bitmap, len(bitmaps)+1)
	arg[0] = (*roaring.Bitmap)(b)
	for i, bm := range bitmaps {
		arg[i+1] = (*roaring.Bitmap)(bm.(*bitmap))
	}
	return (*bitmap)(roaring.HeapOr(arg...))
}