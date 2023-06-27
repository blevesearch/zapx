package zap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/vellum"
)

type section interface {
	Process(opaque map[int]resetable, docNum uint64, f index.Field, fieldID uint16)

	Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error)

	AddrForField(opaque map[int]resetable, fieldID int) int

	Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
		newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error

	InitOpaque(args map[string]interface{}) resetable
}

const (
	sectionInvertedIndex = iota
	sectionNumericRangeIndex
)

var segmentSections = map[uint16]section{
	sectionInvertedIndex:     &invertedIndexSection{},
	sectionNumericRangeIndex: &numericRangeIndexSection{},
}

type invertedIndexSection struct {
}

func (i *invertedIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {
	invIndexOpaque := i.getInvertedIndexOpaque(opaque)
	// no need to handle the ^uint16(0) case here, its handled in opaque?
	invIndexOpaque.process(field, fieldID, docNum)
}

func (i *invertedIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (n int64, err error) {

	invIndexOpaque := i.getInvertedIndexOpaque(opaque)
	_, _ = invIndexOpaque.writeDicts(w)
	return 0, nil
}

func (i *invertedIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	return 0
}

func (i *invertedIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {

	io := i.getInvertedIndexOpaque(opaque)
	fieldAddrs, _, err := mergeAndPersistInvertedSection(segments, drops, fieldsInv,
		io.FieldsMap, io.fieldsSame, newDocNumsIn, io.numDocs, io.chunkMode, w, closeCh)
	if err != nil {
		return err
	}

	io.fieldAddrs = fieldAddrs

	return nil
}

// todo: is it possible to merge this resuable stuff with the interim's tmp0?
func (i *invertedIndexOpaque) grabBuf(size int) []byte {
	buf := i.tmp0
	if cap(buf) < size {
		buf = make([]byte, size)
		i.tmp0 = buf
	}
	return buf[0:size]
}

func (io *invertedIndexOpaque) writeDicts(w *CountHashWriter) (dictOffsets []uint64, err error) {

	dictOffsets = make([]uint64, len(io.FieldsInv))

	fdvOffsetsStart := make([]uint64, len(io.FieldsInv))
	fdvOffsetsEnd := make([]uint64, len(io.FieldsInv))

	buf := io.grabBuf(binary.MaxVarintLen64)

	// these int coders are initialized with chunk size 1024
	// however this will be reset to the correct chunk size
	// while processing each individual field-term section
	tfEncoder := newChunkedIntCoder(1024, uint64(len(io.results)-1))
	locEncoder := newChunkedIntCoder(1024, uint64(len(io.results)-1))

	var docTermMap [][]byte

	if io.builder == nil {
		io.builder, err = vellum.New(&io.builderBuf, nil)
		if err != nil {
			return nil, err
		}
	}

	for fieldID, terms := range io.DictKeys {
		if cap(docTermMap) < len(io.results) {
			docTermMap = make([][]byte, len(io.results))
		} else {
			docTermMap = docTermMap[0:len(io.results)]
			for docNum := range docTermMap { // reset the docTermMap
				docTermMap[docNum] = docTermMap[docNum][:0]
			}
		}

		dict := io.Dicts[fieldID]

		for _, term := range terms { // terms are already sorted
			pid := dict[term] - 1

			postingsBS := io.Postings[pid]

			freqNorms := io.FreqNorms[pid]
			freqNormOffset := 0

			locs := io.Locs[pid]
			locOffset := 0

			chunkSize, err := getChunkSize(io.chunkMode, postingsBS.GetCardinality(), uint64(len(io.results)))
			if err != nil {
				return nil, err
			}
			tfEncoder.SetChunkSize(chunkSize, uint64(len(io.results)-1))
			locEncoder.SetChunkSize(chunkSize, uint64(len(io.results)-1))

			postingsItr := postingsBS.Iterator()
			for postingsItr.HasNext() {
				docNum := uint64(postingsItr.Next())

				freqNorm := freqNorms[freqNormOffset]

				// check if freq/norm is enabled
				if freqNorm.freq > 0 {
					err = tfEncoder.Add(docNum,
						encodeFreqHasLocs(freqNorm.freq, freqNorm.numLocs > 0),
						uint64(math.Float32bits(freqNorm.norm)))
				} else {
					// if disabled, then skip the norm part
					err = tfEncoder.Add(docNum,
						encodeFreqHasLocs(freqNorm.freq, freqNorm.numLocs > 0))
				}
				if err != nil {
					return nil, err
				}

				if freqNorm.numLocs > 0 {
					numBytesLocs := 0
					for _, loc := range locs[locOffset : locOffset+freqNorm.numLocs] {
						numBytesLocs += totalUvarintBytes(
							uint64(loc.fieldID), loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)), loc.arrayposs)
					}

					err = locEncoder.Add(docNum, uint64(numBytesLocs))
					if err != nil {
						return nil, err
					}
					for _, loc := range locs[locOffset : locOffset+freqNorm.numLocs] {
						err = locEncoder.Add(docNum,
							uint64(loc.fieldID), loc.pos, loc.start, loc.end,
							uint64(len(loc.arrayposs)))
						if err != nil {
							return nil, err
						}

						err = locEncoder.Add(docNum, loc.arrayposs...)
						if err != nil {
							return nil, err
						}
					}
					locOffset += freqNorm.numLocs
				}

				freqNormOffset++

				docTermMap[docNum] = append(
					append(docTermMap[docNum], term...),
					termSeparator)
			}

			tfEncoder.Close()
			locEncoder.Close()
			// io.incrementBytesWritten(locEncoder.getBytesWritten())

			postingsOffset, err :=
				writePostings(postingsBS, tfEncoder, locEncoder, nil, w, buf)
			if err != nil {
				return nil, err
			}

			if postingsOffset > uint64(0) {
				err = io.builder.Insert([]byte(term), postingsOffset)
				if err != nil {
					return nil, err
				}
			}

			tfEncoder.Reset()
			locEncoder.Reset()
		}

		err = io.builder.Close()
		if err != nil {
			return nil, err
		}

		// record where this dictionary starts
		dictOffsets[fieldID] = uint64(w.Count())

		vellumData := io.builderBuf.Bytes()

		// write out the length of the vellum data
		n := binary.PutUvarint(buf, uint64(len(vellumData)))
		_, err = w.Write(buf[:n])
		if err != nil {
			return nil, err
		}

		// write this vellum to disk
		_, err = w.Write(vellumData)
		if err != nil {
			return nil, err
		}

		// io.incrementBytesWritten(uint64(len(vellumData)))

		// reset vellum for reuse
		io.builderBuf.Reset()

		err = io.builder.Reset(&io.builderBuf)
		if err != nil {
			return nil, err
		}

		// write the field doc values
		// NOTE: doc values continue to use legacy chunk mode
		chunkSize, err := getChunkSize(LegacyChunkMode, 0, 0)
		if err != nil {
			return nil, err
		}

		fdvEncoder := newChunkedContentCoder(chunkSize, uint64(len(io.results)-1), w, false)
		if io.IncludeDocValues[fieldID] {
			for docNum, docTerms := range docTermMap {
				if len(docTerms) > 0 {
					err = fdvEncoder.Add(uint64(docNum), docTerms)
					if err != nil {
						return nil, err
					}
				}
			}
			err = fdvEncoder.Close()
			if err != nil {
				return nil, err
			}

			// io.incrementBytesWritten(fdvEncoder.getBytesWritten())

			fdvOffsetsStart[fieldID] = uint64(w.Count())

			_, err = fdvEncoder.Write()
			if err != nil {
				return nil, err
			}

			fdvOffsetsEnd[fieldID] = uint64(w.Count())
			fdvEncoder.Reset()
		} else {
			fdvOffsetsStart[fieldID] = fieldNotUninverted
			fdvOffsetsEnd[fieldID] = fieldNotUninverted
		}

		fieldStart := w.Count()

		err = binary.Write(w, binary.BigEndian, fdvOffsetsStart[fieldID])
		if err != nil {
			return nil, err
		}

		err = binary.Write(w, binary.BigEndian, fdvOffsetsEnd[fieldID])
		if err != nil {
			return nil, err
		}

		err = binary.Write(w, binary.BigEndian, dictOffsets[fieldID])
		if err != nil {
			return nil, err
		}

		io.fieldAddrs[fieldID] = fieldStart
		// must record/update the addrForField info for this field over here?
	}

	fdvIndexOffset := uint64(w.Count())

	// for i := 0; i < len(fdvOffsetsStart); i++ {
	// 	n := binary.PutUvarint(buf, fdvOffsetsStart[i])
	// 	_, err := w.Write(buf[:n])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	n = binary.PutUvarint(buf, fdvOffsetsEnd[i])
	// 	_, err = w.Write(buf[:n])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	log.Printf("fdvIndexOffset: %d", fdvIndexOffset)

	return dictOffsets, nil
}

func (io *invertedIndexOpaque) process(field index.Field, fieldID uint16, docNum uint64) {
	if !io.init && io.results != nil {
		io.prepareDicts()
		io.init = true
	}

	if fieldID == ^uint16(0) {
		for fieldID, tfs := range io.reusableFieldTFs {
			dict := io.Dicts[fieldID]
			norm := math.Float32frombits(uint32(io.reusableFieldLens[fieldID]))

			for term, tf := range tfs {
				pid := dict[term] - 1
				bs := io.Postings[pid]
				bs.Add(uint32(docNum))

				io.FreqNorms[pid] = append(io.FreqNorms[pid],
					interimFreqNorm{
						freq:    uint64(tf.Frequency()),
						norm:    norm,
						numLocs: len(tf.Locations),
					})

				if len(tf.Locations) > 0 {
					locs := io.Locs[pid]

					for _, loc := range tf.Locations {
						var locf = uint16(fieldID)
						if loc.Field != "" {
							locf = uint16(io.getOrDefineField(loc.Field))
						}
						var arrayposs []uint64
						if len(loc.ArrayPositions) > 0 {
							arrayposs = loc.ArrayPositions
						}
						locs = append(locs, interimLoc{
							fieldID:   locf,
							pos:       uint64(loc.Position),
							start:     uint64(loc.Start),
							end:       uint64(loc.End),
							arrayposs: arrayposs,
						})
					}

					io.Locs[pid] = locs
				}
			}
		}

		for i := 0; i < len(io.FieldsInv); i++ { // clear these for reuse
			io.reusableFieldLens[i] = 0
			io.reusableFieldTFs[i] = nil
		}
		return
	}

	if len(io.reusableFieldTFs) == 0 {
		io.reusableFieldTFs = make([]index.TokenFrequencies, len(io.FieldsInv))
	}
	if len(io.reusableFieldLens) == 0 {
		io.reusableFieldLens = make([]int, len(io.FieldsInv))
	}

	io.reusableFieldLens[fieldID] += field.AnalyzedLength()

	existingFreqs := io.reusableFieldTFs[fieldID]
	if existingFreqs != nil {
		existingFreqs.MergeAll(field.Name(), field.AnalyzedTokenFrequencies())
	} else {
		io.reusableFieldTFs[fieldID] = field.AnalyzedTokenFrequencies()
	}
}

func (i *invertedIndexOpaque) prepareDicts() {
	var pidNext int

	var totTFs int
	var totLocs int

	visitField := func(field index.Field) {
		fieldID := uint16(i.getOrDefineField(field.Name()))

		dict := i.Dicts[fieldID]
		dictKeys := i.DictKeys[fieldID]

		tfs := field.AnalyzedTokenFrequencies()
		for term, tf := range tfs {
			pidPlus1, exists := dict[term]
			if !exists {
				pidNext++
				pidPlus1 = uint64(pidNext)

				dict[term] = pidPlus1
				dictKeys = append(dictKeys, term)

				i.numTermsPerPostingsList = append(i.numTermsPerPostingsList, 0)
				i.numLocsPerPostingsList = append(i.numLocsPerPostingsList, 0)
			}

			pid := pidPlus1 - 1

			i.numTermsPerPostingsList[pid] += 1
			i.numLocsPerPostingsList[pid] += len(tf.Locations)

			totLocs += len(tf.Locations)
		}

		totTFs += len(tfs)

		i.DictKeys[fieldID] = dictKeys
	}

	for _, result := range i.results {
		// walk each composite field
		result.VisitComposite(func(field index.CompositeField) {
			visitField(field)
		})

		// walk each field
		result.VisitFields(visitField)
	}

	numPostingsLists := pidNext

	if cap(i.Postings) >= numPostingsLists {
		i.Postings = i.Postings[:numPostingsLists]
	} else {
		postings := make([]*roaring.Bitmap, numPostingsLists)
		copy(postings, i.Postings[:cap(i.Postings)])
		for i := 0; i < numPostingsLists; i++ {
			if postings[i] == nil {
				postings[i] = roaring.New()
			}
		}
		i.Postings = postings
	}

	if cap(i.FreqNorms) >= numPostingsLists {
		i.FreqNorms = i.FreqNorms[:numPostingsLists]
	} else {
		i.FreqNorms = make([][]interimFreqNorm, numPostingsLists)
	}

	if cap(i.freqNormsBacking) >= totTFs {
		i.freqNormsBacking = i.freqNormsBacking[:totTFs]
	} else {
		i.freqNormsBacking = make([]interimFreqNorm, totTFs)
	}

	freqNormsBacking := i.freqNormsBacking
	for pid, numTerms := range i.numTermsPerPostingsList {
		i.FreqNorms[pid] = freqNormsBacking[0:0]
		freqNormsBacking = freqNormsBacking[numTerms:]
	}

	if cap(i.Locs) >= numPostingsLists {
		i.Locs = i.Locs[:numPostingsLists]
	} else {
		i.Locs = make([][]interimLoc, numPostingsLists)
	}

	if cap(i.locsBacking) >= totLocs {
		i.locsBacking = i.locsBacking[:totLocs]
	} else {
		i.locsBacking = make([]interimLoc, totLocs)
	}

	locsBacking := i.locsBacking
	for pid, numLocs := range i.numLocsPerPostingsList {
		i.Locs[pid] = locsBacking[0:0]
		locsBacking = locsBacking[numLocs:]
	}

	for _, dict := range i.DictKeys {
		sort.Strings(dict)
	}
}

func (i *invertedIndexSection) getInvertedIndexOpaque(opaque map[int]resetable) *invertedIndexOpaque {
	if _, ok := opaque[sectionInvertedIndex]; !ok {
		opaque[sectionInvertedIndex] = i.InitOpaque(nil)
	}
	return opaque[sectionInvertedIndex].(*invertedIndexOpaque)
}

// revisit this function's purpose etc.
func (i *invertedIndexOpaque) getOrDefineField(fieldName string) int {
	fieldIDPlus1, exists := i.FieldsMap[fieldName]
	if !exists {
		fieldIDPlus1 = uint16(len(i.FieldsInv) + 1)
		i.FieldsMap[fieldName] = fieldIDPlus1
		i.FieldsInv = append(i.FieldsInv, fieldName)

		i.Dicts = append(i.Dicts, make(map[string]uint64))

		n := len(i.DictKeys)
		if n < cap(i.DictKeys) {
			i.DictKeys = i.DictKeys[:n+1]
			i.DictKeys[n] = i.DictKeys[n][:0]
		} else {
			i.DictKeys = append(i.DictKeys, []string(nil))
		}
	}

	return int(fieldIDPlus1 - 1)
}

func (i *invertedIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &invertedIndexOpaque{
		fieldAddrs: map[int]int{},
	}
	if args != nil {
		for k, v := range args {
			rv.Set(k, v)
		}
	}

	return rv
}

type invertedIndexOpaque struct {
	results []index.Document

	chunkMode uint32

	FieldsInv []string

	FieldsMap map[string]uint16

	// indicates whethere the following structs are initialized
	init bool

	Dicts []map[string]uint64

	// Terms for each field, where terms are sorted ascending
	//  field id -> []term
	DictKeys [][]string

	// Fields whose IncludeDocValues is true
	//  field id -> bool
	IncludeDocValues []bool

	// postings id -> bitmap of docNums
	Postings []*roaring.Bitmap

	// postings id -> freq/norm's, one for each docNum in postings
	FreqNorms        [][]interimFreqNorm
	freqNormsBacking []interimFreqNorm

	// postings id -> locs, one for each freq
	Locs        [][]interimLoc
	locsBacking []interimLoc

	numTermsPerPostingsList []int // key is postings list id
	numLocsPerPostingsList  []int // key is postings list id

	builder    *vellum.Builder
	builderBuf bytes.Buffer

	// reusable stuff for processing fields etc.
	reusableFieldLens []int
	reusableFieldTFs  []index.TokenFrequencies

	tmp0 []byte

	fieldAddrs map[int]int

	// merge related stuff?
	fieldsSame bool
	numDocs    uint64
}

func (i *invertedIndexOpaque) Reset() {
	// cleanup stuff over here
}
func (i *invertedIndexOpaque) Set(key string, val interface{}) {
	switch key {
	case "result":
		i.results = val.([]index.Document)
	case "chunkMode":
		i.chunkMode = val.(uint32)
	case "fieldsSame":
		i.fieldsSame = val.(bool)
	case "fieldsMap":
		i.FieldsMap = val.(map[string]uint16)
	case "numDocs":
		i.numDocs = val.(uint64)
	}
}

type numericRangeIndexSection struct {
}

func (n *numericRangeIndexSection) Process(opaque map[int]resetable, docNum uint64, field index.Field, fieldID uint16) {
	if nf, ok := field.(index.NumericField); ok {
		nfv, _ := nf.Number()
		n.process(opaque, docNum, nfv, fieldID)
	}
}

func (n *numericRangeIndexSection) process(opaque map[int]resetable, docNum uint64, val float64, fieldID uint16) {
	nro := n.getNumericRangeOpaque(opaque)
	nro.byField[int(fieldID)] = AddNumericValue(val, docNum, nro.byField[int(fieldID)])
}

func (n *numericRangeIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (int64, error) {
	nro := n.getNumericRangeOpaque(opaque)

	// for each field
	for fieldId, nrNodes := range nro.byField {

		// for each value in field
		for _, node := range nrNodes {

			// write out bitmap of doc nums having this value

			// FIXME support reusing buffer?
			var err error

			// record address of this bitmap for later use
			node.addr = w.Count()
			_, err = writeRoaringWithLen(node.docs, w, make([]byte, binary.MaxVarintLen64))
			if err != nil {
				return 0, err
			}
		}

		fieldStart := w.Count()
		// now write the number of values we have
		writeUvarints(w, uint64(len(nrNodes)))
		// now write out each of the values
		for _, node := range nrNodes {
			// FIXME must write in little endian to use unsafe slice conversion on read-side
			binary.Write(w, binary.LittleEndian, node.val)
		}
		// now write out each of the addresses
		for _, node := range nrNodes {
			// FIXME must write in little endian to use unsafe slice conversion on read-side
			binary.Write(w, binary.LittleEndian, uint64(node.addr))
		}
		nro.fieldAddrs[fieldId] = fieldStart
	}

	return 0, nil
}

func (n *numericRangeIndexSection) getNumericRangeOpaque(opaque map[int]resetable) *numericRangeOpaque {
	if _, ok := opaque[sectionNumericRangeIndex]; !ok {
		opaque[sectionNumericRangeIndex] = n.InitOpaque(nil)
	}
	return opaque[sectionNumericRangeIndex].(*numericRangeOpaque)
}

func (n *numericRangeIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	nro := n.getNumericRangeOpaque(opaque)
	return nro.fieldAddrs[fieldID]
}

// FIXME need to check the closeCh sometime
func (n *numericRangeIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase, drops []*roaring.Bitmap, fieldsInv []string,
	newDocNumsIn [][]uint64, w *CountHashWriter, closeCh chan struct{}) error {

	// try to merge field at a time?
	for fieldID := range fieldsInv {

		// iterate each segment
		for segI, sb := range segments {
			fieldNumericSection := sb.fieldsSectionsMap[fieldID][sectionNumericRangeIndex]

			// read how many values are in this field
			numNumericValues, sz := binary.Uvarint(sb.mem[fieldNumericSection : fieldNumericSection+binary.MaxVarintLen64])
			pos := fieldNumericSection + uint64(sz)
			floatData, err := ByteSliceToFloat6464Slice(sb.mem[pos : pos+(numNumericValues*8)])
			if err != nil {
				return err
			}

			floatOffsets := pos + (numNumericValues * 8)
			floatOffsetsSlice, err := ByteSliceToUint64Slice(sb.mem[floatOffsets : floatOffsets+(numNumericValues*8)])
			if err != nil {
				return err
			}

			// now walk each float val
			for walkIndex := 0; walkIndex < int(numNumericValues); walkIndex++ {
				valueBitmapOffset := floatOffsetsSlice[walkIndex]

				// load/read roaring for this value

				bitSetLen, read := binary.Uvarint(sb.mem[valueBitmapOffset : valueBitmapOffset+binary.MaxVarintLen64])
				bitSetStart := valueBitmapOffset + uint64(read)

				roaringBytes := sb.mem[bitSetStart : bitSetStart+bitSetLen]

				// fixme reuse roaring bitmap in this loop
				postings := roaring.New()
				_, err := postings.FromBuffer(roaringBytes)
				if err != nil {
					return fmt.Errorf("error loading roaring bitmap: %v", err)
				}

				// now iterate it
				itr := postings.Iterator()
				for itr.HasNext() {
					segmentLocalDocNum := itr.Next()
					n.process(opaque, newDocNumsIn[segI][segmentLocalDocNum], floatData[walkIndex], uint16(fieldID))
				}
			}
		}
	}

	// now we have merged all the numeric data, we can start writing
	_, err := n.Persist(opaque, w)

	return err
}

func (n *numericRangeIndexSection) InitOpaque(args map[string]interface{}) resetable {
	return &numericRangeOpaque{
		byField:    map[int]nrNodes{},
		fieldAddrs: map[int]int{},
	}
}

type numericRangeOpaque struct {
	byField    map[int]nrNodes
	fieldAddrs map[int]int
}

func (n *numericRangeOpaque) Reset() {
	n.byField = map[int]nrNodes{}
	n.fieldAddrs = map[int]int{}
}

func (n *numericRangeOpaque) Set(key string, val interface{}) {
}
