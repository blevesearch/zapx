//  Copyright (c) 2025 Couchbase, Inc.
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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	seg "github.com/blevesearch/scorch_segment_api/v2"
)

func init() {
	registerSegmentSection(SectionNestedIndex, &nestedIndexSection{})
	invertedTextIndexSectionExclusionChecks = append(invertedTextIndexSectionExclusionChecks, func(field index.Field) bool {
		_, ok := field.(index.NestedField)
		return ok
	})
}

// -----------------------------------------------------------------------------

type fieldTree []*indexNode

func newFieldTree(numChild int) fieldTree {
	return make([]*indexNode, numChild)
}

func (ft fieldTree) mergeWith(other fieldTree) fieldTree {
	if len(ft) == 0 {
		return other
	}
	if len(other) == 0 {
		return ft
	}
	// Grow the current tree if needed
	if len(ft) < len(other) {
		newTree := newFieldTree(len(other))
		copy(newTree, ft)
		ft = newTree
	}

	// Merge each node
	for i, otherNode := range other {
		if otherNode == nil {
			continue
		}
		if ft[i] == nil {
			ft[i] = &indexNode{
				results:     make(map[int]index.Document),
				childForest: make(map[string]fieldTree),
			}
		}
		ft[i].mergeWith(otherNode)
	}

	return ft
}

func (ft fieldTree) writeTo(w *CountHashWriter) (uint64, error) {
	// Write the number of nodes in this tree
	nodeOffsets := make([]uint64, len(ft))
	for i, node := range ft {
		offset, err := node.writeTo(w)
		if err != nil {
			return 0, err
		}
		nodeOffsets[i] = offset
	}
	// Write the number of nodes
	rv := uint64(w.Count())
	_, err := writeUvarints(w, uint64(len(ft)))
	if err != nil {
		return 0, err
	}
	// Write the offsets for each node
	for _, offset := range nodeOffsets {
		_, err := writeUvarints(w, offset)
		if err != nil {
			return 0, err
		}
	}
	return rv, nil
}

type indexNode struct {
	results map[int]index.Document

	childForest map[string]fieldTree
}

func (in *indexNode) mergeWith(other *indexNode) {
	if other == nil {
		return
	}
	// Merge results
	if in.results == nil {
		in.results = make(map[int]index.Document)
	}
	for docNum, doc := range other.results {
		in.results[docNum] = doc
	}

	// Merge child trees
	if in.childForest == nil {
		in.childForest = make(map[string]fieldTree)
	}
	for name, otherTree := range other.childForest {
		currentTree := in.childForest[name]
		in.childForest[name] = currentTree.mergeWith(otherTree)
	}
}

func (in *indexNode) writeTo(w *CountHashWriter) (uint64, error) {
	var segStart, segEnd uint64
	if len(in.results) != 0 {
		segStart = uint64(w.Count())
		docContainer := NewDocumentMap(in.results)
		buf, _, _, err := newSegment(docContainer, DefaultChunkMode, true)
		if err != nil {
			return 0, err
		}
		bufBytes := buf.Bytes()
		// Write the segment data
		_, err = w.Write(bufBytes)
		if err != nil {
			return 0, err
		}
		segEnd = uint64(w.Count())
	} else {
		segStart, segEnd = fieldNotUninverted, fieldNotUninverted
	}
	var treeOffsetMap map[string]uint64
	if len(in.childForest) > 0 {
		treeOffsetMap = make(map[string]uint64)
		for name, tree := range in.childForest {
			treeOffset, err := tree.writeTo(w)
			if err != nil {
				return 0, err
			}
			treeOffsetMap[name] = uint64(treeOffset)
		}
	}
	rv := uint64(w.Count())
	// Write the segment start and end
	_, err := writeUvarints(w, segStart, segEnd)
	if err != nil {
		return 0, err
	}
	_, err = writeUvarints(w, uint64(len(treeOffsetMap)))
	if err != nil {
		return 0, err
	}
	for name, offset := range treeOffsetMap {
		_, err := writeUvarints(w, uint64(len(name)))
		if err != nil {
			return 0, err
		}
		_, err = w.Write([]byte(name))
		if err != nil {
			return 0, err
		}
		_, err = writeUvarints(w, offset)
		if err != nil {
			return 0, err
		}
	}
	return rv, nil
}

func nestedFieldToTree(rootDocNum int, nf index.NestedField) fieldTree {
	// Create a new field tree with the number of children
	tree := newFieldTree(nf.NumChildren())
	// Visit each child and add it to the tree
	nf.VisitChildren(func(ap int, doc index.Document) {
		curNode := &indexNode{
			results:     make(map[int]index.Document),
			childForest: make(map[string]fieldTree),
		}
		flatFields := make([]index.Field, 0)
		doc.VisitFields(func(field index.Field) {
			if childField, ok := field.(index.NestedField); ok {
				childTree := nestedFieldToTree(rootDocNum, childField)
				curNode.childForest[childField.Name()] = childTree
			} else {
				flatFields = append(flatFields, field)
			}
		})
		nestedDocument := newNestedDocument(flatFields)
		curNode.results[rootDocNum] = nestedDocument
		tree[ap] = curNode
	})
	return tree
}

// -----------------------------------------------------------------------------

type mergeTree []*mergeNode

func newMergeTree(numChild int) mergeTree {
	return make([]*mergeNode, numChild)
}

func (mt mergeTree) mergeWith(other mergeTree, drops *roaring.Bitmap, newDocNumsIn []uint64) mergeTree {
	if len(mt) == 0 {
		return other
	}
	if len(other) == 0 {
		return mt
	}
	// Grow the current tree if needed
	if len(mt) < len(other) {
		newTree := make(mergeTree, len(other))
		copy(newTree, mt)
		mt = newTree
	}

	// Merge each node
	for i, otherNode := range other {
		if otherNode == nil {
			continue
		}
		if mt[i] == nil {
			mt[i] = &mergeNode{
				sbs:         make([]*SegmentBase, 0),
				childForest: make(map[string]mergeTree),
			}
		}
		mt[i].mergeWith(otherNode, drops, newDocNumsIn)
	}

	return mt
}

func (mt mergeTree) writeTo(w *CountHashWriter, closeCh chan struct{}) (uint64, error) {
	// Write the number of nodes in this tree
	nodeOffsets := make([]uint64, len(mt))
	for i, node := range mt {
		offset, err := node.writeTo(w, closeCh)
		if err != nil {
			return 0, err
		}
		nodeOffsets[i] = offset
	}
	// Write the number of nodes
	rv := uint64(w.Count())
	_, err := writeUvarints(w, uint64(len(mt)))
	if err != nil {
		return 0, err
	}
	// Write the offsets for each node
	for _, offset := range nodeOffsets {
		_, err := writeUvarints(w, offset)
		if err != nil {
			return 0, err
		}
	}
	return rv, nil
}

type mergeNode struct {
	sbs []*SegmentBase

	drops []*roaring.Bitmap

	newDocNumsIn [][]uint64

	childForest map[string]mergeTree
}

func (mn *mergeNode) mergeWith(other *mergeNode, drops *roaring.Bitmap, newDocNumsIn []uint64) {
	if other == nil {
		return
	}
	// Merge segments
	mn.sbs = append(mn.sbs, other.sbs...)

	// Merge drops
	mn.drops = append(mn.drops, other.drops...)

	// Merge new document numbers
	mn.newDocNumsIn = append(mn.newDocNumsIn, other.newDocNumsIn...)

	// Merge child trees
	if mn.childForest == nil {
		mn.childForest = make(map[string]mergeTree)
	}
	for name, otherTree := range other.childForest {
		currentTree := mn.childForest[name]
		mn.childForest[name] = currentTree.mergeWith(otherTree, drops, newDocNumsIn)
	}
}

func (mn *mergeNode) writeTo(w *CountHashWriter, closeCh chan struct{}) (uint64, error) {
	var segStart, segEnd uint64
	if len(mn.sbs) != 0 {
		segStart = uint64(w.Count())
		_, err := mergeNestedSegmentBases(mn.sbs, mn.drops, DefaultChunkMode, closeCh, w, mn.newDocNumsIn)
		if err != nil {
			return 0, err
		}
		segEnd = uint64(w.Count())
	} else {
		segStart, segEnd = fieldNotUninverted, fieldNotUninverted
	}
	var treeOffsetMap map[string]uint64
	if len(mn.childForest) > 0 {
		treeOffsetMap = make(map[string]uint64)
		for name, tree := range mn.childForest {
			treeOffset, err := tree.writeTo(w, closeCh)
			if err != nil {
				return 0, err
			}
			treeOffsetMap[name] = uint64(treeOffset)
		}
	}
	rv := uint64(w.Count())
	// Write the segment start and end
	_, err := writeUvarints(w, segStart, segEnd)
	if err != nil {
		return 0, err
	}
	_, err = writeUvarints(w, uint64(len(treeOffsetMap)))
	if err != nil {
		return 0, err
	}
	for name, offset := range treeOffsetMap {
		_, err := writeUvarints(w, uint64(len(name)))
		if err != nil {
			return 0, err
		}
		_, err = w.Write([]byte(name))
		if err != nil {
			return 0, err
		}
		_, err = writeUvarints(w, offset)
		if err != nil {
			return 0, err
		}
	}
	return rv, nil
}

// -----------------------------------------------------------------------------

type nestedIndexOpaque struct {
	forest map[int]fieldTree

	treeAddrs map[int]int
}

func (no *nestedIndexOpaque) Set(key string, value interface{}) {
}

func (no *nestedIndexOpaque) Reset() (err error) {
	no.forest = nil
	no.treeAddrs = nil
	return err
}

// -----------------------------------------------------------------------------

func (no *nestedIndexOpaque) writeNested(w *CountHashWriter) (err error) {
	for fieldID, tree := range no.forest {
		treeOffset, err := tree.writeTo(w)
		if err != nil {
			return err
		}

		no.treeAddrs[fieldID] = w.Count()

		writeUvarints(w, fieldNotUninverted)

		writeUvarints(w, fieldNotUninverted)

		writeUvarints(w, treeOffset)
	}
	return nil
}

// -----------------------------------------------------------------------------

type nestedIndexSection struct {
}

func (n *nestedIndexSection) getNestedIndexOpaque(opaque map[int]resetable) *nestedIndexOpaque {
	if _, ok := opaque[SectionNestedIndex]; !ok {
		opaque[SectionNestedIndex] = n.InitOpaque(nil)
	}
	return opaque[SectionNestedIndex].(*nestedIndexOpaque)
}

func (n *nestedIndexSection) InitOpaque(args map[string]interface{}) resetable {
	rv := &nestedIndexOpaque{
		forest:    make(map[int]fieldTree),
		treeAddrs: make(map[int]int),
	}
	for k, v := range args {
		rv.Set(k, v)
	}
	return rv
}

func (n *nestedIndexSection) Process(opaque map[int]resetable, docNum uint32, field index.Field, fieldID uint16) {
	if fieldID == math.MaxUint16 {
		return
	}
	if nf, ok := field.(index.NestedField); ok {
		no := n.getNestedIndexOpaque(opaque)
		curTree := no.forest[int(fieldID)]
		newTree := nestedFieldToTree(int(docNum), nf)
		mergedTree := curTree.mergeWith(newTree)
		no.forest[int(fieldID)] = mergedTree
	}
}

func (n *nestedIndexSection) Persist(opaque map[int]resetable, w *CountHashWriter) (int64, error) {
	nestedIndexOpaque := n.getNestedIndexOpaque(opaque)
	err := nestedIndexOpaque.writeNested(w)
	return 0, err
}

func (n *nestedIndexSection) AddrForField(opaque map[int]resetable, fieldID int) int {
	nestedIndexOpaque := n.getNestedIndexOpaque(opaque)
	return nestedIndexOpaque.treeAddrs[fieldID]
}

func treeFromOffset(offset uint64, mem []byte) (mergeTree, error) {
	numChild, n := binary.Uvarint(mem[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid number of nodes at offset %d", offset)
	}
	offset += uint64(n)

	tree := newMergeTree(int(numChild))
	for i := 0; i < int(numChild); i++ {
		nodeOffset, n := binary.Uvarint(mem[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid node offset at offset %d", offset)
		}
		offset += uint64(n)
		node, err := readNodeFromOffset(nodeOffset, mem)
		if err != nil {
			return nil, err
		}
		tree[i] = node
	}
	return tree, nil
}

func readNodeFromOffset(offset uint64, mem []byte) (*mergeNode, error) {
	rv := &mergeNode{
		sbs:         make([]*SegmentBase, 0),
		childForest: make(map[string]mergeTree),
	}
	segStart, n := binary.Uvarint(mem[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid segment start at offset %d", offset)
	}
	offset += uint64(n)
	segEnd, n := binary.Uvarint(mem[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid segment end at offset %d", offset)
	}
	offset += uint64(n)
	if segStart != fieldNotUninverted && segEnd != fieldNotUninverted {
		s, err := OpenFromBuffer(mem[segStart:segEnd])
		if err != nil {
			return nil, err
		}
		rv.sbs = append(rv.sbs, &s.SegmentBase)
	}
	numTrees, n := binary.Uvarint(mem[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid number of child trees at offset %d", offset)
	}
	offset += uint64(n)
	for i := 0; i < int(numTrees); i++ {
		nameLen, n := binary.Uvarint(mem[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid name length at offset %d", offset)
		}
		offset += uint64(n)
		name := string(mem[offset : offset+nameLen])
		offset += nameLen
		treeOffset, n := binary.Uvarint(mem[offset:])
		if n <= 0 {
			return nil, fmt.Errorf("invalid tree offset at offset %d", offset)
		}
		offset += uint64(n)
		tree, err := treeFromOffset(treeOffset, mem)
		if err != nil {
			return nil, err
		}
		rv.childForest[name] = tree
	}
	return rv, nil
}

func (n *nestedIndexSection) Merge(opaque map[int]resetable, segments []*SegmentBase,
	drops []*roaring.Bitmap, fieldsInv []string, newDocNumsIn [][]uint64,
	w *CountHashWriter, closeCh chan struct{}) error {

	no := n.getNestedIndexOpaque(opaque)
	forest := make(map[int]mergeTree)

	for fieldID, fieldName := range fieldsInv {
		for segIdx, sb := range segments {
			if isClosed(closeCh) {
				return seg.ErrClosed
			}
			if _, ok := sb.fieldsMap[fieldName]; !ok {
				continue
			}
			pos := int(sb.fieldsSectionsMap[sb.fieldsMap[fieldName]-1][SectionNestedIndex])
			if pos == 0 {
				continue
			}
			_, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			_, n = binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			treeOffset, n := binary.Uvarint(sb.mem[pos : pos+binary.MaxVarintLen64])
			pos += n
			tree, err := treeFromOffset(treeOffset, sb.mem)
			if err != nil {
				return err
			}
			forest[fieldID] = forest[fieldID].mergeWith(tree, drops[segIdx], newDocNumsIn[segIdx])
		}
	}

	for fieldID, tree := range forest {
		treeOffset, err := tree.writeTo(w, closeCh)
		if err != nil {
			return err
		}

		no.treeAddrs[fieldID] = w.Count()

		writeUvarints(w, fieldNotUninverted)

		writeUvarints(w, fieldNotUninverted)

		writeUvarints(w, treeOffset)
	}

	return nil
}

// -----------------------------------------------------------------------------
type nestedDocument struct {
	fields []index.Field
}

func newNestedDocument(fields []index.Field) *nestedDocument {
	return &nestedDocument{
		fields: fields,
	}
}

func (nd *nestedDocument) ID() string {
	return ""
}
func (nd *nestedDocument) Size() int {
	return 0
}
func (nd *nestedDocument) VisitFields(visitor index.FieldVisitor) {
	for _, field := range nd.fields {
		visitor(field)
	}
}
func (nd *nestedDocument) VisitComposite(visitor index.CompositeFieldVisitor) {
}
func (nd *nestedDocument) HasComposite() bool {
	return false
}
func (nd *nestedDocument) NumPlainTextBytes() uint64 {
	return 0
}
func (nd *nestedDocument) AddIDField() {
}
func (nd *nestedDocument) StoredFieldsBytes() uint64 {
	return 0
}
func (nd *nestedDocument) Indexed() bool {
	return false
}
