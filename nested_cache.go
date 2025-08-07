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
	"sync"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type tree []*treeNode

func newTree(numChild int) tree {
	return make(tree, numChild)
}

type treeNode struct {
	// non nested fields are there in a segment
	fieldSeg segment.Segment
	// nested field name -> tree
	fieldTree map[string]tree
}

func newNestedIndexCache() *nestedIndexCache {
	return &nestedIndexCache{
		cache: make(map[string]tree),
	}
}

type nestedIndexCache struct {
	m sync.RWMutex
	// root nested field name -> tree
	cache map[string]tree
}

func (nc *nestedIndexCache) Clear() {
	nc.m.Lock()
	nc.cache = nil
	nc.m.Unlock()
}

func processIterator(t tree, nestItr index.NestedIterator) segment.Segment {
	_, arrayPos, ok := nestItr.Next()
	if !ok {
		return nil
	}
	if arrayPos < 0 || arrayPos >= len(t) {
		return nil
	}
	currentNode := t[arrayPos]
	for nestItr.HasNext() && currentNode != nil {
		path, arrayPos, ok := nestItr.Next()
		if !ok {
			return nil
		}
		nextTree, ok := currentNode.fieldTree[path]
		if !ok {
			return nil
		}
		if arrayPos < 0 || arrayPos >= len(nextTree) {
			return nil
		}
		currentNode = nextTree[arrayPos]
	}
	if currentNode == nil {
		return nil
	}
	return currentNode.fieldSeg
}

func (nc *nestedIndexCache) loadNestedSegment(sb *SegmentBase, rootPath string, nestLoc uint64,
	nestItr index.NestedIterator) (segment.Segment, error) {
	tree, err := nc.loadOrCreateTree(sb.mem, rootPath, nestLoc)
	if err != nil {
		return nil, fmt.Errorf("failed to load or create tree for path %s: %w", rootPath, err)
	}
	return processIterator(tree, nestItr), nil
}

// ---------------------------------------------------------------------------------------------------------

func (nc *nestedIndexCache) loadOrCreateTree(mem []byte, rootPath string, nestLoc uint64) (tree, error) {
	nc.m.RLock()
	tree, ok := nc.cache[rootPath]
	if ok {
		nc.m.RUnlock()
		return tree, nil
	}
	nc.m.RUnlock()
	nc.m.Lock()
	defer nc.m.Unlock()
	tree, ok = nc.cache[rootPath]
	if ok {
		return tree, nil
	}
	tree, err := nc.createTreeLOCKED(mem, nestLoc)
	if err != nil {
		return nil, fmt.Errorf("failed to create nested index tree for path %s: %w", rootPath, err)
	}
	nc.cache[rootPath] = tree
	return tree, nil
}

func (nc *nestedIndexCache) createTreeLOCKED(mem []byte, offset uint64) (tree, error) {
	numArrayPositions, read := binary.Uvarint(mem[offset : offset+binary.MaxVarintLen64])
	if numArrayPositions == 0 || read <= 0 {
		return nil, fmt.Errorf("numArrayPositions length is 0")
	}
	offset += uint64(read)

	tree := newTree(int(numArrayPositions))

	for i := 0; i < int(numArrayPositions); i++ {
		// read the offset of the tree for this array position
		nodeOffset, read := binary.Uvarint(mem[offset : offset+binary.MaxVarintLen64])
		if read <= 0 {
			return nil, fmt.Errorf("failed to read tree offset at position %d", offset)
		}
		offset += uint64(read)
		node, err := nc.createNodeLOCKED(mem, nodeOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to create node at position %d: %w", nodeOffset, err)
		}
		tree[i] = node
	}
	return tree, nil
}
func (nc *nestedIndexCache) createNodeLOCKED(mem []byte, offset uint64) (*treeNode, error) {
	rv := &treeNode{
		fieldTree: make(map[string]tree),
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
		rv.fieldSeg = s
	}
	numTrees, n := binary.Uvarint(mem[offset:])
	if n <= 0 {
		return nil, fmt.Errorf("invalid number of child trees at offset %d", offset)
	}
	offset += uint64(n)
	for i := 0; i < int(numTrees); i++ {
		nameLen, n := binary.Uvarint(mem[offset:])
		if n <= 0 || nameLen == 0 {
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
		tree, err := nc.createTreeLOCKED(mem, treeOffset)
		if err != nil {
			return nil, err
		}
		rv.fieldTree[name] = tree
	}
	if rv.fieldSeg == nil && len(rv.fieldTree) == 0 {
		return nil, fmt.Errorf("tree node at offset %d has no segment or child trees", offset)
	}
	return rv, nil
}
