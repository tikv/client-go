// Copyright 2024 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package art

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/kv"
)

func (t *ART) Iter(lowerBound, upperBound []byte) (*Iterator, error) {
	return t.iter(lowerBound, upperBound, false, false)
}

func (t *ART) IterReverse(upperBound, lowerBound []byte) (*Iterator, error) {
	return t.iter(lowerBound, upperBound, true, false)
}

func (t *ART) IterWithFlags(lowerBound, upperBound []byte) *Iterator {
	it, _ := t.iter(lowerBound, upperBound, false, true)
	return it
}

func (t *ART) IterReverseWithFlags(upperBound []byte) *Iterator {
	it, _ := t.iter(nil, upperBound, true, true)
	return it
}

func (t *ART) iter(lowerBound, upperBound []byte, reverse, includeFlags bool) (*Iterator, error) {
	it := &Iterator{
		tree:         t,
		reverse:      reverse,
		valid:        true,
		includeFlags: includeFlags,
		inner: &baseIter{
			allocator: &t.allocator,
		},
		// the default value of currAddr is not equal to any valid address
		// arena.BadAddr's idx is maxuint32 - 1, which is impossible in common cases,
		// this avoids the initial value of currAddr equals to endAddr.
		currAddr: arena.BadAddr,
		endAddr:  arena.NullAddr,
	}
	it.init(lowerBound, upperBound)
	if !it.valid {
		return it, nil
	}
	if err := it.Next(); err != nil {
		return nil, err
	}
	return it, nil
}

type Iterator struct {
	tree         *ART
	reverse      bool
	valid        bool
	includeFlags bool
	inner        *baseIter
	currLeaf     *artLeaf
	currAddr     arena.MemdbArenaAddr
	endAddr      arena.MemdbArenaAddr
}

func (it *Iterator) Valid() bool        { return it.valid }
func (it *Iterator) Key() []byte        { return it.currLeaf.GetKey() }
func (it *Iterator) Flags() kv.KeyFlags { return it.currLeaf.GetKeyFlags() }
func (it *Iterator) Value() []byte {
	if it.currLeaf.vLogAddr.IsNull() {
		return nil
	}
	return it.tree.allocator.vlogAllocator.GetValue(it.currLeaf.vLogAddr)
}

// HasValue returns false if it is flags only.
func (it *Iterator) HasValue() bool {
	return !it.isFlagsOnly()
}

func (it *Iterator) isFlagsOnly() bool {
	return it.currLeaf != nil && it.currLeaf.vLogAddr.IsNull()
}

func (it *Iterator) Next() error {
	if !it.valid {
		// iterate is finished
		return errors.New("Art: iterator is finished")
	}
	if it.currAddr == it.endAddr {
		it.valid = false
		return nil
	}

	var nextLeaf artNode
	for {
		if it.reverse {
			nextLeaf = it.inner.prev()
		} else {
			nextLeaf = it.inner.next()
		}
		if nextLeaf.addr.IsNull() {
			it.valid = false
			return nil
		}
		it.setCurrLeaf(nextLeaf.addr)
		if it.currLeaf.vLogAddr.IsNull() {
			// if it.includeFlags is true, the iterator should return even the value is null.
			if it.includeFlags && !it.currLeaf.isDeleted() {
				return nil
			}
			if nextLeaf.addr == it.endAddr {
				it.valid = false
				return nil
			}
			continue
		}
		return nil
	}
}

func (it *Iterator) setCurrLeaf(node arena.MemdbArenaAddr) {
	it.currAddr = node
	it.currLeaf = it.tree.allocator.getLeaf(node)
}

func (it *Iterator) Close() {}

func (it *Iterator) Handle() arena.MemKeyHandle {
	return it.currAddr.ToHandle()
}

func (it *Iterator) init(lowerBound, upperBound []byte) {
	if it.tree.root.addr.IsNull() {
		it.valid = false
		return
	}
	if len(lowerBound) > 0 && len(upperBound) > 0 {
		if bytes.Compare(lowerBound, upperBound) >= 0 {
			it.valid = false
			return
		}
	}

	startKey, endKey := lowerBound, upperBound
	if it.reverse {
		startKey, endKey = upperBound, lowerBound
	}

	if len(startKey) == 0 {
		it.inner.seekToFirst(it.tree.root, it.reverse)
	} else {
		it.inner.seek(it.tree.root, startKey)
	}
	if len(endKey) == 0 {
		it.endAddr = arena.NullAddr
		return
	}

	helper := new(baseIter)
	helper.allocator = &it.tree.allocator
	helper.seek(it.tree.root, endKey)
	cmp := it.inner.compare(helper)
	if cmp == 0 {
		// no keys exist between start key and end key, set the iterator to invalid.
		it.valid = false
		return
	}

	if it.reverse {
		it.endAddr = helper.next().addr
	} else {
		it.endAddr = helper.prev().addr
	}
	cmp = it.inner.compare(helper)
	if cmp == 0 {
		// the current key is valid.
		return
	}
	// in asc scan, if cmp > 0, it means current key is larger than end key, set the iterator to invalid.
	// in desc scan, if cmp < 0, it means current key is less than end key, set the iterator to invalid.
	if cmp < 0 == it.reverse || len(helper.idxes) == 0 {
		it.valid = false
	}
}

// baseIter is the inner iterator for ART tree.
// You need to call seek or seekToFirst to initialize the iterator.
// after initialization, you can call next or prev to iterate the ART.
// next or prev returns nullArtNode if there is no more leaf node.
type baseIter struct {
	allocator *artAllocator
	// the baseIter iterate the ART tree in DFS order, the idxes and nodes are the current visiting stack.
	idxes []int
	nodes []artNode
}

// seekToFirst seeks the boundary of the tree, sequential or reverse.
func (it *baseIter) seekToFirst(root artNode, reverse bool) {
	// if the seek key is empty, it means -inf or +inf, return root node directly.
	it.nodes = []artNode{root}
	if reverse {
		it.idxes = []int{node256cap}
	} else {
		it.idxes = []int{inplaceIndex}
	}
}

// seek the first node and index that >= key, nodes[0] is the root node
func (it *baseIter) seek(root artNode, key artKey) {
	if len(key) == 0 {
		panic("seek with empty key is not allowed")
	}
	curr := root
	depth := uint32(0)
	var node *nodeBase
	for {
		if curr.isLeaf() {
			if key.valid(int(depth)) {
				lf := curr.asLeaf(it.allocator)
				if bytes.Compare(key, lf.GetKey()) > 0 {
					// the seek key is not exist, and it's longer and larger than the current leaf's key.
					// e.g. key: [1, 1, 1], leaf: [1, 1].
					it.idxes[len(it.idxes)-1]++
				}
			}
			return
		}

		it.nodes = append(it.nodes, curr)
		node = curr.asNode(it.allocator)
		if node.prefixLen > 0 {
			mismatchIdx := node.matchDeep(it.allocator, &curr, key, depth)
			if mismatchIdx < node.prefixLen {
				// Check whether the seek key is smaller than the prefix, as no leaf node matches the seek key.
				// If the seek key is smaller than the prefix, all the children are located on the right side of the seek key.
				// Otherwise, the children are located on the left side of the seek key.
				var prefix []byte
				if mismatchIdx < maxInNodePrefixLen {
					prefix = node.prefix[:]
				} else {
					leafNode := minimumLeafNode(it.allocator, curr)
					prefix = leafNode.asLeaf(it.allocator).getKeyDepth(depth)
				}
				if mismatchIdx+depth == uint32(len(key)) || key[depth+mismatchIdx] < prefix[mismatchIdx] {
					// mismatchIdx + depth == len(key) indicates that the seek key is a prefix of any leaf in the current node, implying that key < leafKey.
					// If key < leafKey, set index to -1 means all the children are larger than the seek key.
					it.idxes = append(it.idxes, -1)
				} else {
					// If key > prefix, set index to 256 means all the children are smaller than the seek key.
					it.idxes = append(it.idxes, node256cap)
				}
				return
			}
			depth += min(mismatchIdx, node.prefixLen)
		}

		char := key.charAt(int(depth))
		idx, next := curr.findChild(it.allocator, char, !key.valid(int(depth)))
		if next.addr.IsNull() {
			nextIdx := seekToIdx(it.allocator, curr, char)
			it.idxes = append(it.idxes, nextIdx)
			return
		}
		it.idxes = append(it.idxes, idx)
		curr = next
		depth++
	}
}

// seekToIdx finds the index where all nodes before it are less than the given character.
func seekToIdx(a *artAllocator, curr artNode, char byte) int {
	var nextIdx int
	switch curr.kind {
	case typeNode4:
		n4 := curr.asNode4(a)
		for ; nextIdx < int(n4.nodeNum); nextIdx++ {
			if n4.keys[nextIdx] >= char {
				break
			}
		}
	case typeNode16:
		n16 := curr.asNode16(a)
		nextIdx, _ = sort.Find(int(n16.nodeNum), func(i int) int {
			if n16.keys[i] < char {
				return 1
			}
			return -1
		})
	case typeNode48:
		n48 := curr.asNode48(a)
		nextIdx = n48.nextPresentIdx(int(char))
	case typeNode256:
		n256 := curr.asNode256(a)
		nextIdx = n256.nextPresentIdx(int(char))
	default:
		panic("invalid node type")
	}
	return nextIdx
}

// compare compares the path of nodes, return 1 if self > other, -1 if self < other, 0 if self == other
func (it *baseIter) compare(other *baseIter) int {
	l1, l2 := len(it.idxes), len(other.idxes)
	for i, l := 0, min(l1, l2); i < l; i++ {
		if it.idxes[i] == other.idxes[i] {
			continue
		}
		if it.idxes[i] > other.idxes[i] {
			return 1
		}
		if it.idxes[i] < other.idxes[i] {
			return -1
		}
	}
	if l1 == l2 {
		return 0
	} else if l1 < l2 {
		return -1
	} else {
		return 1
	}
}

// next returns the next leaf node
// it returns nullArtNode if there is no more leaf node
func (it *baseIter) next() artNode {
	for {
		depth := len(it.nodes) - 1
		curr := it.nodes[depth]
		idx := it.idxes[depth]
		var child *artNode
		switch curr.kind {
		case typeNode4:
			n4 := it.allocator.getNode4(curr.addr)
			if idx == inplaceIndex {
				idx = 0 // mark in-place leaf is visited
				it.idxes[depth] = idx
				if !n4.inplaceLeaf.addr.IsNull() {
					return n4.inplaceLeaf
				}
			} else if idx == node4cap {
				break
			}
			if idx >= 0 && idx < int(n4.nodeNum) {
				it.idxes[depth] = idx
				child = &n4.children[idx]
			} else if idx >= int(n4.nodeNum) {
				// idx >= n4.nodeNum means this node is drain, break to pop stack.
				break
			} else {
				panicForInvalidIndex(idx)
			}
		case typeNode16:
			n16 := it.allocator.getNode16(curr.addr)
			if idx == inplaceIndex {
				idx = 0 // mark in-place leaf is visited
				it.idxes[depth] = idx
				if !n16.inplaceLeaf.addr.IsNull() {
					return n16.inplaceLeaf
				}
			} else if idx == node16cap {
				break
			}
			if idx >= 0 && idx < int(n16.nodeNum) {
				it.idxes[depth] = idx
				child = &n16.children[idx]
			} else if idx >= int(n16.nodeNum) {
				// idx >= n16.nodeNum means this node is drain, break to pop stack.
				break
			} else {
				panicForInvalidIndex(idx)
			}
		case typeNode48:
			n48 := it.allocator.getNode48(curr.addr)
			if idx == inplaceIndex {
				idx = 0 // mark in-place leaf is visited
				it.idxes[depth] = idx
				if !n48.inplaceLeaf.addr.IsNull() {
					return n48.inplaceLeaf
				}
			} else if idx == node256cap {
				break
			}
			idx = n48.nextPresentIdx(idx)
			if idx >= 0 && idx < node256cap {
				it.idxes[depth] = idx
				child = &n48.children[n48.keys[idx]]
			} else if idx == node256cap {
				// idx == node256cap means this node is drain, break to pop stack.
				break
			} else {
				panicForInvalidIndex(idx)
			}
		case typeNode256:
			n256 := it.allocator.getNode256(curr.addr)
			if idx == inplaceIndex {
				idx = 0 // mark in-place leaf is visited
				it.idxes[depth] = idx
				if !n256.inplaceLeaf.addr.IsNull() {
					return n256.inplaceLeaf
				}
			} else if idx == node256cap {
				break
			}
			idx = n256.nextPresentIdx(idx)
			if idx >= 0 && idx < 256 {
				it.idxes[depth] = idx
				child = &n256.children[idx]
			} else if idx == node256cap {
				// idx == node256cap means this node is drain, break to pop stack.
				break
			} else {
				panicForInvalidIndex(idx)
			}
		default:
			panic("invalid node type")
		}
		if child != nil {
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return *child
			}
			it.nodes = append(it.nodes, *child)
			it.idxes = append(it.idxes, inplaceIndex)
			continue
		}
		it.nodes = it.nodes[:depth]
		it.idxes = it.idxes[:depth]
		if depth == 0 {
			return nullArtNode
		}
		it.idxes[depth-1]++
	}
}

func (it *baseIter) prev() artNode {
	for {
		depth := len(it.nodes) - 1
		curr := it.nodes[depth]
		idx := it.idxes[depth]
		idx--
		if idx != notExistIndex {
			var child *artNode
			switch curr.kind {
			case typeNode4:
				n4 := it.allocator.getNode4(curr.addr)
				idx = min(idx, int(n4.nodeNum)-1)
				if idx >= 0 {
					it.idxes[depth] = idx
					child = &n4.children[idx]
				} else if idx == inplaceIndex {
					it.idxes[depth] = idx
					if !n4.inplaceLeaf.addr.IsNull() {
						return n4.inplaceLeaf
					}
				} else {
					panicForInvalidIndex(idx)
				}
			case typeNode16:
				n16 := it.allocator.getNode16(curr.addr)
				idx = min(idx, int(n16.nodeNum)-1)
				if idx >= 0 {
					it.idxes[depth] = idx
					child = &n16.children[idx]
				} else if idx == inplaceIndex {
					it.idxes[depth] = idx
					if !n16.inplaceLeaf.addr.IsNull() {
						return n16.inplaceLeaf
					}
				} else {
					panicForInvalidIndex(idx)
				}
			case typeNode48:
				n48 := it.allocator.getNode48(curr.addr)
				if idx >= 0 && n48.present[idx>>n48s]&(1<<(idx%n48m)) == 0 {
					// if idx >= 0 and n48.keys[idx] is not present, goto the previous present key.
					// for idx < 0, we check inplaceLeaf later.
					idx = n48.prevPresentIdx(idx)
				}
				if idx >= 0 {
					it.idxes[depth] = idx
					child = &n48.children[n48.keys[idx]]
				} else if idx == inplaceIndex {
					it.idxes[depth] = idx
					if !n48.inplaceLeaf.addr.IsNull() {
						return n48.inplaceLeaf
					}
				} else {
					panicForInvalidIndex(idx)
				}
			case typeNode256:
				n256 := it.allocator.getNode256(curr.addr)
				if idx >= 0 && n256.present[idx>>n48s]&(1<<(idx%n48m)) == 0 {
					// if idx >= 0 and n256.keys[idx] is not present, goto the previous present key.
					// for idx < 0, we check inplaceLeaf later.
					idx = n256.prevPresentIdx(idx)
				}
				if idx >= 0 {
					it.idxes[depth] = idx
					child = &n256.children[idx]
				} else if idx == inplaceIndex {
					it.idxes[depth] = idx
					if !n256.inplaceLeaf.addr.IsNull() {
						return n256.inplaceLeaf
					}
				} else {
					panicForInvalidIndex(idx)
				}
			default:
				panic("invalid node type")
			}
			if child != nil {
				if child.kind == typeLeaf {
					return *child
				}
				it.nodes = append(it.nodes, *child)
				it.idxes = append(it.idxes, node256cap)
				continue
			}
		}
		it.nodes = it.nodes[:depth]
		it.idxes = it.idxes[:depth]
		if depth == 0 {
			return nullArtNode
		}
	}
}

func panicForInvalidIndex(idx int) {
	msg := fmt.Sprintf("ART iterator meets an invalid index %d", idx)
	panic(msg)
}
