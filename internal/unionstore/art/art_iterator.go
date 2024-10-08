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
	if it.currLeaf.vAddr.IsNull() {
		return nil
	}
	return it.tree.allocator.vlogAllocator.GetValue(it.currLeaf.vAddr)
}

// HasValue returns false if it is flags only.
func (it *Iterator) HasValue() bool {
	return !it.isFlagsOnly()
}

func (it *Iterator) isFlagsOnly() bool {
	return it.currLeaf != nil && it.currLeaf.vAddr.IsNull()
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
		if it.currLeaf.vAddr.IsNull() {
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

	if it.reverse {
		it.inner.idxes, it.inner.nodes = it.seek(upperBound)
		if len(lowerBound) == 0 {
			it.endAddr = arena.NullAddr
		} else {
			helper := new(baseIter)
			helper.allocator = &it.tree.allocator
			helper.idxes, helper.nodes = it.seek(lowerBound)
			if it.inner.compare(helper) > 0 {
				// lowerBound is inclusive, call next to find the smallest leaf node that >= lowerBound.
				it.endAddr = helper.next().addr
				if it.inner.compare(helper) < 0 || len(helper.idxes) == 0 {
					it.valid = false
				}
				return
			}
			it.valid = false
		}
		return
	}

	it.inner.idxes, it.inner.nodes = it.seek(lowerBound)
	if len(upperBound) == 0 {
		it.endAddr = arena.NullAddr
	} else {
		helper := new(baseIter)
		helper.allocator = &it.tree.allocator
		helper.idxes, helper.nodes = it.seek(upperBound)
		if it.inner.compare(helper) < 0 {
			// upperBound is exclusive, so we move the helper cursor to the previous node, which is the true endAddr.
			it.endAddr = helper.prev().addr
			if it.inner.compare(helper) > 0 || len(helper.idxes) == 0 {
				it.valid = false
			}
			return
		}
		it.valid = false
		return
	}
}

// seek the first node and index that >= key, return the indexes and nodes of the lookup path
// nodes[0] is the root node
func (it *Iterator) seek(key artKey) ([]int, []artNode) {
	curr := it.tree.root
	depth := uint32(0)
	idxes := make([]int, 0, 8)
	nodes := make([]artNode, 0, 8)
	if len(key) == 0 {
		// if the seek key is empty, it means -inf or +inf, return root node directly.
		nodes = append(nodes, curr)
		if it.reverse {
			idxes = append(idxes, node256cap)
		} else {
			idxes = append(idxes, inplaceIndex)
		}
		return idxes, nodes
	}
	var node *nodeBase
	for {
		if curr.isLeaf() {
			if key.valid(int(depth)) {
				lf := curr.asLeaf(&it.tree.allocator)
				if bytes.Compare(key, lf.GetKey()) > 0 {
					// the seek key is not exist, and it's longer and larger than the current leaf's key.
					// e.g. key: [1, 1, 1], leaf: [1, 1].
					idxes[len(idxes)-1]++
				}
			}
			break
		}

		node = curr.asNode(&it.tree.allocator)
		if node.prefixLen > 0 {
			mismatchIdx := node.matchDeep(&it.tree.allocator, &curr, key, depth)
			if mismatchIdx < node.prefixLen {
				// no leaf node is match with the seek key
				leafNode := minimum(&it.tree.allocator, curr)
				leafKey := leafNode.asLeaf(&it.tree.allocator).GetKey()
				if mismatchIdx+depth == uint32(len(key)) || key[depth+mismatchIdx] < leafKey[depth+mismatchIdx] {
					// key < leafKey, set index to -1 means all the children are larger than the seek key
					idxes = append(idxes, -1)
				} else {
					// key > leafKey, set index to 256 means all the children are less than the seek key
					idxes = append(idxes, node256cap)
				}
				nodes = append(nodes, curr)
				return idxes, nodes
			}
			depth += min(mismatchIdx, node.prefixLen)
		}

		nodes = append(nodes, curr)
		char := key.charAt(int(depth))
		idx, next := curr.findChild(&it.tree.allocator, char, !key.valid(int(depth)))
		if next.addr.IsNull() {
			nextIdx := 0
			switch curr.kind {
			case typeNode4:
				n4 := curr.asNode4(&it.tree.allocator)
				for ; nextIdx < int(n4.nodeNum); nextIdx++ {
					if n4.keys[nextIdx] >= char {
						break
					}
				}
			case typeNode16:
				n16 := curr.asNode16(&it.tree.allocator)
				nextIdx, _ = sort.Find(int(n16.nodeNum), func(i int) int {
					if n16.keys[i] < char {
						return 1
					}
					return -1
				})
			case typeNode48:
				n48 := curr.asNode48(&it.tree.allocator)
				nextIdx = n48.nextPresentIdx(int(char))
			case typeNode256:
				n256 := curr.asNode256(&it.tree.allocator)
				nextIdx = n256.nextPresentIdx(int(char))
			}
			idxes = append(idxes, nextIdx)
			return idxes, nodes
		}
		idxes = append(idxes, idx)
		curr = next
		depth++
	}
	return idxes, nodes
}

type baseIter struct {
	allocator *artAllocator
	idxes     []int
	nodes     []artNode
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
	depth := len(it.nodes) - 1
	curr := it.nodes[depth]
	idx := it.idxes[depth]
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
		if idx < int(n4.nodeNum) {
			it.idxes[depth] = idx
			child := n4.children[idx]
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, inplaceIndex)
			return it.next()
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
		if idx < int(n16.nodeNum) {
			it.idxes[depth] = idx
			child := n16.children[idx]
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, inplaceIndex)
			return it.next()
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
		if idx < node256cap {
			it.idxes[depth] = idx
			child := n48.children[n48.keys[idx]]
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, inplaceIndex)
			return it.next()
		}
	case typeNode256:
		n256 := it.allocator.getNode256(curr.addr)
		if idx == inplaceIndex {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if !n256.inplaceLeaf.addr.IsNull() {
				return n256.inplaceLeaf
			}
		} else if idx == 256 {
			break
		}
		idx = n256.nextPresentIdx(idx)
		if idx < 256 {
			it.idxes[depth] = idx
			child := n256.children[idx]
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, inplaceIndex)
			return it.next()
		}
	}
	it.nodes = it.nodes[:depth]
	it.idxes = it.idxes[:depth]
	if depth == 0 {
		return nullArtNode
	}
	it.idxes[depth-1]++
	return it.next()
}

func (it *baseIter) prev() artNode {
	depth := len(it.nodes) - 1
	curr := it.nodes[depth]
	idx := it.idxes[depth]
	idx--
	switch curr.kind {
	case typeNode4:
		n4 := it.allocator.getNode4(curr.addr)
		idx = min(idx, int(n4.nodeNum)-1)
		if idx >= 0 {
			it.idxes[depth] = idx
			child := n4.children[idx]
			if child.kind == typeLeaf {
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, node256cap)
			return it.prev()
		} else if idx == inplaceIndex {
			it.idxes[depth] = idx
			if !n4.inplaceLeaf.addr.IsNull() {
				return n4.inplaceLeaf
			}
		}
	case typeNode16:
		n16 := it.allocator.getNode16(curr.addr)
		idx = min(idx, int(n16.nodeNum)-1)
		if idx >= 0 {
			it.idxes[depth] = idx
			child := n16.children[idx]
			if child.kind == typeLeaf {
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, node256cap)
			return it.prev()
		} else if idx == inplaceIndex {
			it.idxes[depth] = idx
			if !n16.inplaceLeaf.addr.IsNull() {
				return n16.inplaceLeaf
			}
		}
	case typeNode48:
		n48 := it.allocator.getNode48(curr.addr)
		if idx >= 0 {
			idx = n48.prevPresentIdx(idx)
		}
		if idx >= 0 {
			it.idxes[depth] = idx
			child := n48.children[n48.keys[idx]]
			if child.kind == typeLeaf {
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, node256cap)
			return it.prev()
		} else if idx == inplaceIndex {
			it.idxes[depth] = idx
			if !n48.inplaceLeaf.addr.IsNull() {
				return n48.inplaceLeaf
			}
		}
	case typeNode256:
		n256 := it.allocator.getNode256(curr.addr)
		if idx >= 0 {
			idx = n256.prevPresentIdx(idx)
		}
		if idx >= 0 {
			it.idxes[depth] = idx
			child := n256.children[idx]
			if child.kind == typeLeaf {
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, node256cap)
			return it.prev()
		} else if idx == -1 {
			it.idxes[depth] = idx
			if !n256.inplaceLeaf.addr.IsNull() {
				return n256.inplaceLeaf
			}
		}
	}
	it.nodes = it.nodes[:depth]
	it.idxes = it.idxes[:depth]
	if depth == 0 {
		return nullArtNode
	}
	return it.prev()
}
