package art

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/kv"
)

type ArtIterator struct {
	tree         *Art
	reverse      bool
	valid        bool
	inner        *baseIter
	endAddr      nodeAddr
	currLeaf     *leaf
	currAddr     nodeAddr
	includeFlags bool
}

func (t *Art) IterWithFlags(start []byte, end []byte) *ArtIterator {
	it, _ := t.iter(start, end, false, true)
	return it
}

func (t *Art) IterReverseWithFlags(end []byte) *ArtIterator {
	it, _ := t.iter(nil, end, true, true)
	return it
}

func (t *Art) Iter(lowerBound, upperBound []byte) (*ArtIterator, error) {
	return t.iter(lowerBound, upperBound, false, false)
}

// IterReverse creates a reversed Iterator, the given key should be the upper bound.
func (t *Art) IterReverse(upperBound, lowerBound []byte) (*ArtIterator, error) {
	return t.iter(lowerBound, upperBound, true, false)
}

func (t *Art) iter(lowerBound, upperBound []byte, reverse, includeFlags bool) (*ArtIterator, error) {
	i := &ArtIterator{
		tree:    t,
		reverse: reverse,
		valid:   true,
		inner: &baseIter{
			allocator: &t.allocator,
		},
		endAddr:      nullAddr,
		includeFlags: includeFlags,
	}
	i.init(lowerBound, upperBound)
	if !i.valid {
		return i, nil
	}
	if err := i.Next(); err != nil {
		return nil, err
	}
	return i, nil
}

func (it *ArtIterator) init(lowerBound, upperBound []byte) {
	if it.tree.root.addr.isNull() {
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
			it.endAddr = nullAddr // minimum(&it.tree.allocator, it.tree.root).addr
		} else {
			helper := new(baseIter)
			helper.allocator = &it.tree.allocator
			helper.idxes, helper.nodes = it.seek(lowerBound)

			if it.inner.Compare(helper) > 0 {
				it.endAddr = helper.next().addr
				if it.inner.Compare(helper) < 0 || len(helper.idxes) == 0 {
					it.valid = false
				}
				return
			}
			it.valid = false
			return
		}
	} else {
		it.inner.idxes, it.inner.nodes = it.seek(lowerBound)
		if len(upperBound) == 0 {
			it.endAddr = nullAddr // maximum(&it.tree.allocator, it.tree.root).addr
		} else {
			helper := new(baseIter)
			helper.allocator = &it.tree.allocator
			helper.idxes, helper.nodes = it.seek(upperBound)
			if it.inner.Compare(helper) < 0 {
				it.endAddr = helper.prev().addr
				if it.inner.Compare(helper) > 0 || len(helper.idxes) == 0 {
					it.valid = false
				}
				return
			}
			it.valid = false
			return
		}
	}
}

func (it *ArtIterator) Valid() bool { return it.valid }

func (it *ArtIterator) Key() []byte {
	return it.currLeaf.getKey()
}

func (it *ArtIterator) Flags() kv.KeyFlags {
	return it.currLeaf.getKeyFlags()
}

func (it *ArtIterator) Value() []byte {
	return it.tree.getValue(it.currLeaf)
}

// HasValue returns false if it is flags only.
func (it *ArtIterator) HasValue() bool {
	return !it.isFlagsOnly()
}

func (it *ArtIterator) isFlagsOnly() bool {
	return it.currLeaf != nil && it.currLeaf.vAddr.isNull()
}

// seek the first leaf node that >= key
// return the indexes and nodes in the path
// nodes[0] is the root node
// nodes[len(nodes)-1][indexes[len(indexes)-1]] is the target leaf node
func (it *ArtIterator) seek(key Key) ([]int, []artNode) {
	curr := it.tree.root
	depth := uint32(0)
	idxes := make([]int, 0, 8)
	nodes := make([]artNode, 0, 8)
	if len(key) == 0 {
		nodes = append(nodes, curr)
		if it.reverse {
			idxes = append(idxes, node256cap)
		} else {
			idxes = append(idxes, -1)
		}
		return idxes, nodes
	}
	for {
		if curr.isLeaf() {
			if key.valid(int(depth)) {
				lf := curr.leaf(&it.tree.allocator)
				if bytes.Compare(key, lf.getKey()) > 0 {
					// key not exists, but longer and larger than the current leaf.
					idxes[len(idxes)-1]++
				}
			}
			break
		}

		node := curr.node(&it.tree.allocator)
		if node.prefixLen > 0 {
			mismatchIdx := curr.matchDeep(&it.tree.allocator, key, depth)
			if mismatchIdx < uint32(node.prefixLen) {
				leafNode := minimum(&it.tree.allocator, curr)
				leafKey := leafNode.leaf(&it.tree.allocator).getKey()
				if mismatchIdx+depth == uint32(len(key)) || key[depth+mismatchIdx] < leafKey[depth+mismatchIdx] {
					idxes = append(idxes, -1)
					nodes = append(nodes, curr)
				} else {
					idxes = append(idxes, node256cap)
					nodes = append(nodes, curr)
				}
				return idxes, nodes
			}
			depth += min(mismatchIdx, uint32(node.prefixLen))
		}

		nodes = append(nodes, curr)
		char := key.charAt(int(depth))
		idx, next := curr.findChild(&it.tree.allocator, char, key.valid(int(depth)))
		if next.addr == nullAddr {
			var near int
			switch curr.kind {
			case typeNode4:
				n4 := curr.node4(&it.tree.allocator)
				var i int
				i = 0
				for ; i < int(n4.nodeNum); i++ {
					if n4.keys[i] >= char {
						break
					}
				}
				near = i
			case typeNode16:
				n16 := curr.node16(&it.tree.allocator)
				var i int
				i = 0
				for ; i < int(n16.nodeNum); i++ {
					if n16.keys[i] >= char {
						break
					}
				}
				near = i
			case typeNode48:
				n48 := curr.node48(&it.tree.allocator)
				near = n48.nextPresentIdx(int(char))
			case typeNode256:
				n256 := curr.node256(&it.tree.allocator)
				near = n256.nextPresentIdx(int(char))
			}
			idxes = append(idxes, near)
			return idxes, nodes
		}
		idxes = append(idxes, idx)
		curr = next
		depth++
	}
	return idxes, nodes
}

func (it *ArtIterator) Next() error {
	if !it.valid {
		// iterate is finished
		return errors.New("Art: iterator is finished")
	}
	// the default currAddr is (0, 0), which is root node(it's not a leaf in all time), so endAddr is never (0, 0).
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
		if nextLeaf == nullArtNode {
			it.valid = false
			return nil
		}
		it.setCurrLeaf(nextLeaf.addr)
		if it.currLeaf.vAddr.isNull() {
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

func (it *ArtIterator) setCurrLeaf(node nodeAddr) {
	it.currAddr = node
	it.currLeaf = it.tree.allocator.getLeaf(node)
}

func (it *ArtIterator) Close() {
	it.currLeaf = nil
	it.inner.close()
}

// baseIter is the helper tool for ArtIterator
// it only provides the next and prev method
type baseIter struct {
	allocator *artAllocator
	idxes     []int
	nodes     []artNode
}

func (it *baseIter) Compare(other *baseIter) int {
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
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n4.inplaceLeaf.addr != nullAddr {
				return n4.inplaceLeaf
			}
		} else if idx == node4cap {
			break
		}
		idx = n4.nextPresentIdx(idx)
		if idx < int(n4.nodeNum) {
			it.idxes[depth] = idx
			child := n4.children[idx]
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, -1)
			return it.next()
		}
	case typeNode16:
		n16 := it.allocator.getNode16(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n16.inplaceLeaf.addr != nullAddr {
				return n16.inplaceLeaf
			}
		} else if idx == node16cap {
			break
		}
		idx = n16.nextPresentIdx(idx)
		if idx < int(n16.nodeNum) {
			it.idxes[depth] = idx
			child := n16.children[idx]
			if child.kind == typeLeaf {
				it.idxes[depth]++
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, -1)
			return it.next()
		}
	case typeNode48:
		n48 := it.allocator.getNode48(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n48.inplaceLeaf.addr != nullAddr {
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
			it.idxes = append(it.idxes, -1)
			return it.next()
		}
	case typeNode256:
		n256 := it.allocator.getNode256(curr.addr)
		if idx == -1 {
			idx = 0 // mark in-place leaf is visited
			it.idxes[depth] = idx
			if n256.inplaceLeaf.addr != nullAddr {
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
			it.idxes = append(it.idxes, -1)
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

		if idx >= 0 {
			idx = n4.prevPresentIdx(idx)
		}

		if idx >= 0 {
			it.idxes[depth] = idx
			child := n4.children[idx]
			if child.kind == typeLeaf {
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, node256cap)
			return it.prev()
		} else if idx == -1 {
			it.idxes[depth] = idx
			if n4.inplaceLeaf.addr != nullAddr {
				return n4.inplaceLeaf
			}
		} else if idx == -2 {
			break
		}
	case typeNode16:
		n16 := it.allocator.getNode16(curr.addr)

		if idx >= 0 {
			idx = n16.prevPresentIdx(idx)
		}

		if idx >= 0 {
			it.idxes[depth] = idx
			child := n16.children[idx]
			if child.kind == typeLeaf {
				return child
			}
			it.nodes = append(it.nodes, child)
			it.idxes = append(it.idxes, node256cap)
			return it.prev()
		} else if idx == -1 {
			it.idxes[depth] = idx
			if n16.inplaceLeaf.addr != nullAddr {
				return n16.inplaceLeaf
			}
		} else if idx == -2 {
			break
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
		} else if idx == -1 {
			it.idxes[depth] = idx
			if n48.inplaceLeaf.addr != nullAddr {
				return n48.inplaceLeaf
			}
		} else if idx == -2 {
			break
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
			if n256.inplaceLeaf.addr != nullAddr {
				return n256.inplaceLeaf
			}
		} else if idx == -2 {
			break
		}
	}
	it.nodes = it.nodes[:depth]
	it.idxes = it.idxes[:depth]
	if depth == 0 {
		return nullArtNode
	}
	return it.prev()
}

func (it *baseIter) close() {
	it.nodes = it.nodes[:0]
	it.idxes = it.idxes[:0]
}

type ArtMemKeyHandle struct {
	UserData uint16
	idx      uint16
	off      uint32
}

func (it *ArtIterator) Handle() ArtMemKeyHandle {
	return ArtMemKeyHandle{
		idx: uint16(it.currAddr.idx),
		off: it.currAddr.off,
	}
}

func (h ArtMemKeyHandle) toAddr() nodeAddr {
	return nodeAddr{idx: uint32(h.idx), off: h.off}
}
