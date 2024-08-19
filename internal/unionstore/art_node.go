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

package unionstore

import (
	"bytes"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/tikv/client-go/v2/kv"
)

type artNodeKind uint16

const (
	typeARTInvalid artNodeKind = 0
	typeARTNode4   artNodeKind = 1
	typeARTNode16  artNodeKind = 2
	typeARTNode48  artNodeKind = 3
	typeARTNode256 artNodeKind = 4
	typeARTLeaf    artNodeKind = 5
)

const (
	artMaxPrefixLen  = 20
	artNode4cap      = 4
	artNode16cap     = 16
	artNode48cap     = 48
	artNode256cap    = 256
	artInplaceIndex  = -1
	artNotExistIndex = -2
)

const (
	artNode4size   = int(unsafe.Sizeof(artNode4{}))
	artNode16size  = int(unsafe.Sizeof(artNode16{}))
	artNode48size  = int(unsafe.Sizeof(artNode48{}))
	artNode256size = int(unsafe.Sizeof(artNode256{}))
	artLeafSize    = int(unsafe.Sizeof(artLeaf{}))
)

var (
	// nullNode48 is used to initialize the artNode48
	nullNode48 = artNode48{}
	// nullNode256 is used to initialize the artNode256
	nullNode256 = artNode256{}
)

var nullArtNode = artNode{kind: typeARTInvalid, addr: nullAddr}

func init() {
	for i := 0; i < artNode48cap; i++ {
		nullNode48.children[i] = artNode{kind: typeARTInvalid, addr: nullAddr}
	}
	for i := 0; i < artNode256cap; i++ {
		nullNode256.children[i] = artNode{kind: typeARTInvalid, addr: nullAddr}
	}
}

type artKey []byte

type artNode struct {
	kind artNodeKind
	addr memdbArenaAddr
}

type artNodeBase struct {
	nodeNum     uint8
	prefixLen   uint32
	prefix      [artMaxPrefixLen]byte
	inplaceLeaf artNode
}

func (n *artNodeBase) init() {
	// initialize basic artNodeBase
	n.nodeNum = 0
	n.prefixLen = 0
	n.inplaceLeaf = nullArtNode
}

type artNode4 struct {
	artNodeBase
	keys     [artNode4cap]byte
	children [artNode4cap]artNode
}

type artNode16 struct {
	artNodeBase
	keys     [artNode16cap]byte
	children [artNode16cap]artNode
}

// artN48s and artN48m are used to calculate the artNode48's and artNode256's present index and bit index.
// Given idx between 0 and 255:
//
//	present index = idx >> artN48s
//	bit index = idx % artN48m
const (
	artN48s = 6  // 2^artN48s == artN48m
	artN48m = 64 // it should be sizeof(artNode48.present[0])
)

type artNode48 struct {
	artNodeBase
	// present is a bitmap to indicate the existence of children.
	// The bitmap is divided into 4 uint64 mainly aims to speed up the iterator.
	present  [4]uint64
	keys     [artNode256cap]uint8 // map byte to index
	children [artNode48cap]artNode
}

type artNode256 struct {
	artNodeBase
	// present is a bitmap to indicate the existence of children.
	// The bitmap is divided into 4 uint64 mainly aims to speed up the iterator.
	present  [4]uint64
	children [artNode256cap]artNode
}

type artLeaf struct {
	vAddr memdbArenaAddr
	klen  uint16
	flags uint16
}

func (an *artNode) isLeaf() bool {
	return an.kind == typeARTLeaf
}

func (an *artNode) leaf(a *artAllocator) *artLeaf {
	return a.getLeaf(an.addr)
}

// node gets the inner baseNode of the artNode
func (an *artNode) node(a *artAllocator) *artNodeBase {
	switch an.kind {
	case typeARTNode4:
		return &an.node4(a).artNodeBase
	case typeARTNode16:
		return &an.node16(a).artNodeBase
	case typeARTNode48:
		return &an.node48(a).artNodeBase
	case typeARTNode256:
		return &an.node256(a).artNodeBase
	default:
		panic("invalid artNodeBase kind")
	}
}

// at returns the nth child of the node.
func (an *artNode) at(a *artAllocator, idx int) artNode {
	switch an.kind {
	case typeARTNode4:
		return an.node4(a).children[idx]
	case typeARTNode16:
		return an.node16(a).children[idx]
	case typeARTNode48:
		n48 := an.node48(a)
		return n48.children[n48.keys[idx]]
	case typeARTNode256:
		return an.node256(a).children[idx]
	default:
		panic("invalid artNodeBase kind")
	}
}

func (n48 *artNode48) init() {
	// initialize artNodeBase
	n48.artNodeBase.init()
	// initialize artNode48
	n48.present[0], n48.present[1], n48.present[2], n48.present[3] = 0, 0, 0, 0
	copy(n48.children[:], nullNode48.children[:])
}

// nextPresentIdx returns the next present index starting from the given index.
// Finding from present bitmap is faster than from keys.
func (n48 *artNode48) nextPresentIdx(start int) int {
	for presentOffset := start >> artN48s; presentOffset < 4; presentOffset++ {
		present := n48.present[presentOffset]
		offset := start % artN48m
		start = 0
		mask := math.MaxUint64 - (uint64(1) << offset) + 1 // e.g. offset=3 => 0b111...111000
		curr := present & mask
		zeros := bits.TrailingZeros64(curr)
		if zeros < artN48m {
			return presentOffset*artN48m + zeros
		}
	}
	return artNode256cap
}

// prevPresentIdx returns the next present index starting from the given index.
// Finding from present bitmap is faster than from keys.
func (n48 *artNode48) prevPresentIdx(start int) int {
	for presentOffset := start >> artN48s; presentOffset >= 0; presentOffset-- {
		present := n48.present[presentOffset]
		offset := start % artN48m
		start = artN48m - 1
		mask := uint64(1)<<(offset+1) - 1 // e.g. offset=3 => 0b000...0001111
		curr := present & mask
		zeros := bits.LeadingZeros64(curr)
		if zeros < artN48m {
			return presentOffset*artN48m + artN48m - (zeros + 1)
		}
	}
	return artInplaceIndex
}

func (n256 *artNode256) init() {
	// initialize artNodeBase
	n256.artNodeBase.init()
	// initialize artNode256
	copy(n256.children[:], nullNode256.children[:])
}

func (n256 *artNode256) nextPresentIdx(start int) int {
	for presentOffset := start >> artN48s; presentOffset < 4; presentOffset++ {
		present := n256.present[presentOffset]
		offset := start % artN48m
		start = 0
		mask := math.MaxUint64 - (uint64(1) << offset) + 1 // e.g. offset=3 => 0b111...111000
		curr := present & mask
		zeros := bits.TrailingZeros64(curr)
		if zeros < artN48m {
			return presentOffset*artN48m + zeros
		}
	}
	return artNode256cap
}

func (n256 *artNode256) prevPresentIdx(start int) int {
	for presentOffset := start >> artN48s; presentOffset >= 0; presentOffset-- {
		present := n256.present[presentOffset]
		offset := start % artN48m
		start = artN48m - 1
		mask := uint64(1)<<(offset+1) - 1 // e.g. offset=3 => 0b000...0001111
		curr := present & mask
		zeros := bits.LeadingZeros64(curr)
		if zeros < artN48m {
			return presentOffset*artN48m + artN48m - (zeros + 1)
		}
	}
	return artInplaceIndex
}

// key methods

func (k artKey) charAt(pos int) byte {
	if pos < 0 || pos >= len(k) {
		return 0
	}
	return k[pos]
}

func (k artKey) valid(pos int) bool {
	return pos < len(k)
}

// artLeaf methods

// getKey gets the full key of the artLeaf
func (l *artLeaf) getKey() []byte {
	base := unsafe.Add(unsafe.Pointer(l), artLeafSize)
	return unsafe.Slice((*byte)(base), int(l.klen))
}

// getKeyDepth gets the partial key start from depth of the artLeaf
func (l *artLeaf) getKeyDepth(depth uint32) []byte {
	base := unsafe.Add(unsafe.Pointer(l), artLeafSize+int(depth))
	return unsafe.Slice((*byte)(base), int(l.klen)-int(depth))
}

func (l *artLeaf) match(depth uint32, key artKey) bool {
	return bytes.Equal(l.getKeyDepth(depth), key[depth:])
}

func (l *artLeaf) setKeyFlags(flags kv.KeyFlags) memdbArenaAddr {
	l.flags = uint16(flags) & flagMask
	return l.vAddr
}

func (l *artLeaf) getKeyFlags() kv.KeyFlags {
	return kv.KeyFlags(l.flags & flagMask)
}

const (
	deleteFlag uint16 = 1 << 15
	flagMask          = ^deleteFlag
)

// markDelete marks the artLeaf as deleted
func (l *artLeaf) markDelete() {
	l.flags = deleteFlag
}

//nolint:unused
func (l *artLeaf) unmarkDelete() {
	l.flags &= flagMask
}

func (l *artLeaf) isDeleted() bool {
	return l.flags&deleteFlag != 0
}

// Node methods
func (n *artNodeBase) setPrefix(key artKey, prefixLen uint32) {
	n.prefixLen = prefixLen
	copy(n.prefix[:], key[:min(prefixLen, artMaxPrefixLen)])
}

func (n *artNodeBase) match(key artKey, depth uint32) uint32 {
	idx := uint32(0)

	limit := min(min(n.prefixLen, artMaxPrefixLen), uint32(len(key))-depth)
	for ; idx < limit; idx++ {
		if n.prefix[idx] != key[idx+depth] {
			return idx
		}
	}

	return idx
}

func (an *artNode) node4(a *artAllocator) *artNode4 {
	return a.getNode4(an.addr)
}

func (an *artNode) node16(a *artAllocator) *artNode16 {
	return a.getNode16(an.addr)
}

func (an *artNode) node48(a *artAllocator) *artNode48 {
	return a.getNode48(an.addr)
}

func (an *artNode) node256(a *artAllocator) *artNode256 {
	return a.getNode256(an.addr)
}

func (an *artNode) matchDeep(a *artAllocator, key artKey, depth uint32) uint32 /* mismatch index*/ {
	n := an.node(a)
	mismatchIdx := n.match(key, depth)
	if mismatchIdx < min(n.prefixLen, artMaxPrefixLen) {
		return mismatchIdx
	}

	leafArtNode := minimum(a, *an)
	lKey := leafArtNode.leaf(a).getKey()
	return longestCommonPrefix(lKey, key, depth)
}

func longestCommonPrefix(l1Key, l2Key artKey, depth uint32) uint32 {
	idx, limit := depth, min(uint32(len(l1Key)), uint32(len(l2Key)))
	// TODO: possible optimization
	// Compare the key by loop can be very slow if the final LCP is large.
	// Maybe optimize it by comparing the key in chunks if the limit exceeds certain threshold.
	for ; idx < limit; idx++ {
		if l1Key[idx] != l2Key[idx] {
			break
		}
	}
	return idx - depth
}

// Find the minimum artLeaf under an artNode
func minimum(a *artAllocator, an artNode) artNode {
	for {
		switch an.kind {
		case typeARTLeaf:
			return an
		case typeARTNode4:
			n4 := an.node4(a)
			if !n4.inplaceLeaf.addr.isNull() {
				return n4.inplaceLeaf
			}
			an = n4.children[0]
		case typeARTNode16:
			n16 := an.node16(a)
			if !n16.inplaceLeaf.addr.isNull() {
				return n16.inplaceLeaf
			}
			an = n16.children[0]
		case typeARTNode48:
			n48 := an.node48(a)
			if !n48.inplaceLeaf.addr.isNull() {
				return n48.inplaceLeaf
			}
			idx := n48.nextPresentIdx(0)
			an = n48.children[n48.keys[idx]]
		case typeARTNode256:
			n256 := an.node256(a)
			if !n256.inplaceLeaf.addr.isNull() {
				return n256.inplaceLeaf
			}
			idx := n256.nextPresentIdx(0)
			an = n256.children[idx]
		case typeARTInvalid:
			return nullArtNode
		}
	}
}

func (an *artNode) findChild(a *artAllocator, c byte, valid bool) (int, artNode) {
	if !valid {
		return artInplaceIndex, an.node(a).inplaceLeaf
	}
	switch an.kind {
	case typeARTNode4:
		return an.findChild4(a, c)
	case typeARTNode16:
		return an.findChild16(a, c)
	case typeARTNode48:
		return an.findChild48(a, c)
	case typeARTNode256:
		return an.findChild256(a, c)
	}
	return artNotExistIndex, nullArtNode
}

func (an *artNode) findChild4(a *artAllocator, c byte) (int, artNode) {
	n4 := an.node4(a)
	for idx := 0; idx < int(n4.nodeNum); idx++ {
		if n4.keys[idx] == c {
			return idx, n4.children[idx]
		}
	}

	return -2, nullArtNode
}

func (an *artNode) findChild16(a *artAllocator, c byte) (int, artNode) {
	n16 := an.node16(a)

	idx, found := sort.Find(int(n16.nodeNum), func(i int) int {
		if n16.keys[i] < c {
			return 1
		}
		if n16.keys[i] == c {
			return 0
		}
		return -1
	})
	if found {
		return idx, n16.children[idx]
	}
	return -2, nullArtNode
}

func (an *artNode) findChild48(a *artAllocator, c byte) (int, artNode) {
	n48 := an.node48(a)
	if n48.present[c>>artN48s]&(1<<(c%artN48m)) != 0 {
		return int(c), n48.children[n48.keys[c]]
	}
	return -2, nullArtNode
}

func (an *artNode) findChild256(a *artAllocator, c byte) (int, artNode) {
	n256 := an.node256(a)
	return int(c), n256.children[c]
}

func (an *artNode) swapChild(a *artAllocator, c byte, child artNode) {
	switch an.kind {
	case typeARTNode4:
		n4 := an.node4(a)
		for idx := uint8(0); idx < n4.nodeNum; idx++ {
			if n4.keys[idx] == c {
				n4.children[idx] = child
				return
			}
		}
		panic("swap child failed")
	case typeARTNode16:
		n16 := an.node16(a)
		for idx := uint8(0); idx < n16.nodeNum; idx++ {
			if n16.keys[idx] == c {
				n16.children[idx] = child
				return
			}
		}
		panic("swap child failed")
	case typeARTNode48:
		n48 := an.node48(a)
		if n48.present[c>>artN48s]&(1<<(c%artN48m)) != 0 {
			n48.children[n48.keys[c]] = child
			return
		}
		panic("swap child failed")
	case typeARTNode256:
		an.addChild256(a, c, false, child)
	}
}

// addChild adds a child to the node.
// the added index `c` should not exist.
func (an *artNode) addChild(a *artAllocator, c byte, inplace bool, child artNode) bool {
	switch an.kind {
	case typeARTNode4:
		return an.addChild4(a, c, inplace, child)
	case typeARTNode16:
		return an.addChild16(a, c, inplace, child)
	case typeARTNode48:
		return an.addChild48(a, c, inplace, child)
	case typeARTNode256:
		return an.addChild256(a, c, inplace, child)
	}
	return false
}

func (an *artNode) addChild4(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node4(a)
	if node.nodeNum >= artNode4cap {
		an.grow(a)
		an.addChild(a, c, inplace, child)
		return true
	}

	if inplace {
		node.inplaceLeaf = child
		return false
	}

	i := uint8(0)
	for ; i < node.nodeNum; i++ {
		if c < node.keys[i] {
			break
		}
	}

	if i < node.nodeNum {
		for j := node.nodeNum; j > i; j-- {
			node.keys[j] = node.keys[j-1]
			node.children[j] = node.children[j-1]
		}
	}
	node.keys[i] = c
	node.children[i] = child
	node.nodeNum++
	if node.keys[0] == 49 && node.keys[1] == 49 {
		a := 1
		_ = a
	}
	return false
}

func (an *artNode) addChild16(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node16(a)
	if node.nodeNum >= artNode16cap {
		an.grow(a)
		an.addChild(a, c, inplace, child)
		return true
	}

	if inplace {
		node.inplaceLeaf = child
		return false
	}

	i, _ := sort.Find(int(node.nodeNum), func(i int) int {
		if node.keys[i] < c {
			return 1
		}
		if node.keys[i] == c {
			return 0
		}
		return -1
	})

	if i < int(node.nodeNum) {
		copy(node.keys[i+1:node.nodeNum+1], node.keys[i:node.nodeNum])
		copy(node.children[i+1:node.nodeNum+1], node.children[i:node.nodeNum])
	}

	node.keys[i] = c
	node.children[i] = child
	node.nodeNum++
	return false
}

func (an *artNode) addChild48(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node48(a)
	if node.nodeNum >= artNode48cap {
		an.grow(a)
		an.addChild(a, c, inplace, child)
		return true
	}

	if inplace {
		node.inplaceLeaf = child
		return false
	}

	node.keys[c] = node.nodeNum
	node.present[c>>artN48s] |= 1 << (c % artN48m)
	node.children[node.nodeNum] = child
	node.nodeNum++
	return false
}

func (an *artNode) addChild256(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node256(a)

	if inplace {
		node.inplaceLeaf = child
		return false
	}

	node.present[c>>artN48s] |= 1 << (c % artN48m)
	node.children[c] = child
	node.nodeNum++
	return false
}

func (n *artNodeBase) copyMeta(src *artNodeBase) {
	n.nodeNum = src.nodeNum
	n.prefixLen = src.prefixLen
	n.inplaceLeaf = src.inplaceLeaf
	copy(n.prefix[:], src.prefix[:])
}

func (an *artNode) grow(a *artAllocator) {
	switch an.kind {
	case typeARTNode4:
		n4 := an.node4(a)
		newAddr, n16 := a.allocNode16()
		n16.copyMeta(&n4.artNodeBase)

		copy(n16.keys[:], n4.keys[:])
		copy(n16.children[:], n4.children[:])

		// replace addr and free artNode4
		a.freeNode4(an.addr)
		an.kind = typeARTNode16
		an.addr = newAddr
	case typeARTNode16:
		n16 := an.node16(a)
		newAddr, n48 := a.allocNode48()
		n48.copyMeta(&n16.artNodeBase)

		var numChildren uint8
		for i := uint8(0); i < n16.artNodeBase.nodeNum; i++ {
			ch := n16.keys[i]
			n48.keys[ch] = numChildren
			n48.present[ch>>artN48s] |= 1 << (ch % artN48m)
			n48.children[numChildren] = n16.children[i]
			numChildren++
		}

		// replace addr and free artNode16
		a.freeNode16(an.addr)
		an.kind = typeARTNode48
		an.addr = newAddr
	case typeARTNode48:
		n48 := an.node48(a)
		newAddr, n256 := a.allocNode256()
		n256.copyMeta(&n48.artNodeBase)

		for i := n48.nextPresentIdx(0); i < artNode256cap; i = n48.nextPresentIdx(i + 1) {
			n256.children[i] = n48.children[n48.keys[i]]
		}
		copy(n256.present[:], n48.present[:])

		// replace addr and free artNode48
		a.freeNode48(an.addr)
		an.kind = typeARTNode256
		an.addr = newAddr
	}
}
