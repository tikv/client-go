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
	"math"
	"math/bits"
	"runtime"
	"sort"
	"testing"
	"unsafe"

	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/kv"
)

type nodeKind uint16

const (
	typeInvalid nodeKind = 0
	typeNode4   nodeKind = 1
	typeNode16  nodeKind = 2
	typeNode48  nodeKind = 3
	typeNode256 nodeKind = 4
	typeLeaf    nodeKind = 5
)

const (
	// maxInNodePrefixLen is the maximum length of the prefix stored within an art node.
	maxInNodePrefixLen = 20
	node4cap           = 4
	node16cap          = 16
	node48cap          = 48
	node256cap         = 256
	// inplaceIndex is a special index to indicate the index of an in-place leaf in a node,
	// the in-place leaf has the same key with its parent node and doesn't occupy the quota of the node.
	// the other valid index of a node is [0, nodeNum), all the other leaves in the node have larger key than the in-place leaf.
	inplaceIndex  = -1
	notExistIndex = -2
)

const (
	node4size   = int(unsafe.Sizeof(node4{}))
	node16size  = int(unsafe.Sizeof(node16{}))
	node48size  = int(unsafe.Sizeof(node48{}))
	node256size = int(unsafe.Sizeof(node256{}))
	leafSize    = int(unsafe.Sizeof(artLeaf{}))
)

var nullArtNode = artNode{kind: typeInvalid, addr: arena.NullAddr}

type artKey []byte

type artNode struct {
	kind nodeKind
	addr arena.MemdbArenaAddr
}

type nodeBase struct {
	nodeNum     uint8
	prefixLen   uint32
	prefix      [maxInNodePrefixLen]byte
	inplaceLeaf artNode
}

func (n *nodeBase) init() {
	// initialize basic nodeBase
	n.nodeNum = 0
	n.prefixLen = 0
	n.inplaceLeaf = nullArtNode
}

type node4 struct {
	nodeBase
	keys     [node4cap]byte
	children [node4cap]artNode
}

type node16 struct {
	nodeBase
	keys     [node16cap]byte
	children [node16cap]artNode
}

// n48s and n48m are used to calculate the node48's and node256's present index and bit index.
// Given idx between 0 and 255:
//
//	present index = idx >> n48s
//	bit index = idx % n48m
const (
	n48s = 6  // 2^n48s == n48m
	n48m = 64 // it should be sizeof(node48.present[0])
)

type node48 struct {
	nodeBase
	// present is a bitmap to indicate the existence of children.
	// The bitmap is divided into 4 uint64 mainly aims to speed up the iterator.
	present  [4]uint64
	keys     [node256cap]uint8 // map byte to index
	children [node48cap]artNode
}

type node256 struct {
	nodeBase
	// present is a bitmap to indicate the existence of children.
	// The bitmap is divided into 4 uint64 mainly aims to speed up the iterator.
	present  [4]uint64
	children [node256cap]artNode
}

const MaxKeyLen = math.MaxUint16

type artLeaf struct {
	vLogAddr arena.MemdbArenaAddr
	keyLen   uint16
	flags    uint16
}

func (an *artNode) isLeaf() bool {
	return an.kind == typeLeaf
}

func (an *artNode) asLeaf(a *artAllocator) *artLeaf {
	return a.getLeaf(an.addr)
}

// node gets the inner baseNode of the artNode
func (an *artNode) asNode(a *artAllocator) *nodeBase {
	switch an.kind {
	case typeNode4:
		return &an.asNode4(a).nodeBase
	case typeNode16:
		return &an.asNode16(a).nodeBase
	case typeNode48:
		return &an.asNode48(a).nodeBase
	case typeNode256:
		return &an.asNode256(a).nodeBase
	default:
		panic("invalid nodeBase kind")
	}
}

// at returns the nth child of the node, used for test.
func (an *artNode) at(a *artAllocator, idx int) artNode {
	switch an.kind {
	case typeNode4:
		return an.asNode4(a).children[idx]
	case typeNode16:
		return an.asNode16(a).children[idx]
	case typeNode48:
		n48 := an.asNode48(a)
		return n48.children[n48.keys[idx]]
	case typeNode256:
		return an.asNode256(a).children[idx]
	default:
		panic("invalid nodeBase kind")
	}
}

func (n48 *node48) init() {
	// initialize nodeBase
	n48.nodeBase.init()
	// initialize node48
	n48.present[0], n48.present[1], n48.present[2], n48.present[3] = 0, 0, 0, 0
}

// nextPresentIdx returns the next present index starting from the given index.
// Finding from present bitmap is faster than from keys.
func (n48 *node48) nextPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset < 4; presentOffset++ {
		offset := start % n48m
		start = 0
		mask := math.MaxUint64 - (uint64(1) << offset) + 1 // e.g. offset=3 => 0b111...111000
		curr := n48.present[presentOffset] & mask
		zeros := bits.TrailingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + zeros
		}
	}
	return node256cap
}

// prevPresentIdx returns the next present index starting from the given index.
// Finding from present bitmap is faster than from keys.
func (n48 *node48) prevPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset >= 0; presentOffset-- {
		offset := start % n48m
		start = n48m - 1
		mask := uint64(1)<<(offset+1) - 1 // e.g. offset=3 => 0b000...0001111
		curr := n48.present[presentOffset] & mask
		zeros := bits.LeadingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + n48m - (zeros + 1)
		}
	}
	return inplaceIndex
}

func (n256 *node256) init() {
	// initialize nodeBase
	n256.nodeBase.init()
	// initialize node256
	n256.present[0], n256.present[1], n256.present[2], n256.present[3] = 0, 0, 0, 0
}

func (n256 *node256) nextPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset < 4; presentOffset++ {
		offset := start % n48m
		start = 0
		mask := math.MaxUint64 - (uint64(1) << offset) + 1 // e.g. offset=3 => 0b111...111000
		curr := n256.present[presentOffset] & mask
		zeros := bits.TrailingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + zeros
		}
	}
	return node256cap
}

func (n256 *node256) prevPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset >= 0; presentOffset-- {
		offset := start % n48m
		start = n48m - 1
		mask := uint64(1)<<(offset+1) - 1 // e.g. offset=3 => 0b000...0001111
		curr := n256.present[presentOffset] & mask
		zeros := bits.LeadingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + n48m - (zeros + 1)
		}
	}
	return inplaceIndex
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

// GetKey gets the full key of the leaf
func (l *artLeaf) GetKey() []byte {
	base := unsafe.Add(unsafe.Pointer(l), leafSize)
	return unsafe.Slice((*byte)(base), int(l.keyLen))
}

// getKeyDepth gets the partial key start from depth of the artLeaf
func (l *artLeaf) getKeyDepth(depth uint32) []byte {
	base := unsafe.Add(unsafe.Pointer(l), leafSize+int(depth))
	return unsafe.Slice((*byte)(base), int(l.keyLen)-int(depth))
}

func (l *artLeaf) match(depth uint32, key artKey) bool {
	return bytes.Equal(l.getKeyDepth(depth), key[depth:])
}

func (l *artLeaf) setKeyFlags(flags kv.KeyFlags) {
	l.flags = uint16(flags) & flagMask
}

// GetKeyFlags gets the flags of the leaf
func (l *artLeaf) GetKeyFlags() kv.KeyFlags {
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

// node methods
func (n *nodeBase) setPrefix(key artKey, prefixLen uint32) {
	n.prefixLen = prefixLen
	copy(n.prefix[:], key[:min(prefixLen, maxInNodePrefixLen)])
}

// match returns the mismatch index of the key and the node's prefix within maxInNodePrefixLen.
// Node if the nodeBase.prefixLen > maxInNodePrefixLen and the returned mismatch index equals to maxInNodePrefixLen,
// key[maxInNodePrefixLen:] will not be checked by this function.
func (n *nodeBase) match(key artKey, depth uint32) uint32 /* mismatch index */ {
	return longestCommonPrefix(key[depth:], n.prefix[:min(n.prefixLen, maxInNodePrefixLen)], 0)
}

// matchDeep returns the mismatch index of the key and the node's prefix.
// If the key is fully match, the mismatch index is equal to the nodeBase.prefixLen.
// The nodeBase only stores prefix within maxInNodePrefixLen, if it's not enough for matching,
// the matchDeep func looks up and match by the leaf, this function always returns the actual mismatch index.
func (n *nodeBase) matchDeep(a *artAllocator, an *artNode, key artKey, depth uint32) uint32 /* mismatch index */ {
	// Match in-node prefix, if the whole prefix is longer than maxInNodePrefixLen, we need to match deeper.
	mismatchIdx := n.match(key, depth)
	if mismatchIdx < maxInNodePrefixLen || n.prefixLen <= maxInNodePrefixLen {
		return mismatchIdx
	}
	if mismatchIdx > maxInNodePrefixLen {
		panic(fmt.Sprintf("matchDeep invalid state, the mismatchIdx=%v and maxInNodePrefixLen=%v", mismatchIdx, maxInNodePrefixLen))
	}
	// If the whole prefix is longer maxInNodePrefixLen and mismatchIdx == maxInNodePrefixLen, we need to match deeper with any leaf.
	leafArtNode := minimumLeafNode(a, *an)
	lKey := leafArtNode.asLeaf(a).GetKey()
	return longestCommonPrefix(lKey, key, depth+maxInNodePrefixLen) + maxInNodePrefixLen
}

func (an *artNode) asNode4(a *artAllocator) *node4 {
	return a.getNode4(an.addr)
}

func (an *artNode) asNode16(a *artAllocator) *node16 {
	return a.getNode16(an.addr)
}

func (an *artNode) asNode48(a *artAllocator) *node48 {
	return a.getNode48(an.addr)
}

func (an *artNode) asNode256(a *artAllocator) *node256 {
	return a.getNode256(an.addr)
}

// for amd64 and arm64 architectures, we use the chunk comparison to speed up finding the longest common prefix.
const enableChunkComparison = runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64"

// longestCommonPrefix returns the length of the longest common prefix of two keys.
// the LCP is calculated from the given depth, you need to guarantee l1Key[:depth] equals to l2Key[:depth] before calling this function.
func longestCommonPrefix(l1Key, l2Key artKey, depth uint32) uint32 {
	if enableChunkComparison {
		return longestCommonPrefixByChunk(l1Key, l2Key, depth)
	}
	// For other architectures, we use the byte-by-byte comparison.
	idx, limit := depth, uint32(min(len(l1Key), len(l2Key)))
	for ; idx < limit; idx++ {
		if l1Key[idx] != l2Key[idx] {
			break
		}
	}
	return idx - depth
}

// longestCommonPrefixByChunk compares two keys by 8 bytes at a time, which is significantly faster when the keys are long.
// Note this function only support architecture which is under little-endian and can read memory across unaligned address.
func longestCommonPrefixByChunk(l1Key, l2Key artKey, depth uint32) uint32 {
	idx, limit := depth, uint32(min(len(l1Key), len(l2Key)))

	if idx == limit {
		return 0
	}

	p1 := unsafe.Pointer(&l1Key[depth])
	p2 := unsafe.Pointer(&l2Key[depth])

	// Compare 8 bytes at a time
	remaining := limit - depth
	for remaining >= 8 {
		if *(*uint64)(p1) != *(*uint64)(p2) {
			// Find first different byte using trailing zeros
			xor := *(*uint64)(p1) ^ *(*uint64)(p2)
			return limit - remaining + uint32(bits.TrailingZeros64(xor)>>3) - depth
		}

		p1 = unsafe.Add(p1, 8)
		p2 = unsafe.Add(p2, 8)
		remaining -= 8
	}

	// Compare rest bytes
	idx = limit - remaining
	for ; idx < limit; idx++ {
		if l1Key[idx] != l2Key[idx] {
			break
		}
	}
	return idx - depth
}

// Find the minimumLeafNode artLeaf under an artNode
func minimumLeafNode(a *artAllocator, an artNode) artNode {
	for {
		switch an.kind {
		case typeLeaf:
			return an
		case typeNode4:
			n4 := an.asNode4(a)
			if !n4.inplaceLeaf.addr.IsNull() {
				return n4.inplaceLeaf
			}
			an = n4.children[0]
		case typeNode16:
			n16 := an.asNode16(a)
			if !n16.inplaceLeaf.addr.IsNull() {
				return n16.inplaceLeaf
			}
			an = n16.children[0]
		case typeNode48:
			n48 := an.asNode48(a)
			if !n48.inplaceLeaf.addr.IsNull() {
				return n48.inplaceLeaf
			}
			idx := n48.nextPresentIdx(0)
			an = n48.children[n48.keys[idx]]
		case typeNode256:
			n256 := an.asNode256(a)
			if !n256.inplaceLeaf.addr.IsNull() {
				return n256.inplaceLeaf
			}
			idx := n256.nextPresentIdx(0)
			an = n256.children[idx]
		default:
			panic("invalid node kind")
		}
	}
}

// findChild finds the child node by the given key byte, returns the index of the child and the child node.
// inplace indicates whether the target key is a leaf of the node, if valid is false, the in-place leaf will be returned.
func (an *artNode) findChild(a *artAllocator, c byte, inplace bool) (int, artNode) {
	if inplace {
		return inplaceIndex, an.asNode(a).inplaceLeaf
	}
	switch an.kind {
	case typeNode4:
		n4 := an.asNode4(a)
		idx := n4.findChild(c)
		if idx != notExistIndex {
			return idx, n4.children[idx]
		}
	case typeNode16:
		n16 := an.asNode16(a)
		idx := n16.findChild(c)
		if idx != notExistIndex {
			return idx, n16.children[idx]
		}
	case typeNode48:
		n48 := an.asNode48(a)
		idx := n48.findChild(c)
		if idx != notExistIndex {
			return idx, n48.children[n48.keys[idx]]
		}
	case typeNode256:
		n256 := an.asNode256(a)
		idx := n256.findChild(c)
		if idx != notExistIndex {
			return idx, n256.children[idx]
		}
	}
	return notExistIndex, nullArtNode
}

func (n4 *node4) findChild(c byte) int {
	for idx := 0; idx < int(n4.nodeNum); idx++ {
		if n4.keys[idx] == c {
			return idx
		}
	}

	return notExistIndex
}

func (n16 *node16) findChild(c byte) int {
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
		return idx
	}
	return notExistIndex
}

func (n48 *node48) findChild(c byte) int {
	if n48.present[c>>n48s]&(1<<(c%n48m)) != 0 {
		return int(c)
	}
	return notExistIndex
}

func (n256 *node256) findChild(c byte) int {
	if n256.present[c>>n48s]&(1<<(c%n48m)) != 0 {
		return int(c)
	}
	return notExistIndex
}

func (an *artNode) replaceChild(a *artAllocator, c byte, child artNode) {
	switch an.kind {
	case typeNode4:
		n4 := an.asNode4(a)
		idx := n4.findChild(c)
		if idx != notExistIndex {
			n4.children[idx] = child
			return
		}
	case typeNode16:
		n16 := an.asNode16(a)
		idx := n16.findChild(c)
		if idx != notExistIndex {
			n16.children[idx] = child
			return
		}
	case typeNode48:
		n48 := an.asNode48(a)
		idx := n48.findChild(c)
		if idx != notExistIndex {
			n48.children[n48.keys[idx]] = child
			return
		}
	case typeNode256:
		n256 := an.asNode256(a)
		if n256.present[c>>n48s]&(1<<(c%n48m)) != 0 {
			n256.children[c] = child
			return
		}
	}
	panic("replace child failed")
}

// addChild adds a child to the node.
// the added index `c` should not exist.
// Return true if the node is grown to higher capacity.
func (an *artNode) addChild(a *artAllocator, c byte, inplace bool, child artNode) bool {
	if inplace {
		an.asNode(a).inplaceLeaf = child
		return false
	}
	switch an.kind {
	case typeNode4:
		return an.addChild4(a, c, child)
	case typeNode16:
		return an.addChild16(a, c, child)
	case typeNode48:
		return an.addChild48(a, c, child)
	case typeNode256:
		return an.addChild256(a, c, child)
	}
	panic("add child failed")
}

func (an *artNode) addChild4(a *artAllocator, c byte, child artNode) bool {
	n4 := an.asNode4(a)

	if n4.nodeNum >= node4cap {
		an.growNode4(n4, a)
		an.addChild16(a, c, child)
		return true
	}

	i := uint8(0)
	for ; i < n4.nodeNum; i++ {
		if c <= n4.keys[i] {
			if testing.Testing() && c == n4.keys[i] {
				panic("key already exists")
			}
			break
		}
	}

	if i < n4.nodeNum {
		copy(n4.keys[i+1:n4.nodeNum+1], n4.keys[i:n4.nodeNum])
		copy(n4.children[i+1:n4.nodeNum+1], n4.children[i:n4.nodeNum])
	}
	n4.keys[i] = c
	n4.children[i] = child
	n4.nodeNum++
	return false
}

func (an *artNode) addChild16(a *artAllocator, c byte, child artNode) bool {
	n16 := an.asNode16(a)

	if n16.nodeNum >= node16cap {
		an.growNode16(n16, a)
		an.addChild48(a, c, child)
		return true
	}

	i, found := sort.Find(int(n16.nodeNum), func(i int) int {
		if n16.keys[i] < c {
			return 1
		}
		if n16.keys[i] == c {
			return 0
		}
		return -1
	})

	if testing.Testing() && found {
		panic("key already exists")
	}

	if i < int(n16.nodeNum) {
		copy(n16.keys[i+1:n16.nodeNum+1], n16.keys[i:n16.nodeNum])
		copy(n16.children[i+1:n16.nodeNum+1], n16.children[i:n16.nodeNum])
	}

	n16.keys[i] = c
	n16.children[i] = child
	n16.nodeNum++
	return false
}

func (an *artNode) addChild48(a *artAllocator, c byte, child artNode) bool {
	n48 := an.asNode48(a)

	if n48.nodeNum >= node48cap {
		an.growNode48(n48, a)
		an.addChild256(a, c, child)
		return true
	}

	if testing.Testing() && n48.present[c>>n48s]&(1<<(c%n48m)) != 0 {
		panic("key already exists")
	}

	n48.keys[c] = n48.nodeNum
	n48.present[c>>n48s] |= 1 << (c % n48m)
	n48.children[n48.nodeNum] = child
	n48.nodeNum++
	return false
}

func (an *artNode) addChild256(a *artAllocator, c byte, child artNode) bool {
	n256 := an.asNode256(a)

	if testing.Testing() && n256.present[c>>n48s]&(1<<(c%n48m)) != 0 {
		panic("key already exists")
	}

	n256.present[c>>n48s] |= 1 << (c % n48m)
	n256.children[c] = child
	n256.nodeNum++
	return false
}

func (n *nodeBase) copyMeta(src *nodeBase) {
	n.nodeNum = src.nodeNum
	n.prefixLen = src.prefixLen
	n.inplaceLeaf = src.inplaceLeaf
	copy(n.prefix[:], src.prefix[:])
}

func (an *artNode) growNode4(n4 *node4, a *artAllocator) {
	newAddr, n16 := a.allocNode16()
	n16.copyMeta(&n4.nodeBase)

	copy(n16.keys[:], n4.keys[:])
	copy(n16.children[:], n4.children[:])

	// replace addr and free node4
	a.freeNode4(an.addr)
	an.kind = typeNode16
	an.addr = newAddr
}

func (an *artNode) growNode16(n16 *node16, a *artAllocator) {
	newAddr, n48 := a.allocNode48()
	n48.copyMeta(&n16.nodeBase)

	for i := uint8(0); i < n16.nodeBase.nodeNum; i++ {
		ch := n16.keys[i]
		n48.keys[ch] = i
		n48.present[ch>>n48s] |= 1 << (ch % n48m)
		n48.children[i] = n16.children[i]
	}

	// replace addr and free node16
	a.freeNode16(an.addr)
	an.kind = typeNode48
	an.addr = newAddr
}

func (an *artNode) growNode48(n48 *node48, a *artAllocator) {
	newAddr, n256 := a.allocNode256()
	n256.copyMeta(&n48.nodeBase)

	for i := n48.nextPresentIdx(0); i < node256cap; i = n48.nextPresentIdx(i + 1) {
		n256.children[i] = n48.children[n48.keys[i]]
	}
	copy(n256.present[:], n48.present[:])

	// replace addr and free node48
	a.freeNode48(an.addr)
	an.kind = typeNode256
	an.addr = newAddr
}
