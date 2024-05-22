package art

import (
	"bytes"
	"math"
	"math/bits"
	"unsafe"

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
	maxPrefixLen = 10
	node4cap     = 4
	node16cap    = 16
	node48cap    = 48
	node256cap   = 256
)

const (
	// if we use 16-bit idx for nodeAddr, and break nodeAddr into embed fields, the artNodeSize can be 8 bits.
	// By reducing 4 bits for nodeAddr, we can save about 1/4-1/3 memories for node.
	artNodeSize = 12
	node4size   = 80
	node16size  = 236
	node48size  = 888
	node256size = 3096
	leafSize    = 12
)

var (
	// nullNode48 is used to initialize the node 256
	nullNode48 = node48{}
	// nullNode256 is used to initialize the node 256
	nullNode256 = node256{}
)

var nullArtNode = artNode{kind: typeInvalid, addr: nullAddr}

func init() {
	for i := 0; i < node48cap; i++ {
		nullNode48.children[i] = artNode{kind: typeInvalid, addr: nullAddr}
	}
	for i := 0; i < node256cap; i++ {
		nullNode256.children[i] = artNode{kind: typeInvalid, addr: nullAddr}
	}
}

type Key []byte
type artNode struct {
	kind nodeKind
	addr nodeAddr
}

type node struct {
	nodeNum     uint8
	prefixLen   uint8
	prefix      [maxPrefixLen]byte
	inplaceLeaf artNode
}

type node4 struct {
	node
	present  uint8 // bitmap, only lower 4-bit is used
	keys     [node4cap]byte
	children [node4cap]artNode
}

type node16 struct {
	present uint16 // bitmap
	node
	keys     [node16cap]byte
	children [node16cap]artNode
}

// Node with 48 children
const (
	n48s = 6  // 2^n48s == n48m
	n48m = 64 // it should be sizeof(node48.present[0])
)

type node48 struct {
	node
	present  [4]uint64
	keys     [node256cap]uint8 // map byte to index
	children [node48cap]artNode
}

type node256 struct {
	node
	children [node256cap]artNode
}

type leaf struct {
	vAddr nodeAddr
	klen  uint16
	flags uint16
}

func (n *artNode) isLeaf() bool {
	return n.kind == typeLeaf
}

func (n *artNode) leaf(a *artAllocator) *leaf {
	return a.getLeaf(n.addr)
}

func (n *artNode) node(a *artAllocator) *node {
	switch n.kind {
	case typeNode4:
		return &n.node4(a).node
	case typeNode16:
		return &n.node16(a).node
	case typeNode48:
		return &n.node48(a).node
	case typeNode256:
		return &n.node256(a).node
	default:
		panic("invalid node kind")
	}
}

func (n *artNode) at(a *artAllocator, idx int) artNode {
	switch n.kind {
	case typeNode4:
		return n.node4(a).children[idx]
	case typeNode16:
		return n.node16(a).children[idx]
	case typeNode48:
		n48 := n.node48(a)
		return n48.children[n48.keys[idx]]
	case typeNode256:
		return n.node256(a).children[idx]
	default:
		panic("invalid node kind")
	}
}

func (n4 *node4) init() {
	// initialize basic node
	n4.nodeNum = 0
	n4.prefixLen = 0
	n4.inplaceLeaf = nullArtNode
	// initialize node4
	n4.present = 0
}

func (n4 *node4) nextPresentIdx(start int) int {
	mask := math.MaxUint8 - (uint8(1) << start) + 1 // e.g. offset=3 => 0b11111000
	present := n4.present & mask
	present |= 0xf0
	return bits.TrailingZeros8(present)
}

func (n4 *node4) prevPresentIdx(start int) int {
	mask := uint8(1<<(start+1) - 1) // e.g. start=3 => 0b000...0001111
	present := n4.present & mask
	zeros := bits.LeadingZeros8(present)
	return 8 - zeros - 1
}

func (n16 *node16) init() {
	// initialize basic node
	n16.nodeNum = 0
	n16.prefixLen = 0
	n16.inplaceLeaf = nullArtNode
	// initialize node16
	n16.present = 0
}

func (n16 *node16) nextPresentIdx(start int) int {
	mask := math.MaxUint16 - (uint16(1) << start) + 1 // e.g. offset=3 => 0b111...111000
	present := n16.present & mask
	return bits.TrailingZeros16(present)
}

func (n16 *node16) prevPresentIdx(start int) int {
	mask := uint16(1<<(start+1) - 1) // e.g. start=3 => 0b000...0001111
	present := n16.present & mask
	zeros := bits.LeadingZeros16(present)
	return 16 - zeros - 1
}

func (n48 *node48) init() {
	// initialize basic node
	n48.nodeNum = 0
	n48.prefixLen = 0
	n48.inplaceLeaf = nullArtNode
	// initialize node48
	n48.present[0], n48.present[1], n48.present[2], n48.present[3] = 0, 0, 0, 0
	copy(n48.children[:], nullNode48.children[:])
}

func (n48 *node48) isPresent(idx int) bool {
	return n48.present[idx>>n48s]&(1<<uint8(idx%n48m)) != 0
}

func (n48 *node48) nextPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset < 4; presentOffset++ {
		present := n48.present[presentOffset]
		offset := start % n48m
		start = 0
		mask := math.MaxUint64 - (uint64(1) << offset) + 1 // e.g. offset=3 => 0b111...111000
		curr := present & mask
		zeros := bits.TrailingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + zeros
		}
	}
	return 256
}

func (n48 *node48) prevPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset >= 0; presentOffset-- {
		present := n48.present[presentOffset]
		offset := start % n48m
		start = n48m - 1
		mask := uint64(1)<<(offset+1) - 1 // e.g. offset=3 => 0b000...0001111
		curr := present & mask
		zeros := bits.LeadingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + n48m - (zeros + 1)
		}
	}
	return -1
}

func (n256 *node256) init() {
	// initialize basic node
	n256.nodeNum = 0
	n256.prefixLen = 0
	n256.inplaceLeaf = nullArtNode
	// initialize node256
	copy(n256.children[:], nullNode256.children[:])
}

func (n256 *node256) nextPresentIdx(start int) int {
	for ; start < node256cap; start++ {
		if !n256.children[start].addr.isNull() {
			return start
		}
	}
	return start
}

func (n256 *node256) prevPresentIdx(start int) int {
	for ; start > -1; start-- {
		if !n256.children[start].addr.isNull() {
			return start
		}
	}
	return start
}

// key methods
func (k Key) charAt(pos int) byte {
	if pos < 0 || pos >= len(k) {
		return 0
	}
	return k[pos]
}

func (k Key) valid(pos int) bool {
	return pos >= 0 && pos < len(k)
}

// leaf methods

func (l *leaf) getKey() Key {
	base := unsafe.Add(unsafe.Pointer(l), leafSize)
	return unsafe.Slice((*byte)(base), int(l.klen))
}

func (l *leaf) match(key Key) bool {
	lKey := l.getKey()
	if len(key) == 0 && len(lKey) == 0 {
		return true
	}

	if key == nil || len(lKey) != len(key) {
		return false
	}
	return bytes.Compare(lKey[:len(key)], key) == 0
}

func (l *leaf) setKeyFlags(flags kv.KeyFlags) nodeAddr {
	l.flags = uint16(flags) & flagMask
	return l.vAddr
}

func (l *leaf) getKeyFlags() kv.KeyFlags {
	return kv.KeyFlags(l.flags & flagMask)
}

const (
	deleteFlag uint16 = 1 << 15
	flagMask          = ^deleteFlag
)

// markDelete marks the leaf as deleted
func (l *leaf) markDelete() {
	l.flags = deleteFlag
}

func (l *leaf) unmarkDelete() {
	l.flags &= flagMask
}

func (l *leaf) isDeleted() bool {
	return l.flags&deleteFlag != 0
}

// Node methods
func (n *node) setPrefix(key Key, prefixLen uint32) {
	n.prefixLen = uint8(prefixLen)
	copy(n.prefix[:], key[:min(prefixLen, maxPrefixLen)])
}

func (n *node) match(key Key, depth uint32) uint32 {
	idx := uint32(0)
	if len(key)-int(depth) < 0 {
		return idx
	}

	limit := min(uint32(min(n.prefixLen, maxPrefixLen)), uint32(len(key))-depth)
	for ; idx < limit; idx++ {
		if n.prefix[idx] != key[idx+depth] {
			return idx
		}
	}

	return idx
}

func (n *artNode) node4(a *artAllocator) *node4 {
	return a.getNode4(n.addr)
}

func (n *artNode) node16(a *artAllocator) *node16 {
	return a.getNode16(n.addr)
}

func (n *artNode) node48(a *artAllocator) *node48 {
	return a.getNode48(n.addr)
}

func (n *artNode) node256(a *artAllocator) *node256 {
	return a.getNode256(n.addr)
}

func (an artNode) matchDeep(a *artAllocator, key Key, depth uint32) uint32 /* mismatch index*/ {
	n := an.node(a)
	mismatchIdx := n.match(key, depth)
	if mismatchIdx < maxPrefixLen {
		return mismatchIdx
	}

	leafArtNode := minimum(a, an)
	leaf := leafArtNode.leaf(a)
	lKey := leaf.getKey()
	limit := min(uint32(len(lKey)), uint32(len(key))) - depth
	for ; mismatchIdx < limit; mismatchIdx++ {
		if lKey[mismatchIdx+depth] != key[mismatchIdx+depth] {
			break
		}
	}
	return mismatchIdx
}

// Find the minimum leaf under an artNode
func minimum(a *artAllocator, an artNode) artNode {
	switch an.kind {
	case typeLeaf:
		return an
	case typeNode4:
		n4 := an.node4(a)
		if !n4.inplaceLeaf.addr.isNull() {
			return n4.inplaceLeaf
		}
		if !n4.children[0].addr.isNull() {
			return minimum(a, n4.children[0])
		}
	case typeNode16:
		n16 := an.node16(a)
		if !n16.inplaceLeaf.addr.isNull() {
			return n16.inplaceLeaf
		}
		if !n16.children[0].addr.isNull() {
			return minimum(a, n16.children[0])
		}
	case typeNode48:
		n48 := an.node48(a)
		if !n48.inplaceLeaf.addr.isNull() {
			return n48.inplaceLeaf
		}

		idx := uint8(0)
		for n48.present[idx>>n48s]&(1<<uint8(idx%n48m)) == 0 {
			idx++
		}
		if !n48.children[n48.keys[idx]].addr.isNull() {
			return minimum(a, n48.children[n48.keys[idx]])
		}
	case typeNode256:
		n256 := an.node256(a)
		if !n256.inplaceLeaf.addr.isNull() {
			return n256.inplaceLeaf
		} else {
			for idx := 0; idx < 256; idx++ {
				if !n256.children[idx].addr.isNull() {
					return minimum(a, n256.children[idx])
				}
			}
		}
	}
	return nullArtNode
}

// Find the minimum leaf under an artNode
func maximum(a *artAllocator, an artNode) artNode {
	switch an.kind {
	case typeLeaf:
		return an
	case typeNode4:
		n4 := an.node4(a)
		if n4.nodeNum > 0 {
			return maximum(a, n4.children[n4.nodeNum-1])
		}
		if !n4.inplaceLeaf.addr.isNull() {
			return n4.inplaceLeaf
		}
	case typeNode16:
		n16 := an.node16(a)
		if n16.nodeNum > 0 {
			return maximum(a, n16.children[n16.nodeNum-1])
		}
		if !n16.inplaceLeaf.addr.isNull() {
			return n16.inplaceLeaf
		}
	case typeNode48:
		n48 := an.node48(a)

		idx := n48.prevPresentIdx(node256cap - 1)
		if idx >= 0 {
			return maximum(a, n48.children[n48.keys[idx]])
		}
		if !n48.inplaceLeaf.addr.isNull() {
			return n48.inplaceLeaf
		}
	case typeNode256:
		n256 := an.node256(a)
		idx := n256.prevPresentIdx(node256cap - 1)
		if idx >= 0 {
			return maximum(a, n256.children[idx])
		}
		if !n256.inplaceLeaf.addr.isNull() {
			return n256.inplaceLeaf
		}
	}
	return nullArtNode
}

func (an artNode) findChild(a *artAllocator, c byte, valid bool) (int, artNode) {
	node := an.node(a)
	if !valid {
		return -1, node.inplaceLeaf
	}
	switch an.kind {
	case typeNode4:
		return an._findChild4(a, c)
	case typeNode16:
		return an._findChild16(a, c)
	case typeNode48:
		return an._findChild48(a, c)
	case typeNode256:
		return an._findChild256(a, c)
	}
	return -2, nullArtNode
}

func (an artNode) _findChild4(a *artAllocator, c byte) (int, artNode) {
	n4 := an.node4(a)
	for idx := 0; idx < int(n4.nodeNum); idx++ {
		if n4.keys[idx] == c {
			return idx, n4.children[idx]
		}
	}
	return -2, nullArtNode
}

func (an artNode) _findChild16(a *artAllocator, c byte) (int, artNode) {
	n16 := an.node16(a)
	for idx := 0; idx < int(n16.nodeNum); idx++ {
		if n16.keys[idx] == c {
			return idx, n16.children[idx]
		}
	}
	return -2, nullArtNode
}

func (an artNode) _findChild48(a *artAllocator, c byte) (int, artNode) {
	n48 := an.node48(a)
	if n48.present[c>>n48s]&(1<<(c%n48m)) != 0 {
		return int(c), n48.children[n48.keys[c]]
	}
	return -2, nullArtNode
}

func (an artNode) _findChild256(a *artAllocator, c byte) (int, artNode) {
	n256 := an.node256(a)
	return int(c), n256.children[c]
}

func (an *artNode) swapChild(a *artAllocator, c byte, child artNode) {
	switch an.kind {
	case typeNode4:
		n4 := an.node4(a)
		for idx := uint8(0); idx < n4.nodeNum; idx++ {
			if n4.keys[idx] == c {
				n4.children[idx] = child
				return
			}
		}
		panic("swap child failed")
	case typeNode16:
		n16 := an.node16(a)
		for idx := uint8(0); idx < n16.nodeNum; idx++ {
			if n16.keys[idx] == c {
				n16.children[idx] = child
				return
			}
		}
		panic("swap child failed")
	case typeNode48:
		n48 := an.node48(a)
		if n48.present[c>>n48s]&(1<<(c%n48m)) != 0 {
			n48.children[n48.keys[c]] = child
			return
		}
		panic("swap child failed")
	case typeNode256:
		an._addChild256(a, c, false, child)
	}
}

func (an *artNode) addChild(a *artAllocator, c byte, inplace bool, child artNode) bool {
	switch an.kind {
	case typeNode4:
		return an._addChild4(a, c, inplace, child)
	case typeNode16:
		return an._addChild16(a, c, inplace, child)
	case typeNode48:
		return an._addChild48(a, c, inplace, child)
	case typeNode256:
		return an._addChild256(a, c, inplace, child)
	}
	return false
}

func (an *artNode) _addChild4(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node4(a)
	if node.nodeNum >= node4cap {
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
	node.present = (node.present << 1) + 1
	node.keys[i] = c
	node.children[i] = child
	node.nodeNum++
	if node.keys[0] == 49 && node.keys[1] == 49 {
		a := 1
		_ = a
	}
	return false
}

func (an *artNode) _addChild16(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node16(a)
	if node.nodeNum >= node16cap {
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
	node.present = (node.present << 1) + 1
	node.keys[i] = c
	node.children[i] = child
	node.nodeNum++
	return false
}

func (an *artNode) _addChild48(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node48(a)
	if node.nodeNum >= node48cap {
		an.grow(a)
		an.addChild(a, c, inplace, child)
		return true
	}

	if inplace {
		node.inplaceLeaf = child
		return false
	}

	lower, upper := 0, node48cap-1
	for lower < upper {
		middle := (lower + upper) / 2
		if node.children[middle].addr.isNull() {
			upper = middle
		} else {
			lower = middle + 1
		}
	}
	node.keys[c] = uint8(lower)
	node.present[c>>n48s] |= 1 << (c % n48m)
	node.children[lower] = child
	node.nodeNum++
	return false
}

func (an *artNode) _addChild256(a *artAllocator, c byte, inplace bool, child artNode) bool {
	node := an.node256(a)

	if inplace {
		node.inplaceLeaf = child
	} else {
		node.children[c] = child
		node.nodeNum++
	}
	return false
}

func (n *node) copyMeta(src *node) {
	n.nodeNum = src.nodeNum
	n.prefixLen = src.prefixLen
	n.inplaceLeaf = src.inplaceLeaf
	copy(n.prefix[:], src.prefix[:])
}

func (an *artNode) grow(a *artAllocator) *artNode {
	switch an.kind {
	case typeNode4:
		n4 := an.node4(a)
		newAddr, n16 := a.allocNode16()
		n16.copyMeta(&n4.node)

		n16.present = uint16(n4.present)
		copy(n16.keys[:], n4.keys[:])
		copy(n16.children[:], n4.children[:])

		// replace addr and free node4
		a.freeNode4(an.addr)
		an.kind = typeNode16
		an.addr = newAddr
	case typeNode16:
		n16 := an.node16(a)
		newAddr, n48 := a.allocNode48()
		n48.copyMeta(&n16.node)

		var numChildren uint8
		for i := uint8(0); i < n16.node.nodeNum; i++ {
			if n16.present&(1<<i) != 0 {
				ch := n16.keys[i]
				n48.keys[ch] = numChildren
				n48.present[ch>>n48s] |= 1 << uint8(ch%n48m)
				n48.children[numChildren] = n16.children[i]
				numChildren++
			}
		}

		// replace addr and free node16
		a.freeNode16(an.addr)
		an.kind = typeNode48
		an.addr = newAddr
	case typeNode48:
		n48 := an.node48(a)
		newAddr, n256 := a.allocNode256()
		n256.copyMeta(&n48.node)

		for i := uint16(0); i < node256cap; i++ {
			if n48.present[i>>n48s]&(1<<(i%n48m)) != 0 {
				n256.children[i] = n48.children[n48.keys[i]]
			}
		}

		// replace addr and free node48
		a.freeNode48(an.addr)
		an.kind = typeNode256
		an.addr = newAddr
	}
	return nil
}
