package art

import (
	"bytes"
	"math"
	"math/bits"
	"sort"
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
	maxPrefixLen  = 20
	node4cap      = 4
	node16cap     = 16
	node48cap     = 48
	node256cap    = 256
	inplaceIndex  = -1
	notExistIndex = -2
)

const (
	node4size   = uint32(unsafe.Sizeof(node4{}))
	node16size  = uint32(unsafe.Sizeof(node16{}))
	node48size  = uint32(unsafe.Sizeof(node48{}))
	node256size = uint32(unsafe.Sizeof(node256{}))
	leafSize    = uint32(unsafe.Sizeof(leaf{}))
)

var (
	// nullNode48 is used to initialize the node48
	nullNode48 = node48{}
	// nullNode256 is used to initialize the node256
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

type nodeBase struct {
	nodeNum     uint8
	prefixLen   uint32
	prefix      [maxPrefixLen]byte
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

// Node with 48 children
const (
	n48s = 6  // 2^n48s == n48m
	n48m = 64 // it should be sizeof(node48.present[0])
)

type node48 struct {
	nodeBase
	present  [4]uint64
	keys     [node256cap]uint8 // map byte to index
	children [node48cap]artNode
}

type node256 struct {
	nodeBase
	present  [4]uint64
	children [node256cap]artNode
}

type leaf struct {
	vAddr nodeAddr
	klen  uint16
	flags uint16
}

func (an *artNode) isLeaf() bool {
	return an.kind == typeLeaf
}

func (an *artNode) leaf(a *artAllocator) *leaf {
	return a.getLeaf(an.addr)
}

func (an *artNode) node(a *artAllocator) *nodeBase {
	switch an.kind {
	case typeNode4:
		return &an.node4(a).nodeBase
	case typeNode16:
		return &an.node16(a).nodeBase
	case typeNode48:
		return &an.node48(a).nodeBase
	case typeNode256:
		return &an.node256(a).nodeBase
	default:
		panic("invalid nodeBase kind")
	}
}

// at returns the nth child of the node.
func (an *artNode) at(a *artAllocator, idx int) artNode {
	switch an.kind {
	case typeNode4:
		return an.node4(a).children[idx]
	case typeNode16:
		return an.node16(a).children[idx]
	case typeNode48:
		n48 := an.node48(a)
		return n48.children[n48.keys[idx]]
	case typeNode256:
		return an.node256(a).children[idx]
	default:
		panic("invalid nodeBase kind")
	}
}

func (n4 *node4) nextPresentIdx(start int) int {
	if start < int(n4.nodeNum) {
		return start
	}
	return node4cap
}

func (n4 *node4) prevPresentIdx(start int) int {
	return min(start, int(n4.nodeNum)-1)
}

func (n48 *node48) init() {
	// initialize nodeBase
	n48.nodeBase.init()
	// initialize node48
	n48.present[0], n48.present[1], n48.present[2], n48.present[3] = 0, 0, 0, 0
	copy(n48.children[:], nullNode48.children[:])
}

// nextPresentIdx returns the next present index starting from the given index.
// Finding from present bitmap is faster than from keys.
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
	return node256cap
}

// prevPresentIdx returns the next present index starting from the given index.
// Finding from present bitmap is faster than from keys.
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
	return inplaceIndex
}

func (n256 *node256) init() {
	// initialize nodeBase
	n256.nodeBase.init()
	// initialize node256
	copy(n256.children[:], nullNode256.children[:])
}

func (n256 *node256) nextPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset < 4; presentOffset++ {
		present := n256.present[presentOffset]
		offset := start % n48m
		start = 0
		mask := math.MaxUint64 - (uint64(1) << offset) + 1 // e.g. offset=3 => 0b111...111000
		curr := present & mask
		zeros := bits.TrailingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + zeros
		}
	}
	return node256cap
}

func (n256 *node256) prevPresentIdx(start int) int {
	for presentOffset := start >> n48s; presentOffset >= 0; presentOffset-- {
		present := n256.present[presentOffset]
		offset := start % n48m
		start = n48m - 1
		mask := uint64(1)<<(offset+1) - 1 // e.g. offset=3 => 0b000...0001111
		curr := present & mask
		zeros := bits.LeadingZeros64(curr)
		if zeros < n48m {
			return presentOffset*n48m + n48m - (zeros + 1)
		}
	}
	return inplaceIndex
}

// key methods
func (k Key) charAt(pos int) byte {
	if pos < 0 || pos >= len(k) {
		return 0
	}
	return k[pos]
}

func (k Key) valid(pos int) bool {
	return pos < len(k)
}

// leaf methods

// getKey gets the full key of the leaf
func (l *leaf) getKey() Key {
	base := unsafe.Add(unsafe.Pointer(l), leafSize)
	return unsafe.Slice((*byte)(base), int(l.klen))
}

// getKeyDepth gets the partial key start from depth of the leaf
func (l *leaf) getKeyDepth(depth uint32) Key {
	base := unsafe.Add(unsafe.Pointer(l), leafSize+depth)
	return unsafe.Slice((*byte)(base), int(l.klen)-int(depth))
}

func (l *leaf) match(depth uint32, key Key) bool {
	//return bytes.Equal(l.getKey(), key)
	return bytes.Equal(l.getKeyDepth(depth), key[depth:])
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

//nolint:unused
func (l *leaf) unmarkDelete() {
	l.flags &= flagMask
}

func (l *leaf) isDeleted() bool {
	return l.flags&deleteFlag != 0
}

// Node methods
func (n *nodeBase) setPrefix(key Key, prefixLen uint32) {
	n.prefixLen = prefixLen
	copy(n.prefix[:], key[:min(prefixLen, maxPrefixLen)])
}

func (n *nodeBase) match(key Key, depth uint32) uint32 {
	idx := uint32(0)

	limit := min(min(n.prefixLen, maxPrefixLen), uint32(len(key))-depth)
	for ; idx < limit; idx++ {
		if n.prefix[idx] != key[idx+depth] {
			return idx
		}
	}

	return idx
}

func (an *artNode) node4(a *artAllocator) *node4 {
	return a.getNode4(an.addr)
}

func (an *artNode) node16(a *artAllocator) *node16 {
	return a.getNode16(an.addr)
}

func (an *artNode) node48(a *artAllocator) *node48 {
	return a.getNode48(an.addr)
}

func (an *artNode) node256(a *artAllocator) *node256 {
	return a.getNode256(an.addr)
}

func (an *artNode) matchDeep(a *artAllocator, key Key, depth uint32) uint32 /* mismatch index*/ {
	n := an.node(a)
	mismatchIdx := n.match(key, depth)
	if mismatchIdx < min(n.prefixLen, maxPrefixLen) {
		return mismatchIdx
	}

	leafArtNode := minimum(a, *an)
	lKey := leafArtNode.leaf(a).getKey()
	return longestCommonPrefix(lKey, key, depth)
}

func longestCommonPrefix(l1Key, l2Key Key, depth uint32) uint32 {
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

// Find the minimum leaf under an artNode
func minimum(a *artAllocator, an artNode) artNode {
	for {
		switch an.kind {
		case typeLeaf:
			return an
		case typeNode4:
			n4 := an.node4(a)
			if !n4.inplaceLeaf.addr.isNull() {
				return n4.inplaceLeaf
			}
			an = n4.children[0]
		case typeNode16:
			n16 := an.node16(a)
			if !n16.inplaceLeaf.addr.isNull() {
				return n16.inplaceLeaf
			}
			an = n16.children[0]
		case typeNode48:
			n48 := an.node48(a)
			if !n48.inplaceLeaf.addr.isNull() {
				return n48.inplaceLeaf
			}
			idx := n48.nextPresentIdx(0)
			an = n48.children[n48.keys[idx]]
		case typeNode256:
			n256 := an.node256(a)
			if !n256.inplaceLeaf.addr.isNull() {
				return n256.inplaceLeaf
			}
			idx := n256.nextPresentIdx(0)
			an = n256.children[idx]
		case typeInvalid:
			return nullArtNode
		}
	}
}

func (an *artNode) findChild(a *artAllocator, c byte, valid bool) (int, artNode) {
	if !valid {
		return inplaceIndex, an.node(a).inplaceLeaf
	}
	switch an.kind {
	case typeNode4:
		return an.findChild4(a, c)
	case typeNode16:
		return an.findChild16(a, c)
	case typeNode48:
		return an.findChild48(a, c)
	case typeNode256:
		return an.findChild256(a, c)
	}
	return notExistIndex, nullArtNode
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
	if n48.present[c>>n48s]&(1<<(c%n48m)) != 0 {
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
		an.addChild256(a, c, false, child)
	}
}

// addChild adds a child to the node.
// the added index `c` should not exist.
func (an *artNode) addChild(a *artAllocator, c byte, inplace bool, child artNode) bool {
	switch an.kind {
	case typeNode4:
		return an.addChild4(a, c, inplace, child)
	case typeNode16:
		return an.addChild16(a, c, inplace, child)
	case typeNode48:
		return an.addChild48(a, c, inplace, child)
	case typeNode256:
		return an.addChild256(a, c, inplace, child)
	}
	return false
}

func (an *artNode) addChild4(a *artAllocator, c byte, inplace bool, child artNode) bool {
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
	if node.nodeNum >= node16cap {
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
	if node.nodeNum >= node48cap {
		an.grow(a)
		an.addChild(a, c, inplace, child)
		return true
	}

	if inplace {
		node.inplaceLeaf = child
		return false
	}

	node.keys[c] = node.nodeNum
	node.present[c>>n48s] |= 1 << (c % n48m)
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

	node.present[c>>n48s] |= 1 << (c % n48m)
	node.children[c] = child
	node.nodeNum++
	return false
}

func (n *nodeBase) copyMeta(src *nodeBase) {
	n.nodeNum = src.nodeNum
	n.prefixLen = src.prefixLen
	n.inplaceLeaf = src.inplaceLeaf
	copy(n.prefix[:], src.prefix[:])
}

func (an *artNode) grow(a *artAllocator) {
	switch an.kind {
	case typeNode4:
		n4 := an.node4(a)
		newAddr, n16 := a.allocNode16()
		n16.copyMeta(&n4.nodeBase)

		copy(n16.keys[:], n4.keys[:])
		copy(n16.children[:], n4.children[:])

		// replace addr and free node4
		a.freeNode4(an.addr)
		an.kind = typeNode16
		an.addr = newAddr
	case typeNode16:
		n16 := an.node16(a)
		newAddr, n48 := a.allocNode48()
		n48.copyMeta(&n16.nodeBase)

		var numChildren uint8
		for i := uint8(0); i < n16.nodeBase.nodeNum; i++ {
			ch := n16.keys[i]
			n48.keys[ch] = numChildren
			n48.present[ch>>n48s] |= 1 << (ch % n48m)
			n48.children[numChildren] = n16.children[i]
			numChildren++
		}

		// replace addr and free node16
		a.freeNode16(an.addr)
		an.kind = typeNode48
		an.addr = newAddr
	case typeNode48:
		n48 := an.node48(a)
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
}
