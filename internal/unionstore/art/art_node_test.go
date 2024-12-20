package art

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

func TestAllocNode(t *testing.T) {
	checkNodeInitialization := func(t *testing.T, n any) {
		var base *nodeBase
		switch n := n.(type) {
		case *node4:
			base = &n.nodeBase
		case *node16:
			base = &n.nodeBase
		case *node48:
			base = &n.nodeBase
			require.Equal(t, [4]uint64{0, 0, 0, 0}, n.present)
		case *node256:
			base = &n.nodeBase
			require.Equal(t, [4]uint64{0, 0, 0, 0}, n.present)
		default:
			require.Fail(t, "unknown node type")
		}
		require.Equal(t, uint8(0), base.nodeNum)
		require.Equal(t, uint32(0), base.prefixLen)
		require.Equal(t, base.inplaceLeaf, nullArtNode)
	}

	var allocator artAllocator
	allocator.init()
	cnt := 10_000

	// alloc node4
	n4s := make([]arena.MemdbArenaAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n4 := allocator.allocNode4()
		require.False(t, addr.IsNull())
		require.NotNil(t, n4)
		checkNodeInitialization(t, n4)
		n4.nodeNum = uint8(i % 4)
		n4.prefixLen = uint32(i % maxInNodePrefixLen)
		n4s = append(n4s, addr)
	}

	// alloc node16
	n16s := make([]arena.MemdbArenaAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n16 := allocator.allocNode16()
		require.False(t, addr.IsNull())
		require.NotNil(t, n16)
		checkNodeInitialization(t, n16)
		n16.nodeNum = uint8(i % 16)
		n16.prefixLen = uint32(i % maxInNodePrefixLen)
		n16s = append(n16s, addr)
	}

	// alloc node48
	n48s := make([]arena.MemdbArenaAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n48 := allocator.allocNode48()
		require.False(t, addr.IsNull())
		require.NotNil(t, n48)
		checkNodeInitialization(t, n48)
		n48.nodeNum = uint8(i % 48)
		n48.prefixLen = uint32(i % maxInNodePrefixLen)
		n48s = append(n48s, addr)
	}

	// alloc node256
	n256s := make([]arena.MemdbArenaAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n256 := allocator.allocNode256()
		require.False(t, addr.IsNull())
		require.NotNil(t, n256)
		checkNodeInitialization(t, n256)
		n256.nodeNum = uint8(i % 256)
		n256.prefixLen = uint32(i % maxInNodePrefixLen)
		n256s = append(n256s, addr)
	}

	// alloc leaf
	leafs := make([]arena.MemdbArenaAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		key := []byte(strconv.Itoa(i))
		addr, leaf := allocator.allocLeaf(key)
		require.False(t, addr.IsNull())
		require.NotNil(t, leaf)
		require.Equal(t, key, leaf.GetKey())
		leafs = append(leafs, addr)
	}

	// test memory safety by checking the assign value
	for i, addr := range n4s {
		n4 := allocator.getNode4(addr)
		require.Equal(t, uint8(i%4), n4.nodeNum, i)
		require.Equal(t, uint32(i%maxInNodePrefixLen), n4.prefixLen, i)
	}
	for i, addr := range n16s {
		n16 := allocator.getNode16(addr)
		require.Equal(t, uint8(i%16), n16.nodeNum)
		require.Equal(t, uint32(i%maxInNodePrefixLen), n16.prefixLen, i)
	}
	for i, addr := range n48s {
		n48 := allocator.getNode48(addr)
		require.Equal(t, uint8(i%48), n48.nodeNum)
		require.Equal(t, uint32(i%maxInNodePrefixLen), n48.prefixLen, i)
	}
	for i, addr := range n256s {
		n256 := allocator.getNode256(addr)
		require.Equal(t, uint8(i%256), n256.nodeNum)
		require.Equal(t, uint32(i%maxInNodePrefixLen), n256.prefixLen, i)
	}
	for i, addr := range leafs {
		key := []byte(strconv.Itoa(i))
		leaf := allocator.getLeaf(addr)
		require.Equal(t, key, leaf.GetKey())
	}
}

func TestNodePrefix(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	testFn := func(an *artNode, n *nodeBase) {
		// simple match
		key := []byte{1, 2, 3, 4, 5, 6, 1, 2, 3, 1}
		n.setPrefix([]byte{1, 2, 3, 4, 5, 66, 77, 88, 99}, 5)

		idx := n.match(key, 0) // return mismatch index
		require.Equal(t, uint32(5), idx)
		idx = n.match(key[:4], 0) // key is shorter than prefix
		require.Equal(t, uint32(4), idx)
		idx = n.match(key, 1) // key[0+depth] != prefix[0]
		require.Equal(t, uint32(0), idx)
		idx = n.match(key, 6) // key[3+depth] != prefix[3]
		require.Equal(t, uint32(3), idx)
		idx = n.match(append([]byte{1}, key...), 1)
		require.Equal(t, uint32(5), idx)

		// deep match
		leafKey := append(make([]byte, maxInNodePrefixLen), []byte{1, 2, 3, 4, 5}...)
		leafAddr, _ := allocator.allocLeaf(leafKey)
		an.addChild(&allocator, 2, false, artNode{kind: typeLeaf, addr: leafAddr})
		// the real prefix is [0, ..., 0, 1], but the node.prefix only store [0, ..., 0]
		n.setPrefix(leafKey, maxInNodePrefixLen+1)
		matchKey := append(make([]byte, maxInNodePrefixLen), []byte{1, 22, 33, 44, 55}...)
		mismatchKey := append(make([]byte, maxInNodePrefixLen), []byte{11, 22, 33, 44, 55}...)
		require.Equal(t, uint32(maxInNodePrefixLen+1), n.matchDeep(&allocator, an, matchKey, 0))
		require.Equal(t, uint32(maxInNodePrefixLen), n.matchDeep(&allocator, an, mismatchKey, 0))

		// deep match with depth
		leafKey = append(make([]byte, 10), leafKey...)
		matchKey = append(make([]byte, 10), matchKey...)
		mismatchKey = append(make([]byte, 10), mismatchKey...)
		leafAddr, _ = allocator.allocLeaf(leafKey)
		an.replaceChild(&allocator, 2, artNode{kind: typeLeaf, addr: leafAddr})
		n.setPrefix(leafKey[10:], maxInNodePrefixLen+1)
		require.Equal(t, uint32(maxInNodePrefixLen+1), n.matchDeep(&allocator, an, matchKey, 10))
		require.Equal(t, uint32(maxInNodePrefixLen), n.matchDeep(&allocator, an, mismatchKey, 10))
	}

	addr, n4 := allocator.allocNode4()
	testFn(&artNode{kind: typeNode4, addr: addr}, &n4.nodeBase)
	addr, n16 := allocator.allocNode16()
	testFn(&artNode{kind: typeNode16, addr: addr}, &n16.nodeBase)
	addr, n48 := allocator.allocNode48()
	testFn(&artNode{kind: typeNode48, addr: addr}, &n48.nodeBase)
	addr, n256 := allocator.allocNode256()
	testFn(&artNode{kind: typeNode256, addr: addr}, &n256.nodeBase)
}

func TestOrderedChild(t *testing.T) {
	// the keys of node4 and node16 should be kept ordered when adding new children.
	var allocator artAllocator
	allocator.init()

	testFn := func(fn func() *artNode, getKeys func(*artNode) []byte, cap int) {
		// add children in ascend order
		an := fn()
		for i := 0; i < cap; i++ {
			addr, _ := allocator.allocNode4()
			an.addChild(&allocator, byte(i), false, artNode{kind: typeNode4, addr: addr})

			node := an.asNode(&allocator)
			require.Equal(t, node.nodeNum, uint8(i+1))
			keys := getKeys(an)
			for j := 0; j <= i; j++ {
				require.Equal(t, byte(j), keys[j])
			}
		}

		// add children in descend order
		an = fn()
		for i := 0; i < cap; i++ {
			addr, _ := allocator.allocNode4()
			an.addChild(&allocator, byte(255-i), false, artNode{kind: typeNode4, addr: addr})

			node := an.asNode(&allocator)
			require.Equal(t, node.nodeNum, uint8(i+1))
			keys := getKeys(an)
			for j := 0; j <= i; j++ {
				require.Equal(t, byte(255-i+j), keys[j])
			}
		}
	}

	testFn(func() *artNode {
		addr, _ := allocator.allocNode4()
		return &artNode{kind: typeNode4, addr: addr}
	}, func(an *artNode) []byte {
		return an.asNode4(&allocator).keys[:]
	}, 4)

	testFn(func() *artNode {
		addr, _ := allocator.allocNode16()
		return &artNode{kind: typeNode16, addr: addr}
	}, func(an *artNode) []byte {
		return an.asNode16(&allocator).keys[:]
	}, 16)
}

func TestNextPrevPresentIdx(t *testing.T) {
	// the present bitmap is used to find the next/prev present child index in node48 and node256.
	var allocator artAllocator
	allocator.init()

	testFn := func(an *artNode, node interface {
		nextPresentIdx(int) int
		prevPresentIdx(int) int
	}) {
		require.Equal(t, node.nextPresentIdx(0), node256cap)
		require.Equal(t, node.prevPresentIdx(0), inplaceIndex)

		mid := 128
		addr, _ := allocator.allocNode4()
		an.addChild(&allocator, byte(mid), false, artNode{kind: typeNode4, addr: addr})
		for i := 0; i < node256cap; i++ {
			expectedNext := mid
			expectedPrev := inplaceIndex
			if i > mid {
				expectedNext = node256cap
			}
			if i >= mid {
				expectedPrev = mid
			}
			require.Equal(t, node.nextPresentIdx(i), expectedNext)
			require.Equal(t, node.prevPresentIdx(i), expectedPrev)
		}
	}

	addr, n48 := allocator.allocNode48()
	testFn(&artNode{kind: typeNode48, addr: addr}, n48)
	addr, n256 := allocator.allocNode48()
	testFn(&artNode{kind: typeNode48, addr: addr}, n256)
}

func TestLCP(t *testing.T) {
	k1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	k2 := []byte{1, 2, 3, 4, 5, 7, 7, 8}
	for i := 0; i < 6; i++ {
		require.Equal(t, uint32(5-i), longestCommonPrefix(k1, k2, uint32(i)))
	}
}

func TestNodeAddChild(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	check := func(n artNode) {
		require.Equal(t, uint8(0), n.asNode(&allocator).nodeNum)
		lfAddr, _ := allocator.allocLeaf([]byte{1, 2, 3, 4, 5})
		lfNode := artNode{kind: typeLeaf, addr: lfAddr}
		n.addChild(&allocator, 1, false, lfNode)
		require.Equal(t, uint8(1), n.asNode(&allocator).nodeNum)
		require.Panics(t, func() {
			// addChild should panic if the key is existed.
			n.addChild(&allocator, 1, false, lfNode)
		})
		n.addChild(&allocator, 2, false, lfNode)
		require.Equal(t, uint8(2), n.asNode(&allocator).nodeNum)
		// inplace leaf won't be counted in nodeNum
		n.addChild(&allocator, 0, true, lfNode)
		require.Equal(t, uint8(2), n.asNode(&allocator).nodeNum)
	}

	addr, _ := allocator.allocNode4()
	check(artNode{kind: typeNode4, addr: addr})
	addr, _ = allocator.allocNode16()
	check(artNode{kind: typeNode16, addr: addr})
	addr, _ = allocator.allocNode48()
	check(artNode{kind: typeNode48, addr: addr})
	addr, _ = allocator.allocNode256()
	check(artNode{kind: typeNode256, addr: addr})
}

func TestNodeGrow(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	check := func(n artNode) {
		capacities := map[nodeKind]int{
			typeNode4:  node4cap,
			typeNode16: node16cap,
			typeNode48: node48cap,
		}
		growTypes := map[nodeKind]nodeKind{
			typeNode4:  typeNode16,
			typeNode16: typeNode48,
			typeNode48: typeNode256,
		}

		capacity, ok := capacities[n.kind]
		require.True(t, ok)
		beforeKind := n.kind
		afterKind, ok := growTypes[n.kind]
		require.True(t, ok)

		for i := 0; i < capacity; i++ {
			lfAddr, _ := allocator.allocLeaf([]byte{byte(i)})
			lfNode := artNode{kind: typeLeaf, addr: lfAddr}
			n.addChild(&allocator, byte(i), false, lfNode)
			require.Equal(t, beforeKind, n.kind)
		}
		lfAddr, _ := allocator.allocLeaf([]byte{byte(capacity)})
		lfNode := artNode{kind: typeLeaf, addr: lfAddr}
		n.addChild(&allocator, byte(capacity), false, lfNode)
		require.Equal(t, afterKind, n.kind)
	}

	addr, _ := allocator.allocNode4()
	check(artNode{kind: typeNode4, addr: addr})
	addr, _ = allocator.allocNode16()
	check(artNode{kind: typeNode16, addr: addr})
	addr, _ = allocator.allocNode48()
	check(artNode{kind: typeNode48, addr: addr})
}

func TestReplaceChild(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	check := func(n artNode) {
		require.Equal(t, uint8(0), n.asNode(&allocator).nodeNum)
		lfAddr, _ := allocator.allocLeaf([]byte{1, 2, 3, 4, 5})
		lfNode := artNode{kind: typeLeaf, addr: lfAddr}
		n.addChild(&allocator, 1, false, lfNode)
		require.Equal(t, uint8(1), n.asNode(&allocator).nodeNum)
		newLfAddr, _ := allocator.allocLeaf([]byte{1, 2, 3, 4, 4})
		newLfNode := artNode{kind: typeLeaf, addr: newLfAddr}
		require.Panics(t, func() {
			// replaceChild should panic if the key is not existed.
			n.replaceChild(&allocator, 2, newLfNode)
		})
		n.replaceChild(&allocator, 1, newLfNode)
		require.Equal(t, uint8(1), n.asNode(&allocator).nodeNum)
		_, childLf := n.findChild(&allocator, 1, false)
		require.NotEqual(t, childLf.addr, lfAddr)
		require.Equal(t, childLf.addr, newLfAddr)
	}

	addr, _ := allocator.allocNode4()
	check(artNode{kind: typeNode4, addr: addr})
	addr, _ = allocator.allocNode16()
	check(artNode{kind: typeNode16, addr: addr})
	addr, _ = allocator.allocNode48()
	check(artNode{kind: typeNode48, addr: addr})
	addr, _ = allocator.allocNode256()
	check(artNode{kind: typeNode256, addr: addr})

}

func TestMinimumNode(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	check := func(kind nodeKind) {
		var addr arena.MemdbArenaAddr

		switch kind {
		case typeNode4:
			addr, _ = allocator.allocNode4()
		case typeNode16:
			addr, _ = allocator.allocNode16()
		case typeNode48:
			addr, _ = allocator.allocNode48()
		case typeNode256:
			addr, _ = allocator.allocNode256()
		}

		node := artNode{kind: kind, addr: addr}

		for _, char := range []byte{255, 127, 63, 0} {
			lfAddr, _ := allocator.allocLeaf([]byte{char})
			lfNode := artNode{kind: typeLeaf, addr: lfAddr}
			node.addChild(&allocator, char, false, lfNode)
			minNode := minimumLeafNode(&allocator, node)
			require.Equal(t, typeLeaf, minNode.kind)
			require.Equal(t, lfAddr, minNode.addr)
		}

		lfAddr, _ := allocator.allocLeaf([]byte{0})
		lfNode := artNode{kind: typeLeaf, addr: lfAddr}
		node.addChild(&allocator, 0, true, lfNode)
		minNode := minimumLeafNode(&allocator, node)
		require.Equal(t, typeLeaf, minNode.kind)
		require.Equal(t, lfAddr, minNode.addr)
	}

	check(typeNode4)
	check(typeNode16)
	check(typeNode48)
	check(typeNode256)
}

func TestKey2Chunk(t *testing.T) {
	key := artKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

	for i := 0; i < len(key); i++ {
		diffKey := make(artKey, len(key))
		copy(diffKey, key)
		diffKey[i] = 255
		require.Equal(t, uint32(i), longestCommonPrefix(key, diffKey, 0))
	}
}
