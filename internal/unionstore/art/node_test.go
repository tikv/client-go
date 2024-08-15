package art

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
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
			for i := 0; i < node256cap; i++ {
				require.Equal(t, n.children[i], nullArtNode)
			}
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
	n4s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n4 := allocator.allocNode4()
		require.False(t, addr.isNull())
		require.NotNil(t, n4)
		checkNodeInitialization(t, n4)
		n4.nodeNum = uint8(i % 4)
		n4.prefixLen = uint32(i % maxPrefixLen)
		n4s = append(n4s, addr)
	}

	// alloc node16
	n16s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n16 := allocator.allocNode16()
		require.False(t, addr.isNull())
		require.NotNil(t, n16)
		checkNodeInitialization(t, n16)
		n16.nodeNum = uint8(i % 16)
		n16.prefixLen = uint32(i % maxPrefixLen)
		n16s = append(n16s, addr)
	}

	// alloc node48
	n48s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n48 := allocator.allocNode48()
		require.False(t, addr.isNull())
		require.NotNil(t, n48)
		checkNodeInitialization(t, n48)
		n48.nodeNum = uint8(i % 48)
		n48.prefixLen = uint32(i % maxPrefixLen)
		n48s = append(n48s, addr)
	}

	// alloc node256
	n256s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n256 := allocator.allocNode256()
		require.False(t, addr.isNull())
		require.NotNil(t, n256)
		checkNodeInitialization(t, n256)
		n256.nodeNum = uint8(i % 256)
		n256.prefixLen = uint32(i % maxPrefixLen)
		n256s = append(n256s, addr)
	}

	// alloc leaf
	leafs := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		key := []byte(strconv.Itoa(i))
		addr, leaf := allocator.allocLeaf(key)
		require.False(t, addr.isNull())
		require.NotNil(t, leaf)
		require.Equal(t, key, []byte(leaf.getKey()))
		leafs = append(leafs, addr)
	}

	// test memory safety by checking the assign value
	for i, addr := range n4s {
		n4 := allocator.getNode4(addr)
		require.Equal(t, uint8(i%4), n4.nodeNum, i)
		require.Equal(t, uint32(i%maxPrefixLen), n4.prefixLen, i)
	}
	for i, addr := range n16s {
		n16 := allocator.getNode16(addr)
		require.Equal(t, uint8(i%16), n16.nodeNum)
		require.Equal(t, uint32(i%maxPrefixLen), n16.prefixLen, i)
	}
	for i, addr := range n48s {
		n48 := allocator.getNode48(addr)
		require.Equal(t, uint8(i%48), n48.nodeNum)
		require.Equal(t, uint32(i%maxPrefixLen), n48.prefixLen, i)
	}
	for i, addr := range n256s {
		n256 := allocator.getNode256(addr)
		require.Equal(t, uint8(i%256), n256.nodeNum)
		require.Equal(t, uint32(i%maxPrefixLen), n256.prefixLen, i)
	}
	for i, addr := range leafs {
		key := []byte(strconv.Itoa(i))
		leaf := allocator.getLeaf(addr)
		require.Equal(t, key, []byte(leaf.getKey()))
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
		leafKey := append(make([]byte, maxPrefixLen), []byte{1, 2, 3, 4, 5}...)
		leafAddr, _ := allocator.allocLeaf(leafKey)
		an.addChild(&allocator, 2, false, artNode{kind: typeLeaf, addr: leafAddr})
		// the real prefix is [0, ..., 0, 1], but the node.prefix only store [0, ..., 0]
		n.setPrefix(leafKey, maxPrefixLen+1)
		matchKey := append(make([]byte, maxPrefixLen), []byte{1, 22, 33, 44, 55}...)
		mismatchKey := append(make([]byte, maxPrefixLen), []byte{11, 22, 33, 44, 55}...)
		require.Equal(t, uint32(maxPrefixLen+1), an.matchDeep(&allocator, matchKey, 0))
		require.Equal(t, uint32(maxPrefixLen), an.matchDeep(&allocator, mismatchKey, 0))

		// deep match with depth
		leafKey = append(make([]byte, 10), leafKey...)
		matchKey = append(make([]byte, 10), matchKey...)
		mismatchKey = append(make([]byte, 10), mismatchKey...)
		leafAddr, _ = allocator.allocLeaf(leafKey)
		an.swapChild(&allocator, 2, artNode{kind: typeLeaf, addr: leafAddr})
		n.setPrefix(leafKey[10:], maxPrefixLen+1)
		require.Equal(t, uint32(maxPrefixLen+1), an.matchDeep(&allocator, matchKey, 10))
		require.Equal(t, uint32(maxPrefixLen), an.matchDeep(&allocator, mismatchKey, 10))
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

func TestOrderChild(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	addr, n4 := allocator.allocNode4()
	aNode := artNode{kind: typeNode4, addr: addr}
	leaf1, _ := allocator.allocLeaf([]byte{1})
	leaf2, _ := allocator.allocLeaf([]byte{2})
	leaf3, _ := allocator.allocLeaf([]byte{3})
	leaf4, _ := allocator.allocLeaf([]byte{4})
	leaf5, _ := allocator.allocLeaf([]byte{5})
	leaf6, _ := allocator.allocLeaf([]byte{6})

	grow := aNode.addChild(&allocator, 1, false, artNode{kind: typeLeaf, addr: leaf1})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 0, 0, 0}, n4.keys)

	grow = aNode.addChild(&allocator, 3, false, artNode{kind: typeLeaf, addr: leaf3})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 3, 0, 0}, n4.keys)

	grow = aNode.addChild(&allocator, 2, false, artNode{kind: typeLeaf, addr: leaf2})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 2, 3, 0}, n4.keys)

	grow = aNode.addChild(&allocator, 6, false, artNode{kind: typeLeaf, addr: leaf6})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 2, 3, 6}, n4.keys)

	grow = aNode.addChild(&allocator, 4, false, artNode{kind: typeLeaf, addr: leaf4})
	require.True(t, grow)
	require.Equal(t, aNode.kind, typeNode16)
	n16 := aNode.node16(&allocator)
	require.Equal(t, [16]byte{1, 2, 3, 4, 6}, n16.keys)

	grow = aNode.addChild(&allocator, 5, false, artNode{kind: typeLeaf, addr: leaf5})
	require.False(t, grow)
	require.Equal(t, [16]byte{1, 2, 3, 4, 5, 6}, n16.keys)

	node48bits := 0b1111110
	// insert 8-17
	for i := 0; i < 10; i++ {
		b := byte(i + 8)
		leaf, _ := allocator.allocLeaf([]byte{b})
		grow := aNode.addChild(&allocator, b, false, artNode{kind: typeLeaf, addr: leaf})
		require.False(t, grow)
		node48bits |= 1 << b
	}

	leaf7, _ := allocator.allocLeaf([]byte{7})
	grow = aNode.addChild(&allocator, 7, false, artNode{kind: typeLeaf, addr: leaf7})
	require.True(t, grow)
	require.Equal(t, aNode.kind, typeNode48)
	n48 := aNode.node48(&allocator)
	require.Equal(t, n48.keys[7], uint8(16))
	require.Equal(t, n48.present[0], uint64(node48bits|(1<<7)))
	require.Equal(t, n48.nodeNum, uint8(17))

	for i := 18; i <= 48; i++ {
		b := byte(i)
		leaf, _ := allocator.allocLeaf([]byte{b})
		grow := aNode.addChild(&allocator, b, false, artNode{kind: typeLeaf, addr: leaf})
		require.False(t, grow)
		require.Equal(t, aNode.kind, typeNode48)
	}

	leaf255, _ := allocator.allocLeaf([]byte{255})
	grow = aNode.addChild(&allocator, 255, false, artNode{kind: typeLeaf, addr: leaf255})
	require.True(t, grow)
	require.Equal(t, aNode.kind, typeNode256)
	n256 := aNode.node256(&allocator)
	require.Equal(t, n256.children[255].addr, leaf255)
}

func TestN4NextPrevPresentIdx(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	n4Addr, n4 := allocator.allocNode4()
	for i := 0; i < node256cap; i++ {
		nextIdx := n4.nextPresentIdx(i)
		require.Equal(t, 4, nextIdx, i)
		prevIdx := n4.prevPresentIdx(i)
		require.Equal(t, -1, prevIdx, i)
	}

	an := artNode{kind: typeNode4, addr: n4Addr}
	for i := 0; i < 4; i++ {
		n4Addr, _ := allocator.allocNode4()
		an.addChild(&allocator, byte(i), false, artNode{kind: typeNode4, addr: n4Addr})
		for j := 0; j < node256cap; j++ {
			nextIdx := n4.nextPresentIdx(j)
			if j <= i {
				require.Equal(t, j, nextIdx, j)
			} else {
				require.Equal(t, 4, nextIdx, j)
			}
			prevIdx := n4.prevPresentIdx(j)
			if j >= i {
				require.Equal(t, i, prevIdx, j)
			} else {
				require.Equal(t, j, prevIdx, j)
			}
		}
	}
}

func TestN48NextPrevPresentIdx(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	{
		_, n48 := allocator.allocNode48()
		for i := 0; i < node256cap; i++ {
			nextIdx := n48.nextPresentIdx(i)
			require.Equal(t, node256cap, nextIdx, i)
			prevIdx := n48.prevPresentIdx(i)
			require.Equal(t, -1, prevIdx, i)
		}
	}

	for k := 0; k < node256cap; k++ {
		n48Addr, n48 := allocator.allocNode48()
		n4Addr, _ := allocator.allocNode4()
		an := artNode{kind: typeNode48, addr: n48Addr}
		an.addChild(&allocator, byte(k), false, artNode{kind: typeNode4, addr: n4Addr})
		for i := 0; i < 256; i++ {
			nextIdx := n48.nextPresentIdx(i)
			if i <= k {
				require.Equal(t, k, nextIdx, i)
			} else {
				require.Equal(t, node256cap, nextIdx, i)
			}
			prevIdx := n48.prevPresentIdx(i)
			if i >= k {
				require.Equal(t, k, prevIdx, i)
			} else {
				require.Equal(t, -1, prevIdx, i)
			}
		}
	}
}

func TestN256NextPrevPresentIdx(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	{
		_, n256 := allocator.allocNode256()
		for i := 0; i < 256; i++ {
			nextIdx := n256.nextPresentIdx(i)
			require.Equal(t, node256cap, nextIdx, i)
			prevIdx := n256.prevPresentIdx(i)
			require.Equal(t, -1, prevIdx, i)
		}
	}

	for k := 0; k < 256; k++ {
		n256Addr, n256 := allocator.allocNode256()
		n4Addr, _ := allocator.allocNode4()
		an := artNode{kind: typeNode256, addr: n256Addr}
		an.addChild(&allocator, byte(k), false, artNode{kind: typeNode4, addr: n4Addr})
		for i := 0; i < 256; i++ {
			nextIdx := n256.nextPresentIdx(i)
			if i <= k {
				require.Equal(t, k, nextIdx, i)
			} else {
				require.Equal(t, node256cap, nextIdx, i)
			}
			prevIdx := n256.prevPresentIdx(i)
			if i >= k {
				require.Equal(t, k, prevIdx, i)
			} else {
				require.Equal(t, -1, prevIdx, i)
			}
		}
	}
}
