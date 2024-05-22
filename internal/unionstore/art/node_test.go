package art

import (
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeSize(t *testing.T) {
	var (
		an    artNode
		n4    node4
		n16   node16
		n48   node48
		n256  node256
		nLeaf leaf
	)

	require.Equal(t, int(unsafe.Sizeof(an)), artNodeSize)
	require.Equal(t, int(unsafe.Sizeof(n4)), node4size)
	require.Equal(t, int(unsafe.Sizeof(n16)), node16size)
	require.Equal(t, int(unsafe.Sizeof(n48)), node48size)
	require.Equal(t, int(unsafe.Sizeof(n256)), node256size)
	require.Equal(t, int(unsafe.Sizeof(nLeaf)), leafSize)
}

func checkNodeInitialization(t *testing.T, n any) {
	var node *node
	switch n := n.(type) {
	case *node4:
		node = &n.node
		require.Equal(t, uint8(0), n.present)
	case *node16:
		node = &n.node
		require.Equal(t, uint16(0), n.present)
	case *node48:
		node = &n.node
		require.Equal(t, [4]uint64{0, 0, 0, 0}, n.present)
	case *node256:
		node = &n.node
		for i := 0; i < node256cap; i++ {
			require.Equal(t, n.children[i], nullArtNode)
		}
	default:
		require.Fail(t, "unknown node type")
	}
	require.Equal(t, uint8(0), node.nodeNum)
	require.Equal(t, uint8(0), node.prefixLen)
	require.Equal(t, node.inplaceLeaf, nullArtNode)
}

func TestAllocNode(t *testing.T) {
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
		n4s = append(n4s, addr)
	}
	for i, addr := range n4s {
		n4 := allocator.getNode4(addr)
		require.Equal(t, uint8(i%4), n4.nodeNum, i)
	}

	// alloc node16
	n16s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n16 := allocator.allocNode16()
		require.False(t, addr.isNull())
		require.NotNil(t, n16)
		checkNodeInitialization(t, n16)
		n16.nodeNum = uint8(i % 16)
		n16s = append(n16s, addr)
	}
	for i, addr := range n16s {
		n16 := allocator.getNode16(addr)
		require.Equal(t, uint8(i%16), n16.nodeNum)
	}

	// alloc node48
	n48s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n48 := allocator.allocNode48()
		require.False(t, addr.isNull())
		require.NotNil(t, n48)
		checkNodeInitialization(t, n48)
		n48.nodeNum = uint8(i % 48)
		n48s = append(n48s, addr)
	}
	for i, addr := range n48s {
		n48 := allocator.getNode48(addr)
		require.Equal(t, uint8(i%48), n48.nodeNum)
	}

	// alloc node256
	n256s := make([]nodeAddr, 0, cnt)
	for i := 0; i < cnt; i++ {
		addr, n256 := allocator.allocNode256()
		require.False(t, addr.isNull())
		require.NotNil(t, n256)
		checkNodeInitialization(t, n256)
		n256.nodeNum = uint8(i % 256)
		n256s = append(n256s, addr)
	}
	for i, addr := range n256s {
		n256 := allocator.getNode256(addr)
		require.Equal(t, uint8(i%256), n256.nodeNum)
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
	for i, addr := range leafs {
		key := []byte(strconv.Itoa(i))
		leaf := allocator.getLeaf(addr)
		require.Equal(t, key, []byte(leaf.getKey()))
	}
}

func TestNodeMatchWithKey(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	key := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	_, n16 := allocator.allocNode16()

	n16.setPrefix([]byte{1, 2, 3, 4, 5, 66, 77, 88, 99}, 5)

	idx := n16.match(key, 0)
	assert.Equal(t, uint32(5), idx)
	idx = n16.match(key[:4], 0)
	assert.Equal(t, uint32(4), idx)
	idx = n16.match(key, 1)
	assert.Equal(t, uint32(0), idx)
	idx = n16.match(append([]byte{1}, key...), 1)
	assert.Equal(t, uint32(5), idx)
	idx = n16.match(key, 100)
	assert.Equal(t, uint32(0), idx)
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
	require.Equal(t, n4.present, uint8(0b0001))

	grow = aNode.addChild(&allocator, 3, false, artNode{kind: typeLeaf, addr: leaf3})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 3, 0, 0}, n4.keys)
	require.Equal(t, n4.present, uint8(0b0011))

	grow = aNode.addChild(&allocator, 2, false, artNode{kind: typeLeaf, addr: leaf2})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 2, 3, 0}, n4.keys)
	require.Equal(t, n4.present, uint8(0b0111))

	grow = aNode.addChild(&allocator, 6, false, artNode{kind: typeLeaf, addr: leaf6})
	require.False(t, grow)
	require.Equal(t, [4]byte{1, 2, 3, 6}, n4.keys)
	require.Equal(t, n4.present, uint8(0b1111))

	grow = aNode.addChild(&allocator, 4, false, artNode{kind: typeLeaf, addr: leaf4})
	require.True(t, grow)
	require.Equal(t, aNode.kind, typeNode16)
	n16 := aNode.node16(&allocator)
	require.Equal(t, [16]byte{1, 2, 3, 4, 6}, n16.keys)
	require.Equal(t, n16.present, uint16(0b11111))

	grow = aNode.addChild(&allocator, 5, false, artNode{kind: typeLeaf, addr: leaf5})
	require.False(t, grow)
	require.Equal(t, [16]byte{1, 2, 3, 4, 5, 6}, n16.keys)
	require.Equal(t, n16.present, uint16(0b111111))

	node48bits := 0b1111110
	// insert 8-17
	for i := 0; i < 10; i++ {
		b := byte(i + 8)
		leaf, _ := allocator.allocLeaf([]byte{b})
		grow := aNode.addChild(&allocator, b, false, artNode{kind: typeLeaf, addr: leaf})
		require.False(t, grow)
		require.Equal(t, n16.present, uint16((1<<(i+6+1))-1))
		node48bits |= (1 << b)
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

func TestN16NextPrevPresentIdx(t *testing.T) {
	var allocator artAllocator
	allocator.init()

	n16Addr, n16 := allocator.allocNode16()
	for i := 0; i < node256cap; i++ {
		nextIdx := n16.nextPresentIdx(i)
		require.Equal(t, 16, nextIdx, i)
		prevIdx := n16.prevPresentIdx(i)
		require.Equal(t, -1, prevIdx, i)
	}

	an := artNode{kind: typeNode16, addr: n16Addr}
	for i := 0; i < node256cap; i++ {
		n4Addr, _ := allocator.allocNode4()
		an.addChild(&allocator, byte(i), false, artNode{kind: typeNode4, addr: n4Addr})
		for j := 0; j < 16; j++ {
			nextIdx := n16.nextPresentIdx(j)
			if j <= i {
				require.Equal(t, j, nextIdx, j)
			} else {
				require.Equal(t, 16, nextIdx, j)
			}
			prevIdx := n16.prevPresentIdx(j)
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
