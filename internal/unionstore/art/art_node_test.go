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
		n4.prefixLen = uint32(i % maxPrefixLen)
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
		n16.prefixLen = uint32(i % maxPrefixLen)
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
		n48.prefixLen = uint32(i % maxPrefixLen)
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
		n256.prefixLen = uint32(i % maxPrefixLen)
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

			node := an.node(&allocator)
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

			node := an.node(&allocator)
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
		return an.node4(&allocator).keys[:]
	}, 4)

	testFn(func() *artNode {
		addr, _ := allocator.allocNode16()
		return &artNode{kind: typeNode16, addr: addr}
	}, func(an *artNode) []byte {
		return an.node16(&allocator).keys[:]
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
