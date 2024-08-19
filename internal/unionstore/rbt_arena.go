package unionstore

import (
	"unsafe"

	"github.com/tikv/client-go/v2/kv"
)

type nodeAllocator struct {
	memdbArena

	// Dummy node, so that we can make X.left.up = X.
	// We then use this instead of NULL to mean the top or bottom
	// end of the rb tree. It is a black node.
	nullNode memdbNode
}

func (a *nodeAllocator) init() {
	a.nullNode = memdbNode{
		up:    nullAddr,
		left:  nullAddr,
		right: nullAddr,
		vptr:  nullAddr,
	}
}

func (a *nodeAllocator) getNode(addr memdbArenaAddr) *memdbNode {
	if addr.isNull() {
		return &a.nullNode
	}

	return (*memdbNode)(unsafe.Pointer(&a.blocks[addr.idx].buf[addr.off]))
}

func (a *nodeAllocator) allocNode(key []byte) (memdbArenaAddr, *memdbNode) {
	nodeSize := 8*4 + 2 + kv.FlagBytes + len(key)
	prevBlocks := len(a.blocks)
	addr, mem := a.alloc(nodeSize, true)
	n := (*memdbNode)(unsafe.Pointer(&mem[0]))
	n.vptr = nullAddr
	n.klen = uint16(len(key))
	copy(n.getKey(), key)
	if prevBlocks != len(a.blocks) {
		a.onMemChange()
	}
	return addr, n
}

func (a *nodeAllocator) freeNode(addr memdbArenaAddr) {
	if testMode {
		// Make it easier for debug.
		n := a.getNode(addr)
		badAddr := nullAddr
		badAddr.idx--
		n.left = badAddr
		n.right = badAddr
		n.up = badAddr
		n.vptr = badAddr
		return
	}
	// TODO: reuse freed nodes. Need to fix lastTraversedNode when implementing this.
}

func (a *nodeAllocator) reset() {
	a.memdbArena.reset()
	a.init()
}
