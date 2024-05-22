package art

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

const (
	alignMask = 1<<32 - 8 // 29 bit 1 and 3 bit 0.

	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
	initBlockSize   = 4 * 1024
)

var (
	nullAddr = nodeAddr{math.MaxUint32, math.MaxUint32}
	endian   = binary.LittleEndian
)

type nodeAddr struct {
	idx uint32
	off uint32
}

func (addr nodeAddr) isNull() bool {
	if addr == nullAddr {
		return true
	}
	if addr.idx == math.MaxUint32 || addr.off == math.MaxUint32 {
		// defensive programming, the code should never run to here.
		// it always means something wrong... (maybe caused by data race?)
		// because we never set part of idx/off to math.MaxUint64
		logutil.BgLogger().Warn("Invalid nodeAddr", zap.Uint32("idx", addr.idx), zap.Uint32("off", addr.off))
		return true
	}
	return false
}

// store and load is used by vlog, due to pointer in vlog is not aligned.
func (addr nodeAddr) store(dst []byte) {
	endian.PutUint32(dst, addr.idx)
	endian.PutUint32(dst[4:], addr.off)
}

func (addr *nodeAddr) load(src []byte) {
	addr.idx = endian.Uint32(src)
	addr.off = endian.Uint32(src[4:])
}

type memArenaBlock struct {
	buf    []byte
	length int
}

type memArena struct {
	blockSize int
	blocks    []memArenaBlock
	// the total size of all blocks, also the approximate memory footprint of the arena.
	capacity uint64
	// when it enlarges or shrinks, call this function with the current memory footprint (in bytes)
	memChangeHook atomic.Pointer[func()]
}

// fixedSizeArena is a fixed size arena allocator.
// because the size of each type of node is fixed, the discarded nodes can be reused.
// reusing blocks reduces the memory pieces.
type nodeArena struct {
	memArena
	freeNode4   []nodeAddr
	freeNode16  []nodeAddr
	freeNode48  []nodeAddr
	freeNode256 []nodeAddr
}

type vlogArena struct {
	memArena
}

type leafArena struct {
	memArena
}

type artAllocator struct {
	vlogAllocator vlogArena
	nodeAllocator nodeArena
}

func (allocator *artAllocator) init() {
	allocator.nodeAllocator.freeNode4 = make([]nodeAddr, 0, 1<<4)
	allocator.nodeAllocator.freeNode16 = make([]nodeAddr, 0, 1<<3)
	allocator.nodeAllocator.freeNode48 = make([]nodeAddr, 0, 1<<2)
	allocator.nodeAllocator.freeNode256 = make([]nodeAddr, 0, 1)
}

func (f *artAllocator) allocNode4() (nodeAddr, *node4) {
	var (
		addr nodeAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode4) > 0 {
		addr = f.nodeAllocator.freeNode4[len(f.nodeAllocator.freeNode4)-1]
		f.nodeAllocator.freeNode4 = f.nodeAllocator.freeNode4[:len(f.nodeAllocator.freeNode4)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(node4size, true)
	}
	n4 := (*node4)(unsafe.Pointer(&data[0]))
	n4.init()
	return addr, n4
}

func (f *artAllocator) freeNode4(addr nodeAddr) {
	f.nodeAllocator.freeNode4 = append(f.nodeAllocator.freeNode4, addr)
}

func (f *artAllocator) getNode4(addr nodeAddr) *node4 {
	if addr.isNull() {
		return nil
	}
	data := f.nodeAllocator.getData(addr)
	return (*node4)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode16() (nodeAddr, *node16) {
	var (
		addr nodeAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode16) > 0 {
		addr = f.nodeAllocator.freeNode16[len(f.nodeAllocator.freeNode16)-1]
		f.nodeAllocator.freeNode16 = f.nodeAllocator.freeNode16[:len(f.nodeAllocator.freeNode16)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(node16size, true)
	}
	n16 := (*node16)(unsafe.Pointer(&data[0]))
	n16.init()
	return addr, n16
}

func (f *artAllocator) freeNode16(addr nodeAddr) {
	f.nodeAllocator.freeNode16 = append(f.nodeAllocator.freeNode16, addr)
}

func (f *artAllocator) getNode16(addr nodeAddr) *node16 {
	if addr.isNull() {
		return nil
	}
	data := f.nodeAllocator.getData(addr)
	return (*node16)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode48() (nodeAddr, *node48) {
	var (
		addr nodeAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode48) > 0 {
		addr = f.nodeAllocator.freeNode48[len(f.nodeAllocator.freeNode48)-1]
		f.nodeAllocator.freeNode48 = f.nodeAllocator.freeNode48[:len(f.nodeAllocator.freeNode48)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(node48size, true)
	}
	n48 := (*node48)(unsafe.Pointer(&data[0]))
	n48.init()
	return addr, n48
}

func (f *artAllocator) freeNode48(addr nodeAddr) {
	f.nodeAllocator.freeNode48 = append(f.nodeAllocator.freeNode48, addr)
}

func (f *artAllocator) getNode48(addr nodeAddr) *node48 {
	if addr.isNull() {
		return nil
	}
	data := f.nodeAllocator.getData(addr)
	return (*node48)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode256() (nodeAddr, *node256) {
	var (
		addr nodeAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode256) > 0 {
		addr = f.nodeAllocator.freeNode256[len(f.nodeAllocator.freeNode256)-1]
		f.nodeAllocator.freeNode256 = f.nodeAllocator.freeNode256[:len(f.nodeAllocator.freeNode256)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(node256size, true)
	}
	n256 := (*node256)(unsafe.Pointer(&data[0]))
	n256.init()
	return addr, n256
}

func (f *artAllocator) freeNode256(addr nodeAddr) {
	f.nodeAllocator.freeNode256 = append(f.nodeAllocator.freeNode256, addr)
}

func (f *artAllocator) getNode256(addr nodeAddr) *node256 {
	if addr.isNull() {
		return nil
	}
	data := f.nodeAllocator.getData(addr)
	return (*node256)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocLeaf(key Key) (nodeAddr, *leaf) {
	size := leafSize + len(key)
	addr, data := f.nodeAllocator.alloc(size, true)
	lf := (*leaf)(unsafe.Pointer(&data[0]))
	lf.klen = uint16(len(key))
	lf.flags = 0
	lf.vAddr = nullAddr
	copy(data[leafSize:], key)
	return addr, lf
}

func (f *artAllocator) getLeaf(addr nodeAddr) *leaf {
	if addr.isNull() {
		return nil
	}
	data := f.nodeAllocator.getData(addr)
	return (*leaf)(unsafe.Pointer(&data[0]))
}

// memArena get all the data, DO NOT access others data.
func (a *memArena) getData(addr nodeAddr) []byte {
	return a.blocks[addr.idx].buf[addr.off:]
}

func (a *memArena) alloc(size int, align bool) (nodeAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}
	prevBlocks := len(a.blocks)
	if len(a.blocks) == 0 {
		a.enlarge(size, initBlockSize)
	}
	addr, data := a.allocInLastBlock(size, align)
	if !addr.isNull() {
		return addr, data
	}

	a.enlarge(size, a.blockSize<<1)
	addr, data = a.allocInLastBlock(size, align)
	if prevBlocks != len(a.blocks) {
		a.onMemChange()
	}
	return addr, data
}

func (a *memArena) enlarge(allocSize, blockSize int) {
	a.blockSize = blockSize
	for a.blockSize <= allocSize {
		a.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, memArenaBlock{
		buf: make([]byte, a.blockSize),
	})
	a.capacity += uint64(a.blockSize)
	// We shall not call a.onMemChange() here, since it will make the latest block empty, which breaks a precondition
	// for some operations (e.g. revertToCheckpoint)
}

func (a *memArena) allocInLastBlock(size int, align bool) (nodeAddr, []byte) {
	idx := len(a.blocks) - 1
	offset, data := a.blocks[idx].alloc(size, align)
	if offset == nullBlockOffset {
		return nullAddr, nil
	}
	return nodeAddr{uint32(idx), offset}, data
}

func (a *memArena) onMemChange() {
	hook := a.memChangeHook.Load()
	if hook != nil {
		(*hook)()
	}
}

func (a *memArena) reset() {
	for i := range a.blocks {
		a.blocks[i].reset()
	}
	a.blocks = a.blocks[:0]
	a.blockSize = 0
	a.capacity = 0
	a.onMemChange()
}

func (a *memArenaBlock) alloc(size int, align bool) (uint32, []byte) {
	offset := a.length
	if align {
		// We must align the allocated address for node
		// to make runtime.checkptrAlignment happy.
		offset = (a.length + 7) & alignMask
	}
	newLen := offset + size
	if newLen > len(a.buf) {
		return nullBlockOffset, nil
	}
	a.length = newLen
	return uint32(offset), a.buf[offset:newLen]
}

func (a *memArenaBlock) reset() {
	a.buf = nil
	a.length = 0
}

const memdbVlogHdrSize = 8 + 8 + 4

type vlogHdr struct {
	nodeAddr nodeAddr
	oldValue nodeAddr
	valueLen uint32
}

func (hdr *vlogHdr) store(dst []byte) {
	cursor := 0
	endian.PutUint32(dst[cursor:], hdr.valueLen)
	cursor += 4
	hdr.oldValue.store(dst[cursor:])
	cursor += 8
	hdr.nodeAddr.store(dst[cursor:])
}

func (hdr *vlogHdr) load(src []byte) {
	cursor := 0
	hdr.valueLen = endian.Uint32(src[cursor:])
	cursor += 4
	hdr.oldValue.load(src[cursor:])
	cursor += 8
	hdr.nodeAddr.load(src[cursor:])
}

func (f *artAllocator) allocValue(leafAddr nodeAddr, oldAddr nodeAddr, value []byte) nodeAddr {
	size := memdbVlogHdrSize + len(value)
	addr, data := f.vlogAllocator.alloc(size, false)
	copy(data, value)
	hdr := vlogHdr{leafAddr, oldAddr, uint32(len(value))}
	hdr.store(data[len(value):])
	addr.off += uint32(size)
	return addr
}

func (f *artAllocator) getValue(valAddr nodeAddr) []byte {
	hdrOff := valAddr.off - memdbVlogHdrSize
	block := f.vlogAllocator.blocks[valAddr.idx].buf
	valLen := endian.Uint32(block[hdrOff:])
	if valLen == 0 {
		return tombstone
	}
	valOff := hdrOff - valLen
	return block[valOff:hdrOff:hdrOff]
}
