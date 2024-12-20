// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_arena.go
//

// Copyright 2020 PingCAP, Inc.
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

package arena

import (
	"encoding/binary"
	"math"

	"github.com/tikv/client-go/v2/kv"
	"go.uber.org/atomic"
)

const (
	alignMask = 0xFFFFFFF8 // 29 bits of 1 and 3 bits of 0

	nullBlockOffset = math.MaxUint32
	maxBlockSize    = 128 << 20
	initBlockSize   = 4 * 1024
)

var (
	Tombstone          = []byte{}
	NullAddr           = MemdbArenaAddr{math.MaxUint32, math.MaxUint32}
	NullU64Addr uint64 = math.MaxUint64
	BadAddr            = MemdbArenaAddr{math.MaxUint32 - 1, math.MaxUint32}
	endian             = binary.LittleEndian
)

type MemdbArenaAddr struct {
	idx uint32
	off uint32
}

func U64ToAddr(u64 uint64) MemdbArenaAddr {
	return MemdbArenaAddr{uint32(u64 >> 32), uint32(u64)}
}

func (addr MemdbArenaAddr) AsU64() uint64 {
	return uint64(addr.idx)<<32 | uint64(addr.off)
}

func (addr MemdbArenaAddr) IsNull() bool {
	// Combine all checks into a single condition
	return addr == NullAddr || addr.idx == math.MaxUint32 || addr.off == math.MaxUint32
}

func (addr MemdbArenaAddr) ToHandle() MemKeyHandle {
	return MemKeyHandle{idx: uint16(addr.idx), off: addr.off}
}

// store and load is used by vlog, due to pointer in vlog is not aligned.

func (addr MemdbArenaAddr) store(dst []byte) {
	endian.PutUint32(dst, addr.idx)
	endian.PutUint32(dst[4:], addr.off)
}

func (addr *MemdbArenaAddr) load(src []byte) {
	addr.idx = endian.Uint32(src)
	addr.off = endian.Uint32(src[4:])
}

type MemdbArena struct {
	blockSize int
	blocks    []memdbArenaBlock
	// the total size of all blocks, also the approximate memory footprint of the arena.
	capacity uint64
	// when it enlarges or shrinks, call this function with the current memory footprint (in bytes)
	memChangeHook atomic.Pointer[func()]
}

func (a *MemdbArena) Alloc(size int, align bool) (MemdbArenaAddr, []byte) {
	if size > maxBlockSize {
		panic("alloc size is larger than max block size")
	}

	if len(a.blocks) == 0 {
		a.enlarge(size, initBlockSize)
	}

	addr, data := a.allocInLastBlock(size, align)
	if !addr.IsNull() {
		return addr, data
	}

	a.enlarge(size, a.blockSize<<1)
	return a.allocInLastBlock(size, align)
}

func (a *MemdbArena) enlarge(allocSize, blockSize int) {
	a.blockSize = blockSize
	for a.blockSize <= allocSize {
		a.blockSize <<= 1
	}
	// Size will never larger than maxBlockSize.
	if a.blockSize > maxBlockSize {
		a.blockSize = maxBlockSize
	}
	a.blocks = append(a.blocks, memdbArenaBlock{
		buf: make([]byte, a.blockSize),
	})
	a.capacity += uint64(a.blockSize)
	// We shall not call a.OnMemChange() here, since it will make the latest block empty, which breaks a precondition
	// for some operations (e.g. RevertToCheckpoint)
}

func (a *MemdbArena) Blocks() int {
	return len(a.blocks)
}

func (a *MemdbArena) Capacity() uint64 {
	return a.capacity
}

// SetMemChangeHook sets the hook function that will be called when the memory footprint of the arena changes.
func (a *MemdbArena) SetMemChangeHook(hook func()) {
	a.memChangeHook.Store(&hook)
}

// MemHookSet returns whether the memory change hook is set.
func (a *MemdbArena) MemHookSet() bool {
	return a.memChangeHook.Load() != nil
}

// OnMemChange should only be called right before exiting memdb.
// This is because the hook can lead to a panic, and leave memdb in an inconsistent state.
func (a *MemdbArena) OnMemChange() {
	hook := a.memChangeHook.Load()
	if hook != nil {
		(*hook)()
	}
}

func (a *MemdbArena) allocInLastBlock(size int, align bool) (MemdbArenaAddr, []byte) {
	idx := len(a.blocks) - 1
	offset, data := a.blocks[idx].alloc(size, align)
	if offset == nullBlockOffset {
		return NullAddr, nil
	}
	return MemdbArenaAddr{uint32(idx), offset}, data
}

// GetData gets data slice of given addr, DO NOT access others data.
func (a *MemdbArena) GetData(addr MemdbArenaAddr) []byte {
	return a.blocks[addr.idx].buf[addr.off:]
}

func (a *MemdbArena) Reset() {
	for i := range a.blocks {
		a.blocks[i].reset()
	}
	a.blocks = a.blocks[:0]
	a.blockSize = 0
	a.capacity = 0
	a.OnMemChange()
}

type memdbArenaBlock struct {
	buf    []byte
	length int
}

func (a *memdbArenaBlock) alloc(size int, align bool) (uint32, []byte) {
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
	return uint32(offset), a.buf[offset : offset+size]
}

func (a *memdbArenaBlock) reset() {
	a.buf = nil
	a.length = 0
}

// MemDBCheckpoint is the Checkpoint of memory DB.
type MemDBCheckpoint struct {
	blockSize     int
	blocks        int
	offsetInBlock int
}

func (cp *MemDBCheckpoint) IsSamePosition(other *MemDBCheckpoint) bool {
	return cp.blocks == other.blocks && cp.offsetInBlock == other.offsetInBlock
}

func (a *MemdbArena) Checkpoint() MemDBCheckpoint {
	snap := MemDBCheckpoint{
		blockSize: a.blockSize,
		blocks:    len(a.blocks),
	}
	if len(a.blocks) > 0 {
		snap.offsetInBlock = a.blocks[len(a.blocks)-1].length
	}
	return snap
}

func (a *MemdbArena) Truncate(snap *MemDBCheckpoint) {
	for i := snap.blocks; i < len(a.blocks); i++ {
		a.blocks[i] = memdbArenaBlock{}
	}
	a.blocks = a.blocks[:snap.blocks]
	if len(a.blocks) > 0 {
		a.blocks[len(a.blocks)-1].length = snap.offsetInBlock
	}
	a.blockSize = snap.blockSize

	a.capacity = 0
	for _, block := range a.blocks {
		a.capacity += uint64(block.length)
	}
	// We shall not call a.OnMemChange() here, since it may cause a panic and leave memdb in an inconsistent state
}

// KeyFlagsGetter is an interface to get key and key flags, usually a leaf or node.
type KeyFlagsGetter interface {
	GetKey() []byte
	GetKeyFlags() kv.KeyFlags
}

// VlogMemDB is the interface of the memory buffer which supports vlog to revert node and inspect node.
type VlogMemDB[G KeyFlagsGetter] interface {
	RevertVAddr(hdr *MemdbVlogHdr)
	InspectNode(addr MemdbArenaAddr) (G, MemdbArenaAddr)
}

type MemdbVlog[G KeyFlagsGetter, M VlogMemDB[G]] struct {
	MemdbArena
}

const memdbVlogHdrSize = 8 + 8 + 4

type MemdbVlogHdr struct {
	NodeAddr MemdbArenaAddr
	OldValue MemdbArenaAddr
	ValueLen uint32
}

func (hdr *MemdbVlogHdr) store(dst []byte) {
	cursor := 0
	endian.PutUint32(dst[cursor:], hdr.ValueLen)
	cursor += 4
	hdr.OldValue.store(dst[cursor:])
	cursor += 8
	hdr.NodeAddr.store(dst[cursor:])
}

func (hdr *MemdbVlogHdr) load(src []byte) {
	cursor := 0
	hdr.ValueLen = endian.Uint32(src[cursor:])
	cursor += 4
	hdr.OldValue.load(src[cursor:])
	cursor += 8
	hdr.NodeAddr.load(src[cursor:])
}

// AppendValue appends a value and it's vlog header to the vlog.
func (l *MemdbVlog[G, M]) AppendValue(nodeAddr MemdbArenaAddr, oldValueAddr MemdbArenaAddr, value []byte) MemdbArenaAddr {
	prevBlocks := len(l.blocks)
	size := memdbVlogHdrSize + len(value)
	addr, mem := l.Alloc(size, false)

	copy(mem, value)
	hdr := MemdbVlogHdr{nodeAddr, oldValueAddr, uint32(len(value))}
	hdr.store(mem[len(value):])

	addr.off += uint32(size)
	if prevBlocks != len(l.blocks) {
		l.OnMemChange()
	}
	return addr
}

// GetValue is a pure function that gets a value.
func (l *MemdbVlog[G, M]) GetValue(addr MemdbArenaAddr) []byte {
	lenOff := addr.off - memdbVlogHdrSize
	block := l.blocks[addr.idx].buf
	valueLen := endian.Uint32(block[lenOff:])
	if valueLen == 0 {
		return Tombstone
	}
	valueOff := lenOff - valueLen
	return block[valueOff:lenOff:lenOff]
}

func (l *MemdbVlog[G, M]) GetSnapshotValue(addr MemdbArenaAddr, snap *MemDBCheckpoint) ([]byte, bool) {
	result := l.SelectValueHistory(addr, func(addr MemdbArenaAddr) bool {
		return !l.CanModify(snap, addr)
	})
	if result.IsNull() {
		return nil, false
	}
	return l.GetValue(result), true
}

func (l *MemdbVlog[G, M]) SelectValueHistory(addr MemdbArenaAddr, predicate func(MemdbArenaAddr) bool) MemdbArenaAddr {
	for !addr.IsNull() {
		if predicate(addr) {
			return addr
		}
		var hdr MemdbVlogHdr
		hdr.load(l.blocks[addr.idx].buf[addr.off-memdbVlogHdrSize:])
		addr = hdr.OldValue
	}
	return NullAddr
}

func (l *MemdbVlog[G, M]) RevertToCheckpoint(m M, cp *MemDBCheckpoint) {
	cursor := l.Checkpoint()
	for !cp.IsSamePosition(&cursor) {
		hdrOff := cursor.offsetInBlock - memdbVlogHdrSize
		block := l.blocks[cursor.blocks-1].buf
		var hdr MemdbVlogHdr
		hdr.load(block[hdrOff:])
		m.RevertVAddr(&hdr)
		l.moveBackCursor(&cursor, &hdr)
	}
}

func (l *MemdbVlog[G, M]) InspectKVInLog(m M, head, tail *MemDBCheckpoint, f func([]byte, kv.KeyFlags, []byte)) {
	cursor := *tail
	for !head.IsSamePosition(&cursor) {
		cursorAddr := MemdbArenaAddr{idx: uint32(cursor.blocks - 1), off: uint32(cursor.offsetInBlock)}
		hdrOff := cursorAddr.off - memdbVlogHdrSize
		block := l.blocks[cursorAddr.idx].buf
		var hdr MemdbVlogHdr
		hdr.load(block[hdrOff:])

		node, vptr := m.InspectNode(hdr.NodeAddr)

		// Skip older versions.
		if vptr == cursorAddr {
			value := block[hdrOff-hdr.ValueLen : hdrOff]
			f(node.GetKey(), node.GetKeyFlags(), value)
		}

		l.moveBackCursor(&cursor, &hdr)
	}
}

func (l *MemdbVlog[G, M]) moveBackCursor(cursor *MemDBCheckpoint, hdr *MemdbVlogHdr) {
	cursor.offsetInBlock -= (memdbVlogHdrSize + int(hdr.ValueLen))
	if cursor.offsetInBlock == 0 {
		cursor.blocks--
		if cursor.blocks > 0 {
			cursor.offsetInBlock = l.blocks[cursor.blocks-1].length
		}
	}
}

func (l *MemdbVlog[G, M]) CanModify(cp *MemDBCheckpoint, addr MemdbArenaAddr) bool {
	if cp == nil {
		return true
	}
	if int(addr.idx) > cp.blocks-1 {
		return true
	}
	if int(addr.idx) == cp.blocks-1 && int(addr.off) > cp.offsetInBlock {
		return true
	}
	return false
}

// MemKeyHandle represents a pointer for key in MemBuffer.
type MemKeyHandle struct {
	// Opaque user data
	UserData uint16
	idx      uint16
	off      uint32
}

func (h MemKeyHandle) ToAddr() MemdbArenaAddr {
	return MemdbArenaAddr{idx: uint32(h.idx), off: h.off}
}
