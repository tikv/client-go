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

package unionstore

import (
	"fmt"
	"math"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

type ART struct {
	sync.RWMutex
	skipMutex       bool
	allocator       artAllocator
	root            artNode
	stages          []MemDBCheckpoint
	vlogInvalid     bool
	dirty           bool
	entrySizeLimit  uint64
	bufferSizeLimit uint64
	len             int
	size            int
}

func newART() *ART {
	var t ART
	t.root = nullArtNode
	t.stages = make([]MemDBCheckpoint, 0, 2)
	t.entrySizeLimit = math.MaxUint64
	t.bufferSizeLimit = math.MaxUint64
	t.allocator.nodeAllocator.freeNode4 = make([]memdbArenaAddr, 0, 1<<4)
	t.allocator.nodeAllocator.freeNode16 = make([]memdbArenaAddr, 0, 1<<3)
	t.allocator.nodeAllocator.freeNode48 = make([]memdbArenaAddr, 0, 1<<2)
	return &t
}

func (t *ART) Set(key, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return t.set(key, value, nil)
}

// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
func (t *ART) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return t.set(key, value, ops)
}

func (t *ART) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	_ = t.set(key, nil, ops)
}

func (t *ART) Delete(key []byte) error {
	return t.set(key, tombstone, nil)
}

func (t *ART) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return t.set(key, tombstone, ops)
}

func (t *ART) Get(key []byte) ([]byte, error) {
	_, leaf := t.search(key)
	if leaf == nil || leaf.vAddr.isNull() {
		return nil, tikverr.ErrNotExist
	}
	return t.getValue(leaf), nil
}

// GetFlags returns the latest flags associated with key.
func (t *ART) GetFlags(key []byte) (kv.KeyFlags, error) {
	_, leaf := t.search(key)
	if leaf == nil {
		return 0, tikverr.ErrNotExist
	}
	if leaf.vAddr.isNull() && leaf.isDeleted() {
		return 0, tikverr.ErrNotExist
	}
	return leaf.getKeyFlags(), nil
}

func (t *ART) set(key artKey, value []byte, ops []kv.FlagsOp) error {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	if value != nil {
		if size := uint64(len(key) + len(value)); size > t.entrySizeLimit {
			return &tikverr.ErrEntryTooLarge{
				Limit: t.entrySizeLimit,
				Size:  size,
			}
		}
	}
	if len(t.stages) == 0 {
		t.dirty = true
	}
	addr, leaf := t.recursiveInsert(key)
	t.setValue(addr, leaf, value, ops)
	if uint64(t.Size()) > t.bufferSizeLimit {
		return &tikverr.ErrTxnTooLarge{Size: t.Size()}
	}
	return nil
}

// recursiveInsert returns the node address of the key.
// if insert is true, it will insert the key if not exists, unless nullAddr is returned.
func (t *ART) recursiveInsert(key artKey) (memdbArenaAddr, *artLeaf) {
	// lazy init root node and allocator.
	// this saves memory for read only txns.
	if t.root.addr.isNull() {
		addr, _ := t.allocator.allocNode4()
		t.root = artNode{kind: typeARTNode4, addr: addr}
	}

	depth := uint32(0)
	prev := nullArtNode
	current := t.root
	var node *artNodeBase
	for {
		prevDepth := int(depth - 1)
		if current.isLeaf() {
			leaf1 := current.leaf(&t.allocator)
			if leaf1.match(depth-1, key) {
				// same key, return the artLeaf and overwrite the value.
				return current.addr, leaf1
			}
			// Expand the artLeaf to a artNode4.
			//        ┌─────────┐
			//        │   new   │
			//        │  artNode4  │
			//        └────┬────┘
			//             │
			//      ┌──────┴──────┐
			//      │             │
			// ┌────▼────┐   ┌────▼────┐
			// │   old   │   │   new   │
			// │  leaf1  │   │  leaf2  │
			// └─────────┘   └─────────┘
			newLeafAddr, leaf2 := t.newLeaf(key)
			l1Key, l2Key := artKey(leaf1.getKey()), artKey(leaf2.getKey())
			lcp := longestCommonPrefix(l1Key, l2Key, depth)

			an, n4 := t.newNode4()
			n4.setPrefix(key[depth:], lcp)

			depth += lcp
			an.addChild(&t.allocator, l1Key.charAt(int(depth)), !l1Key.valid(int(depth)), current)
			an.addChild(&t.allocator, l2Key.charAt(int(depth)), !l2Key.valid(int(depth)), newLeafAddr)
			if prev == nullArtNode {
				t.root = an
			} else {
				prev.swapChild(&t.allocator, key.charAt(prevDepth), an)
			}
			return newLeafAddr.addr, leaf2
		}

		// inline: performance critical path
		// get the basic node information.
		switch current.kind {
		case typeARTNode4:
			node = &current.node4(&t.allocator).artNodeBase
		case typeARTNode16:
			node = &current.node16(&t.allocator).artNodeBase
		case typeARTNode48:
			node = &current.node48(&t.allocator).artNodeBase
		case typeARTNode256:
			node = &current.node256(&t.allocator).artNodeBase
		default:
			panic("invalid artNodeBase kind")
		}

		if node.prefixLen > 0 {
			mismatchIdx := current.matchDeep(&t.allocator, key, depth)
			if mismatchIdx >= node.prefixLen {
				// all the prefix match, go deeper.
				depth += node.prefixLen
				_, next := current.findChild(&t.allocator, key.charAt(int(depth)), key.valid(int(depth)))

				if next == nullArtNode {
					newLeaf, lf := t.newLeaf(key)
					grown := current.addChild(&t.allocator, key.charAt(int(depth)), !key.valid(int(depth)), newLeaf)
					if grown {
						if prev == nullArtNode {
							t.root = current
						} else {
							prev.swapChild(&t.allocator, key.charAt(prevDepth), current)
						}
					}
					return newLeaf.addr, lf
				}
				prev = current
				current = next
				depth++
				continue
			}
			// instead, we split the node into different prefixes.
			newArtNode, newN4 := t.newNode4()
			newN4.prefixLen = mismatchIdx
			copy(newN4.prefix[:], key[depth:depth+mismatchIdx])

			// move the current node as the children of the new node.
			if node.prefixLen <= artMaxPrefixLen {
				nodeKey := node.prefix[mismatchIdx]
				node.prefixLen -= mismatchIdx + 1
				copy(node.prefix[:], node.prefix[mismatchIdx+1:])
				newArtNode.addChild(&t.allocator, nodeKey, false, current)
			} else {
				node.prefixLen -= mismatchIdx + 1
				leafArtNode := minimum(&t.allocator, current)
				leaf := leafArtNode.leaf(&t.allocator)
				leafKey := artKey(leaf.getKey())
				kMin := depth + mismatchIdx + 1
				kMax := depth + mismatchIdx + 1 + min(node.prefixLen, artMaxPrefixLen)
				copy(node.prefix[:], leafKey[kMin:kMax])
				newArtNode.addChild(&t.allocator, leafKey.charAt(int(depth+mismatchIdx)), !leafKey.valid(int(depth)), current)
			}

			// insert the artLeaf into new node
			newLeafAddr, newLeaf := t.newLeaf(key)
			newArtNode.addChild(&t.allocator, key.charAt(int(depth+mismatchIdx)), !key.valid(int(depth+mismatchIdx)), newLeafAddr)
			if prev == nullArtNode {
				t.root = newArtNode
			} else {
				prev.swapChild(&t.allocator, key.charAt(prevDepth), newArtNode)
			}
			return newLeafAddr.addr, newLeaf
		}
		// next
		valid := key.valid(int(depth))
		_, next := current.findChild(&t.allocator, key.charAt(int(depth)), valid)
		if next == nullArtNode {
			newLeaf, lf := t.newLeaf(key)
			if current.addChild(&t.allocator, key.charAt(int(depth)), !key.valid(int(depth)), newLeaf) {
				if prev == nullArtNode {
					t.root = current
				} else {
					prev.swapChild(&t.allocator, key.charAt(prevDepth), current)
				}
			}
			return newLeaf.addr, lf
		}
		if !valid && next.kind == typeARTLeaf {
			return next.addr, next.leaf(&t.allocator)
		}
		prev = current
		current = next
		depth++
		continue
	}
}

func (t *ART) search(key artKey) (memdbArenaAddr, *artLeaf) {
	current := t.root
	if current == nullArtNode {
		return nullAddr, nil
	}
	depth := uint32(0)
	var node *artNodeBase
	for {
		if current.isLeaf() {
			lf := current.leaf(&t.allocator)
			if lf.match(0, key) {
				return current.addr, lf
			}
			return nullAddr, nil
		}

		// inline: performance critical path
		// get the basic node information.
		switch current.kind {
		case typeARTNode4:
			node = &current.node4(&t.allocator).artNodeBase
		case typeARTNode16:
			node = &current.node16(&t.allocator).artNodeBase
		case typeARTNode48:
			node = &current.node48(&t.allocator).artNodeBase
		case typeARTNode256:
			node = &current.node256(&t.allocator).artNodeBase
		default:
			panic("invalid artNodeBase kind")
		}

		if node.prefixLen > 0 {
			prefixLen := node.match(key, depth)
			if prefixLen < min(node.prefixLen, artMaxPrefixLen) {
				return nullAddr, nil
			}
			// If node.prefixLen > artMaxPrefixLen, we optimistically match the prefix here.
			// False positive is possible, but it's fine since we will check the full artLeaf key at last.
			depth += node.prefixLen
		}

		_, current = current.findChild(&t.allocator, key.charAt(int(depth)), key.valid(int(depth)))
		if current.addr == nullAddr {
			return nullAddr, nil
		}
		depth++
	}
}

func (t *ART) newNode4() (artNode, *artNode4) {
	addr, n4 := t.allocator.allocNode4()
	return artNode{kind: typeARTNode4, addr: addr}, n4
}

func (t *ART) newLeaf(key artKey) (artNode, *artLeaf) {
	addr, lf := t.allocator.allocLeaf(key)
	return artNode{kind: typeARTLeaf, addr: addr}, lf
}

func (t *ART) setValue(addr memdbArenaAddr, l *artLeaf, value []byte, ops []kv.FlagsOp) {
	flags := l.getKeyFlags()
	if flags == 0 && l.vAddr == nullAddr {
		t.len++
		t.size += int(l.klen)
	}
	if value != nil {
		flags = kv.ApplyFlagsOps(flags, append([]kv.FlagsOp{kv.DelNeedConstraintCheckInPrewrite}, ops...)...)
	} else {
		// an UpdateFlag operation, do not delete the NeedConstraintCheckInPrewrite flag.
		flags = kv.ApplyFlagsOps(flags, ops...)
	}
	if flags.AndPersistent() != 0 {
		t.dirty = true
	}
	l.setKeyFlags(flags)
	if value == nil {
		// value == nil means it updates flags only.
		return
	}
	if t.trySwapValue(l.vAddr, value) {
		return
	}
	t.size += len(value)
	vAddr := t.allocator.vlogAllocator.appendValue(addr, l.vAddr, value)
	l.vAddr = vAddr
}

// trySwapValue checks if the value can be updated in place, return true if it's updated.
func (t *ART) trySwapValue(addr memdbArenaAddr, value []byte) bool {
	if addr.isNull() {
		return false
	}
	if len(t.stages) > 0 {
		cp := t.stages[len(t.stages)-1]
		if !t.canSwapValue(&cp, addr) {
			return false
		}
	}
	oldVal := t.allocator.vlogAllocator.getValue(addr)
	if len(oldVal) > 0 && len(oldVal) == len(value) {
		copy(oldVal, value)
		return true
	}
	t.size -= len(oldVal)
	return false
}

func (t *ART) canSwapValue(cp *MemDBCheckpoint, addr memdbArenaAddr) bool {
	if cp == nil {
		return true
	}
	if int(addr.idx) >= cp.blocks {
		return true
	}
	if int(addr.idx) == cp.blocks-1 && int(addr.off) > cp.offsetInBlock {
		return true
	}
	return false
}

func (t *ART) getValue(l *artLeaf) []byte {
	if l.vAddr.isNull() {
		return nil
	}
	return t.allocator.vlogAllocator.getValue(l.vAddr)
}

func (t *ART) Dirty() bool {
	return t.dirty
}

// Mem returns the memory usage of MemBuffer.
func (t *ART) Mem() uint64 {
	return t.allocator.vlogAllocator.capacity + t.allocator.nodeAllocator.capacity
}

// Len returns the count of entries in the MemBuffer.
func (t *ART) Len() int {
	return t.len
}

// Size returns the size of the MemBuffer.
func (t *ART) Size() int {
	return t.size
}

func (t *ART) checkpoint() MemDBCheckpoint {
	snap := MemDBCheckpoint{
		blockSize: t.allocator.vlogAllocator.blockSize,
		blocks:    len(t.allocator.vlogAllocator.blocks),
	}
	if snap.blocks > 0 {
		snap.offsetInBlock = t.allocator.vlogAllocator.blocks[snap.blocks-1].length
	}
	return snap
}

func (t *ART) revertNode(hdr *memdbVlogHdr) {
	lf := t.allocator.getLeaf(hdr.nodeAddr)
	lf.vAddr = hdr.oldValue
	t.size -= int(hdr.valueLen)
	if hdr.oldValue.isNull() {
		keptFlags := lf.getKeyFlags()
		keptFlags = keptFlags.AndPersistent()
		if keptFlags == 0 {
			lf.markDelete()
			t.len--
		} else {
			lf.setKeyFlags(keptFlags)
		}
	} else {
		t.size += len(t.allocator.vlogAllocator.getValue(hdr.oldValue))
	}
}

func (t *ART) inspectNode(addr memdbArenaAddr) (*artLeaf, memdbArenaAddr) {
	lf := t.allocator.getLeaf(addr)
	return lf, lf.vAddr
}

// Checkpoint returns a checkpoint of ART.
func (t *ART) Checkpoint() *MemDBCheckpoint {
	cp := t.allocator.vlogAllocator.checkpoint()
	return &cp
}

// RevertToCheckpoint reverts the ART to the checkpoint.
func (t *ART) RevertToCheckpoint(cp *MemDBCheckpoint) {
	t.revertToCheckpoint(cp)
	t.truncate(cp)
	t.allocator.vlogAllocator.onMemChange()
}

func (t *ART) Stages() []MemDBCheckpoint {
	return t.stages
}

func (t *ART) Staging() int {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	t.stages = append(t.stages, t.checkpoint())
	return len(t.stages)
}

func (t *ART) Release(h int) {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	if h != len(t.stages) {
		panic("cannot release staging buffer")
	}
	if h == 1 {
		tail := t.checkpoint()
		if !t.stages[0].isSamePosition(&tail) {
			t.dirty = true
		}
	}
	t.stages = t.stages[:h-1]
}

func (t *ART) Cleanup(h int) {
	if !t.skipMutex {
		t.Lock()
		defer t.Unlock()
	}
	if h > len(t.stages) {
		return
	}
	if h < len(t.stages) {
		panic(fmt.Sprintf("cannot cleanup staging buffer, h=%v, len(db.stages)=%v", h, len(t.stages)))
	}

	cp := &t.stages[h-1]
	if !t.vlogInvalid {
		curr := t.checkpoint()
		if !curr.isSamePosition(cp) {
			t.revertToCheckpoint(cp)
			t.truncate(cp)
		}
	}
	t.stages = t.stages[:h-1]
	t.allocator.vlogAllocator.onMemChange()
}

func (t *ART) revertToCheckpoint(cp *MemDBCheckpoint) {
	cursor := t.checkpoint()
	for !cp.isSamePosition(&cursor) {
		hdrOff := cursor.offsetInBlock - memdbVlogHdrSize
		block := t.allocator.vlogAllocator.blocks[cursor.blocks-1].buf
		var hdr memdbVlogHdr
		hdr.load(block[hdrOff:])
		lf := t.allocator.getLeaf(hdr.nodeAddr)
		lf.vAddr = hdr.oldValue
		t.size -= int(hdr.valueLen)
		if hdr.oldValue.isNull() {
			keptFlags := lf.getKeyFlags()
			keptFlags = keptFlags.AndPersistent()
			if keptFlags == 0 {
				lf.markDelete()
				t.len--
			} else {
				lf.setKeyFlags(keptFlags)
			}
		} else {
			t.size += len(t.allocator.vlogAllocator.getValue(hdr.oldValue))
		}
		t.moveBackCursor(&cursor, &hdr)
	}
}

func (t *ART) moveBackCursor(cursor *MemDBCheckpoint, hdr *memdbVlogHdr) {
	cursor.offsetInBlock -= (memdbVlogHdrSize + int(hdr.valueLen))
	if cursor.offsetInBlock == 0 {
		cursor.blocks--
		if cursor.blocks > 0 {
			cursor.offsetInBlock = t.allocator.vlogAllocator.blocks[cursor.blocks-1].length
		}
	}
}

func (t *ART) truncate(snap *MemDBCheckpoint) {
	vlogAllocator := &t.allocator.vlogAllocator
	for i := snap.blocks; i < len(vlogAllocator.blocks); i++ {
		vlogAllocator.blocks[i] = memdbArenaBlock{}
	}
	vlogAllocator.blocks = vlogAllocator.blocks[:snap.blocks]
	if len(vlogAllocator.blocks) > 0 {
		vlogAllocator.blocks[len(vlogAllocator.blocks)-1].length = snap.offsetInBlock
	}
	vlogAllocator.blockSize = snap.blockSize
	// recalculate the capacity
	vlogAllocator.capacity = 0
	for _, block := range vlogAllocator.blocks {
		vlogAllocator.capacity += uint64(block.length)
	}
	// We shall not call a.onMemChange() here, since it may cause a panic and leave memdb in an inconsistent state
}

// DiscardValues releases the memory used by all values.
// NOTE: any operation need value will panic after this function.
func (t *ART) DiscardValues() {
	t.vlogInvalid = true
	t.allocator.vlogAllocator.reset()
}

// InspectStage used to inspect the value updates in the given stage.
func (t *ART) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	idx := handle - 1
	tail := t.checkpoint()
	head := t.stages[idx]
	t.allocator.vlogAllocator.inspectKVInLog(t, &head, &tail, f)
}

// SelectValueHistory select the latest value which makes `predicate` returns true from the modification history.
func (t *ART) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	_, x := t.search(key)
	if x == nil {
		return nil, tikverr.ErrNotExist
	}
	if x.vAddr.isNull() {
		// A flags only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	result := t.selectValueHistory(x.vAddr, func(addr memdbArenaAddr) bool {
		return predicate(t.allocator.vlogAllocator.getValue(addr))
	})
	if result.isNull() {
		return nil, nil
	}
	return t.allocator.vlogAllocator.getValue(result), nil
}

func (t *ART) selectValueHistory(addr memdbArenaAddr, predicate func(memdbArenaAddr) bool) memdbArenaAddr {
	for !addr.isNull() {
		if predicate(addr) {
			return addr
		}
		var hdr memdbVlogHdr
		hdr.load(t.allocator.vlogAllocator.blocks[addr.idx].buf[addr.off-memdbVlogHdrSize:])
		addr = hdr.oldValue
	}
	return nullAddr
}

func (t *ART) getSnapshotValue(addr memdbArenaAddr, cp *MemDBCheckpoint) ([]byte, bool) {
	result := t.selectValueHistory(addr, func(addr memdbArenaAddr) bool {
		return !t.canSwapValue(cp, addr)
	})
	if result.isNull() {
		return nil, false
	}
	return t.allocator.vlogAllocator.getValue(result), true
}

func (t *ART) SetMemoryFootprintChangeHook(fn func(uint64)) {
	hook := func() {
		fn(t.allocator.nodeAllocator.capacity + t.allocator.vlogAllocator.capacity)
	}
	t.allocator.nodeAllocator.memChangeHook.Store(&hook)
	t.allocator.vlogAllocator.memChangeHook.Store(&hook)
}

// MemHookSet implements the MemBuffer interface.
func (t *ART) MemHookSet() bool {
	return t.allocator.nodeAllocator.memChangeHook.Load() != nil
}

// GetKeyByHandle returns key by handle.
func (t *ART) GetKeyByHandle(handle MemKeyHandle) []byte {
	lf := t.allocator.getLeaf(handle.toAddr())
	return lf.getKey()
}

// GetValueByHandle returns value by handle.
func (t *ART) GetValueByHandle(handle MemKeyHandle) ([]byte, bool) {
	if t.vlogInvalid {
		return nil, false
	}
	lf := t.allocator.getLeaf(handle.toAddr())
	if lf.vAddr.isNull() {
		return nil, false
	}
	return t.allocator.vlogAllocator.getValue(lf.vAddr), true
}

func (t *ART) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	t.entrySizeLimit = entryLimit
	t.bufferSizeLimit = bufferLimit
}

func (t *ART) SetSkipMutex(skip bool) {
	t.skipMutex = skip
}

func (t *ART) RemoveFromBuffer(key []byte) {
	panic("unimplemented")
}
