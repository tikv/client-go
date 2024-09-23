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

//nolint:unused
package art

import (
	"fmt"
	"math"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/kv"
)

var testMode = false

// ART is rollbackable Adaptive Radix Tree optimized for TiDB's transaction states buffer use scenario.
// You can think ART is a combination of two separate tree map, one for key => value and another for key => keyFlags.
//
// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
//
// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
// When discarding a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
// If there are persistent flags associated with key, we will keep this key in node without value.
type ART struct {
	allocator       artAllocator
	root            artNode
	stages          []arena.MemDBCheckpoint
	vlogInvalid     bool
	dirty           bool
	entrySizeLimit  uint64
	bufferSizeLimit uint64
	len             int
	size            int
}

func New() *ART {
	var t ART
	t.root = nullArtNode
	t.stages = make([]arena.MemDBCheckpoint, 0, 2)
	t.entrySizeLimit = math.MaxUint64
	t.bufferSizeLimit = math.MaxUint64
	t.allocator.nodeAllocator.freeNode4 = make([]arena.MemdbArenaAddr, 0, 1<<4)
	t.allocator.nodeAllocator.freeNode16 = make([]arena.MemdbArenaAddr, 0, 1<<3)
	t.allocator.nodeAllocator.freeNode48 = make([]arena.MemdbArenaAddr, 0, 1<<2)
	return &t
}

func (t *ART) Get(key []byte) ([]byte, error) {
	// 1. search the leaf node.
	_, leaf := t.search(key)
	if leaf == nil || leaf.vAddr.IsNull() {
		return nil, tikverr.ErrNotExist
	}
	// 2. get the value from the vlog.
	return t.allocator.vlogAllocator.GetValue(leaf.vAddr), nil
}

// GetFlags returns the latest flags associated with key.
func (t *ART) GetFlags(key []byte) (kv.KeyFlags, error) {
	_, leaf := t.search(key)
	if leaf == nil {
		return 0, tikverr.ErrNotExist
	}
	if leaf.vAddr.IsNull() && leaf.isDeleted() {
		return 0, tikverr.ErrNotExist
	}
	return leaf.GetKeyFlags(), nil
}

func (t *ART) Set(key artKey, value []byte, ops ...kv.FlagsOp) error {
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
	// 1. create or search the existing leaf in the tree.
	addr, leaf := t.recursiveInsert(key)
	// 2. set the value and flags.
	t.setValue(addr, leaf, value, ops)
	if uint64(t.Size()) > t.bufferSizeLimit {
		return &tikverr.ErrTxnTooLarge{Size: t.Size()}
	}
	return nil
}

// search looks up the leaf with the given key.
// It returns the memory arena address and leaf itself it there is a match leaf,
// returns arena.NullAddr and nil if the key is not found.
func (t *ART) search(key artKey) (arena.MemdbArenaAddr, *artLeaf) {
	current := t.root
	if current == nullArtNode {
		return arena.NullAddr, nil
	}
	depth := uint32(0)
	var node *nodeBase
	for {
		if current.isLeaf() {
			lf := current.asLeaf(&t.allocator)
			if lf.match(0, key) {
				return current.addr, lf
			}
			return arena.NullAddr, nil
		}

		// inline: performance critical path
		// get the basic node information.
		switch current.kind {
		case typeNode4:
			node = &current.asNode4(&t.allocator).nodeBase
		case typeNode16:
			node = &current.asNode16(&t.allocator).nodeBase
		case typeNode48:
			node = &current.asNode48(&t.allocator).nodeBase
		case typeNode256:
			node = &current.asNode256(&t.allocator).nodeBase
		default:
			panic("invalid nodeBase kind")
		}

		if node.prefixLen > 0 {
			prefixLen := node.match(key, depth)
			if prefixLen < min(node.prefixLen, maxPrefixLen) {
				return arena.NullAddr, nil
			}
			// If node.prefixLen > maxPrefixLen, we optimistically match the prefix here.
			// False positive is possible, but it's fine since we will check the full artLeaf key at last.
			depth += node.prefixLen
		}

		_, current = current.findChild(&t.allocator, key.charAt(int(depth)), !key.valid(int(depth)))
		if current.addr.IsNull() {
			return arena.NullAddr, nil
		}
		depth++
	}
}

// recursiveInsert returns the node address of the key.
// It will insert the key if not exists, returns the newly inserted or existing leaf.
func (t *ART) recursiveInsert(key artKey) (arena.MemdbArenaAddr, *artLeaf) {
	// lazy init root node and allocator.
	// this saves memory for read only txns.
	if t.root.addr.IsNull() {
		t.root, _ = t.newNode4()
	}

	depth := uint32(0)
	prevDepth := 0
	prev := nullArtNode
	current := t.root
	var node *nodeBase
	for {
		if current.isLeaf() {
			return t.expandLeaf(key, depth, prev, current)
		}

		// inline: performance critical path
		// get the basic node information.
		switch current.kind {
		case typeNode4:
			node = &current.asNode4(&t.allocator).nodeBase
		case typeNode16:
			node = &current.asNode16(&t.allocator).nodeBase
		case typeNode48:
			node = &current.asNode48(&t.allocator).nodeBase
		case typeNode256:
			node = &current.asNode256(&t.allocator).nodeBase
		default:
			panic("invalid nodeBase kind")
		}

		if node.prefixLen > 0 {
			mismatchIdx := node.matchDeep(&t.allocator, &current, key, depth)
			if mismatchIdx < node.prefixLen {
				// if the prefix doesn't match, we split the node into different prefixes.
				return t.expandNode(key, depth, mismatchIdx, prev, current, node)
			}
			depth += node.prefixLen
		}

		// search next node
		valid := key.valid(int(depth))
		_, next := current.findChild(&t.allocator, key.charAt(int(depth)), !valid)
		if next == nullArtNode {
			// insert as leaf if there is no child.
			newAn, newLeaf := t.newLeaf(key)
			if current.addChild(&t.allocator, key.charAt(int(depth)), !valid, newAn) {
				if prev == nullArtNode {
					t.root = current
				} else {
					prev.replaceChild(&t.allocator, key.charAt(prevDepth), current)
				}
			}
			return newAn.addr, newLeaf
		}
		if !valid && next.kind == typeLeaf {
			// key is drained, return the leaf.
			return next.addr, next.asLeaf(&t.allocator)
		}
		prev = current
		current = next
		prevDepth = int(depth)
		depth++
		continue
	}
}

// expandLeaf expands the existing artLeaf to a node4 if the keys are different.
// it returns the addr and leaf of the given key.
func (t *ART) expandLeaf(key artKey, depth uint32, prev, current artNode) (arena.MemdbArenaAddr, *artLeaf) {
	// Expand the artLeaf to a node4.
	//
	//                            ┌────────────┐
	//                            │     new    │
	//                            │    node4   │
	//  ┌─────────┐               └──────┬─────┘
	//  │   old   │   --->               │
	//  │  leaf1  │             ┌────────┴────────┐
	//  └─────────┘             │                 │
	//                     ┌────▼────┐       ┌────▼────┐
	//                     │   old   │       │   new   │
	//                     │  leaf1  │       │  leaf2  │
	//                     └─────────┘       └─────────┘
	leaf1 := current.asLeaf(&t.allocator)
	if leaf1.match(depth-1, key) {
		// same key, return the artLeaf and overwrite the value.
		return current.addr, leaf1
	}
	prevDepth := int(depth - 1)

	leaf2Addr, leaf2 := t.newLeaf(key)
	l1Key, l2Key := artKey(leaf1.GetKey()), artKey(leaf2.GetKey())
	lcp := longestCommonPrefix(l1Key, l2Key, depth)

	// calculate the common prefix length of new node.
	newAn, newN4 := t.newNode4()
	newN4.setPrefix(key[depth:], lcp)
	depth += lcp
	newAn.addChild(&t.allocator, l1Key.charAt(int(depth)), !l1Key.valid(int(depth)), current)
	newAn.addChild(&t.allocator, l2Key.charAt(int(depth)), !l2Key.valid(int(depth)), leaf2Addr)

	// swap the old leaf with the new node4.
	if prev == nullArtNode {
		t.root = newAn
	} else {
		prev.replaceChild(&t.allocator, key.charAt(prevDepth), newAn)
	}
	return leaf2Addr.addr, leaf2
}

func (t *ART) expandNode(key artKey, depth, mismatchIdx uint32, prev, current artNode, currNode *nodeBase) (arena.MemdbArenaAddr, *artLeaf) {
	// prefix mismatch, create a new parent node which has a shorter prefix.
	// example of insert "acc" into node with "abc prefix:
	//                                    ┌────────────┐
	//                                    │ new node4  │
	//                                    │ prefix: a  │
	//                                    └──────┬─────┘
	//  ┌─────────────┐                 ┌── b ───┴── c ───┐
	//  │    node4    │   --->          │                 │
	//  │ prefix: abc │          ┌──────▼─────┐    ┌──────▼─────┐
	//  └─────────────┘          │ old node4  │    │  new leaf  │
	//                           │ prefix: c  │    │  key: acc  │
	//                           └────────────┘    └────────────┘
	prevDepth := int(depth - 1)

	// set prefix for new node.
	newAn, newN4 := t.newNode4()
	newN4.setPrefix(key[depth:], mismatchIdx)

	// update prefix for old node and move it as a child of the new node.
	if currNode.prefixLen <= maxPrefixLen {
		nodeKey := currNode.prefix[mismatchIdx]
		currNode.prefixLen -= mismatchIdx + 1
		copy(currNode.prefix[:], currNode.prefix[mismatchIdx+1:])
		newAn.addChild(&t.allocator, nodeKey, false, current)
	} else {
		currNode.prefixLen -= mismatchIdx + 1
		leafArtNode := minimum(&t.allocator, current)
		leaf := leafArtNode.asLeaf(&t.allocator)
		leafKey := artKey(leaf.GetKey())
		kMin := depth + mismatchIdx + 1
		kMax := depth + mismatchIdx + 1 + min(currNode.prefixLen, maxPrefixLen)
		copy(currNode.prefix[:], leafKey[kMin:kMax])
		newAn.addChild(&t.allocator, leafKey.charAt(int(depth+mismatchIdx)), !leafKey.valid(int(depth)), current)
	}

	// insert the artLeaf into new node
	newLeafAddr, newLeaf := t.newLeaf(key)
	newAn.addChild(&t.allocator, key.charAt(int(depth+mismatchIdx)), !key.valid(int(depth+mismatchIdx)), newLeafAddr)
	if prev == nullArtNode {
		t.root = newAn
	} else {
		prev.replaceChild(&t.allocator, key.charAt(prevDepth), newAn)
	}
	return newLeafAddr.addr, newLeaf
}

func (t *ART) newNode4() (artNode, *node4) {
	addr, n4 := t.allocator.allocNode4()
	return artNode{kind: typeNode4, addr: addr}, n4
}

func (t *ART) newLeaf(key artKey) (artNode, *artLeaf) {
	addr, lf := t.allocator.allocLeaf(key)
	return artNode{kind: typeLeaf, addr: addr}, lf
}

func (t *ART) setValue(addr arena.MemdbArenaAddr, l *artLeaf, value []byte, ops []kv.FlagsOp) {
	flags := l.GetKeyFlags()
	if flags == 0 && l.vAddr.IsNull() || l.isDeleted() {
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
	oldSize, swapper := t.trySwapValue(l.vAddr, value)
	if swapper {
		return
	}
	t.size += len(value) - oldSize
	vAddr := t.allocator.vlogAllocator.AppendValue(addr, l.vAddr, value)
	l.vAddr = vAddr
}

// trySwapValue checks if the value can be updated in place.
// It returns 0 and true if it's updated, returns the size of old value and false if it cannot be updated in place.
func (t *ART) trySwapValue(addr arena.MemdbArenaAddr, value []byte) (int, bool) {
	if addr.IsNull() {
		return 0, false
	}
	oldVal := t.allocator.vlogAllocator.GetValue(addr)
	if len(t.stages) > 0 {
		cp := t.stages[len(t.stages)-1]
		if !t.allocator.vlogAllocator.CanModify(&cp, addr) {
			return len(oldVal), false
		}
	}
	if len(oldVal) > 0 && len(oldVal) == len(value) {
		copy(oldVal, value)
		return 0, true
	}
	return len(oldVal), false
}

func (t *ART) Dirty() bool {
	return t.dirty
}

// Mem returns the memory usage of MemBuffer.
func (t *ART) Mem() uint64 {
	return t.allocator.nodeAllocator.Capacity() + t.allocator.vlogAllocator.Capacity()
}

// Len returns the count of entries in the MemBuffer.
func (t *ART) Len() int {
	return t.len
}

// Size returns the size of the MemBuffer.
func (t *ART) Size() int {
	return t.size
}

func (t *ART) checkpoint() arena.MemDBCheckpoint {
	return t.allocator.vlogAllocator.Checkpoint()
}

func (t *ART) RevertVAddr(hdr *arena.MemdbVlogHdr) {
	lf := t.allocator.getLeaf(hdr.NodeAddr)
	if lf == nil {
		panic("revert an invalid node")
	}
	lf.vAddr = hdr.OldValue
	t.size -= int(hdr.ValueLen)
	if hdr.OldValue.IsNull() {
		keptFlags := lf.GetKeyFlags()
		keptFlags = keptFlags.AndPersistent()
		if keptFlags == 0 {
			lf.markDelete()
			t.len--
			t.size -= int(lf.klen)
		} else {
			lf.setKeyFlags(keptFlags)
		}
	} else {
		t.size += len(t.allocator.vlogAllocator.GetValue(hdr.OldValue))
	}
}

func (t *ART) InspectNode(addr arena.MemdbArenaAddr) (*artLeaf, arena.MemdbArenaAddr) {
	lf := t.allocator.getLeaf(addr)
	return lf, lf.vAddr
}

// Checkpoint returns a checkpoint of ART.
func (t *ART) Checkpoint() *arena.MemDBCheckpoint {
	cp := t.allocator.vlogAllocator.Checkpoint()
	return &cp
}

// RevertToCheckpoint reverts the ART to the checkpoint.
func (t *ART) RevertToCheckpoint(cp *arena.MemDBCheckpoint) {
	t.allocator.vlogAllocator.RevertToCheckpoint(t, cp)
	t.allocator.vlogAllocator.Truncate(cp)
	t.allocator.vlogAllocator.OnMemChange()
}

func (t *ART) Stages() []arena.MemDBCheckpoint {
	return t.stages
}

func (t *ART) Staging() int {
	t.stages = append(t.stages, t.checkpoint())
	return len(t.stages)
}

func (t *ART) Release(h int) {
	if h == 0 {
		// 0 is the invalid and no-effect handle.
		return
	}
	if h != len(t.stages) {
		panic("cannot release staging buffer")
	}
	if h == 1 {
		tail := t.checkpoint()
		if !t.stages[0].IsSamePosition(&tail) {
			t.dirty = true
		}
	}
	t.stages = t.stages[:h-1]
}

func (t *ART) Cleanup(h int) {
	if h == 0 {
		// 0 is the invalid and no-effect handle.
		return
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
		if !curr.IsSamePosition(cp) {
			t.allocator.vlogAllocator.RevertToCheckpoint(t, cp)
			t.allocator.vlogAllocator.Truncate(cp)
		}
	}
	t.stages = t.stages[:h-1]
	t.allocator.vlogAllocator.OnMemChange()
}

// Reset resets the MemBuffer to initial states.
func (t *ART) Reset() {
	t.root = nullArtNode
	t.stages = t.stages[:0]
	t.dirty = false
	t.vlogInvalid = false
	t.size = 0
	t.len = 0
	t.allocator.nodeAllocator.Reset()
	t.allocator.vlogAllocator.Reset()
}

// DiscardValues releases the memory used by all values.
// NOTE: any operation need value will panic after this function.
func (t *ART) DiscardValues() {
	panic("unimplemented")
}

// InspectStage used to inspect the value updates in the given stage.
func (t *ART) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	idx := handle - 1
	tail := t.allocator.vlogAllocator.Checkpoint()
	head := t.stages[idx]
	t.allocator.vlogAllocator.InspectKVInLog(t, &head, &tail, f)
}

// SelectValueHistory select the latest value which makes `predicate` returns true from the modification history.
func (t *ART) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	panic("unimplemented")
}

func (t *ART) SetMemoryFootprintChangeHook(fn func(uint64)) {
	panic("unimplemented")
}

// MemHookSet implements the MemBuffer interface.
func (t *ART) MemHookSet() bool {
	panic("unimplemented")
}

// GetKeyByHandle returns key by handle.
func (t *ART) GetKeyByHandle(handle arena.MemKeyHandle) []byte {
	lf := t.allocator.getLeaf(handle.ToAddr())
	return lf.GetKey()
}

// GetValueByHandle returns value by handle.
func (t *ART) GetValueByHandle(handle arena.MemKeyHandle) ([]byte, bool) {
	if t.vlogInvalid {
		return nil, false
	}
	lf := t.allocator.getLeaf(handle.ToAddr())
	if lf.vAddr.IsNull() {
		return nil, false
	}
	return t.allocator.vlogAllocator.GetValue(lf.vAddr), true
}

func (t *ART) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	t.entrySizeLimit, t.bufferSizeLimit = entryLimit, bufferLimit
}

func (t *ART) RemoveFromBuffer(key []byte) {
	panic("unimplemented")
}
