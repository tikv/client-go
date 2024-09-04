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
	"math"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/kv"
)

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
	// 1. search the asLeaf node.
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
	return leaf.getKeyFlags(), nil
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
	// 1. create or search the exist asLeaf in the tree.
	addr, leaf := t.recursiveInsert(key)
	// 2. set the value and flags.
	t.setValue(addr, leaf, value, ops)
	if uint64(t.Size()) > t.bufferSizeLimit {
		return &tikverr.ErrTxnTooLarge{Size: t.Size()}
	}
	return nil
}

// search looks up the asLeaf with the given key.
// It returns the address of asLeaf and asLeaf itself it there is a match asLeaf,
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
			node = &current.node4(&t.allocator).nodeBase
		case typeNode16:
			node = &current.node16(&t.allocator).nodeBase
		case typeNode48:
			node = &current.node48(&t.allocator).nodeBase
		case typeNode256:
			node = &current.node256(&t.allocator).nodeBase
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

		_, current = current.findChild(&t.allocator, key.charAt(int(depth)), key.valid(int(depth)))
		if current.addr.IsNull() {
			return arena.NullAddr, nil
		}
		depth++
	}
}

// recursiveInsert returns the node address of the key.
// It will insert the key if not exists, returns the newly inserted or exist leaf.
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
			node = &current.node4(&t.allocator).nodeBase
		case typeNode16:
			node = &current.node16(&t.allocator).nodeBase
		case typeNode48:
			node = &current.node48(&t.allocator).nodeBase
		case typeNode256:
			node = &current.node256(&t.allocator).nodeBase
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
		_, next := current.findChild(&t.allocator, key.charAt(int(depth)), valid)
		if next == nullArtNode {
			// insert as asLeaf if there is no child.
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
		if !valid && next.kind == typeLeaf {
			// key is drained, return the asLeaf.
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
// it returns the addr and asLeaf of the given key.
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
	an, n4 := t.newNode4()
	n4.setPrefix(key[depth:], lcp)
	depth += lcp
	an.addChild(&t.allocator, l1Key.charAt(int(depth)), !l1Key.valid(int(depth)), current)
	an.addChild(&t.allocator, l2Key.charAt(int(depth)), !l2Key.valid(int(depth)), leaf2Addr)

	// swap the old asLeaf with the new node4.
	if prev == nullArtNode {
		t.root = an
	} else {
		prev.swapChild(&t.allocator, key.charAt(prevDepth), an)
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
	//  └─────────────┘          │ old node4  │    │ new asLeaf │
	//                           │ prefix: c  │    │  key: acc  │
	//                           └────────────┘    └────────────┘
	prevDepth := int(depth - 1)

	// set prefix for new node.
	newArtNode, newN4 := t.newNode4()
	newN4.setPrefix(key[depth:], mismatchIdx)

	// update prefix for old node and move it as a child of the new node.
	if currNode.prefixLen <= maxPrefixLen {
		nodeKey := currNode.prefix[mismatchIdx]
		currNode.prefixLen -= mismatchIdx + 1
		copy(currNode.prefix[:], currNode.prefix[mismatchIdx+1:])
		newArtNode.addChild(&t.allocator, nodeKey, false, current)
	} else {
		currNode.prefixLen -= mismatchIdx + 1
		leafArtNode := minimum(&t.allocator, current)
		leaf := leafArtNode.asLeaf(&t.allocator)
		leafKey := artKey(leaf.GetKey())
		kMin := depth + mismatchIdx + 1
		kMax := depth + mismatchIdx + 1 + min(currNode.prefixLen, maxPrefixLen)
		copy(currNode.prefix[:], leafKey[kMin:kMax])
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

func (t *ART) newNode4() (artNode, *node4) {
	addr, n4 := t.allocator.allocNode4()
	return artNode{kind: typeNode4, addr: addr}, n4
}

func (t *ART) newLeaf(key artKey) (artNode, *artLeaf) {
	addr, lf := t.allocator.allocLeaf(key)
	return artNode{kind: typeLeaf, addr: addr}, lf
}

func (t *ART) setValue(addr arena.MemdbArenaAddr, l *artLeaf, value []byte, ops []kv.FlagsOp) {
	flags := l.getKeyFlags()
	if flags == 0 && l.vAddr.IsNull() {
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
	panic("unimplemented")
}

// Mem returns the memory usage of MemBuffer.
func (t *ART) Mem() uint64 {
	panic("unimplemented")
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
	panic("unimplemented")
}

func (t *ART) RevertNode(hdr *arena.MemdbVlogHdr) {
	panic("unimplemented")
}

func (t *ART) InspectNode(addr arena.MemdbArenaAddr) (*artLeaf, arena.MemdbArenaAddr) {
	panic("unimplemented")
}

// Checkpoint returns a checkpoint of ART.
func (t *ART) Checkpoint() *arena.MemDBCheckpoint {
	panic("unimplemented")
}

// RevertToCheckpoint reverts the ART to the checkpoint.
func (t *ART) RevertToCheckpoint(cp *arena.MemDBCheckpoint) {
	panic("unimplemented")
}

func (t *ART) Stages() []arena.MemDBCheckpoint {
	panic("unimplemented")
}

func (t *ART) Staging() int {
	return 0
}

func (t *ART) Release(h int) {
}

func (t *ART) Cleanup(h int) {
}

func (t *ART) revertToCheckpoint(cp *arena.MemDBCheckpoint) {
	panic("unimplemented")
}

func (t *ART) moveBackCursor(cursor *arena.MemDBCheckpoint, hdr *arena.MemdbVlogHdr) {
	panic("unimplemented")
}

func (t *ART) truncate(snap *arena.MemDBCheckpoint) {
	panic("unimplemented")
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
	panic("unimplemented")
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
	panic("unimplemented")
}

// GetValueByHandle returns value by handle.
func (t *ART) GetValueByHandle(handle arena.MemKeyHandle) ([]byte, bool) {
	panic("unimplemented")
}

func (t *ART) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	panic("unimplemented")
}

func (t *ART) RemoveFromBuffer(key []byte) {
	panic("unimplemented")
}
