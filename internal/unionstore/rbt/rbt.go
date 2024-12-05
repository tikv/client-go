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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb.go
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

package rbt

import (
	"bytes"
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/kv"
)

const unlimitedSize = math.MaxUint64

var testMode = false

// RBT is rollbackable Red-Black Tree optimized for TiDB's transaction states buffer use scenario.
// You can think RBT is a combination of two separate tree map, one for key => value and another for key => keyFlags.
//
// The value map is rollbackable, that means you can use the `Staging`, `Release` and `Cleanup` API to safely modify KVs.
//
// The flags map is not rollbackable. There are two types of flag, persistent and non-persistent.
// When discarding a newly added KV in `Cleanup`, the non-persistent flags will be cleared.
// If there are persistent flags associated with key, we will keep this key in node without value.
type RBT struct {
	root      arena.MemdbArenaAddr
	allocator nodeAllocator
	vlog      arena.MemdbVlog[*memdbNode, *RBT]

	entrySizeLimit  uint64
	bufferSizeLimit uint64
	count           int
	size            int

	vlogInvalid bool
	dirty       bool
	stages      []arena.MemDBCheckpoint

	// The lastTraversedNode stores addr in uint64 of the last traversed node.
	// Compare to atomic.Pointer, atomic.Uint64 can avoid allocation so it's more efficient.
	lastTraversedNode atomic.Uint64
	hitCount          atomic.Uint64
	missCount         atomic.Uint64
}

func New() *RBT {
	db := new(RBT)
	db.allocator.init()
	db.root = arena.NullAddr
	db.stages = make([]arena.MemDBCheckpoint, 0, 2)
	db.entrySizeLimit = unlimitedSize
	db.bufferSizeLimit = unlimitedSize
	db.lastTraversedNode.Store(arena.NullU64Addr)
	return db
}

// updateLastTraversed updates the last traversed node atomically
func (db *RBT) updateLastTraversed(node MemdbNodeAddr) {
	db.lastTraversedNode.Store(node.addr.AsU64())
}

// checkKeyInCache retrieves the last traversed node if the key matches
func (db *RBT) checkKeyInCache(key []byte) (MemdbNodeAddr, bool) {
	addrU64 := db.lastTraversedNode.Load()
	if addrU64 == arena.NullU64Addr {
		return nullNodeAddr, false
	}
	addr := arena.U64ToAddr(addrU64)
	node := db.getNode(addr)

	if bytes.Equal(key, node.memdbNode.getKey()) {
		return node, true
	}

	return nullNodeAddr, false
}

func (db *RBT) RevertVAddr(hdr *arena.MemdbVlogHdr) {
	node := db.getNode(hdr.NodeAddr)
	node.vptr = hdr.OldValue
	db.size -= int(hdr.ValueLen)
	// oldValue.isNull() == true means this is a newly added value.
	if hdr.OldValue.IsNull() {
		// If there are no flags associated with this key, we need to delete this node.
		keptFlags := node.getKeyFlags().AndPersistent()
		if keptFlags == 0 {
			node.markDelete()
			db.count--
			db.size -= int(node.klen)
		} else {
			db.dirty = true
			node.resetKeyFlags(keptFlags)
		}
	} else {
		db.size += len(db.vlog.GetValue(hdr.OldValue))
	}
}

func (db *RBT) InspectNode(addr arena.MemdbArenaAddr) (*memdbNode, arena.MemdbArenaAddr) {
	node := db.allocator.getNode(addr)
	return node, node.vptr
}

// IsStaging returns whether the MemBuffer is in staging status.
func (db *RBT) IsStaging() bool {
	return len(db.stages) > 0
}

// Staging create a new staging buffer inside the MemBuffer.
// Subsequent writes will be temporarily stored in this new staging buffer.
// When you think all modifications looks good, you can call `Release` to public all of them to the upper level buffer.
func (db *RBT) Staging() int {
	db.stages = append(db.stages, db.vlog.Checkpoint())
	return len(db.stages)
}

// Release publish all modifications in the latest staging buffer to upper level.
func (db *RBT) Release(h int) {
	if h == 0 {
		// 0 is the invalid and no-effect handle.
		return
	}
	if h != len(db.stages) {
		// This should never happens in production environment.
		// Use panic to make debug easier.
		panic("cannot release staging buffer")
	}

	if h == 1 {
		tail := db.vlog.Checkpoint()
		if !db.stages[0].IsSamePosition(&tail) {
			db.dirty = true
		}
	}
	db.stages = db.stages[:h-1]
}

// Cleanup cleanup the resources referenced by the StagingHandle.
// If the changes are not published by `Release`, they will be discarded.
func (db *RBT) Cleanup(h int) {
	if h == 0 {
		// 0 is the invalid and no-effect handle.
		return
	}
	if h > len(db.stages) {
		return
	}
	if h < len(db.stages) {
		// This should never happens in production environment.
		// Use panic to make debug easier.
		panic(fmt.Sprintf("cannot cleanup staging buffer, h=%v, len(db.stages)=%v", h, len(db.stages)))
	}

	cp := &db.stages[h-1]
	if !db.vlogInvalid {
		curr := db.vlog.Checkpoint()
		if !curr.IsSamePosition(cp) {
			db.vlog.RevertToCheckpoint(db, cp)
			db.vlog.Truncate(cp)
		}
	}
	db.stages = db.stages[:h-1]
	db.vlog.OnMemChange()
}

// Checkpoint returns a checkpoint of RBT.
func (db *RBT) Checkpoint() *arena.MemDBCheckpoint {
	cp := db.vlog.Checkpoint()
	return &cp
}

// RevertToCheckpoint reverts the RBT to the checkpoint.
func (db *RBT) RevertToCheckpoint(cp *arena.MemDBCheckpoint) {
	db.vlog.RevertToCheckpoint(db, cp)
	db.vlog.Truncate(cp)
	db.vlog.OnMemChange()
}

// Reset resets the MemBuffer to initial states.
func (db *RBT) Reset() {
	db.root = arena.NullAddr
	db.stages = db.stages[:0]
	db.dirty = false
	db.vlogInvalid = false
	db.size = 0
	db.count = 0
	db.vlog.Reset()
	db.allocator.reset()
	db.lastTraversedNode.Store(arena.NullU64Addr)
}

// DiscardValues releases the memory used by all values.
// NOTE: any operation need value will panic after this function.
func (db *RBT) DiscardValues() {
	db.vlogInvalid = true
	db.vlog.Reset()
}

// InspectStage used to inspect the value updates in the given stage.
func (db *RBT) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	idx := handle - 1
	tail := db.vlog.Checkpoint()
	head := db.stages[idx]
	db.vlog.InspectKVInLog(db, &head, &tail, f)
}

// Get gets the value for key k from kv store.
// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
func (db *RBT) Get(key []byte) ([]byte, error) {
	if db.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is resetted")
	}

	x := db.traverse(key, false)
	if x.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if x.vptr.IsNull() {
		// A flag only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	return db.vlog.GetValue(x.vptr), nil
}

// SelectValueHistory select the latest value which makes `predicate` returns true from the modification history.
func (db *RBT) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	x := db.traverse(key, false)
	if x.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if x.vptr.IsNull() {
		// A flag only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	result := db.vlog.SelectValueHistory(x.vptr, func(addr arena.MemdbArenaAddr) bool {
		return predicate(db.vlog.GetValue(addr))
	})
	if result.IsNull() {
		return nil, nil
	}
	return db.vlog.GetValue(result), nil
}

// GetFlags returns the latest flags associated with key.
func (db *RBT) GetFlags(key []byte) (kv.KeyFlags, error) {
	x := db.traverse(key, false)
	if x.isNull() || x.isDeleted() {
		return 0, tikverr.ErrNotExist
	}
	return x.getKeyFlags(), nil
}

// GetKeyByHandle returns key by handle.
func (db *RBT) GetKeyByHandle(handle arena.MemKeyHandle) []byte {
	x := db.getNode(handle.ToAddr())
	return x.getKey()
}

// GetValueByHandle returns value by handle.
func (db *RBT) GetValueByHandle(handle arena.MemKeyHandle) ([]byte, bool) {
	if db.vlogInvalid {
		return nil, false
	}
	x := db.getNode(handle.ToAddr())
	if x.vptr.IsNull() {
		return nil, false
	}
	return db.vlog.GetValue(x.vptr), true
}

// Len returns the number of entries in the DB.
func (db *RBT) Len() int {
	return db.count
}

// Size returns sum of keys and values length.
func (db *RBT) Size() int {
	return db.size
}

// Dirty returns whether the root staging buffer is updated.
func (db *RBT) Dirty() bool {
	return db.dirty
}

func (db *RBT) Set(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if db.vlogInvalid {
		// panic for easier debugging.
		panic("vlog is reset")
	}

	if len(key) > MaxKeyLen {
		return &tikverr.ErrKeyTooLarge{
			KeySize: len(key),
		}
	}

	if value != nil {
		if size := uint64(len(key) + len(value)); size > db.entrySizeLimit {
			return &tikverr.ErrEntryTooLarge{
				Limit: db.entrySizeLimit,
				Size:  size,
			}
		}
	}

	if len(db.stages) == 0 {
		db.dirty = true
	}
	x := db.traverse(key, true)

	// the NeedConstraintCheckInPrewrite flag is temporary,
	// every write to the node removes the flag unless it's explicitly set.
	// This set must be in the latest stage so no special processing is needed.
	flags := x.GetKeyFlags()
	if flags == 0 && x.vptr.IsNull() && x.isDeleted() {
		x.unmarkDelete()
		db.count++
		db.size += int(x.klen)
	}
	if value != nil {
		flags = kv.ApplyFlagsOps(flags, append([]kv.FlagsOp{kv.DelNeedConstraintCheckInPrewrite}, ops...)...)
	} else {
		// an UpdateFlag operation, do not delete the NeedConstraintCheckInPrewrite flag.
		flags = kv.ApplyFlagsOps(flags, ops...)
	}
	if flags.AndPersistent() != 0 {
		db.dirty = true
	}
	x.resetKeyFlags(flags)

	if value == nil {
		return nil
	}

	db.setValue(x, value)
	if uint64(db.Size()) > db.bufferSizeLimit {
		return &tikverr.ErrTxnTooLarge{Size: db.Size()}
	}
	return nil
}

func (db *RBT) setValue(x MemdbNodeAddr, value []byte) {
	var activeCp *arena.MemDBCheckpoint
	if len(db.stages) > 0 {
		activeCp = &db.stages[len(db.stages)-1]
	}

	var oldVal []byte
	if !x.vptr.IsNull() {
		oldVal = db.vlog.GetValue(x.vptr)
	}

	if len(oldVal) > 0 && db.vlog.CanModify(activeCp, x.vptr) {
		// For easier to implement, we only consider this case.
		// It is the most common usage in TiDB's transaction buffers.
		if len(oldVal) == len(value) {
			copy(oldVal, value)
			return
		}
	}
	x.vptr = db.vlog.AppendValue(x.addr, x.vptr, value)
	db.size = db.size - len(oldVal) + len(value)
}

// traverse search for and if not found and insert is true, will add a new node in.
// Returns a pointer to the new node, or the node found.
func (db *RBT) traverse(key []byte, insert bool) MemdbNodeAddr {
	if node, found := db.checkKeyInCache(key); found {
		db.hitCount.Add(1)
		return node
	}
	db.missCount.Add(1)

	x := db.getRoot()
	y := MemdbNodeAddr{nil, arena.NullAddr}
	found := false

	// walk x down the tree
	for !x.isNull() && !found {
		cmp := bytes.Compare(key, x.getKey())
		if cmp < 0 {
			if insert && x.left.IsNull() {
				y = x
			}
			x = x.getLeft(db)
		} else if cmp > 0 {
			if insert && x.right.IsNull() {
				y = x
			}
			x = x.getRight(db)
		} else {
			found = true
		}
	}

	if found {
		db.updateLastTraversed(x)
	}

	if found || !insert {
		return x
	}

	z := db.allocNode(key)
	z.up = y.addr

	if y.isNull() {
		db.root = z.addr
	} else {
		cmp := bytes.Compare(z.getKey(), y.getKey())
		if cmp < 0 {
			y.left = z.addr
		} else {
			y.right = z.addr
		}
	}

	z.left = arena.NullAddr
	z.right = arena.NullAddr

	// colour this new node red
	z.setRed()

	// Having added a red node, we must now walk back up the tree balancing it,
	// by a series of rotations and changing of colours
	x = z

	// While we are not at the top and our parent node is red
	// NOTE: Since the root node is guaranteed black, then we
	// are also going to stop if we are the child of the root

	for x.addr != db.root {
		xUp := x.getUp(db)
		if xUp.isBlack() {
			break
		}

		xUpUp := xUp.getUp(db)
		// if our parent is on the left side of our grandparent
		if x.up == xUpUp.left {
			// get the right side of our grandparent (uncle?)
			y = xUpUp.getRight(db)
			if y.isRed() {
				// make our parent black
				xUp.setBlack()
				// make our uncle black
				y.setBlack()
				// make our grandparent red
				xUpUp.setRed()
				// now consider our grandparent
				x = xUp.getUp(db)
			} else {
				// if we are on the right side of our parent
				if x.addr == xUp.right {
					// Move up to our parent
					x = x.getUp(db)
					db.leftRotate(x)
					xUp = x.getUp(db)
					xUpUp = xUp.getUp(db)
				}

				xUp.setBlack()
				xUpUp.setRed()
				db.rightRotate(xUpUp)
			}
		} else {
			// everything here is the same as above, but exchanging left for right
			y = xUpUp.getLeft(db)
			if y.isRed() {
				xUp.setBlack()
				y.setBlack()
				xUpUp.setRed()

				x = xUp.getUp(db)
			} else {
				if x.addr == xUp.left {
					x = x.getUp(db)
					db.rightRotate(x)
					xUp = x.getUp(db)
					xUpUp = xUp.getUp(db)
				}

				xUp.setBlack()
				xUpUp.setRed()
				db.leftRotate(xUpUp)
			}
		}
	}

	// Set the root node black
	db.getRoot().setBlack()

	db.updateLastTraversed(z)

	return z
}

//
// Rotate our tree thus:-
//
//             X        leftRotate(X)--->           Y
//           /   \                                /   \
//          A     Y     <---rightRotate(Y)       X     C
//              /   \                          /   \
//             B     C                        A     B
//
// NOTE: This does not change the ordering.
//
// We assume that neither X nor Y is NULL
//

func (db *RBT) leftRotate(x MemdbNodeAddr) {
	y := x.getRight(db)

	// Turn Y's left subtree into X's right subtree (move B)
	x.right = y.left

	// If B is not null, set it's parent to be X
	if !y.left.IsNull() {
		left := y.getLeft(db)
		left.up = x.addr
	}

	// Set Y's parent to be what X's parent was
	y.up = x.up

	// if X was the root
	if x.up.IsNull() {
		db.root = y.addr
	} else {
		xUp := x.getUp(db)
		// Set X's parent's left or right pointer to be Y
		if x.addr == xUp.left {
			xUp.left = y.addr
		} else {
			xUp.right = y.addr
		}
	}

	// Put X on Y's left
	y.left = x.addr
	// Set X's parent to be Y
	x.up = y.addr
}

func (db *RBT) rightRotate(y MemdbNodeAddr) {
	x := y.getLeft(db)

	// Turn X's right subtree into Y's left subtree (move B)
	y.left = x.right

	// If B is not null, set it's parent to be Y
	if !x.right.IsNull() {
		right := x.getRight(db)
		right.up = y.addr
	}

	// Set X's parent to be what Y's parent was
	x.up = y.up

	// if Y was the root
	if y.up.IsNull() {
		db.root = x.addr
	} else {
		yUp := y.getUp(db)
		// Set Y's parent's left or right pointer to be X
		if y.addr == yUp.left {
			yUp.left = x.addr
		} else {
			yUp.right = x.addr
		}
	}

	// Put Y on X's right
	x.right = y.addr
	// Set Y's parent to be X
	y.up = x.addr
}

func (db *RBT) deleteNode(z MemdbNodeAddr) {
	var x, y MemdbNodeAddr
	if db.lastTraversedNode.Load() == z.addr.AsU64() {
		db.lastTraversedNode.Store(arena.NullU64Addr)
	}

	db.count--
	db.size -= int(z.klen)

	if z.left.IsNull() || z.right.IsNull() {
		y = z
	} else {
		y = db.successor(z)
	}

	if !y.left.IsNull() {
		x = y.getLeft(db)
	} else {
		x = y.getRight(db)
	}
	x.up = y.up

	if y.up.IsNull() {
		db.root = x.addr
	} else {
		yUp := y.getUp(db)
		if y.addr == yUp.left {
			yUp.left = x.addr
		} else {
			yUp.right = x.addr
		}
	}

	needFix := y.isBlack()

	// NOTE: traditional red-black tree will copy key from Y to Z and free Y.
	// We cannot do the same thing here, due to Y's pointer is stored in vlog and the space in Z may not suitable for Y.
	// So we need to copy states from Z to Y, and relink all nodes formerly connected to Z.
	if y != z {
		db.replaceNode(z, y)
	}

	if needFix {
		db.deleteNodeFix(x)
	}

	db.allocator.freeNode(z.addr)
}

func (db *RBT) replaceNode(old MemdbNodeAddr, new MemdbNodeAddr) {
	if !old.up.IsNull() {
		oldUp := old.getUp(db)
		if old.addr == oldUp.left {
			oldUp.left = new.addr
		} else {
			oldUp.right = new.addr
		}
	} else {
		db.root = new.addr
	}
	new.up = old.up

	left := old.getLeft(db)
	left.up = new.addr
	new.left = old.left

	right := old.getRight(db)
	right.up = new.addr
	new.right = old.right

	if old.isBlack() {
		new.setBlack()
	} else {
		new.setRed()
	}
}

func (db *RBT) deleteNodeFix(x MemdbNodeAddr) {
	for x.addr != db.root && x.isBlack() {
		xUp := x.getUp(db)
		if x.addr == xUp.left {
			w := xUp.getRight(db)
			if w.isRed() {
				w.setBlack()
				xUp.setRed()
				db.leftRotate(xUp)
				w = x.getUp(db).getRight(db)
			}

			if w.getLeft(db).isBlack() && w.getRight(db).isBlack() {
				w.setRed()
				x = x.getUp(db)
			} else {
				if w.getRight(db).isBlack() {
					w.getLeft(db).setBlack()
					w.setRed()
					db.rightRotate(w)
					w = x.getUp(db).getRight(db)
				}

				xUp := x.getUp(db)
				if xUp.isBlack() {
					w.setBlack()
				} else {
					w.setRed()
				}
				xUp.setBlack()
				w.getRight(db).setBlack()
				db.leftRotate(xUp)
				x = db.getRoot()
			}
		} else {
			w := xUp.getLeft(db)
			if w.isRed() {
				w.setBlack()
				xUp.setRed()
				db.rightRotate(xUp)
				w = x.getUp(db).getLeft(db)
			}

			if w.getRight(db).isBlack() && w.getLeft(db).isBlack() {
				w.setRed()
				x = x.getUp(db)
			} else {
				if w.getLeft(db).isBlack() {
					w.getRight(db).setBlack()
					w.setRed()
					db.leftRotate(w)
					w = x.getUp(db).getLeft(db)
				}

				xUp := x.getUp(db)
				if xUp.isBlack() {
					w.setBlack()
				} else {
					w.setRed()
				}
				xUp.setBlack()
				w.getLeft(db).setBlack()
				db.rightRotate(xUp)
				x = db.getRoot()
			}
		}
	}
	x.setBlack()
}

func (db *RBT) successor(x MemdbNodeAddr) (y MemdbNodeAddr) {
	if !x.right.IsNull() {
		// If right is not NULL then go right one and
		// then keep going left until we find a node with
		// no left pointer.

		y = x.getRight(db)
		for !y.left.IsNull() {
			y = y.getLeft(db)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// left of its parent (or the root) and then return the
	// parent.

	y = x.getUp(db)
	for !y.isNull() && x.addr == y.right {
		x = y
		y = y.getUp(db)
	}
	return y
}

func (db *RBT) predecessor(x MemdbNodeAddr) (y MemdbNodeAddr) {
	if !x.left.IsNull() {
		// If left is not NULL then go left one and
		// then keep going right until we find a node with
		// no right pointer.

		y = x.getLeft(db)
		for !y.right.IsNull() {
			y = y.getRight(db)
		}
		return
	}

	// Go up the tree until we get to a node that is on the
	// right of its parent (or the root) and then return the
	// parent.

	y = x.getUp(db)
	for !y.isNull() && x.addr == y.left {
		x = y
		y = y.getUp(db)
	}
	return y
}

func (db *RBT) getNode(x arena.MemdbArenaAddr) MemdbNodeAddr {
	return MemdbNodeAddr{db.allocator.getNode(x), x}
}

func (db *RBT) getRoot() MemdbNodeAddr {
	return db.getNode(db.root)
}

func (db *RBT) allocNode(key []byte) MemdbNodeAddr {
	db.size += len(key)
	db.count++
	x, xn := db.allocator.allocNode(key)
	return MemdbNodeAddr{xn, x}
}

var nullNodeAddr = MemdbNodeAddr{nil, arena.NullAddr}

type MemdbNodeAddr struct {
	*memdbNode
	addr arena.MemdbArenaAddr
}

func (a *MemdbNodeAddr) isNull() bool {
	return a.addr.IsNull()
}

func (a MemdbNodeAddr) getUp(db *RBT) MemdbNodeAddr {
	return db.getNode(a.up)
}

func (a MemdbNodeAddr) getLeft(db *RBT) MemdbNodeAddr {
	return db.getNode(a.left)
}

func (a MemdbNodeAddr) getRight(db *RBT) MemdbNodeAddr {
	return db.getNode(a.right)
}

const MaxKeyLen = math.MaxUint16

type memdbNode struct {
	up    arena.MemdbArenaAddr
	left  arena.MemdbArenaAddr
	right arena.MemdbArenaAddr
	vptr  arena.MemdbArenaAddr
	klen  uint16
	flags uint16
}

func (n *memdbNode) isRed() bool {
	return n.flags&nodeColorBit != 0
}

func (n *memdbNode) isBlack() bool {
	return !n.isRed()
}

func (n *memdbNode) setRed() {
	n.flags |= nodeColorBit
}

func (n *memdbNode) setBlack() {
	n.flags &= ^nodeColorBit
}

func (n *memdbNode) GetKey() []byte {
	return n.getKey()
}

func (n *memdbNode) getKey() []byte {
	base := unsafe.Add(unsafe.Pointer(&n.flags), kv.FlagBytes)
	return unsafe.Slice((*byte)(base), int(n.klen))
}

const (
	// bit 1 => red, bit 0 => black
	nodeColorBit uint16 = 0x8000
	// bit 1 => node is deleted, bit 0 => node is not deleted
	// This flag is used to mark a node as deleted, so that we can reuse the node to avoid memory leak.
	deleteFlag    uint16 = 1 << 14
	nodeFlagsMask        = ^(nodeColorBit | deleteFlag)
)

func (n *memdbNode) GetKeyFlags() kv.KeyFlags {
	return n.getKeyFlags()
}

func (n *memdbNode) getKeyFlags() kv.KeyFlags {
	return kv.KeyFlags(n.flags & nodeFlagsMask)
}

func (n *memdbNode) resetKeyFlags(f kv.KeyFlags) {
	n.flags = (^nodeFlagsMask & n.flags) | uint16(f)
}

func (n *memdbNode) markDelete() {
	n.flags = (nodeColorBit & n.flags) | deleteFlag
}

func (n *memdbNode) unmarkDelete() {
	n.flags &= ^deleteFlag
}

func (n *memdbNode) isDeleted() bool {
	return n.flags&deleteFlag != 0
}

// RemoveFromBuffer removes a record from the mem buffer. It should be only used for test.
func (db *RBT) RemoveFromBuffer(key []byte) {
	x := db.traverse(key, false)
	if x.isNull() {
		return
	}
	db.size -= len(db.vlog.GetValue(x.vptr))
	db.deleteNode(x)
}

// SetMemoryFootprintChangeHook sets the hook function that is triggered when memdb grows.
func (db *RBT) SetMemoryFootprintChangeHook(hook func(uint64)) {
	innerHook := func() {
		hook(db.allocator.Capacity() + db.vlog.Capacity())
	}
	db.allocator.SetMemChangeHook(innerHook)
	db.vlog.SetMemChangeHook(innerHook)
}

// Mem returns the current memory footprint
func (db *RBT) Mem() uint64 {
	return db.allocator.Capacity() + db.vlog.Capacity()
}

// GetEntrySizeLimit gets the size limit for each entry and total buffer.
func (db *RBT) GetEntrySizeLimit() (uint64, uint64) {
	return db.entrySizeLimit, db.bufferSizeLimit
}

// SetEntrySizeLimit sets the size limit for each entry and total buffer.
func (db *RBT) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	db.entrySizeLimit = entryLimit
	db.bufferSizeLimit = bufferLimit
}

// MemHookSet implements the MemBuffer interface.
func (db *RBT) MemHookSet() bool {
	return db.allocator.MemHookSet()
}

func (db *RBT) GetCacheHitCount() uint64 {
	return db.hitCount.Load()
}

func (db *RBT) GetCacheMissCount() uint64 {
	return db.missCount.Load()
}
