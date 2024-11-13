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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_iterator.go
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

	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/kv"
)

// RBTIterator is an Iterator with KeyFlags related functions.
type RBTIterator struct {
	db           *RBT
	curr         MemdbNodeAddr
	start        []byte
	end          []byte
	reverse      bool
	includeFlags bool
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (db *RBT) Iter(k []byte, upperBound []byte) (*RBTIterator, error) {
	i := &RBTIterator{
		db:    db,
		start: k,
		end:   upperBound,
	}
	i.init()
	return i, nil
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
// It yields only keys that >= lowerBound. If lowerBound is nil, it means the lowerBound is unbounded.
func (db *RBT) IterReverse(k []byte, lowerBound []byte) (*RBTIterator, error) {
	i := &RBTIterator{
		db:      db,
		start:   lowerBound,
		end:     k,
		reverse: true,
	}
	i.init()
	return i, nil
}

// IterWithFlags returns a RBTIterator.
func (db *RBT) IterWithFlags(k []byte, upperBound []byte) *RBTIterator {
	i := &RBTIterator{
		db:           db,
		start:        k,
		end:          upperBound,
		includeFlags: true,
	}
	i.init()
	return i
}

// IterReverseWithFlags returns a reversed RBTIterator.
func (db *RBT) IterReverseWithFlags(k []byte) *RBTIterator {
	i := &RBTIterator{
		db:           db,
		end:          k,
		reverse:      true,
		includeFlags: true,
	}
	i.init()
	return i
}

func (i *RBTIterator) init() {
	if i.reverse {
		if len(i.end) == 0 {
			i.seekToLast()
		} else {
			i.seek(i.end)
		}
	} else {
		if len(i.start) == 0 {
			i.seekToFirst()
		} else {
			i.seek(i.start)
		}
	}

	if (i.isFlagsOnly() && !i.includeFlags) || (!i.curr.isNull() && i.curr.isDeleted()) {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}

// Valid returns true if the current iterator is valid.
func (i *RBTIterator) Valid() bool {
	if !i.reverse {
		return !i.curr.isNull() && (i.end == nil || bytes.Compare(i.Key(), i.end) < 0)
	}
	return !i.curr.isNull() && (i.start == nil || bytes.Compare(i.Key(), i.start) >= 0)
}

// Flags returns flags belong to current iterator.
func (i *RBTIterator) Flags() kv.KeyFlags {
	return i.curr.getKeyFlags()
}

// UpdateFlags updates and apply with flagsOp.
func (i *RBTIterator) UpdateFlags(ops ...kv.FlagsOp) {
	origin := i.curr.getKeyFlags()
	n := kv.ApplyFlagsOps(origin, ops...)
	i.curr.resetKeyFlags(n)
}

// HasValue returns false if it is flags only.
func (i *RBTIterator) HasValue() bool {
	return !i.isFlagsOnly()
}

// Key returns current key.
func (i *RBTIterator) Key() []byte {
	return i.curr.getKey()
}

// Handle returns MemKeyHandle with the current position.
func (i *RBTIterator) Handle() arena.MemKeyHandle {
	return i.curr.addr.ToHandle()
}

// Value returns the value.
func (i *RBTIterator) Value() []byte {
	return i.db.vlog.GetValue(i.curr.vptr)
}

// Next goes the next position.
func (i *RBTIterator) Next() error {
	for {
		if i.reverse {
			i.curr = i.db.predecessor(i.curr)
		} else {
			i.curr = i.db.successor(i.curr)
		}

		if i.curr.isDeleted() {
			continue
		}

		// We need to skip persistent flags only nodes.
		if i.includeFlags || !i.isFlagsOnly() {
			break
		}
	}
	return nil
}

// Close closes the current iterator.
func (i *RBTIterator) Close() {}

func (i *RBTIterator) seekToFirst() {
	y := MemdbNodeAddr{nil, arena.NullAddr}
	x := i.db.getNode(i.db.root)

	for !x.isNull() {
		y = x
		x = y.getLeft(i.db)
	}

	i.curr = y
	for !i.curr.isNull() && i.curr.isDeleted() {
		i.curr = i.db.successor(i.curr)
	}
}

func (i *RBTIterator) seekToLast() {
	y := MemdbNodeAddr{nil, arena.NullAddr}
	x := i.db.getNode(i.db.root)

	for !x.isNull() {
		y = x
		x = y.getRight(i.db)
	}

	i.curr = y
	for !i.curr.isNull() && i.curr.isDeleted() {
		i.curr = i.db.predecessor(i.curr)
	}
}

func (i *RBTIterator) seek(key []byte) {
	y := MemdbNodeAddr{nil, arena.NullAddr}
	x := i.db.getNode(i.db.root)

	var cmp int
	for !x.isNull() {
		y = x
		cmp = bytes.Compare(key, y.getKey())

		if cmp < 0 {
			x = y.getLeft(i.db)
		} else if cmp > 0 {
			x = y.getRight(i.db)
		} else {
			break
		}
	}

	if !i.reverse {
		if cmp > 0 {
			// Move to next
			i.curr = i.db.successor(y)
			return
		}
		i.curr = y
		return
	}

	if cmp <= 0 && !y.isNull() {
		i.curr = i.db.predecessor(y)
		return
	}
	i.curr = y
}

func (i *RBTIterator) isFlagsOnly() bool {
	return !i.curr.isNull() && i.curr.vptr.IsNull()
}
