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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_snapshot.go
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
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *RBT) SnapshotGetter() *rbtSnapGetter {
	return &rbtSnapGetter{
		db: db,
		cp: db.getSnapshot(),
	}
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (db *RBT) SnapshotIter(start, end []byte) *rbtSnapIter {
	it := &rbtSnapIter{
		RBTIterator: &RBTIterator{
			db:    db,
			start: start,
			end:   end,
		},
		cp: db.getSnapshot(),
	}
	it.init()
	return it
}

// SnapshotIterReverse returns a reverse Iterator for a snapshot of MemBuffer.
func (db *RBT) SnapshotIterReverse(k, lowerBound []byte) *rbtSnapIter {
	it := &rbtSnapIter{
		RBTIterator: &RBTIterator{
			db:      db,
			start:   lowerBound,
			end:     k,
			reverse: true,
		},
		cp: db.getSnapshot(),
	}
	it.init()
	return it
}

func (db *RBT) getSnapshot() arena.MemDBCheckpoint {
	if len(db.stages) > 0 {
		return db.stages[0]
	}
	return db.vlog.Checkpoint()
}

type rbtSnapGetter struct {
	db *RBT
	cp arena.MemDBCheckpoint
}

func (snap *rbtSnapGetter) Get(ctx context.Context, key []byte) ([]byte, error) {
	x := snap.db.traverse(key, false)
	if x.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if x.vptr.IsNull() {
		// A flag only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	v, ok := snap.db.vlog.GetSnapshotValue(x.vptr, &snap.cp)
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

type rbtSnapIter struct {
	*RBTIterator
	value []byte
	cp    arena.MemDBCheckpoint
}

func (i *rbtSnapIter) Value() []byte {
	return i.value
}

func (i *rbtSnapIter) Next() error {
	i.value = nil
	for i.Valid() {
		if err := i.RBTIterator.Next(); err != nil {
			return err
		}
		if i.setValue() {
			return nil
		}
	}
	return nil
}

func (i *rbtSnapIter) setValue() bool {
	if !i.Valid() {
		return false
	}
	if v, ok := i.db.vlog.GetSnapshotValue(i.curr.vptr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}

func (i *rbtSnapIter) init() {
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

	if !i.setValue() {
		err := i.Next()
		_ = err // memdbIterator will never fail
	}
}
