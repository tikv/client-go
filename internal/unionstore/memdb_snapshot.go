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

package unionstore

import (
	tikverr "github.com/tikv/client-go/v2/error"
)

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *MemDB) SnapshotGetter() Getter {
	return &memdbSnapGetter{
		db: db,
		cp: db.getSnapshot(),
	}
}

// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
func (db *MemDB) SnapshotIter(start, end []byte) Iterator {
	it := &memdbSnapIter{
		MemdbIterator: &MemdbIterator{
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
func (db *MemDB) SnapshotIterReverse(k, lowerBound []byte) Iterator {
	it := &memdbSnapIter{
		MemdbIterator: &MemdbIterator{
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

func (db *MemDB) getSnapshot() MemDBCheckpoint {
	if len(db.stages) > 0 {
		return db.stages[0]
	}
	return db.vlog.checkpoint()
}

type memdbSnapGetter struct {
	db *MemDB
	cp MemDBCheckpoint
}

func (snap *memdbSnapGetter) Get(key []byte) ([]byte, error) {
	x := snap.db.traverse(key, false)
	if x.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if x.vptr.isNull() {
		// A flag only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	v, ok := snap.db.vlog.getSnapshotValue(x.vptr, &snap.cp)
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

type memdbSnapIter struct {
	*MemdbIterator
	value []byte
	cp    MemDBCheckpoint
}

func (i *memdbSnapIter) Value() []byte {
	return i.value
}

func (i *memdbSnapIter) Next() error {
	i.value = nil
	for i.Valid() {
		if err := i.MemdbIterator.Next(); err != nil {
			return err
		}
		if i.setValue() {
			return nil
		}
	}
	return nil
}

func (i *memdbSnapIter) setValue() bool {
	if !i.Valid() {
		return false
	}
	if v, ok := i.db.vlog.getSnapshotValue(i.curr.vptr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}

func (i *memdbSnapIter) init() {
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
