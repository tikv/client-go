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

package art

import (
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
)

func (t *Art) getSnapshot() ARTCheckpoint {
	if len(t.stages) > 0 {
		return t.stages[0]
	}
	return t.checkpoint()
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (t *Art) SnapshotGetter() *SnapGetter {
	return &SnapGetter{
		db: t,
		cp: t.getSnapshot(),
	}
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (t *Art) SnapshotIter(start, end []byte) *SnapIter {
	inner, err := t.Iter(start, end)
	if err != nil {
		panic(err)
	}
	it := &SnapIter{
		ArtIterator: inner,
		cp:          t.getSnapshot(),
	}
	for !it.setValue() && it.valid {
		_ = it.Next()
	}
	return it
}

// SnapshotIterReverse returns a reverse Iterator for a snapshot of MemBuffer.
func (t *Art) SnapshotIterReverse(k, lowerBound []byte) *SnapIter {
	inner, err := t.IterReverse(k, lowerBound)
	if err != nil {
		panic(err)
	}
	it := &SnapIter{
		ArtIterator: inner,
		cp:          t.getSnapshot(),
	}
	for !it.setValue() && it.valid {
		_ = it.Next()
	}
	return it
}

type SnapGetter struct {
	db *Art
	cp ARTCheckpoint
}

func (snap *SnapGetter) Get(ctx context.Context, key []byte) ([]byte, error) {
	addr, lf := snap.db.search(key)
	if addr.isNull() {
		return nil, tikverr.ErrNotExist
	}
	if lf.vAddr.isNull() {
		// A flags only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	v, ok := snap.db.getSnapshotValue(lf.vAddr, &snap.cp)
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

type SnapIter struct {
	*ArtIterator
	value []byte
	cp    ARTCheckpoint
}

func (i *SnapIter) Value() []byte {
	return i.value
}

func (i *SnapIter) Next() error {
	i.value = nil
	for i.Valid() {
		if err := i.ArtIterator.Next(); err != nil {
			return err
		}
		if i.setValue() {
			return nil
		}
	}
	return nil
}

func (i *SnapIter) setValue() bool {
	if !i.Valid() {
		return false
	}
	if v, ok := i.tree.getSnapshotValue(i.currLeaf.vAddr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}
