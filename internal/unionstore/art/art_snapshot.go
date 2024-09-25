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

package art

import (
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

func (t *ART) getSnapshot() arena.MemDBCheckpoint {
	if len(t.stages) > 0 {
		return t.stages[0]
	}
	return t.checkpoint()
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (t *ART) SnapshotGetter() *SnapGetter {
	return &SnapGetter{
		tree: t,
		cp:   t.getSnapshot(),
	}
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (t *ART) SnapshotIter(start, end []byte) *SnapIter {
	inner, err := t.Iter(start, end)
	if err != nil {
		panic(err)
	}
	it := &SnapIter{
		Iterator: inner,
		cp:       t.getSnapshot(),
	}
	for !it.setValue() && it.Valid() {
		_ = it.Next()
	}
	return it
}

// SnapshotIterReverse returns a reverse Iterator for a snapshot of MemBuffer.
func (t *ART) SnapshotIterReverse(k, lowerBound []byte) *SnapIter {
	inner, err := t.IterReverse(k, lowerBound)
	if err != nil {
		panic(err)
	}
	it := &SnapIter{
		Iterator: inner,
		cp:       t.getSnapshot(),
	}
	for !it.setValue() && it.valid {
		_ = it.Next()
	}
	return it
}

type SnapGetter struct {
	tree *ART
	cp   arena.MemDBCheckpoint
}

func (snap *SnapGetter) Get(ctx context.Context, key []byte) ([]byte, error) {
	addr, lf := snap.tree.search(key)
	if addr.IsNull() {
		return nil, tikverr.ErrNotExist
	}
	if lf.vAddr.IsNull() {
		// A flags only key, act as value not exists
		return nil, tikverr.ErrNotExist
	}
	v, ok := snap.tree.allocator.vlogAllocator.GetSnapshotValue(lf.vAddr, &snap.cp)
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

type SnapIter struct {
	*Iterator
	value []byte
	cp    arena.MemDBCheckpoint
}

func (i *SnapIter) Value() []byte {
	return i.value
}

func (i *SnapIter) Next() error {
	i.value = nil
	for i.Valid() {
		if err := i.Iterator.Next(); err != nil {
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
	if v, ok := i.tree.allocator.vlogAllocator.GetSnapshotValue(i.currLeaf.vAddr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}
