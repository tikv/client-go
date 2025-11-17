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
	"github.com/tikv/client-go/v2/kv"
)

type Snapshot struct {
	tree *ART
	cp   arena.MemDBCheckpoint
}

func (t *ART) getSnapshotCheckpoint() arena.MemDBCheckpoint {
	if len(t.stages) > 0 {
		return t.stages[0]
	}
	return t.checkpoint()
}

// GetSnapshot returns a Getter for a snapshot of MemBuffer's stage[0]
func (t *ART) GetSnapshot() *Snapshot {
	return &Snapshot{
		tree: t,
		cp:   t.getSnapshotCheckpoint(),
	}
}

func (s *Snapshot) NewSnapshotIterator(start, end []byte, desc bool) *SnapIter {
	var (
		inner *Iterator
		err   error
	)
	if desc {
		inner, err = s.tree.IterReverse(start, end)
	} else {
		inner, err = s.tree.Iter(start, end)
	}
	if err != nil {
		panic(err)
	}
	inner.ignoreSeqNo = true
	it := &SnapIter{
		Iterator: inner,
		cp:       s.cp,
	}
	it.tree.allocator.snapshotInc()
	for !it.setValue() && it.Valid() {
		_ = it.Next()
	}
	return it
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

// Close releases the resources of the iterator and related version.
// Make sure to call `Close` after the iterator is not used.
func (i *SnapIter) Close() {
	i.Iterator.Close()
	i.tree.allocator.snapshotDec()
}

func (i *SnapIter) setValue() bool {
	if !i.Valid() {
		return false
	}
	if v, ok := i.tree.allocator.vlogAllocator.GetSnapshotValue(i.currLeaf.vLogAddr, &i.cp); ok {
		i.value = v
		return true
	}
	return false
}

func (snap *Snapshot) Get(_ context.Context, key []byte, _ ...kv.GetOption) (entry kv.ValueEntry, _ error) {
	addr, lf := snap.tree.traverse(key, false)
	if addr.IsNull() {
		return kv.ValueEntry{}, tikverr.ErrNotExist
	}
	if lf.vLogAddr.IsNull() {
		// A flags only key, act as value not exists
		return kv.ValueEntry{}, tikverr.ErrNotExist
	}
	v, ok := snap.tree.allocator.vlogAllocator.GetSnapshotValue(lf.vLogAddr, &snap.cp)
	if !ok {
		return kv.ValueEntry{}, tikverr.ErrNotExist
	}
	return kv.NewValueEntry(v, 0), nil
}

func (snap *Snapshot) Close() {}
