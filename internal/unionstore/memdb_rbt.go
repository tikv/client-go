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
	"context"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/internal/unionstore/rbt"
	"github.com/tikv/client-go/v2/kv"
)

// rbtDBWithContext wraps RBT to satisfy the MemBuffer interface.
type rbtDBWithContext struct {
	// This RWMutex only used to ensure rbtSnapGetter.Get will not race with
	// concurrent MemBuffer.Set, MemBuffer.SetWithFlags, MemBuffer.Delete and MemBuffer.UpdateFlags.
	sync.RWMutex
	*rbt.RBT

	// when the RBT is wrapper by upper RWMutex, we can skip the internal mutex.
	skipMutex bool
}

func newRbtDBWithContext() *rbtDBWithContext {
	return &rbtDBWithContext{
		skipMutex: false,
		RBT:       rbt.New(),
	}
}

//nolint:unused
func (db *rbtDBWithContext) setSkipMutex(skip bool) {
	db.skipMutex = skip
}

func (db *rbtDBWithContext) set(key, value []byte, ops ...kv.FlagsOp) error {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	return db.RBT.Set(key, value, ops...)
}

// UpdateFlags update the flags associated with key.
func (db *rbtDBWithContext) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	err := db.set(key, nil, ops...)
	_ = err // set without value will never fail
}

// Set sets the value for key k as v into kv store.
// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
func (db *rbtDBWithContext) Set(key []byte, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return db.set(key, value)
}

// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
func (db *rbtDBWithContext) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return db.set(key, value, ops...)
}

// Delete removes the entry for key k from kv store.
func (db *rbtDBWithContext) Delete(key []byte) error {
	return db.set(key, arena.Tombstone)
}

// DeleteWithFlags delete key with the given KeyFlags
func (db *rbtDBWithContext) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return db.set(key, arena.Tombstone, ops...)
}

func (db *rbtDBWithContext) Staging() int {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	return db.RBT.Staging()
}

func (db *rbtDBWithContext) Cleanup(handle int) {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	db.RBT.Cleanup(handle)
}

func (db *rbtDBWithContext) Release(handle int) {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	db.RBT.Release(handle)
}

func (db *rbtDBWithContext) Get(_ context.Context, k []byte, _ ...kv.GetOption) (kv.ValueEntry, error) {
	val, err := db.RBT.Get(k)
	if err != nil {
		return kv.ValueEntry{}, err
	}
	return kv.NewValueEntry(val, 0), nil
}

func (db *rbtDBWithContext) GetLocal(_ context.Context, k []byte) ([]byte, error) {
	return db.RBT.Get(k)
}

func (db *rbtDBWithContext) Flush(bool) (bool, error) { return false, nil }

func (db *rbtDBWithContext) FlushWait() error { return nil }

// GetMemDB implements the MemBuffer interface.
func (db *rbtDBWithContext) GetMemDB() *MemDB {
	return nil
}

// BatchGet returns the values for given keys from the MemBuffer.
func (db *rbtDBWithContext) BatchGet(ctx context.Context, keys [][]byte, _ ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	if db.Len() == 0 {
		return map[string]kv.ValueEntry{}, nil
	}
	m := make(map[string]kv.ValueEntry, len(keys))
	for _, k := range keys {
		v, err := db.Get(ctx, k)
		if err != nil {
			if tikverr.IsErrNotFound(err) {
				continue
			}
			return nil, err
		}
		m[string(k)] = v
	}
	return m, nil
}

// GetMetrics implements the MemBuffer interface.
func (db *rbtDBWithContext) GetMetrics() Metrics { return Metrics{} }

// Iter implements the Retriever interface.
func (db *rbtDBWithContext) Iter(lower, upper []byte) (Iterator, error) {
	return db.RBT.Iter(lower, upper)
}

// IterReverse implements the Retriever interface.
func (db *rbtDBWithContext) IterReverse(upper, lower []byte) (Iterator, error) {
	return db.RBT.IterReverse(upper, lower)
}

func (db *rbtDBWithContext) ForEachInSnapshotRange(lower []byte, upper []byte, f func(k, v []byte) (stop bool, err error), reverse bool) error {
	db.RLock()
	defer db.RUnlock()
	var iter Iterator
	if reverse {
		iter = db.SnapshotIterReverse(upper, lower)
	} else {
		iter = db.SnapshotIter(lower, upper)
	}
	defer iter.Close()
	for iter.Valid() {
		stop, err := f(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		err = iter.Next()
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotIter(lower, upper []byte) Iterator {
	return db.RBT.GetSnapshot().SnapshotIter(lower, upper)
}

// SnapshotIterReverse returns a reversed Iterator for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotIterReverse(upper, lower []byte) Iterator {
	return db.RBT.GetSnapshot().SnapshotIterReverse(upper, lower)
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotGetter() kv.Getter {
	return db.RBT.GetSnapshot()
}

func (db *rbtDBWithContext) BatchedSnapshotIter(lower, upper []byte, reverse bool) Iterator {
	// TODO: implement *batched* iter
	if reverse {
		return db.SnapshotIterReverse(upper, lower)
	} else {
		return db.SnapshotIter(lower, upper)
	}
}

type rbtSnapshot struct {
	*rbt.Snapshot
}

// NewSnapshotIterator wraps `RBT.SnapshotIterReverse` and `RBT.SnapshotIter` and cast the result into an `Iterator`.
func (a *rbtSnapshot) NewSnapshotIterator(start, end []byte, reverse bool) Iterator {
	if reverse {
		return a.SnapshotIterReverse(start, end)
	} else {
		return a.SnapshotIter(start, end)
	}
}

// GetSnapshot returns a snapshot of the RBT.
func (db *rbtDBWithContext) GetSnapshot() MemBufferSnapshot {
	// The RBT doesn't maintain the sequence number, so the seqCheck is a noop function.
	seqCheck := func() error {
		return nil
	}
	return &SnapshotWithMutex[*rbtSnapshot]{
		mu:       &db.RWMutex,
		seqCheck: seqCheck,
		snapshot: &rbtSnapshot{db.RBT.GetSnapshot()},
	}
}
