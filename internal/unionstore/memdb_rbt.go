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

func (db *rbtDBWithContext) Get(_ context.Context, k []byte) ([]byte, error) {
	return db.RBT.Get(k)
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
func (db *rbtDBWithContext) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	if db.Len() == 0 {
		return map[string][]byte{}, nil
	}
	m := make(map[string][]byte, len(keys))
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

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotIter(lower, upper []byte) Iterator {
	return db.RBT.SnapshotIter(lower, upper)
}

// SnapshotIterReverse returns a reversed Iterator for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotIterReverse(upper, lower []byte) Iterator {
	return db.RBT.SnapshotIterReverse(upper, lower)
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotGetter() Getter {
	return db.RBT.SnapshotGetter()
}
