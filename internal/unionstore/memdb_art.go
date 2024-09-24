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
package unionstore

import (
	"context"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
	"github.com/tikv/client-go/v2/internal/unionstore/art"
	"github.com/tikv/client-go/v2/kv"
)

// artDBWithContext wraps ART to satisfy the MemBuffer interface.
type artDBWithContext struct {
	// This RWMutex only used to ensure rbtSnapGetter.Get will not race with
	// concurrent MemBuffer.Set, MemBuffer.SetWithFlags, MemBuffer.Delete and MemBuffer.UpdateFlags.
	sync.RWMutex
	*art.ART

	// when the ART is wrapper by upper RWMutex, we can skip the internal mutex.
	skipMutex bool
}

//nolint:unused
func newArtDBWithContext() *artDBWithContext {
	return &artDBWithContext{ART: art.New()}
}

func (db *artDBWithContext) setSkipMutex(skip bool) {
	db.skipMutex = skip
}

func (db *artDBWithContext) set(key, value []byte, ops []kv.FlagsOp) error {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	return db.ART.Set(key, value, ops...)
}

func (db *artDBWithContext) Set(key, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return db.set(key, value, nil)
}

// SetWithFlags put key-value into the last active staging buffer with the given KeyFlags.
func (db *artDBWithContext) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	return db.set(key, value, ops)
}

func (db *artDBWithContext) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	_ = db.set(key, nil, ops)
}

func (db *artDBWithContext) Delete(key []byte) error {
	return db.set(key, arena.Tombstone, nil)
}

func (db *artDBWithContext) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return db.set(key, arena.Tombstone, ops)
}

func (db *artDBWithContext) Staging() int {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	return db.ART.Staging()
}

func (db *artDBWithContext) Cleanup(handle int) {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	db.ART.Cleanup(handle)
}

func (db *artDBWithContext) Release(handle int) {
	if !db.skipMutex {
		db.Lock()
		defer db.Unlock()
	}
	db.ART.Release(handle)
}

func (db *artDBWithContext) Get(_ context.Context, k []byte) ([]byte, error) {
	return db.ART.Get(k)
}

func (db *artDBWithContext) GetLocal(_ context.Context, k []byte) ([]byte, error) {
	return db.ART.Get(k)
}

func (db *artDBWithContext) Flush(bool) (bool, error) { return false, nil }

func (db *artDBWithContext) FlushWait() error { return nil }

// GetMemDB implements the MemBuffer interface.
func (db *artDBWithContext) GetMemDB() *MemDB {
	return db
}

// BatchGet returns the values for given keys from the MemBuffer.
func (db *artDBWithContext) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
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
func (db *artDBWithContext) GetMetrics() Metrics { return Metrics{} }

// Iter implements the Retriever interface.
func (db *artDBWithContext) Iter(lower, upper []byte) (Iterator, error) {
	return db.ART.Iter(lower, upper)
}

// IterReverse implements the Retriever interface.
func (db *artDBWithContext) IterReverse(upper, lower []byte) (Iterator, error) {
	return db.ART.IterReverse(upper, lower)
}

// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
func (db *artDBWithContext) SnapshotIter(lower, upper []byte) Iterator {
	return db.ART.SnapshotIter(lower, upper)
}

// SnapshotIterReverse returns a reversed Iterator for a snapshot of MemBuffer.
func (db *artDBWithContext) SnapshotIterReverse(upper, lower []byte) Iterator {
	return db.ART.SnapshotIterReverse(upper, lower)
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *artDBWithContext) SnapshotGetter() Getter {
	return db.ART.SnapshotGetter()
}
