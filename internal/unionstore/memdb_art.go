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
	"fmt"
	"sync"

	"github.com/pingcap/errors"

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

func (db *artDBWithContext) ForEachInSnapshotRange(lower []byte, upper []byte, f func(k, v []byte) (stop bool, err error), reverse bool) error {
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

type snapshotBatchedIter struct {
	db                    *artDBWithContext
	snapshotTruncateSeqNo int
	lower                 []byte
	upper                 []byte
	reverse               bool

	// current batch
	kvs       []KvPair
	pos       int
	batchSize int
	nextKey   []byte
}

func (db *artDBWithContext) BatchedSnapshotIter(lower, upper []byte, reverse bool) Iterator {
	iter := &snapshotBatchedIter{
		db:                    db,
		snapshotTruncateSeqNo: db.SnapshotSeqNo,
		lower:                 lower,
		upper:                 upper,
		reverse:               reverse,
		batchSize:             4,
	}

	// Position at first key immediately
	iter.fillBatch()
	return iter
}

func (it *snapshotBatchedIter) fillBatch() error {
	if it.snapshotTruncateSeqNo != it.db.SnapshotSeqNo {
		return errors.New(fmt.Sprintf("invalid iter: truncation happened, iter's=%d, db's=%d",
			it.snapshotTruncateSeqNo, it.db.SnapshotSeqNo))
	}

	it.db.RLock()
	defer it.db.RUnlock()

	if it.kvs == nil {
		it.kvs = make([]KvPair, 0, it.batchSize)
	} else {
		it.kvs = it.kvs[:0]
	}

	var snapshotIter Iterator
	if it.reverse {
		searchUpper := it.upper
		if it.nextKey != nil {
			searchUpper = it.nextKey
		}
		snapshotIter = it.db.SnapshotIterReverse(searchUpper, it.lower)
	} else {
		searchLower := it.lower
		if it.nextKey != nil {
			searchLower = it.nextKey
		}
		snapshotIter = it.db.SnapshotIter(searchLower, it.upper)
	}
	defer snapshotIter.Close()

	// fill current batch
	for i := 0; i < it.batchSize && snapshotIter.Valid(); i++ {
		it.kvs = append(it.kvs, KvPair{
			Key:   snapshotIter.Key(),
			Value: snapshotIter.Value(),
		})
		if err := snapshotIter.Next(); err != nil {
			return err
		}
	}

	// update state
	it.pos = 0
	if len(it.kvs) > 0 {
		lastKV := it.kvs[len(it.kvs)-1]
		if it.reverse {
			it.nextKey = append([]byte(nil), lastKV.Key...)
		} else {
			it.nextKey = append(append([]byte(nil), lastKV.Key...), 0)
		}
	} else {
		it.nextKey = nil
	}

	it.batchSize = min(it.batchSize*2, 4096)
	return nil
}

func (it *snapshotBatchedIter) Valid() bool {
	return it.snapshotTruncateSeqNo == it.db.SnapshotSeqNo &&
		it.pos < len(it.kvs)
}

func (it *snapshotBatchedIter) Next() error {
	if it.snapshotTruncateSeqNo != it.db.SnapshotSeqNo {
		return errors.New(
			fmt.Sprintf(
				"invalid snapshotBatchedIter: truncation happened, iter's=%d, db's=%d",
				it.snapshotTruncateSeqNo,
				it.db.SnapshotSeqNo,
			),
		)
	}

	it.pos++
	if it.pos >= len(it.kvs) {
		return it.fillBatch()
	}
	return nil
}

func (it *snapshotBatchedIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.kvs[it.pos].Key
}

func (it *snapshotBatchedIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.kvs[it.pos].Value
}

func (it *snapshotBatchedIter) Close() {
	it.kvs = nil
	it.nextKey = nil
}
