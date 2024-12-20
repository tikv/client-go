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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/union_store.go
//

// Copyright 2021 PingCAP, Inc.

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
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

// Iterator is the interface for a iterator on KV store.
type Iterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

// Getter is the interface for the Get method.
type Getter interface {
	// Get gets the value for key k from kv store.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
	Get(ctx context.Context, k []byte) ([]byte, error)
}

// uSnapshot defines the interface for the snapshot fetched from KV store.
type uSnapshot interface {
	// Get gets the value for key k from kv store.
	// If corresponding kv pair does not exist, it returns nil and ErrNotExist.
	Get(ctx context.Context, k []byte) ([]byte, error)
	// Iter creates an Iterator positioned on the first entry that k <= entry's key.
	// If such entry is not found, it returns an invalid Iterator with no error.
	// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
	// The Iterator must be Closed after use.
	Iter(k []byte, upperBound []byte) (Iterator, error)

	// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
	// The returned iterator will iterate from greater key to smaller key.
	// If k is nil, the returned iterator will be positioned at the last key.
	// It yields only keys that >= lowerBound. If lowerBound is nil, it means the lowerBound is unbounded.
	IterReverse(k, lowerBound []byte) (Iterator, error)
}

// KVUnionStore is an in-memory Store which contains a buffer for write and a
// snapshot for read.
type KVUnionStore struct {
	memBuffer MemBuffer
	snapshot  uSnapshot
}

// NewUnionStore builds a new unionStore.
func NewUnionStore(memBuffer MemBuffer, snapshot uSnapshot) *KVUnionStore {
	return &KVUnionStore{
		snapshot:  snapshot,
		memBuffer: memBuffer,
	}
}

// GetMemBuffer return the MemBuffer binding to this unionStore.
func (us *KVUnionStore) GetMemBuffer() MemBuffer {
	return us.memBuffer
}

// Get implements the Retriever interface.
func (us *KVUnionStore) Get(ctx context.Context, k []byte) ([]byte, error) {
	v, err := us.memBuffer.Get(ctx, k)
	if tikverr.IsErrNotFound(err) {
		v, err = us.snapshot.Get(ctx, k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

// Iter implements the Retriever interface.
func (us *KVUnionStore) Iter(k, upperBound []byte) (Iterator, error) {
	bufferIt, err := us.memBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse implements the Retriever interface.
func (us *KVUnionStore) IterReverse(k, lowerBound []byte) (Iterator, error) {
	bufferIt, err := us.memBuffer.IterReverse(k, lowerBound)
	if err != nil {
		return nil, err
	}
	retrieverIt, err := us.snapshot.IterReverse(k, lowerBound)
	if err != nil {
		return nil, err
	}
	return NewUnionIter(bufferIt, retrieverIt, true)
}

// HasPresumeKeyNotExists gets the key exist error info for the lazy check.
func (us *KVUnionStore) HasPresumeKeyNotExists(k []byte) bool {
	flags, err := us.memBuffer.GetFlags(k)
	if err != nil {
		return false
	}
	return flags.HasPresumeKeyNotExists()
}

// UnmarkPresumeKeyNotExists deletes the key exist error info for the lazy check.
func (us *KVUnionStore) UnmarkPresumeKeyNotExists(k []byte) {
	us.memBuffer.UpdateFlags(k, kv.DelPresumeKeyNotExists)
}

// SetEntrySizeLimit sets the size limit for each entry and total buffer.
func (us *KVUnionStore) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {
	if entryLimit == 0 {
		entryLimit = unlimitedSize
	}
	if bufferLimit == 0 {
		bufferLimit = unlimitedSize
	}
	us.memBuffer.SetEntrySizeLimit(entryLimit, bufferLimit)
}

// MemBuffer is an interface that stores mutations that written during transaction execution.
// It now unifies MemDB and PipelinedMemDB.
// The implementations should follow the transaction guarantees:
// 1. The transaction should see its own writes.
// 2. The latter writes overwrite the earlier writes.
type MemBuffer interface {
	// RLock locks the MemBuffer for shared reading.
	RLock()
	// RUnlock unlocks the MemBuffer for shared reading.
	RUnlock()
	// Get gets the value for key k from the MemBuffer.
	Get(context.Context, []byte) ([]byte, error)
	// GetLocal gets the value from the buffer in local memory.
	// It makes nonsense for MemDB, but makes a difference for pipelined DML.
	GetLocal(context.Context, []byte) ([]byte, error)
	// BatchGet gets the values for given keys from the MemBuffer and cache the result if there are remote buffer.
	BatchGet(context.Context, [][]byte) (map[string][]byte, error)
	// GetFlags gets the flags for key k from the MemBuffer.
	GetFlags([]byte) (kv.KeyFlags, error)
	// Set sets the value for key k in the MemBuffer.
	Set([]byte, []byte) error
	// SetWithFlags sets the value for key k in the MemBuffer with flags.
	SetWithFlags([]byte, []byte, ...kv.FlagsOp) error
	// UpdateFlags updates the flags for key k in the MemBuffer.
	UpdateFlags([]byte, ...kv.FlagsOp)
	// RemoveFromBuffer removes the key k from the MemBuffer, only used for test.
	RemoveFromBuffer(key []byte)
	// Delete deletes the key k in the MemBuffer.
	Delete([]byte) error
	// DeleteWithFlags deletes the key k in the MemBuffer with flags.
	DeleteWithFlags([]byte, ...kv.FlagsOp) error

	// Iter implements the Retriever interface.
	// Any write operation to the memdb invalidates this iterator immediately after its creation.
	// Attempting to use such an invalidated iterator will result in a panic.
	Iter([]byte, []byte) (Iterator, error)
	// IterReverse implements the Retriever interface.
	// Any write operation to the memdb invalidates this iterator immediately after its creation.
	// Attempting to use such an invalidated iterator will result in a panic.
	IterReverse([]byte, []byte) (Iterator, error)
	// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
	// Deprecated: use GetSnapshot instead.
	SnapshotIter([]byte, []byte) Iterator
	// SnapshotIterReverse returns a reversed Iterator for a snapshot of MemBuffer.
	// Deprecated: use GetSnapshot instead.
	SnapshotIterReverse([]byte, []byte) Iterator

	// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
	SnapshotGetter() Getter
	// InspectStage iterates all buffered keys and values in MemBuffer.
	InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte))
	// SetEntrySizeLimit sets the size limit for each entry and total buffer.
	SetEntrySizeLimit(uint64, uint64)
	// Dirty returns true if the MemBuffer is NOT read only.
	Dirty() bool
	// SetMemoryFootprintChangeHook sets the hook for memory footprint change.
	SetMemoryFootprintChangeHook(hook func(uint64))
	// MemHookSet returns whether the memory footprint change hook is set.
	MemHookSet() bool
	// Mem returns the memory usage of MemBuffer.
	Mem() uint64
	// Len returns the count of entries in the MemBuffer.
	Len() int
	// Size returns the size of the MemBuffer.
	Size() int
	// Staging create a new staging buffer inside the MemBuffer.
	Staging() int
	// Cleanup the resources referenced by the StagingHandle.
	Cleanup(int)
	// Release publish all modifications in the latest staging buffer to upper level.
	Release(int)
	// Checkpoint returns the checkpoint of the MemBuffer.
	Checkpoint() *MemDBCheckpoint
	// RevertToCheckpoint reverts the MemBuffer to the specified checkpoint.
	RevertToCheckpoint(*MemDBCheckpoint)
	// GetMemDB returns the MemDB binding to this MemBuffer.
	// This method can also be used for bypassing the wrapper of MemDB.
	GetMemDB() *MemDB
	// Flush flushes the pipelined memdb when the keys or sizes reach the threshold.
	// If force is true, it will flush the memdb without size limitation.
	// it returns true when the memdb is flushed, and returns error when there are any failures.
	Flush(force bool) (bool, error)
	// FlushWait waits for the flushing task done and return error.
	FlushWait() error
	// GetMetrics returns the metrics related to flushing
	GetMetrics() Metrics
	// GetSnapshot returns a snapshot of the MemBuffer.
	GetSnapshot() MemBufferSnapshot
}

type Metrics struct {
	WaitDuration   time.Duration
	TotalDuration  time.Duration
	MemDBHitCount  uint64
	MemDBMissCount uint64
}

var (
	_ MemBuffer = &PipelinedMemDB{}
	_ MemBuffer = &rbtDBWithContext{}
	_ MemBuffer = &artDBWithContext{}
)

type MemBufferSnapshot interface {
	Getter

	// ForEachInSnapshotRange scans the key-value pairs in the state[0] snapshot if it exists,
	// otherwise it uses the current checkpoint as snapshot.
	//
	// NOTE: returned kv-pairs are only valid during the iteration. If you want to use them after the iteration,
	// you need to make a copy.
	//
	// The method is protected by a RWLock to prevent potential iterator invalidation, i.e.
	// You cannot modify the MemBuffer during the iteration.
	//
	// Use it when you need to scan the whole range, otherwise consider using BatchedSnapshotIter.
	ForEachInSnapshotRange(lower []byte, upper []byte, f func(k, v []byte) (stop bool, err error), reverse bool) error

	// BatchedSnapshotIter returns an iterator of the "snapshot", namely stage[0].
	// It iterates in batches and prevents iterator invalidation.
	//
	// Use it when you need on-demand "next", otherwise consider using ForEachInSnapshotRange.
	// NOTE: you should never use it when there are no stages.
	//
	// The iterator becomes invalid when any operation that may modify the "snapshot",
	// e.g. RevertToCheckpoint or releasing stage[0].
	BatchedSnapshotIter(lower, upper []byte, reverse bool) Iterator

	// Close releases the snapshot.
	Close()
}

type SnapshotWithMutex struct {
	mu       *sync.RWMutex
	seqCheck func() error
	db       MemBuffer
	getter   Getter
}

func (s *SnapshotWithMutex) Get(ctx context.Context, k []byte) ([]byte, error) {
	if err := s.seqCheck(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getter.Get(ctx, k)
}

type snapshotBatchedIter struct {
	seqCheck func() error
	db       MemBuffer
	lower    []byte
	upper    []byte
	reverse  bool
	err      error

	// current batch
	keys      [][]byte
	values    [][]byte
	pos       int
	batchSize int
	nextKey   []byte
}

func (s *SnapshotWithMutex) BatchedSnapshotIter(lower, upper []byte, reverse bool) Iterator {
	iter := &snapshotBatchedIter{
		seqCheck:  s.seqCheck,
		db:        s.db,
		lower:     lower,
		upper:     upper,
		reverse:   reverse,
		batchSize: 32,
	}

	iter.err = iter.fillBatch()
	return iter
}

func (it *snapshotBatchedIter) fillBatch() error {
	// The check of sequence numbers don't have to be protected by the rwlock, as the invariant is that
	// there cannot be concurrent writes to the seqNo variables.
	if err := it.seqCheck(); err != nil {
		return err
	}

	it.db.RLock()
	defer it.db.RUnlock()

	if it.keys == nil || it.values == nil || cap(it.keys) < it.batchSize || cap(it.values) < it.batchSize {
		it.keys = make([][]byte, 0, it.batchSize)
		it.values = make([][]byte, 0, it.batchSize)
	} else {
		it.keys = it.keys[:0]
		it.values = it.values[:0]
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
	// Further optimization: let the underlying memdb support batch iter.
	for i := 0; i < it.batchSize && snapshotIter.Valid(); i++ {
		it.keys = it.keys[:i+1]
		it.values = it.values[:i+1]
		it.keys[i] = snapshotIter.Key()
		it.values[i] = snapshotIter.Value()
		if err := snapshotIter.Next(); err != nil {
			return err
		}
	}

	// update state
	it.pos = 0
	if len(it.keys) > 0 {
		lastKey := it.keys[len(it.keys)-1]
		keyLen := len(lastKey)

		if it.reverse {
			if cap(it.nextKey) >= keyLen {
				it.nextKey = it.nextKey[:keyLen]
			} else {
				it.nextKey = make([]byte, keyLen)
			}
			copy(it.nextKey, lastKey)
		} else {
			if cap(it.nextKey) >= keyLen+1 {
				it.nextKey = it.nextKey[:keyLen+1]
			} else {
				it.nextKey = make([]byte, keyLen+1)
			}
			copy(it.nextKey, lastKey)
			it.nextKey[keyLen] = 0
		}
	} else {
		it.nextKey = nil
	}

	it.batchSize = min(it.batchSize*2, 4096)
	return nil
}

func (it *snapshotBatchedIter) Valid() bool {
	return it.seqCheck() == nil &&
		it.pos < len(it.keys) &&
		it.err == nil
}

func (it *snapshotBatchedIter) Next() error {
	if it.err != nil {
		return it.err
	}
	if err := it.seqCheck(); err != nil {
		return err
	}

	it.pos++
	if it.pos >= len(it.keys) {
		return it.fillBatch()
	}
	return nil
}

func (it *snapshotBatchedIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.pos]
}

func (it *snapshotBatchedIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.values[it.pos]
}

func (it *snapshotBatchedIter) Close() {
	it.keys = nil
	it.values = nil
	it.nextKey = nil
}

func (s *SnapshotWithMutex) ForEachInSnapshotRange(lower []byte, upper []byte, f func(k, v []byte) (stop bool, err error), reverse bool) error {
	s.db.RLock()
	defer s.db.RUnlock()
	var iter Iterator
	if reverse {
		iter = s.db.SnapshotIterReverse(upper, lower)
	} else {
		iter = s.db.SnapshotIter(lower, upper)
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

func (s *SnapshotWithMutex) Close() {}
