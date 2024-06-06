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
	"math"
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
		entryLimit = math.MaxUint64
	}
	if bufferLimit == 0 {
		bufferLimit = math.MaxUint64
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
	Iter([]byte, []byte) (Iterator, error)
	// IterReverse implements the Retriever interface.
	IterReverse([]byte, []byte) (Iterator, error)
	// SnapshotIter returns an Iterator for a snapshot of MemBuffer.
	SnapshotIter([]byte, []byte) Iterator
	// SnapshotIterReverse returns a reversed Iterator for a snapshot of MemBuffer.
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
	// GetFlushDetails returns the metrics related to flushing
	GetFlushMetrics() FlushMetrics
}

type FlushMetrics struct {
	WaitDuration time.Duration
}

var (
	_ MemBuffer = &MemDBWithContext{}
	_ MemBuffer = &PipelinedMemDB{}
)

// MemDBWithContext wraps MemDB to satisfy the MemBuffer interface.
type MemDBWithContext struct {
	*MemDB
}

func NewMemDBWithContext() *MemDBWithContext {
	return &MemDBWithContext{MemDB: newMemDB()}
}

func (db *MemDBWithContext) Get(_ context.Context, k []byte) ([]byte, error) {
	return db.MemDB.Get(k)
}

func (db *MemDBWithContext) GetLocal(_ context.Context, k []byte) ([]byte, error) {
	return db.MemDB.Get(k)
}

func (db *MemDBWithContext) Flush(bool) (bool, error) { return false, nil }

func (db *MemDBWithContext) FlushWait() error { return nil }

// GetMemDB returns the inner MemDB
func (db *MemDBWithContext) GetMemDB() *MemDB {
	return db.MemDB
}

// BatchGet returns the values for given keys from the MemBuffer.
func (db *MemDBWithContext) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
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

// GetFlushMetrisc implements the MemBuffer interface.
func (db *MemDBWithContext) GetFlushMetrics() FlushMetrics { return FlushMetrics{} }
