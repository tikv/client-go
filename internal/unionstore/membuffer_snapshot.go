// Copyright 2025 TiKV Authors
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

	"github.com/tikv/client-go/v2/kv"
)

// SnapshotWithMutex wraps a MemBuffer's snapshot with a mutex to ensure thread-safe access.
// A MemBuffer's snapshot can always read the already-committed data and by pass the staging data through the multi-version system provided by the memdb.
// It uses an RWMutex to prevent concurrent writes to the MemBuffer during read operations, since the underlying memdb (ART or RBT) is not thread-safe.
// And this wrap also avoid the user to call RLock and RUnlock manually.
// A sequence number check is also implemented to ensure the snapshot remains valid during access.
// While the MemBuffer doesn't support interleaving iterators and writes, the SnapshotWithMutex wrapper makes this possible.
type SnapshotWithMutex[S memdbSnapshot] struct {
	mu       *sync.RWMutex
	seqCheck func() error
	snapshot S
}

var _ MemBufferSnapshot = (*SnapshotWithMutex[memdbSnapshot])(nil)

func (s *SnapshotWithMutex[_]) Get(ctx context.Context, k []byte, options ...kv.GetOption) (kv.ValueEntry, error) {
	if err := s.seqCheck(); err != nil {
		return kv.ValueEntry{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.Get(ctx, k, options...)
}

type snapshotBatchedIter[S memdbSnapshot] struct {
	mu       *sync.RWMutex
	seqCheck func() error
	snapshot S
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

func (s *SnapshotWithMutex[S]) BatchedSnapshotIter(lower, upper []byte, reverse bool) Iterator {
	iter := &snapshotBatchedIter[S]{
		mu:        s.mu,
		seqCheck:  s.seqCheck,
		snapshot:  s.snapshot,
		lower:     lower,
		upper:     upper,
		reverse:   reverse,
		batchSize: 32,
	}

	iter.err = iter.fillBatch()
	return iter
}

func (it *snapshotBatchedIter[_]) fillBatch() error {
	// The check of sequence numbers don't have to be protected by the rwlock, as the invariant is that
	// there cannot be concurrent writes to the seqNo variables.
	if err := it.seqCheck(); err != nil {
		return err
	}

	it.mu.RLock()
	defer it.mu.RUnlock()

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
		snapshotIter = it.snapshot.NewSnapshotIterator(searchUpper, it.lower, true)
	} else {
		searchLower := it.lower
		if it.nextKey != nil {
			searchLower = it.nextKey
		}
		snapshotIter = it.snapshot.NewSnapshotIterator(searchLower, it.upper, false)
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

func (it *snapshotBatchedIter[_]) Valid() bool {
	return it.seqCheck() == nil &&
		it.pos < len(it.keys) &&
		it.err == nil
}

func (it *snapshotBatchedIter[_]) Next() error {
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

func (it *snapshotBatchedIter[_]) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.keys[it.pos]
}

func (it *snapshotBatchedIter[_]) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.values[it.pos]
}

func (it *snapshotBatchedIter[_]) Close() {
	it.keys = nil
	it.values = nil
	it.nextKey = nil
}

func (s *SnapshotWithMutex[_]) ForEachInSnapshotRange(lower []byte, upper []byte, f func(k, v []byte) (stop bool, err error), reverse bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var iter Iterator
	if reverse {
		iter = s.snapshot.NewSnapshotIterator(upper, lower, true)
	} else {
		iter = s.snapshot.NewSnapshotIterator(lower, upper, false)
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

func (s *SnapshotWithMutex[_]) Close() {}
