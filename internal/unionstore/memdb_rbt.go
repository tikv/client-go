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

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/rbt"
)

var _ MemBuffer = &rbtDBWithContext{}

// rbtDBWithContext wraps RBT to satisfy the MemBuffer interface.
// It is used for testing.
type rbtDBWithContext struct {
	*rbt.RBT
}

func newRbtDBWithContext() *rbtDBWithContext {
	return &rbtDBWithContext{RBT: rbt.New()}
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
	panic("unimplemented")
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
	return db.RBT.SnapshotIter(upper, lower)
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *rbtDBWithContext) SnapshotGetter() Getter {
	return db.RBT.SnapshotGetter()
}
