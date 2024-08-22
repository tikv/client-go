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

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/unionstore/art"
)

// artDBWithContext wraps ART to satisfy the MemBuffer interface.
type artDBWithContext struct {
	*art.ART
}

//nolint:unused
func newArtDBWithContext() *artDBWithContext {
	return &artDBWithContext{ART: art.New()}
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
	panic("unimplemented")
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
	return db.ART.SnapshotIter(upper, lower)
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (db *artDBWithContext) SnapshotGetter() Getter {
	return db.ART.SnapshotGetter()
}
