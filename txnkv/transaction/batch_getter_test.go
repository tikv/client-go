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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/snapshot_test.go
//

// Copyright 2016 PingCAP, Inc.
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

package transaction

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

func TestBufferBatchGetter(t *testing.T) {
	snap := newMockStore()
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	ke := []byte("e")
	snap.Set(ka, kv.NewValueEntry(ka, 1))
	snap.Set(kb, kv.NewValueEntry(kb, 2))
	snap.Set(kc, kv.NewValueEntry(kc, 3))
	snap.Set(kd, kv.NewValueEntry(kd, 4))

	buffer := newMockStore()
	buffer.Set(ka, kv.NewValueEntry([]byte("a1"), 11))
	buffer.Delete(kb)

	// test batch get and with require commit ts
	batchGetter := NewBufferBatchGetter(buffer, snap)
	result, err := batchGetter.BatchGet(context.Background(), [][]byte{ka, kb, kc, kd, ke}, kv.WithReturnCommitTS())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, kv.NewValueEntry([]byte("a1"), 11), result[string(ka)])
	assert.Equal(t, kv.NewValueEntry([]byte("c"), 3), result[string(kc)])
	assert.Equal(t, kv.NewValueEntry([]byte("d"), 4), result[string(kd)])

	// test batch get without require commit ts
	result, err = batchGetter.BatchGet(context.Background(), [][]byte{ka, kb, kc, kd, ke})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, kv.NewValueEntry([]byte("a1"), 0), result[string(ka)])
	assert.Equal(t, kv.NewValueEntry([]byte("c"), 0), result[string(kc)])
	assert.Equal(t, kv.NewValueEntry([]byte("d"), 0), result[string(kd)])
}

type mockBatchGetterStore struct {
	index [][]byte
	value []kv.ValueEntry
}

func newMockStore() *mockBatchGetterStore {
	return &mockBatchGetterStore{
		index: make([][]byte, 0),
		value: make([]kv.ValueEntry, 0),
	}
}

func (s *mockBatchGetterStore) Len() int {
	return len(s.index)
}

func (s *mockBatchGetterStore) Get(_ context.Context, k []byte, options ...kv.GetOption) (kv.ValueEntry, error) {
	var opt kv.GetOptions
	opt.Apply(options)

	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			entry := s.value[i]
			if !opt.ReturnCommitTS() {
				entry.CommitTS = 0
			}
			return entry, nil
		}
	}
	return kv.ValueEntry{}, tikverr.ErrNotExist
}

func (s *mockBatchGetterStore) BatchGet(ctx context.Context, keys [][]byte, options ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	m := make(map[string]kv.ValueEntry)
	for _, k := range keys {
		v, err := s.Get(ctx, k, kv.BatchGetToGetOptions(options)...)
		if err == nil {
			m[string(k)] = v
			continue
		}
		if tikverr.IsErrNotFound(err) {
			continue
		}
		return m, err
	}
	return m, nil
}

func (s *mockBatchGetterStore) Set(k []byte, entry kv.ValueEntry) error {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			s.value[i] = entry
			return nil
		}
	}
	s.index = append(s.index, k)
	s.value = append(s.value, entry)
	return nil
}

func (s *mockBatchGetterStore) Delete(k []byte) error {
	s.Set(k, kv.ValueEntry{})
	return nil
}
