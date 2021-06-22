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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"

	. "github.com/pingcap/check"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/unionstore"
)

var _ = Suite(&testBatchGetterSuite{})

type testBatchGetterSuite struct{}

func (s *testBatchGetterSuite) TestBufferBatchGetter(c *C) {
	snap := newMockStore()
	ka := []byte("a")
	kb := []byte("b")
	kc := []byte("c")
	kd := []byte("d")
	snap.Set(ka, ka)
	snap.Set(kb, kb)
	snap.Set(kc, kc)
	snap.Set(kd, kd)

	buffer := unionstore.NewUnionStore(snap)
	buffer.GetMemBuffer().Set(ka, []byte("a1"))
	buffer.GetMemBuffer().Delete(kb)

	batchGetter := NewBufferBatchGetter(buffer.GetMemBuffer(), snap)
	result, err := batchGetter.BatchGet(context.Background(), [][]byte{ka, kb, kc, kd})
	c.Assert(err, IsNil)
	c.Assert(len(result), Equals, 3)
	c.Assert(string(result[string(ka)]), Equals, "a1")
	c.Assert(string(result[string(kc)]), Equals, "c")
	c.Assert(string(result[string(kd)]), Equals, "d")
}

type mockBatchGetterStore struct {
	index [][]byte
	value [][]byte
}

func newMockStore() *mockBatchGetterStore {
	return &mockBatchGetterStore{
		index: make([][]byte, 0),
		value: make([][]byte, 0),
	}
}

func (s *mockBatchGetterStore) Len() int {
	return len(s.index)
}

func (s *mockBatchGetterStore) Get(ctx context.Context, k []byte) ([]byte, error) {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			return s.value[i], nil
		}
	}
	return nil, tikverr.ErrNotExist
}

func (s *mockBatchGetterStore) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	m := make(map[string][]byte)
	for _, k := range keys {
		v, err := s.Get(ctx, k)
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

func (s *mockBatchGetterStore) Set(k []byte, v []byte) error {
	for i, key := range s.index {
		if kv.CmpKey(key, k) == 0 {
			s.value[i] = v
			return nil
		}
	}
	s.index = append(s.index, k)
	s.value = append(s.value, v)
	return nil
}

func (s *mockBatchGetterStore) Delete(k []byte) error {
	s.Set(k, []byte{})
	return nil
}

func (s *mockBatchGetterStore) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error) {
	panic("unimplemented")
}

func (s *mockBatchGetterStore) IterReverse(k []byte) (unionstore.Iterator, error) {
	panic("unimplemented")
}
