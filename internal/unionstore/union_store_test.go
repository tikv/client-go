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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/union_store_test.go
//

// Copyright 2015 PingCAP, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestUnionStoreGetSet(t *testing.T) {
	assert := assert.New(t)
	store := newMemDB()
	us := NewUnionStore(&mockSnapshot{store})

	err := store.Set([]byte("1"), []byte("1"))
	assert.Nil(err)
	v, err := us.Get(context.TODO(), []byte("1"))
	assert.Nil(err)
	assert.Equal(v, []byte("1"))
	err = us.GetMemBuffer().Set([]byte("1"), []byte("2"))
	assert.Nil(err)
	v, err = us.Get(context.TODO(), []byte("1"))
	assert.Nil(err)
	assert.Equal(v, []byte("2"))
	assert.Equal(us.GetMemBuffer().Size(), 2)
	assert.Equal(us.GetMemBuffer().Len(), 1)
}

func TestUnionStoreDelete(t *testing.T) {
	assert := assert.New(t)
	store := newMemDB()
	us := NewUnionStore(&mockSnapshot{store})

	err := store.Set([]byte("1"), []byte("1"))
	assert.Nil(err)
	err = us.GetMemBuffer().Delete([]byte("1"))
	assert.Nil(err)
	_, err = us.Get(context.TODO(), []byte("1"))
	assert.True(tikverr.IsErrNotFound(err))

	err = us.GetMemBuffer().Set([]byte("1"), []byte("2"))
	assert.Nil(err)
	v, err := us.Get(context.TODO(), []byte("1"))
	assert.Nil(err)
	assert.Equal(v, []byte("2"))
}

func TestUnionStoreSeek(t *testing.T) {
	assert := assert.New(t)
	store := newMemDB()
	us := NewUnionStore(&mockSnapshot{store})

	err := store.Set([]byte("1"), []byte("1"))
	assert.Nil(err)
	err = store.Set([]byte("2"), []byte("2"))
	assert.Nil(err)
	err = store.Set([]byte("3"), []byte("3"))
	assert.Nil(err)

	iter, err := us.Iter(nil, nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("1"), []byte("2"), []byte("3")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")})

	iter, err = us.Iter([]byte("2"), nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("3")}, [][]byte{[]byte("2"), []byte("3")})

	err = us.GetMemBuffer().Set([]byte("4"), []byte("4"))
	assert.Nil(err)
	iter, err = us.Iter([]byte("2"), nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("3"), []byte("4")}, [][]byte{[]byte("2"), []byte("3"), []byte("4")})

	err = us.GetMemBuffer().Delete([]byte("3"))
	assert.Nil(err)
	iter, err = us.Iter([]byte("2"), nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("4")}, [][]byte{[]byte("2"), []byte("4")})
}

func TestUnionStoreIterReverse(t *testing.T) {
	assert := assert.New(t)
	store := newMemDB()
	us := NewUnionStore(&mockSnapshot{store})

	err := store.Set([]byte("1"), []byte("1"))
	assert.Nil(err)
	err = store.Set([]byte("2"), []byte("2"))
	assert.Nil(err)
	err = store.Set([]byte("3"), []byte("3"))
	assert.Nil(err)

	iter, err := us.IterReverse(nil, nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("3"), []byte("2"), []byte("1")}, [][]byte{[]byte("3"), []byte("2"), []byte("1")})

	iter, err = us.IterReverse([]byte("3"), []byte("1"))
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("1")}, [][]byte{[]byte("2"), []byte("1")})

	iter, err = us.IterReverse([]byte("3"), nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("1")}, [][]byte{[]byte("2"), []byte("1")})

	err = us.GetMemBuffer().Set([]byte("0"), []byte("0"))
	assert.Nil(err)
	iter, err = us.IterReverse([]byte("3"), nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("1"), []byte("0")}, [][]byte{[]byte("2"), []byte("1"), []byte("0")})

	err = us.GetMemBuffer().Delete([]byte("1"))
	assert.Nil(err)
	iter, err = us.IterReverse([]byte("3"), nil)
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2"), []byte("0")}, [][]byte{[]byte("2"), []byte("0")})

	iter, err = us.IterReverse([]byte("3"), []byte("1"))
	assert.Nil(err)
	checkIterator(t, iter, [][]byte{[]byte("2")}, [][]byte{[]byte("2")})
}

func checkIterator(t *testing.T, iter Iterator, keys [][]byte, values [][]byte) {
	assert := assert.New(t)
	defer iter.Close()
	assert.Equal(len(keys), len(values))
	for i, k := range keys {
		v := values[i]
		assert.True(iter.Valid())
		assert.Equal(iter.Key(), k)
		assert.Equal(iter.Value(), v)
		assert.Nil(iter.Next())
	}
	assert.False(iter.Valid())
}
