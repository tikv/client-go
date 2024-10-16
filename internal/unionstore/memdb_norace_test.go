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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_norace_test.go
//

// Copyright 2020 PingCAP, Inc.
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

//go:build !race

package unionstore

import (
	"context"
	rand2 "crypto/rand"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/pingcap/goleveldb/leveldb/comparer"
	leveldb "github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/stretchr/testify/require"
)

// The test takes too long under the race detector.
func TestRandom(t *testing.T) {
	require := require.New(t)

	const cnt = 50000
	keys := make([][]byte, cnt)
	for i := range keys {
		keys[i] = make([]byte, rand.Intn(19)+1)
		rand2.Read(keys[i])
	}

	rbtDB := newRbtDBWithContext()
	artDB := newArtDBWithContext()
	p2 := leveldb.New(comparer.DefaultComparer, 4*1024)
	for _, k := range keys {
		rbtDB.Set(k, k)
		artDB.Set(k, k)
		_ = p2.Put(k, k)
	}

	require.Equal(rbtDB.Len(), p2.Len())
	require.Equal(rbtDB.Size(), p2.Size())

	require.Equal(artDB.Len(), p2.Len())
	require.Equal(artDB.Size(), p2.Size())

	rand.Shuffle(cnt, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		op := rand.Float64()
		if op < 0.35 {
			rbtDB.RemoveFromBuffer(k)
			p2.Delete(k)
		} else {
			newValue := make([]byte, rand.Intn(19)+1)
			rand2.Read(newValue)
			rbtDB.Set(k, newValue)
			_ = p2.Put(k, newValue)
		}
	}
	checkConsist(t, rbtDB, p2)
}

// The test takes too long under the race detector.
func TestRandomDerive(t *testing.T) {
	db := NewMemDB()
	golden := leveldb.New(comparer.DefaultComparer, 4*1024)
	testRandomDeriveRecur(t, db, golden, 0)
}

func testRandomDeriveRecur(t *testing.T, db *MemDB, golden *leveldb.DB, depth int) [][2][]byte {
	var keys [][]byte
	if op := rand.Float64(); op < 0.33 {
		start, end := rand.Intn(512), rand.Intn(512)+512
		cnt := end - start
		keys = make([][]byte, cnt)
		for i := range keys {
			keys[i] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[i], uint64(start+i))
		}
	} else if op < 0.66 {
		keys = make([][]byte, rand.Intn(512)+512)
		for i := range keys {
			keys[i] = make([]byte, rand.Intn(19)+1)
			rand2.Read(keys[i])
		}
	} else {
		keys = make([][]byte, 512)
		for i := range keys {
			keys[i] = make([]byte, 8)
			binary.BigEndian.PutUint64(keys[i], uint64(i))
		}
	}

	vals := make([][]byte, len(keys))
	for i := range vals {
		vals[i] = make([]byte, rand.Intn(255)+1)
		rand2.Read(vals[i])
	}

	h := db.Staging()
	opLog := make([][2][]byte, 0, len(keys))
	for i := range keys {
		db.Set(keys[i], vals[i])
		old, err := golden.Get(keys[i])
		if err != nil {
			opLog = append(opLog, [2][]byte{keys[i], nil})
		} else {
			opLog = append(opLog, [2][]byte{keys[i], old})
		}
		golden.Put(keys[i], vals[i])
	}

	if depth < 100 {
		childOps := testRandomDeriveRecur(t, db, golden, depth+1)
		opLog = append(opLog, childOps...)
	}

	if rand.Float64() < 0.3 && depth > 0 {
		db.Cleanup(h)
		for i := len(opLog) - 1; i >= 0; i-- {
			if opLog[i][1] == nil {
				golden.Delete(opLog[i][0])
			} else {
				golden.Put(opLog[i][0], opLog[i][1])
			}
		}
		opLog = nil
	} else {
		db.Release(h)
	}

	if depth%10 == 0 {
		checkConsist(t, db, golden)
	}

	return opLog
}

func TestRandomAB(t *testing.T) {
	testRandomAB(t, newRbtDBWithContext(), newArtDBWithContext())
}

func testRandomAB(t *testing.T, bufferA, bufferB MemBuffer) {
	require := require.New(t)

	checkIters := func(iter1, iter2 Iterator, k []byte) {
		for iter1.Valid() {
			require.True(iter2.Valid())
			require.Equal(iter1.Key(), iter2.Key())
			require.Equal(iter1.Value(), iter2.Value())
			require.Nil(iter1.Next())
			require.Nil(iter2.Next())
		}
		require.False(iter2.Valid())
	}

	const cnt = 50000
	keys := make([][]byte, cnt)
	for i := 0; i < cnt; i++ {
		h := bufferA.Staging()
		require.Equal(h, bufferB.Staging())

		keys[i] = make([]byte, rand.Intn(19)+1)
		rand2.Read(keys[i])

		bufferA.Set(keys[i], keys[i])
		bufferB.Set(keys[i], keys[i])

		if i%2 == 0 {
			bufferA.Cleanup(h)
			bufferB.Cleanup(h)
		} else {
			bufferA.Release(h)
			bufferB.Release(h)
		}

		if i%5000 == 0 {
			// iter test is slow, run it less frequently
			key := make([]byte, rand.Intn(19)+1)
			rand2.Read(key)

			require.Equal(bufferA.Dirty(), bufferB.Dirty())
			require.Equal(bufferA.Len(), bufferB.Len())
			require.Equal(bufferA.Size(), bufferB.Size(), i)
			v1, err1 := bufferA.Get(context.Background(), key)
			v2, err2 := bufferB.Get(context.Background(), key)
			require.Equal(err1, err2)
			require.Equal(v1, v2)

			iter1, err := bufferA.Iter(key, nil)
			require.Nil(err)
			iter2, err := bufferB.Iter(key, nil)
			require.Nil(err)
			checkIters(iter1, iter2, key)

			iter1, err = bufferA.IterReverse(nil, key)
			require.Nil(err)
			iter2, err = bufferB.IterReverse(nil, key)
			require.Nil(err)
			checkIters(iter1, iter2, key)
		}
	}
}
