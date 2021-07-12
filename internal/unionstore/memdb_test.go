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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_test.go
//

// Copyright 2020 PingCAP, Inc.
//
// Copyright 2015 Wenbin Xiao
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

package unionstore

import (
	"encoding/binary"
	"fmt"
	"testing"

	leveldb "github.com/pingcap/goleveldb/leveldb/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/kv"
)

type KeyFlags = kv.KeyFlags

func init() {
	testMode = true
}

// DeleteKey is used in test to verify the `deleteNode` used in `vlog.revertToCheckpoint`.
func (db *MemDB) DeleteKey(key []byte) {
	x := db.traverse(key, false)
	if x.isNull() {
		return
	}
	db.size -= len(db.vlog.getValue(x.vptr))
	db.deleteNode(x)
}

func TestGetSet(t *testing.T) {
	require := require.New(t)

	const cnt = 10000
	p := fillDB(cnt)

	var buf [4]byte
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := p.Get(buf[:])
		require.Nil(err)
		require.Equal(v, buf[:])
	}
}

func TestBigKV(t *testing.T) {
	assert := assert.New(t)
	db := newMemDB()
	db.Set([]byte{1}, make([]byte, 80<<20))
	assert.Equal(db.vlog.blockSize, maxBlockSize)
	assert.Equal(len(db.vlog.blocks), 1)
	h := db.Staging()
	db.Set([]byte{2}, make([]byte, 127<<20))
	db.Release(h)
	assert.Equal(db.vlog.blockSize, maxBlockSize)
	assert.Equal(len(db.vlog.blocks), 2)
	assert.PanicsWithValue("alloc size is larger than max block size", func() { db.Set([]byte{3}, make([]byte, maxBlockSize+1)) })
}

func TestIterator(t *testing.T) {
	assert := assert.New(t)
	const cnt = 10000
	db := fillDB(cnt)

	var buf [4]byte
	var i int

	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		assert.Equal(it.Key(), buf[:])
		assert.Equal(it.Value(), buf[:])
		i++
	}
	assert.Equal(i, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		assert.Equal(it.Key(), buf[:])
		assert.Equal(it.Value(), buf[:])
		i--
	}
	assert.Equal(i, -1)
}

func TestDiscard(t *testing.T) {
	assert := assert.New(t)

	const cnt = 10000
	db := newMemDB()
	base := deriveAndFill(0, cnt, 0, db)
	sz := db.Size()

	db.Cleanup(deriveAndFill(0, cnt, 1, db))
	assert.Equal(db.Len(), cnt)
	assert.Equal(db.Size(), sz)

	var buf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		v, err := db.Get(buf[:])
		assert.Nil(err)
		assert.Equal(v, buf[:])
	}

	var i int
	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		assert.Equal(it.Key(), buf[:])
		assert.Equal(it.Value(), buf[:])
		i++
	}
	assert.Equal(i, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		assert.Equal(it.Key(), buf[:])
		assert.Equal(it.Value(), buf[:])
		i--
	}
	assert.Equal(i, -1)

	db.Cleanup(base)
	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		_, err := db.Get(buf[:])
		assert.NotNil(err)
	}
	it1, _ := db.Iter(nil, nil)
	it := it1.(*MemdbIterator)
	it.seekToFirst()
	assert.False(it.Valid())
	it.seekToLast()
	assert.False(it.Valid())
	it.seek([]byte{0xff})
	assert.False(it.Valid())
}

func TestFlushOverwrite(t *testing.T) {
	assert := assert.New(t)

	const cnt = 10000
	db := newMemDB()
	db.Release(deriveAndFill(0, cnt, 0, db))
	sz := db.Size()

	db.Release(deriveAndFill(0, cnt, 1, db))

	assert.Equal(db.Len(), cnt)
	assert.Equal(db.Size(), sz)

	var kbuf, vbuf [4]byte

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		v, err := db.Get(kbuf[:])
		assert.Nil(err)
		assert.Equal(v, vbuf[:])
	}

	var i int
	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		assert.Equal(it.Key(), kbuf[:])
		assert.Equal(it.Value(), vbuf[:])
		i++
	}
	assert.Equal(i, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		assert.Equal(it.Key(), kbuf[:])
		assert.Equal(it.Value(), vbuf[:])
		i--
	}
	assert.Equal(i, -1)
}

func TestComplexUpdate(t *testing.T) {
	assert := assert.New(t)

	const (
		keep      = 3000
		overwrite = 6000
		insert    = 9000
	)

	db := newMemDB()
	db.Release(deriveAndFill(0, overwrite, 0, db))
	assert.Equal(db.Len(), overwrite)
	db.Release(deriveAndFill(keep, insert, 1, db))
	assert.Equal(db.Len(), insert)

	var kbuf, vbuf [4]byte

	for i := 0; i < insert; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i >= keep {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v, err := db.Get(kbuf[:])
		assert.Nil(err)
		assert.Equal(v, vbuf[:])
	}
}

func TestNestedSandbox(t *testing.T) {
	assert := assert.New(t)
	db := newMemDB()
	h0 := deriveAndFill(0, 200, 0, db)
	h1 := deriveAndFill(0, 100, 1, db)
	h2 := deriveAndFill(50, 150, 2, db)
	h3 := deriveAndFill(100, 120, 3, db)
	h4 := deriveAndFill(0, 150, 4, db)
	db.Cleanup(h4) // Discard (0..150 -> 4)
	db.Release(h3) // Flush (100..120 -> 3)
	db.Cleanup(h2) // Discard (100..120 -> 3) & (50..150 -> 2)
	db.Release(h1) // Flush (0..100 -> 1)
	db.Release(h0) // Flush (0..100 -> 1) & (0..200 -> 0)
	// The final result should be (0..100 -> 1) & (101..200 -> 0)

	var kbuf, vbuf [4]byte

	for i := 0; i < 200; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		v, err := db.Get(kbuf[:])
		assert.Nil(err)
		assert.Equal(v, vbuf[:])
	}

	var i int

	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		assert.Equal(it.Key(), kbuf[:])
		assert.Equal(it.Value(), vbuf[:])
		i++
	}
	assert.Equal(i, 200)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i))
		if i < 100 {
			binary.BigEndian.PutUint32(vbuf[:], uint32(i+1))
		}
		assert.Equal(it.Key(), kbuf[:])
		assert.Equal(it.Value(), vbuf[:])
		i--
	}
	assert.Equal(i, -1)
}

func TestOverwrite(t *testing.T) {
	assert := assert.New(t)

	const cnt = 10000
	db := fillDB(cnt)
	var buf [4]byte

	sz := db.Size()
	for i := 0; i < cnt; i += 3 {
		var newBuf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		binary.BigEndian.PutUint32(newBuf[:], uint32(i*10))
		db.Set(buf[:], newBuf[:])
	}
	assert.Equal(db.Len(), cnt)
	assert.Equal(db.Size(), sz)

	for i := 0; i < cnt; i++ {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		val, _ := db.Get(buf[:])
		v := binary.BigEndian.Uint32(val)
		if i%3 == 0 {
			assert.Equal(v, uint32(i*10))
		} else {
			assert.Equal(v, uint32(i))
		}
	}

	var i int

	for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		assert.Equal(it.Key(), buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			assert.Equal(v, uint32(i*10))
		} else {
			assert.Equal(v, uint32(i))
		}
		i++
	}
	assert.Equal(i, cnt)

	i--
	for it, _ := db.IterReverse(nil); it.Valid(); it.Next() {
		binary.BigEndian.PutUint32(buf[:], uint32(i))
		assert.Equal(it.Key(), buf[:])
		v := binary.BigEndian.Uint32(it.Value())
		if i%3 == 0 {
			assert.Equal(v, uint32(i*10))
		} else {
			assert.Equal(v, uint32(i))
		}
		i--
	}
	assert.Equal(i, -1)
}

func TestKVLargeThanBlock(t *testing.T) {
	assert := assert.New(t)
	db := newMemDB()
	db.Set([]byte{1}, make([]byte, 1))
	db.Set([]byte{2}, make([]byte, 4096))
	assert.Equal(len(db.vlog.blocks), 2)
	db.Set([]byte{3}, make([]byte, 3000))
	assert.Equal(len(db.vlog.blocks), 2)
	val, err := db.Get([]byte{3})
	assert.Nil(err)
	assert.Equal(len(val), 3000)
}

func TestEmptyDB(t *testing.T) {
	assert := assert.New(t)
	db := newMemDB()
	_, err := db.Get([]byte{0})
	assert.NotNil(err)
	it1, _ := db.Iter(nil, nil)
	it := it1.(*MemdbIterator)
	it.seekToFirst()
	assert.False(it.Valid())
	it.seekToLast()
	assert.False(it.Valid())
	it.seek([]byte{0xff})
	assert.False(it.Valid())
}

func TestReset(t *testing.T) {
	assert := assert.New(t)
	db := fillDB(1000)
	db.Reset()
	_, err := db.Get([]byte{0, 0, 0, 0})
	assert.NotNil(err)
	it1, _ := db.Iter(nil, nil)
	it := it1.(*MemdbIterator)
	it.seekToFirst()
	assert.False(it.Valid())
	it.seekToLast()
	assert.False(it.Valid())
	it.seek([]byte{0xff})
	assert.False(it.Valid())
}

func TestInspectStage(t *testing.T) {
	assert := assert.New(t)

	db := newMemDB()
	h1 := deriveAndFill(0, 1000, 0, db)
	h2 := deriveAndFill(500, 1000, 1, db)
	for i := 500; i < 1500; i++ {
		var kbuf [4]byte
		// don't update in place
		var vbuf [5]byte
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+2))
		db.Set(kbuf[:], vbuf[:])
	}
	h3 := deriveAndFill(1000, 2000, 3, db)

	db.InspectStage(h3, func(key []byte, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		assert.True(k >= 1000 && k < 2000)
		assert.Equal(v-k, 3)
	})

	db.InspectStage(h2, func(key []byte, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		assert.True(k >= 500 && k < 2000)
		if k < 1000 {
			assert.Equal(v-k, 2)
		} else {
			assert.Equal(v-k, 3)
		}
	})

	db.Cleanup(h3)
	db.Release(h2)

	db.InspectStage(h1, func(key []byte, _ KeyFlags, val []byte) {
		k := int(binary.BigEndian.Uint32(key))
		v := int(binary.BigEndian.Uint32(val))

		assert.True(k >= 0 && k < 1500)
		if k < 500 {
			assert.Equal(v-k, 0)
		} else {
			assert.Equal(v-k, 2)
		}
	})

	db.Release(h1)
}

func TestDirty(t *testing.T) {
	assert := assert.New(t)

	db := newMemDB()
	db.Set([]byte{1}, []byte{1})
	assert.True(db.Dirty())

	db = newMemDB()
	h := db.Staging()
	db.Set([]byte{1}, []byte{1})
	db.Cleanup(h)
	assert.False(db.Dirty())

	h = db.Staging()
	db.Set([]byte{1}, []byte{1})
	db.Release(h)
	assert.True(db.Dirty())

	// persistent flags will make memdb dirty.
	db = newMemDB()
	h = db.Staging()
	db.SetWithFlags([]byte{1}, []byte{1}, kv.SetKeyLocked)
	db.Cleanup(h)
	assert.True(db.Dirty())

	// non-persistent flags will not make memdb dirty.
	db = newMemDB()
	h = db.Staging()
	db.SetWithFlags([]byte{1}, []byte{1}, kv.SetPresumeKeyNotExists)
	db.Cleanup(h)
	assert.False(db.Dirty())
}

func TestFlags(t *testing.T) {
	assert := assert.New(t)

	const cnt = 10000
	db := newMemDB()
	h := db.Staging()
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		if i%2 == 0 {
			db.SetWithFlags(buf[:], buf[:], kv.SetPresumeKeyNotExists, kv.SetKeyLocked)
		} else {
			db.SetWithFlags(buf[:], buf[:], kv.SetPresumeKeyNotExists)
		}
	}
	db.Cleanup(h)

	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		_, err := db.Get(buf[:])
		assert.NotNil(err)
		flags, err := db.GetFlags(buf[:])
		if i%2 == 0 {
			assert.Nil(err)
			assert.True(flags.HasLocked())
			assert.False(flags.HasPresumeKeyNotExists())
		} else {
			assert.NotNil(err)
		}
	}

	assert.Equal(db.Len(), 5000)
	assert.Equal(db.Size(), 20000)

	it1, _ := db.Iter(nil, nil)
	it := it1.(*MemdbIterator)
	assert.False(it.Valid())

	it.includeFlags = true
	it.init()

	for ; it.Valid(); it.Next() {
		k := binary.BigEndian.Uint32(it.Key())
		assert.True(k%2 == 0)
	}

	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		db.UpdateFlags(buf[:], kv.DelKeyLocked)
	}
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		_, err := db.Get(buf[:])
		assert.NotNil(err)

		// UpdateFlags will create missing node.
		flags, err := db.GetFlags(buf[:])
		assert.Nil(err)
		assert.False(flags.HasLocked())
	}
}

func checkConsist(t *testing.T, p1 *MemDB, p2 *leveldb.DB) {
	assert := assert.New(t)

	assert.Equal(p1.Len(), p2.Len())
	assert.Equal(p1.Size(), p2.Size())

	it1, _ := p1.Iter(nil, nil)
	it2 := p2.NewIterator(nil)

	var prevKey, prevVal []byte
	for it2.First(); it2.Valid(); it2.Next() {
		v, err := p1.Get(it2.Key())
		assert.Nil(err)
		assert.Equal(v, it2.Value())

		assert.Equal(it1.Key(), it2.Key())
		assert.Equal(it1.Value(), it2.Value())

		it, _ := p1.Iter(it2.Key(), nil)
		assert.Equal(it.Key(), it2.Key())
		assert.Equal(it.Value(), it2.Value())

		if prevKey != nil {
			it, _ = p1.IterReverse(it2.Key())
			assert.Equal(it.Key(), prevKey)
			assert.Equal(it.Value(), prevVal)
		}

		it1.Next()
		prevKey = it2.Key()
		prevVal = it2.Value()
	}

	it1, _ = p1.IterReverse(nil)
	for it2.Last(); it2.Valid(); it2.Prev() {
		assert.Equal(it1.Key(), it2.Key())
		assert.Equal(it1.Value(), it2.Value())
		it1.Next()
	}
}

func fillDB(cnt int) *MemDB {
	db := newMemDB()
	h := deriveAndFill(0, cnt, 0, db)
	db.Release(h)
	return db
}

func deriveAndFill(start, end, valueBase int, db *MemDB) int {
	h := db.Staging()
	var kbuf, vbuf [4]byte
	for i := start; i < end; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+valueBase))
		db.Set(kbuf[:], vbuf[:])
	}
	return h
}

const (
	startIndex = 0
	testCount  = 2
	indexStep  = 2
)

func insertData(t *testing.T, buffer *MemDB) {
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		err := buffer.Set(val, val)
		assert.Nil(t, err)
	}
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%010d", n))
}

func decodeInt(s []byte) int {
	var n int
	fmt.Sscanf(string(s), "%010d", &n)
	return n
}

func valToStr(iter Iterator) string {
	val := iter.Value()
	return string(val)
}

func checkNewIterator(t *testing.T, buffer *MemDB) {
	assert := assert.New(t)
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Iter(val, nil)
		assert.Nil(err)
		assert.Equal(iter.Key(), val)
		assert.Equal(decodeInt([]byte(valToStr(iter))), i*indexStep)
		iter.Close()
	}

	// Test iterator Next()
	for i := startIndex; i < testCount-1; i++ {
		val := encodeInt(i * indexStep)
		iter, err := buffer.Iter(val, nil)
		assert.Nil(err)
		assert.Equal(iter.Key(), val)
		assert.Equal(valToStr(iter), string(val))

		err = iter.Next()
		assert.Nil(err)
		assert.True(iter.Valid())

		val = encodeInt((i + 1) * indexStep)
		assert.Equal(iter.Key(), val)
		assert.Equal(valToStr(iter), string(val))
		iter.Close()
	}

	// Non exist and beyond maximum seek test
	iter, err := buffer.Iter(encodeInt(testCount*indexStep), nil)
	assert.Nil(err)
	assert.False(iter.Valid())

	// Non exist but between existing keys seek test,
	// it returns the smallest key that larger than the one we are seeking
	inBetween := encodeInt((testCount-1)*indexStep - 1)
	last := encodeInt((testCount - 1) * indexStep)
	iter, err = buffer.Iter(inBetween, nil)
	assert.Nil(err)
	assert.True(iter.Valid())
	assert.NotEqual(iter.Key(), inBetween)
	assert.Equal(iter.Key(), last)
	iter.Close()
}

func mustGet(t *testing.T, buffer *MemDB) {
	for i := startIndex; i < testCount; i++ {
		s := encodeInt(i * indexStep)
		val, err := buffer.Get(s)
		assert.Nil(t, err)
		assert.Equal(t, string(val), string(s))
	}
}

func TestKVGetSet(t *testing.T) {
	buffer := newMemDB()
	insertData(t, buffer)
	mustGet(t, buffer)
}

func TestNewIterator(t *testing.T) {
	assert := assert.New(t)
	buffer := newMemDB()
	// should be invalid
	iter, err := buffer.Iter(nil, nil)
	assert.Nil(err)
	assert.False(iter.Valid())

	insertData(t, buffer)
	checkNewIterator(t, buffer)
}

// FnKeyCmp is the function for iterator the keys
type FnKeyCmp func(key []byte) bool

// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or an error occurs.
func NextUntil(it Iterator, fn FnKeyCmp) error {
	var err error
	for it.Valid() && !fn(it.Key()) {
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func TestIterNextUntil(t *testing.T) {
	assert := assert.New(t)
	buffer := newMemDB()
	insertData(t, buffer)

	iter, err := buffer.Iter(nil, nil)
	assert.Nil(err)

	err = NextUntil(iter, func(k []byte) bool {
		return false
	})
	assert.Nil(err)
	assert.False(iter.Valid())
}

func TestBasicNewIterator(t *testing.T) {
	assert := assert.New(t)
	buffer := newMemDB()
	it, err := buffer.Iter([]byte("2"), nil)
	assert.Nil(err)
	assert.False(it.Valid())
}

func TestNewIteratorMin(t *testing.T) {
	assert := assert.New(t)
	kvs := []struct {
		key   string
		value string
	}{
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "lock-version"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"},
		{"DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"},
	}
	buffer := newMemDB()
	for _, kv := range kvs {
		err := buffer.Set([]byte(kv.key), []byte(kv.value))
		assert.Nil(err)
	}

	cnt := 0
	it, err := buffer.Iter(nil, nil)
	assert.Nil(err)
	for it.Valid() {
		cnt++
		err := it.Next()
		assert.Nil(err)
	}
	assert.Equal(cnt, 6)

	it, err = buffer.Iter([]byte("DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"), nil)
	assert.Nil(err)
	assert.Equal(string(it.Key()), "DATA_test_main_db_tbl_tbl_test_record__00000000000000000001")
}

func TestMemDBStaging(t *testing.T) {
	assert := assert.New(t)
	buffer := newMemDB()
	err := buffer.Set([]byte("x"), make([]byte, 2))
	assert.Nil(err)

	h1 := buffer.Staging()
	err = buffer.Set([]byte("x"), make([]byte, 3))
	assert.Nil(err)

	h2 := buffer.Staging()
	err = buffer.Set([]byte("yz"), make([]byte, 1))
	assert.Nil(err)

	v, _ := buffer.Get([]byte("x"))
	assert.Equal(len(v), 3)

	buffer.Release(h2)

	v, _ = buffer.Get([]byte("yz"))
	assert.Equal(len(v), 1)

	buffer.Cleanup(h1)

	v, _ = buffer.Get([]byte("x"))
	assert.Equal(len(v), 2)
}

func TestBufferLimit(t *testing.T) {
	assert := assert.New(t)
	buffer := newMemDB()
	buffer.bufferSizeLimit = 1000
	buffer.entrySizeLimit = 500

	err := buffer.Set([]byte("x"), make([]byte, 500))
	assert.NotNil(err) // entry size limit

	err = buffer.Set([]byte("x"), make([]byte, 499))
	assert.Nil(err)
	err = buffer.Set([]byte("yz"), make([]byte, 499))
	assert.NotNil(err) // buffer size limit

	err = buffer.Delete(make([]byte, 499))
	assert.Nil(err)

	err = buffer.Delete(make([]byte, 500))
	assert.NotNil(err)
}
