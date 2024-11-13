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

package rbt

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/kv"
)

func init() {
	testMode = true
}

func deriveAndFill(start, end, valueBase int, db *RBT) int {
	h := db.Staging()
	var kbuf, vbuf [4]byte
	for i := start; i < end; i++ {
		binary.BigEndian.PutUint32(kbuf[:], uint32(i))
		binary.BigEndian.PutUint32(vbuf[:], uint32(i+valueBase))
		db.Set(kbuf[:], vbuf[:])
	}
	return h
}

func TestDiscard(t *testing.T) {
	assert := assert.New(t)

	const cnt = 10000
	db := New()
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
	for it, _ := db.IterReverse(nil, nil); it.Valid(); it.Next() {
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
	it, _ := db.Iter(nil, nil)
	it.seekToFirst()
	assert.False(it.Valid())
	it.seekToLast()
	assert.False(it.Valid())
	it.seek([]byte{0xff})
	assert.False(it.Valid())
}

func TestEmptyDB(t *testing.T) {
	assert := assert.New(t)
	db := New()
	_, err := db.Get([]byte{0})
	assert.NotNil(err)
	it, _ := db.Iter(nil, nil)
	it.seekToFirst()
	assert.False(it.Valid())
	it.seekToLast()
	assert.False(it.Valid())
	it.seek([]byte{0xff})
	assert.False(it.Valid())
}

func TestFlags(t *testing.T) {
	assert := assert.New(t)

	const cnt = 10000
	db := New()
	h := db.Staging()
	for i := uint32(0); i < cnt; i++ {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], i)
		if i%2 == 0 {
			db.Set(buf[:], buf[:], kv.SetPresumeKeyNotExists, kv.SetKeyLocked)
		} else {
			db.Set(buf[:], buf[:], kv.SetPresumeKeyNotExists)
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

	it, _ := db.Iter(nil, nil)
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
		db.Set(buf[:], nil, kv.DelKeyLocked)
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
