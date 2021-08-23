// Copyright 2021 PingCAP, Inc.
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

package txnsnapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/internal/unionstore"
)

type mockRecord struct {
	key   []byte
	value []byte
}

func r(key, value string) *mockRecord {
	bKey := []byte(key)
	bValue := []byte(value)
	if value == "nil" {
		bValue = nil
	}

	return &mockRecord{bKey, bValue}
}

type mockIter struct {
	data []*mockRecord
	cur  int
}

func newMockIter(records []*mockRecord) *mockIter {
	return &mockIter{
		records,
		0,
	}
}

func (m *mockIter) Valid() bool {
	return m.cur >= 0 && m.cur < len(m.data)
}

func (m *mockIter) Key() []byte {
	return m.data[m.cur].key
}

func (m *mockIter) Value() []byte {
	return m.data[m.cur].value
}

func (m *mockIter) Next() error {
	if m.Valid() {
		m.cur += 1
	}

	return nil
}

func (m *mockIter) Close() {
	m.cur = -1
}

func TestOneByOneIter(t *testing.T) {
	assert := assert.New(t)

	iter1 := newMockIter([]*mockRecord{
		r("k1", "v1"),
		r("k5", "v3"),
	})
	iter2 := newMockIter([]*mockRecord{
		r("k2", "v2"),
		r("k4", "v4"),
	})
	iter3 := newMockIter([]*mockRecord{})
	iter4 := newMockIter([]*mockRecord{
		r("k3", "v3"),
		r("k6", "v6"),
	})

	// test for normal iter
	oneByOne := newOneByOneIter([]unionstore.Iterator{iter1, iter2, iter3, iter4})
	expected := make([]*mockRecord, 0)
	expected = append(expected, iter1.data...)
	expected = append(expected, iter2.data...)
	expected = append(expected, iter3.data...)
	expected = append(expected, iter4.data...)
	records := make([]*mockRecord, 0)
	for oneByOne.Valid() {
		records = append(records, r(string(oneByOne.Key()), string(oneByOne.Value())))
		err := oneByOne.Next()
		assert.Nil(err)
	}
	assert.NotNil(oneByOne.Next())
	assert.Equal(len(expected), len(records))
	for i := range expected {
		assert.Equal(expected[i].key, records[i].key)
		assert.Equal(expected[i].value, records[i].value)
	}

	// test for close
	oneByOne = newOneByOneIter([]unionstore.Iterator{iter1, iter2, iter3, iter4})
	oneByOne.Close()
	assert.False(oneByOne.Valid())
	assert.Nil(oneByOne.Key())
	assert.Nil(oneByOne.Value())
	assert.NotNil(oneByOne.Next())

	// test for one inner iter
	iter1 = newMockIter([]*mockRecord{
		r("k1", "v1"),
		r("k5", "v3"),
	})
	oneByOne = newOneByOneIter([]unionstore.Iterator{iter1})
	expected = make([]*mockRecord, 0)
	expected = append(expected, iter1.data...)
	records = make([]*mockRecord, 0)
	for oneByOne.Valid() {
		records = append(records, r(string(oneByOne.Key()), string(oneByOne.Value())))
		err := oneByOne.Next()
		assert.Nil(err)
	}
	assert.NotNil(oneByOne.Next())
	assert.Equal(len(expected), len(records))
	for i := range expected {
		assert.Equal(expected[i].key, records[i].key)
		assert.Equal(expected[i].value, records[i].value)
	}

	// test for empty iter
	iter3 = newMockIter([]*mockRecord{})
	oneByOne = newOneByOneIter([]unionstore.Iterator{iter3})
	assert.False(oneByOne.Valid())
	assert.Nil(oneByOne.Key())
	assert.Nil(oneByOne.Value())
	assert.NotNil(oneByOne.Next())
}
