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

package util

import (
	// "container/list"
	// "container/list"
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeRangesSearchOverlapped(t *testing.T) {
	assert := require.New(t)

	mr := NewMergeRanges()
	type Case struct {
		key int
		ok  bool
		rng *keyRange
	}
	checkOverlapped := func(c Case) {
		mark, ok := mr.searchOverlapped(iToKey(c.key))
		assert.Equal(c.ok, ok)
		if c.rng == nil {
			assert.Nil(mark)
		} else {
			assert.Equal(c.rng, mark.Value.(*keyRange))
		}
	}

	checkOverlapped(Case{0, false, nil})

	insert(mr, 0, 1)
	cases := []Case{
		{0, true, makeKeyRange(0, 1)},
		{1, true, makeKeyRange(0, 1)},
		{2, false, nil},
	}
	for _, c := range cases {
		checkOverlapped(c)
	}

	insert(mr, 3, 5)
	cases = []Case{
		{0, true, makeKeyRange(0, 1)},
		{1, true, makeKeyRange(0, 1)},
		{2, false, makeKeyRange(3, 5)},
		{3, true, makeKeyRange(3, 5)},
		{4, true, makeKeyRange(3, 5)},
		{5, true, makeKeyRange(3, 5)},
		{6, false, nil},
	}
	for _, c := range cases {
		checkOverlapped(c)
	}
}

func TestMergeRangesCovered(t *testing.T) {
	assert := require.New(t)

	mr := NewMergeRanges()
	type Case struct {
		start    int
		end      int
		expected bool
	}
	checkCovered := func(c Case) {
		assert.Equal(c.expected, mr.Covered(iToKey(c.start), iToKey(c.end)))
	}

	checkCovered(Case{0, 1, false})

	insert(mr, 0, 1)
	cases := []Case{
		{0, 1, true},
		{1, 2, false},
	}
	for _, c := range cases {
		checkCovered(c)
	}

	insert(mr, 2, 4)
	cases = []Case{
		{0, 1, true},
		{1, 2, false},
		{2, 3, true},
		{2, 4, true},
		{1, 2, false},
		{1, 3, false},
		{1, 4, false},
		{1, 5, false},
		{2, 5, false},
		{4, 5, false},
		{0, 4, false},
	}
	for _, c := range cases {
		checkCovered(c)
	}
}

func TestMergeRangesInsert(t *testing.T) {
	assert := require.New(t)

	mr := NewMergeRanges()
	checkMr := func(keys [][2]int) {
		expected := makeMergeRanges(keys)
		assert.Equal(expected.ranges, mr.ranges)
	}

	insert(mr, 2, 3)
	checkMr([][2]int{{2, 3}})
	insert(mr, 3, 4)
	checkMr([][2]int{{2, 4}})

	insert(mr, 0, 1)
	checkMr([][2]int{{0, 1}, {2, 4}})
	insert(mr, 1, 2)
	checkMr([][2]int{{0, 4}})

	insert(mr, 5, 6)
	checkMr([][2]int{{0, 4}, {5, 6}})
	insert(mr, 4, 5)
	checkMr([][2]int{{0, 6}})

	insert(mr, 10, 11)
	checkMr([][2]int{{0, 6}, {10, 11}})
	insert(mr, 3, 12)
	checkMr([][2]int{{0, 12}})

	assert.True(mr.Covered(iToKey(0), iToKey(12)))
}

func TestMergeRangesOverlappingRanges(t *testing.T) {
	assert := require.New(t)

	mr := makeMergeRanges([][2]int{{2, 12}})
	checkMr := func(keys [][2]int) {
		expected := makeMergeRanges(keys)
		assert.Equal(expected.ranges, mr.ranges)
	}

	insert(mr, 2, 6)
	checkMr([][2]int{{2, 12}})

	insert(mr, 3, 8)
	checkMr([][2]int{{2, 12}})

	insert(mr, 6, 12)
	checkMr([][2]int{{2, 12}})

	insert(mr, 1, 4)
	checkMr([][2]int{{1, 12}})

	insert(mr, 4, 15)
	checkMr([][2]int{{1, 15}})

	insert(mr, 20, 21)
	insert(mr, 22, 23)
	insert(mr, 24, 25)
	insert(mr, 14, 30)
	checkMr([][2]int{{1, 30}})

	insert(mr, 40, 41)
	insert(mr, 42, 43)
	insert(mr, 50, 51)
	insert(mr, 53, 54)
	insert(mr, 0, 42)
	checkMr([][2]int{{0, 43}, {50, 51}, {53, 54}})
}

func TestMergeRangesEmptyKey(t *testing.T) {
	assert := require.New(t)

	mr := NewMergeRanges()

	assert.False(mr.Covered(nil, nil))

	mr.Insert(nil, iToKey(10))
	mr.Insert(iToKey(10), nil)
	assert.True(mr.Covered(iToKey(0), iToKey(100)))
	assert.True(mr.Covered(nil, nil))
}

func iToKey(i int) []byte {
	return []byte(fmt.Sprintf("%04d", i))
}

func makeKeyRange(start, end int) *keyRange {
	return &keyRange{
		start: iToKey(start),
		end:   iToKey(end),
	}
}

func insert(mr *MergeRanges, start, end int) {
	mr.Insert(iToKey(start), iToKey(end))
}

func makeMergeRanges(keys [][2]int) *MergeRanges {
	l := list.New()
	for _, key := range keys {
		l.PushBack(&keyRange{iToKey(key[0]), iToKey(key[1])})
	}
	return &MergeRanges{ranges: l}
}
