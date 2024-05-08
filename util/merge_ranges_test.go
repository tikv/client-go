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
	"container/list"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeRangesSearchOverlapped(t *testing.T) {
	assert := require.New(t)

	mr := NewMergeRanges()
	type Case struct {
		key        int
		overlapped bool
		rng        *keyRange // the range overlap with or after the key
	}
	checkOverlapped := func(c Case) {
		mark, overlapped := mr.searchOverlapped(iToKey(c.key))
		assert.Equal(c.overlapped, overlapped)
		if c.rng == nil {
			assert.Nil(mark)
		} else {
			assert.Equal(c.rng, mark.Value.(*keyRange))
		}
	}

	// mr: empty
	checkOverlapped(Case{0, false, nil})

	insertRange(mr, 0, 1)
	// mr: [0,1)
	cases := []Case{
		{0, true, makeRange(0, 1)},
		{1, true, makeRange(0, 1)},
		{2, false, nil},
	}
	for _, c := range cases {
		checkOverlapped(c)
	}

	insertRange(mr, 3, 5)
	// mr: [0,1), [3,5)
	cases = []Case{
		{0, true, makeRange(0, 1)},
		{1, true, makeRange(0, 1)},
		{2, false, makeRange(3, 5)},
		{3, true, makeRange(3, 5)},
		{4, true, makeRange(3, 5)},
		{5, true, makeRange(3, 5)},
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
		start   int
		end     int
		covered bool
	}
	checkCovered := func(c Case) {
		assert.Equal(c.covered, mr.Covered(iToKey(c.start), iToKey(c.end)))
	}

	// mr: empty
	checkCovered(Case{0, 1, false})

	insertRange(mr, 0, 1)
	// mr: [0,1)
	cases := []Case{
		{0, 1, true},
		{1, 2, false},
	}
	for _, c := range cases {
		checkCovered(c)
	}

	insertRange(mr, 2, 4)
	// mr: [0,1), [2,4)
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
	assertEqual := func(keys [][2]int) {
		expected := makeMergeRanges(keys)
		assert.Equal(expected.ranges, mr.ranges)
	}

	insertRange(mr, 2, 3)
	assertEqual([][2]int{{2, 3}})
	insertRange(mr, 3, 4)
	assertEqual([][2]int{{2, 4}})

	insertRange(mr, 0, 1)
	assertEqual([][2]int{{0, 1}, {2, 4}})
	insertRange(mr, 1, 2)
	assertEqual([][2]int{{0, 4}})

	insertRange(mr, 5, 6)
	assertEqual([][2]int{{0, 4}, {5, 6}})
	insertRange(mr, 4, 5)
	assertEqual([][2]int{{0, 6}})

	insertRange(mr, 10, 11)
	assertEqual([][2]int{{0, 6}, {10, 11}})
	insertRange(mr, 3, 12)
	assertEqual([][2]int{{0, 12}})

	assert.True(mr.Covered(iToKey(0), iToKey(12)))
}

func TestMergeRangesOverlappingRanges(t *testing.T) {
	assert := require.New(t)

	// mr: [2,12)
	mr := makeMergeRanges([][2]int{{2, 12}})
	assertEqual := func(keys [][2]int) {
		expected := makeMergeRanges(keys)
		assert.Equal(expected.ranges, mr.ranges)
	}

	insertRange(mr, 2, 6)
	assertEqual([][2]int{{2, 12}})

	insertRange(mr, 3, 8)
	assertEqual([][2]int{{2, 12}})

	insertRange(mr, 6, 12)
	assertEqual([][2]int{{2, 12}})

	insertRange(mr, 1, 4)
	assertEqual([][2]int{{1, 12}})

	insertRange(mr, 4, 15)
	assertEqual([][2]int{{1, 15}})

	insertRange(mr, 20, 21)
	insertRange(mr, 22, 23)
	insertRange(mr, 24, 25)
	insertRange(mr, 14, 30)
	assertEqual([][2]int{{1, 30}})

	insertRange(mr, 40, 41)
	insertRange(mr, 42, 43)
	insertRange(mr, 50, 51)
	insertRange(mr, 53, 54)
	insertRange(mr, 0, 42)
	assertEqual([][2]int{{0, 43}, {50, 51}, {53, 54}})
}

func TestMergeRangesEmptyKey(t *testing.T) {
	assert := require.New(t)

	mr := NewMergeRanges()

	assert.False(mr.Covered(nil, nil))

	mr.Insert(nil, iToKey(10)) // (infinite, 10)
	mr.Insert(iToKey(10), nil) // [10, infinite)
	assert.True(mr.Covered(iToKey(0), iToKey(100)))
	assert.True(mr.Covered(nil, nil))
}

func iToKey(i int) []byte {
	return []byte(fmt.Sprintf("%04d", i))
}

func makeRange(start, end int) *keyRange {
	return &keyRange{
		start: iToKey(start),
		end:   iToKey(end),
	}
}

func insertRange(mr *MergeRanges, start, end int) {
	mr.Insert(iToKey(start), iToKey(end))
}

func makeMergeRanges(keys [][2]int) *MergeRanges {
	l := list.New()
	for _, key := range keys {
		l.PushBack(&keyRange{iToKey(key[0]), iToKey(key[1])})
	}
	return &MergeRanges{ranges: l}
}
