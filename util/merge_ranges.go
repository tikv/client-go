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
	"bytes"
	"container/list"
)

// To make life easier.
// MergeRanges is used for txn file only, all keys are TiDB keys, and must not be bigger than following `maxEndKey`.
var maxEndKey = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

type keyRange struct {
	start []byte
	end   []byte
}

type MergeRanges struct {
	ranges *list.List
}

func NewMergeRanges() *MergeRanges {
	r := &MergeRanges{
		ranges: list.New(),
	}
	return r
}

func (m *MergeRanges) IsEmpty() bool {
	return m.ranges.Len() == 0
}

// searchByStart search range by start key.
// If the key is found, return the element and true.
// If the key is not found, return the element to insert BEFORE and false. When the mark is nil, push to the back.
func (m *MergeRanges) searchByStart(key []byte) (mark *list.Element, ok bool) {
	for e := m.ranges.Front(); e != nil; e = e.Next() {
		cmp := bytes.Compare(e.Value.(*keyRange).start, key)
		if cmp == 0 {
			return e, true
		} else if cmp > 0 {
			return e, false
		}
	}
	return nil, false
}

// searchOverlapped search the overlapped range for key.
// range.start <= key <= range.end
// If the key is found, return the element and true.
// If the key is not found, return the element to insert BEFORE and false. When the mark is nil, push to the back.
func (m *MergeRanges) searchOverlapped(key []byte) (mark *list.Element, overlapped bool) {
	mark, overlapped = m.searchByStart(key)
	if overlapped {
		return mark, true
	}

	var prev *list.Element
	if mark == nil {
		prev = m.ranges.Back()
	} else {
		prev = mark.Prev()
	}

	if prev == nil {
		return m.ranges.Front(), false
	}
	if bytes.Compare(key, prev.Value.(*keyRange).end) <= 0 {
		return prev, true
	}
	return mark, false
}

// Covered returns whether the range [start,end) is fully covered.
func (m *MergeRanges) Covered(start []byte, end []byte) bool {
	if len(end) == 0 {
		end = maxEndKey
	}

	mark, ok := m.searchOverlapped(start)
	if ok {
		return bytes.Compare(end, mark.Value.(*keyRange).end) <= 0
	}
	return false
}

// Insert inserts the range [start,end) into MergeRanges.
func (m *MergeRanges) Insert(start []byte, end []byte) {
	if len(end) == 0 {
		end = maxEndKey
	}

	left, ok := m.searchOverlapped(start)
	if ok {
		if bytes.Compare(end, left.Value.(*keyRange).end) <= 0 {
			return
		}
		start = left.Value.(*keyRange).start
	}

	if left == nil {
		m.ranges.PushBack(&keyRange{start, end})
		return
	}

	right, ok := m.searchOverlapped(end)
	rightBound := right
	if ok {
		end = right.Value.(*keyRange).end
		rightBound = right.Next()
	}

	m.ranges.InsertBefore(&keyRange{start, end}, left)

	// Remove overlapped.
	var next *list.Element
	for e := left; e != rightBound; e = next {
		next = e.Next()
		m.ranges.Remove(e)
	}
}
