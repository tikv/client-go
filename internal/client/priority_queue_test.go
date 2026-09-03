// Copyright 2023 TiKV Authors
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

package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type FakeItem struct {
	pri      uint64
	value    int
	canceled bool
}

func (f *FakeItem) priority() uint64 {
	return f.pri
}

func (f *FakeItem) isCanceled() bool {
	return f.canceled
}

func TestPriority(t *testing.T) {
	re := require.New(t)
	testFunc := func(aq *PriorityQueue) {
		for i := 1; i <= 5; i++ {
			aq.Push(&FakeItem{value: i, pri: uint64(i)})
		}
		re.Equal(5, aq.Len())
		re.Equal(uint64(5), aq.highestPriority())
		aq.clean()
		re.Equal(5, aq.Len())

		arr := aq.Take(1)
		re.Len(arr, 1)
		re.Equal(uint64(5), arr[0].priority())
		re.Equal(uint64(4), aq.highestPriority())

		arr = aq.Take(2)
		re.Len(arr, 2)
		re.Equal(uint64(4), arr[0].priority())
		re.Equal(uint64(3), arr[1].priority())
		re.Equal(uint64(2), aq.highestPriority())

		arr = aq.Take(5)
		re.Len(arr, 2)
		re.Equal(uint64(2), arr[0].priority())
		re.Equal(uint64(1), arr[1].priority())
		re.Equal(uint64(0), aq.highestPriority())
		re.Equal(0, aq.Len())

		aq.Push(&FakeItem{value: 1, pri: 1, canceled: true})
		re.Equal(1, aq.Len())
		aq.clean()
		re.Equal(0, aq.Len())
	}
	hq := NewPriorityQueue()
	testFunc(hq)
}
