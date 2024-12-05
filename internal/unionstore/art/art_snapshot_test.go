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

package art

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

func TestSnapshotIteratorPreventFreeNode(t *testing.T) {
	check := func(num int) {
		tree := New()
		for i := 0; i < num; i++ {
			tree.Set([]byte{0, byte(i)}, []byte{0, byte(i)})
		}
		var unusedNodeSlice *[]arena.MemdbArenaAddr
		switch num {
		case 4:
			unusedNodeSlice = &tree.allocator.nodeAllocator.unusedNode4
		case 16:
			unusedNodeSlice = &tree.allocator.nodeAllocator.unusedNode16
		case 48:
			unusedNodeSlice = &tree.allocator.nodeAllocator.unusedNode48
		default:
			panic("unsupported num")
		}
		it := tree.SnapshotIter(nil, nil)
		require.Equal(t, 0, len(*unusedNodeSlice))
		tree.Set([]byte{0, byte(num)}, []byte{0, byte(num)})
		require.Equal(t, 1, len(*unusedNodeSlice))
		it.Close()
		require.Equal(t, 0, len(*unusedNodeSlice))
	}

	check(4)
	check(16)
	check(48)
}

func TestConcurrentSnapshotIterNoRace(t *testing.T) {
	check := func(num int) {
		tree := New()
		for i := 0; i < num; i++ {
			tree.Set([]byte{0, byte(i)}, []byte{0, byte(i)})
		}

		const concurrency = 100
		it := tree.SnapshotIter(nil, nil)

		tree.Set([]byte{0, byte(num)}, []byte{0, byte(num)})

		var wg sync.WaitGroup
		wg.Add(concurrency)
		go func() {
			it.Close()
			wg.Done()
		}()
		for i := 1; i < concurrency; i++ {
			go func(it *SnapIter) {
				concurrentIt := tree.SnapshotIter(nil, nil)
				concurrentIt.Close()
				wg.Done()
			}(it)
		}
		wg.Wait()

		require.Empty(t, tree.allocator.nodeAllocator.unusedNode4)
		require.Empty(t, tree.allocator.nodeAllocator.unusedNode16)
		require.Empty(t, tree.allocator.nodeAllocator.unusedNode48)
	}

	check(4)
	check(16)
	check(48)
}
