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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

func TestIterateNodeCapacity(t *testing.T) {
	check := func(tree *ART, startKey, endKey []byte, startVal, endVal int) {
		// iter
		it, err := tree.Iter(startKey, endKey)
		require.Nil(t, err)
		handles := make([]arena.MemKeyHandle, 0, endVal-startVal+1)
		for i := startVal; i <= endVal; i++ {
			require.True(t, it.Valid(), i)
			require.Equal(t, it.Key(), []byte{byte(i)})
			require.Equal(t, it.Value(), []byte{byte(i)})
			handles = append(handles, it.Handle())
			require.Nil(t, it.Next())
		}
		require.False(t, it.Valid())
		require.Error(t, it.Next())
		for i, handle := range handles {
			require.Equal(t, tree.GetKeyByHandle(handle), []byte{byte(startVal + i)})
			val, valid := tree.GetValueByHandle(handle)
			require.True(t, valid)
			require.Equal(t, val, []byte{byte(startVal + i)})
		}
		// reverse iter
		it, err = tree.IterReverse(endKey, startKey)
		require.Nil(t, err)
		handles = handles[:0]
		for i := endVal; i >= startVal; i-- {
			require.True(t, it.Valid(), i)
			require.Equal(t, it.Key(), []byte{byte(i)})
			require.Equal(t, it.Value(), []byte{byte(i)})
			handles = append(handles, it.Handle())
			require.Nil(t, it.Next())
		}
		require.False(t, it.Valid())
		require.Error(t, it.Next())
		for i, handle := range handles {
			require.Equal(t, tree.GetKeyByHandle(handle), []byte{byte(endVal - i)})
			val, valid := tree.GetValueByHandle(handle)
			require.True(t, valid)
			require.Equal(t, val, []byte{byte(endVal - i)})
		}
	}
	for _, capacity := range []int{node4cap, node16cap, node48cap, node256cap} {
		tree := New()
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			require.Nil(t, tree.Set(key, key))
		}
		check(tree, nil, nil, 0, capacity-1)
		mid := capacity / 2
		check(tree, []byte{byte(mid)}, nil, mid, capacity-1) // lower bound is inclusive
		check(tree, nil, []byte{byte(mid)}, 0, mid-1)        // upper bound is exclusive
	}
}

func TestIterSeekLeaf(t *testing.T) {
	for _, capacity := range []int{node4cap, node16cap, node48cap, node256cap} {
		tree := New()
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			require.Nil(t, tree.Set(key, key))
		}
		for i := 0; i < capacity; i++ {
			key := []byte{byte(i)}
			it, err := tree.Iter(key, nil)
			require.Nil(t, err)
			it.inner.seek(tree.root, key)
			idxes, nodes := it.inner.idxes, it.inner.nodes
			require.Greater(t, len(idxes), 0)
			require.Equal(t, len(idxes), len(nodes))
			leafNode := nodes[len(nodes)-1].at(&tree.allocator, idxes[len(idxes)-1])
			require.NotEqual(t, leafNode, nullArtNode)
			leaf := leafNode.asLeaf(&tree.allocator)
			require.Equal(t, leaf.GetKey(), key)
		}
	}
}

func TestMultiLevelIterate(t *testing.T) {
	tree := New()
	var keys [][]byte
	for i := 0; i < 20; i++ {
		key := make([]byte, i+1)
		keys = append(keys, key)
	}
	for _, key := range keys {
		require.Nil(t, tree.Set(key, key))
	}
	// iter
	it, err := tree.Iter(nil, nil)
	require.Nil(t, err)
	handles := make([]arena.MemKeyHandle, 0, len(keys))
	for _, key := range keys {
		require.True(t, it.Valid())
		require.Equal(t, it.Key(), key)
		require.Equal(t, it.Value(), key)
		handles = append(handles, it.Handle())
		require.Nil(t, it.Next())
	}
	require.False(t, it.Valid())
	require.Error(t, it.Next())
	for i, handle := range handles {
		require.Equal(t, tree.GetKeyByHandle(handle), keys[i])
		val, valid := tree.GetValueByHandle(handle)
		require.True(t, valid)
		require.Equal(t, val, keys[i])
	}
	// reverse iter
	it, err = tree.IterReverse(nil, nil)
	require.Nil(t, err)
	handles = handles[:0]
	for i := len(keys) - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		require.Equal(t, it.Key(), keys[i])
		require.Equal(t, it.Value(), keys[i])
		handles = append(handles, it.Handle())
		require.Nil(t, it.Next())
	}
	require.False(t, it.Valid())
	require.Error(t, it.Next())
	for i, handle := range handles {
		require.Equal(t, tree.GetKeyByHandle(handle), keys[len(keys)-i-1])
		val, valid := tree.GetValueByHandle(handle)
		require.True(t, valid)
		require.Equal(t, val, keys[len(keys)-i-1])
	}
}

func TestSeekMeetLeaf(t *testing.T) {
	tree := New()
	require.Nil(t, tree.Set([]byte{1}, []byte{1}))
	iter, err := tree.Iter([]byte{1}, []byte{1, 1})
	require.Nil(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, iter.Key(), []byte{1})
	require.Equal(t, iter.Value(), []byte{1})
	require.Nil(t, iter.Next())
	require.False(t, iter.Valid())
	require.Nil(t, tree.Set([]byte{2}, []byte{2}))
	iter, err = tree.IterReverse([]byte{2, 2}, []byte{1, 1})
	require.Nil(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, iter.Key(), []byte{2})
	require.Equal(t, iter.Value(), []byte{2})
	require.Nil(t, iter.Next())
	require.False(t, iter.Valid())

	tree = New()
	require.Nil(t, tree.Set([]byte{1, 1}, []byte{1, 1}))
	iter, err = tree.Iter([]byte{1, 0, 0}, []byte{1, 2, 0})
	require.Nil(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, iter.Key(), []byte{1, 1})
	require.Equal(t, iter.Value(), []byte{1, 1})
	require.Nil(t, iter.Next())
	require.False(t, iter.Valid())
}

func TestSeekInExistNode(t *testing.T) {
	check := func(tree *ART, kind nodeKind) {
		cnt := 0
		switch kind {
		case typeNode4:
			cnt = node4cap
		case typeNode16:
			cnt = node16cap
		case typeNode48:
			cnt = node48cap
		case typeNode256:
			cnt = node48cap + 1
		default:
			panic("invalid node kind")
		}
		for i := 0; i < cnt; i++ {
			key := []byte{byte(2 * i)}
			require.Nil(t, tree.Set(key, key))
		}
		require.Equal(t, tree.root.kind, kind)
		for i := 0; i < cnt-1; i++ {
			helper := new(baseIter)
			helper.allocator = &tree.allocator
			helper.seek(tree.root, []byte{byte(2*i + 1)})
			idxes := helper.idxes
			expect := 0
			switch kind {
			case typeNode4, typeNode16:
				// node4 and node16 is not indexed by char
				expect = i + 1
			case typeNode48, typeNode256:
				// node48 and node256 is indexed by char
				expect = 2 * (i + 1)
			default:
				panic("invalid node kind")
			}
			require.Equal(t, idxes, []int{expect})
		}
	}

	check(New(), typeNode4)
	check(New(), typeNode16)
	check(New(), typeNode48)
	check(New(), typeNode256)
}

func TestSeekToIdx(t *testing.T) {
	tree := New()
	check := func(kind nodeKind) {
		var addr arena.MemdbArenaAddr
		switch kind {
		case typeNode4:
			addr, _ = tree.allocator.allocNode4()
		case typeNode16:
			addr, _ = tree.allocator.allocNode16()
		case typeNode48:
			addr, _ = tree.allocator.allocNode48()
		case typeNode256:
			addr, _ = tree.allocator.allocNode256()
		}
		node := artNode{kind: kind, addr: addr}
		lfAddr, _ := tree.allocator.allocLeaf([]byte{10})
		lfNode := artNode{kind: typeLeaf, addr: lfAddr}
		node.addChild(&tree.allocator, 10, false, lfNode)

		var (
			existIdx int
			maxIdx   int
		)
		switch kind {
		case typeNode4, typeNode16:
			existIdx = 0
			maxIdx = 1
		case typeNode48, typeNode256:
			existIdx = 10
			maxIdx = node256cap
		}

		nextIdx := seekToIdx(&tree.allocator, node, 1)
		require.Equal(t, existIdx, nextIdx)
		nextIdx = seekToIdx(&tree.allocator, node, 11)
		require.Equal(t, maxIdx, nextIdx)
	}

	check(typeNode4)
	check(typeNode16)
	check(typeNode48)
	check(typeNode256)
}

func TestIterateHandle(t *testing.T) {
	tree := New()
	h := tree.Staging()
	require.Nil(t, tree.Set([]byte{1}, []byte{2}))
	it := tree.IterWithFlags(nil, nil)
	handle := it.Handle()

	require.Equal(t, tree.GetKeyByHandle(handle), []byte{1})
	val, valid := tree.GetValueByHandle(handle)
	require.True(t, valid)
	require.Equal(t, val, []byte{2})

	tree.Cleanup(h)
	require.Equal(t, tree.GetKeyByHandle(handle), []byte{1})
	_, valid = tree.GetValueByHandle(handle)
	require.False(t, valid)
}

func TestSeekPrefixMismatch(t *testing.T) {
	tree := New()

	shortPrefix := make([]byte, 10)
	longPrefix := make([]byte, 30)
	for i := 0; i < len(shortPrefix); i++ {
		shortPrefix[i] = 1
	}
	for i := 0; i < len(longPrefix); i++ {
		longPrefix[i] = 2
	}

	keys := [][]byte{
		append(shortPrefix, 1),
		append(shortPrefix, 2),
		append(longPrefix, 3),
		append(longPrefix, 4),
	}
	for _, key := range keys {
		require.Nil(t, tree.Set(key, key))
	}

	it, err := tree.Iter(append(shortPrefix[:len(shortPrefix)-1], 0), append(longPrefix[:len(longPrefix)-1], 3))
	require.Nil(t, err)
	for _, key := range keys {
		require.True(t, it.Valid())
		require.Equal(t, it.Key(), key)
		require.Equal(t, it.Value(), key)
		require.Nil(t, it.Next())
	}
	require.False(t, it.Valid())
}

func TestIterPositionCompare(t *testing.T) {
	compare := func(idx1, idx2 []int) int {
		helper1, helper2 := new(baseIter), new(baseIter)
		helper1.idxes, helper2.idxes = idx1, idx2
		return helper1.compare(helper2)
	}

	require.Equal(t, compare([]int{1, 2, 3}, []int{1, 2, 3}), 0)
	require.Equal(t, compare([]int{1, 2, 2}, []int{1, 2, 3}), -1)
	require.Equal(t, compare([]int{1, 2, 4}, []int{1, 2, 3}), 1)
	require.Equal(t, compare([]int{1, 2, 3}, []int{1, 2}), 1)
	require.Equal(t, compare([]int{1, 2}, []int{1, 2, 3}), -1)
}

func TestIterSeekNoResult(t *testing.T) {
	check := func(kind nodeKind) {
		var child int
		switch kind {
		case typeNode4:
			child = 0
		case typeNode16:
			child = node4cap + 1
		case typeNode48:
			child = node16cap + 1
		case typeNode256:
			child = node48cap + 1
		}
		// let the node expand to target kind
		tree := New()
		for i := 0; i < child; i++ {
			require.Nil(t, tree.Set([]byte{1, byte(i)}, []byte{1, byte(i)}))
		}

		require.Nil(t, tree.Set([]byte{1, 100}, []byte{1, 100}))
		require.Nil(t, tree.Set([]byte{1, 200}, []byte{1, 200}))
		it, err := tree.Iter([]byte{1, 100, 1}, []byte{1, 200})
		require.Nil(t, err)
		require.False(t, it.Valid())
		it, err = tree.IterReverse([]byte{1, 200}, []byte{1, 100, 1})
		require.Nil(t, err)
		require.False(t, it.Valid())
	}

	check(typeNode4)
	check(typeNode16)
	check(typeNode48)
	check(typeNode256)
}
