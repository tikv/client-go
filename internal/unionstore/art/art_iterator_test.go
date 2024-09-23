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
			idxes, nodes := it.seek(key)
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
			it := &Iterator{
				tree: tree,
			}
			idxes, _ := it.seek([]byte{byte(2*i + 1)})
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

func TestIterNoResult(t *testing.T) {
	tree := New()
	require.Nil(t, tree.Set([]byte{1, 1}, []byte{1, 1}))
	// Test lower bound and upper bound seek same position
	iter, err := tree.Iter([]byte{1, 0, 0}, []byte{1, 0, 1})
	require.Nil(t, err)
	require.False(t, iter.Valid())
	iter, err = tree.IterReverse([]byte{1, 0, 1}, []byte{1, 0, 0})
	require.Nil(t, err)
	require.False(t, iter.Valid())
	// Test lower bound >= upper bound
	iter, err = tree.Iter([]byte{1, 0, 1}, []byte{1, 0, 0})
	require.Nil(t, err)
	require.False(t, iter.Valid())
	iter, err = tree.IterReverse([]byte{1, 0, 0}, []byte{1, 0, 1})
	require.Nil(t, err)
	require.False(t, iter.Valid())
	iter, err = tree.Iter([]byte{1, 1}, []byte{1, 1})
	require.Nil(t, err)
	require.False(t, iter.Valid())
}

func TestIterCleanedRecord(t *testing.T) {
	tree := New()
	require.Nil(t, tree.Set([]byte{1}, []byte{1}))
	handle := tree.Staging()
	require.Nil(t, tree.Set([]byte{2}, []byte{2}))
	tree.Cleanup(handle)
	iter, err := tree.Iter(nil, nil)
	require.Nil(t, err)
	require.True(t, iter.Valid())
	require.Equal(t, iter.Key(), []byte{1})
	require.Equal(t, iter.Value(), []byte{1})
	require.Nil(t, iter.Next())
	require.False(t, iter.Valid())
}
