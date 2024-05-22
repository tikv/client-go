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

package unionstore

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/util"
)

func emptyBufferBatchGetter(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	return nil, nil
}

func TestPipelinedFlushTrigger(t *testing.T) {
	t.Skip()
	// because memdb's memory usage is hard to control, we use a cargo-culted value here.
	avgKeySize := int(MinFlushMemSize/MinFlushKeys) / 3

	// block the flush goroutine for checking the flushingMemDB status.
	blockCh := make(chan struct{})
	// Will not flush when keys number >= MinFlushKeys and size < MinFlushMemSize
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)-1)
		// (key + value) * MinFlushKeys < MinFlushMemSize
		memdb.Set(key, value)
		flushed, err := memdb.Flush(false)
		require.False(t, flushed)
		require.Nil(t, err)
		require.False(t, memdb.OnFlushing())
	}
	require.Equal(t, memdb.memDB.Len(), MinFlushKeys)
	require.Less(t, memdb.memDB.Mem(), MinFlushMemSize)

	// Will not flush when keys number < MinFlushKeys and size >= MinFlushMemSize
	avgKeySize = int(MinFlushMemSize/MinFlushKeys) / 2
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys-1; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize*2-len(key)+1)
		// (key + value) * (MinFLushKeys - 1) > MinFlushMemSize
		memdb.Set(key, value)
		flushed, err := memdb.Flush(false)
		require.False(t, flushed)
		require.Nil(t, err)
		require.False(t, memdb.OnFlushing())
	}
	require.Less(t, memdb.memDB.Len(), MinFlushKeys)
	require.Greater(t, memdb.memDB.Mem(), MinFlushMemSize)
	require.Less(t, memdb.memDB.Mem(), ForceFlushMemSizeThreshold)

	// Flush when keys number >= MinFlushKeys and mem size >= MinFlushMemSize
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize*2-len(key)+1) // (key + value) * MinFLushKeys > MinFlushKeys
		memdb.Set(key, value)
		flushed, err := memdb.Flush(false)
		require.Nil(t, err)
		if i == MinFlushKeys-1 {
			require.True(t, flushed)
			require.True(t, memdb.OnFlushing())
		} else {
			require.False(t, flushed)
			require.False(t, memdb.OnFlushing())
		}
	}
	require.Equal(t, memdb.memDB.Len(), 0)
	require.Equal(t, memdb.memDB.Size(), 0)
	// the flushingMemDB length and size should be added to the total length and size.
	require.Equal(t, memdb.Len(), MinFlushKeys)
	require.Equal(t, memdb.Size(), memdb.flushingMemDB.Size())
	close(blockCh)
	require.Nil(t, memdb.FlushWait())
}

func TestPipelinedFlushSkip(t *testing.T) {
	blockCh := make(chan struct{})
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		memdb.Set(key, value)
	}
	flushed, err := memdb.Flush(false)
	require.True(t, flushed)
	require.Nil(t, err)
	require.True(t, memdb.OnFlushing())
	require.Equal(t, memdb.memDB.Len(), 0)
	require.Equal(t, memdb.memDB.Size(), 0)
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(MinFlushKeys + i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		memdb.Set(key, value)
	}
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	// flush is skipped because there is an ongoing flush.
	require.False(t, flushed)
	require.Equal(t, memdb.memDB.Len(), MinFlushKeys)
	close(blockCh)
	require.Nil(t, memdb.FlushWait())
	// can flush when the ongoing flush is done.
	flushed, err = memdb.Flush(false)
	require.True(t, flushed)
	require.Nil(t, err)
	require.Equal(t, memdb.memDB.Len(), 0)
	require.Equal(t, memdb.len, 2*MinFlushKeys)
	require.Nil(t, memdb.FlushWait())
}

func TestPipelinedFlushBlock(t *testing.T) {
	blockCh := make(chan struct{})
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		memdb.Set(key, value)
	}
	flushed, err := memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)
	require.True(t, memdb.OnFlushing())
	require.Equal(t, memdb.memDB.Len(), 0)
	require.Equal(t, memdb.memDB.Size(), 0)

	// When size of memdb is greater than ForceFlushMemSizeThreshold, Flush will be blocked.
	for i := 0; i < MinFlushKeys-1; i++ {
		key := []byte(strconv.Itoa(MinFlushKeys + i))
		value := make([]byte, int(ForceFlushMemSizeThreshold/(MinFlushKeys-1))-len(key)+1)
		memdb.Set(key, value)
	}
	require.Greater(t, memdb.memDB.Mem(), ForceFlushMemSizeThreshold)
	flushReturned := make(chan struct{})
	oneSec := time.After(time.Second)
	go func() {
		flushed, err := memdb.Flush(false)
		require.Nil(t, err)
		require.True(t, flushed)
		close(flushReturned)
	}()
	select {
	case <-flushReturned:
		require.Fail(t, "Flush should be blocked")
	case <-oneSec:
	}
	require.True(t, memdb.OnFlushing())
	blockCh <- struct{}{} // first flush done
	<-flushReturned       // second flush start
	require.True(t, memdb.OnFlushing())
	close(blockCh)
	require.Nil(t, memdb.FlushWait())
}

func TestPipelinedFlushGet(t *testing.T) {
	blockCh := make(chan struct{})
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		<-blockCh
		return nil
	})
	memdb.Set([]byte("key"), []byte("value"))
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		memdb.Set(key, value)
	}
	value, err := memdb.Get(context.Background(), []byte("key"))
	require.Nil(t, err)
	require.Equal(t, value, []byte("value"))
	flushed, err := memdb.Flush(false)
	require.True(t, flushed)
	require.Nil(t, err)
	require.True(t, memdb.OnFlushing())

	// The key is in flushingMemDB memdb instead of current mutable memdb.
	_, err = memdb.memDB.Get(context.Background(), []byte("key"))
	require.True(t, tikverr.IsErrNotFound(err))
	// But we still can get the value by PipelinedMemDB.Get.
	value, err = memdb.Get(context.Background(), []byte("key"))
	require.Nil(t, err)
	require.Equal(t, value, []byte("value"))

	// finish the first flush
	blockCh <- struct{}{}
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		memdb.Set(key, value)
	}
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)
	require.True(t, memdb.OnFlushing())

	// now the key is guaranteed to be flushed into stores, though PipelinedMemDB.Get does not see it, snapshot get should get it.
	_, err = memdb.Get(context.Background(), []byte("key"))
	require.True(t, tikverr.IsErrNotFound(err))
	close(blockCh)
	require.Nil(t, memdb.FlushWait())
}

func TestPipelinedFlushSize(t *testing.T) {
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		return nil
	})
	size := 0
	keys := 0
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		keys++
		size += len(key) + len(value)
		memdb.Set(key, value)
		require.Equal(t, memdb.Len(), keys)
		require.Equal(t, memdb.Size(), size)
	}
	// keys & size should be accumulated into PipelinedMemDB.
	flushed, err := memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)
	require.Equal(t, memdb.memDB.Len(), 0)
	require.Equal(t, memdb.memDB.Size(), 0)
	require.Equal(t, memdb.Len(), keys)
	require.Equal(t, memdb.Size(), size)

	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(MinFlushKeys + i))
		value := make([]byte, int(MinFlushMemSize/MinFlushKeys)-len(key)+1)
		keys++
		size += len(key) + len(value)
		memdb.Set(key, value)
		require.Equal(t, memdb.Len(), keys)
		require.Equal(t, memdb.Size(), size)
	}
	require.Equal(t, memdb.Len(), keys)
	require.Equal(t, memdb.Size(), size)
	// with final flush, keys & size should not be changed.
	flushed, err = memdb.Flush(true)
	require.Nil(t, err)
	require.True(t, flushed)
	require.Equal(t, memdb.Len(), keys)
	require.Equal(t, memdb.Size(), size)
	require.Nil(t, memdb.FlushWait())
}

func TestPipelinedFlushGeneration(t *testing.T) {
	generationCh := make(chan uint64)
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(generation uint64, db *ArenaArt) error {
		generationCh <- generation
		return nil
	})
	for i := 0; i < 100; i++ {
		memdb.Set([]byte{uint8(i)}, []byte{uint8(i)})
		memdb.Flush(true)
		// generation start from 1
		require.Equal(t, <-generationCh, uint64(i+1))
	}
	require.Nil(t, memdb.FlushWait())
}

func TestErrorIterator(t *testing.T) {
	iteratorToErr := func(iter Iterator) {
		for iter.Valid() {
			err := iter.Next()
			if err != nil {
				return
			}
		}
		t.Log("iterator does not return error")
		t.Fail()
	}

	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error {
		return nil
	})
	iteratorToErr(memdb.SnapshotIter(nil, nil))
	iteratorToErr(memdb.SnapshotIterReverse(nil, nil))
}

func TestPipelinedAdjustFlushCondition(t *testing.T) {
	util.EnableFailpoints()
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err := memdb.Flush(false)
	require.Nil(t, err)
	require.False(t, flushed)

	// can flush even only 1 key
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(1)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)
	require.Nil(t, memdb.FlushWait())

	// need 2 keys to flush
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(2)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.False(t, flushed)
	require.Nil(t, memdb.FlushWait())

	// need 2 keys to flush, but force threshold reached
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(2)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(2)`))
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *ArenaArt) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)

	require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
	require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
	require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
	require.Nil(t, memdb.FlushWait())
}

func TestMemBufferBatchGetCache(t *testing.T) {
	util.EnableFailpoints()
	flushDone := make(chan struct{})
	var remoteMutex sync.RWMutex
	remoteBuffer := make(map[string][]byte, 16)
	pipelinedMemdb := NewPipelinedMemDB(func(_ context.Context, keys [][]byte) (map[string][]byte, error) {
		remoteMutex.RLock()
		defer remoteMutex.RUnlock()
		m := make(map[string][]byte, len(keys))
		for _, k := range keys {
			if val, ok := remoteBuffer[string(k)]; ok {
				m[string(k)] = val
			}
		}
		return m, nil
	}, func(_ uint64, db *ArenaArt) error {
		remoteMutex.Lock()
		defer remoteMutex.Unlock()
		for it, _ := db.Iter(nil, nil); it.Valid(); it.Next() {
			remoteBuffer[string(it.Key())] = it.Value()
		}
		flushDone <- struct{}{}
		return nil
	})

	mustGetFromCache := func(key []byte) ([]byte, error) {
		require.NotNil(t, pipelinedMemdb.batchGetCache)
		v, ok := pipelinedMemdb.batchGetCache[string(key)]
		require.True(t, ok)
		inner := v.Inner()
		if inner == nil {
			return nil, tikverr.ErrNotExist
		}
		return *inner, nil
	}
	mustNotExistInCache := func(key []byte) {
		require.NotNil(t, pipelinedMemdb.batchGetCache)
		_, ok := pipelinedMemdb.batchGetCache[string(key)]
		require.False(t, ok)
	}
	mustFlush := func() {
		flushed, err := pipelinedMemdb.Flush(true)
		require.Nil(t, err)
		require.True(t, flushed)
		<-flushDone
	}

	err := pipelinedMemdb.Set([]byte("k1"), []byte("v11"))
	require.Nil(t, err)
	mustFlush()
	err = pipelinedMemdb.Set([]byte("k2"), []byte("v21"))
	require.Nil(t, err)
	mustFlush()
	// memdb: [], flushing memdb: [k2 -> v21], remoteBuffer: [k1 -> v11, k2 -> v21]
	_, err = pipelinedMemdb.GetLocal(context.Background(), []byte("k1"))
	require.Error(t, err)
	require.True(t, tikverr.IsErrNotFound(err))
	v, err := pipelinedMemdb.GetLocal(context.Background(), []byte("k2"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("v21"))
	// batch get caches the result
	// cache: [k1 -> v11, k2 -> v21]
	m, err := pipelinedMemdb.BatchGet(context.Background(), [][]byte{[]byte("k1"), []byte("k2")})
	require.Nil(t, err)
	require.Equal(t, m, map[string][]byte{"k1": []byte("v11"), "k2": []byte("v21")})
	v, err = mustGetFromCache([]byte("k1"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("v11"))
	v, err = mustGetFromCache([]byte("k2"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("v21"))
	mustNotExistInCache([]byte("k3"))
	// cache: [k1 -> v11, k2 -> v21, k3 -> not exist]
	m, err = pipelinedMemdb.BatchGet(context.Background(), [][]byte{[]byte("k3")})
	require.Nil(t, err)
	require.Len(t, m, 0)
	_, err = mustGetFromCache([]byte("k3"))
	require.Error(t, err)
	require.True(t, tikverr.IsErrNotFound(err))

	// memdb: [], flushing memdb: [k1 -> [], k3 -> []], remoteBuffer: [k1 -> [], k2 -> v21, k3 -> []]
	pipelinedMemdb.Delete([]byte("k1"))
	pipelinedMemdb.Set([]byte("k2"), []byte("v22"))
	pipelinedMemdb.Delete([]byte("k3"))
	mustFlush()
	require.Nil(t, pipelinedMemdb.batchGetCache)
	// cache: [k1 -> [], k2 -> v22, k3 -> [], k4 -> not exist]
	m, err = pipelinedMemdb.BatchGet(context.Background(), [][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")})
	require.Nil(t, err)
	require.Equal(t, m, map[string][]byte{"k1": {}, "k2": []byte("v22"), "k3": {}})
	v, err = mustGetFromCache([]byte("k1"))
	require.Nil(t, err)
	require.Len(t, v, 0)
	v, err = mustGetFromCache([]byte("k2"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("v22"))
	v, err = mustGetFromCache([]byte("k3"))
	require.Nil(t, err)
	require.Len(t, v, 0)
	_, err = mustGetFromCache([]byte("k4"))
	require.Error(t, err)
	require.True(t, tikverr.IsErrNotFound(err))
	require.Nil(t, pipelinedMemdb.FlushWait())
}
