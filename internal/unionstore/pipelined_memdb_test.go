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
	avgKeySize := MinFlushSize / MinFlushKeys

	// block the flush goroutine for checking the flushingMemDB status.
	blockCh := make(chan struct{})
	defer close(blockCh)
	// Will not flush when keys number >= MinFlushKeys and size < MinFlushSize
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)-1) // (key + value) * MinFLushKeys < MinFlushKeys
		memdb.Set(key, value)
		flushed, err := memdb.Flush(false)
		require.False(t, flushed)
		require.Nil(t, err)
		require.False(t, memdb.OnFlushing())
	}
	require.Equal(t, memdb.memDB.Len(), MinFlushKeys)
	require.Less(t, memdb.memDB.Size(), MinFlushSize)

	// Will not flush when keys number < MinFlushKeys and size >= MinFlushSize
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys-1; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)+1) // (key + value) * (MinFLushKeys - 1) > MinFlushKeys
		memdb.Set(key, value)
		flushed, err := memdb.Flush(false)
		require.False(t, flushed)
		require.Nil(t, err)
		require.False(t, memdb.OnFlushing())
	}
	require.Less(t, memdb.memDB.Len(), MinFlushKeys)
	require.Greater(t, memdb.memDB.Size(), MinFlushSize)

	// Flush when keys number >= MinFlushKeys and size >= MinFlushSize
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)+1) // (key + value) * MinFLushKeys > MinFlushKeys
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
}

func TestPipelinedFlushSkip(t *testing.T) {
	blockCh := make(chan struct{})
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
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
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
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
}

func TestPipelinedFlushBlock(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh)
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		memdb.Set(key, value)
	}
	flushed, err := memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)
	require.True(t, memdb.OnFlushing())
	require.Equal(t, memdb.memDB.Len(), 0)
	require.Equal(t, memdb.memDB.Size(), 0)

	// When size of memdb is greater than ForceFlushSizeThreshold, Flush will be blocked.
	for i := 0; i < MinFlushKeys-1; i++ {
		key := []byte(strconv.Itoa(MinFlushKeys + i))
		value := make([]byte, ForceFlushSizeThreshold/(MinFlushKeys-1)-len(key)+1)
		memdb.Set(key, value)
	}
	require.Greater(t, memdb.memDB.Size(), ForceFlushSizeThreshold)
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
}

func TestPipelinedFlushGet(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh)
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		<-blockCh
		return nil
	})
	memdb.Set([]byte("key"), []byte("value"))
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
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
	_, err = memdb.memDB.Get([]byte("key"))
	require.True(t, tikverr.IsErrNotFound(err))
	// But we still can get the value by PipelinedMemDB.Get.
	value, err = memdb.Get(context.Background(), []byte("key"))
	require.Nil(t, err)
	require.Equal(t, value, []byte("value"))

	// finish the first flush
	blockCh <- struct{}{}
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		memdb.Set(key, value)
	}
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)
	require.True(t, memdb.OnFlushing())

	// now the key is guaranteed to be flushed into stores, though PipelinedMemDB.Get does not see it, snapshot get should get it.
	_, err = memdb.Get(context.Background(), []byte("key"))
	require.True(t, tikverr.IsErrNotFound(err))
}

func TestPipelinedFlushSize(t *testing.T) {
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		return nil
	})
	size := 0
	keys := 0
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
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
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
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
}

func TestPipelinedFlushGeneration(t *testing.T) {
	generationCh := make(chan uint64)
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(generation uint64, db *MemDB) error {
		generationCh <- generation
		return nil
	})
	for i := 0; i < 100; i++ {
		memdb.Set([]byte{uint8(i)}, []byte{uint8(i)})
		memdb.Flush(true)
		// generation start from 1
		require.Equal(t, <-generationCh, uint64(i+1))
	}
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

	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error {
		return nil
	})
	iteratorToErr(memdb.SnapshotIter(nil, nil))
	iteratorToErr(memdb.SnapshotIterReverse(nil, nil))
}

func TestPipelinedAdjustFlushCondition(t *testing.T) {
	util.EnableFailpoints()
	memdb := NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err := memdb.Flush(false)
	require.Nil(t, err)
	require.False(t, flushed)

	// can flush even only 1 key
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(1)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)

	// need 2 keys to flush
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(2)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.False(t, flushed)

	// need 2 keys to flush, but force threshold reached
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushKeys", `return(2)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBMinFlushSize", `return(1)`))
	require.Nil(t, failpoint.Enable("tikvclient/pipelinedMemDBForceFlushSizeThreshold", `return(2)`))
	memdb = NewPipelinedMemDB(emptyBufferBatchGetter, func(_ uint64, db *MemDB) error { return nil })
	memdb.Set([]byte("key"), []byte("value"))
	flushed, err = memdb.Flush(false)
	require.Nil(t, err)
	require.True(t, flushed)

	require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushKeys"))
	require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBMinFlushSize"))
	require.Nil(t, failpoint.Disable("tikvclient/pipelinedMemDBForceFlushSizeThreshold"))
}
