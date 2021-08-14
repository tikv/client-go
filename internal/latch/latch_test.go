// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/latch/latch_test.go
//

// Copyright 2018 PingCAP, Inc.
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

package latch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
)

var tso atomic.Uint64

func getTso() uint64 {
	return tso.Inc()
}

func newLock(latches *Latches, keys [][]byte) (startTs uint64, lock *Lock) {
	startTs = getTso()
	lock = latches.genLock(startTs, keys)
	return
}

func TestWakeUp(t *testing.T) {
	latches := NewLatches(256)

	keysA := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	_, lockA := newLock(latches, keysA)

	keysB := [][]byte{[]byte("d"), []byte("e"), []byte("a"), []byte("c")}
	startTSB, lockB := newLock(latches, keysB)

	// A acquire lock success.
	result := latches.acquire(lockA)
	assert.Equal(t, acquireSuccess, result)

	// B acquire lock failed.
	result = latches.acquire(lockB)
	assert.Equal(t, acquireLocked, result)

	// A release lock, and get wakeup list.
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	wakeupList = latches.release(lockA, wakeupList)
	assert.Equal(t, startTSB, wakeupList[0].startTS)

	// B acquire failed since startTSB has stale for some keys.
	result = latches.acquire(lockB)
	assert.Equal(t, acquireStale, result)

	// B release lock since it received a stale.
	wakeupList = latches.release(lockB, wakeupList)
	assert.Len(t, wakeupList, 0)

	// B restart:get a new startTS.
	startTSB = getTso()
	lockB = latches.genLock(startTSB, keysB)
	result = latches.acquire(lockB)
	assert.Equal(t, acquireSuccess, result)
}

func TestFirstAcquireFailedWithStale(t *testing.T) {
	latches := NewLatches(256)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	_, lockA := newLock(latches, keys)
	startTSB, lockB := newLock(latches, keys)

	// acquire lockA success
	result := latches.acquire(lockA)
	assert.Equal(t, acquireSuccess, result)

	// release lockA
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	latches.release(lockA, wakeupList)
	assert.Greater(t, commitTSA, startTSB)

	// acquire lockB first time, should be failed with stale since commitTSA > startTSB
	result = latches.acquire(lockB)
	assert.Equal(t, acquireStale, result)
	latches.release(lockB, wakeupList)
}

func TestRecycle(t *testing.T) {
	latches := NewLatches(8)
	now := time.Now()
	startTS := oracle.GoTimeToTS(now)
	lock := latches.genLock(startTS, [][]byte{
		[]byte("a"), []byte("b"),
	})
	lock1 := latches.genLock(startTS, [][]byte{
		[]byte("b"), []byte("c"),
	})
	assert.Equal(t, acquireSuccess, latches.acquire(lock))
	assert.Equal(t, acquireLocked, latches.acquire(lock1))
	lock.SetCommitTS(startTS + 1)
	var wakeupList []*Lock
	latches.release(lock, wakeupList)
	// Release lock will grant latch to lock1 automatically,
	// so release lock1 is called here.
	latches.release(lock1, wakeupList)

	lock2 := latches.genLock(startTS+3, [][]byte{
		[]byte("b"), []byte("c"),
	})
	assert.Equal(t, acquireSuccess, latches.acquire(lock2))
	wakeupList = wakeupList[:0]
	latches.release(lock2, wakeupList)

	allEmpty := true
	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		if latch.queue != nil {
			allEmpty = false
		}
	}
	assert.False(t, allEmpty)

	currentTS := oracle.GoTimeToTS(now.Add(expireDuration)) + 3
	latches.recycle(currentTS)

	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		assert.Nil(t, latch.queue)
	}
}
