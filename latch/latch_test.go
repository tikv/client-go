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
// See the License for the specific language governing permissions and
// limitations under the License.

package latch

import (
	"github.com/stretchr/testify/suite"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tikv/client-go/v2/oracle"
)

func TestT(t *testing.T) {
	suite.Run(t, new(testLatchSuite))
}

var baseTso uint64

type testLatchSuite struct {
	suite.Suite
	latches *Latches
}

func (s *testLatchSuite) SetupTest() {
	s.latches = NewLatches(256)
}

func (s *testLatchSuite) newLock(keys [][]byte) (startTS uint64, lock *Lock) {
	startTS = getTso()
	lock = s.latches.genLock(startTS, keys)
	return
}

func getTso() uint64 {
	return atomic.AddUint64(&baseTso, uint64(1))
}

func (s *testLatchSuite) TestWakeUp() {
	keysA := [][]byte{
		[]byte("a"), []byte("b"), []byte("c")}
	_, lockA := s.newLock(keysA)

	keysB := [][]byte{[]byte("d"), []byte("e"), []byte("a"), []byte("c")}
	startTSB, lockB := s.newLock(keysB)

	// A acquire lock success.
	result := s.latches.acquire(lockA)
	s.Equal(acquireSuccess, result)

	// B acquire lock failed.
	result = s.latches.acquire(lockB)
	s.Equal(acquireLocked, result)

	// A release lock, and get wakeup list.
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	wakeupList = s.latches.release(lockA, wakeupList)
	s.Equal(startTSB, wakeupList[0].startTS)

	// B acquire failed since startTSB has stale for some keys.
	result = s.latches.acquire(lockB)
	s.Equal(acquireStale, result)

	// B release lock since it received a stale.
	wakeupList = s.latches.release(lockB, wakeupList)
	s.Len(wakeupList, 0)

	// B restart:get a new startTS.
	startTSB = getTso()
	lockB = s.latches.genLock(startTSB, keysB)
	result = s.latches.acquire(lockB)
	s.Equal(acquireSuccess, result)
}

func (s *testLatchSuite) TestFirstAcquireFailedWithStale() {
	keys := [][]byte{
		[]byte("a"), []byte("b"), []byte("c")}
	_, lockA := s.newLock(keys)
	startTSB, lockB := s.newLock(keys)
	// acquire lockA success
	result := s.latches.acquire(lockA)
	s.Equal(acquireSuccess, result)
	// release lockA
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	s.latches.release(lockA, wakeupList)

	s.Greater(commitTSA, startTSB)
	// acquire lockB first time, should be failed with stale since commitTSA > startTSB
	result = s.latches.acquire(lockB)
	s.Equal(acquireStale, result)
	s.latches.release(lockB, wakeupList)
}

func (s *testLatchSuite) TestRecycle() {
	latches := NewLatches(8)
	now := time.Now()
	startTS := oracle.GoTimeToTS(now)
	lock := latches.genLock(startTS, [][]byte{
		[]byte("a"), []byte("b"),
	})
	lock1 := latches.genLock(startTS, [][]byte{
		[]byte("b"), []byte("c"),
	})
	s.Equal(acquireSuccess, latches.acquire(lock))
	s.Equal(acquireLocked, latches.acquire(lock1))
	lock.SetCommitTS(startTS + 1)
	var wakeupList []*Lock
	latches.release(lock, wakeupList)
	// Release lock will grant latch to lock1 automatically,
	// so release lock1 is called here.
	latches.release(lock1, wakeupList)

	lock2 := latches.genLock(startTS+3, [][]byte{
		[]byte("b"), []byte("c"),
	})
	s.Equal(acquireSuccess, latches.acquire(lock2))
	wakeupList = wakeupList[:0]
	latches.release(lock2, wakeupList)

	allEmpty := true
	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		if latch.queue != nil {
			allEmpty = false
		}
	}
	s.False(allEmpty)

	currentTS := oracle.GoTimeToTS(now.Add(expireDuration)) + 3
	latches.recycle(currentTS)

	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		s.Nil(latch.queue)
	}
}
