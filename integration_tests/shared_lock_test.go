// Copyright 2025 TiKV Authors
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

package tikv_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func TestSharedLock(t *testing.T) {
	if !*withTiKV {
		t.Skip("skipping TestSharedLock because with-tikv is not enabled")
		return
	}
	if config.NextGen {
		t.Skip("skipping TestSharedLock because next-gen doesn't support shared lock yet")
		return
	}
	suite.Run(t, new(testSharedLockSuite))
}

type testSharedLockSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testSharedLockSuite) SetupSuite() {
	atomic.StoreUint64(&transaction.ManagedLockTTL, 3000) // 3s
	atomic.StoreUint64(&transaction.CommitMaxBackoff, 1000)
	s.Nil(failpoint.Enable("tikvclient/injectLiveness", `return("reachable")`))
}

func (s *testSharedLockSuite) TearDownSuite() {
	s.Nil(failpoint.Disable("tikvclient/injectLiveness"))
	atomic.StoreUint64(&transaction.ManagedLockTTL, 20000)
	atomic.StoreUint64(&transaction.CommitMaxBackoff, 40000)
}

func (s *testSharedLockSuite) SetupTest() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
}

func (s *testSharedLockSuite) TearDownTest() {
	s.store.Close()
}

func (s *testSharedLockSuite) begin() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	txn.SetPessimistic(true)
	return txn
}

func (s *testSharedLockSuite) getTS() uint64 {
	ts, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	s.Nil(err)
	return ts
}

func (s *testSharedLockSuite) scanLocks(key []byte, maxTS uint64) []*txnlock.Lock {
	locks, err := s.store.ScanLocks(context.Background(), key, append(key, 0), maxTS)
	s.Nil(err)
	if len(locks) == 0 {
		return nil
	}
	return locks
}

func (s *testSharedLockSuite) TestSharedLockBlockExclusiveLock() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := []byte("TestSharedLockBlockExclusiveLock_pk1")
		pk2 := []byte("TestSharedLockBlockExclusiveLock_pk2")
		pk3 := []byte("TestSharedLockBlockExclusiveLock_pk3")
		key := []byte("TestSharedLockBlockExclusiveLock_shared_key")

		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk1))
		s.Equal(txn1.GetCommitter().GetPrimaryKey(), pk1)
		lockctx1 := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockctx1.InShareMode = true
		s.Nil(txn1.LockKeys(context.Background(), lockctx1, key))

		s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk2))
		s.Equal(txn2.GetCommitter().GetPrimaryKey(), pk2)
		lockctx2 := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockctx2.InShareMode = true
		s.Nil(txn2.LockKeys(context.Background(), lockctx2, key))

		flags, err := txn2.GetMemBuffer().GetFlags(key)
		s.Nil(err)
		s.True(flags.HasLockedInShareMode())

		s.Nil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk3))
		s.Equal(txn3.GetCommitter().GetPrimaryKey(), pk3)
		lockDone := make(chan time.Time)
		go func() {
			s.NotNil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), key)) // should block and return conflict
			lockDone <- time.Now()
		}()

		time.Sleep(500 * time.Millisecond)
		beforeRelease := time.Now()

		if commit {
			s.Nil(txn1.Commit(context.Background()))
			s.Nil(txn2.Commit(context.Background()))
		} else {
			s.Nil(txn1.Rollback())
			s.Nil(txn2.Rollback())
		}

		afterRelease := <-lockDone
		s.True(afterRelease.After(beforeRelease), "txn3(exclusive lock) should block until txn1(shared lock) and txn2(shared lock) commit")
		s.Nil(txn3.Rollback())
	}
}

func (s *testSharedLockSuite) TestExclusiveLockBlockSharedLock() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := []byte("TestExclusiveLockBlockSharedLock_pk1")
		pk2 := []byte("TestExclusiveLockBlockSharedLock_pk2")
		pk3 := []byte("TestExclusiveLockBlockSharedLock_pk3")
		key := []byte("TestExclusiveLockBlockSharedLock_shared_key")

		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk1))
		s.Equal(txn1.GetCommitter().GetPrimaryKey(), pk1)
		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), key))

		s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk2))
		s.Equal(txn2.GetCommitter().GetPrimaryKey(), pk2)
		s.Nil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk3))
		s.Equal(txn3.GetCommitter().GetPrimaryKey(), pk3)

		txn2LockDone := make(chan time.Time)
		go func() {
			lockctx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			lockctx.InShareMode = true
			s.NotNil(txn2.LockKeys(context.Background(), lockctx, key)) // should block and return conflict
			txn2LockDone <- time.Now()
		}()
		txn3LockDone := make(chan time.Time)
		go func() {
			lockctx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			lockctx.InShareMode = true
			s.NotNil(txn3.LockKeys(context.Background(), lockctx, key)) // should block and return conflict
			txn3LockDone <- time.Now()
		}()

		time.Sleep(500 * time.Millisecond)
		beforeRelease := time.Now()

		if commit {
			s.Nil(txn1.Commit(context.Background()))
		} else {
			s.Nil(txn1.Rollback())
		}

		txn2Locked := <-txn2LockDone
		txn3Locked := <-txn3LockDone
		s.True(txn2Locked.After(beforeRelease), "txn2(shared lock) should block until txn1(exclusive lock) commit/rollback")
		s.True(txn3Locked.After(beforeRelease), "txn3(shared lock) should block until txn1(exclusive lock) commit/rollback")
		s.Nil(txn2.Rollback())
		s.Nil(txn3.Rollback())
	}
}

func (s *testSharedLockSuite) TestResolveSharedLock() {
	txn1 := s.begin()

	pk := []byte("TestResolveSharedLock_pk")
	key := []byte("TestResolveSharedLock_shared_key")
	_, err := s.store.SplitRegions(context.Background(), [][]byte{pk, key}, false, nil)
	s.Nil(err)

	s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn1.GetCommitter().GetPrimaryKey())
	lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	lockCtx.InShareMode = true
	s.Nil(txn1.LockKeys(context.Background(), lockCtx, key))

	s.Nil(failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	txn1.SetSessionID(1)
	s.Nil(txn1.Commit(context.Background()))

	locks := s.scanLocks(key, s.getTS())
	s.Len(locks, 1)
	lock := locks[0]
	s.Equal(key, lock.Key)
	s.Equal(pk, lock.Primary)

	s.Equal(txn1.StartTS(), lock.TxnID)
	s.Equal(lock.LockType, kvrpcpb.Op_Lock)

	txn2 := s.begin()
	s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn2.GetCommitter().GetPrimaryKey())
	s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), key))

	locks = s.scanLocks(key, s.getTS())
	s.Len(locks, 1)
	lock = locks[0]
	s.NotNil(lock)
	s.Equal(key, lock.Key)
	s.Equal(pk, lock.Primary)
	s.Equal(txn2.StartTS(), lock.TxnID)
	s.Equal(lock.LockType, kvrpcpb.Op_PessimisticLock)

	s.Nil(txn2.Rollback())
	s.Len(s.scanLocks(key, s.getTS()), 0)
	s.Nil(failpoint.Disable("tikvclient/beforeCommitSecondaries"))
}

func (s *testSharedLockSuite) TestScanSharedLock() {
	pk1 := []byte("TestScanSharedLock_pk_1")
	pk2 := []byte("TestScanSharedLock_pk_2")
	pk3 := []byte("TestScanSharedLock_pk_3")
	sharedKey := []byte("TestScanSharedLock_shared_key")
	txn1 := s.begin()
	txn2 := s.begin()
	txn3 := s.begin()

	pks := [][]byte{pk1, pk2, pk3}
	txns := []transaction.TxnProbe{txn1, txn2, txn3}

	for i, txn := range txns {
		s.Nil(txn.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pks[i]))
		s.Equal(pks[i], txn.GetCommitter().GetPrimaryKey())
		lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockCtx.InShareMode = true
		s.Nil(txn.LockKeys(context.Background(), lockCtx, sharedKey))
	}

	maxTS2LockNum := map[uint64]int{
		txn1.StartTS() - 1: 0,
		txn1.StartTS():     1,
		txn2.StartTS():     2,
		txn3.StartTS():     3,
	}
	for maxTS, lockNum := range maxTS2LockNum {
		locks := s.scanLocks(sharedKey, maxTS)
		s.Equal(len(locks), lockNum, fmt.Sprintf("when maxTS=%d, expect %d locks", maxTS, lockNum))
		for _, lock := range locks {
			s.Equal(sharedKey, lock.Key)
			s.LessOrEqual(lock.TxnID, maxTS)
		}
	}

	for _, txn := range txns {
		s.Nil(txn.Rollback())
	}
	locks, err := s.store.ScanLocks(context.Background(), sharedKey, append(sharedKey, 0), txn3.StartTS())
	s.Nil(err)
	s.Equal(len(locks), 0, "no locks after rollback")
}

func (s *testSharedLockSuite) TestGCSharedLock() {
	originManagedLockTTL := atomic.LoadUint64(&transaction.ManagedLockTTL)
	atomic.StoreUint64(&transaction.ManagedLockTTL, 1000) // 1000ms, increased for test stability in busy environments
	defer atomic.StoreUint64(&transaction.ManagedLockTTL, originManagedLockTTL)

	txn1 := s.begin()
	txn2 := s.begin()
	txn3 := s.begin()
	pk1 := []byte("TestGCSharedLock_pk1")
	pk2 := []byte("TestGCSharedLock_pk2")
	pk3 := []byte("TestGCSharedLock_pk3")
	sharedKey := []byte("TestGCSharedLock_shared_key")

	pks := [][]byte{pk1, pk2, pk3}
	txns := []transaction.TxnProbe{txn1, txn2, txn3}

	for i, txn := range txns {
		s.Nil(txn.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pks[i]))
		s.Equal(pks[i], txn.GetCommitter().GetPrimaryKey())
		lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockCtx.InShareMode = true
		s.Nil(txn.LockKeys(context.Background(), lockCtx, sharedKey))
	}
	// keep heartbeat for txn3 only
	txn1.GetCommitter().CloseTTLManager()
	txn2.GetCommitter().CloseTTLManager()

	// Verify txn3's TTL manager is still running
	s.True(txn3.GetCommitter().IsTTLRunning(), "txn3's TTL manager should be running")

	var locks []*txnlock.Lock
	s.Eventually(func() bool {
		locks = s.scanLocks(sharedKey, s.getTS())
		return len(locks) == 3
	}, 5*time.Second, 100*time.Millisecond, "expect 3 locks after pipelined locks being applied")

	s.Len(locks, 3)
	for _, lock := range locks {
		s.Equal(sharedKey, lock.Key)
		s.Equal(lock.LockType, kvrpcpb.Op_PessimisticLock)
	}

	// wait managed lock ttl to expire for txn1 and txn2
	time.Sleep(time.Duration(atomic.LoadUint64(&transaction.ManagedLockTTL))*time.Millisecond + 200*time.Millisecond)

	// Verify txn3's TTL manager is still running after the sleep
	s.True(txn3.GetCommitter().IsTTLRunning(), "txn3's TTL manager should still be running after sleep")

	lr := s.store.NewLockResolver()
	bo := tikv.NewGcResolveLockMaxBackoffer(context.Background())
	ttl, err := lr.ResolveLocks(bo, 0, locks)
	s.Nil(err)
	s.Zero(ttl)

	locks = s.scanLocks(sharedKey, s.getTS())
	s.Len(locks, 1, "expected 1 lock (txn3's) to remain, but got %d; txn3 TTL running: %v", len(locks), txn3.GetCommitter().IsTTLRunning())
	s.Equal(txn3.StartTS(), locks[0].TxnID)
	s.Equal(sharedKey, locks[0].Key)
	s.Nil(txn3.Rollback())
}

func (s *testSharedLockSuite) TestSharedLockCommitAndRollback() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := []byte("TestSharedLockCommitAndRollback_pk1")
		pk2 := []byte("TestSharedLockCommitAndRollback_pk2")
		pk3 := []byte("TestSharedLockCommitAndRollback_pk3")
		key := []byte("TestSharedLockCommitAndRollback_shared_key")

		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk1))
		s.Equal(txn1.GetCommitter().GetPrimaryKey(), pk1)
		s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk2))
		s.Equal(txn2.GetCommitter().GetPrimaryKey(), pk2)
		s.Nil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk3))
		s.Equal(txn3.GetCommitter().GetPrimaryKey(), pk3)

		for _, txn := range []transaction.TxnProbe{txn1, txn2, txn3} {
			lockctx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			lockctx.InShareMode = true
			s.Nil(txn.LockKeys(context.Background(), lockctx, key))
		}

		var locks []*txnlock.Lock
		s.Eventually(func() bool {
			locks = s.scanLocks(key, s.getTS())
			return len(locks) == 3
		}, 5*time.Second, 100*time.Millisecond, "expect 3 locks after pipelined locks being applied")
		s.Len(locks, 3)

		for i, txn := range []transaction.TxnProbe{txn1, txn2, txn3} {
			if commit {
				s.Nil(txn.Commit(context.Background()))
			} else {
				s.Nil(txn.Rollback())
			}

			currLocks := 3 - i
			s.Eventually(func() bool {
				locks = s.scanLocks(key, s.getTS())
				s.LessOrEqual(len(locks), currLocks)
				if len(locks) == currLocks {
					return false
				}
				return len(locks) == currLocks-1
			}, 5*time.Second, 100*time.Millisecond, "after txn %d commit/rollback, expect %d locks remain", i+1, currLocks-1)

			for _, lock := range locks {
				s.Equal(key, lock.Key)
				s.NotEqual(lock.TxnID, txn.StartTS())
			}
		}
		locks = s.scanLocks(key, s.getTS())
		s.Len(locks, 0)
	}
}

func (s *testSharedLockSuite) TestPrewriteResolveExpiredSharedLock() {
	// Set short ManagedLockTTL so locks expire quickly
	originManagedLockTTL := atomic.LoadUint64(&transaction.ManagedLockTTL)
	atomic.StoreUint64(&transaction.ManagedLockTTL, 500) // 500ms, increased for test stability
	defer atomic.StoreUint64(&transaction.ManagedLockTTL, originManagedLockTTL)

	pk := []byte("TestPrewriteResolveExpiredSharedLock_pk")
	key := []byte("TestPrewriteResolveExpiredSharedLock_key")

	// Step 1: Create a pessimistic transaction with shared lock
	txn1 := s.begin()
	s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn1.GetCommitter().GetPrimaryKey())

	lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	lockCtx.InShareMode = true
	s.Nil(txn1.LockKeys(context.Background(), lockCtx, key))

	// Wait for the shared lock to be visible
	s.Eventually(func() bool {
		locks := s.scanLocks(key, s.getTS())
		return len(locks) == 1
	}, 5*time.Second, 100*time.Millisecond, "expect 1 shared lock")

	// Step 2: Close TTL manager and wait for lock to expire
	txn1.GetCommitter().CloseTTLManager()
	time.Sleep(time.Duration(atomic.LoadUint64(&transaction.ManagedLockTTL))*time.Millisecond + 200*time.Millisecond)

	// Verify the shared lock still exists (but is expired)
	locks := s.scanLocks(key, s.getTS())
	s.Len(locks, 1)
	s.Equal(key, locks[0].Key)

	// Step 3: Create an optimistic transaction to write to the same key
	txn2, err := s.store.Begin()
	s.Nil(err)
	txn2.SetPessimistic(false) // Make it optimistic

	value := []byte("value_from_txn2")
	s.Nil(txn2.Set(key, value))

	// Step 4: Commit should succeed after resolving the expired shared lock
	// This exercises the extractKeyErrs -> GetSharedLockInfos() code path
	err = txn2.Commit(context.Background())
	s.Nil(err, "optimistic transaction should successfully resolve expired shared lock and commit")

	// Step 5: Verify the write succeeded
	snapshot := s.store.GetSnapshot(txn2.CommitTS())
	v, err := snapshot.Get(context.Background(), key)
	s.Nil(err)
	s.Equal(value, v.Value)

	// Step 6: Verify the shared lock is gone
	locks = s.scanLocks(key, s.getTS())
	s.Len(locks, 0, "shared lock should have been resolved")

	// Cleanup
	s.Nil(txn1.Rollback())
}
