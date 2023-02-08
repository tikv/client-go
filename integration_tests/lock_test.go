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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/lock_test.go
//

// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

var getMaxBackoff = tikv.ConfigProbe{}.GetGetMaxBackoff()

func TestLock(t *testing.T) {
	suite.Run(t, new(testLockSuite))
}

type testLockSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testLockSuite) SetupTest() {
	s.store = tikv.StoreProbe{KVStore: NewTestUniStore(s.T())}
}

func (s *testLockSuite) TearDownTest() {
	s.store.Close()
}

func (s *testLockSuite) lockKey(key, value, primaryKey, primaryValue []byte, ttl uint64, commitPrimary bool, asyncCommit bool) (uint64, uint64) {
	txn, err := s.store.Begin()
	s.Nil(err)
	if len(value) > 0 {
		err = txn.Set(key, value)
	} else {
		err = txn.Delete(key)
	}
	s.Nil(err)

	if len(primaryValue) > 0 {
		err = txn.Set(primaryKey, primaryValue)
	} else {
		err = txn.Delete(primaryKey)
	}
	s.Nil(err)
	tpc, err := txn.NewCommitter(0)
	s.Nil(err)
	tpc.SetPrimaryKey(primaryKey)
	tpc.SetLockTTL(ttl)
	if asyncCommit {
		tpc.SetUseAsyncCommit()
	}

	ctx := context.Background()
	err = tpc.PrewriteAllMutations(ctx)
	s.Nil(err)

	if commitPrimary {
		commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
		tpc.SetCommitTS(commitTS)
		err = tpc.CommitMutations(ctx)
		s.Nil(err)
	}
	return txn.StartTS(), tpc.GetCommitTS()
}

func (s *testLockSuite) putAlphabets() {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV([]byte{ch}, []byte{ch})
	}
}

func (s *testLockSuite) putKV(key, value []byte) (uint64, uint64) {
	txn, err := s.store.Begin()
	s.Nil(err)
	err = txn.Set(key, value)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	return txn.StartTS(), txn.GetCommitTS()
}

func (s *testLockSuite) prepareAlphabetLocks() {
	s.putKV([]byte("c"), []byte("cc"))
	s.lockKey([]byte("c"), []byte("c"), []byte("z1"), []byte("z1"), 3000, true, false)
	s.lockKey([]byte("d"), []byte("dd"), []byte("z2"), []byte("z2"), 3000, false, false)
	s.lockKey([]byte("foo"), []byte("foo"), []byte("z3"), []byte("z3"), 3000, false, false)
	s.putKV([]byte("bar"), []byte("bar"))
	s.lockKey([]byte("bar"), nil, []byte("z4"), []byte("z4"), 3000, true, false)
}

func (s *testLockSuite) TestScanLockResolveWithGet() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	txn, err := s.store.Begin()
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		v, err := txn.Get(context.TODO(), []byte{ch})
		s.Nil(err)
		s.Equal(v, []byte{ch})
	}
}

func (s *testLockSuite) TestScanLockResolveWithSeek() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	txn, err := s.store.Begin()
	s.Nil(err)
	iter, err := txn.Iter([]byte("a"), nil)
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.True(iter.Valid())
		s.Equal(iter.Key(), []byte{ch})
		s.Equal(iter.Value(), []byte{ch})
		s.Nil(iter.Next())
	}
}

func (s *testLockSuite) TestScanLockResolveWithSeekKeyOnly() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	txn, err := s.store.Begin()
	s.Nil(err)
	txn.GetSnapshot().SetKeyOnly(true)
	iter, err := txn.Iter([]byte("a"), nil)
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.True(iter.Valid())
		s.Equal(iter.Key(), []byte{ch})
		s.Nil(iter.Next())
	}
}

func (s *testLockSuite) TestScanLockResolveWithBatchGet() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	var keys [][]byte
	for ch := byte('a'); ch <= byte('z'); ch++ {
		keys = append(keys, []byte{ch})
	}

	txn, err := s.store.Begin()
	s.Nil(err)
	m, err := toTiDBTxn(&txn).BatchGet(context.Background(), toTiDBKeys(keys))
	s.Nil(err)
	s.Equal(len(m), int('z'-'a'+1))
	for ch := byte('a'); ch <= byte('z'); ch++ {
		k := []byte{ch}
		s.Equal(m[string(k)], k)
	}
}

func (s *testLockSuite) TestCleanLock() {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		k := []byte{ch}
		s.lockKey(k, k, k, k, 3000, false, false)
	}
	txn, err := s.store.Begin()
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch + 1})
		s.Nil(err)
	}
	err = txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testLockSuite) TestGetTxnStatus() {
	startTS, commitTS := s.putKV([]byte("a"), []byte("a"))
	status, err := s.store.GetLockResolver().GetTxnStatus(startTS, startTS, []byte("a"))
	s.Nil(err)
	s.True(status.IsCommitted())
	s.Equal(status.CommitTS(), commitTS)

	startTS, commitTS = s.lockKey([]byte("a"), []byte("a"), []byte("a"), []byte("a"), 3000, true, false)
	status, err = s.store.GetLockResolver().GetTxnStatus(startTS, startTS, []byte("a"))
	s.Nil(err)
	s.True(status.IsCommitted())
	s.Equal(status.CommitTS(), commitTS)

	startTS, _ = s.lockKey([]byte("a"), []byte("a"), []byte("a"), []byte("a"), 3000, false, false)
	status, err = s.store.GetLockResolver().GetTxnStatus(startTS, startTS, []byte("a"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(0), fmt.Sprintf("action:%s", status.Action()))
}

func (s *testLockSuite) TestCheckTxnStatusTTL() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	s.prewriteTxnWithTTL(txn, 1000)

	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	lr := s.store.NewLockResolver()
	callerStartTS, err := s.store.GetOracle().GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)

	// Check the lock TTL of a transaction.
	status, err := lr.LockResolver.GetTxnStatus(txn.StartTS(), callerStartTS, []byte("key"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))

	// Rollback the txn.
	lock := s.mustGetLock([]byte("key"))

	err = s.store.NewLockResolver().ForceResolveLock(context.Background(), lock)
	s.Nil(err)

	// Check its status is rollbacked.
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), callerStartTS, []byte("key"))
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_NoAction)

	// Check a committed txn.
	startTS, commitTS := s.putKV([]byte("a"), []byte("a"))
	status, err = lr.LockResolver.GetTxnStatus(startTS, callerStartTS, []byte("a"))
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), commitTS)
}

func (s *testLockSuite) TestTxnHeartBeat() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	s.prewriteTxn(txn)

	newTTL, err := s.store.SendTxnHeartbeat(context.Background(), []byte("key"), txn.StartTS(), 6666)
	s.Nil(err)
	s.Equal(newTTL, uint64(6666))

	newTTL, err = s.store.SendTxnHeartbeat(context.Background(), []byte("key"), txn.StartTS(), 5555)
	s.Nil(err)
	s.Equal(newTTL, uint64(6666))

	lock := s.mustGetLock([]byte("key"))
	err = s.store.NewLockResolver().ForceResolveLock(context.Background(), lock)
	s.Nil(err)

	newTTL, err = s.store.SendTxnHeartbeat(context.Background(), []byte("key"), txn.StartTS(), 6666)
	s.NotNil(err)
	s.Equal(newTTL, uint64(0))
}

func (s *testLockSuite) TestCheckTxnStatus() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	txn.Set([]byte("second"), []byte("xxx"))
	s.prewriteTxnWithTTL(txn, 1000)

	o := s.store.GetOracle()
	currentTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	s.Greater(currentTS, txn.StartTS())

	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	resolver := s.store.NewLockResolver()
	// Call getTxnStatus to check the lock status.
	status, err := resolver.GetTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, currentTS, true, false, nil)
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_MinCommitTSPushed)

	// Test the ResolveLocks API
	lock := s.mustGetLock([]byte("second"))
	timeBeforeExpire, err := resolver.ResolveLocks(bo, currentTS, []*txnkv.Lock{lock})
	s.Nil(err)
	s.True(timeBeforeExpire > int64(0))

	// Force rollback the lock using lock.TTL = 0.
	lock.TTL = uint64(0)
	timeBeforeExpire, err = resolver.ResolveLocks(bo, currentTS, []*txnkv.Lock{lock})
	s.Nil(err)
	s.Equal(timeBeforeExpire, int64(0))

	// Then call getTxnStatus again and check the lock status.
	currentTS, err = o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	status, err = s.store.NewLockResolver().GetTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, 0, true, false, nil)
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_NoAction)

	// Call getTxnStatus on a committed transaction.
	startTS, commitTS := s.putKV([]byte("a"), []byte("a"))
	status, err = s.store.NewLockResolver().GetTxnStatus(bo, startTS, []byte("a"), currentTS, currentTS, true, false, nil)
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), commitTS)
}

func (s *testLockSuite) TestCheckTxnStatusNoWait() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	txn.Set([]byte("second"), []byte("xxx"))
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	// Increase lock TTL to make CI more stable.
	committer.SetLockTTLByTimeAndSize(txn.GetStartTime(), 200*1024*1024)

	// Only prewrite the secondary key to simulate a concurrent prewrite case:
	// prewrite secondary regions success and prewrite the primary region is pending.
	err = committer.PrewriteMutations(context.Background(), committer.MutationsOfKeys([][]byte{[]byte("second")}))
	s.Nil(err)

	o := s.store.GetOracle()
	currentTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	resolver := s.store.NewLockResolver()

	// Call getTxnStatus for the TxnNotFound case.
	_, err = resolver.GetTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, currentTS, false, false, nil)
	s.NotNil(err)
	s.True(resolver.IsErrorNotFound(err))

	errCh := make(chan error)
	go func() {
		errCh <- committer.PrewriteMutations(context.Background(), committer.MutationsOfKeys([][]byte{[]byte("key")}))
	}()

	lock := &txnkv.Lock{
		Key:     []byte("second"),
		Primary: []byte("key"),
		TxnID:   txn.StartTS(),
		TTL:     100000,
	}
	// Call getTxnStatusFromLock to cover the retry logic.
	status, err := resolver.GetTxnStatusFromLock(bo, lock, currentTS, false)
	s.Nil(err)
	s.Greater(status.TTL(), uint64(0))
	s.Nil(<-errCh)
	s.Nil(committer.CleanupMutations(context.Background()))

	// Call getTxnStatusFromLock to cover TxnNotFound and retry timeout.
	startTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	lock = &txnkv.Lock{
		Key:     []byte("second"),
		Primary: []byte("key_not_exist"),
		TxnID:   startTS,
		TTL:     1000,
	}
	status, err = resolver.GetTxnStatusFromLock(bo, lock, currentTS, false)
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_LockNotExistRollback)
}

func (s *testLockSuite) prewriteTxn(txn transaction.TxnProbe) {
	s.prewriteTxnWithTTL(txn, 0)
}

func (s *testLockSuite) prewriteTxnWithTTL(txn transaction.TxnProbe, ttl uint64) {
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	if ttl > 0 {
		elapsed := time.Since(txn.GetStartTime()) / time.Millisecond
		committer.SetLockTTL(uint64(elapsed) + ttl)
	}
	err = committer.PrewriteAllMutations(context.Background())
	s.Nil(err)
}

func (s *testLockSuite) mustGetLock(key []byte) *txnkv.Lock {
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	bo := tikv.NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver,
	})
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	s.Nil(err)
	resp, err := s.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutShort)
	s.Nil(err)
	s.NotNil(resp.Resp)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	s.NotNil(keyErr)
	lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
	s.Nil(err)
	return lock
}

func (s *testLockSuite) ttlEquals(x, y uint64) {
	if x < y {
		x, y = y, x
	}
	s.LessOrEqual(x-y, uint64(50))
}

func (s *testLockSuite) TestLockTTL() {
	managedLockTTL := atomic.LoadUint64(&transaction.ManagedLockTTL)
	atomic.StoreUint64(&transaction.ManagedLockTTL, 20000)                // set to 20s
	defer atomic.StoreUint64(&transaction.ManagedLockTTL, managedLockTTL) // restore value

	defaultLockTTL := tikv.ConfigProbe{}.GetDefaultLockTTL()
	ttlFactor := tikv.ConfigProbe{}.GetTTLFactor()

	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	time.Sleep(time.Millisecond)
	s.prewriteTxnWithTTL(txn, 3100)
	l := s.mustGetLock([]byte("key"))
	s.True(l.TTL >= defaultLockTTL)

	// Huge txn has a greater TTL.
	txn, err = s.store.Begin()
	start := time.Now()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	for i := 0; i < 2048; i++ {
		k, v := randKV(1024, 1024)
		txn.Set([]byte(k), []byte(v))
	}
	elapsed := time.Since(start) / time.Millisecond
	s.prewriteTxn(txn)
	l = s.mustGetLock([]byte("key"))
	s.ttlEquals(l.TTL, uint64(ttlFactor*2)+uint64(elapsed))

	// Txn with long read time.
	start = time.Now()
	txn, err = s.store.Begin()
	s.Nil(err)
	time.Sleep(time.Millisecond * 50)
	txn.Set([]byte("key"), []byte("value"))
	elapsed = time.Since(start) / time.Millisecond
	s.prewriteTxn(txn)
	l = s.mustGetLock([]byte("key"))
	s.ttlEquals(l.TTL, defaultLockTTL+uint64(elapsed))
}

func (s *testLockSuite) TestBatchResolveLocks() {
	// The first transaction is a normal transaction with a long TTL
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("k1"), []byte("v1"))
	txn.Set([]byte("k2"), []byte("v2"))
	s.prewriteTxnWithTTL(txn, 20000)

	// The second transaction is an async commit transaction
	txn, err = s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("k3"), []byte("v3"))
	txn.Set([]byte("k4"), []byte("v4"))
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.SetUseAsyncCommit()
	committer.SetLockTTL(20000)
	err = committer.PrewriteAllMutations(context.Background())
	s.Nil(err)

	var locks []*txnkv.Lock
	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		l := s.mustGetLock([]byte(key))
		locks = append(locks, l)
	}

	// Locks may not expired
	msBeforeLockExpired := s.store.GetOracle().UntilExpired(locks[0].TxnID, locks[1].TTL, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Greater(msBeforeLockExpired, int64(0))
	msBeforeLockExpired = s.store.GetOracle().UntilExpired(locks[3].TxnID, locks[3].TTL, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Greater(msBeforeLockExpired, int64(0))

	lr := s.store.NewLockResolver()
	bo := tikv.NewGcResolveLockMaxBackoffer(context.Background())
	loc, err := s.store.GetRegionCache().LocateKey(bo, locks[0].Primary)
	s.Nil(err)
	// Check BatchResolveLocks resolve the lock even the ttl is not expired.
	success, err := lr.BatchResolveLocks(bo, locks, loc.Region)
	s.True(success)
	s.Nil(err)

	txn, err = s.store.Begin()
	s.Nil(err)
	// transaction 1 is rolled back
	_, err = txn.Get(context.Background(), []byte("k1"))
	s.Equal(err, tikverr.ErrNotExist)
	_, err = txn.Get(context.Background(), []byte("k2"))
	s.Equal(err, tikverr.ErrNotExist)
	// transaction 2 is committed
	v, err := txn.Get(context.Background(), []byte("k3"))
	s.Nil(err)
	s.True(bytes.Equal(v, []byte("v3")))
	v, err = txn.Get(context.Background(), []byte("k4"))
	s.Nil(err)
	s.True(bytes.Equal(v, []byte("v4")))
}

func (s *testLockSuite) TestNewLockZeroTTL() {
	l := txnlock.NewLock(&kvrpcpb.LockInfo{})
	s.Equal(l.TTL, uint64(0))
}

func init() {
	// Speed up tests.
	tikv.ConfigProbe{}.SetOracleUpdateInterval(2)
}

func (s *testLockSuite) TestZeroMinCommitTS() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set([]byte("key"), []byte("value"))
	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)

	mockValue := fmt.Sprintf(`return(%d)`, txn.StartTS())
	s.Nil(failpoint.Enable("tikvclient/mockZeroCommitTS", mockValue))
	s.prewriteTxnWithTTL(txn, 1000)
	s.Nil(failpoint.Disable("tikvclient/mockZeroCommitTS"))

	lock := s.mustGetLock([]byte("key"))
	expire, pushed, _, err := s.store.NewLockResolver().ResolveLocksForRead(bo, 0, []*txnkv.Lock{lock}, true)
	s.Nil(err)
	s.Len(pushed, 0)
	s.Greater(expire, int64(0))

	expire, pushed, _, err = s.store.NewLockResolver().ResolveLocksForRead(bo, math.MaxUint64, []*txnkv.Lock{lock}, true)
	s.Nil(err)
	s.Len(pushed, 1)
	s.Equal(expire, int64(0))

	// Clean up this test.
	lock.TTL = uint64(0)
	expire, err = s.store.NewLockResolver().ResolveLocks(bo, 0, []*txnkv.Lock{lock})
	s.Nil(err)
	s.Equal(expire, int64(0))
}

func (s *testLockSuite) prepareTxnFallenBackFromAsyncCommit() {
	txn, err := s.store.Begin()
	s.Nil(err)
	err = txn.Set([]byte("fb1"), []byte("1"))
	s.Nil(err)
	err = txn.Set([]byte("fb2"), []byte("2"))
	s.Nil(err)

	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	s.Equal(committer.GetMutations().Len(), 2)
	committer.SetLockTTL(0)
	committer.SetUseAsyncCommit()
	committer.SetCommitTS(committer.GetStartTS() + (100 << 18)) // 100ms

	err = committer.PrewriteMutations(context.Background(), committer.GetMutations().Slice(0, 1))
	s.Nil(err)
	s.True(committer.IsAsyncCommit())

	// Set an invalid maxCommitTS to produce MaxCommitTsTooLarge
	committer.SetMaxCommitTS(committer.GetStartTS() - 1)
	err = committer.PrewriteMutations(context.Background(), committer.GetMutations().Slice(1, 2))
	s.Nil(err)
	s.False(committer.IsAsyncCommit()) // Fallback due to MaxCommitTsTooLarge
}

func (s *testLockSuite) TestCheckLocksFallenBackFromAsyncCommit() {
	s.prepareTxnFallenBackFromAsyncCommit()

	lock := s.mustGetLock([]byte("fb1"))
	s.True(lock.UseAsyncCommit)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	lr := s.store.NewLockResolver()
	status, err := lr.GetTxnStatusFromLock(bo, lock, 0, false)
	s.Nil(err)
	s.Equal(txnlock.LockProbe{}.GetPrimaryKeyFromTxnStatus(status), []byte("fb1"))

	err = lr.CheckAllSecondaries(bo, lock, &status)
	s.True(lr.IsNonAsyncCommitLock(err))

	status, err = lr.GetTxnStatusFromLock(bo, lock, 0, true)
	s.Nil(err)
	s.Equal(status.Action(), kvrpcpb.Action_TTLExpireRollback)
	s.Equal(status.TTL(), uint64(0))
}

func (s *testLockSuite) TestResolveTxnFallenBackFromAsyncCommit() {
	s.prepareTxnFallenBackFromAsyncCommit()

	lock := s.mustGetLock([]byte("fb1"))
	s.True(lock.UseAsyncCommit)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	expire, err := s.store.NewLockResolver().ResolveLocks(bo, 0, []*txnkv.Lock{lock})
	s.Nil(err)
	s.Equal(expire, int64(0))

	t3, err := s.store.Begin()
	s.Nil(err)
	_, err = t3.Get(context.Background(), []byte("fb1"))
	s.True(tikverr.IsErrNotFound(err))
	_, err = t3.Get(context.Background(), []byte("fb2"))
	s.True(tikverr.IsErrNotFound(err))
}

func (s *testLockSuite) TestBatchResolveTxnFallenBackFromAsyncCommit() {
	s.prepareTxnFallenBackFromAsyncCommit()

	lock := s.mustGetLock([]byte("fb1"))
	s.True(lock.UseAsyncCommit)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte("fb1"))
	s.Nil(err)
	ok, err := s.store.NewLockResolver().BatchResolveLocks(bo, []*txnkv.Lock{lock}, loc.Region)
	s.Nil(err)
	s.True(ok)

	t3, err := s.store.Begin()
	s.Nil(err)
	_, err = t3.Get(context.Background(), []byte("fb1"))
	s.True(tikverr.IsErrNotFound(err))
	_, err = t3.Get(context.Background(), []byte("fb2"))
	s.True(tikverr.IsErrNotFound(err))
}

func (s *testLockSuite) TestDeadlockReportWaitChain() {
	// Utilities to make the test logic clear and simple.
	type txnWrapper struct {
		transaction.TxnProbe
		wg sync.WaitGroup
	}

	makeLockCtx := func(txn *txnWrapper, resourceGroupTag string) *kv.LockCtx {
		lockctx := kv.NewLockCtx(txn.StartTS(), 1000, time.Now())
		lockctx.ResourceGroupTag = []byte(resourceGroupTag)
		return lockctx
	}

	// Prepares several transactions and each locks a key.
	prepareTxns := func(num int) []*txnWrapper {
		res := make([]*txnWrapper, 0, num)
		for i := 0; i < num; i++ {
			txnProbe, err := s.store.Begin()
			s.Nil(err)
			txn := &txnWrapper{TxnProbe: txnProbe}
			txn.SetPessimistic(true)
			tag := fmt.Sprintf("tag-init%v", i)
			key := []byte{'k', byte(i)}
			err = txn.LockKeys(context.Background(), makeLockCtx(txn, tag), key)
			s.Nil(err)

			res = append(res, txn)
		}
		return res
	}

	// Let the i-th trnasaction lock the key that has been locked by j-th transaction
	tryLock := func(txns []*txnWrapper, i int, j int) error {
		s.T().Logf("txn %v try locking %v", i, j)
		txn := txns[i]
		tag := fmt.Sprintf("tag-%v-%v", i, j)
		key := []byte{'k', byte(j)}
		return txn.LockKeys(context.Background(), makeLockCtx(txn, tag), key)
	}

	// Asserts the i-th transaction waits for the j-th transaction.
	makeWaitFor := func(txns []*txnWrapper, i int, j int) {
		txns[i].wg.Add(1)
		go func() {
			defer txns[i].wg.Done()
			err := tryLock(txns, i, j)
			// After the lock being waited for is released, the transaction returns a WriteConflict error
			// unconditionally, which is by design.
			s.NotNil(err)
			s.T().Logf("txn %v wait for %v finished, err: %s", i, j, err.Error())
			_, ok := errors.Cause(err).(*tikverr.ErrWriteConflict)
			s.True(ok)
		}()
	}

	waitAndRollback := func(txns []*txnWrapper, i int) {
		// It's expected that each transaction should be rolled back after its blocker, so that `Rollback` will not
		// run when there's concurrent `LockKeys` running.
		// If it's blocked on the `Wait` forever, it means the transaction's blocker is not rolled back.
		s.T().Logf("rollback txn %v", i)
		txns[i].wg.Wait()
		err := txns[i].Rollback()
		s.Nil(err)
	}

	// Check the given WaitForEntry is caused by txn[i] waiting for txn[j].
	checkWaitChainEntry := func(txns []*txnWrapper, entry *deadlockpb.WaitForEntry, i, j int) {
		s.Equal(entry.Txn, txns[i].StartTS())
		s.Equal(entry.WaitForTxn, txns[j].StartTS())
		s.Equal(entry.Key, []byte{'k', byte(j)})
		s.Equal(string(entry.ResourceGroupTag), fmt.Sprintf("tag-%v-%v", i, j))
	}

	s.T().Log("test case 1: 1->0->1")

	txns := prepareTxns(2)

	makeWaitFor(txns, 0, 1)
	// Sleep for a while to make sure it has been blocked.
	time.Sleep(time.Millisecond * 100)

	// txn2 tries locking k1 and encounters deadlock error.
	err := tryLock(txns, 1, 0)
	s.NotNil(err)
	dl, ok := errors.Cause(err).(*tikverr.ErrDeadlock)
	s.True(ok)

	waitChain := dl.GetWaitChain()
	s.Equal(len(waitChain), 2)
	checkWaitChainEntry(txns, waitChain[0], 0, 1)
	checkWaitChainEntry(txns, waitChain[1], 1, 0)

	// Each transaction should be rolled back after its blocker being rolled back
	waitAndRollback(txns, 1)
	waitAndRollback(txns, 0)

	s.T().Log("test case 2: 3->2->0->1->3")
	txns = prepareTxns(4)

	makeWaitFor(txns, 0, 1)
	makeWaitFor(txns, 2, 0)
	makeWaitFor(txns, 1, 3)
	// Sleep for a while to make sure it has been blocked.
	time.Sleep(time.Millisecond * 100)

	err = tryLock(txns, 3, 2)
	s.NotNil(err)
	dl, ok = errors.Cause(err).(*tikverr.ErrDeadlock)
	s.True(ok)

	waitChain = dl.GetWaitChain()
	s.Equal(len(waitChain), 4)
	s.T().Logf("wait chain: \n** %v\n**%v\n**%v\n**%v\n", waitChain[0], waitChain[1], waitChain[2], waitChain[3])
	checkWaitChainEntry(txns, waitChain[0], 2, 0)
	checkWaitChainEntry(txns, waitChain[1], 0, 1)
	checkWaitChainEntry(txns, waitChain[2], 1, 3)
	checkWaitChainEntry(txns, waitChain[3], 3, 2)

	// Each transaction should be rolled back after its blocker being rolled back
	waitAndRollback(txns, 3)
	waitAndRollback(txns, 1)
	waitAndRollback(txns, 0)
	waitAndRollback(txns, 2)
}

func (s *testLockSuite) TestStartHeartBeatAfterLockingPrimary() {
	atomic.StoreUint64(&transaction.ManagedLockTTL, 500)
	s.Nil(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", `return`))
	s.Nil(failpoint.Enable("tikvclient/afterPrimaryBatch", `pause`))
	defer func() {
		atomic.StoreUint64(&transaction.ManagedLockTTL, 20000)
		s.Nil(failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit"))
	}()

	txn, err := s.store.Begin()
	s.Nil(err)
	txn.SetPessimistic(true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	lockResCh := make(chan error)
	go func() {
		err = txn.LockKeys(context.Background(), lockCtx, []byte("a"), []byte("b"))
		lockResCh <- err
	}()

	time.Sleep(500 * time.Millisecond)

	// Check the TTL should have been updated
	lr := s.store.NewLockResolver()
	status, err := lr.LockResolver.GetTxnStatus(txn.StartTS(), 0, []byte("a"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(600))
	s.Equal(status.CommitTS(), uint64(0))

	// Let locking the secondary key fail
	s.Nil(failpoint.Enable("tikvclient/PessimisticLockErrWriteConflict", "return"))
	s.Nil(failpoint.Disable("tikvclient/afterPrimaryBatch"))
	s.Error(<-lockResCh)
	s.Nil(failpoint.Disable("tikvclient/PessimisticLockErrWriteConflict"))

	err = txn.LockKeys(context.Background(), lockCtx, []byte("c"), []byte("d"))
	s.Nil(err)

	time.Sleep(500 * time.Millisecond)

	// The original primary key "a" should be rolled back because its TTL is not updated
	lr = s.store.NewLockResolver()
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), 0, []byte("a"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Equal(status.TTL(), uint64(0))

	// The TTL of the new primary lock should be updated.
	lr = s.store.NewLockResolver()
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), 0, []byte("c"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(1200))
	s.Equal(status.CommitTS(), uint64(0))

	s.Nil(txn.Rollback())
}

func (s *testLockSuite) TestPrewriteEncountersLargerTsLock() {
	t1, err := s.store.Begin()
	s.Nil(err)
	s.Nil(t1.Set([]byte("k1"), []byte("v1")))
	s.Nil(t1.Set([]byte("k2"), []byte("v2")))

	// t2 has larger TS. Let t2 prewrite only the secondary lock.
	t2, err := s.store.Begin()
	s.Nil(err)
	s.Nil(t2.Set([]byte("k1"), []byte("v1")))
	s.Nil(t2.Set([]byte("k2"), []byte("v2")))
	committer, err := t2.NewCommitter(1)
	s.Nil(err)
	committer.SetLockTTL(20000) // set TTL to 20s

	s.Nil(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", "return"))
	defer failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit")
	s.Nil(failpoint.Enable("tikvclient/prewritePrimary", "pause"))
	ch := make(chan struct{})
	go func() {
		err = committer.PrewriteAllMutations(context.Background())
		s.Nil(err)
		ch <- struct{}{}
	}()
	time.Sleep(200 * time.Millisecond) // make prewrite earlier than t1 commits

	// Set 1 second timeout. If we still need to wait until t2 expires, we will get a timeout error
	// instead of write conflict.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = t1.Commit(ctx)
	s.True(tikverr.IsErrWriteConflict(err))

	s.Nil(failpoint.Disable("tikvclient/prewritePrimary"))
	<-ch
}

func (s *testLockSuite) TestResolveLocksForRead() {
	ctx := context.Background()
	var resolvedLocks, committedLocks []uint64
	var locks []*txnlock.Lock

	// commitTS < readStartTS
	startTS, _ := s.lockKey([]byte("k1"), []byte("v1"), []byte("k11"), []byte("v11"), 3000, true, false)
	committedLocks = append(committedLocks, startTS)
	lock := s.mustGetLock([]byte("k1"))
	locks = append(locks, lock)

	// rolled back
	startTS, _ = s.lockKey([]byte("k2"), []byte("v2"), []byte("k22"), []byte("v22"), 3000, false, false)
	lock = s.mustGetLock([]byte("k22"))
	err := s.store.NewLockResolver().ForceResolveLock(ctx, lock)
	s.Nil(err)
	resolvedLocks = append(resolvedLocks, startTS)
	lock = s.mustGetLock([]byte("k2"))
	locks = append(locks, lock)

	// pushed
	startTS, _ = s.lockKey([]byte("k3"), []byte("v3"), []byte("k33"), []byte("v33"), 3000, false, false)
	resolvedLocks = append(resolvedLocks, startTS)
	lock = s.mustGetLock([]byte("k3"))
	locks = append(locks, lock)

	// can't be pushed and isn't expired
	_, _ = s.lockKey([]byte("k4"), []byte("v4"), []byte("k44"), []byte("v44"), 3000, false, true)
	lock = s.mustGetLock([]byte("k4"))
	locks = append(locks, lock)

	// can't be pushed but is expired
	startTS, _ = s.lockKey([]byte("k5"), []byte("v5"), []byte("k55"), []byte("v55"), 0, false, true)
	committedLocks = append(committedLocks, startTS)
	lock = s.mustGetLock([]byte("k5"))
	locks = append(locks, lock)

	// commitTS > readStartTS
	var readStartTS uint64
	{
		t, err := s.store.Begin()
		s.Nil(err)
		resolvedLocks = append(resolvedLocks, t.StartTS())
		s.Nil(t.Set([]byte("k6"), []byte("v6")))
		s.Nil(t.Set([]byte("k66"), []byte("v66")))
		committer, err := t.NewCommitter(1)
		s.Nil(err)
		s.Nil(committer.PrewriteAllMutations(ctx))
		committer.SetPrimaryKey([]byte("k66"))

		readStartTS, err = s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)

		commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
		s.Greater(commitTS, readStartTS)
		committer.SetCommitTS(commitTS)
		err = committer.CommitMutations(ctx)
		s.Nil(err)
		lock = s.mustGetLock([]byte("k6"))
		locks = append(locks, lock)
	}

	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	lr := s.store.NewLockResolver()
	defer lr.Close()
	msBeforeExpired, resolved, committed, err := lr.ResolveLocksForRead(bo, readStartTS, locks, false)
	s.Nil(err)
	s.Greater(msBeforeExpired, int64(0))
	s.Equal(resolvedLocks, resolved)
	s.Equal(committedLocks, committed)
}

func (s *testLockSuite) TestLockWaitTimeLimit() {
	k1 := []byte("k1")
	k2 := []byte("k2")

	txn1, err := s.store.Begin()
	s.Nil(err)
	txn1.SetPessimistic(true)

	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k1, k2)
	s.Nil(err)

	txn2, err := s.store.Begin()
	s.Nil(err)
	txn2.SetPessimistic(true)

	// test no wait
	lockCtx = kv.NewLockCtx(txn2.StartTS(), kv.LockNoWait, time.Now())
	err = txn2.LockKeys(context.Background(), lockCtx, k1)
	// cannot acquire lock immediately thus error
	s.Equal(tikverr.ErrLockAcquireFailAndNoWaitSet.Error(), err.Error())
	// Default wait time is 1s, we use 500ms as an upper bound
	s.Less(time.Since(lockCtx.WaitStartTime), 500*time.Millisecond)

	// test for wait limited time (200ms)
	lockCtx = kv.NewLockCtx(txn2.StartTS(), 200, time.Now())
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire lock in time thus error
	s.Equal(tikverr.ErrLockWaitTimeout.Error(), err.Error())
	s.GreaterOrEqual(time.Since(lockCtx.WaitStartTime), 200*time.Millisecond)
	s.Less(time.Since(lockCtx.WaitStartTime), 800*time.Millisecond)

	s.Nil(txn1.Rollback())
	s.Nil(txn2.Rollback())
}
