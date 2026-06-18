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
	stderrs "errors"
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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
)

var getMaxBackoff = tikv.ConfigProbe{}.GetGetMaxBackoff()

func TestLock(t *testing.T) {
	suite.Run(t, new(testLockSuite))
}

func TestLockWithTiKV(t *testing.T) {
	suite.Run(t, new(testLockWithTiKVSuite))
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

func (s *testLockSuite) key(name string) []byte {
	return encodeKey("~lock", name)
}

func (s *testLockSuite) alphabetKey(ch byte) []byte {
	return s.key(string(ch))
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
		s.putKV(s.alphabetKey(ch), []byte{ch})
	}
}

func (s *testLockSuite) putKV(key, value []byte) (uint64, uint64) {
	txn, err := s.store.Begin()
	s.Nil(err)
	err = txn.Set(key, value)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	return txn.StartTS(), txn.CommitTS()
}

func (s *testLockSuite) prepareAlphabetLocks() {
	s.putKV(s.key("c"), []byte("cc"))
	s.lockKey(s.key("c"), []byte("c"), s.key("z1"), []byte("z1"), 3000, true, false)
	s.lockKey(s.key("d"), []byte("dd"), s.key("z2"), []byte("z2"), 3000, false, false)
	s.lockKey(s.key("foo"), []byte("foo"), s.key("z3"), []byte("z3"), 3000, false, false)
	s.putKV(s.key("bar"), []byte("bar"))
	s.lockKey(s.key("bar"), nil, s.key("z4"), []byte("z4"), 3000, true, false)
}

func (s *testLockSuite) TestScanLockResolveWithGet() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	txn, err := s.store.Begin()
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		v, err := txn.Get(context.TODO(), s.alphabetKey(ch))
		s.Nil(err)
		s.Equal(v.Value, []byte{ch})
	}
}

func (s *testLockSuite) TestScanLockResolveWithSeek() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	txn, err := s.store.Begin()
	s.Nil(err)
	iter, err := txn.Iter(s.key("a"), nil)
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.True(iter.Valid())
		s.Equal(iter.Key(), s.alphabetKey(ch))
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
	iter, err := txn.Iter(s.key("a"), nil)
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.True(iter.Valid())
		s.Equal(iter.Key(), s.alphabetKey(ch))
		s.Nil(iter.Next())
	}
}

func (s *testLockSuite) TestScanLockResolveWithBatchGet() {
	s.putAlphabets()
	s.prepareAlphabetLocks()

	var keys [][]byte
	for ch := byte('a'); ch <= byte('z'); ch++ {
		keys = append(keys, s.alphabetKey(ch))
	}

	txn, err := s.store.Begin()
	s.Nil(err)
	m, err := toTiDBTxn(&txn).BatchGet(context.Background(), toTiDBKeys(keys))
	s.Nil(err)
	s.Equal(len(m), int('z'-'a'+1))
	for ch := byte('a'); ch <= byte('z'); ch++ {
		k := s.alphabetKey(ch)
		s.Equal(m[string(k)].Value, []byte{ch})
	}
}

func (s *testLockSuite) TestCleanLock() {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		k := s.alphabetKey(ch)
		s.lockKey(k, k, k, k, 3000, false, false)
	}
	txn, err := s.store.Begin()
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set(s.alphabetKey(ch), []byte{ch + 1})
		s.Nil(err)
	}
	err = txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testLockSuite) TestGetTxnStatus() {
	startTS, commitTS := s.putKV(s.key("a"), []byte("a"))
	status, err := s.store.GetLockResolver().GetTxnStatus(startTS, startTS, s.key("a"))
	s.Nil(err)
	s.True(status.IsCommitted())
	s.Equal(status.CommitTS(), commitTS)

	startTS, commitTS = s.lockKey(s.key("a"), []byte("a"), s.key("a"), []byte("a"), 3000, true, false)
	status, err = s.store.GetLockResolver().GetTxnStatus(startTS, startTS, s.key("a"))
	s.Nil(err)
	s.True(status.IsCommitted())
	s.Equal(status.CommitTS(), commitTS)

	startTS, _ = s.lockKey(s.key("a"), []byte("a"), s.key("a"), []byte("a"), 3000, false, false)
	status, err = s.store.GetLockResolver().GetTxnStatus(startTS, startTS, s.key("a"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(0), fmt.Sprintf("action:%s", status.Action()))
}

func (s *testLockSuite) TestCheckTxnStatusTTL() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set(s.key("key"), []byte("value"))
	s.prewriteTxnWithTTL(txn, 1000)

	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	lr := s.store.NewLockResolver()
	callerStartTS, err := s.store.GetOracle().GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)

	// Check the lock TTL of a transaction.
	status, err := lr.LockResolver.GetTxnStatus(txn.StartTS(), callerStartTS, s.key("key"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))

	// Rollback the txn.
	lock := s.mustGetLock(s.key("key"))

	err = s.store.NewLockResolver().ForceResolveLock(context.Background(), lock)
	s.Nil(err)

	// Check its status is rollbacked.
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), callerStartTS, s.key("key"))
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_NoAction)

	// Check a committed txn.
	startTS, commitTS := s.putKV(s.key("a"), []byte("a"))
	status, err = lr.LockResolver.GetTxnStatus(startTS, callerStartTS, s.key("a"))
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), commitTS)
}

func (s *testLockSuite) TestTxnHeartBeat() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set(s.key("key"), []byte("value"))
	s.prewriteTxn(txn)

	newTTL, err := s.store.SendTxnHeartbeat(context.Background(), s.key("key"), txn.StartTS(), 6666)
	s.Nil(err)
	s.Equal(newTTL, uint64(6666))

	newTTL, err = s.store.SendTxnHeartbeat(context.Background(), s.key("key"), txn.StartTS(), 5555)
	s.Nil(err)
	s.Equal(newTTL, uint64(6666))

	lock := s.mustGetLock(s.key("key"))
	err = s.store.NewLockResolver().ForceResolveLock(context.Background(), lock)
	s.Nil(err)

	newTTL, err = s.store.SendTxnHeartbeat(context.Background(), s.key("key"), txn.StartTS(), 6666)
	s.NotNil(err)
	s.Equal(newTTL, uint64(0))
}

func (s *testLockSuite) TestCheckTxnStatus() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set(s.key("key"), []byte("value"))
	txn.Set(s.key("second"), []byte("xxx"))
	s.prewriteTxnWithTTL(txn, 1000)

	o := s.store.GetOracle()
	currentTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	s.Greater(currentTS, txn.StartTS())

	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	resolver := s.store.NewLockResolver()
	// Call getTxnStatus to check the lock status.
	status, err := resolver.GetTxnStatus(bo, txn.StartTS(), s.key("key"), currentTS, currentTS, true, false, nil)
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_MinCommitTSPushed)

	// Test the ResolveLocks API
	lock := s.mustGetLock(s.key("second"))
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
	status, err = s.store.NewLockResolver().GetTxnStatus(bo, txn.StartTS(), s.key("key"), currentTS, 0, true, false, nil)
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), uint64(0))
	s.Equal(status.Action(), kvrpcpb.Action_NoAction)

	// Call getTxnStatus on a committed transaction.
	startTS, commitTS := s.putKV(s.key("a"), []byte("a"))
	status, err = s.store.NewLockResolver().GetTxnStatus(bo, startTS, s.key("a"), currentTS, currentTS, true, false, nil)
	s.Nil(err)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.CommitTS(), commitTS)
}

func (s *testLockSuite) TestCheckTxnStatusNoWait() {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set(s.key("key"), []byte("value"))
	txn.Set(s.key("second"), []byte("xxx"))
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	// Increase lock TTL to make CI more stable.
	committer.SetLockTTLByTimeAndSize(txn.GetStartTime(), 200*1024*1024)

	// Only prewrite the secondary key to simulate a concurrent prewrite case:
	// prewrite secondary regions success and prewrite the primary region is pending.
	err = committer.PrewriteMutations(context.Background(), committer.MutationsOfKeys([][]byte{s.key("second")}))
	s.Nil(err)

	o := s.store.GetOracle()
	currentTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	resolver := s.store.NewLockResolver()

	// Call getTxnStatus for the TxnNotFound case.
	_, err = resolver.GetTxnStatus(bo, txn.StartTS(), s.key("key"), currentTS, currentTS, false, false, nil)
	s.NotNil(err)
	s.True(resolver.IsErrorNotFound(err))

	errCh := make(chan error)
	go func() {
		errCh <- committer.PrewriteMutations(context.Background(), committer.MutationsOfKeys([][]byte{s.key("key")}))
	}()

	lock := &txnkv.Lock{
		Key:     s.key("second"),
		Primary: s.key("key"),
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
		Key:     s.key("second"),
		Primary: s.key("key_not_exist"),
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
	txn.Set(s.key("ttl_key_1"), []byte("value"))
	time.Sleep(time.Millisecond)
	s.prewriteTxnWithTTL(txn, 3100)
	l := s.mustGetLock(s.key("ttl_key_1"))
	s.True(l.TTL >= defaultLockTTL)

	// Huge txn has a greater TTL.
	txn, err = s.store.Begin()
	start := time.Now()
	s.Nil(err)
	txn.Set(s.key("ttl_key_2"), []byte("value"))
	for i := 0; i < 2048; i++ {
		k, v := randKV(1024, 1024)
		txn.Set([]byte(k), []byte(v))
	}
	elapsed := time.Since(start) / time.Millisecond
	s.prewriteTxn(txn)
	l = s.mustGetLock(s.key("ttl_key_2"))
	s.ttlEquals(l.TTL, uint64(ttlFactor*2)+uint64(elapsed))

	// Txn with long read time.
	start = time.Now()
	txn, err = s.store.Begin()
	s.Nil(err)
	time.Sleep(time.Millisecond * 50)
	txn.Set(s.key("ttl_key_3"), []byte("value"))
	elapsed = time.Since(start) / time.Millisecond
	s.prewriteTxn(txn)
	l = s.mustGetLock(s.key("ttl_key_3"))
	s.ttlEquals(l.TTL, defaultLockTTL+uint64(elapsed))
}

func (s *testLockSuite) TestBatchResolveLocks() {
	// The first transaction is a normal transaction with a long TTL
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.Set(s.key("k1"), []byte("v1"))
	txn.Set(s.key("k2"), []byte("v2"))
	s.prewriteTxnWithTTL(txn, 20000)

	// The second transaction is an async commit transaction
	txn, err = s.store.Begin()
	s.Nil(err)
	txn.Set(s.key("k3"), []byte("v3"))
	txn.Set(s.key("k4"), []byte("v4"))
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.SetUseAsyncCommit()
	committer.SetLockTTL(20000)
	err = committer.PrewriteAllMutations(context.Background())
	s.Nil(err)

	var locks []*txnkv.Lock
	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		l := s.mustGetLock(s.key(key))
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
	_, err = txn.Get(context.Background(), s.key("k1"))
	s.Equal(err, tikverr.ErrNotExist)
	_, err = txn.Get(context.Background(), s.key("k2"))
	s.Equal(err, tikverr.ErrNotExist)
	// transaction 2 is committed
	v, err := txn.Get(context.Background(), s.key("k3"))
	s.Nil(err)
	s.True(bytes.Equal(v.Value, []byte("v3")))
	v, err = txn.Get(context.Background(), s.key("k4"))
	s.Nil(err)
	s.True(bytes.Equal(v.Value, []byte("v4")))
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
	txn.Set(s.key("key"), []byte("value"))
	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)

	mockValue := fmt.Sprintf(`return(%d)`, txn.StartTS())
	s.Nil(failpoint.Enable("tikvclient/mockZeroCommitTS", mockValue))
	s.prewriteTxnWithTTL(txn, 1000)
	s.Nil(failpoint.Disable("tikvclient/mockZeroCommitTS"))

	lock := s.mustGetLock(s.key("key"))
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
	err = txn.Set(s.key("fb1"), []byte("1"))
	s.Nil(err)
	err = txn.Set(s.key("fb2"), []byte("2"))
	s.Nil(err)

	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	s.Equal(committer.GetMutations().Len(), 2)
	committer.SetLockTTL(1)
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

	lock := s.mustGetLock(s.key("fb1"))
	s.True(lock.UseAsyncCommit)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	lr := s.store.NewLockResolver()
	status, err := lr.GetTxnStatusFromLock(bo, lock, 0, false)
	s.Nil(err)
	s.Equal(txnlock.LockProbe{}.GetPrimaryKeyFromTxnStatus(status), s.key("fb1"))

	err = lr.CheckAllSecondaries(bo, lock, &status)
	s.True(lr.IsNonAsyncCommitLock(err))

	resolveStarted := time.Now()
	for {
		status, err = lr.GetTxnStatusFromLock(bo, lock, 0, true)
		s.Nil(err)
		if status.Action() == kvrpcpb.Action_TTLExpireRollback {
			break
		}
		if time.Since(resolveStarted) > 10*time.Second {
			s.NoError(errors.Errorf("Resolve fallback async commit locks timeout"))
		}
	}
	s.Equal(status.Action(), kvrpcpb.Action_TTLExpireRollback)
	s.Equal(status.TTL(), uint64(0))
	s.Equal(status.IsRolledBack(), true)
}

func (s *testLockSuite) TestResolveTxnFallenBackFromAsyncCommit() {
	s.prepareTxnFallenBackFromAsyncCommit()

	lock := s.mustGetLock(s.key("fb1"))
	s.True(lock.UseAsyncCommit)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)

	resolveStarted := time.Now()
	for {
		expire, err := s.store.NewLockResolver().ResolveLocks(bo, 0, []*txnkv.Lock{lock})
		s.Nil(err)
		if expire == 0 {
			break
		}
		if time.Since(resolveStarted) > 10*time.Second {
			s.NoError(errors.Errorf("Resolve fallback async commit locks timeout"))
		}
	}

	t3, err := s.store.Begin()
	s.Nil(err)
	_, err = t3.Get(context.Background(), s.key("fb1"))
	s.True(tikverr.IsErrNotFound(err))
	_, err = t3.Get(context.Background(), s.key("fb2"))
	s.True(tikverr.IsErrNotFound(err))
}

func (s *testLockSuite) TestBatchResolveTxnFallenBackFromAsyncCommit() {
	s.prepareTxnFallenBackFromAsyncCommit()

	lock := s.mustGetLock(s.key("fb1"))
	s.True(lock.UseAsyncCommit)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	loc, err := s.store.GetRegionCache().LocateKey(bo, s.key("fb1"))
	s.Nil(err)
	ok, err := s.store.NewLockResolver().BatchResolveLocks(bo, []*txnkv.Lock{lock}, loc.Region)
	s.Nil(err)
	s.True(ok)

	t3, err := s.store.Begin()
	s.Nil(err)
	_, err = t3.Get(context.Background(), s.key("fb1"))
	s.True(tikverr.IsErrNotFound(err))
	_, err = t3.Get(context.Background(), s.key("fb2"))
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
		err = txn.LockKeys(context.Background(), lockCtx, s.key("a"), s.key("b"))
		lockResCh <- err
	}()

	time.Sleep(500 * time.Millisecond)

	// Check the TTL should have been updated
	lr := s.store.NewLockResolver()
	status, err := lr.LockResolver.GetTxnStatus(txn.StartTS(), 0, s.key("a"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(600))
	s.Equal(status.CommitTS(), uint64(0))

	// Let locking the secondary key fail
	s.Nil(failpoint.Enable("tikvclient/PessimisticLockErrWriteConflict", "return"))
	s.Nil(failpoint.Disable("tikvclient/afterPrimaryBatch"))
	s.Error(<-lockResCh)
	s.Nil(failpoint.Disable("tikvclient/PessimisticLockErrWriteConflict"))

	err = txn.LockKeys(context.Background(), lockCtx, s.key("c"), s.key("d"))
	s.Nil(err)

	time.Sleep(500 * time.Millisecond)

	// The original primary key "a" should be rolled back because its TTL is not updated
	lr = s.store.NewLockResolver()
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), 0, s.key("a"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Equal(status.TTL(), uint64(0))

	// The TTL of the new primary lock should be updated.
	lr = s.store.NewLockResolver()
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), 0, s.key("c"))
	s.Nil(err)
	s.False(status.IsCommitted())
	s.Greater(status.TTL(), uint64(1200))
	s.Equal(status.CommitTS(), uint64(0))

	s.Nil(txn.Rollback())
}

func (s *testLockSuite) TestPrewriteEncountersLargerTsLock() {
	t1, err := s.store.Begin()
	s.Nil(err)
	s.Nil(t1.Set(s.key("k1"), []byte("v1")))
	s.Nil(t1.Set(s.key("k2"), []byte("v2")))

	// t2 has larger TS. Let t2 prewrite only the secondary lock.
	t2, err := s.store.Begin()
	s.Nil(err)
	s.Nil(t2.Set(s.key("k1"), []byte("v1")))
	s.Nil(t2.Set(s.key("k2"), []byte("v2")))
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
	startTS, _ := s.lockKey(s.key("k1"), []byte("v1"), s.key("k11"), []byte("v11"), 3000, true, false)
	committedLocks = append(committedLocks, startTS)
	lock := s.mustGetLock(s.key("k1"))
	locks = append(locks, lock)

	// rolled back
	startTS, _ = s.lockKey(s.key("k2"), []byte("v2"), s.key("k22"), []byte("v22"), 3000, false, false)
	lock = s.mustGetLock(s.key("k22"))
	err := s.store.NewLockResolver().ForceResolveLock(ctx, lock)
	s.Nil(err)
	resolvedLocks = append(resolvedLocks, startTS)
	lock = s.mustGetLock(s.key("k2"))
	locks = append(locks, lock)

	// pushed
	startTS, _ = s.lockKey(s.key("k3"), []byte("v3"), s.key("k33"), []byte("v33"), 3000, false, false)
	resolvedLocks = append(resolvedLocks, startTS)
	lock = s.mustGetLock(s.key("k3"))
	locks = append(locks, lock)

	// can't be pushed and isn't expired
	_, _ = s.lockKey(s.key("k4"), []byte("v4"), s.key("k44"), []byte("v44"), 3000, false, true)
	lock = s.mustGetLock(s.key("k4"))
	locks = append(locks, lock)

	// can't be pushed but is expired
	startTS, _ = s.lockKey(s.key("k5"), []byte("v5"), s.key("k55"), []byte("v55"), 1, false, true)
	committedLocks = append(committedLocks, startTS)
	lock = s.mustGetLock(s.key("k5"))
	locks = append(locks, lock)

	// commitTS > readStartTS
	var readStartTS uint64
	{
		t, err := s.store.Begin()
		s.Nil(err)
		resolvedLocks = append(resolvedLocks, t.StartTS())
		s.Nil(t.Set(s.key("k6"), []byte("v6")))
		s.Nil(t.Set(s.key("k66"), []byte("v66")))
		committer, err := t.NewCommitter(1)
		s.Nil(err)
		s.Nil(committer.PrewriteAllMutations(ctx))
		committer.SetPrimaryKey(s.key("k66"))

		readStartTS, err = s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)

		commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
		s.Greater(commitTS, readStartTS)
		committer.SetCommitTS(commitTS)
		err = committer.CommitMutations(ctx)
		s.Nil(err)
		lock = s.mustGetLock(s.key("k6"))
		locks = append(locks, lock)
	}

	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	lr := s.store.NewLockResolver()
	defer lr.Close()

	// Sleep for a while to make sure the async commit lock "k5" expires, so it could be resolve commit.
	time.Sleep(500 * time.Millisecond)
	msBeforeExpired, resolved, committed, err := lr.ResolveLocksForRead(bo, readStartTS, locks, false)
	s.Nil(err)
	s.Greater(msBeforeExpired, int64(0))
	s.Equal(resolvedLocks, resolved)
	s.Equal(committedLocks, committed)
}

func (s *testLockSuite) TestBatchLiteResolveLocksByRegion() {
	if *withTiKV {
		s.T().Skip("this test validates client-side ResolveLock request planning and avoids real TiKV split timing")
	}

	originalConf := *config.GetGlobalConfig()
	testConf := originalConf
	const resolveLiteThreshold = uint64(8)
	testConf.TiKVClient.ResolveLockLiteThreshold = resolveLiteThreshold
	config.StoreGlobalConfig(&testConf)
	defer config.StoreGlobalConfig(&originalConf)

	// Prewrite all keys but commit only the primary key. The secondary locks stay
	// unresolved and become the inputs to ResolveLocksWithOpts below.
	prepareTxn := func(primary []byte, secondaryKeys [][]byte, txnSize int) (uint64, uint64, []*txnkv.Lock) {
		txn, err := s.store.Begin()
		s.Nil(err)
		s.Nil(txn.Set(primary, []byte("primary_value")))
		for i, key := range secondaryKeys {
			s.Nil(txn.Set(key, []byte(fmt.Sprintf("secondary_value_%d", i))))
		}

		committer, err := txn.NewCommitter(1)
		s.Nil(err)
		committer.SetPrimaryKey(primary)
		committer.SetTxnSize(txnSize)
		committer.SetLockTTL(3000)
		s.Nil(committer.PrewriteAllMutations(context.Background()))

		commitTS, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
		committer.SetCommitTS(commitTS)
		s.Nil(committer.CommitMutations(context.Background()))

		locks := make([]*txnkv.Lock, 0, len(secondaryKeys))
		for _, key := range secondaryKeys {
			locks = append(locks, s.mustGetLock(key))
		}
		return txn.StartTS(), commitTS, locks
	}

	pinLockTxnSize := func(locks []*txnkv.Lock, txnSize uint64) {
		for _, lock := range locks {
			lock.TxnSize = txnSize
		}
	}

	runCase := func(name string, forRead bool) {
		s.Run(name, func() {
			key := func(suffix string) []byte {
				return []byte(fmt.Sprintf("batch_lite_%s_%s", name, suffix))
			}
			scanStartKey := key("")
			scanEndKey := []byte(fmt.Sprintf("batch_lite_%s_\xff", name))

			// Split the keyspace so the multi-key lite transaction is resolved
			// by one ResolveLock request per region.
			splitKey := key("multi_m")
			_, err := s.store.SplitRegions(context.Background(), [][]byte{splitKey}, false, nil)
			s.Nil(err)

			// A small transaction with multiple locks spanning two regions. It
			// should be batched by txn and then split into region-level
			// ResolveLock requests.
			multiKeyTxnKeys := [][]byte{
				key("multi_a1"),
				key("multi_a2"),
				key("multi_z1"),
			}
			multiKeyTxnID, multiKeyCommitTS, multiKeyLocks := prepareTxn(
				key("multi_primary"),
				multiKeyTxnKeys,
				len(multiKeyTxnKeys)+1,
			)

			// A small transaction with one lock. It should keep the old
			// single-key lite resolve shape.
			singleKeyTxnKeys := [][]byte{key("single_secondary")}
			singleKeyTxnID, singleKeyCommitTS, singleKeyLocks := prepareTxn(
				key("single_primary"),
				singleKeyTxnKeys,
				len(singleKeyTxnKeys)+1,
			)

			// A large transaction. It should bypass the batched-lite path and
			// use the original region-level resolve with empty Keys.
			regionLevelTxnKeys := [][]byte{key("region_secondary")}
			regionLevelTxnID, regionLevelCommitTS, regionLevelLocks := prepareTxn(
				key("region_primary"),
				regionLevelTxnKeys,
				int(resolveLiteThreshold)+1,
			)

			// Pin the resolver branch condition directly on the Lock inputs.
			// This keeps the test stable even if the default lite threshold changes.
			pinLockTxnSize(multiKeyLocks, resolveLiteThreshold-1)
			pinLockTxnSize(singleKeyLocks, resolveLiteThreshold-1)
			pinLockTxnSize(regionLevelLocks, resolveLiteThreshold+1)

			boForLocate := tikv.NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
			loc1, err := s.store.GetRegionCache().LocateKey(boForLocate, multiKeyTxnKeys[0])
			s.Nil(err)
			loc2, err := s.store.GetRegionCache().LocateKey(boForLocate, multiKeyTxnKeys[2])
			s.Nil(err)
			s.NotEqual(loc1.Region.GetID(), loc2.Region.GetID())

			resolver := s.store.NewLockResolver()
			defer resolver.Close()
			const expectedResolveReqCount = 4
			const expectedLiteResolveReqCount = 3
			asyncResolveTaskCountBefore := resolver.AsyncResolveTaskCount()
			resolveLockLiteBefore := testutil.ToFloat64(metrics.LockResolverCountWithResolveLockLite)

			// Capture ResolveLock RPCs after RegionRequestSender attaches region
			// context. The hook blocks ResolveLock requests so the test can
			// distinguish client-side async resolve from synchronous resolve.
			var resolveReqMu sync.Mutex
			var resolveReqs []*tikvrpc.Request
			unblockResolveReqs := make(chan struct{})
			var unblockResolveReqsOnce sync.Once
			unblockResolveReqsFn := func() {
				unblockResolveReqsOnce.Do(func() {
					close(unblockResolveReqs)
				})
			}
			defer unblockResolveReqsFn()
			s.Nil(failpoint.Enable("tikvclient/beforeSendReqToRegion", "return"))
			defer func() {
				s.Nil(failpoint.Disable("tikvclient/beforeSendReqToRegion"))
			}()

			ctx := util.WithInternalSourceType(context.Background(), fmt.Sprintf("batch_lite_resolve_%s", name))
			ctx = context.WithValue(ctx, "sendReqToRegionHook", func(req *tikvrpc.Request) {
				if req.Type == tikvrpc.CmdResolveLock {
					resolveReqMu.Lock()
					resolveReqs = append(resolveReqs, req)
					resolveReqMu.Unlock()
					if !forRead {
						s.Equal(asyncResolveTaskCountBefore, resolver.AsyncResolveTaskCount())
					}
					<-unblockResolveReqs
				}
			})
			getResolveReqs := func() []*tikvrpc.Request {
				resolveReqMu.Lock()
				defer resolveReqMu.Unlock()
				return append([]*tikvrpc.Request(nil), resolveReqs...)
			}
			scanRemainingLocks := func() ([]*txnkv.Lock, error) {
				scanTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
				if err != nil {
					return nil, err
				}
				return s.store.ScanLocks(context.Background(), scanStartKey, scanEndKey, scanTS)
			}

			callerStartTS := uint64(0)
			if forRead {
				callerStartTS, err = s.store.CurrentTimestamp(oracle.GlobalTxnScope)
				s.Nil(err)
			}

			bo := tikv.NewBackofferWithVars(ctx, getMaxBackoff, nil)
			locks := make([]*txnkv.Lock, 0, len(multiKeyLocks)+len(singleKeyLocks)+len(regionLevelLocks))
			locks = append(locks, multiKeyLocks...)
			locks = append(locks, singleKeyLocks...)
			locks = append(locks, regionLevelLocks...)
			if forRead {
				resolver.SetAsyncResolveContext(ctx)
			}
			type resolveCallResult struct {
				res txnlock.ResolveLockResult
				err error
			}
			resolveDone := make(chan resolveCallResult, 1)
			go func() {
				res, err := resolver.ResolveLocksWithOpts(bo, txnlock.ResolveLocksOptions{
					CallerStartTS: callerStartTS,
					Locks:         locks,
					ForRead:       forRead,
				})
				resolveDone <- resolveCallResult{res: res, err: err}
			}()

			waitResolveDone := func() resolveCallResult {
				select {
				case result := <-resolveDone:
					return result
				case <-time.After(5 * time.Second):
					s.FailNow("ResolveLocksWithOpts did not return in time")
					return resolveCallResult{}
				}
			}

			if forRead {
				// forRead uses client-side async resolve: ResolveLocksWithOpts
				// should return while the ResolveLock RPCs are still blocked.
				callResult := waitResolveDone()
				s.Nil(callResult.err)
				s.Equal(int64(0), callResult.res.TTL)
				s.Eventually(func() bool {
					return len(getResolveReqs()) >= expectedResolveReqCount
				}, 5*time.Second, 10*time.Millisecond)
				s.Equal(asyncResolveTaskCountBefore+expectedResolveReqCount, resolver.AsyncResolveTaskCount())
				unblockResolveReqsFn()
				s.Eventually(func() bool {
					return resolver.AsyncResolveTaskCount() == asyncResolveTaskCountBefore
				}, 5*time.Second, 10*time.Millisecond)
			} else {
				// Non-read resolve is synchronous: the call should be blocked by
				// the first ResolveLock request until the hook is released.
				s.Eventually(func() bool {
					return len(getResolveReqs()) > 0
				}, 5*time.Second, 10*time.Millisecond)
				s.Equal(asyncResolveTaskCountBefore, resolver.AsyncResolveTaskCount())
				select {
				case callResult := <-resolveDone:
					s.FailNow("synchronous ResolveLocksWithOpts returned before the blocked ResolveLock request was released", callResult)
				case <-time.After(100 * time.Millisecond):
				}
				unblockResolveReqsFn()
				callResult := waitResolveDone()
				s.Nil(callResult.err)
				s.Equal(int64(0), callResult.res.TTL)
			}

			reqsByTxn := make(map[uint64][]*tikvrpc.Request)
			for _, req := range getResolveReqs() {
				s.Equal(util.RequestSourceFromCtx(ctx), req.RequestSource)
				resolveLock := req.ResolveLock()
				reqsByTxn[resolveLock.StartVersion] = append(reqsByTxn[resolveLock.StartVersion], req)
			}

			s.Len(reqsByTxn, 3)
			s.Equal(resolveLockLiteBefore+expectedLiteResolveReqCount, testutil.ToFloat64(metrics.LockResolverCountWithResolveLockLite))

			// The multi-key lite txn is grouped by region. One region has two
			// keys, and the other has one key.
			multiKeyReqs := reqsByTxn[multiKeyTxnID]
			s.Len(multiKeyReqs, 2)
			var multiKeyResolvedKeys [][]byte
			var multiKeyResolvedRegions []uint64
			for _, req := range multiKeyReqs {
				resolveLock := req.ResolveLock()
				s.Equal(multiKeyCommitTS, resolveLock.CommitVersion)
				s.False(resolveLock.GetIsAsync())
				s.NotEmpty(resolveLock.Keys)
				multiKeyResolvedKeys = append(multiKeyResolvedKeys, resolveLock.Keys...)
				multiKeyResolvedRegions = append(multiKeyResolvedRegions, req.RegionId)
			}
			s.ElementsMatch(multiKeyTxnKeys, multiKeyResolvedKeys)
			s.ElementsMatch([]uint64{loc1.Region.GetID(), loc2.Region.GetID()}, multiKeyResolvedRegions)

			// The single-key lite txn still sends a single-key ResolveLock request.
			singleKeyReqs := reqsByTxn[singleKeyTxnID]
			s.Len(singleKeyReqs, 1)
			s.Equal(singleKeyCommitTS, singleKeyReqs[0].ResolveLock().CommitVersion)
			s.False(singleKeyReqs[0].ResolveLock().GetIsAsync())
			s.ElementsMatch(singleKeyTxnKeys, singleKeyReqs[0].ResolveLock().Keys)

			// The large txn keeps the original region-level resolve shape.
			regionLevelReqs := reqsByTxn[regionLevelTxnID]
			s.Len(regionLevelReqs, 1)
			s.Equal(regionLevelCommitTS, regionLevelReqs[0].ResolveLock().CommitVersion)
			s.Equal(forRead && config.NextGen, regionLevelReqs[0].ResolveLock().GetIsAsync())
			s.Empty(regionLevelReqs[0].ResolveLock().Keys)

			// Finally verify the ResolveLock requests actually clear every lock
			// created by this test, including the keys that were grouped by
			// region above.
			remainingLocks, err := scanRemainingLocks()
			s.Nil(err)
			s.Empty(remainingLocks)
		})
	}

	runCase("write", false)
	runCase("for_read", true)
}

func (s *testLockSuite) TestBatchLiteResolveLocksForReadIgnoresInlineCleanupError() {
	if *withTiKV {
		s.T().Skip("this test relies on client-side failpoints for deterministic cleanup errors")
	}

	// This test covers the for-read fallback path where client-side async
	// cleanup is rejected and the cleanup runs inline. Once CheckTxnStatus has
	// confirmed the transaction status, the read can already proceed, so a later
	// physical cleanup error should be swallowed instead of being returned to
	// ResolveLocksForRead.
	key := s.key("batch_lite_for_read_fallback_key")
	primaryKey := s.key("batch_lite_for_read_fallback_primary")
	startTS, _ := s.lockKey(key, []byte("value"), primaryKey, []byte("primary_value"), 3000, true, false)
	lock := s.mustGetLock(key)
	lock.TxnSize = 1

	callerStartTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)

	resolver := s.store.NewLockResolver()
	defer resolver.Close()
	// A zero-sized pool rejects client-side async cleanup and forces the
	// fallback path to execute cleanup inline.
	resolver.SetAsyncResolvePoolSize(0)

	injectResolveFailure := true
	resolveFailureInjected := false
	s.Nil(failpoint.Enable("tikvclient/beforeSendReqToRegion", "return"))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/beforeSendReqToRegion"))
		if resolveFailureInjected {
			s.Nil(failpoint.Disable("tikvclient/tikvStoreSendReqResult"))
		}
	}()

	ctx := context.WithValue(context.Background(), "sendReqToRegionHook", func(req *tikvrpc.Request) {
		if req.Type == tikvrpc.CmdCheckTxnStatus && injectResolveFailure && !resolveFailureInjected {
			// Inject the timeout only after the status check request is sent, so
			// the injected error belongs to the following cleanup ResolveLock RPC.
			resolveFailureInjected = true
			s.Nil(failpoint.Enable("tikvclient/tikvStoreSendReqResult", `return("timeout")`))
		}
	})
	bo := tikv.NewBackofferWithVars(ctx, getMaxBackoff, nil)
	ttl, canIgnore, canAccess, err := resolver.ResolveLocksForRead(bo, callerStartTS, []*txnkv.Lock{lock}, true)
	s.Nil(err)
	s.Equal(int64(0), ttl)
	s.Empty(canIgnore)
	s.Equal([]uint64{startTS}, canAccess)
	s.True(resolveFailureInjected)

	injectResolveFailure = false
	s.Nil(failpoint.Disable("tikvclient/tikvStoreSendReqResult"))
	resolveFailureInjected = false
	remainingLock := s.mustGetLock(key)
	cleanupResolver := s.store.NewLockResolver()
	defer cleanupResolver.Close()
	s.Nil(cleanupResolver.ForceResolveLock(context.Background(), remainingLock))
}

func (s *testLockSuite) TestLockWaitTimeLimit() {
	k1 := s.key("wait_limit_k1")
	k2 := s.key("wait_limit_k2")

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

type testLockWithTiKVSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testLockWithTiKVSuite) SetupTest() {
	if *withTiKV {
		s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
		s.cleanupLocks()
	} else {
		s.store = tikv.StoreProbe{KVStore: NewTestUniStore(s.T())}
		s.cleanupLocks()
	}
}

func (s *testLockWithTiKVSuite) TearDownTest() {
	s.store.Close()
}

func (s *testLockWithTiKVSuite) key(name string) []byte {
	return encodeKey("~lock_tikv", name)
}

func (s *testLockWithTiKVSuite) keyRange(start, end string) ([]byte, []byte) {
	return s.key(start), s.key(end)
}

func (s *testLockWithTiKVSuite) cleanupLocks() {
	// Cleanup possible left locks.
	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	currentTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.NoError(err)
	startKey, endKey := s.keyRange("k", "l")
	remainingLocks, err := s.store.ScanLocks(ctx, startKey, endKey, currentTS)
	s.NoError(err)
	if len(remainingLocks) > 0 {
		s.mustResolve(ctx, bo, remainingLocks, currentTS, startKey, endKey)
	}
}

// TODO: Migrate FairLocking related tests here.

func withRetry[T any](f func() (T, error), limit int, delay time.Duration) (T, error) {
	for {
		res, err := f()
		if err == nil {
			return res, nil
		}

		limit--
		if limit <= 0 {
			return res, err
		}

		if delay != 0 {
			time.Sleep(delay)
		}
	}
}

func (s *testLockWithTiKVSuite) checkIsKeyLocked(key []byte, expectedLocked bool) {
	// To be aware of the result of async operations (e.g. async pessimistic rollback), retry if the check fails.
	_, err := withRetry(func() (interface{}, error) {
		txn, err := s.store.Begin()
		s.NoError(err)

		txn.SetPessimistic(true)

		lockCtx := kv.NewLockCtx(txn.StartTS(), kv.LockNoWait, time.Now())
		err = txn.LockKeys(context.Background(), lockCtx, key)

		var isCheckSuccess bool
		if err != nil && stderrs.Is(err, tikverr.ErrLockAcquireFailAndNoWaitSet) {
			isCheckSuccess = expectedLocked
		} else {
			s.Nil(err)
			isCheckSuccess = !expectedLocked
		}

		if isCheckSuccess {
			s.Nil(txn.Rollback())
			return nil, nil
		}

		s.Nil(txn.Rollback())

		return nil, errors.Errorf("expected key %q locked = %v, but the actual result not match", string(key), expectedLocked)
	}, 5, time.Millisecond*50)

	s.NoError(err)
}

func (s *testLockWithTiKVSuite) TestPrewriteCheckForUpdateTS() {
	if config.NextGen {
		s.T().Skip("NextGen does not support fair locking")
	}
	test := func(asyncCommit bool, onePC bool, causalConsistency bool) {
		k1 := s.key("prewrite_check_k1")
		k2 := s.key("prewrite_check_k2")
		k3 := s.key("prewrite_check_k3")
		k4 := s.key("prewrite_check_k4")
		v1 := []byte("v1")
		v2 := []byte("v2")

		// Clear previous value
		{
			txn, err := s.store.Begin()
			s.NoError(err)
			for _, k := range [][]byte{k1, k2, k3, k4} {
				s.NoError(txn.Delete(k))
			}
			s.NoError(txn.Commit(context.Background()))
		}

		ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))

		// Test failing case
		txn, err := s.store.Begin()
		s.NoError(err)

		txn.SetPessimistic(true)

		txn.SetEnableAsyncCommit(asyncCommit)
		txn.SetEnable1PC(onePC)
		txn.SetCausalConsistency(causalConsistency)

		txn.StartAggressiveLocking()
		lockTime := time.Now()
		lockCtx := kv.NewLockCtx(txn.StartTS(), 200, lockTime)
		s.NoError(txn.LockKeys(ctx, lockCtx, k1))
		s.NoError(txn.LockKeys(ctx, lockCtx, k2))
		s.NoError(txn.LockKeys(ctx, lockCtx, k3))
		txn.DoneAggressiveLocking(ctx)
		s.NoError(txn.Set(k1, v1))

		txn.GetCommitter().CloseTTLManager()

		simulatedCommitter, err := txn.NewCommitter(1)
		s.NoError(err)

		s.NoError(failpoint.Enable("tikvclient/beforePrewrite", "pause"))
		commitFinishCh := make(chan error)
		go func() {
			commitFinishCh <- txn.Commit(ctx)
		}()
		select {
		case <-commitFinishCh:
			s.Fail("txn commit not blocked")
		case <-time.After(time.Millisecond * 100):
		}

		// Simulate pessimistic lock lost. Retry on region error to make it stable when running with TiKV.
		{
			mutations := transaction.NewPlainMutations(1)
			mutations.Push(kvrpcpb.Op_PessimisticLock, k1, nil, true, false, false, false)
			simulatedCommitter.SetForUpdateTS(simulatedCommitter.GetStartTS())
			err := simulatedCommitter.PessimisticRollbackMutations(context.Background(), &mutations)
			s.NoError(err)
			s.checkIsKeyLocked(k1, false)
		}

		// Simulate the key being written by another transaction
		txn2, err := s.store.Begin()
		s.NoError(err)
		s.NoError(txn2.Set(k1, v2))
		s.NoError(txn2.Commit(context.Background()))

		// Simulate stale pessimistic lock request being replayed on TiKV
		{
			simulatedTxn, err := s.store.Begin()
			s.NoError(err)
			simulatedTxn.SetStartTS(txn.StartTS())
			simulatedTxn.SetPessimistic(true)

			simulatedTxn.StartAggressiveLocking()
			lockCtx := kv.NewLockCtx(txn.StartTS(), 200, lockTime)
			s.NoError(simulatedTxn.LockKeys(context.Background(), lockCtx, k1))

			info := simulatedTxn.GetAggressiveLockingKeysInfo()
			s.Equal(1, len(info))
			s.Equal(k1, info[0].Key())
			s.Equal(txn2.CommitTS(), info[0].ActualLockForUpdateTS())

			simulatedTxn.DoneAggressiveLocking(context.Background())
			defer func() {
				s.NoError(simulatedTxn.Rollback())
			}()
		}

		s.NoError(failpoint.Disable("tikvclient/beforePrewrite"))
		err = <-commitFinishCh
		s.Error(err)
		s.Regexp("[pP]essimistic ?[lL]ock ?[nN]ot ?[fF]ound", err.Error())

		snapshot := s.store.GetSnapshot(txn2.CommitTS())
		v, err := snapshot.Get(context.Background(), k1)
		s.NoError(err)
		s.Equal(v2, v.Value)

		snapshot = s.store.GetSnapshot(txn2.CommitTS() - 1)
		_, err = snapshot.Get(context.Background(), k1)
		s.Equal(tikverr.ErrNotExist, err)

		// Test passing case
		txn, err = s.store.Begin()
		s.NoError(err)
		txn.SetPessimistic(true)
		txn.SetEnableAsyncCommit(asyncCommit)
		txn.SetEnable1PC(onePC)
		txn.SetCausalConsistency(causalConsistency)

		// Prepare a conflicting version to key k2
		{
			txn1, err := s.store.Begin()
			s.NoError(err)
			s.NoError(txn1.Set(k2, v2))
			s.NoError(txn1.Commit(context.Background()))
		}

		txn.StartAggressiveLocking()
		lockCtx = kv.NewLockCtx(txn.StartTS(), 200, lockTime)
		// k2: conflicting key
		s.NoError(txn.LockKeys(ctx, lockCtx, k2))
		// k3: non-conflicting key
		s.NoError(txn.LockKeys(ctx, lockCtx, k3))
		// There must be a new tso allocation before committing if any key is locked with conflict, otherwise
		// async commit will become unsafe.
		// In TiDB, there must be a tso allocation for updating forUpdateTS.
		currentTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
		s.NoError(err)
		lockCtx = kv.NewLockCtx(currentTS, 200, lockTime)
		// k4: non-conflicting key but forUpdateTS updated
		s.NoError(txn.LockKeys(ctx, lockCtx, k4))
		s.NoError(txn.Set(k2, v1))
		s.NoError(txn.Set(k3, v1))
		txn.DoneAggressiveLocking(ctx)

		s.NoError(txn.Commit(ctx))

		snapshot = s.store.GetSnapshot(txn.CommitTS())
		v, err = snapshot.Get(context.Background(), k2)
		s.NoError(err)
		s.Equal(v1, v.Value)
		v, err = snapshot.Get(context.Background(), k3)
		s.NoError(err)
		s.Equal(v1, v.Value)

		snapshot = s.store.GetSnapshot(txn.CommitTS() - 1)
		v, err = snapshot.Get(context.Background(), k2)
		s.NoError(err)
		s.Equal(v2, v.Value)
		_, err = snapshot.Get(context.Background(), k3)
		s.Equal(tikverr.ErrNotExist, err)
	}

	test(false, false, false)
	test(true, false, false)
	test(true, true, false)
	test(true, false, true)
}

func (s *testLockWithTiKVSuite) TestCheckTxnStatusSentToSecondary() {
	s.NoError(failpoint.Enable("tikvclient/beforeAsyncPessimisticRollback", `return("skip")`))
	s.NoError(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", "return"))
	s.NoError(failpoint.Enable("tikvclient/shortPessimisticLockTTL", "return"))
	s.NoError(failpoint.Enable("tikvclient/twoPCShortLockTTL", "return"))
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/beforeAsyncPessimisticRollback"))
		s.NoError(failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit"))
		s.NoError(failpoint.Disable("tikvclient/shortPessimisticLockTTL"))
		s.NoError(failpoint.Disable("tikvclient/twoPCShortLockTTL"))
	}()

	k1 := s.key("rollback_read_k1")
	k2 := s.key("rollback_read_k2")
	k3 := s.key("rollback_read_k3")

	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))

	txn, err := s.store.Begin()
	s.NoError(err)
	txn.SetPessimistic(true)

	// Construct write conflict to make the LockKeys operation fail.
	{
		txn2, err := s.store.Begin()
		s.NoError(err)
		s.NoError(txn2.Set(k3, []byte("v3")))
		s.NoError(txn2.Commit(ctx))
	}

	lockCtx := kv.NewLockCtx(txn.StartTS(), 200, time.Now())
	err = txn.LockKeys(ctx, lockCtx, k1, k2, k3)
	s.IsType(&tikverr.ErrWriteConflict{}, errors.Cause(err))

	// At this time: txn's primary is unsetted, and the keys:
	// * k1: stale pessimistic lock, primary
	// * k2: stale pessimistic lock, primary -> k1

	forUpdateTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.NoError(err)
	lockCtx = kv.NewLockCtx(forUpdateTS, 200, time.Now())
	err = txn.LockKeys(ctx, lockCtx, k3) // k3 becomes primary
	err = txn.LockKeys(ctx, lockCtx, k1)
	s.Equal(k3, txn.GetCommitter().GetPrimaryKey())

	// At this time:
	// * k1: pessimistic lock, primary -> k3
	// * k2: stale pessimistic lock, primary -> k1
	// * k3: pessimistic lock, primary

	s.NoError(txn.Set(k1, []byte("v1-1")))
	s.NoError(txn.Set(k3, []byte("v3-1")))

	s.NoError(failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/beforeCommitSecondaries"))
	}()

	s.NoError(txn.Commit(ctx))

	// At this time:
	// * k1: prewritten, primary -> k3
	// * k2: stale pessimistic lock, primary -> k1
	// * k3: committed

	// Trigger resolving lock on k2
	{
		txn2, err := s.store.Begin()
		s.NoError(err)
		txn2.SetPessimistic(true)
		lockCtx = kv.NewLockCtx(txn2.StartTS(), 200, time.Now())
		s.NoError(txn2.LockKeys(ctx, lockCtx, k2))
		s.NoError(txn2.Rollback())
	}

	// Check data consistency
	readTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.NoError(err)
	snapshot := s.store.GetSnapshot(readTS)
	v, err := snapshot.Get(ctx, k3)
	s.NoError(err)
	s.Equal([]byte("v3-1"), v.Value)
	_, err = snapshot.Get(ctx, k2)
	s.Equal(tikverr.ErrNotExist, err)
	v, err = snapshot.Get(ctx, k1)
	s.NoError(err)
	s.Equal([]byte("v1-1"), v.Value)
}

func (s *testLockWithTiKVSuite) TestBatchResolveLocks() {
	s.NoError(failpoint.Enable("tikvclient/beforeAsyncPessimisticRollback", `return("skip")`))
	s.NoError(failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	s.NoError(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", `return`))
	s.NoError(failpoint.Enable("tikvclient/onRollback", `return("skipRollbackPessimisticLock")`))
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/beforeAsyncPessimisticRollback"))
		s.NoError(failpoint.Disable("tikvclient/beforeCommitSecondaries"))
		s.NoError(failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit"))
		s.NoError(failpoint.Disable("tikvclient/onRollback"))
	}()

	k1, k2, k3, k4 := s.key("k1"), s.key("k2"), s.key("k3"), s.key("k4")
	v2, v3 := []byte("v2"), []byte("v3")

	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))

	txn1, err := s.store.Begin()
	s.NoError(err)
	txn1.SetPessimistic(true)

	{
		// Produce write conflict on key k2
		helperTxn, err := s.store.Begin()
		s.NoError(err)
		s.NoError(helperTxn.Set(k2, []byte("v0")))
		s.NoError(helperTxn.Commit(ctx))
	}

	lockCtx := kv.NewLockCtx(txn1.StartTS(), 200, time.Now())
	err = txn1.LockKeys(ctx, lockCtx, k1, k2)
	s.IsType(&tikverr.ErrWriteConflict{}, errors.Cause(err))

	// k1 has txn1's stale pessimistic lock now.

	forUpdateTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.NoError(err)
	lockCtx = kv.NewLockCtx(forUpdateTS, 200, time.Now())
	s.NoError(txn1.LockKeys(ctx, lockCtx, k2, k3))

	s.NoError(txn1.Set(k2, v2))
	s.NoError(txn1.Set(k3, v3))
	s.NoError(txn1.Commit(ctx))

	// k3 has txn1's stale prewrite lock now.

	txn2, err := s.store.Begin()
	txn2.SetPessimistic(true)
	s.NoError(err)
	lockCtx = kv.NewLockCtx(txn1.StartTS(), 200, time.Now())
	err = txn2.LockKeys(ctx, lockCtx, k4)
	s.NoError(err)
	s.NoError(txn2.Rollback())

	// k4 has txn2's stale primary pessimistic lock now.
	currentTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)

	// sleep a while for pipelined pessimistic locks
	time.Sleep(time.Millisecond * 100)

	startKey, endKey := s.keyRange("k", "l")
	remainingLocks, err := s.store.ScanLocks(ctx, startKey, endKey, currentTS)
	s.NoError(err)

	s.Len(remainingLocks, 3)
	s.Equal(remainingLocks[0].Key, k1)
	s.Equal(remainingLocks[0].LockType, kvrpcpb.Op_PessimisticLock)
	s.Equal(remainingLocks[1].Key, k3)
	s.Equal(remainingLocks[1].LockType, kvrpcpb.Op_Put)
	s.Equal(remainingLocks[2].Key, k4)
	s.Equal(remainingLocks[2].LockType, kvrpcpb.Op_PessimisticLock)
	s.Equal(remainingLocks[2].Primary, k4)

	// Perform ScanLock - BatchResolveLock.
	s.NoError(err)
	s.NoError(s.store.GCResolveLockPhase(ctx, currentTS, 1))

	// Do ScanLock again to make sure no locks are left.
	remainingLocks, err = s.store.ScanLocks(ctx, startKey, endKey, currentTS)
	s.NoError(err)
	s.Empty(remainingLocks)

	// Check data consistency
	readTS, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	snapshot := s.store.GetSnapshot(readTS)
	_, err = snapshot.Get(ctx, k1)
	s.Equal(tikverr.ErrNotExist, err)
	v, err := snapshot.Get(ctx, k2)
	s.NoError(err)
	s.Equal(v2, v.Value)
	v, err = snapshot.Get(ctx, k3)
	s.NoError(err)
	s.Equal(v3, v.Value)
}

func (s *testLockWithTiKVSuite) makeLock(startTS uint64, forUpdateTS uint64, key []byte, primary []byte) *txnlock.Lock {
	return &txnlock.Lock{
		Key:             key,
		Primary:         primary,
		TxnID:           startTS,
		TTL:             10,
		TxnSize:         1024,
		LockType:        kvrpcpb.Op_PessimisticLock,
		UseAsyncCommit:  false,
		LockForUpdateTS: forUpdateTS,
		MinCommitTS:     forUpdateTS,
	}
}

func (s *testLockWithTiKVSuite) mustLockNum(ctx context.Context, expectedNum int, scanTS uint64, startKey []byte, endKey []byte) {
	remainingLocks, err := s.store.ScanLocks(ctx, startKey, endKey, scanTS)
	s.NoError(err)
	s.Len(remainingLocks, expectedNum)
}

func (s *testLockWithTiKVSuite) mustResolve(ctx context.Context, bo *retry.Backoffer, remainingLocks []*txnlock.Lock, callerTS uint64, startKey []byte, endKey []byte) {
	if len(remainingLocks) > 0 {
		_, err := s.store.GetLockResolver().ResolveLocksWithOpts(bo, txnlock.ResolveLocksOptions{
			CallerStartTS:            callerTS,
			Locks:                    remainingLocks,
			Lite:                     false,
			ForRead:                  false,
			Detail:                   nil,
			PessimisticRegionResolve: true,
		})
		s.NoError(err)

		lockAfterResolve, err := s.store.ScanLocks(ctx, startKey, endKey, callerTS)
		s.NoError(err)
		s.Len(lockAfterResolve, 0, "expected=%v actual=%v", 0, len(lockAfterResolve))
	}
}

func (s *testLockWithTiKVSuite) TestPessimisticRollbackWithRead() {
	s.NoError(failpoint.Enable("tikvclient/shortPessimisticLockTTL", "return"))
	s.NoError(failpoint.Enable("tikvclient/twoPCShortLockTTL", "return"))
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/shortPessimisticLockTTL"))
		s.NoError(failpoint.Disable("tikvclient/twoPCShortLockTTL"))
	}()
	// Init, cleanup possible left locks.
	bo := tikv.NewBackofferWithVars(context.Background(), int(transaction.PrewriteMaxBackoff.Load()), nil)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	s.cleanupLocks()

	startKey, endKey := s.keyRange("k", "l")

	// Basic case, three keys could be rolled back within one pessimistic rollback request.
	k1, k2, k3 := s.key("k1"), s.key("k2"), s.key("k3")
	txn1, err := s.store.Begin()
	s.NoError(err)
	startTS := txn1.StartTS()
	txn1.SetPessimistic(true)
	lockCtx := kv.NewLockCtx(startTS, 200, time.Now())
	err = txn1.LockKeys(ctx, lockCtx, k1, k2, k3)
	s.NoError(err)
	txn1.GetCommitter().CloseTTLManager()

	time.Sleep(time.Millisecond * 100)
	s.mustLockNum(ctx, 3, startTS+1, startKey, endKey)
	locks := []*txnlock.Lock{
		s.makeLock(startTS, startTS, k3, k1),
	}
	s.mustResolve(ctx, bo, locks, startTS+1, startKey, endKey)

	time.Sleep(time.Millisecond * 100)
	s.mustLockNum(ctx, 0, startTS+1, startKey, endKey)

	// Acquire pessimistic locks for more than 256(RESOLVE_LOCK_BATCH_SIZE) keys.
	formatKey := func(prefix rune, i int) []byte {
		return s.key(fmt.Sprintf("%c%04d", prefix, i))
	}
	numKeys := 1000
	prewriteKeys := make([][]byte, 0, numKeys/2)
	pessimisticLockKeys := make([][]byte, 0, numKeys/2)
	for i := 0; i < numKeys; i++ {
		key := formatKey('k', i)
		if i%2 == 0 {
			err = txn1.LockKeys(ctx, lockCtx, key)
			pessimisticLockKeys = append(pessimisticLockKeys, key)
		} else {
			err = txn1.Set(key, []byte("val"))
			s.NoError(err)
			prewriteKeys = append(prewriteKeys, key)
		}
		s.NoError(err)
	}
	committer, err := txn1.NewCommitter(1)
	s.NoError(err)
	mutations := committer.MutationsOfKeys(prewriteKeys)
	err = committer.PrewriteMutations(ctx, mutations)
	s.NoError(err)

	// All the pessimistic locks belonging to the same transaction are pessimistic
	// rolled back within one request.
	time.Sleep(time.Millisecond * 100)
	pessimisticLock := s.makeLock(startTS, startTS, pessimisticLockKeys[1], pessimisticLockKeys[0])
	_, err = s.store.GetLockResolver().ResolveLocksWithOpts(bo, txnlock.ResolveLocksOptions{
		CallerStartTS:            startTS + 1,
		Locks:                    []*txnlock.Lock{pessimisticLock},
		Lite:                     false,
		ForRead:                  false,
		Detail:                   nil,
		PessimisticRegionResolve: true,
	})
	s.NoError(err)

	time.Sleep(time.Millisecond * 100)
	s.mustLockNum(ctx, numKeys/2, startTS+1, startKey, endKey)

	// Cleanup.
	err = txn1.Rollback()
	s.NoError(err)
}

func (s *testLockWithTiKVSuite) TestPessimisticLockMaxExecutionTime() {
	if !*withTiKV {
		s.T().Skip()
	}
	// This test covers the path where max_execution_time deadline is checked
	// during pessimistic lock operations.
	// Note: TiKV caps lock wait time at 1 second.

	ctx := context.Background()
	k1 := s.key("lock_max_execution_time_k1")
	k2 := s.key("lock_max_execution_time_k2")

	// Setup: Create a lock that will block our test transaction
	txn1, err := s.store.Begin()
	s.NoError(err)
	txn1.SetPessimistic(true)

	// Lock k1 with txn1 to create blocking condition
	lockCtx1 := kv.NewLockCtx(txn1.StartTS(), kv.LockAlwaysWait, time.Now())
	err = txn1.LockKeys(ctx, lockCtx1, k1)
	s.NoError(err)

	// Test case 1: max_execution_time deadline already exceeded
	txn2, err := s.store.Begin()
	s.NoError(err)
	txn2.SetPessimistic(true)

	// Set MaxExecutionDeadline to a time in the past
	baseTime := time.Now()
	lockCtx2 := kv.NewLockCtx(txn2.StartTS(), 800, baseTime)              // 800ms lock wait
	lockCtx2.MaxExecutionDeadline = baseTime.Add(-100 * time.Millisecond) // Already expired

	err = txn2.LockKeys(ctx, lockCtx2, k1)
	s.Error(err)

	// Verify it's the correct max_execution_time error
	var queryInterruptedErr tikverr.ErrQueryInterruptedWithSignal
	s.ErrorAs(err, &queryInterruptedErr)
	s.Equal(uint32(transaction.MaxExecTimeExceededSignal), queryInterruptedErr.Signal)

	// Test case 2: max_execution_time deadline limits lock wait time
	// max_execution_time (200ms) < lock wait time (800ms) < TiKV limit (1000ms)
	txn3, err := s.store.Begin()
	s.NoError(err)
	txn3.SetPessimistic(true)

	startTime := time.Now()
	lockCtx3 := kv.NewLockCtx(txn3.StartTS(), 800, startTime)             // 800ms lock wait
	lockCtx3.MaxExecutionDeadline = startTime.Add(200 * time.Millisecond) // But max exec time only 200ms

	err = txn3.LockKeys(ctx, lockCtx3, k1)
	elapsed := time.Since(startTime)

	s.Error(err)
	s.ErrorAs(err, &queryInterruptedErr)
	s.Equal(uint32(transaction.MaxExecTimeExceededSignal), queryInterruptedErr.Signal)

	// Should timeout around 200ms, not 800ms
	s.Greater(elapsed, 190*time.Millisecond)
	s.Less(elapsed, 400*time.Millisecond)

	// Test case 3: lock wait timeout shorter than max_execution_time
	// lock wait time (150ms) < max_execution_time (600ms) < TiKV limit (1000ms)
	txn4, err := s.store.Begin()
	s.NoError(err)
	txn4.SetPessimistic(true)

	startTime = time.Now()
	lockCtx4 := kv.NewLockCtx(txn4.StartTS(), 150, startTime)             // 150ms lock wait
	lockCtx4.MaxExecutionDeadline = startTime.Add(600 * time.Millisecond) // 600ms max exec time

	err = txn4.LockKeys(ctx, lockCtx4, k1)
	elapsed = time.Since(startTime)

	s.Error(err)
	// Should be lock wait timeout, not max execution time error
	s.Equal(tikverr.ErrLockWaitTimeout.Error(), err.Error())

	// Should timeout around 150ms
	s.Greater(elapsed, 120*time.Millisecond)
	s.Less(elapsed, 300*time.Millisecond)

	// Test case 4: TiKV limit is the constraint
	// max_execution_time (1200ms) > TiKV limit (1000ms) > lock wait time (900ms)
	txn5, err := s.store.Begin()
	s.NoError(err)
	txn5.SetPessimistic(true)

	startTime = time.Now()
	lockCtx5 := kv.NewLockCtx(txn5.StartTS(), 900, startTime)              // 900ms lock wait
	lockCtx5.MaxExecutionDeadline = startTime.Add(1200 * time.Millisecond) // 1200ms max exec time

	err = txn5.LockKeys(ctx, lockCtx5, k1)
	elapsed = time.Since(startTime)

	s.Error(err)
	// Should be lock wait timeout due to TiKV's 1-second cap, not max execution time
	s.Equal(tikverr.ErrLockWaitTimeout.Error(), err.Error())

	// Should timeout around 900ms (limited by requested lock wait time)
	s.Greater(elapsed, 800*time.Millisecond)
	s.Less(elapsed, 1200*time.Millisecond)

	// Test case 5: No max_execution_time deadline (normal behavior)
	txn6, err := s.store.Begin()
	s.NoError(err)
	txn6.SetPessimistic(true)

	lockCtx6 := kv.NewLockCtx(txn6.StartTS(), kv.LockNoWait, time.Now())
	// MaxExecutionDeadline defaults to zero time (no deadline)

	err = txn6.LockKeys(ctx, lockCtx6, k1)
	s.Error(err)
	// Should be immediate lock acquisition failure, not max execution time error
	s.Equal(tikverr.ErrLockAcquireFailAndNoWaitSet.Error(), err.Error())

	// Cleanup: Unlock k1 and test successful lock with max_execution_time
	s.NoError(txn1.Rollback())

	// Test case 6: Successful lock acquisition with max_execution_time set
	txn7, err := s.store.Begin()
	s.NoError(err)
	txn7.SetPessimistic(true)

	lockCtx7 := kv.NewLockCtx(txn7.StartTS(), kv.LockAlwaysWait, time.Now())
	lockCtx7.MaxExecutionDeadline = time.Now().Add(100 * time.Millisecond)

	err = txn7.LockKeys(ctx, lockCtx7, k2) // k2 is not locked
	s.NoError(err)                         // Should succeed

	// Additional coverage: max_execution_time timeout should skip lock resolution.
	blockerTxn, err := s.store.Begin()
	s.NoError(err)
	blockerTxn.SetPessimistic(true)
	lockCtxBlocker := kv.NewLockCtx(blockerTxn.StartTS(), kv.LockAlwaysWait, time.Now())

	s.NoError(blockerTxn.LockKeys(ctx, lockCtxBlocker, k1))
	s.NoError(failpoint.Enable("tikvclient/tryResolveLock", "panic"))

	txn8, err := s.store.Begin()
	s.NoError(err)
	txn8.SetPessimistic(true)

	startTime = time.Now()
	lockCtx8 := kv.NewLockCtx(txn8.StartTS(), 800, startTime)
	lockCtx8.MaxExecutionDeadline = startTime.Add(200 * time.Millisecond)

	err = txn8.LockKeys(ctx, lockCtx8, k1)
	elapsed = time.Since(startTime)

	s.Error(err)
	s.ErrorAs(err, &queryInterruptedErr)
	s.Equal(uint32(transaction.MaxExecTimeExceededSignal), queryInterruptedErr.Signal)
	s.Greater(elapsed, 190*time.Millisecond)
	s.Less(elapsed, 400*time.Millisecond)

	s.NoError(txn8.Rollback())
	s.NoError(failpoint.Disable("tikvclient/tryResolveLock"))
	s.NoError(blockerTxn.Rollback())

	// Cleanup all transactions
	s.NoError(txn2.Rollback())
	s.NoError(txn3.Rollback())
	s.NoError(txn4.Rollback())
	s.NoError(txn5.Rollback())
	s.NoError(txn6.Rollback())
	s.NoError(txn7.Rollback())
}

func TestResolveLockWithTiKVSideAsync(t *testing.T) {
	if !*withTiKV {
		t.Skip()
	}

	originalConf := *config.GetGlobalConfig()
	testConf := originalConf
	const resolveLiteThreshold = uint64(16)
	testConf.TiKVClient.ResolveLockLiteThreshold = resolveLiteThreshold
	config.StoreGlobalConfig(&testConf)
	defer config.StoreGlobalConfig(&originalConf)

	for _, commitPrimary := range []bool{true, false} {
		name := "primary_rollback"
		if commitPrimary {
			name = "primary_commit"
		}

		fetchTSO := func(store tikv.StoreProbe) uint64 {
			ts, err := store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			require.NoError(t, err)
			return ts
		}

		t.Run(name, func(t *testing.T) {
			store := tikv.StoreProbe{KVStore: NewTestStore(t)}
			defer func() {
				require.NoError(t, store.Close())
			}()

			ctx := context.Background()
			lockTTL := uint64(3000)
			if !commitPrimary {
				lockTTL = 1
			}

			keys, values := make([][]byte, 0, 20), make([][]byte, 0, 20)
			for i := 0; i < cap(keys); i++ {
				keys = append(keys, []byte(fmt.Sprintf("resolve_lock_%s_%03d", name, i)))
				values = append(values, []byte(fmt.Sprintf("value_%s_%03d", name, i)))
			}
			require.Greater(t, uint64(len(keys)), resolveLiteThreshold)

			// prewrite
			txn, err := store.Begin()
			require.NoError(t, err)
			for i, key := range keys {
				require.NoError(t, txn.Set(key, values[i]))
			}
			committer, err := txn.NewCommitter(1)
			require.NoError(t, err)
			committer.SetPrimaryKey(keys[0])
			committer.SetTxnSize(len(keys))
			committer.SetLockTTL(lockTTL)
			require.NoError(t, committer.PrewriteAllMutations(ctx))

			// commit or rollback the primary
			if commitPrimary {
				committer.SetCommitTS(fetchTSO(store))
				require.NoError(t, committer.CommitMutations(ctx))
			} else {
				var lastErr error
				require.Eventually(t, func() bool {
					pollCtx, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()

					readTS := fetchTSO(store)
					status, err := store.NewLockResolver().GetTxnStatus(
						retry.NewBackofferWithVars(pollCtx, 1000, nil),
						txn.StartTS(),
						keys[0],
						readTS,
						readTS,
						true,
						false,
						nil,
					)
					if err != nil {
						lastErr = err
						return false
					}
					lastErr = nil
					return status.IsRolledBack()
				}, 10*time.Second, 100*time.Millisecond)
				require.NoError(t, lastErr)
			}

			// check all the locks except the primary lock are kept.
			keysEnd := []byte(fmt.Sprintf("resolve_lock_%s_\xff", name))
			locks, err := store.ScanLocks(ctx, keys[0], keysEnd, fetchTSO(store))
			require.NoError(t, err)
			require.Len(t, locks, len(keys)-1, "only the primary lock should be resolved")

			// trigger resolve lock for read
			resolveLocksBefore := testutil.ToFloat64(metrics.LockResolverCountWithResolveLocks)
			resolveLockLiteBefore := testutil.ToFloat64(metrics.LockResolverCountWithResolveLockLite)
			resolveLockReqs := make([]*kvrpcpb.ResolveLockRequest, 0)
			require.NoError(t, failpoint.Enable("tikvclient/beforeSendReqToRegion", "return"))
			defer func() {
				require.NoError(t, failpoint.Disable("tikvclient/beforeSendReqToRegion"))
			}()
			ctx = context.WithValue(ctx, "sendReqToRegionHook", func(req *tikvrpc.Request) {
				if req.Type == tikvrpc.CmdResolveLock {
					resolveLockReqs = append(resolveLockReqs, req.ResolveLock())
				}
			})
			store.GetLockResolver().Close()

			snapshot := store.GetSnapshot(fetchTSO(store))
			value, err := snapshot.BatchGet(ctx, keys)
			require.NoError(t, err)
			if commitPrimary {
				require.Len(t, value, len(keys))
			} else {
				require.Empty(t, value)
			}

			// wait all locks are resolved
			require.Eventually(t, func() bool {
				locks, err := store.ScanLocks(ctx, keys[0], keysEnd, fetchTSO(store))
				require.NoError(t, err)
				return len(locks) == 0
			}, 5*time.Second, 100*time.Millisecond)

			// check resolve lock requests
			require.NotEmpty(t, resolveLockReqs, "ResolveLock request should be sent")
			for _, req := range resolveLockReqs {
				require.Empty(t, req.Keys)
				if commitPrimary {
					require.Equal(t, committer.GetCommitTS(), req.CommitVersion)
				} else {
					require.Zero(t, req.CommitVersion)
				}
				// check in next-gen the IsAsync should be true in ResolveLockRequest
				require.Equal(t, config.NextGen, req.GetIsAsync(), "ResolveLock should use TiKV-side async resolve")
			}

			// check resolve lock metrics, no resolve lock lite should be used
			require.Greater(t,
				testutil.ToFloat64(metrics.LockResolverCountWithResolveLocks),
				resolveLocksBefore,
				"ResolveLocks should be called",
			)
			require.Equal(
				t,
				resolveLockLiteBefore,
				testutil.ToFloat64(metrics.LockResolverCountWithResolveLockLite),
				"ResolveLock should be non-lite",
			)

			// check the keys are committed or rolled back correctly
			snapshot = store.GetSnapshot(fetchTSO(store))
			for i, key := range keys {
				value, err := snapshot.Get(ctx, key)
				if commitPrimary {
					require.NoError(t, err)
					require.Equal(t, values[i], value.Value)
				} else {
					require.True(t, tikverr.IsErrNotFound(err), "unexpected error: %v", err)
				}
			}
		})
	}
}
