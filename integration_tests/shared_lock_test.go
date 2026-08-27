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
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func TestSharedLock(t *testing.T) {
	if !*withTiKV {
		t.Skip("skipping TestSharedLock because with-tikv is not enabled")
		return
	}
	suite.Run(t, new(testSharedLockSuite))
}

type testSharedLockSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

type dropUpgradeResponseClient struct {
	tikv.Client
	startTS      uint64
	key          []byte
	requestSent  chan struct{}
	sentSignaled atomic.Bool
	shouldDrop   func(*kvrpcpb.PessimisticLockResponse) bool
	dropped      atomic.Bool
}

func (c *dropUpgradeResponseClient) SendRequest(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdPessimisticLock {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	lockReq := req.PessimisticLock()
	if lockReq.GetStartVersion() != c.startTS || len(lockReq.Mutations) != 1 {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	mutation := lockReq.Mutations[0]
	if mutation.Op != kvrpcpb.Op_PessimisticLock || !bytes.Equal(mutation.Key, c.key) {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	if c.sentSignaled.CompareAndSwap(false, true) {
		close(c.requestSent)
	}

	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err != nil || resp == nil {
		return resp, err
	}
	lockResp, ok := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
	if !ok || !c.shouldDrop(lockResp) || !c.dropped.CompareAndSwap(false, true) {
		return resp, nil
	}
	return &tikvrpc.Response{}, nil
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

func (s *testSharedLockSuite) key(name string) []byte {
	return encodeKey("~shared_lock", name)
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

func (s *testSharedLockSuite) waitLocks(key []byte, maxTS uint64, expected int, msgAndArgs ...interface{}) []*txnlock.Lock {
	var locks []*txnlock.Lock
	s.Eventually(func() bool {
		locks = s.scanLocks(key, maxTS)
		return len(locks) == expected
	}, 5*time.Second, 100*time.Millisecond, msgAndArgs...)
	return locks
}

func (s *testSharedLockSuite) waitForLockWait(startTS uint64, key []byte) {
	s.Eventually(func() bool {
		for _, store := range s.store.GetRegionCache().GetStoresByType(tikvrpc.TiKV) {
			resp, err := s.store.GetTiKVClient().SendRequest(
				context.Background(),
				store.GetAddr(),
				tikvrpc.NewRequest(tikvrpc.CmdLockWaitInfo, &kvrpcpb.GetLockWaitInfoRequest{}),
				time.Second,
			)
			if err != nil || resp == nil || resp.Resp == nil {
				continue
			}
			waitResp, ok := resp.Resp.(*kvrpcpb.GetLockWaitInfoResponse)
			if !ok {
				continue
			}
			for _, entry := range waitResp.GetEntries() {
				if entry.GetTxn() == startTS && bytes.Equal(entry.GetKey(), key) {
					return true
				}
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "pending shared lock upgrade did not reach the lock wait queue")
}

func (s *testSharedLockSuite) rollbackPessimisticKey(startTS, forUpdateTS uint64, key []byte) {
	bo := tikv.NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, &kvrpcpb.PessimisticRollbackRequest{
		StartVersion: startTS,
		ForUpdateTs:  forUpdateTS,
		Keys:         [][]byte{key},
	})
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	s.Require().NoError(err)
	resp, err := s.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutShort)
	s.Require().NoError(err)
	regionErr, err := resp.GetRegionError()
	s.Require().NoError(err)
	s.Require().Nil(regionErr)
	s.Require().NotNil(resp.Resp)
	rollbackResp, ok := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
	s.Require().True(ok)
	s.Empty(rollbackResp.GetErrors())
}

func (s *testSharedLockSuite) TestSharedLockBlockExclusiveLock() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := s.key("TestSharedLockBlockExclusiveLock_pk1")
		pk2 := s.key("TestSharedLockBlockExclusiveLock_pk2")
		pk3 := s.key("TestSharedLockBlockExclusiveLock_pk3")
		key := s.key("TestSharedLockBlockExclusiveLock_shared_key")

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

func (s *testSharedLockSuite) TestUpgradeAppliedWithMissingResponseCanCommit() {
	if !config.NextGen {
		s.T().Skip("shared lock upgrade is only supported on next-gen")
	}

	txn := s.begin()
	primaryKey := s.key("TestUpgradeAppliedWithMissingResponseCanCommit_primary")
	upgradeKey := s.key("TestUpgradeAppliedWithMissingResponseCanCommit_upgrade")
	blockerPrimaryKey := s.key("TestUpgradeAppliedWithMissingResponseCanCommit_blocker_primary")

	s.NoError(txn.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), primaryKey))
	sharedLockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	sharedLockCtx.InShareMode = true
	s.NoError(txn.LockKeys(context.Background(), sharedLockCtx, upgradeKey))

	originalClient := s.store.GetTiKVClient()
	droppingClient := &dropUpgradeResponseClient{
		Client:      originalClient,
		startTS:     txn.StartTS(),
		key:         upgradeKey,
		requestSent: make(chan struct{}),
		shouldDrop: func(resp *kvrpcpb.PessimisticLockResponse) bool {
			return resp.GetRegionError() == nil && len(resp.Errors) == 0
		},
	}
	s.store.SetTiKVClient(droppingClient)
	upgradeLockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	upgradeLockCtx.AllowSharedLockUpgrade = true
	err := txn.LockKeys(context.Background(), upgradeLockCtx, upgradeKey)
	s.store.SetTiKVClient(originalClient)
	s.ErrorIs(err, tikverr.ErrBodyMissing)
	s.False(tikverr.IsErrorUndetermined(err))
	s.True(droppingClient.dropped.Load())

	flags, err := txn.GetMemBuffer().GetFlags(upgradeKey)
	s.NoError(err)
	s.True(flags.HasLocked())
	s.True(flags.HasLockedInShareMode())

	blocker := s.begin()
	s.NoError(blocker.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), blockerPrimaryKey))
	blockedSharedLockCtx := kv.NewLockCtx(s.getTS(), kv.LockNoWait, time.Now())
	blockedSharedLockCtx.InShareMode = true
	err = blocker.LockKeys(context.Background(), blockedSharedLockCtx, upgradeKey)
	s.ErrorIs(err, tikverr.ErrLockAcquireFailAndNoWaitSet)
	s.NoError(blocker.Rollback())

	value := []byte("committed-after-missing-upgrade-response")
	s.NoError(txn.Set(upgradeKey, value))
	s.NoError(txn.Commit(context.Background()))

	reader := s.begin()
	got, err := reader.Get(context.Background(), upgradeKey)
	s.NoError(err)
	s.Equal(value, got.Value)
	s.NoError(reader.Rollback())
}

func (s *testSharedLockSuite) TestSharedLockLostWithMissingResponseReconcilesDurableState() {
	if !config.NextGen {
		s.T().Skip("shared lock upgrade is only supported on next-gen")
	}

	testCases := []struct {
		name string
	}{
		{name: "DifferentSharedHolderRemains"},
		{name: "WrapperDeleted"},
		{name: "ExclusiveReplacement"},
	}

	for _, testCase := range testCases {
		s.Run(testCase.name, func() {
			keyPrefix := "TestSharedLockLostWithMissingResponseReconcilesDurableState_" + testCase.name
			upgradeKey := s.key(keyPrefix + "_upgrade")
			upgraderPrimaryKey := s.key(keyPrefix + "_upgrader_primary")
			holderPrimaryKey := s.key(keyPrefix + "_holder_primary")

			upgrader := s.begin()
			holder := s.begin()
			defer func() {
				if upgrader.Valid() {
					s.NoError(upgrader.Rollback())
				}
				if holder.Valid() {
					s.NoError(holder.Rollback())
				}
			}()

			s.NoError(upgrader.LockKeys(
				context.Background(),
				kv.NewLockCtx(s.getTS(), 1000, time.Now()),
				upgraderPrimaryKey,
			))
			upgraderSharedLockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			upgraderSharedLockCtx.InShareMode = true
			s.NoError(upgrader.LockKeys(context.Background(), upgraderSharedLockCtx, upgradeKey))

			s.NoError(holder.LockKeys(
				context.Background(),
				kv.NewLockCtx(s.getTS(), 1000, time.Now()),
				holderPrimaryKey,
			))
			holderSharedLockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			holderSharedLockCtx.InShareMode = true
			s.NoError(holder.LockKeys(context.Background(), holderSharedLockCtx, upgradeKey))

			upgradeForUpdateTS := s.getTS()
			primeUpgradeLockCtx := kv.NewLockCtx(upgradeForUpdateTS, kv.LockNoWait, time.Now())
			primeUpgradeLockCtx.AllowSharedLockUpgrade = true
			primeErr := upgrader.LockKeys(context.Background(), primeUpgradeLockCtx, upgradeKey)
			s.Error(primeErr)
			s.True(tikverr.IsErrWriteConflict(primeErr))
			s.False(tikverr.IsErrorUndetermined(primeErr))

			originalClient := s.store.GetTiKVClient()
			droppingClient := &dropUpgradeResponseClient{
				Client:      originalClient,
				startTS:     upgrader.StartTS(),
				key:         upgradeKey,
				requestSent: make(chan struct{}),
				shouldDrop: func(resp *kvrpcpb.PessimisticLockResponse) bool {
					if resp.GetRegionError() != nil || len(resp.Errors) != 1 {
						return false
					}
					sharedLockLost := resp.Errors[0].GetSharedLockLost()
					return sharedLockLost != nil &&
						sharedLockLost.GetStartTs() == upgrader.StartTS() &&
						bytes.Equal(sharedLockLost.GetKey(), upgradeKey)
				},
			}
			s.store.SetTiKVClient(droppingClient)
			defer s.store.SetTiKVClient(originalClient)

			upgradeResult := make(chan error, 1)
			go func() {
				upgradeLockCtx := kv.NewLockCtx(upgradeForUpdateTS, 30_000, time.Now())
				upgradeLockCtx.AllowSharedLockUpgrade = true
				upgradeResult <- upgrader.LockKeys(context.Background(), upgradeLockCtx, upgradeKey)
			}()

			select {
			case <-droppingClient.requestSent:
			case <-time.After(5 * time.Second):
				s.FailNow("shared lock upgrade request was not sent")
			}
			s.waitForLockWait(upgrader.StartTS(), upgradeKey)

			s.rollbackPessimisticKey(upgrader.StartTS(), s.getTS(), upgradeKey)

			var upgradeErr error
			select {
			case upgradeErr = <-upgradeResult:
			case <-time.After(5 * time.Second):
				s.FailNow("shared lock upgrade did not finish after holder rollback")
			}
			s.store.SetTiKVClient(originalClient)
			s.ErrorIs(upgradeErr, tikverr.ErrBodyMissing)
			s.False(tikverr.IsErrorUndetermined(upgradeErr))
			s.True(droppingClient.dropped.Load())

			flags, err := upgrader.GetMemBuffer().GetFlags(upgradeKey)
			s.NoError(err)
			s.True(flags.HasLocked())
			s.True(flags.HasLockedInShareMode())

			retryUpgrade := func() error {
				retryUpgradeLockCtx := kv.NewLockCtx(s.getTS(), kv.LockNoWait, time.Now())
				retryUpgradeLockCtx.AllowSharedLockUpgrade = true
				return upgrader.LockKeys(context.Background(), retryUpgradeLockCtx, upgradeKey)
			}
			assertDefiniteRetryFailure := func(err error) {
				s.Error(err)
				s.False(tikverr.IsErrorUndetermined(err))
				s.NotErrorIs(err, tikverr.ErrBodyMissing)
			}

			switch testCase.name {
			case "DifferentSharedHolderRemains":
				assertDefiniteRetryFailure(retryUpgrade())
			case "WrapperDeleted":
				s.NoError(holder.Rollback())
				s.Empty(s.waitLocks(upgradeKey, s.getTS(), 0))
				s.Error(upgrader.Commit(context.Background()))
			case "ExclusiveReplacement":
				s.NoError(holder.Rollback())
				s.Empty(s.waitLocks(upgradeKey, s.getTS(), 0))

				replacement := s.begin()
				defer func() {
					if replacement.Valid() {
						s.NoError(replacement.Rollback())
					}
				}()
				replacementPrimaryKey := s.key(keyPrefix + "_replacement_primary")
				s.NoError(replacement.LockKeys(
					context.Background(),
					kv.NewLockCtx(s.getTS(), 1000, time.Now()),
					replacementPrimaryKey,
				))
				s.NoError(replacement.LockKeys(
					context.Background(),
					kv.NewLockCtx(s.getTS(), kv.LockNoWait, time.Now()),
					upgradeKey,
				))

				assertDefiniteRetryFailure(retryUpgrade())
			default:
				s.FailNow("unknown durable-state test case", testCase.name)
			}
		})
	}
}

func (s *testSharedLockSuite) TestExclusiveLockBlockSharedLock() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := s.key("TestExclusiveLockBlockSharedLock_pk1")
		pk2 := s.key("TestExclusiveLockBlockSharedLock_pk2")
		pk3 := s.key("TestExclusiveLockBlockSharedLock_pk3")
		key := s.key("TestExclusiveLockBlockSharedLock_shared_key")

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

	pk := s.key("TestResolveSharedLock_pk")
	key := s.key("TestResolveSharedLock_shared_key")
	_, err := s.store.SplitRegions(context.Background(), [][]byte{pk, key}, false, nil)
	s.Nil(err)

	s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn1.GetCommitter().GetPrimaryKey())
	lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	lockCtx.InShareMode = true
	s.Nil(txn1.LockKeys(context.Background(), lockCtx, key))

	s.Nil(failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/beforeCommitSecondaries"))
	}()
	txn1.SetSessionID(1)
	s.Nil(txn1.Commit(context.Background()))

	locks := s.waitLocks(key, s.getTS(), 1, "expect committed shared lock to be visible")
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

	locks = s.waitLocks(key, s.getTS(), 1, "expect pessimistic lock to be visible")
	s.Len(locks, 1)
	lock = locks[0]
	s.NotNil(lock)
	s.Equal(key, lock.Key)
	s.Equal(pk, lock.Primary)
	s.Equal(txn2.StartTS(), lock.TxnID)
	s.Equal(lock.LockType, kvrpcpb.Op_PessimisticLock)

	s.Nil(txn2.Rollback())
	s.waitLocks(key, s.getTS(), 0, "expect no locks after rollback")
}

func (s *testSharedLockSuite) TestScanSharedLock() {
	pk1 := s.key("TestScanSharedLock_pk_1")
	pk2 := s.key("TestScanSharedLock_pk_2")
	pk3 := s.key("TestScanSharedLock_pk_3")
	sharedKey := s.key("TestScanSharedLock_shared_key")
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
	s.waitLocks(sharedKey, txn3.StartTS(), 3, "expect 3 locks after pipelined locks being applied")
	for maxTS, lockNum := range maxTS2LockNum {
		locks := s.scanLocks(sharedKey, maxTS)
		s.Equal(lockNum, len(locks), "when maxTS=%d, expect %d locks", maxTS, lockNum)
		for _, lock := range locks {
			s.Equal(sharedKey, lock.Key)
			s.LessOrEqual(lock.TxnID, maxTS)
		}
	}

	for _, txn := range txns {
		s.Nil(txn.Rollback())
	}
	s.waitLocks(sharedKey, txn3.StartTS(), 0, "no locks after rollback")
}

func (s *testSharedLockSuite) TestGCSharedLock() {
	originManagedLockTTL := atomic.LoadUint64(&transaction.ManagedLockTTL)
	atomic.StoreUint64(&transaction.ManagedLockTTL, 1000) // 1000ms, increased for test stability in busy environments
	defer atomic.StoreUint64(&transaction.ManagedLockTTL, originManagedLockTTL)

	txn1 := s.begin()
	txn2 := s.begin()
	txn3 := s.begin()
	pk1 := s.key("TestGCSharedLock_pk1")
	pk2 := s.key("TestGCSharedLock_pk2")
	pk3 := s.key("TestGCSharedLock_pk3")
	sharedKey := s.key("TestGCSharedLock_shared_key")

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

	locks := s.waitLocks(sharedKey, s.getTS(), 3, "expect 3 locks after pipelined locks being applied")
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

	locks = s.waitLocks(sharedKey, s.getTS(), 1, "expected 1 lock (txn3's) to remain; txn3 TTL running: %v", txn3.GetCommitter().IsTTLRunning())
	s.Len(locks, 1)
	s.Equal(txn3.StartTS(), locks[0].TxnID)
	s.Equal(sharedKey, locks[0].Key)
	s.Nil(txn3.Rollback())
}

func (s *testSharedLockSuite) TestSharedLockCommitAndRollback() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := s.key("TestSharedLockCommitAndRollback_pk1")
		pk2 := s.key("TestSharedLockCommitAndRollback_pk2")
		pk3 := s.key("TestSharedLockCommitAndRollback_pk3")
		key := s.key("TestSharedLockCommitAndRollback_shared_key")

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
			locks = s.waitLocks(key, s.getTS(), currLocks-1, "after txn %d commit/rollback, expect %d locks remain", i+1, currLocks-1)

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

	pk := s.key("TestPrewriteResolveExpiredSharedLock_pk")
	key := s.key("TestPrewriteResolveExpiredSharedLock_key")

	// Step 1: Create a pessimistic transaction with shared lock
	txn1 := s.begin()
	s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn1.GetCommitter().GetPrimaryKey())

	lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	lockCtx.InShareMode = true
	s.Nil(txn1.LockKeys(context.Background(), lockCtx, key))

	// Wait for the shared lock to be visible.
	s.waitLocks(key, s.getTS(), 1, "expect 1 shared lock")

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
	locks = s.waitLocks(key, s.getTS(), 0, "shared lock should have been resolved")
	s.Len(locks, 0)

	// Cleanup
	s.Nil(txn1.Rollback())
}

func (s *testSharedLockSuite) TestPrewriteResolveExpiredSharedLockWithActiveHolder() {
	originManagedLockTTL := atomic.LoadUint64(&transaction.ManagedLockTTL)
	atomic.StoreUint64(&transaction.ManagedLockTTL, 500)
	defer atomic.StoreUint64(&transaction.ManagedLockTTL, originManagedLockTTL)

	expiredTxn := s.begin()
	activeTxn := s.begin()
	sharedKey := s.key("TestPrewriteResolveExpiredSharedLockWithActiveHolder_key")
	primaryKeys := [][]byte{
		s.key("TestPrewriteResolveExpiredSharedLockWithActiveHolder_expired_pk"),
		s.key("TestPrewriteResolveExpiredSharedLockWithActiveHolder_active_pk"),
	}
	for i, txn := range []transaction.TxnProbe{expiredTxn, activeTxn} {
		s.Nil(txn.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), primaryKeys[i]))
		lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockCtx.InShareMode = true
		s.Nil(txn.LockKeys(context.Background(), lockCtx, sharedKey))
	}
	s.waitLocks(sharedKey, s.getTS(), 2, "expect two shared lock holders")
	expiredTxn.GetCommitter().CloseTTLManager()
	time.Sleep(time.Duration(atomic.LoadUint64(&transaction.ManagedLockTTL))*time.Millisecond + 200*time.Millisecond)
	s.True(activeTxn.GetCommitter().IsTTLRunning())

	contender, err := s.store.Begin()
	s.Nil(err)
	value := []byte("contender-value")
	s.Nil(contender.Set(sharedKey, value))
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- contender.Commit(context.Background())
	}()

	locks := s.waitLocks(sharedKey, s.getTS(), 1, "expired holder should be removed while active holder remains")
	s.Equal(activeTxn.StartTS(), locks[0].TxnID)
	select {
	case err := <-commitDone:
		s.FailNow("prewrite returned while the active shared lock remained", err)
	case <-time.After(200 * time.Millisecond):
	}

	s.Nil(activeTxn.Rollback())
	select {
	case err := <-commitDone:
		s.Nil(err)
	case <-time.After(5 * time.Second):
		s.FailNow("prewrite did not finish after the active holder released")
	}

	snapshot := s.store.GetSnapshot(contender.CommitTS())
	got, err := snapshot.Get(context.Background(), sharedKey)
	s.Nil(err)
	s.Equal(value, got.Value)
	s.Nil(expiredTxn.Rollback())
}

func (s *testSharedLockSuite) TestForceLockRetryOnSharedLock() {
	if config.NextGen {
		s.T().Skip("NextGen does not support allow_lock_with_conflict / ForceLock yet")
	}
	pk1 := s.key("TestForceLockRetryOnSharedLock_pk1")
	pk2 := s.key("TestForceLockRetryOnSharedLock_pk2")
	key := s.key("TestForceLockRetryOnSharedLock_key")

	// Step 1: txn1 acquires a shared lock on key
	txn1 := s.begin()
	s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk1))
	s.Equal(pk1, txn1.GetCommitter().GetPrimaryKey())
	lockCtx1 := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	lockCtx1.InShareMode = true
	s.Nil(txn1.LockKeys(context.Background(), lockCtx1, key))

	// Wait for the shared lock to be visible
	s.Eventually(func() bool {
		return len(s.scanLocks(key, s.getTS())) == 1
	}, 5*time.Second, 100*time.Millisecond, "expect 1 shared lock")

	// Step 2: txn2 in aggressive locking mode (ForceLock) acquires exclusive lock on the same key
	txn2 := s.begin()
	s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk2))
	s.Equal(pk2, txn2.GetCommitter().GetPrimaryKey())

	txn2.StartAggressiveLocking()
	lockCtx2 := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	errCh := make(chan error, 1)
	go func() {
		// Single key + aggressive locking → ForceLock mode
		errCh <- txn2.LockKeys(context.Background(), lockCtx2, key)
	}()

	time.Sleep(100 * time.Millisecond)
	select {
	case <-errCh:
		s.Fail("ForceLock should block on shared lock, not return immediately")
		return
	default:
	}

	s.Nil(txn1.Rollback())
	s.Nil(<-errCh, "ForceLock mode should resolve expired shared lock and succeed")

	txn2.DoneAggressiveLocking(context.Background())

	// Verify txn2 holds the exclusive lock
	locks := s.scanLocks(key, s.getTS())
	s.Len(locks, 1)
	s.Equal(txn2.StartTS(), locks[0].TxnID)

	// Cleanup
	s.Nil(txn2.Rollback())
}
