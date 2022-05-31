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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/async_commit_test.go
//

// Copyright 2020 PingCAP, Inc.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
)

func TestAsyncCommit(t *testing.T) {
	suite.Run(t, new(testAsyncCommitSuite))
}

// testAsyncCommitCommon is used to put common parts that will be both used by
// testAsyncCommitSuite and testAsyncCommitFailSuite.
type testAsyncCommitCommon struct {
	suite.Suite
	cluster testutils.Cluster
	store   *tikv.KVStore
}

// TODO(youjiali1995): remove it after updating TiDB.
type unistoreClientWrapper struct {
	*unistore.RPCClient
}

func (c *unistoreClientWrapper) CloseAddr(addr string) error {
	return nil
}

func (s *testAsyncCommitCommon) setUpTest() {
	if *withTiKV {
		s.store = NewTestStore(s.T())
		return
	}

	client, pdClient, cluster, err := unistore.New("")
	s.Require().Nil(err)

	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := tikv.NewTestTiKVStore(fpClient{Client: &unistoreClientWrapper{client}}, pdClient, nil, nil, 0)
	s.Require().Nil(err)

	s.store = store
}

func (s *testAsyncCommitCommon) tearDownTest() {
	s.store.Close()
}

func (s *testAsyncCommitCommon) putAlphabets(enableAsyncCommit bool) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV([]byte{ch}, []byte{ch}, enableAsyncCommit)
	}
}

func (s *testAsyncCommitCommon) putKV(key, value []byte, enableAsyncCommit bool) (uint64, uint64) {
	txn := s.beginAsyncCommit()
	err := txn.Set(key, value)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	return txn.StartTS(), txn.GetCommitTS()
}

func (s *testAsyncCommitCommon) mustGetFromTxn(txn transaction.TxnProbe, key, expectedValue []byte) {
	v, err := txn.Get(context.Background(), key)
	s.Nil(err)
	s.Equal(v, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetLock(key []byte) *txnkv.Lock {
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver,
	})
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	s.Nil(err)
	resp, err := s.store.SendReq(bo, req, loc.Region, time.Second*10)
	s.Nil(err)
	s.NotNil(resp.Resp)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	s.NotNil(keyErr)
	lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
	s.Nil(err)
	return lock
}

func (s *testAsyncCommitCommon) mustPointGet(key, expectedValue []byte) {
	snap := s.store.GetSnapshot(math.MaxUint64)
	value, err := snap.Get(context.Background(), key)
	s.Nil(err)
	s.Equal(value, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetFromSnapshot(version uint64, key, expectedValue []byte) {
	snap := s.store.GetSnapshot(version)
	value, err := snap.Get(context.Background(), key)
	s.Nil(err)
	s.Equal(value, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetNoneFromSnapshot(version uint64, key []byte) {
	snap := s.store.GetSnapshot(version)
	_, err := snap.Get(context.Background(), key)
	s.Equal(errors.Cause(err), tikverr.ErrNotExist)
}

func (s *testAsyncCommitCommon) beginAsyncCommitWithLinearizability() transaction.TxnProbe {
	txn := s.beginAsyncCommit()
	txn.SetCausalConsistency(false)
	return txn
}

func (s *testAsyncCommitCommon) beginAsyncCommit() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.SetEnableAsyncCommit(true)
	return transaction.TxnProbe{KVTxn: txn}
}

func (s *testAsyncCommitCommon) begin() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Nil(err)
	return transaction.TxnProbe{KVTxn: txn}
}

func (s *testAsyncCommitCommon) begin1PC() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.SetEnable1PC(true)
	return transaction.TxnProbe{KVTxn: txn}
}

type testAsyncCommitSuite struct {
	testAsyncCommitCommon
	bo *tikv.Backoffer
}

func (s *testAsyncCommitSuite) SetupTest() {
	s.testAsyncCommitCommon.setUpTest()
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testAsyncCommitSuite) TearDownTest() {
	s.testAsyncCommitCommon.tearDownTest()
}

func (s *testAsyncCommitSuite) lockKeysWithAsyncCommit(keys, values [][]byte, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
	txn, err := s.store.Begin()
	s.Nil(err)
	txn.SetEnableAsyncCommit(true)
	for i, k := range keys {
		if len(values[i]) > 0 {
			err = txn.Set(k, values[i])
		} else {
			err = txn.Delete(k)
		}
		s.Nil(err)
	}
	if len(primaryValue) > 0 {
		err = txn.Set(primaryKey, primaryValue)
	} else {
		err = txn.Delete(primaryKey)
	}
	s.Nil(err)
	txnProbe := transaction.TxnProbe{KVTxn: txn}
	tpc, err := txnProbe.NewCommitter(0)
	s.Nil(err)
	tpc.SetPrimaryKey(primaryKey)
	tpc.SetUseAsyncCommit()

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

func (s *testAsyncCommitSuite) TestCheckSecondaries() {
	// This test doesn't support tikv mode.
	if *withTiKV {
		return
	}

	s.putAlphabets(true)

	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(loc.Region.GetID(), newRegionID, []byte("e"), []uint64{peerID}, peerID)
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// No locks to check, only primary key is locked, should be successful.
	s.lockKeysWithAsyncCommit([][]byte{}, [][]byte{}, []byte("z"), []byte("z"), false)
	lock := s.mustGetLock([]byte("z"))
	lock.UseAsyncCommit = true
	ts, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	var lockutil txnlock.LockProbe
	status := lockutil.NewLockStatus(nil, true, ts)

	resolver := tikv.NewLockResolverProb(s.store.GetLockResolver())
	err = resolver.ResolveAsyncCommitLock(s.bo, lock, status)
	s.Nil(err)
	currentTS, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	status, err = resolver.GetTxnStatus(s.bo, lock.TxnID, []byte("z"), currentTS, currentTS, true, false, nil)
	s.Nil(err)
	s.True(status.IsCommitted())
	s.Equal(status.CommitTS(), ts)

	// One key is committed (i), one key is locked (a). Should get committed.
	ts, err = s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	commitTs := ts + 10

	gotCheckA := int64(0)
	gotCheckB := int64(0)
	gotResolve := int64(0)
	gotOther := int64(0)
	mock := mockResolveClient{
		Client: s.store.GetTiKVClient(),
		onCheckSecondaries: func(req *kvrpcpb.CheckSecondaryLocksRequest) (*tikvrpc.Response, error) {
			if req.StartVersion != ts {
				return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
			}
			var resp kvrpcpb.CheckSecondaryLocksResponse
			for _, k := range req.Keys {
				if bytes.Equal(k, []byte("a")) {
					atomic.StoreInt64(&gotCheckA, 1)

					resp = kvrpcpb.CheckSecondaryLocksResponse{
						Locks:    []*kvrpcpb.LockInfo{{Key: []byte("a"), PrimaryLock: []byte("z"), LockVersion: ts, UseAsyncCommit: true}},
						CommitTs: commitTs,
					}
				} else if bytes.Equal(k, []byte("i")) {
					atomic.StoreInt64(&gotCheckB, 1)

					resp = kvrpcpb.CheckSecondaryLocksResponse{
						Locks:    []*kvrpcpb.LockInfo{},
						CommitTs: commitTs,
					}
				} else {
					fmt.Printf("Got other key: %s\n", k)
					atomic.StoreInt64(&gotOther, 1)
				}
			}
			return &tikvrpc.Response{Resp: &resp}, nil
		},
		onResolveLock: func(req *kvrpcpb.ResolveLockRequest) (*tikvrpc.Response, error) {
			if req.StartVersion != ts {
				return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
			}
			if req.CommitVersion != commitTs {
				return nil, errors.Errorf("Bad commit version: %d, expected: %d", req.CommitVersion, commitTs)
			}
			for _, k := range req.Keys {
				if bytes.Equal(k, []byte("a")) || bytes.Equal(k, []byte("z")) {
					atomic.StoreInt64(&gotResolve, 1)
				} else {
					atomic.StoreInt64(&gotOther, 1)
				}
			}
			resp := kvrpcpb.ResolveLockResponse{}
			return &tikvrpc.Response{Resp: &resp}, nil
		},
	}
	s.store.SetTiKVClient(&mock)

	status = lockutil.NewLockStatus([][]byte{[]byte("a"), []byte("i")}, true, 0)
	lock = &txnkv.Lock{
		Key:            []byte("a"),
		Primary:        []byte("z"),
		TxnID:          ts,
		LockType:       kvrpcpb.Op_Put,
		UseAsyncCommit: true,
		MinCommitTS:    ts + 5,
	}

	_ = s.beginAsyncCommit()

	err = resolver.ResolveAsyncCommitLock(s.bo, lock, status)
	s.Nil(err)
	s.Equal(gotCheckA, int64(1))
	s.Equal(gotCheckB, int64(1))
	s.Equal(gotOther, int64(0))
	s.Equal(gotResolve, int64(1))

	// One key has been rolled back (b), one is locked (a). Should be rolled back.
	ts, err = s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	commitTs = ts + 10

	gotCheckA = int64(0)
	gotCheckB = int64(0)
	gotResolve = int64(0)
	gotOther = int64(0)
	mock.onResolveLock = func(req *kvrpcpb.ResolveLockRequest) (*tikvrpc.Response, error) {
		if req.StartVersion != ts {
			return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
		}
		if req.CommitVersion != commitTs {
			return nil, errors.Errorf("Bad commit version: %d, expected: 0", req.CommitVersion)
		}
		for _, k := range req.Keys {
			if bytes.Equal(k, []byte("a")) || bytes.Equal(k, []byte("z")) {
				atomic.StoreInt64(&gotResolve, 1)
			} else {
				atomic.StoreInt64(&gotOther, 1)
			}
		}
		resp := kvrpcpb.ResolveLockResponse{}
		return &tikvrpc.Response{Resp: &resp}, nil
	}

	lock.TxnID = ts
	lock.MinCommitTS = ts + 5

	err = resolver.ResolveAsyncCommitLock(s.bo, lock, status)
	s.Nil(err)
	s.Equal(gotCheckA, int64(1))
	s.Equal(gotCheckB, int64(1))
	s.Equal(gotResolve, int64(1))
	s.Equal(gotOther, int64(0))
}

func (s *testAsyncCommitSuite) TestRepeatableRead() {
	var sessionID uint64 = 0
	test := func(isPessimistic bool) {
		s.putKV([]byte("k1"), []byte("v1"), true)

		sessionID++
		ctx := context.WithValue(context.Background(), util.SessionID, sessionID)
		txn1 := s.beginAsyncCommit()
		txn1.SetPessimistic(isPessimistic)
		s.mustGetFromTxn(txn1, []byte("k1"), []byte("v1"))
		txn1.Set([]byte("k1"), []byte("v2"))

		for i := 0; i < 20; i++ {
			_, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			s.Nil(err)
		}

		txn2 := s.beginAsyncCommit()
		s.mustGetFromTxn(txn2, []byte("k1"), []byte("v1"))

		err := txn1.Commit(ctx)
		s.Nil(err)
		// Check txn1 is committed in async commit.
		s.True(txn1.IsAsyncCommit())
		s.mustGetFromTxn(txn2, []byte("k1"), []byte("v1"))
		err = txn2.Rollback()
		s.Nil(err)

		txn3 := s.beginAsyncCommit()
		s.mustGetFromTxn(txn3, []byte("k1"), []byte("v2"))
		err = txn3.Rollback()
		s.Nil(err)
	}

	test(false)
	test(true)
}

// It's just a simple validation of linearizability.
// Extra tests are needed to test this feature with the control of the TiKV cluster.
func (s *testAsyncCommitSuite) TestAsyncCommitLinearizability() {
	t1 := s.beginAsyncCommitWithLinearizability()
	t2 := s.beginAsyncCommitWithLinearizability()
	err := t1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = t2.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	// t2 commits earlier than t1
	err = t2.Commit(ctx)
	s.Nil(err)
	err = t1.Commit(ctx)
	s.Nil(err)
	commitTS1 := t1.GetCommitTS()
	commitTS2 := t2.GetCommitTS()
	s.Less(commitTS2, commitTS1)
}

// TestAsyncCommitWithMultiDC tests that async commit can only be enabled in global transactions
func (s *testAsyncCommitSuite) TestAsyncCommitWithMultiDC() {
	// It requires setting placement rules to run with TiKV
	if *withTiKV {
		return
	}

	localTxn := s.beginAsyncCommit()
	err := localTxn.Set([]byte("a"), []byte("a1"))
	localTxn.SetScope("bj")
	s.Nil(err)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = localTxn.Commit(ctx)
	s.Nil(err)
	s.False(localTxn.IsAsyncCommit())

	globalTxn := s.beginAsyncCommit()
	err = globalTxn.Set([]byte("b"), []byte("b1"))
	globalTxn.SetScope(oracle.GlobalTxnScope)
	s.Nil(err)
	err = globalTxn.Commit(ctx)
	s.Nil(err)
	s.True(globalTxn.IsAsyncCommit())
}

func (s *testAsyncCommitSuite) TestResolveTxnFallbackFromAsyncCommit() {
	keys := [][]byte{[]byte("k0"), []byte("k1")}
	values := [][]byte{[]byte("v00"), []byte("v10")}
	initTest := func() transaction.CommitterProbe {
		t0 := s.begin()
		err := t0.Set(keys[0], values[0])
		s.Nil(err)
		err = t0.Set(keys[1], values[1])
		s.Nil(err)
		err = t0.Commit(context.Background())
		s.Nil(err)

		t1 := s.beginAsyncCommit()
		err = t1.Set(keys[0], []byte("v01"))
		s.Nil(err)
		err = t1.Set(keys[1], []byte("v11"))
		s.Nil(err)

		committer, err := t1.NewCommitter(1)
		s.Nil(err)
		committer.SetLockTTL(1)
		committer.SetUseAsyncCommit()
		return committer
	}
	prewriteKey := func(committer transaction.CommitterProbe, idx int, fallback bool) {
		bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
		loc, err := s.store.GetRegionCache().LocateKey(bo, keys[idx])
		s.Nil(err)
		req := committer.BuildPrewriteRequest(loc.Region.GetID(), loc.Region.GetConfVer(), loc.Region.GetVer(),
			committer.GetMutations().Slice(idx, idx+1), 1)
		if fallback {
			req.Req.(*kvrpcpb.PrewriteRequest).MaxCommitTs = 1
		}
		resp, err := s.store.SendReq(bo, req, loc.Region, 5000)
		s.Nil(err)
		s.NotNil(resp.Resp)
	}
	readKey := func(idx int) {
		t2 := s.begin()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		val, err := t2.Get(ctx, keys[idx])
		s.Nil(err)
		s.Equal(val, values[idx])
	}

	// Case 1: Fallback primary, read primary
	committer := initTest()
	prewriteKey(committer, 0, true)
	prewriteKey(committer, 1, false)
	readKey(0)
	readKey(1)

	// Case 2: Fallback primary, read secondary
	committer = initTest()
	prewriteKey(committer, 0, true)
	prewriteKey(committer, 1, false)
	readKey(1)
	readKey(0)

	// Case 3: Fallback secondary, read primary
	committer = initTest()
	prewriteKey(committer, 0, false)
	prewriteKey(committer, 1, true)
	readKey(0)
	readKey(1)

	// Case 4: Fallback secondary, read secondary
	committer = initTest()
	prewriteKey(committer, 0, false)
	prewriteKey(committer, 1, true)
	readKey(1)
	readKey(0)

	// Case 5: Fallback both, read primary
	committer = initTest()
	prewriteKey(committer, 0, true)
	prewriteKey(committer, 1, true)
	readKey(0)
	readKey(1)

	// Case 6: Fallback both, read secondary
	committer = initTest()
	prewriteKey(committer, 0, true)
	prewriteKey(committer, 1, true)
	readKey(1)
	readKey(0)
}

type mockResolveClient struct {
	tikv.Client
	onResolveLock      func(*kvrpcpb.ResolveLockRequest) (*tikvrpc.Response, error)
	onCheckSecondaries func(*kvrpcpb.CheckSecondaryLocksRequest) (*tikvrpc.Response, error)
}

func (m *mockResolveClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// Intercept check secondary locks and resolve lock messages if the callback is non-nil.
	// If the callback returns (nil, nil), forward to the inner client.
	if cr, ok := req.Req.(*kvrpcpb.CheckSecondaryLocksRequest); ok && m.onCheckSecondaries != nil {
		result, err := m.onCheckSecondaries(cr)
		if result != nil || err != nil {
			return result, err
		}
	} else if rr, ok := req.Req.(*kvrpcpb.ResolveLockRequest); ok && m.onResolveLock != nil {
		result, err := m.onResolveLock(rr)
		if result != nil || err != nil {
			return result, err
		}
	}
	return m.Client.SendRequest(ctx, addr, req, timeout)
}

// TestPessimisticTxnResolveAsyncCommitLock tests that pessimistic transactions resolve non-expired async-commit locks during the prewrite phase.
// Pessimistic transactions will resolve locks immediately during the prewrite phase because of the special logic for handling non-pessimistic lock conflict.
// However, async-commit locks can't be resolved until they expire. This test covers it.
func (s *testAsyncCommitSuite) TestPessimisticTxnResolveAsyncCommitLock() {
	ctx := context.Background()
	k := []byte("k")

	// Lock the key with an async-commit lock.
	s.lockKeysWithAsyncCommit([][]byte{}, [][]byte{}, k, k, false)

	txn, err := s.store.Begin()
	s.Nil(err)
	txn.SetPessimistic(true)
	err = txn.LockKeys(ctx, &kv.LockCtx{ForUpdateTS: txn.StartTS()}, []byte("k1"))
	s.Nil(err)

	txn.Set(k, k)
	err = txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testAsyncCommitSuite) TestRollbackAsyncCommitEnforcesFallback() {
	// This test doesn't support tikv mode.

	t1 := s.beginAsyncCommit()
	t1.SetPessimistic(true)
	t1.Set([]byte("a"), []byte("a"))
	t1.Set([]byte("z"), []byte("z"))
	committer, err := t1.NewCommitter(1)
	s.Nil(err)
	committer.SetUseAsyncCommit()
	committer.SetLockTTL(1000)
	committer.SetMaxCommitTS(oracle.ComposeTS(oracle.ExtractPhysical(committer.GetStartTS())+1500, 0))
	committer.PrewriteMutations(context.Background(), committer.GetMutations().Slice(0, 1))
	s.True(committer.IsAsyncCommit())
	lock := s.mustGetLock([]byte("a"))
	resolver := tikv.NewLockResolverProb(s.store.GetLockResolver())
	for {
		currentTS, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
		status, err := resolver.GetTxnStatus(s.bo, lock.TxnID, []byte("a"), currentTS, currentTS, false, false, nil)
		s.Nil(err)
		if status.IsRolledBack() {
			break
		}
		time.Sleep(time.Millisecond * 30)
	}
	s.True(committer.IsAsyncCommit())
	committer.PrewriteMutations(context.Background(), committer.GetMutations().Slice(1, 2))
	s.False(committer.IsAsyncCommit())
}
