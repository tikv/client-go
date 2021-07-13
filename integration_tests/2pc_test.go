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
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/2pc_test.go
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/2pc_fail_test.go
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/2pc_slow_test.go
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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ninedraft/israce"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/terror"
	drivertxn "github.com/pingcap/tidb/store/driver/txn"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

var (
	txnCommitBatchSize = tikv.ConfigProbe{}.GetTxnCommitBatchSize()
	bigTxnThreshold    = tikv.ConfigProbe{}.GetBigTxnThreshold()
)

func TestCommitter(t *testing.T) {
	suite.Run(t, new(testCommitterSuite))
}

type testCommitterSuite struct {
	suite.Suite
	cluster testutils.Cluster
	store   tikv.StoreProbe
}

func (s *testCommitterSuite) SetupSuite() {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // 3s
	atomic.StoreUint64(&tikv.CommitMaxBackoff, 1000)
	atomic.StoreUint64(&tikv.VeryLongMaxBackoff, 1000)
}

func (s *testCommitterSuite) TearDownSuite() {
	atomic.StoreUint64(&tikv.CommitMaxBackoff, 20000)
	atomic.StoreUint64(&tikv.VeryLongMaxBackoff, 600000)
}

func (s *testCommitterSuite) SetupTest() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	s.Require().Nil(err)
	testutils.BootstrapWithMultiRegions(cluster, []byte("a"), []byte("b"), []byte("c"))
	s.cluster = cluster
	pdCli := &tikv.CodecPDClient{Client: pdClient}
	spkv := tikv.NewMockSafePointKV()
	store, err := tikv.NewKVStore("mocktikv-store", pdCli, spkv, client)
	store.EnableTxnLocalLatches(8096)
	s.Require().Nil(err)

	s.store = tikv.StoreProbe{KVStore: store}
}

func (s *testCommitterSuite) TearDownTest() {
	s.store.Close()
}

func (s *testCommitterSuite) begin() tikv.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func (s *testCommitterSuite) beginAsyncCommit() tikv.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	txn.SetEnableAsyncCommit(true)
	return txn
}

func (s *testCommitterSuite) checkValues(m map[string]string) {
	txn := s.begin()
	for k, v := range m {
		val, err := txn.Get(context.TODO(), []byte(k))
		s.Nil(err)
		s.Equal(string(val), v)
	}
}

func (s *testCommitterSuite) mustCommit(m map[string]string) {
	txn := s.begin()
	for k, v := range m {
		err := txn.Set([]byte(k), []byte(v))
		s.Nil(err)
	}
	err := txn.Commit(context.Background())
	s.Nil(err)

	s.checkValues(m)
}

func randKV(keyLen, valLen int) (string, string) {
	const letters = "abc"
	k, v := make([]byte, keyLen), make([]byte, valLen)
	for i := range k {
		k[i] = letters[rand.Intn(len(letters))]
	}
	for i := range v {
		v[i] = letters[rand.Intn(len(letters))]
	}
	return string(k), string(v)
}

func (s *testCommitterSuite) TestDeleteYourWritesTTL() {
	conf := *config.GetGlobalConfig()
	oldConf := conf
	defer config.StoreGlobalConfig(&oldConf)
	conf.TiKVClient.TTLRefreshedTxnSize = 0
	config.StoreGlobalConfig(&conf)

	{
		txn := s.begin()
		err := txn.GetMemBuffer().SetWithFlags([]byte("bb"), []byte{0}, kv.SetPresumeKeyNotExists)
		s.Nil(err)
		err = txn.Set([]byte("ba"), []byte{1})
		s.Nil(err)
		err = txn.Delete([]byte("bb"))
		s.Nil(err)
		committer, err := txn.NewCommitter(0)
		s.Nil(err)
		err = committer.PrewriteAllMutations(context.Background())
		s.Nil(err)
		s.True(committer.IsTTLRunning())
	}

	{
		txn := s.begin()
		err := txn.GetMemBuffer().SetWithFlags([]byte("dd"), []byte{0}, kv.SetPresumeKeyNotExists)
		s.Nil(err)
		err = txn.Set([]byte("de"), []byte{1})
		s.Nil(err)
		err = txn.Delete([]byte("dd"))
		s.Nil(err)
		committer, err := txn.NewCommitter(0)
		s.Nil(err)
		err = committer.PrewriteAllMutations(context.Background())
		s.Nil(err)
		s.True(committer.IsTTLRunning())
	}
}

func (s *testCommitterSuite) TestCommitRollback() {
	s.mustCommit(map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
	})

	txn := s.begin()
	txn.Set([]byte("a"), []byte("a1"))
	txn.Set([]byte("b"), []byte("b1"))
	txn.Set([]byte("c"), []byte("c1"))

	s.mustCommit(map[string]string{
		"c": "c2",
	})

	err := txn.Commit(context.Background())
	s.NotNil(err)

	s.checkValues(map[string]string{
		"a": "a",
		"b": "b",
		"c": "c2",
	})
}

func (s *testCommitterSuite) TestPrewriteRollback() {
	s.mustCommit(map[string]string{
		"a": "a0",
		"b": "b0",
	})
	ctx := context.Background()
	txn1 := s.begin()
	err := txn1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = txn1.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	committer, err := txn1.NewCommitter(0)
	s.Nil(err)
	err = committer.PrewriteAllMutations(ctx)
	s.Nil(err)

	txn2 := s.begin()
	v, err := txn2.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	s.Equal(v, []byte("a0"))

	err = committer.PrewriteAllMutations(ctx)
	if err != nil {
		// Retry.
		txn1 = s.begin()
		err = txn1.Set([]byte("a"), []byte("a1"))
		s.Nil(err)
		err = txn1.Set([]byte("b"), []byte("b1"))
		s.Nil(err)
		committer, err = txn1.NewCommitter(0)
		s.Nil(err)
		err = committer.PrewriteAllMutations(ctx)
		s.Nil(err)
	}
	commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.Nil(err)
	committer.SetCommitTS(commitTS)
	err = committer.CommitMutations(ctx)
	s.Nil(err)

	txn3 := s.begin()
	v, err = txn3.Get(context.TODO(), []byte("b"))
	s.Nil(err)
	s.Equal(v, []byte("b1"))
}

func (s *testCommitterSuite) TestContextCancel() {
	txn1 := s.begin()
	err := txn1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = txn1.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	committer, err := txn1.NewCommitter(0)
	s.Nil(err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel the context
	err = committer.PrewriteAllMutations(ctx)
	s.Equal(errors.Cause(err), context.Canceled)
}

func (s *testCommitterSuite) TestContextCancel2() {
	txn := s.begin()
	err := txn.Set([]byte("a"), []byte("a"))
	s.Nil(err)
	err = txn.Set([]byte("b"), []byte("b"))
	s.Nil(err)
	ctx, cancel := context.WithCancel(context.Background())
	err = txn.Commit(ctx)
	s.Nil(err)
	cancel()
	// Secondary keys should not be canceled.
	s.Eventually(func() bool {
		return !s.isKeyLocked([]byte("b"))
	}, 2*time.Second, 20*time.Millisecond, "Secondary locks are not committed after 2 seconds")
}

func (s *testCommitterSuite) TestContextCancelRetryable() {
	txn1, txn2, txn3 := s.begin(), s.begin(), s.begin()
	// txn1 locks "b"
	err := txn1.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	committer, err := txn1.NewCommitter(0)
	s.Nil(err)
	err = committer.PrewriteAllMutations(context.Background())
	s.Nil(err)
	// txn3 writes "c"
	err = txn3.Set([]byte("c"), []byte("c3"))
	s.Nil(err)
	err = txn3.Commit(context.Background())
	s.Nil(err)
	// txn2 writes "a"(PK), "b", "c" on different regions.
	// "c" will return a retryable error.
	// "b" will get a Locked error first, then the context must be canceled after backoff for lock.
	err = txn2.Set([]byte("a"), []byte("a2"))
	s.Nil(err)
	err = txn2.Set([]byte("b"), []byte("b2"))
	s.Nil(err)
	err = txn2.Set([]byte("c"), []byte("c2"))
	s.Nil(err)
	err = txn2.Commit(context.Background())
	s.NotNil(err)
	_, ok := err.(*tikverr.ErrWriteConflictInLatch)
	s.True(ok, fmt.Sprintf("err: %s", err))
}

func (s *testCommitterSuite) TestContextCancelCausingUndetermined() {
	// For a normal transaction, if RPC returns context.Canceled error while sending commit
	// requests, the transaction should go to the undetermined state.
	txn := s.begin()
	err := txn.Set([]byte("a"), []byte("va"))
	s.Nil(err)
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.PrewriteAllMutations(context.Background())
	s.Nil(err)

	s.Nil(failpoint.Enable("tikvclient/rpcContextCancelErr", `return(true)`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcContextCancelErr"))
	}()

	err = committer.CommitMutations(context.Background())
	s.NotNil(committer.GetUndeterminedErr())
	s.Equal(errors.Cause(err), context.Canceled)
}

func (s *testCommitterSuite) mustGetRegionID(key []byte) uint64 {
	loc, err := s.store.GetRegionCache().LocateKey(tikv.NewBackofferWithVars(context.Background(), 500, nil), key)
	s.Nil(err)
	return loc.Region.GetID()
}

func (s *testCommitterSuite) isKeyLocked(key []byte) bool {
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	bo := tikv.NewBackofferWithVars(context.Background(), 500, nil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver,
	})
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	s.Nil(err)
	resp, err := s.store.SendReq(bo, req, loc.Region, 5000)
	s.Nil(err)
	s.NotNil(resp.Resp)
	keyErr := (resp.Resp.(*kvrpcpb.GetResponse)).GetError()
	return keyErr.GetLocked() != nil
}

func (s *testCommitterSuite) TestPrewriteCancel() {
	// Setup region delays for key "b" and "c".
	delays := map[uint64]time.Duration{
		s.mustGetRegionID([]byte("b")): time.Millisecond * 10,
		s.mustGetRegionID([]byte("c")): time.Millisecond * 20,
	}
	s.store.SetTiKVClient(&slowClient{
		Client:       s.store.GetTiKVClient(),
		regionDelays: delays,
	})

	txn1, txn2 := s.begin(), s.begin()
	// txn2 writes "b"
	err := txn2.Set([]byte("b"), []byte("b2"))
	s.Nil(err)
	err = txn2.Commit(context.Background())
	s.Nil(err)
	// txn1 writes "a"(PK), "b", "c" on different regions.
	// "b" will return an error and cancel commit.
	err = txn1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = txn1.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	err = txn1.Set([]byte("c"), []byte("c1"))
	s.Nil(err)
	err = txn1.Commit(context.Background())
	s.NotNil(err)
	// "c" should be cleaned up in reasonable time.
	s.Eventually(func() bool {
		return !s.isKeyLocked([]byte("c"))
	}, 500*time.Millisecond, 10*time.Millisecond)
}

// slowClient wraps rpcClient and makes some regions respond with delay.
type slowClient struct {
	tikv.Client
	regionDelays map[uint64]time.Duration
}

func (c *slowClient) SendReq(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	for id, delay := range c.regionDelays {
		reqCtx := &req.Context
		if reqCtx.GetRegionId() == id {
			time.Sleep(delay)
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (s *testCommitterSuite) TestIllegalTso() {
	txn := s.begin()
	data := map[string]string{
		"name": "aa",
		"age":  "12",
	}
	for k, v := range data {
		err := txn.Set([]byte(k), []byte(v))
		s.Nil(err)
	}
	// make start ts bigger.
	txn.SetStartTS(math.MaxUint64)
	err := txn.Commit(context.Background())
	s.NotNil(err)
	s.errMsgMustContain(err, "invalid txnStartTS")
}

func (s *testCommitterSuite) errMsgMustContain(err error, msg string) {
	s.Contains(err.Error(), msg)
}

func (s *testCommitterSuite) TestCommitBeforePrewrite() {
	txn := s.begin()
	err := txn.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	ctx := context.Background()
	committer.Cleanup(ctx)
	err = committer.PrewriteAllMutations(ctx)
	s.NotNil(err)
	s.errMsgMustContain(err, "already rolled back")
}

func (s *testCommitterSuite) TestPrewritePrimaryKeyFailed() {
	// commit (a,a1)
	txn1 := s.begin()
	err := txn1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = txn1.Commit(context.Background())
	s.Nil(err)

	// check a
	txn := s.begin()
	v, err := txn.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	s.Equal(v, []byte("a1"))

	// set txn2's startTs before txn1's
	txn2 := s.begin()
	txn2.SetStartTS(txn1.StartTS() - 1)
	err = txn2.Set([]byte("a"), []byte("a2"))
	s.Nil(err)
	err = txn2.Set([]byte("b"), []byte("b2"))
	s.Nil(err)
	// prewrite:primary a failed, b success
	err = txn2.Commit(context.Background())
	s.NotNil(err)

	// txn2 failed with a rollback for record a.
	txn = s.begin()
	v, err = txn.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	s.Equal(v, []byte("a1"))
	_, err = txn.Get(context.TODO(), []byte("b"))
	s.True(tikverr.IsErrNotFound(err))

	// clean again, shouldn't be failed when a rollback already exist.
	ctx := context.Background()
	committer, err := txn2.NewCommitter(0)
	s.Nil(err)
	committer.Cleanup(ctx)

	// check the data after rollback twice.
	txn = s.begin()
	v, err = txn.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	s.Equal(v, []byte("a1"))

	// update data in a new txn, should be success.
	err = txn.Set([]byte("a"), []byte("a3"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	// check value
	txn = s.begin()
	v, err = txn.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	s.Equal(v, []byte("a3"))
}

func (s *testCommitterSuite) TestWrittenKeysOnConflict() {
	// This test checks that when there is a write conflict, written keys is collected,
	// so we can use it to clean up keys.
	region, _ := s.cluster.GetRegionByKey([]byte("x"))
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte("y"), []uint64{newPeerID}, newPeerID)
	var totalTime time.Duration
	for i := 0; i < 10; i++ {
		txn1 := s.begin()
		txn2 := s.begin()
		txn2.Set([]byte("x1"), []byte("1"))
		committer2, err := txn2.NewCommitter(2)
		s.Nil(err)
		err = committer2.Execute(context.Background())
		s.Nil(err)
		txn1.Set([]byte("x1"), []byte("1"))
		txn1.Set([]byte("y1"), []byte("2"))
		committer1, err := txn1.NewCommitter(2)
		s.Nil(err)
		err = committer1.Execute(context.Background())
		s.NotNil(err)
		committer1.WaitCleanup()
		txn3 := s.begin()
		start := time.Now()
		txn3.Get(context.TODO(), []byte("y1"))
		totalTime += time.Since(start)
		txn3.Commit(context.Background())
	}
	s.Less(totalTime, time.Millisecond*200)
}

func (s *testCommitterSuite) TestPrewriteTxnSize() {
	// Prepare two regions first: (, 100) and [100, )
	region, _ := s.cluster.GetRegionByKey([]byte{50})
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte{100}, []uint64{newPeerID}, newPeerID)

	txn := s.begin()
	var val [1024]byte
	for i := byte(50); i < 120; i++ {
		err := txn.Set([]byte{i}, val[:])
		s.Nil(err)
	}

	committer, err := txn.NewCommitter(1)
	s.Nil(err)

	ctx := context.Background()
	err = committer.PrewriteAllMutations(ctx)
	s.Nil(err)

	// Check the written locks in the first region (50 keys)
	for i := byte(50); i < 100; i++ {
		lock := s.getLockInfo([]byte{i})
		s.Equal(int(lock.TxnSize), 50)
	}

	// Check the written locks in the second region (20 keys)
	for i := byte(100); i < 120; i++ {
		lock := s.getLockInfo([]byte{i})
		s.Equal(int(lock.TxnSize), 20)
	}
}

func (s *testCommitterSuite) TestRejectCommitTS() {
	txn := s.begin()
	s.Nil(txn.Set([]byte("x"), []byte("v")))

	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte("x"))
	s.Nil(err)
	mutations := []*kvrpcpb.Mutation{
		{
			Op:    committer.GetMutations().GetOp(0),
			Key:   committer.GetMutations().GetKey(0),
			Value: committer.GetMutations().GetValue(0),
		},
	}
	prewrite := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  committer.GetPrimaryKey(),
		StartVersion: committer.GetStartTS(),
		LockTtl:      committer.GetLockTTL(),
		MinCommitTs:  committer.GetStartTS() + 100, // Set minCommitTS
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, prewrite)
	_, err = s.store.SendReq(bo, req, loc.Region, 5000)
	s.Nil(err)

	// Make commitTS less than minCommitTS.
	committer.SetCommitTS(committer.GetStartTS() + 1)
	// Ensure that the new commit ts is greater than minCommitTS when retry
	time.Sleep(3 * time.Millisecond)
	err = committer.CommitMutations(context.Background())
	s.Nil(err)

	// Use startTS+2 to read the data and get nothing.
	// Use max.Uint64 to read the data and success.
	// That means the final commitTS > startTS+2, it's not the one we provide.
	// So we cover the rety commitTS logic.
	txn1, err := s.store.BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(committer.GetStartTS() + 2))
	s.Nil(err)
	_, err = txn1.Get(bo.GetCtx(), []byte("x"))
	s.True(tikverr.IsErrNotFound(err))

	txn2, err := s.store.BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(math.MaxUint64))
	s.Nil(err)
	val, err := txn2.Get(bo.GetCtx(), []byte("x"))
	s.Nil(err)
	s.True(bytes.Equal(val, []byte("v")))
}

func (s *testCommitterSuite) TestPessimisticPrewriteRequest() {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	txn := s.begin()
	txn.SetPessimistic(true)
	err := txn.Set([]byte("t1"), []byte("v1"))
	s.Nil(err)
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.SetForUpdateTS(100)
	req := committer.BuildPrewriteRequest(1, 1, 1, committer.GetMutations().Slice(0, 1), 1)
	s.Greater(len(req.Prewrite().IsPessimisticLock), 0)
	s.Equal(req.Prewrite().ForUpdateTs, uint64(100))
}

func (s *testCommitterSuite) TestUnsetPrimaryKey() {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	key := []byte("key")
	txn := s.begin()
	s.Nil(txn.Set(key, key))
	s.Nil(txn.Commit(context.Background()))

	txn = s.begin()
	txn.SetPessimistic(true)
	_, _ = txn.GetUnionStore().Get(context.TODO(), key)
	s.Nil(txn.GetMemBuffer().SetWithFlags(key, key, kv.SetPresumeKeyNotExists))
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	s.NotNil(err)
	s.Nil(txn.Delete(key))
	key2 := []byte("key2")
	s.Nil(txn.Set(key2, key2))
	err = txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testCommitterSuite) TestPessimisticLockedKeysDedup() {
	txn := s.begin()
	txn.SetPessimistic(true)
	lockCtx := &kv.LockCtx{ForUpdateTS: 100, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, []byte("abc"), []byte("def"))
	s.Nil(err)
	lockCtx = &kv.LockCtx{ForUpdateTS: 100, WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, []byte("abc"), []byte("def"))
	s.Nil(err)
	s.Len(txn.CollectLockedKeys(), 2)
}

func (s *testCommitterSuite) TestPessimisticTTL() {
	key := []byte("key")
	txn := s.begin()
	txn.SetPessimistic(true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	s.Nil(err)
	time.Sleep(time.Millisecond * 100)
	key2 := []byte("key2")
	lockCtx = &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, key2)
	s.Nil(err)
	lockInfo := s.getLockInfo(key)
	msBeforeLockExpired := s.store.GetOracle().UntilExpired(txn.StartTS(), lockInfo.LockTtl, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	s.GreaterOrEqual(msBeforeLockExpired, int64(100))

	lr := s.store.NewLockResolver()
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	status, err := lr.GetTxnStatus(bo, txn.StartTS(), key2, 0, txn.StartTS(), true, false, nil)
	s.Nil(err)
	s.GreaterOrEqual(status.TTL(), lockInfo.LockTtl)

	// Check primary lock TTL is auto increasing while the pessimistic txn is ongoing.
	check := func() bool {
		lockInfoNew := s.getLockInfo(key)
		if lockInfoNew.LockTtl > lockInfo.LockTtl {
			currentTS, err := s.store.GetOracle().GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			s.Nil(err)
			// Check that the TTL is update to a reasonable range.
			expire := oracle.ExtractPhysical(txn.StartTS()) + int64(lockInfoNew.LockTtl)
			now := oracle.ExtractPhysical(currentTS)
			s.True(expire > now)
			s.True(uint64(expire-now) <= atomic.LoadUint64(&tikv.ManagedLockTTL))
			return true
		}
		return false
	}
	s.Eventually(check, 5*time.Second, 100*time.Millisecond)
}

func (s *testCommitterSuite) TestPessimisticLockReturnValues() {
	key := []byte("key")
	key2 := []byte("key2")
	txn := s.begin()
	s.Nil(txn.Set(key, key))
	s.Nil(txn.Set(key2, key2))
	s.Nil(txn.Commit(context.Background()))
	txn = s.begin()
	txn.SetPessimistic(true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	lockCtx.InitReturnValues(2)
	s.Nil(txn.LockKeys(context.Background(), lockCtx, key, key2))
	s.Len(lockCtx.Values, 2)
	s.Equal(lockCtx.Values[string(key)].Value, key)
	s.Equal(lockCtx.Values[string(key2)].Value, key2)
}

// TestElapsedTTL tests that elapsed time is correct even if ts physical time is greater than local time.
func (s *testCommitterSuite) TestElapsedTTL() {
	key := []byte("key")
	txn := s.begin()
	txn.SetStartTS(oracle.GoTimeToTS(time.Now().Add(time.Second*10)) + 1)
	txn.SetPessimistic(true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &kv.LockCtx{
		ForUpdateTS:   oracle.ComposeTS(oracle.ExtractPhysical(txn.StartTS())+100, 1),
		WaitStartTime: time.Now(),
	}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	s.Nil(err)
	lockInfo := s.getLockInfo(key)
	s.GreaterOrEqual(lockInfo.LockTtl-atomic.LoadUint64(&tikv.ManagedLockTTL), uint64(100))
	s.Less(lockInfo.LockTtl-atomic.LoadUint64(&tikv.ManagedLockTTL), uint64(150))
}

func (s *testCommitterSuite) TestDeleteYourWriteCauseGhostPrimary() {
	s.cluster.SplitKeys([]byte("d"), []byte("a"), 4)
	k1 := []byte("a") // insert but deleted key at first pos in txn1
	k2 := []byte("b") // insert key at second pos in txn1
	k3 := []byte("c") // insert key in txn1 and will be conflict read by txn2

	// insert k1, k2, k3 and delete k1
	txn1 := s.begin()
	txn1.SetPessimistic(false)
	s.store.ClearTxnLatches()
	txn1.Get(context.Background(), k1)
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, kv.SetPresumeKeyNotExists)
	txn1.Set(k2, []byte{1})
	txn1.Set(k3, []byte{2})
	txn1.Delete(k1)
	committer1, err := txn1.NewCommitter(0)
	s.Nil(err)
	// setup test knob in txn's committer
	ac, bk := make(chan struct{}), make(chan struct{})
	committer1.SetPrimaryKeyBlocker(ac, bk)
	txn1.SetCommitter(committer1)
	var txn1Done sync.WaitGroup
	txn1Done.Add(1)
	go func() {
		err1 := txn1.Commit(context.Background())
		s.Nil(err1)
		txn1Done.Done()
	}()
	// resume after after primary key be committed
	<-ac

	// start txn2 to read k3(prewrite success and primary should be committed)
	txn2 := s.begin()
	txn2.SetPessimistic(false)
	s.store.ClearTxnLatches()
	v, err := txn2.Get(context.Background(), k3)
	s.Nil(err) // should resolve lock and read txn1 k3 result instead of rollback it.
	s.Equal(v[0], byte(2))
	bk <- struct{}{}
	txn1Done.Wait()
}

func (s *testCommitterSuite) TestDeleteAllYourWrites() {
	s.cluster.SplitKeys([]byte("d"), []byte("a"), 4)
	k1 := []byte("a")
	k2 := []byte("b")
	k3 := []byte("c")

	// insert k1, k2, k3 and delete k1, k2, k3
	txn1 := s.begin()
	txn1.SetPessimistic(false)
	s.store.ClearTxnLatches()
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k1)
	txn1.GetMemBuffer().SetWithFlags(k2, []byte{1}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k2)
	txn1.GetMemBuffer().SetWithFlags(k3, []byte{2}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k3)
	err1 := txn1.Commit(context.Background())
	s.Nil(err1)
}

func (s *testCommitterSuite) TestDeleteAllYourWritesWithSFU() {
	s.cluster.SplitKeys([]byte("d"), []byte("a"), 4)
	k1 := []byte("a")
	k2 := []byte("b")
	k3 := []byte("c")

	// insert k1, k2, k2 and delete k1
	txn1 := s.begin()
	txn1.SetPessimistic(false)
	s.store.ClearTxnLatches()
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k1)
	err := txn1.LockKeys(context.Background(), &kv.LockCtx{}, k2, k3) // select * from t where x in (k2, k3) for update
	s.Nil(err)

	committer1, err := txn1.NewCommitter(0)
	s.Nil(err)
	// setup test knob in txn's committer
	ac, bk := make(chan struct{}), make(chan struct{})
	committer1.SetPrimaryKeyBlocker(ac, bk)
	txn1.SetCommitter(committer1)
	var txn1Done sync.WaitGroup
	txn1Done.Add(1)
	go func() {
		err1 := txn1.Commit(context.Background())
		s.Nil(err1)
		txn1Done.Done()
	}()
	// resume after after primary key be committed
	<-ac
	// start txn2 to read k3
	txn2 := s.begin()
	txn2.SetPessimistic(false)
	s.store.ClearTxnLatches()
	err = txn2.Set(k3, []byte{33})
	s.Nil(err)
	var meetLocks []*tikv.Lock
	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	resolver.SetMeetLockCallback(func(locks []*tikv.Lock) {
		meetLocks = append(meetLocks, locks...)
	})
	err = txn2.Commit(context.Background())
	s.Nil(err)
	bk <- struct{}{}
	txn1Done.Wait()
	s.Equal(meetLocks[0].Primary[0], k2[0])
}

// TestAcquireFalseTimeoutLock tests acquiring a key which is a secondary key of another transaction.
// The lock's own TTL is expired but the primary key is still alive due to heartbeats.
func (s *testCommitterSuite) TestAcquireFalseTimeoutLock() {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 1000)       // 1s
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // restore default test value

	// k1 is the primary lock of txn1
	k1 := []byte("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock
	k2 := []byte("k2")

	txn1 := s.begin()
	txn1.SetPessimistic(true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	s.Nil(err)
	// lock the secondary key
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k2)
	s.Nil(err)

	// Heartbeats will increase the TTL of the primary key

	// wait until secondary key exceeds its own TTL
	time.Sleep(time.Duration(atomic.LoadUint64(&tikv.ManagedLockTTL)) * time.Millisecond)
	txn2 := s.begin()
	txn2.SetPessimistic(true)

	// test no wait
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), LockWaitTime: tikv.LockNoWait, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire lock immediately thus error
	s.Equal(err.Error(), tikverr.ErrLockAcquireFailAndNoWaitSet.Error())

	// test for wait limited time (200ms)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), LockWaitTime: 200, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire lock in time thus error
	s.Equal(err.Error(), tikverr.ErrLockWaitTimeout.Error())
}

func (s *testCommitterSuite) getLockInfo(key []byte) *kvrpcpb.LockInfo {
	txn := s.begin()
	err := txn.Set(key, key)
	s.Nil(err)
	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	s.Nil(err)
	req := committer.BuildPrewriteRequest(loc.Region.GetID(), loc.Region.GetConfVer(), loc.Region.GetVer(), committer.GetMutations().Slice(0, 1), 1)
	resp, err := s.store.SendReq(bo, req, loc.Region, 5000)
	s.Nil(err)
	s.NotNil(resp.Resp)
	keyErrs := (resp.Resp.(*kvrpcpb.PrewriteResponse)).Errors
	s.Len(keyErrs, 1)
	locked := keyErrs[0].Locked
	s.NotNil(locked)
	return locked
}

func (s *testCommitterSuite) TestPkNotFound() {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // restore default value
	ctx := context.Background()
	// k1 is the primary lock of txn1.
	k1 := []byte("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock.
	k2 := []byte("k2")
	k3 := []byte("k3")

	txn1 := s.begin()
	txn1.SetPessimistic(true)
	// lock the primary key.
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(ctx, lockCtx, k1)
	s.Nil(err)
	// lock the secondary key.
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(ctx, lockCtx, k2, k3)
	s.Nil(err)
	// Stop txn ttl manager and remove primary key, like tidb server crashes and the priamry key lock does not exists actually,
	// while the secondary lock operation succeeded.
	txn1.GetCommitter().CloseTTLManager()

	var status tikv.TxnStatus
	bo := tikv.NewBackofferWithVars(ctx, 5000, nil)
	lockKey2 := &tikv.Lock{
		Key:             k2,
		Primary:         k1,
		TxnID:           txn1.StartTS(),
		TTL:             0, // let the primary lock k1 expire doing check.
		TxnSize:         txnCommitBatchSize,
		LockType:        kvrpcpb.Op_PessimisticLock,
		LockForUpdateTS: txn1.StartTS(),
	}
	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	status, err = resolver.GetTxnStatusFromLock(bo, lockKey2, oracle.GoTimeToTS(time.Now().Add(200*time.Millisecond)), false)
	s.Nil(err)
	s.Equal(status.Action(), kvrpcpb.Action_TTLExpirePessimisticRollback)

	// Txn2 tries to lock the secondary key k2, there should be no dead loop.
	// Since the resolving key k2 is a pessimistic lock, no rollback record should be written, and later lock
	// and the other secondary key k3 should succeed if there is no fail point enabled.
	status, err = resolver.GetTxnStatusFromLock(bo, lockKey2, oracle.GoTimeToTS(time.Now().Add(200*time.Millisecond)), false)
	s.Nil(err)
	s.Equal(status.Action(), kvrpcpb.Action_LockNotExistDoNothing)
	txn2 := s.begin()
	txn2.SetPessimistic(true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now()}
	err = txn2.LockKeys(ctx, lockCtx, k2)
	s.Nil(err)

	// Pessimistic rollback using smaller forUpdateTS does not take effect.
	lockKey3 := &tikv.Lock{
		Key:             k3,
		Primary:         k1,
		TxnID:           txn1.StartTS(),
		TTL:             tikv.ManagedLockTTL,
		TxnSize:         txnCommitBatchSize,
		LockType:        kvrpcpb.Op_PessimisticLock,
		LockForUpdateTS: txn1.StartTS() - 1,
	}
	err = resolver.ResolvePessimisticLock(ctx, lockKey3)
	s.Nil(err)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(ctx, lockCtx, k3)
	s.Nil(err)

	// After disable fail point, the rollbackIfNotExist flag will be set, and the resolve should succeed. In this
	// case, the returned action of TxnStatus should be LockNotExistDoNothing, and lock on k3 could be resolved.
	txn3 := s.begin()
	txn3.SetPessimistic(true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn3.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	err = txn3.LockKeys(ctx, lockCtx, k3)
	s.Nil(err)
	status, err = resolver.GetTxnStatusFromLock(bo, lockKey3, oracle.GoTimeToTS(time.Now().Add(200*time.Millisecond)), false)
	s.Nil(err)
	s.Equal(status.Action(), kvrpcpb.Action_LockNotExistDoNothing)
}

func (s *testCommitterSuite) TestPessimisticLockPrimary() {
	// a is the primary lock of txn1
	k1 := []byte("a")
	// b is a secondary lock of txn1 and a key txn2 wants to lock, b is on another region
	k2 := []byte("b")

	txn1 := s.begin()
	txn1.SetPessimistic(true)
	// txn1 lock k1
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	s.Nil(err)

	// txn2 wants to lock k1, k2, k1(pk) is blocked by txn1, pessimisticLockKeys has been changed to
	// lock primary key first and then secondary keys concurrently, k2 should not be locked by txn2
	doneCh := make(chan error)
	go func() {
		txn2 := s.begin()
		txn2.SetPessimistic(true)
		lockCtx2 := &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now(), LockWaitTime: 200}
		waitErr := txn2.LockKeys(context.Background(), lockCtx2, k1, k2)
		doneCh <- waitErr
	}()
	time.Sleep(50 * time.Millisecond)

	// txn3 should locks k2 successfully using no wait
	txn3 := s.begin()
	txn3.SetPessimistic(true)
	lockCtx3 := &kv.LockCtx{ForUpdateTS: txn3.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	s.Nil(failpoint.Enable("tikvclient/txnNotFoundRetTTL", "return"))
	err = txn3.LockKeys(context.Background(), lockCtx3, k2)
	s.Nil(failpoint.Disable("tikvclient/txnNotFoundRetTTL"))
	s.Nil(err)
	waitErr := <-doneCh
	s.Equal(tikverr.ErrLockWaitTimeout, waitErr)
}

func (s *testCommitterSuite) TestResolvePessimisticLock() {
	untouchedIndexKey := []byte("t00000001_i000000001")
	untouchedIndexValue := []byte{0, 0, 0, 0, 0, 0, 0, 1, 49}
	noValueIndexKey := []byte("t00000001_i000000002")
	txn := s.begin()
	txn.SetKVFilter(drivertxn.TiDBKVFilter{})
	err := txn.Set(untouchedIndexKey, untouchedIndexValue)
	s.Nil(err)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	err = txn.LockKeys(context.Background(), lockCtx, untouchedIndexKey, noValueIndexKey)
	s.Nil(err)
	commit, err := txn.NewCommitter(1)
	s.Nil(err)
	mutation := commit.MutationsOfKeys([][]byte{untouchedIndexKey, noValueIndexKey})
	s.Equal(mutation.Len(), 2)
	s.Equal(mutation.GetOp(0), kvrpcpb.Op_Lock)
	s.Equal(mutation.GetKey(0), untouchedIndexKey)
	s.Equal(mutation.GetValue(0), untouchedIndexValue)
	s.Equal(mutation.GetOp(1), kvrpcpb.Op_Lock)
	s.Equal(mutation.GetKey(1), noValueIndexKey)
	s.Len(mutation.GetValue(1), 0)
}

func (s *testCommitterSuite) TestCommitDeadLock() {
	// Split into two region and let k1 k2 in different regions.
	s.cluster.SplitKeys([]byte("z"), []byte("a"), 2)
	k1 := []byte("a_deadlock_k1")
	k2 := []byte("y_deadlock_k2")

	region1, _ := s.cluster.GetRegionByKey(k1)
	region2, _ := s.cluster.GetRegionByKey(k2)
	s.True(region1.Id != region2.Id)

	txn1 := s.begin()
	txn1.Set(k1, []byte("t1"))
	txn1.Set(k2, []byte("t1"))
	commit1, err := txn1.NewCommitter(1)
	s.Nil(err)
	commit1.SetPrimaryKey(k1)
	commit1.SetTxnSize(1000 * 1024 * 1024)

	txn2 := s.begin()
	txn2.Set(k1, []byte("t2"))
	txn2.Set(k2, []byte("t2"))
	commit2, err := txn2.NewCommitter(2)
	s.Nil(err)
	commit2.SetPrimaryKey(k2)
	commit2.SetTxnSize(1000 * 1024 * 1024)

	s.cluster.ScheduleDelay(txn2.StartTS(), region1.Id, 5*time.Millisecond)
	s.cluster.ScheduleDelay(txn1.StartTS(), region2.Id, 5*time.Millisecond)

	// Txn1 prewrites k1, k2 and txn2 prewrites k2, k1, the large txn
	// protocol run ttlManager and update their TTL, cause dead lock.
	ch := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ch <- commit2.Execute(context.Background())
		wg.Done()
	}()
	ch <- commit1.Execute(context.Background())
	wg.Wait()
	close(ch)

	res := 0
	for e := range ch {
		if e != nil {
			res++
		}
	}
	s.Equal(res, 1)
}

// TestPushPessimisticLock tests that push forward the minCommiTS of pessimistic locks.
func (s *testCommitterSuite) TestPushPessimisticLock() {
	// k1 is the primary key.
	k1, k2 := []byte("a"), []byte("b")
	ctx := context.Background()

	txn1 := s.begin()
	txn1.SetPessimistic(true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1, k2)
	s.Nil(err)

	txn1.Set(k2, []byte("v2"))
	committer := txn1.GetCommitter()
	err = committer.InitKeysAndMutations()
	s.Nil(err)
	// Strip the prewrite of the primary key.
	committer.SetMutations(committer.GetMutations().Slice(1, 2))
	s.Nil(err)
	err = committer.PrewriteAllMutations(ctx)
	s.Nil(err)
	// The primary lock is a pessimistic lock and the secondary lock is a optimistic lock.
	lock1 := s.getLockInfo(k1)
	s.Equal(lock1.LockType, kvrpcpb.Op_PessimisticLock)
	s.Equal(lock1.PrimaryLock, k1)
	lock2 := s.getLockInfo(k2)
	s.Equal(lock2.LockType, kvrpcpb.Op_Put)
	s.Equal(lock2.PrimaryLock, k1)

	txn2 := s.begin()
	start := time.Now()
	_, err = txn2.Get(ctx, k2)
	elapsed := time.Since(start)
	// The optimistic lock shouldn't block reads.
	s.Less(elapsed, 500*time.Millisecond)
	s.True(tikverr.IsErrNotFound(err))

	txn1.Rollback()
	txn2.Rollback()
}

// TestResolveMixed tests mixed resolve with left behind optimistic locks and pessimistic locks,
// using clean whole region resolve path
func (s *testCommitterSuite) TestResolveMixed() {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // restore default value
	ctx := context.Background()

	// pk is the primary lock of txn1
	pk := []byte("pk")
	secondaryLockkeys := make([][]byte, 0, bigTxnThreshold)
	for i := 0; i < bigTxnThreshold; i++ {
		optimisticLock := []byte(fmt.Sprintf("optimisticLockKey%d", i))
		secondaryLockkeys = append(secondaryLockkeys, optimisticLock)
	}
	pessimisticLockKey := []byte("pessimisticLockKey")

	// make the optimistic and pessimistic lock left with primary lock not found
	txn1 := s.begin()
	txn1.SetPessimistic(true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, pk)
	s.Nil(err)
	// lock the optimistic keys
	for i := 0; i < bigTxnThreshold; i++ {
		txn1.Set(secondaryLockkeys[i], []byte(fmt.Sprintf("v%d", i)))
	}
	committer := txn1.GetCommitter()
	err = committer.InitKeysAndMutations()
	s.Nil(err)
	err = committer.PrewriteAllMutations(ctx)
	s.Nil(err)
	// lock the pessimistic keys
	err = txn1.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	s.Nil(err)
	lock1 := s.getLockInfo(pessimisticLockKey)
	s.Equal(lock1.LockType, kvrpcpb.Op_PessimisticLock)
	s.Equal(lock1.PrimaryLock, pk)
	optimisticLockKey := secondaryLockkeys[0]
	lock2 := s.getLockInfo(optimisticLockKey)
	s.Equal(lock2.LockType, kvrpcpb.Op_Put)
	s.Equal(lock2.PrimaryLock, pk)

	// stop txn ttl manager and remove primary key, make the other keys left behind
	committer.CloseTTLManager()
	muts := tikv.NewPlainMutations(1)
	muts.Push(kvrpcpb.Op_Lock, pk, nil, true)
	err = committer.PessimisticRollbackMutations(context.Background(), &muts)
	s.Nil(err)

	// try to resolve the left optimistic locks, use clean whole region
	time.Sleep(time.Duration(atomic.LoadUint64(&tikv.ManagedLockTTL)) * time.Millisecond)
	optimisticLockInfo := s.getLockInfo(optimisticLockKey)
	lock := tikv.NewLock(optimisticLockInfo)
	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	err = resolver.ResolveLock(ctx, lock)
	s.Nil(err)

	// txn2 tries to lock the pessimisticLockKey, the lock should has been resolved in clean whole region resolve
	txn2 := s.begin()
	txn2.SetPessimistic(true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	err = txn2.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	s.Nil(err)

	err = txn1.Rollback()
	s.Nil(err)
	err = txn2.Rollback()
	s.Nil(err)
}

// TestSecondaryKeys tests that when async commit is enabled, each prewrite message includes an
// accurate list of secondary keys.
func (s *testCommitterSuite) TestPrewriteSecondaryKeys() {
	// Prepare two regions first: (, 100) and [100, )
	region, _ := s.cluster.GetRegionByKey([]byte{50})
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte{100}, []uint64{newPeerID}, newPeerID)

	txn := s.beginAsyncCommit()
	var val [1024]byte
	for i := byte(50); i < 120; i++ {
		err := txn.Set([]byte{i}, val[:])
		s.Nil(err)
	}
	// Some duplicates.
	for i := byte(50); i < 120; i += 10 {
		err := txn.Set([]byte{i}, val[512:700])
		s.Nil(err)
	}

	committer, err := txn.NewCommitter(1)
	s.Nil(err)

	mock := mockClient{inner: s.store.GetTiKVClient()}
	s.store.SetTiKVClient(&mock)
	ctx := context.Background()
	// TODO remove this when minCommitTS is returned from mockStore prewrite response.
	committer.SetMinCommitTS(committer.GetStartTS() + 10)
	committer.SetNoFallBack()
	err = committer.Execute(ctx)
	s.Nil(err)
	s.True(mock.seenPrimaryReq > 0)
	s.True(mock.seenSecondaryReq > 0)
}

func (s *testCommitterSuite) TestAsyncCommit() {
	ctx := context.Background()
	pk := []byte("tpk")
	pkVal := []byte("pkVal")
	k1 := []byte("tk1")
	k1Val := []byte("k1Val")
	txn1 := s.beginAsyncCommit()
	err := txn1.Set(pk, pkVal)
	s.Nil(err)
	err = txn1.Set(k1, k1Val)
	s.Nil(err)

	committer, err := txn1.NewCommitter(0)
	s.Nil(err)
	committer.SetMinCommitTS(txn1.StartTS() + 10)
	err = committer.Execute(ctx)
	s.Nil(err)

	s.checkValues(map[string]string{
		string(pk): string(pkVal),
		string(k1): string(k1Val),
	})
}

func updateGlobalConfig(f func(conf *config.Config)) {
	g := config.GetGlobalConfig()
	newConf := *g
	f(&newConf)
	config.StoreGlobalConfig(&newConf)
}

// restoreFunc gets a function that restore the config to the current value.
func restoreGlobalConfFunc() (restore func()) {
	g := config.GetGlobalConfig()
	return func() {
		config.StoreGlobalConfig(g)
	}
}

func (s *testCommitterSuite) TestAsyncCommitCheck() {
	defer restoreGlobalConfFunc()()
	updateGlobalConfig(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.KeysLimit = 16
		conf.TiKVClient.AsyncCommit.TotalKeySizeLimit = 64
	})

	txn := s.beginAsyncCommit()
	buf := []byte{0, 0, 0, 0}
	// Set 16 keys, each key is 4 bytes long. So the total size of keys is 64 bytes.
	for i := 0; i < 16; i++ {
		buf[0] = byte(i)
		err := txn.Set(buf, []byte("v"))
		s.Nil(err)
	}

	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	s.True(committer.CheckAsyncCommit())

	updateGlobalConfig(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.KeysLimit = 15
	})
	s.False(committer.CheckAsyncCommit())

	updateGlobalConfig(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.KeysLimit = 20
		conf.TiKVClient.AsyncCommit.TotalKeySizeLimit = 63
	})
	s.False(committer.CheckAsyncCommit())
}

type mockClient struct {
	inner            tikv.Client
	seenPrimaryReq   uint32
	seenSecondaryReq uint32
}

func (m *mockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// If we find a prewrite request, check if it satisfies our constraints.
	if pr, ok := req.Req.(*kvrpcpb.PrewriteRequest); ok {
		if pr.UseAsyncCommit {
			if isPrimary(pr) {
				// The primary key should not be included, nor should there be any duplicates. All keys should be present.
				if !includesPrimary(pr) && allKeysNoDups(pr) {
					atomic.StoreUint32(&m.seenPrimaryReq, 1)
				}
			} else {
				// Secondaries should only be sent with the primary key
				if len(pr.Secondaries) == 0 {
					atomic.StoreUint32(&m.seenSecondaryReq, 1)
				}
			}
		}
	}
	return m.inner.SendRequest(ctx, addr, req, timeout)
}

func (m *mockClient) Close() error {
	return m.inner.Close()
}

func isPrimary(req *kvrpcpb.PrewriteRequest) bool {
	for _, m := range req.Mutations {
		if bytes.Equal(req.PrimaryLock, m.Key) {
			return true
		}
	}

	return false
}

func includesPrimary(req *kvrpcpb.PrewriteRequest) bool {
	for _, k := range req.Secondaries {
		if bytes.Equal(req.PrimaryLock, k) {
			return true
		}
	}

	return false
}

func allKeysNoDups(req *kvrpcpb.PrewriteRequest) bool {
	check := make(map[string]bool)

	// Create the check map and check for duplicates.
	for _, k := range req.Secondaries {
		s := string(k)
		if check[s] {
			return false
		}
		check[s] = true
	}

	// Check every key is present.
	for i := byte(50); i < 120; i++ {
		k := []byte{i}
		if !bytes.Equal(req.PrimaryLock, k) && !check[string(k)] {
			return false
		}
	}
	return true
}

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRpcErrors() {
	s.Nil(failpoint.Enable("tikvclient/rpcCommitResult", `return("timeout")`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcCommitResult"))
	}()
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1 := s.begin()
	err := t1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = t1.Commit(context.Background())
	s.NotNil(err)
	s.True(terror.ErrorEqual(err, terror.ErrResultUndetermined), errors.ErrorStack(err))

	// We don't need to call "Rollback" after "Commit" fails.
	err = t1.Rollback()
	s.Equal(err, tikverr.ErrInvalidTxn)
}

// TestFailCommitPrimaryRegionError tests RegionError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryRegionError() {
	s.Nil(failpoint.Enable("tikvclient/rpcCommitResult", `return("notLeader")`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcCommitResult"))
	}()
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it exceeds max retry timeout on RegionError.
	t2 := s.begin()
	err := t2.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	err = t2.Commit(context.Background())
	s.NotNil(err)
	s.True(terror.ErrorNotEqual(err, terror.ErrResultUndetermined))
}

// TestFailCommitPrimaryRPCErrorThenRegionError tests the case when commit first
// receive a rpc timeout, then region errors afterwrards.
func (s *testCommitterSuite) TestFailCommitPrimaryRPCErrorThenRegionError() {
	s.Nil(failpoint.Enable("tikvclient/rpcCommitResult", `1*return("timeout")->return("notLeader")`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcCommitResult"))
	}()
	// The region error will be wrapped to ErrResultUndetermined.
	t1 := s.begin()
	err := t1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = t1.Commit(context.Background())
	s.NotNil(err)
	s.True(terror.ErrorEqual(err, terror.ErrResultUndetermined), errors.ErrorStack(err))
}

// TestFailCommitPrimaryKeyError tests KeyError is handled properly when
// committing primary region task.
func (s *testCommitterSuite) TestFailCommitPrimaryKeyError() {
	s.Nil(failpoint.Enable("tikvclient/rpcCommitResult", `return("keyError")`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcCommitResult"))
	}()
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it meets KeyError.
	t3 := s.begin()
	err := t3.Set([]byte("c"), []byte("c1"))
	s.Nil(err)
	err = t3.Commit(context.Background())
	s.NotNil(err)
	s.True(terror.ErrorNotEqual(err, terror.ErrResultUndetermined))
}

// TestFailCommitPrimaryRPCErrorThenKeyError tests KeyError overwrites the undeterminedErr.
func (s *testCommitterSuite) TestFailCommitPrimaryRPCErrorThenKeyError() {
	s.Nil(failpoint.Enable("tikvclient/rpcCommitResult", `1*return("timeout")->return("keyError")`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcCommitResult"))
	}()
	// Ensure it returns the original error without wrapped to ErrResultUndetermined
	// if it meets KeyError.
	t3 := s.begin()
	err := t3.Set([]byte("c"), []byte("c1"))
	s.Nil(err)
	err = t3.Commit(context.Background())
	s.NotNil(err)
	s.False(terror.ErrorEqual(err, terror.ErrResultUndetermined))
}

func (s *testCommitterSuite) TestFailCommitTimeout() {
	s.Nil(failpoint.Enable("tikvclient/rpcCommitTimeout", `return(true)`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcCommitTimeout"))
	}()
	txn := s.begin()
	err := txn.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	err = txn.Set([]byte("b"), []byte("b1"))
	s.Nil(err)
	err = txn.Set([]byte("c"), []byte("c1"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.NotNil(err)

	txn2 := s.begin()
	value, err := txn2.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	s.Greater(len(value), 0)
	_, err = txn2.Get(context.TODO(), []byte("b"))
	s.Nil(err)
	s.Greater(len(value), 0)
}

// TestCommitMultipleRegions tests commit multiple regions.
// The test takes too long under the race detector.
func (s *testCommitterSuite) TestCommitMultipleRegions() {
	if israce.Race {
		s.T().Skip("skip slow tests")
	}
	m := make(map[string]string)
	for i := 0; i < 100; i++ {
		k, v := randKV(10, 10)
		m[k] = v
	}
	s.mustCommit(m)

	// Test big values.
	m = make(map[string]string)
	for i := 0; i < 50; i++ {
		k, v := randKV(11, int(txnCommitBatchSize)/7)
		m[k] = v
	}
	s.mustCommit(m)
}
