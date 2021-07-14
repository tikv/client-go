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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/snapshot_fail_test.go
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
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSnapshotFail(t *testing.T) {
	suite.Run(t, new(testSnapshotFailSuite))
}

type testSnapshotFailSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testSnapshotFailSuite) SetupSuite() {
	client, pdClient, cluster, err := unistore.New("")
	s.Require().Nil(err)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(fpClient{Client: client}, pdClient, nil, nil, 0)
	s.Require().Nil(err)
	s.store = tikv.StoreProbe{KVStore: store}
}

func (s *testSnapshotFailSuite) TearDownSuite() {
	s.store.Close()
}

func (s *testSnapshotFailSuite) TearDownTest() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	iter, err := txn.Iter([]byte(""), []byte(""))
	s.Require().Nil(err)
	for iter.Valid() {
		err = txn.Delete(iter.Key())
		s.Require().Nil(err)
		err = iter.Next()
		s.Require().Nil(err)
	}
	s.Require().Nil(txn.Commit(context.TODO()))
}

func (s *testSnapshotFailSuite) TestBatchGetResponseKeyError() {
	// Meaningless to test with tikv because it has a mock key error
	if *withTiKV {
		return
	}

	// Put two KV pairs
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	err = txn.Set([]byte("k1"), []byte("v1"))
	s.Nil(err)
	err = txn.Set([]byte("k2"), []byte("v2"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)

	s.Require().Nil(failpoint.Enable("tikvclient/rpcBatchGetResult", `1*return("keyError")`))
	defer func() {
		s.Require().Nil(failpoint.Disable("tikvclient/rpcBatchGetResult"))
	}()

	txn, err = s.store.Begin()
	s.Require().Nil(err)
	res, err := toTiDBTxn(&txn).BatchGet(context.Background(), toTiDBKeys([][]byte{[]byte("k1"), []byte("k2")}))
	s.Nil(err)
	s.Equal(res, map[string][]byte{"k1": []byte("v1"), "k2": []byte("v2")})
}

func (s *testSnapshotFailSuite) TestScanResponseKeyError() {
	// Meaningless to test with tikv because it has a mock key error
	if *withTiKV {
		return
	}

	// Put two KV pairs
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	err = txn.Set([]byte("k1"), []byte("v1"))
	s.Nil(err)
	err = txn.Set([]byte("k2"), []byte("v2"))
	s.Nil(err)
	err = txn.Set([]byte("k3"), []byte("v3"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)

	s.Require().Nil(failpoint.Enable("tikvclient/rpcScanResult", `1*return("keyError")`))
	txn, err = s.store.Begin()
	s.Require().Nil(err)
	iter, err := txn.Iter([]byte("a"), []byte("z"))
	s.Nil(err)
	s.Equal(iter.Key(), []byte("k1"))
	s.Equal(iter.Value(), []byte("v1"))
	s.Nil(iter.Next())
	s.Equal(iter.Key(), []byte("k2"))
	s.Equal(iter.Value(), []byte("v2"))
	s.Nil(iter.Next())
	s.Equal(iter.Key(), []byte("k3"))
	s.Equal(iter.Value(), []byte("v3"))
	s.Nil(iter.Next())
	s.False(iter.Valid())
	s.Require().Nil(failpoint.Disable("tikvclient/rpcScanResult"))

	s.Require().Nil(failpoint.Enable("tikvclient/rpcScanResult", `1*return("keyError")`))
	txn, err = s.store.Begin()
	s.Require().Nil(err)
	iter, err = txn.Iter([]byte("k2"), []byte("k4"))
	s.Nil(err)
	s.Equal(iter.Key(), []byte("k2"))
	s.Equal(iter.Value(), []byte("v2"))
	s.Nil(iter.Next())
	s.Equal(iter.Key(), []byte("k3"))
	s.Equal(iter.Value(), []byte("v3"))
	s.Nil(iter.Next())
	s.False(iter.Valid())
	s.Require().Nil(failpoint.Disable("tikvclient/rpcScanResult"))
}

func (s *testSnapshotFailSuite) TestRetryMaxTsPointGetSkipLock() {

	// Prewrite k1 and k2 with async commit but don't commit them
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	err = txn.Set([]byte("k1"), []byte("v1"))
	s.Nil(err)
	err = txn.Set([]byte("k2"), []byte("v2"))
	s.Nil(err)
	txn.SetEnableAsyncCommit(true)

	s.Require().Nil(failpoint.Enable("tikvclient/asyncCommitDoNothing", "return"))
	s.Require().Nil(failpoint.Enable("tikvclient/twoPCShortLockTTL", "return"))
	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	err = committer.Execute(context.Background())
	s.Nil(err)
	s.Require().Nil(failpoint.Disable("tikvclient/twoPCShortLockTTL"))

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	getCh := make(chan []byte)
	go func() {
		// Sleep a while to make the TTL of the first txn expire, then we make sure we resolve lock by this get
		time.Sleep(200 * time.Millisecond)
		s.Require().Nil(failpoint.Enable("tikvclient/beforeSendPointGet", "1*off->pause"))
		res, err := snapshot.Get(context.Background(), []byte("k2"))
		s.Nil(err)
		getCh <- res
	}()
	// The get should be blocked by the failpoint. But the lock should have been resolved.
	select {
	case res := <-getCh:
		s.Fail("too early %s", string(res))
	case <-time.After(1 * time.Second):
	}

	// Prewrite k1 and k2 again without committing them
	txn, err = s.store.Begin()
	s.Require().Nil(err)
	txn.SetEnableAsyncCommit(true)
	err = txn.Set([]byte("k1"), []byte("v3"))
	s.Nil(err)
	err = txn.Set([]byte("k2"), []byte("v4"))
	s.Nil(err)
	committer, err = txn.NewCommitter(1)
	s.Nil(err)
	err = committer.Execute(context.Background())
	s.Nil(err)

	s.Require().Nil(failpoint.Disable("tikvclient/beforeSendPointGet"))

	// After disabling the failpoint, the get request should bypass the new locks and read the old result
	select {
	case res := <-getCh:
		s.Equal(res, []byte("v2"))
	case <-time.After(1 * time.Second):
		s.Fail("get timeout")
	}
}

func (s *testSnapshotFailSuite) TestRetryPointGetResolveTS() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	s.Nil(txn.Set([]byte("k1"), []byte("v1")))
	err = txn.Set([]byte("k2"), []byte("v2"))
	s.Nil(err)
	txn.SetEnableAsyncCommit(false)
	txn.SetEnable1PC(false)
	txn.SetCausalConsistency(true)

	// Prewrite the lock without committing it
	s.Require().Nil(failpoint.Enable("tikvclient/beforeCommit", `pause`))
	ch := make(chan struct{})
	committer, err := txn.NewCommitter(1)
	s.Equal(committer.GetPrimaryKey(), []byte("k1"))
	go func() {
		s.Nil(err)
		err = committer.Execute(context.Background())
		s.Nil(err)
		ch <- struct{}{}
	}()

	// Wait until prewrite finishes
	time.Sleep(200 * time.Millisecond)
	// Should get nothing with max version, and **not pushing forward minCommitTS** of the primary lock
	snapshot := s.store.GetSnapshot(math.MaxUint64)
	_, err = snapshot.Get(context.Background(), []byte("k2"))
	s.True(tikverr.IsErrNotFound(err))

	initialCommitTS := committer.GetCommitTS()
	s.Require().Nil(failpoint.Disable("tikvclient/beforeCommit"))

	<-ch
	// check the minCommitTS is not pushed forward
	snapshot = s.store.GetSnapshot(initialCommitTS)
	v, err := snapshot.Get(context.Background(), []byte("k2"))
	s.Nil(err)
	s.Equal(v, []byte("v2"))
}

func (s *testSnapshotFailSuite) TestResetSnapshotTS() {
	x := []byte("x_key_TestResetSnapshotTS")
	y := []byte("y_key_TestResetSnapshotTS")
	ctx := context.Background()

	txn, err := s.store.Begin()
	s.Nil(err)
	s.Nil(txn.Set(x, []byte("x0")))
	s.Nil(txn.Set(y, []byte("y0")))
	err = txn.Commit(ctx)
	s.Nil(err)

	txn, err = s.store.Begin()
	s.Nil(err)
	s.Nil(txn.Set(x, []byte("x1")))
	s.Nil(txn.Set(y, []byte("y1")))
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.SetLockTTL(3000)
	s.Nil(committer.PrewriteAllMutations(ctx))

	txn2, err := s.store.Begin()
	val, err := txn2.Get(ctx, y)
	s.Nil(err)
	s.Equal(val, []byte("y0"))

	// Only commit the primary key x
	s.Nil(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", `return`))
	s.Nil(failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	committer.SetCommitTS(txn2.StartTS() + 1)
	err = committer.CommitMutations(ctx)
	s.Nil(err)
	s.Nil(failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit"))
	s.Nil(failpoint.Disable("tikvclient/beforeCommitSecondaries"))

	// After reset setting snapshotTS, the resolvedLocks should be reset.
	// So when it encounters the locked y, it must check the primary key instead of
	// just ignore the lock.
	txn2.GetSnapshot().SetSnapshotTS(committer.GetCommitTS())
	val, err = txn2.Get(ctx, y)
	s.Nil(err)
	s.Equal(val, []byte("y1"))
}
