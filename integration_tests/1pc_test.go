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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/1pc_test.go
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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/mockstore"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
)

func TestOnePC(t *testing.T) {
	suite.Run(t, new(testOnePCSuite))
}

type testOnePCSuite struct {
	testAsyncCommitCommon
	bo *tikv.Backoffer
}

func (s *testOnePCSuite) SetupTest() {
	s.testAsyncCommitCommon.setUpTest()
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testOnePCSuite) Test1PC() {
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))

	k1 := []byte("k1")
	v1 := []byte("v1")

	txn := s.begin1PC()
	err := txn.Set(k1, v1)
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	s.True(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), txn.GetCommitter().GetCommitTS())
	s.Greater(txn.GetCommitter().GetOnePCCommitTS(), txn.StartTS())
	// ttlManager is not used for 1PC.
	s.True(txn.GetCommitter().IsTTLUninitialized())

	// 1PC doesn't work if sessionID == 0
	k2 := []byte("k2")
	v2 := []byte("v2")

	txn = s.begin1PC()
	err = txn.Set(k2, v2)
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), uint64(0))
	s.Greater(txn.GetCommitter().GetCommitTS(), txn.StartTS())

	// 1PC doesn't work if system variable not set

	k3 := []byte("k3")
	v3 := []byte("v3")

	txn = s.begin()
	err = txn.Set(k3, v3)
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), uint64(0))
	s.Greater(txn.GetCommitter().GetCommitTS(), txn.StartTS())

	// Test multiple keys
	k4 := []byte("k4")
	v4 := []byte("v4")
	k5 := []byte("k5")
	v5 := []byte("v5")
	k6 := []byte("k6")
	v6 := []byte("v6")

	txn = s.begin1PC()
	err = txn.Set(k4, v4)
	s.Nil(err)
	err = txn.Set(k5, v5)
	s.Nil(err)
	err = txn.Set(k6, v6)
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	s.True(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), txn.GetCommitter().GetCommitTS())
	s.Greater(txn.GetCommitter().GetOnePCCommitTS(), txn.StartTS())
	// Check keys are committed with the same version
	s.mustGetFromSnapshot(txn.GetCommitTS(), k4, v4)
	s.mustGetFromSnapshot(txn.GetCommitTS(), k5, v5)
	s.mustGetFromSnapshot(txn.GetCommitTS(), k6, v6)
	s.mustGetNoneFromSnapshot(txn.GetCommitTS()-1, k4)
	s.mustGetNoneFromSnapshot(txn.GetCommitTS()-1, k5)
	s.mustGetNoneFromSnapshot(txn.GetCommitTS()-1, k6)

	// Overwriting in MVCC
	v6New := []byte("v6new")
	txn = s.begin1PC()
	err = txn.Set(k6, v6New)
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	s.True(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), txn.GetCommitter().GetCommitTS())
	s.Greater(txn.GetCommitter().GetOnePCCommitTS(), txn.StartTS())
	s.mustGetFromSnapshot(txn.GetCommitTS(), k6, v6New)
	s.mustGetFromSnapshot(txn.GetCommitTS()-1, k6, v6)

	// Check all keys
	keys := [][]byte{k1, k2, k3, k4, k5, k6}
	values := [][]byte{v1, v2, v3, v4, v5, v6New}
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	snap := s.store.GetSnapshot(ver)
	for i, k := range keys {
		v, err := snap.Get(ctx, k)
		s.Nil(err)
		s.Equal(v, values[i])
	}
}

func (s *testOnePCSuite) Test1PCIsolation() {
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))

	k := []byte("k")
	v1 := []byte("v1")

	txn := s.begin1PC()
	txn.Set(k, v1)
	err := txn.Commit(ctx)
	s.Nil(err)

	v2 := []byte("v2")
	txn = s.begin1PC()
	txn.Set(k, v2)

	// Make `txn`'s commitTs more likely to be less than `txn2`'s startTs if there's bug in commitTs
	// calculation.
	for i := 0; i < 10; i++ {
		_, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		s.Nil(err)
	}

	txn2 := s.begin1PC()
	s.mustGetFromTxn(txn2, k, v1)

	err = txn.Commit(ctx)
	s.True(txn.GetCommitter().IsOnePC())
	s.Nil(err)

	s.mustGetFromTxn(txn2, k, v1)
	s.Nil(txn2.Rollback())

	s.mustGetFromSnapshot(txn.GetCommitTS(), k, v2)
	s.mustGetFromSnapshot(txn.GetCommitTS()-1, k, v1)
}

func (s *testOnePCSuite) Test1PCDisallowMultiRegion() {
	// This test doesn't support tikv mode.
	if *mockstore.WithTiKV {
		return
	}

	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))

	txn := s.begin1PC()

	keys := []string{"k0", "k1", "k2", "k3"}
	values := []string{"v0", "v1", "v2", "v3"}

	err := txn.Set([]byte(keys[0]), []byte(values[0]))
	s.Nil(err)
	err = txn.Set([]byte(keys[3]), []byte(values[3]))
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)

	// 1PC doesn't work if it affects multiple regions.
	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte(keys[2]))
	s.Nil(err)
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(loc.Region.GetID(), newRegionID, []byte(keys[2]), []uint64{newPeerID}, newPeerID)

	txn = s.begin1PC()
	err = txn.Set([]byte(keys[1]), []byte(values[1]))
	s.Nil(err)
	err = txn.Set([]byte(keys[2]), []byte(values[2]))
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	s.False(txn.GetCommitter().IsOnePC())
	s.Equal(txn.GetCommitter().GetOnePCCommitTS(), uint64(0))
	s.Greater(txn.GetCommitter().GetCommitTS(), txn.StartTS())

	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	snap := s.store.GetSnapshot(ver)
	for i, k := range keys {
		v, err := snap.Get(ctx, []byte(k))
		s.Nil(err)
		s.Equal(v, []byte(values[i]))
	}
}

// It's just a simple validation of linearizability.
// Extra tests are needed to test this feature with the control of the TiKV cluster.
func (s *testOnePCSuite) Test1PCLinearizability() {
	t1 := s.begin()
	t2 := s.begin()
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
	commitTS1 := t1.GetCommitter().GetCommitTS()
	commitTS2 := t2.GetCommitter().GetCommitTS()
	s.Less(commitTS2, commitTS1)
}

func (s *testOnePCSuite) Test1PCWithMultiDC() {
	// It requires setting placement rules to run with TiKV
	if *mockstore.WithTiKV {
		return
	}

	localTxn := s.begin1PC()
	err := localTxn.Set([]byte("a"), []byte("a1"))
	localTxn.SetScope("bj")
	s.Nil(err)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = localTxn.Commit(ctx)
	s.Nil(err)
	s.False(localTxn.GetCommitter().IsOnePC())

	globalTxn := s.begin1PC()
	err = globalTxn.Set([]byte("b"), []byte("b1"))
	globalTxn.SetScope(oracle.GlobalTxnScope)
	s.Nil(err)
	err = globalTxn.Commit(ctx)
	s.Nil(err)
	s.True(globalTxn.GetCommitter().IsOnePC())
}

func (s *testOnePCSuite) TestTxnCommitCounter() {
	initial := metrics.GetTxnCommitCounter()

	// 2PC
	txn := s.begin()
	err := txn.Set([]byte("k"), []byte("v"))
	s.Nil(err)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = txn.Commit(ctx)
	s.Nil(err)
	curr := metrics.GetTxnCommitCounter()
	diff := curr.Sub(initial)
	s.Equal(diff.TwoPC, int64(1))
	s.Equal(diff.AsyncCommit, int64(0))
	s.Equal(diff.OnePC, int64(0))

	// AsyncCommit
	txn = s.beginAsyncCommit()
	err = txn.Set([]byte("k1"), []byte("v1"))
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	curr = metrics.GetTxnCommitCounter()
	diff = curr.Sub(initial)
	s.Equal(diff.TwoPC, int64(1))
	s.Equal(diff.AsyncCommit, int64(1))
	s.Equal(diff.OnePC, int64(0))

	// 1PC
	txn = s.begin1PC()
	err = txn.Set([]byte("k2"), []byte("v2"))
	s.Nil(err)
	err = txn.Commit(ctx)
	s.Nil(err)
	curr = metrics.GetTxnCommitCounter()
	diff = curr.Sub(initial)
	s.Equal(diff.TwoPC, int64(1))
	s.Equal(diff.AsyncCommit, int64(1))
	s.Equal(diff.OnePC, int64(1))
}
