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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/async_commit_fail_test.go
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
	"sort"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
)

func TestAsyncCommitFail(t *testing.T) {
	suite.Run(t, new(testAsyncCommitFailSuite))
}

type testAsyncCommitFailSuite struct {
	testAsyncCommitCommon
}

func (s *testAsyncCommitFailSuite) SetupTest() {
	s.testAsyncCommitCommon.setUpTest()
}

func (s *testAsyncCommitFailSuite) TearDownTest() {
	s.testAsyncCommitCommon.tearDownTest()
}

// TestFailCommitPrimaryRpcErrors tests rpc errors are handled properly when
// committing primary region task.
func (s *testAsyncCommitFailSuite) TestFailAsyncCommitPrewriteRpcErrors() {
	// This test doesn't support tikv mode because it needs setting failpoint in unistore.
	if *withTiKV {
		return
	}

	s.Nil(failpoint.Enable("tikvclient/noRetryOnRpcError", "return(true)"))
	s.Nil(failpoint.Enable("tikvclient/rpcPrewriteTimeout", `return(true)`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcPrewriteTimeout"))
		s.Nil(failpoint.Disable("tikvclient/noRetryOnRpcError"))
	}()
	// The rpc error will be wrapped to ErrResultUndetermined.
	t1 := s.beginAsyncCommit()
	err := t1.Set([]byte("a"), []byte("a1"))
	s.Nil(err)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = t1.Commit(ctx)
	s.NotNil(err)
	s.True(tikverr.IsErrorUndetermined(err), errors.WithStack(err))

	// We don't need to call "Rollback" after "Commit" fails.
	err = t1.Rollback()
	s.Equal(err, tikverr.ErrInvalidTxn)

	// Create a new transaction to check. The previous transaction should actually commit.
	t2 := s.beginAsyncCommit()
	res, err := t2.Get(context.Background(), []byte("a"))
	s.Nil(err)
	s.True(bytes.Equal(res, []byte("a1")))
}

func (s *testAsyncCommitFailSuite) TestAsyncCommitPrewriteCancelled() {
	// This test doesn't support tikv mode because it needs setting failpoint in unistore.
	if *withTiKV {
		return
	}

	// Split into two regions.
	splitKey := "s"
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte(splitKey))
	s.Nil(err)
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(loc.Region.GetID(), newRegionID, []byte(splitKey), []uint64{newPeerID}, newPeerID)
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	s.Nil(failpoint.Enable("tikvclient/rpcPrewriteResult", `1*return("writeConflict")->sleep(50)`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcPrewriteResult"))
	}()

	t1 := s.beginAsyncCommit()
	err = t1.Set([]byte("a"), []byte("a"))
	s.Nil(err)
	err = t1.Set([]byte("z"), []byte("z"))
	s.Nil(err)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = t1.Commit(ctx)
	s.NotNil(err)
	_, ok := errors.Cause(err).(*tikverr.ErrWriteConflict)
	s.True(ok, errors.WithStack(err))
}

func (s *testAsyncCommitFailSuite) TestPointGetWithAsyncCommit() {
	s.putAlphabets(true)

	txn := s.beginAsyncCommit()
	txn.Set([]byte("a"), []byte("v1"))
	txn.Set([]byte("b"), []byte("v2"))
	s.mustPointGet([]byte("a"), []byte("a"))
	s.mustPointGet([]byte("b"), []byte("b"))

	// PointGet cannot ignore async commit transactions' locks.
	s.Nil(failpoint.Enable("tikvclient/asyncCommitDoNothing", "return"))
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err := txn.Commit(ctx)
	s.Nil(err)
	s.True(txn.GetCommitter().IsAsyncCommit())
	s.mustPointGet([]byte("a"), []byte("v1"))
	s.mustPointGet([]byte("b"), []byte("v2"))
	s.Nil(failpoint.Disable("tikvclient/asyncCommitDoNothing"))

	// PointGet will not push the `max_ts` to its ts which is MaxUint64.
	txn2 := s.beginAsyncCommit()
	s.mustGetFromTxn(txn2, []byte("a"), []byte("v1"))
	s.mustGetFromTxn(txn2, []byte("b"), []byte("v2"))
	err = txn2.Rollback()
	s.Nil(err)
}

func (s *testAsyncCommitFailSuite) TestSecondaryListInPrimaryLock() {
	// This test doesn't support tikv mode.
	if *withTiKV {
		return
	}

	s.putAlphabets(true)

	// Split into several regions.
	for _, splitKey := range []string{"h", "o", "u"} {
		bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
		loc, err := s.store.GetRegionCache().LocateKey(bo, []byte(splitKey))
		s.Nil(err)
		newRegionID := s.cluster.AllocID()
		newPeerID := s.cluster.AllocID()
		s.cluster.Split(loc.Region.GetID(), newRegionID, []byte(splitKey), []uint64{newPeerID}, newPeerID)
		s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	}

	// Ensure the region has been split
	s.Eventually(func() bool {
		checkRegionBound := func(key, startKey, endKey []byte) bool {
			bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
			loc, err := s.store.GetRegionCache().LocateKey(bo, key)
			s.Nil(err)
			if bytes.Equal(loc.StartKey, startKey) && bytes.Equal(loc.EndKey, endKey) {
				return true
			}
			s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)
			return false
		}
		return checkRegionBound([]byte("i"), []byte("h"), []byte("o")) &&
			checkRegionBound([]byte("p"), []byte("o"), []byte("u"))
	}, time.Second, 10*time.Millisecond, "region is not split successfully")

	var sessionID uint64 = 0
	test := func(keys []string, values []string) {
		sessionID++
		ctx := context.WithValue(context.Background(), util.SessionID, sessionID)

		txn := s.beginAsyncCommit()
		for i := range keys {
			txn.Set([]byte(keys[i]), []byte(values[i]))
		}

		s.Nil(failpoint.Enable("tikvclient/asyncCommitDoNothing", "return"))

		err := txn.Commit(ctx)
		s.Nil(err)

		primary := txn.GetCommitter().GetPrimaryKey()
		bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
		lockResolver := tikv.NewLockResolverProb(s.store.GetLockResolver())
		txnStatus, err := lockResolver.GetTxnStatus(bo, txn.StartTS(), primary, 0, 0, false, false, nil)
		s.Nil(err)
		s.False(txnStatus.IsCommitted())
		s.Equal(txnStatus.Action(), kvrpcpb.Action_NoAction)
		// Currently when the transaction has no secondary, the `secondaries` field of the txnStatus
		// will be set nil. So here initialize the `expectedSecondaries` to nil too.
		var expectedSecondaries [][]byte
		for _, k := range keys {
			if !bytes.Equal([]byte(k), primary) {
				expectedSecondaries = append(expectedSecondaries, []byte(k))
			}
		}
		sort.Slice(expectedSecondaries, func(i, j int) bool {
			return bytes.Compare(expectedSecondaries[i], expectedSecondaries[j]) < 0
		})

		gotSecondaries := lockResolver.GetSecondariesFromTxnStatus(txnStatus)
		sort.Slice(gotSecondaries, func(i, j int) bool {
			return bytes.Compare(gotSecondaries[i], gotSecondaries[j]) < 0
		})

		s.Equal(gotSecondaries, expectedSecondaries)

		s.Nil(failpoint.Disable("tikvclient/asyncCommitDoNothing"))
		txn.GetCommitter().Cleanup(context.Background())
	}

	test([]string{"a"}, []string{"a1"})
	test([]string{"a", "b"}, []string{"a2", "b2"})
	test([]string{"a", "b", "d"}, []string{"a3", "b3", "d3"})
	test([]string{"a", "b", "h", "i", "u"}, []string{"a4", "b4", "h4", "i4", "u4"})
	test([]string{"i", "a", "z", "u", "b"}, []string{"i5", "a5", "z5", "u5", "b5"})
}

func (s *testAsyncCommitFailSuite) TestAsyncCommitContextCancelCausingUndetermined() {
	// For an async commit transaction, if RPC returns context.Canceled error when prewriting, the
	// transaction should go to undetermined state.
	txn := s.beginAsyncCommit()
	err := txn.Set([]byte("a"), []byte("va"))
	s.Nil(err)

	s.Nil(failpoint.Enable("tikvclient/rpcContextCancelErr", `return(true)`))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/rpcContextCancelErr"))
	}()

	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = txn.Commit(ctx)
	s.NotNil(err)
	s.NotNil(txn.GetCommitter().GetUndeterminedErr())
}
