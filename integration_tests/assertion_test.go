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

package tikv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestAssertion(t *testing.T) {
	suite.Run(t, new(testAssertionSuite))
}

type testAssertionSuite struct {
	suite.Suite
	cluster testutils.Cluster
	store   tikv.StoreProbe
}

func (s *testAssertionSuite) SetupTest() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
}

func (s *testAssertionSuite) TearDownTest() {
	s.store.Close()
}

type mockAmender struct{}

func (*mockAmender) AmendTxn(ctx context.Context, startInfoSchema transaction.SchemaVer, change *transaction.RelatedSchemaChange, mutations transaction.CommitterMutations) (transaction.CommitterMutations, error) {
	return nil, nil
}

func (s *testAssertionSuite) testAssertionImpl(keyPrefix string, pessimistic bool, lockKeys bool, assertionLevel kvrpcpb.AssertionLevel, enableAmend bool) {
	if assertionLevel != kvrpcpb.AssertionLevel_Strict {
		s.Nil(failpoint.Enable("tikvclient/assertionSkipCheckFromPrewrite", "return"))
		defer func() {
			s.Nil(failpoint.Disable("tikvclient/assertionSkipCheckFromPrewrite"))
		}()
	}
	if assertionLevel != kvrpcpb.AssertionLevel_Fast {
		s.Nil(failpoint.Enable("tikvclient/assertionSkipCheckFromLock", "return"))
		defer func() {
			s.Nil(failpoint.Disable("tikvclient/assertionSkipCheckFromLock"))
		}()
	}

	// Compose the key
	k := func(i byte) []byte {
		return append([]byte(keyPrefix), 'k', i)
	}

	// Prepare some data. Make k11, k13, k23 exist; k15, k43 deleted.
	prepareTxn, err := s.store.Begin()
	err = prepareTxn.Set(k(15), []byte("v15"))
	s.Nil(err)
	err = prepareTxn.Set(k(43), []byte("v43"))
	s.Nil(err)
	err = prepareTxn.Commit(context.Background())
	s.Nil(err)

	prepareTxn, err = s.store.Begin()
	s.Nil(err)
	err = prepareTxn.Set(k(11), []byte("v11"))
	s.Nil(err)
	err = prepareTxn.Set(k(13), []byte("v13"))
	s.Nil(err)
	err = prepareTxn.Set(k(23), []byte("v23"))
	s.Nil(err)
	err = prepareTxn.Delete(k(15))
	s.Nil(err)
	err = prepareTxn.Delete(k(43))
	s.Nil(err)
	err = prepareTxn.Commit(context.Background())
	s.Nil(err)
	prepareStartTS := prepareTxn.GetCommitter().GetStartTS()
	prepareCommitTS := prepareTxn.GetCommitTS()

	// A helper to perform a complete transaction. When multiple keys are passed in, assertion will be set on only
	// the last key.
	doTxn := func(lastAssertion kv.FlagsOp, keys ...[]byte) (uint64, error) {
		txn, err := s.store.Begin()
		s.Nil(err)
		txn.SetAssertionLevel(assertionLevel)
		txn.SetPessimistic(pessimistic)
		if enableAmend {
			txn.SetSchemaAmender(&mockAmender{})
		}
		if lockKeys {
			lockCtx := kv.NewLockCtx(txn.StartTS(), 1000, time.Now())
			lockCtx.InitCheckExistence(1)
			err = txn.LockKeys(context.Background(), lockCtx, keys...)
			s.Nil(err)
		} else if pessimistic {
			// Since we don't want to lock the keys to be tested, set another key as the primary.
			err = txn.LockKeysWithWaitTime(context.Background(), 10000, []byte("primary"))
			s.Nil(err)
		}
		for _, key := range keys {
			err = txn.Set(key, append([]byte{'v'}, key...))
			s.Nil(err)
		}
		txn.GetMemBuffer().UpdateFlags(keys[len(keys)-1], lastAssertion)
		err = txn.Commit(context.Background())
		startTS := txn.GetCommitter().GetStartTS()
		return startTS, err
	}

	checkAssertionFailError := func(err error, startTS uint64, key []byte, assertion kvrpcpb.Assertion, existingStartTS uint64, existingCommitTS uint64) {
		assertionFailed, ok := errors.Cause(err).(*tikverr.ErrAssertionFailed)
		s.True(ok)
		s.Equal(startTS, assertionFailed.StartTs)
		s.Equal(key, assertionFailed.Key)
		s.Equal(assertion, assertionFailed.Assertion)
		s.Equal(existingStartTS, assertionFailed.ExistingStartTs)
		s.Equal(existingCommitTS, assertionFailed.ExistingCommitTs)
	}

	if assertionLevel == kvrpcpb.AssertionLevel_Strict && !enableAmend {
		// Single key.
		_, err = doTxn(kv.SetAssertExist, k(11))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertNotExist, k(12))
		s.Nil(err)
		startTS, err := doTxn(kv.SetAssertNotExist, k(13))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(13), kvrpcpb.Assertion_NotExist, prepareStartTS, prepareCommitTS)
		startTS, err = doTxn(kv.SetAssertExist, k(14))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(14), kvrpcpb.Assertion_Exist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(15))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(15), kvrpcpb.Assertion_Exist, prepareStartTS, prepareCommitTS)

		// Multiple keys
		startTS, err = doTxn(kv.SetAssertNotExist, k(21), k(22), k(23))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(23), kvrpcpb.Assertion_NotExist, prepareStartTS, prepareCommitTS)
		startTS, err = doTxn(kv.SetAssertExist, k(31), k(32), k(33))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(33), kvrpcpb.Assertion_Exist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(41), k(42), k(43))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(43), kvrpcpb.Assertion_Exist, prepareStartTS, prepareCommitTS)
	} else if assertionLevel == kvrpcpb.AssertionLevel_Fast && pessimistic && lockKeys && !enableAmend {
		// Different from STRICT level, the already-existing version's startTS and commitTS cannot be fetched.

		// Single key.
		_, err = doTxn(kv.SetAssertExist, k(11))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertNotExist, k(12))
		s.Nil(err)
		startTS, err := doTxn(kv.SetAssertNotExist, k(13))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(13), kvrpcpb.Assertion_NotExist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(14))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(14), kvrpcpb.Assertion_Exist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(15))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(15), kvrpcpb.Assertion_Exist, 0, 0)

		// Multiple keys
		startTS, err = doTxn(kv.SetAssertNotExist, k(21), k(22), k(23))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(23), kvrpcpb.Assertion_NotExist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(31), k(32), k(33))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(33), kvrpcpb.Assertion_Exist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(41), k(42), k(43))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(43), kvrpcpb.Assertion_Exist, 0, 0)
	} else {
		// Nothing will be detected.

		// Single key.
		_, err = doTxn(kv.SetAssertExist, k(11))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertNotExist, k(12))
		s.Nil(err)
		_, err := doTxn(kv.SetAssertNotExist, k(13))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertExist, k(14))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertExist, k(15))
		s.Nil(err)

		// Multiple keys
		_, err = doTxn(kv.SetAssertNotExist, k(21), k(22), k(23))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertExist, k(31), k(32), k(33))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertExist, k(41), k(42), k(43))
		s.Nil(err)
	}
}

func (s *testAssertionSuite) TestPrewriteAssertion() {
	// When the test cases runs with TiKV, the TiKV cluster can be reused, thus there may be deleted versions caused by
	// previous tests. This test case may meet different behavior if there are deleted versions. To avoid it, compose a
	// key prefix with a timestamp to ensure the keys to be unique.
	ts, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	prefix := fmt.Sprintf("test-prewrite-assertion-%d-", ts)
	s.testAssertionImpl(prefix+"a", false, false, kvrpcpb.AssertionLevel_Strict, false)
	s.testAssertionImpl(prefix+"b", true, false, kvrpcpb.AssertionLevel_Strict, false)
	s.testAssertionImpl(prefix+"c", true, true, kvrpcpb.AssertionLevel_Strict, false)
	s.testAssertionImpl(prefix+"a", false, false, kvrpcpb.AssertionLevel_Strict, true)
	s.testAssertionImpl(prefix+"b", true, false, kvrpcpb.AssertionLevel_Strict, true)
	s.testAssertionImpl(prefix+"c", true, true, kvrpcpb.AssertionLevel_Strict, true)
}

func (s *testAssertionSuite) TestFastAssertion() {
	// When the test cases runs with TiKV, the TiKV cluster can be reused, thus there may be deleted versions caused by
	// previous tests. This test case may meet different behavior if there are deleted versions. To avoid it, compose a
	// key prefix with a timestamp to ensure the keys to be unique.
	ts, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	prefix := fmt.Sprintf("test-fast-assertion-%d-", ts)
	s.testAssertionImpl(prefix+"a", false, false, kvrpcpb.AssertionLevel_Fast, false)
	s.testAssertionImpl(prefix+"b", true, false, kvrpcpb.AssertionLevel_Fast, false)
	s.testAssertionImpl(prefix+"c", true, true, kvrpcpb.AssertionLevel_Fast, false)
	s.testAssertionImpl(prefix+"a", false, false, kvrpcpb.AssertionLevel_Fast, true)
	s.testAssertionImpl(prefix+"b", true, false, kvrpcpb.AssertionLevel_Fast, true)
	s.testAssertionImpl(prefix+"c", true, true, kvrpcpb.AssertionLevel_Fast, true)
}
