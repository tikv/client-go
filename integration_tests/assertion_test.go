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
	"strings"
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

func (s *testAssertionSuite) testAssertionImpl(keyPrefix string, pessimistic bool, lockKeys bool, assertionLevel kvrpcpb.AssertionLevel) {
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

	// Prepare some data. Make k1, k3, k7 exist.
	prepareTxn, err := s.store.Begin()
	s.Nil(err)
	err = prepareTxn.Set(k(1), []byte("v1"))
	s.Nil(err)
	err = prepareTxn.Set(k(3), []byte("v3"))
	s.Nil(err)
	err = prepareTxn.Set(k(7), []byte("v7"))
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

	if assertionLevel == kvrpcpb.AssertionLevel_Strict {
		// Single key.
		_, err = doTxn(kv.SetAssertExist, k(1))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertNotExist, k(2))
		s.Nil(err)
		startTS, err := doTxn(kv.SetAssertNotExist, k(3))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(3), kvrpcpb.Assertion_NotExist, prepareStartTS, prepareCommitTS)
		startTS, err = doTxn(kv.SetAssertExist, k(4))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(4), kvrpcpb.Assertion_Exist, 0, 0)

		// Multiple keys
		startTS, err = doTxn(kv.SetAssertNotExist, k(5), k(6), k(7))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(7), kvrpcpb.Assertion_NotExist, prepareStartTS, prepareCommitTS)
		startTS, err = doTxn(kv.SetAssertExist, k(8), k(9), k(10))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(10), kvrpcpb.Assertion_Exist, 0, 0)
	} else if assertionLevel == kvrpcpb.AssertionLevel_Fast && pessimistic && lockKeys {
		// Different from STRICT level, the already-existing version's startTS and commitTS cannot be fetched.

		// Single key.
		_, err = doTxn(kv.SetAssertExist, k(1))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertNotExist, k(2))
		s.Nil(err)
		startTS, err := doTxn(kv.SetAssertNotExist, k(3))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(3), kvrpcpb.Assertion_NotExist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(4))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(4), kvrpcpb.Assertion_Exist, 0, 0)

		// Multiple keys
		startTS, err = doTxn(kv.SetAssertNotExist, k(5), k(6), k(7))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(7), kvrpcpb.Assertion_NotExist, 0, 0)
		startTS, err = doTxn(kv.SetAssertExist, k(8), k(9), k(10))
		s.NotNil(err)
		checkAssertionFailError(err, startTS, k(10), kvrpcpb.Assertion_Exist, 0, 0)
	} else {
		// Nothing will be detected.

		// Single key.
		_, err = doTxn(kv.SetAssertExist, k(1))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertNotExist, k(2))
		s.Nil(err)
		_, err := doTxn(kv.SetAssertNotExist, k(3))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertExist, k(4))
		s.Nil(err)

		// Multiple keys
		_, err = doTxn(kv.SetAssertNotExist, k(5), k(6), k(7))
		s.Nil(err)
		_, err = doTxn(kv.SetAssertExist, k(8), k(9), k(10))
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
	s.testAssertionImpl(prefix+"a", false, false, kvrpcpb.AssertionLevel_Strict)
	s.testAssertionImpl(prefix+"b", true, false, kvrpcpb.AssertionLevel_Strict)
	s.testAssertionImpl(prefix+"c", true, true, kvrpcpb.AssertionLevel_Strict)
}

func (s *testAssertionSuite) TestFastAssertion() {
	// When the test cases runs with TiKV, the TiKV cluster can be reused, thus there may be deleted versions caused by
	// previous tests. This test case may meet different behavior if there are deleted versions. To avoid it, compose a
	// key prefix with a timestamp to ensure the keys to be unique.
	ts, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	s.Nil(err)
	prefix := fmt.Sprintf("test-fast-assertion-%d-", ts)
	s.testAssertionImpl(prefix+"a", false, false, kvrpcpb.AssertionLevel_Fast)
	s.testAssertionImpl(prefix+"b", true, false, kvrpcpb.AssertionLevel_Fast)
	s.testAssertionImpl(prefix+"c", true, true, kvrpcpb.AssertionLevel_Fast)
}

func (s *testAssertionSuite) TestAssertionErrorLessPriorToOtherError() {
	s.NoError(failpoint.Enable("tikvclient/shortPessimisticLockTTL", "return"))
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/shortPessimisticLockTTL"))
	}()

	testOnce := func(lockKey []byte, nonLockKey []byte, noBatch bool, delayPrimary bool, delaySecondary bool) {
		if noBatch {
			s.NoError(failpoint.Enable("tikvclient/twoPCRequestBatchSizeLimit", "return"))
			defer func() {
				s.NoError(failpoint.Disable("tikvclient/twoPCRequestBatchSizeLimit"))
			}()
		}

		ctx := context.Background()
		// Transaction 1 locks the keys.
		tx, err := s.store.Begin()
		s.NoError(err)
		tx.SetPessimistic(true)
		tx.SetAssertionLevel(kvrpcpb.AssertionLevel_Strict)
		s.NoError(tx.LockKeysWithWaitTime(ctx, 1, lockKey))
		s.NoError(tx.Set(lockKey, []byte("rv1")))
		s.NoError(tx.Set(nonLockKey, []byte("iv1")))
		tx.GetMemBuffer().UpdateFlags(lockKey, kv.SetAssertNotExist)
		tx.GetMemBuffer().UpdateFlags(nonLockKey, kv.SetAssertNotExist)
		tx.GetCommitter().CloseTTLManager()

		// Transaction 2 resolves the lock of transaction 1 and then
		tx2, err := s.store.Begin()
		s.NoError(err)
		tx2.SetPessimistic(true)
		tx2.SetAssertionLevel(kvrpcpb.AssertionLevel_Strict)
		s.NoError(tx.LockKeysWithWaitTime(ctx, 1000, lockKey))
		s.NoError(tx2.Set(lockKey, []byte("rv2")))
		s.NoError(tx2.Set(nonLockKey, []byte("iv2")))
		tx2.GetMemBuffer().UpdateFlags(lockKey, kv.SetAssertNotExist)
		tx2.GetMemBuffer().UpdateFlags(nonLockKey, kv.SetAssertNotExist)
		s.NoError(tx2.Commit(ctx))

		if delayPrimary {
			s.NoError(failpoint.Enable("tikvclient/prewritePrimary", "sleep(200)"))
			defer func() {
				s.NoError(failpoint.Disable("tikvclient/prewritePrimary"))
			}()
		}
		if delaySecondary {
			s.NoError(failpoint.Enable("tikvclient/prewriteSecondary", "sleep(200)"))
			defer func() {
				s.NoError(failpoint.Disable("tikvclient/prewriteSecondary"))
			}()
		}

		err = tx.Commit(ctx)
		s.NotNil(err)
		s.NotContains(strings.ToLower(err.Error()), "assertion")
	}

	testOnce([]byte("kr1"), []byte("ki1"), false, false, false)
	testOnce([]byte("kr2"), []byte("ki2"), true, false, false)
	testOnce([]byte("kr3"), []byte("ki3"), true, true, false)
	testOnce([]byte("kr4"), []byte("ki4"), true, false, true)
}
