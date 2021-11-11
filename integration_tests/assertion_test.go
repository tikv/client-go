package tikv_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
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

func (s *testAssertionSuite) TestPrewriteAssertion() {
	// TODO: Enable testing with unistore after supporting assertion in unistore.
	if !*withTiKV {
		// Skip the test.
		return
	}

	s.Nil(failpoint.Enable("tikvclient/assertionSkipCheckFromLock", "return"))
	defer func() {
		s.Nil(failpoint.Disable("tikvclient/assertionSkipCheckFromLock"))
	}()

	testWithTxnKind := func(keyPrefix byte, pessimistic bool, lockKeys bool) {
		// Compose the key
		k := func(i byte) []byte {
			return []byte{keyPrefix, 'k', i}
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
			txn.SetAssertionLevel(kvrpcpb.AssertionLevel_Strict)
			txn.SetPessimistic(pessimistic)
			if lockKeys {
				err = txn.LockKeysWithWaitTime(context.Background(), 10000, keys...)
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
	}

	testWithTxnKind('a', false, false)
	testWithTxnKind('b', true, false)
	testWithTxnKind('c', true, true)
}
