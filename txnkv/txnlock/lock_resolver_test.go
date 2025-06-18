package txnlock

import (
	"context"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/util"
)

// TestLockResolverCache is used to cover the issue https://github.com/pingcap/tidb/issues/59494.
func TestLockResolverCache(t *testing.T) {
	util.EnableFailpoints()
	lockResolver := NewLockResolver(nil)
	lock := func(key, primary string, startTS uint64, useAsyncCommit bool, secondaries [][]byte) *kvrpcpb.LockInfo {
		return &kvrpcpb.LockInfo{
			Key:            []byte(key),
			PrimaryLock:    []byte(primary),
			LockVersion:    startTS,
			UseAsyncCommit: useAsyncCommit,
			MinCommitTs:    startTS + 1,
			Secondaries:    secondaries,
		}
	}

	resolvedTxnTS := uint64(1)
	k1 := "k1"
	k2 := "k2"
	resolvedTxnStatus := TxnStatus{
		ttl:         0,
		commitTS:    10,
		primaryLock: lock(k1, k1, resolvedTxnTS, true, [][]byte{[]byte(k2)}),
	}
	lockResolver.mu.resolved[resolvedTxnTS] = resolvedTxnStatus
	toResolveLock := lock(k2, k1, resolvedTxnTS, true, [][]byte{})
	backOff := retry.NewBackoffer(context.Background(), asyncResolveLockMaxBackoff)

	// Save the async commit transaction resolved result to the resolver cache.
	lockResolver.saveResolved(resolvedTxnTS, resolvedTxnStatus)

	// With failpoint, the async commit transaction will be resolved and `CheckSecondaries` would not be called.
	// Otherwise, the test would panic as the storage is nil.
	require.Nil(t, failpoint.Enable("tikvclient/resolveAsyncCommitLockReturn", "return"))
	_, err := lockResolver.ResolveLocks(backOff, 5, []*Lock{NewLock(toResolveLock)})
	require.NoError(t, err)
	require.Nil(t, failpoint.Disable("tikvclient/resolveAsyncCommitLockReturn"))
}
