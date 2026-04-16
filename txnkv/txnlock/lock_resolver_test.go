package txnlock

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestLockResolverCache is used to cover the issue https://github.com/pingcap/tidb/issues/59494.
func TestLockResolverCache(t *testing.T) {
	util.EnableFailpoints()
	lockResolver := NewLockResolver(nil)
	t.Cleanup(lockResolver.Close)
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

func TestResolveLockLoggerBatchesEntries(t *testing.T) {
	restoreConfig := overrideResolveLockLogConfig(2, time.Hour, 16)
	defer restoreConfig()

	core, observedLogs := observer.New(zap.InfoLevel)
	restoreLogger := log.ReplaceGlobals(zap.New(core), nil)
	defer restoreLogger()

	lockResolver := NewLockResolver(nil)
	t.Cleanup(lockResolver.Close)
	lockResolver.saveResolved(1, TxnStatus{commitTS: 10})
	lockResolver.saveResolved(2, TxnStatus{action: kvrpcpb.Action_NoAction})

	bo := retry.NewBackoffer(context.Background(), asyncResolveLockMaxBackoff)
	locks := []*Lock{
		{
			Key:      []byte("k1"),
			Primary:  []byte("k1"),
			TxnID:    1,
			TxnSize:  1,
			LockType: kvrpcpb.Op_Put,
		},
		{
			Key:             []byte("k2"),
			Primary:         []byte("k2"),
			TxnID:           2,
			LockForUpdateTS: 20,
			LockType:        kvrpcpb.Op_PessimisticLock,
		},
	}

	_, err := lockResolver.ResolveLocksWithOpts(bo, ResolveLocksOptions{
		CallerStartTS: 123,
		Locks:         locks,
		Lite:          true,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(observedLogs.FilterMessage("txn resolve locks").AllUntimed()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	entries := observedLogs.FilterMessage("txn resolve locks").AllUntimed()
	require.Len(t, entries, 1)

	fields := entries[0].ContextMap()
	require.Equal(t, int64(2), fields["count"])

	resolvedLocks, ok := fields["resolve"].([]interface{})
	require.True(t, ok)
	require.Len(t, resolvedLocks, 2)

	resolveLockEntry, ok := resolvedLocks[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Put", resolveLockEntry["resolveType"])
	require.Equal(t, "commit", resolveLockEntry["action"])
	require.Equal(t, uint64(1), resolveLockEntry["txnStartTS"])
	require.Equal(t, uint64(10), resolveLockEntry["commitTS"])
	require.Equal(t, false, resolveLockEntry["regionResolve"])
	require.Equal(t, true, resolveLockEntry["resolvedByCheckTxnStatus"])

	resolvePessimisticLockEntry, ok := resolvedLocks[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "PessimisticLock", resolvePessimisticLockEntry["resolveType"])
	require.Equal(t, "rollback", resolvePessimisticLockEntry["action"])
	require.Equal(t, uint64(2), resolvePessimisticLockEntry["txnStartTS"])
	require.Equal(t, uint64(20), resolvePessimisticLockEntry["forUpdateTS"])
	require.Equal(t, false, resolvePessimisticLockEntry["regionResolve"])
	require.Equal(t, true, resolvePessimisticLockEntry["resolvedByCheckTxnStatus"])
}

func TestResolveLockLoggerReportsMissedEntries(t *testing.T) {
	restoreConfig := overrideResolveLockLogConfig(20, 10*time.Millisecond, 1)
	defer restoreConfig()

	core, observedLogs := observer.New(zap.InfoLevel)
	restoreLogger := log.ReplaceGlobals(zap.New(core), nil)
	defer restoreLogger()

	lockResolver := NewLockResolver(nil)
	t.Cleanup(lockResolver.Close)

	// exhaust asyncLogRunning to make sure `recordResolveLockLogEntry` the background loop will not consume.
	lockResolver.asyncLogRunning.Do(func() {})
	lockResolver.recordResolveLockLogEntry(resolveLockLogEntry{
		resolveType: "Put",
		lock: Lock{
			Key:     []byte("k1"),
			Primary: []byte("k1"),
			TxnID:   1,
		},
		action:     "commit",
		txnStartTS: 1,
	})
	lockResolver.recordResolveLockLogEntry(resolveLockLogEntry{
		resolveType: "Pessimistic",
		lock: Lock{
			Key:     []byte("k2"),
			Primary: []byte("k2"),
			TxnID:   2,
		},
		action:     "commit",
		txnStartTS: 2,
	})

	go lockResolver.collectResolveLockLogsLoop()

	require.Eventually(t, func() bool {
		return len(observedLogs.FilterMessage("missed resolve lock logs due to log channel full, some resolve lock operations may not be logged").AllUntimed()) == 1 &&
			len(observedLogs.FilterMessage("txn resolve locks").AllUntimed()) == 1
	}, time.Second, 100*time.Millisecond)

	missedEntries := observedLogs.FilterMessage("missed resolve lock logs due to log channel full, some resolve lock operations may not be logged").AllUntimed()
	require.Len(t, missedEntries, 1)
	require.Equal(t, uint64(1), missedEntries[0].ContextMap()["missed"])

	logEntries := observedLogs.FilterMessage("txn resolve locks").AllUntimed()
	require.Len(t, logEntries, 1)
	require.Equal(t, int64(1), logEntries[0].ContextMap()["count"])
}

func overrideResolveLockLogConfig(batchSize int, flushInterval time.Duration, channelSize int) func() {
	oldBatchSize := resolveLockLogBatchSize
	oldFlushInterval := resolveLockLogFlushInterval
	oldChannelSize := resolveLockLogChannelSize
	resolveLockLogBatchSize = batchSize
	resolveLockLogFlushInterval = flushInterval
	resolveLockLogChannelSize = channelSize
	return func() {
		resolveLockLogBatchSize = oldBatchSize
		resolveLockLogFlushInterval = oldFlushInterval
		resolveLockLogChannelSize = oldChannelSize
	}
}
