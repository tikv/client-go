package txnlock

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
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

func TestTryAsyncResolve(t *testing.T) {
	require.Equal(t, AsyncResolveLockSemaphoreLimit, cap(globalAsyncResolveLockSemaphore))
	mockMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_try_async_resolve_running_tasks",
		Help: "Test gauge for TestTryAsyncResolve",
	})
	checkMetricVal := func(v float64) {
		m := &dto.Metric{}
		require.NoError(t, mockMetric.Write(m))
		require.Equal(t, v, m.GetGauge().GetValue())
	}

	// default lock resolver should use the global async resolve lock semaphore
	lockResolver := NewLockResolver(nil)
	assert.Equal(t, cap(lockResolver.asyncResolvePool.semaphore), cap(globalAsyncResolveLockSemaphore))
	assert.Equal(t, 0, len(globalAsyncResolveLockSemaphore))

	exitLatches := make([]chan struct{}, 0, 16)
	tryAsync := func() (isAsync bool) {
		enterLatch := make(chan struct{})
		exitLatch := make(chan struct{})

		isAsync = lockResolver.asyncResolvePool.tryAsyncResolve(func() {
			close(enterLatch)
			<-exitLatch
		}, mockMetric)

		if isAsync {
			exitLatches = append(exitLatches, exitLatch)
		}

		return isAsync
	}

	exitTask := func(idx int) {
		close(exitLatches[idx])
		exitLatches[idx] = nil
	}

	defer func() {
		// clean up
		for _, l := range exitLatches {
			if l != nil {
				close(l)
			}
		}
		lockResolver.Close()
	}()

	// close old pool and mock a custom asyncResolveLockSemaphore with limit 5
	lockResolver.asyncResolvePool.Close()
	lockResolver.asyncResolvePool = newAsyncResolveTaskPool(make(chan struct{}, 5))
	waitSemaphoreSizeWithCheck := func(cnt int) {
		assert.Eventually(t, func() bool {
			return len(lockResolver.asyncResolvePool.semaphore) == cnt
		}, 10*time.Second, 10*time.Millisecond)
		checkMetricVal(float64(cnt))
	}

	// try to async resolve 3 times
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	waitSemaphoreSizeWithCheck(3)

	// exit 1 async goroutine
	exitTask(1)
	waitSemaphoreSizeWithCheck(2)

	// try to async resolve 3 more times, semaphore is used up.
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	require.True(t, tryAsync())
	waitSemaphoreSizeWithCheck(5)

	// more async task will be rejected
	require.False(t, tryAsync())
	require.False(t, tryAsync())
	require.False(t, tryAsync())
	waitSemaphoreSizeWithCheck(5)
	// after some time, the metric should still be correct to test pending async tasks do not cause metric change.
	time.Sleep(10 * time.Millisecond)
	checkMetricVal(5)

	// exit a task
	exitTask(3)
	waitSemaphoreSizeWithCheck(4)

	// a new task will be accepted again
	require.True(t, tryAsync())
	waitSemaphoreSizeWithCheck(5)

	// exit all tasks
	for i := range exitLatches {
		if exitLatches[i] != nil {
			exitTask(i)
		}
	}
	waitSemaphoreSizeWithCheck(0)

	// close resolver, then all async tasks should be rejected
	lockResolver.Close()
	require.False(t, tryAsync())
	waitSemaphoreSizeWithCheck(0)
}
