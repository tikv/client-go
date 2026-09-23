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
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
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

// TestCheckSecondariesResponseError covers the regression where the response
// error was ignored: an error response with empty locks and a zero commit ts was
// interpreted as a missing lock, that is as a rolled-back transaction.
func TestCheckSecondariesResponseError(t *testing.T) {
	const txnID = uint64(4200)
	keys := [][]byte{[]byte("a"), []byte("b")}

	t.Run("error_with_empty_locks_is_propagated", func(t *testing.T) {
		f := newCheckSecondariesFixture(t)
		f.respond(0, nil, 0, &kvrpcpb.KeyError{Abort: "shared lock invariant violated"})

		shared := &asyncResolveData{commitTs: 10, keys: [][]byte{}, missingLock: false}
		err := f.resolver.checkSecondaries(checkSecondariesBo(), txnID, keys, f.regions[0], shared)

		require.Error(t, err)
		require.Contains(t, err.Error(), "shared lock invariant violated")

		// The error must not be turned into a missing lock.
		require.False(t, shared.missingLock)
		require.Equal(t, uint64(10), shared.commitTs)
		require.Empty(t, shared.keys)
		require.Equal(t, 1, f.store.count(tikvrpc.CmdCheckSecondaryLocks))
	})

	t.Run("nil_error_with_empty_locks_keeps_missing_lock_semantics", func(t *testing.T) {
		f := newCheckSecondariesFixture(t)
		f.respond(0, nil, 0, nil)

		shared := &asyncResolveData{commitTs: 10, keys: [][]byte{}, missingLock: false}
		require.NoError(t, f.resolver.checkSecondaries(checkSecondariesBo(), txnID, keys, f.regions[0], shared))

		// A zero commit ts with no locks still means the transaction was rolled back.
		require.True(t, shared.missingLock)
		require.Equal(t, uint64(0), shared.commitTs)
		require.Empty(t, shared.keys)
	})

}

// TestResolveAsyncCommitLockDiscardsAggregateOnError verifies that a shard error
// prevents the caller from caching partial data or sending ResolveLock.
func TestResolveAsyncCommitLockDiscardsAggregateOnError(t *testing.T) {
	const txnID = uint64(4200)
	const minCommitTs = uint64(10)
	// A commit ts on the successful shard, so the aggregate would be cacheable if
	// it were allowed to stand: that is what makes "no status saved" observable.
	const aggregateCommitTs = uint64(99)

	f := newCheckSecondariesFixture(t)
	// The first shard reports one lock for two requested keys, which means the
	// other key was committed or rolled back at this commit ts.
	f.respond(0, []*kvrpcpb.LockInfo{asyncCommitSecondary([]byte("a"), txnID, 30)}, aggregateCommitTs, nil)
	// The second shard is rejected by the server-side invariant check.
	f.respond(1, nil, 0, &kvrpcpb.KeyError{Abort: "shared lock invariant violated"})

	lock := &Lock{
		Key:            []byte("p"),
		Primary:        []byte("p"),
		TxnID:          txnID,
		UseAsyncCommit: true,
		MinCommitTS:    minCommitTs,
	}
	// An undetermined status is what makes the caller consult the secondaries.
	// The ttl must be non-zero: with ttl, commitTS and action all zero the status
	// is already determined as rolled back, and the caller would skip the check.
	// A live ttl with no commit ts means "still locked, status unknown".
	status := TxnStatus{
		ttl: 100,
		primaryLock: &kvrpcpb.LockInfo{
			Key:            []byte("p"),
			LockVersion:    txnID,
			UseAsyncCommit: true,
			MinCommitTs:    minCommitTs,
			Secondaries:    [][]byte{[]byte("a"), []byte("z")},
		},
	}
	require.False(t, status.IsStatusDetermined())
	// The aggregate is determined, so a successful run would cache it and this
	// makes the "nothing cached" assertion below meaningful.
	require.True(t, TxnStatus{ttl: 100, commitTS: aggregateCommitTs}.IsStatusDetermined())

	resolved, err := f.resolver.resolveAsyncCommitLock(checkSecondariesBo(), lock, status, false)

	// The error propagates and no partial aggregate leaks to the caller.
	require.Error(t, err)
	require.Contains(t, err.Error(), "shared lock invariant violated")
	require.Equal(t, TxnStatus{}, resolved)
	// Both shards were checked and nothing else was sent, in particular no
	// ResolveLock from the resolve phase.
	require.Equal(t, 2, f.store.count(tikvrpc.CmdCheckSecondaryLocks))
	require.Equal(t, 2, len(f.store.sent))
	require.Zero(t, f.store.count(tikvrpc.CmdResolveLock))
	// Nothing may be persisted from a failed check. The aggregate above is
	// cacheable, so this would fail if the caller saved it despite the error.
	_, cached := f.resolver.getResolved(txnID)
	require.False(t, cached, "a failed check must not save a resolved status")
}

// metadataOnlyMVCCStore satisfies mocktikv.MVCCStore by embedding it. The
// fixture only uses region metadata, so an unexpected data access dispatches
// through the nil embedded interface and fails loudly.
type metadataOnlyMVCCStore struct {
	mocktikv.MVCCStore
}

type checkSecondariesStore struct {
	cache     *locate.RegionCache
	responses map[uint64]*kvrpcpb.CheckSecondaryLocksResponse
	fail      func(format string, args ...any)
	sent      []tikvrpc.CmdType
}

func (s *checkSecondariesStore) GetRegionCache() *locate.RegionCache {
	return s.cache
}

func (s *checkSecondariesStore) GetOracle() oracle.Oracle {
	return nil
}

func (s *checkSecondariesStore) SendReq(_ *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, _ time.Duration) (*tikvrpc.Response, error) {
	s.sent = append(s.sent, req.Type)
	// Only CheckSecondaryLocks is expected during the check phase. Any other
	// command, in particular ResolveLock, means the check phase decided to act on
	// a lock state it must not have inferred.
	resp, ok := s.responses[regionID.GetID()]
	if !ok {
		s.fail("unexpected command %s for region %d", req.Type, regionID.GetID())
	}
	return &tikvrpc.Response{Resp: resp}, nil
}

func (s *checkSecondariesStore) count(cmd tikvrpc.CmdType) int {
	n := 0
	for _, sent := range s.sent {
		if sent == cmd {
			n++
		}
	}
	return n
}

// checkSecondariesFixture is an in-memory cluster with two regions and a
// resolver whose lock checks run inline.
type checkSecondariesFixture struct {
	resolver *LockResolver
	store    *checkSecondariesStore
	// regions[i] is the region that holds the keys of the i-th shard.
	regions []locate.RegionVerID
}

func newCheckSecondariesFixture(t *testing.T) *checkSecondariesFixture {
	t.Helper()

	// Avoid a leveldb-backed store and its background goroutines; only metadata is used.
	cluster := mocktikv.NewCluster(metadataOnlyMVCCStore{})
	mocktikv.BootstrapWithMultiRegions(cluster, []byte("m"))

	cache := locate.NewTestRegionCache()
	cache.SetPDClient(locate.NewCodecPDClient(apicodec.ModeTxn, mocktikv.NewPDClient(cluster)))
	t.Cleanup(cache.Close)

	bo := retry.NewBackofferWithVars(context.Background(), asyncResolveLockMaxBackoff, nil)
	left, err := cache.LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	right, err := cache.LocateKey(bo, []byte("z"))
	require.NoError(t, err)
	require.NotEqual(t, left.Region.GetID(), right.Region.GetID(), "the fixture must have two regions")

	store := &checkSecondariesStore{
		cache:     cache,
		responses: make(map[uint64]*kvrpcpb.CheckSecondaryLocksResponse),
		fail:      t.Fatalf,
	}
	resolver := NewLockResolver(store)
	// Run checks inline with a pool isolated from the global async resolve state.
	resolver.asyncResolvePool = newAsyncResolveTaskPool(make(chan struct{}, 1))
	resolver.asyncResolvePool.Close()
	t.Cleanup(resolver.Close)

	return &checkSecondariesFixture{
		resolver: resolver,
		store:    store,
		regions:  []locate.RegionVerID{left.Region, right.Region},
	}
}

func (f *checkSecondariesFixture) respond(shard int, locks []*kvrpcpb.LockInfo, commitTs uint64, keyErr *kvrpcpb.KeyError) {
	f.store.responses[f.regions[shard].GetID()] = &kvrpcpb.CheckSecondaryLocksResponse{
		Locks:    locks,
		CommitTs: commitTs,
		Error:    keyErr,
	}
}

func checkSecondariesBo() *retry.Backoffer {
	return retry.NewBackofferWithVars(context.Background(), asyncResolveLockMaxBackoff, nil)
}

func asyncCommitSecondary(key []byte, txnID uint64, minCommitTs uint64) *kvrpcpb.LockInfo {
	return &kvrpcpb.LockInfo{
		Key:            key,
		LockVersion:    txnID,
		UseAsyncCommit: true,
		MinCommitTs:    minCommitTs,
	}
}
