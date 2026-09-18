// Copyright 2026 TiKV Authors
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

package locate

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/txnprotocol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/pd/client/opt"
)

func TestTxnProtocolPublicationRejectsOlderLookups(t *testing.T) {
	store := newUninitializedStore(1)
	cache := newTxnVersionMapStoreCache(store)
	updater := storeCacheUpdater{stores: cache}
	older := time.Now()
	newer := older.Add(time.Second)
	_, err := store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 2), newer)
	require.NoError(t, err)
	for _, meta := range []*metapb.Store{storeMetaWithRange(1, 0, 1), {Id: 1}} {
		updater.refreshStores(context.Background(), older, []*metapb.Store{meta})
		require.Equal(t, uint32(2), store.getTxnProtocolVersionRange().Max)
	}
	// Authoritative later downgrades and disappearance of the field still apply.
	updater.refreshStores(context.Background(), newer.Add(time.Second), []*metapb.Store{storeMetaWithRange(1, 0, 0)})
	require.Zero(t, store.getTxnProtocolVersionRange().Max)
	require.True(t, store.getTxnProtocolVersionRange().Present)
	updater.refreshStores(context.Background(), newer.Add(2*time.Second), []*metapb.Store{{Id: 1}})
	require.False(t, store.getTxnProtocolVersionRange().Present)
}

type txnProtocolListStoreCache struct {
	storeCache
	fetchAll func(context.Context, ...opt.GetStoreOption) ([]*metapb.Store, error)
}

func (c *txnProtocolListStoreCache) fetchAllStores(ctx context.Context, opts ...opt.GetStoreOption) ([]*metapb.Store, error) {
	return c.fetchAll(ctx, opts...)
}

func TestTxnProtocolMetadataRefreshUsesLeader(t *testing.T) {
	store := newUninitializedStore(1)
	base := newTxnVersionMapStoreCache(store)
	var routerAllowed atomic.Bool
	checkLeader := func(opts []opt.GetStoreOption) {
		op := &opt.GetStoreOp{}
		for _, apply := range opts {
			apply(op)
		}
		if op.AllowRouterServiceHandle {
			routerAllowed.Store(true)
		}
	}
	cache := &txnVersionReloadStoreCache{storeCache: base}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		checkLeader(opts)
		return storeMetaWithRange(id, 0, 2), nil
	}
	_, err := store.initResolve(retry.NewNoopBackoff(context.Background()), cache)
	require.NoError(t, err)
	_, err = store.reResolve(cache)
	require.NoError(t, err)
	_, err = store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.NoError(t, err)
	listCache := &txnProtocolListStoreCache{storeCache: base, fetchAll: func(ctx context.Context, opts ...opt.GetStoreOption) ([]*metapb.Store, error) {
		checkLeader(opts)
		return []*metapb.Store{storeMetaWithRange(1, 0, 1)}, nil
	}}
	updater := storeCacheUpdater{stores: listCache}
	updater.tick(context.Background(), time.Now())
	require.False(t, routerAllowed.Load())
	require.Equal(t, uint32(1), store.getTxnProtocolVersionRange().Max)
}

func useTxnProtocolVersionForLocate(t *testing.T, version kvrpcpb.TxnProtocolVersion) {
	t.Helper()
	previous := tikvrpc.GetDefaultTxnProtocolVersion()
	require.NoError(t, tikvrpc.SetDefaultTxnProtocolVersion(version))
	t.Cleanup(func() { require.NoError(t, tikvrpc.SetDefaultTxnProtocolVersion(previous)) })
}

func storeMetaWithRange(id uint64, min, max uint32) *metapb.Store {
	return &metapb.Store{
		Id:      id,
		Address: "addr",
		TxnProtocolVersionRange: &metapb.TxnProtocolVersionRange{
			Min: min,
			Max: max,
		},
	}
}

func TestStoreTxnProtocolVersionRangePublish(t *testing.T) {
	store := newUninitializedStore(1)

	// A Store that never reported a range reads as unknown, which selection
	// normalizes to [0, 0].
	require.Equal(t, txnprotocol.StoreRange{}, store.getTxnProtocolVersionRange())

	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 2), time.Now())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())

	// A valid change replaces the snapshot atomically.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 1, 2), time.Now())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 1, Max: 2}, store.getTxnProtocolVersionRange())

	// min > max is invalid metadata: keep last-known-good and warn once.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 3, 2), time.Now())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 1, Max: 2}, store.getTxnProtocolVersionRange())
	require.True(t, store.txnProtocolVersionRangeMu.warned)

	// A missing field downgrades a previously known range to unknown.
	store.publishTxnProtocolVersionRange(&metapb.Store{Id: 1, Address: "addr"}, time.Now())
	require.Equal(t, txnprotocol.StoreRange{}, store.getTxnProtocolVersionRange())
	require.False(t, store.txnProtocolVersionRangeMu.warned)

	// With no known-good range, invalid metadata still keeps unknown.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 3, 2), time.Now())
	require.Equal(t, txnprotocol.StoreRange{}, store.getTxnProtocolVersionRange())
	require.True(t, store.txnProtocolVersionRangeMu.warned)

	// A valid range after invalid metadata is accepted and resets the warning.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 1), time.Now())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 1}, store.getTxnProtocolVersionRange())
	require.False(t, store.txnProtocolVersionRangeMu.warned)
}

func TestStoreTxnProtocolVersionRangeInvalidWarningState(t *testing.T) {
	store := newUninitializedStore(1)

	// The first invalid value is reported.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 3, 2), time.Now())
	require.True(t, store.txnProtocolVersionRangeMu.warned)
	require.Equal(t, uint32(3), store.txnProtocolVersionRangeMu.warnedMin)
	require.Equal(t, uint32(2), store.txnProtocolVersionRangeMu.warnedMax)

	// Repeating the same invalid value must not be reported again: only a changed
	// invalid value is a state change.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 3, 2), time.Now())
	require.Equal(t, uint32(3), store.txnProtocolVersionRangeMu.warnedMin)

	// A different invalid value is a state change.
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 4, 2), time.Now())
	require.Equal(t, uint32(4), store.txnProtocolVersionRangeMu.warnedMin)
}

// txnVersionMapStoreCache is a minimal storeCache used by the refresher and
// reload tests. Only the methods the code under test uses are implemented.
type txnVersionMapStoreCache struct {
	storeCache

	mu     sync.Mutex
	stores map[uint64]*Store
}

func newTxnVersionMapStoreCache(stores ...*Store) *txnVersionMapStoreCache {
	c := &txnVersionMapStoreCache{stores: make(map[uint64]*Store, len(stores))}
	for _, store := range stores {
		c.stores[store.storeID] = store
	}
	return c
}

func (c *txnVersionMapStoreCache) get(id uint64) (*Store, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	store, ok := c.stores[id]
	return store, ok
}

func (c *txnVersionMapStoreCache) getOrInsertDefault(id uint64) *Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	store, ok := c.stores[id]
	if !ok {
		store = newUninitializedStore(id)
		c.stores[id] = store
	}
	return store
}

// TestStoreCacheUpdaterRefreshesOnlyRangeOfExistingStore pins that the periodic
// full-store updater refreshes the range of an existing Store without touching
// address, labels, resolve state or anything else.
func TestStoreCacheUpdaterRefreshesOnlyRangeOfExistingStore(t *testing.T) {
	cached := newStore(1, "old-addr", "", "", tikvrpc.TiKV, resolved, []*metapb.StoreLabel{{Key: "zone", Value: "z1"}})
	cache := newTxnVersionMapStoreCache(cached)
	updater := &storeCacheUpdater{stores: cache}

	updater.refreshStores(context.Background(), time.Now(), []*metapb.Store{{
		Id:                      1,
		Address:                 "new-addr",
		State:                   metapb.StoreState_Up,
		Labels:                  []*metapb.StoreLabel{{Key: "zone", Value: "z2"}},
		TxnProtocolVersionRange: &metapb.TxnProtocolVersionRange{Min: 0, Max: 2},
	}})

	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, cached.getTxnProtocolVersionRange())
	require.Equal(t, "old-addr", cached.GetAddr())
	require.Equal(t, resolved, cached.getResolveState())
	require.Equal(t, "z1", cached.GetLabels()[0].GetValue())

	// A missing range in the next round downgrades the cached range to unknown.
	updater.refreshStores(context.Background(), time.Now(), []*metapb.Store{{
		Id:      1,
		Address: "new-addr",
		State:   metapb.StoreState_Up,
	}})
	require.Equal(t, txnprotocol.StoreRange{}, cached.getTxnProtocolVersionRange())

	// Invalid metadata keeps the last-known-good range (unknown here).
	updater.refreshStores(context.Background(), time.Now(), []*metapb.Store{storeMetaWithRange(1, 3, 2)})
	require.Equal(t, txnprotocol.StoreRange{}, cached.getTxnProtocolVersionRange())
}

func TestStoreCacheUpdaterInsertsMissingStoreWithRange(t *testing.T) {
	cache := newTxnVersionMapStoreCache()
	updater := &storeCacheUpdater{stores: cache}

	updater.refreshStores(context.Background(), time.Now(), []*metapb.Store{storeMetaWithRange(9, 0, 2)})

	store, ok := cache.get(9)
	require.True(t, ok)
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

func TestStoreTxnProtocolVersionRangeAtomicSnapshot(t *testing.T) {
	store := newUninitializedStore(1)
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 1), time.Now())

	stop := make(chan struct{})
	var (
		wg           sync.WaitGroup
		invalidRange atomic.Bool
	)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(min, max uint32) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					store.publishTxnProtocolVersionRange(storeMetaWithRange(1, min, max), time.Now())
				}
			}
		}(uint32(i), uint32(i)+1)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					r := store.getTxnProtocolVersionRange()
					if r.Present && r.Min > r.Max {
						// Do not call require from a worker goroutine: record the
						// violation and assert it on the test goroutine.
						invalidRange.Store(true)
					}
				}
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	require.False(t, invalidRange.Load(), "a published range must always be valid")
}

// txnVersionReloadStoreCache lets tests control what a conditional PD reload
// observes.
type txnVersionReloadStoreCache struct {
	storeCache

	fetchStoreFn func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error)
	// fetchCalls counts every PD store lookup served by this cache.
	fetchCalls atomic.Int32
	// routerAllowed records whether any fetch allowed the router service.
	routerAllowed atomic.Bool
}

func (c *txnVersionReloadStoreCache) fetchStore(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
	c.fetchCalls.Add(1)
	op := &opt.GetStoreOp{}
	for _, o := range opts {
		o(op)
	}
	if op.AllowRouterServiceHandle {
		c.routerAllowed.Store(true)
	}
	return c.fetchStoreFn(ctx, id, opts...)
}

// txnVersionPDControl wraps a RegionCache's store cache so tests can change the
// range that a conditional reload observes.
type txnVersionPDControl struct {
	cache *txnVersionReloadStoreCache
	r     atomic.Pointer[metapb.TxnProtocolVersionRange]
	// fail makes the next PD lookups fail.
	fail atomic.Bool
	// onFetch is called at the start of every PD lookup.
	onFetch func()
	// releaseFetch, when non-nil, holds every PD lookup open until it is closed.
	releaseFetch chan struct{}
}

func (c *txnVersionPDControl) setRange(min, max uint32) {
	c.r.Store(&metapb.TxnProtocolVersionRange{Min: min, Max: max})
}

func (c *txnVersionPDControl) fetchCalls() int32 {
	return c.cache.fetchCalls.Load()
}

func (c *txnVersionPDControl) pdRange() *metapb.TxnProtocolVersionRange {
	return c.r.Load()
}

// wrapStoreCacheForTxnProtocolVersionRange replaces the store cache of rc with a
// wrapper whose PD lookups return the configured range. Stores keep their real
// metadata, so address resolution and routing are unaffected. Wrapping the same
// cache repeatedly reuses the underlying cache instead of nesting wrappers.
func wrapStoreCacheForTxnProtocolVersionRange(rc *RegionCache, min, max uint32) *txnVersionPDControl {
	base := rc.stores
	if wrapper, ok := base.(*txnVersionReloadStoreCache); ok {
		base = wrapper.storeCache
	}
	control := &txnVersionPDControl{cache: &txnVersionReloadStoreCache{storeCache: base}}
	control.setRange(min, max)
	control.cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		if control.onFetch != nil {
			control.onFetch()
		}
		if control.releaseFetch != nil {
			select {
			case <-control.releaseFetch:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if control.fail.Load() {
			return nil, context.DeadlineExceeded
		}
		meta, err := base.fetchStore(ctx, id, opts...)
		if err != nil || meta == nil {
			return meta, err
		}
		r := control.pdRange()
		if r == nil {
			return meta, nil
		}
		cloned := *meta
		cloned.TxnProtocolVersionRange = r
		return &cloned, nil
	}
	rc.stores = control.cache
	return control
}

func useTxnVersionReloadIntervals(t *testing.T, cooldown, timeout time.Duration) {
	t.Helper()
	previousCooldown, previousTimeout := txnProtocolVersionReloadCooldown, txnProtocolVersionReloadTimeout
	txnProtocolVersionReloadCooldown, txnProtocolVersionReloadTimeout = cooldown, timeout
	t.Cleanup(func() {
		txnProtocolVersionReloadCooldown, txnProtocolVersionReloadTimeout = previousCooldown, previousTimeout
	})
}

func TestReloadTxnProtocolVersionRangeSingleFlightAndCooldown(t *testing.T) {
	// A long cooldown keeps late callers inside the in-flight window even on a
	// loaded machine, so the single-flight assertion is not timing sensitive. The
	// reload timeout must stay above the window the test holds it open for.
	useTxnVersionReloadIntervals(t, time.Second, 10*time.Second)

	store := newUninitializedStore(1)
	release := make(chan struct{})
	var fetchCalls atomic.Int32
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		fetchCalls.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return storeMetaWithRange(id, 0, 2), nil
	}

	const callers = 8
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
		}()
	}

	// Wait until the owner started, then let the cooldown elapse while the reload
	// is still in flight: late callers must join it instead of issuing another PD
	// request.
	require.Eventually(t, func() bool { return fetchCalls.Load() >= 1 }, time.Second, time.Millisecond)
	time.Sleep(2 * txnProtocolVersionReloadCooldown)
	close(release)
	wg.Wait()

	require.Equal(t, int32(1), fetchCalls.Load())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
	require.False(t, cache.routerAllowed.Load(), "conditional reload must be handled by the PD leader")
}

func TestReloadTxnProtocolVersionRangeCooldown(t *testing.T) {
	useTxnVersionReloadIntervals(t, 50*time.Millisecond, time.Second)

	store := newUninitializedStore(1)
	var fetchCalls atomic.Int32
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		fetchCalls.Add(1)
		return storeMetaWithRange(id, 0, 2), nil
	}

	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, int32(1), fetchCalls.Load())

	// Inside the cooldown the caller returns without issuing another PD request.
	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, int32(1), fetchCalls.Load())

	// After the cooldown it may reload again.
	time.Sleep(txnProtocolVersionReloadCooldown + 10*time.Millisecond)
	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, int32(2), fetchCalls.Load())
}

func TestReloadTxnProtocolVersionRangeDifferentStores(t *testing.T) {
	useTxnVersionReloadIntervals(t, time.Millisecond, time.Second)

	release := make(chan struct{})
	var (
		firstStarted  = make(chan struct{})
		secondStarted = make(chan struct{})
		firstOnce     sync.Once
		secondOnce    sync.Once
	)
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		switch id {
		case 1:
			firstOnce.Do(func() { close(firstStarted) })
			<-release
		case 2:
			secondOnce.Do(func() { close(secondStarted) })
		}
		return storeMetaWithRange(id, 0, 2), nil
	}

	store1, store2 := newUninitializedStore(1), newUninitializedStore(2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		store1.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	}()
	<-firstStarted

	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		store2.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a reload of a different store must not be blocked by an in-flight one")
	}
	<-secondStarted

	close(release)
	wg.Wait()
}

func TestReloadTxnProtocolVersionRangeFailureEntersCooldown(t *testing.T) {
	useTxnVersionReloadIntervals(t, 50*time.Millisecond, time.Second)

	store := newUninitializedStore(1)
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 1), time.Now())
	var fetchCalls atomic.Int32
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		fetchCalls.Add(1)
		return nil, context.DeadlineExceeded
	}

	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, int32(1), fetchCalls.Load())
	// A failed attempt still enters the cooldown and keeps the old snapshot.
	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, int32(1), fetchCalls.Load())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 1}, store.getTxnProtocolVersionRange())

	time.Sleep(txnProtocolVersionReloadCooldown + 10*time.Millisecond)
	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, int32(2), fetchCalls.Load())
}

func TestReloadTxnProtocolVersionRangeWaiterCancellationIsolated(t *testing.T) {
	useTxnVersionReloadIntervals(t, time.Millisecond, time.Second)

	store := newUninitializedStore(1)
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return storeMetaWithRange(id, 0, 2), nil
	}

	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	}()
	<-started

	// A waiter that gives up must not abort the shared reload.
	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		store.reloadTxnProtocolVersionRange(waiterCtx, cache, context.Background())
	}()
	cancel()
	select {
	case <-waiterDone:
	case <-time.After(time.Second):
		t.Fatal("a canceled waiter must stop waiting")
	}

	close(release)
	<-ownerDone
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

func TestReloadTxnProtocolVersionRangeOwnerCancellationKeepsSharedTask(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		useTxnVersionReloadIntervals(t, time.Millisecond, 10*time.Second)

		store := newUninitializedStore(1)
		release := make(chan struct{})
		started := make(chan struct{})
		var once sync.Once
		cache := &txnVersionReloadStoreCache{}
		cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return storeMetaWithRange(id, 0, 2), nil
		}

		ownerCtx, cancelOwner := context.WithCancel(context.Background())
		ownerResult := make(chan error, 1)
		go func() {
			_, err := store.reloadTxnProtocolVersionRange(ownerCtx, cache, context.Background())
			ownerResult <- err
		}()
		<-started

		// Canceling the caller that started the shared task must return immediately
		// without touching the PD lookup.
		cancelOwner()
		select {
		case err := <-ownerResult:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("the owner must stop waiting as soon as its context is canceled")
		}

		// The shared task is still running, so an uncanceled waiter can join it and
		// observe its result.
		waiterResult := make(chan error, 1)
		go func() {
			_, err := store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
			waiterResult <- err
		}()
		synctest.Wait() // Ensure the remaining waiter has joined the in-flight task.
		close(release)

		select {
		case err := <-waiterResult:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("the shared reload must complete for the remaining waiter")
		}
		require.Equal(t, int32(1), cache.fetchCalls.Load())
		require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
	})
}

func TestReloadTxnProtocolVersionRangeHonorsClientShutdown(t *testing.T) {
	useTxnVersionReloadIntervals(t, time.Millisecond, time.Second)

	store := newUninitializedStore(1)
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 1), time.Now())
	var gotCanceled atomic.Bool
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		<-ctx.Done()
		gotCanceled.Store(true)
		return nil, ctx.Err()
	}

	lifecycle, shutdown := context.WithCancel(context.Background())
	shutdown()
	store.reloadTxnProtocolVersionRange(context.Background(), cache, lifecycle)

	require.True(t, gotCanceled.Load())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 1}, store.getTxnProtocolVersionRange())

	// The canceled task leaves a consistent single-flight state: a later call with
	// a live lifecycle can reload the range.
	time.Sleep(txnProtocolVersionReloadCooldown + 10*time.Millisecond)
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		return storeMetaWithRange(id, 0, 2), nil
	}
	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 2}, store.getTxnProtocolVersionRange())
}

func TestReloadTxnProtocolVersionRangeIgnoresTombstoneAndNil(t *testing.T) {
	useTxnVersionReloadIntervals(t, time.Millisecond, time.Second)

	store := newUninitializedStore(1)
	store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 1), time.Now())
	cache := &txnVersionReloadStoreCache{}
	cache.fetchStoreFn = func(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
		meta := storeMetaWithRange(id, 0, 2)
		meta.State = metapb.StoreState_Tombstone
		return meta, nil
	}

	store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
	require.Equal(t, txnprotocol.StoreRange{Present: true, Min: 0, Max: 1}, store.getTxnProtocolVersionRange())
}

func TestTxnProtocolReloadRetainsOutcomeDuringCooldown(t *testing.T) {
	useTxnVersionReloadIntervals(t, time.Hour, time.Second)
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			store := newUninitializedStore(1)
			cache := &txnVersionReloadStoreCache{}
			cache.fetchStoreFn = func(context.Context, uint64, ...opt.GetStoreOption) (*metapb.Store, error) {
				if failed {
					return nil, context.DeadlineExceeded
				}
				return storeMetaWithRange(1, 0, 2), nil
			}
			first, firstErr := store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
			if failed {
				require.ErrorIs(t, firstErr, context.DeadlineExceeded)
			} else {
				require.NoError(t, firstErr)
				require.Equal(t, uint32(2), first.Max)
			}
			// An unrelated publication must not change the recorded PD outcome.
			_, err := store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 0), time.Now())
			require.NoError(t, err)
			second, secondErr := store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
			require.Equal(t, first, second)
			require.Equal(t, firstErr, secondErr)
			require.Equal(t, int32(1), cache.fetchCalls.Load())
		})
	}
}

func TestTxnProtocolReloadRejectsUnusableResult(t *testing.T) {
	for _, name := range []string{"nil", "tombstone", "invalid", "superseded"} {
		t.Run(name, func(t *testing.T) {
			store := newUninitializedStore(1)
			_, err := store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 1), time.Now())
			require.NoError(t, err)
			cache := &txnVersionReloadStoreCache{}
			cache.fetchStoreFn = func(context.Context, uint64, ...opt.GetStoreOption) (*metapb.Store, error) {
				meta := storeMetaWithRange(1, 0, 2)
				switch name {
				case "nil":
					return nil, nil
				case "tombstone":
					meta.State = metapb.StoreState_Tombstone
				case "invalid":
					meta.TxnProtocolVersionRange.Min = 3
				case "superseded":
					store.publishTxnProtocolVersionRange(storeMetaWithRange(1, 0, 0), time.Now())
				}
				return meta, nil
			}
			_, err = store.reloadTxnProtocolVersionRange(context.Background(), cache, context.Background())
			require.Error(t, err)
			if name == "superseded" {
				require.Zero(t, store.getTxnProtocolVersionRange().Max)
			} else {
				require.Equal(t, uint32(1), store.getTxnProtocolVersionRange().Max)
			}
		})
	}
}
