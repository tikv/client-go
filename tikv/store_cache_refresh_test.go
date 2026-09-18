// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package tikv

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestStoreCacheRefreshUpdatesLeaderFromNotLeader(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	oldStore := storeIDs[0]
	newStore := storeIDs[1]

	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, regionID, loc.Region.GetID())
	cached := store.GetRegionCache().GetCachedRegionWithRLock(loc.Region)
	require.NotNil(t, cached)
	require.Equal(t, oldStore, cached.GetLeaderStoreID())

	status := store.GetStoreCacheStatus(oldStore)
	require.Greater(t, status.Matched, 0)
	require.False(t, status.Ready)

	cluster.ChangeLeader(regionID, peerIDs[1])

	res := store.RefreshStoreCache(context.Background(), oldStore)
	require.Equal(t, 0, res.Remaining, "errors=%v updated=%d matched=%d failed=%d", res.Errors, res.Updated, res.Matched, res.Failed)
	require.Equal(t, 0, res.Failed)
	require.True(t, res.Ready)
	require.Greater(t, res.Updated, 0)
	cached = store.GetRegionCache().GetCachedRegionWithRLock(loc.Region)
	require.Equal(t, newStore, cached.GetLeaderStoreID())
	n := 0
	store.refreshTasks.Range(func(_, _ any) bool {
		n++
		return true
	})
	require.Zero(t, n, "ready refresh should drop the store task")
}

func TestStoreCacheRefreshStaleTaskDoesNotStartAfterRecycle(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)

	var inFlight, maxInFlight atomic.Int64
	holdProbe := make(chan struct{})
	var blockProbe atomic.Bool
	entered := make(chan struct{}, 8)
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				n := inFlight.Add(1)
				for {
					old := maxInFlight.Load()
					if n <= old || maxInFlight.CompareAndSwap(old, n) {
						break
					}
				}
				if blockProbe.Load() {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-holdProbe
				}
				inFlight.Add(-1)
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	oldLoc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	cluster.ChangeLeader(regionID, peerIDs[1])

	loaded := make(chan struct{})
	proceed := make(chan struct{})
	firstCh := make(chan StoreCacheRefreshResult, 1)
	go func() {
		firstCh <- store.refreshStoreCache(context.Background(), storeIDs[0], func() {
			close(loaded)
			<-proceed
		})
	}()
	<-loaded

	second := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.True(t, second.Ready, "first completing refresh should recycle: %+v", second)

	childPeers := cluster.AllocIDs(3)
	childID := cluster.AllocID()
	cluster.Split(regionID, childID, []byte("m"), childPeers, childPeers[0])
	store.GetRegionCache().InvalidateCachedRegion(oldLoc.Region)
	zloc, err := store.GetRegionCache().LocateKey(bo, []byte("z"))
	require.NoError(t, err)
	require.Equal(t, storeIDs[0], store.GetRegionCache().GetCachedRegionWithRLock(zloc.Region).GetLeaderStoreID())
	require.Greater(t, store.GetStoreCacheStatus(storeIDs[0]).Matched, 0)

	blockProbe.Store(true)
	thirdCh := make(chan StoreCacheRefreshResult, 1)
	go func() {
		thirdCh <- store.RefreshStoreCache(context.Background(), storeIDs[0])
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("new-round refresh did not enter a blocking probe")
	}

	close(proceed)
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	seen := maxInFlight.Load()
	close(holdProbe)
	<-firstCh
	<-thirdCh
	require.Equal(t, int64(1), seen, "stale recycled task started a second worker set")
	require.Equal(t, int64(1), maxInFlight.Load())
}

func TestStoreCacheRefreshEmptyStoreID(t *testing.T) {
	client, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()
	res := store.RefreshStoreCache(context.Background(), 0)
	require.True(t, res.Ready)
	require.Equal(t, 0, res.Matched)
}

func TestStoreCacheStatusDoesNotCountOtherStore(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()
	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	st := store.GetStoreCacheStatus(storeIDs[2])
	require.Equal(t, 0, st.Matched)
	require.True(t, st.Ready)
}

func TestApplyLeaderIfOnStoreDoesNotOverrideNewerLeader(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	cache := store.GetRegionCache()
	cache.UpdateLeader(loc.Region, &metapb.Peer{Id: peerIDs[2], StoreId: storeIDs[2]}, 0)
	require.Equal(t, storeIDs[2], cache.GetCachedRegionWithRLock(loc.Region).GetLeaderStoreID())

	applied, moved := cache.ApplyLeaderIfOnStore(loc.Region, &metapb.Peer{Id: peerIDs[1], StoreId: storeIDs[1]}, storeIDs[0])
	require.False(t, applied)
	require.True(t, moved)
	require.Equal(t, storeIDs[2], cache.GetCachedRegionWithRLock(loc.Region).GetLeaderStoreID())
}

func TestStoreCacheRefreshDoesNotOverrideNewerLeaderDuringRPC(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	var store *KVStore
	store, err = NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				if store == nil {
					return
				}
				cache := store.GetRegionCache()
				bo := NewBackofferWithVars(context.Background(), 5000, nil)
				loc, locErr := cache.LocateKey(bo, []byte("a"))
				if locErr != nil {
					return
				}
				cache.UpdateLeader(loc.Region, &metapb.Peer{Id: peerIDs[2], StoreId: storeIDs[2]}, 0)
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, storeIDs[0], store.GetRegionCache().GetCachedRegionWithRLock(loc.Region).GetLeaderStoreID())

	cluster.ChangeLeader(regionID, peerIDs[1])
	res := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Equal(t, 0, res.Updated, "errors=%v remaining=%d failed=%d", res.Errors, res.Remaining, res.Failed)
	require.Equal(t, storeIDs[2], store.GetRegionCache().GetCachedRegionWithRLock(loc.Region).GetLeaderStoreID())
}

func TestStoreCacheRefreshFailedThenExpiredNotReady(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	res := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, res.Ready)
	require.Greater(t, res.Failed, 0)
	require.Greater(t, res.Remaining, 0)

	store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.Equal(t, 0, status.Matched)
	require.Greater(t, status.Failed, 0)
	require.False(t, status.Ready)
}

func TestStoreCacheRefreshConcurrentCallsShareOneJob(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)

	var inFlight, maxInFlight atomic.Int64
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				n := inFlight.Add(1)
				for {
					old := maxInFlight.Load()
					if n <= old || maxInFlight.CompareAndSwap(old, n) {
						break
					}
				}
				time.Sleep(80 * time.Millisecond)
				inFlight.Add(-1)
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	cluster.ChangeLeader(regionID, peerIDs[1])

	var wg sync.WaitGroup
	results := make([]StoreCacheRefreshResult, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = store.RefreshStoreCache(context.Background(), storeIDs[0])
		}(i)
	}
	wg.Wait()
	require.Equal(t, int64(1), maxInFlight.Load())
	require.True(t, results[0].Ready)
	require.True(t, results[1].Ready)
	require.Equal(t, results[0].Updated, results[1].Updated)
}

func TestStoreCacheRefreshJobDeadline(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				time.Sleep(200 * time.Millisecond)
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	res := store.RefreshStoreCache(ctx, storeIDs[0])
	require.False(t, res.Ready)
	require.Greater(t, res.Failed, 0)
}

func TestStoreCacheRefreshResolvesFailureAfterBusinessMovesLeader(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	first := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, first.Ready)
	require.Greater(t, first.Failed, 0)

	store.GetRegionCache().UpdateLeader(loc.Region, &metapb.Peer{Id: peerIDs[1], StoreId: storeIDs[1]}, 0)
	result := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.True(t, result.Ready, "business corrected cached route but task never recovers: %+v", result)
	require.Equal(t, 0, result.Failed)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.True(t, status.Ready)
	require.Equal(t, 0, status.Failed)
}

func TestStoreCacheRefreshSetsInternalRequestSource(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	var got string
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onReq: func(req *tikvrpc.Request) {
				got = req.GetRequestSource()
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	cluster.ChangeLeader(regionID, peerIDs[1])
	_ = store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Equal(t, "internal_store_cache_refresh", got)
}

func TestStoreCacheRefreshRecoversAfterRegionVersionReplacement(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peers, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	old, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.False(t, store.RefreshStoreCache(context.Background(), storeIDs[0]).Ready)

	childPeers := cluster.AllocIDs(3)
	childID := cluster.AllocID()
	cluster.Split(regionID, childID, []byte("m"), childPeers, childPeers[1])
	cluster.ChangeLeader(regionID, peers[1])
	store.GetRegionCache().InvalidateCachedRegion(old.Region)
	newer, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.NotEqual(t, old.Region, newer.Region)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("z"))
	require.NoError(t, err)
	result := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.True(t, result.Ready, "all replacement routes valid on new store but old version never resolves: %+v", result)
}

func TestStoreCacheRefreshKeepsFailureIfSplitSiblingStillOnStore(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peers, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	old, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.False(t, store.RefreshStoreCache(context.Background(), storeIDs[0]).Ready)

	childPeers := cluster.AllocIDs(3)
	childID := cluster.AllocID()
	// Right sibling keeps a leader on the original store.
	cluster.Split(regionID, childID, []byte("m"), childPeers, childPeers[0])
	cluster.ChangeLeader(regionID, peers[1])
	store.GetRegionCache().InvalidateCachedRegion(old.Region)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	zloc, err := store.GetRegionCache().LocateKey(bo, []byte("z"))
	require.NoError(t, err)
	zCached := store.GetRegionCache().GetCachedRegionWithRLock(zloc.Region)
	require.NotNil(t, zCached)
	require.Equal(t, storeIDs[0], zCached.GetLeaderStoreID())
	result := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, result.Ready, "right sibling still on old store but parent failure was cleared: %+v", result)
}

func TestStoreCacheRefreshKeepsFailureIfRightHalfUncached(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peers, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	old, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.False(t, store.RefreshStoreCache(context.Background(), storeIDs[0]).Ready)

	childPeers := cluster.AllocIDs(3)
	childID := cluster.AllocID()
	cluster.Split(regionID, childID, []byte("m"), childPeers, childPeers[1])
	cluster.ChangeLeader(regionID, peers[1])
	store.GetRegionCache().InvalidateCachedRegion(old.Region)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.False(t, status.Ready, "uncached right half must not clear the parent failure: %+v", status)
	require.Greater(t, status.Failed, 0)
	result := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Equal(t, 0, result.Remaining, "errors=%v updated=%d failed=%d scanned=%d", result.Errors, result.Updated, result.Failed, result.Scanned)
	require.Equal(t, 0, result.Failed)
	require.True(t, result.Ready, "POST must walk past startKey and converge after both halves left: %+v", result)
}

func TestStoreCacheRefreshSplitRightHalfStaysThenConverges(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peers, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	old, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.False(t, store.RefreshStoreCache(context.Background(), storeIDs[0]).Ready)

	rightPeers := cluster.AllocIDs(3)
	rightID := cluster.AllocID()
	cluster.Split(regionID, rightID, []byte("m"), rightPeers, rightPeers[0])
	cluster.ChangeLeader(regionID, peers[1])
	store.GetRegionCache().InvalidateCachedRegion(old.Region)
	stuck := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, stuck.Ready, "right sibling still on old store but parent failure was cleared: %+v", stuck)
	require.Greater(t, stuck.Failed, 0)

	cluster.ChangeLeader(rightID, rightPeers[1])
	done := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Equal(t, 0, done.Remaining, "errors=%v updated=%d failed=%d scanned=%d", done.Errors, done.Updated, done.Failed, done.Scanned)
	require.Equal(t, 0, done.Failed)
	require.True(t, done.Ready, "after right half leaves the store, failed range must converge: %+v", done)
}

func TestStoreCacheRefreshManySplitsConverges(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.Greater(t, store.RefreshStoreCache(context.Background(), storeIDs[0]).Failed, 0)
	store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	cluster.ChangeLeader(regionID, peerIDs[1])
	for i := 0; i < 129; i++ {
		rightID, peers := cluster.AllocID(), cluster.AllocIDs(3)
		cluster.Split(regionID, rightID, []byte(fmt.Sprintf("m%03d", i)), peers, peers[1])
		regionID = rightID
	}
	var last StoreCacheRefreshResult
	for i := 0; i < 3; i++ {
		last = store.RefreshStoreCache(context.Background(), storeIDs[0])
		if last.Ready {
			require.Equal(t, 0, last.Remaining)
			require.Equal(t, 0, last.Failed)
			return
		}
	}
	require.True(t, last.Ready, "128-span budget must skip cached prefix and reach suffix: %+v", last)
}

func TestStoreCacheRefreshRecoverGoneCancelled(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	require.False(t, store.RefreshStoreCache(context.Background(), storeIDs[0]).Ready)
	store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	extra, _ := store.recoverGoneUnresolved(ctx, storeIDs[0], map[locate.RegionVerID]struct{}{})
	require.Empty(t, extra)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.Greater(t, status.Failed, 0)
	require.False(t, status.Ready)
}

func TestStoreCacheStatusReadDoesNotCreateTasks(t *testing.T) {
	client, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()
	for id := uint64(1); id <= 1000; id++ {
		store.GetStoreCacheStatus(id)
	}
	count := 0
	store.refreshTasks.Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Zero(t, count, "read-only status queries created permanent task entries")
}

func TestStoreCacheRefreshWaiterGetsOwnRound(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	firstCh := make(chan StoreCacheRefreshResult, 1)
	waitCh := make(chan StoreCacheRefreshResult, 1)
	go func() { firstCh <- store.RefreshStoreCache(context.Background(), storeIDs[0]) }()
	<-started
	go func() { waitCh <- store.RefreshStoreCache(context.Background(), storeIDs[0]) }()
	time.Sleep(20 * time.Millisecond)
	close(release)
	first := <-firstCh
	cluster.ChangeLeader(regionID, peerIDs[1])
	second := store.RefreshStoreCache(context.Background(), storeIDs[0])
	waited := <-waitCh
	require.Equal(t, first.Failed, waited.Failed)
	require.Equal(t, first.Ready, waited.Ready)
	require.NotEqual(t, first.Ready, second.Ready)
}

func TestStoreCacheRefreshRetainsUnrelatedFailure(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	otherRegionID := cluster.AllocID()
	otherPeers := cluster.AllocIDs(3)
	cluster.Split(regionID, otherRegionID, []byte("m"), otherPeers, otherPeers[0])

	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	oldLoc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	previous := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Greater(t, previous.Failed, 0)
	require.False(t, previous.Ready)

	store.GetRegionCache().InvalidateCachedRegion(oldLoc.Region)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("z"))
	require.NoError(t, err)
	cluster.ChangeLeader(otherRegionID, otherPeers[1])
	result := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, result.Ready, "unresolved prior failure lost: %+v", result)
	require.Greater(t, result.Failed, 0)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.Greater(t, status.Failed, 0)
	require.False(t, status.Ready)
}

func TestStoreCacheRefreshCloseRefusesAndCancels(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	started := make(chan struct{})
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				select {
				case <-started:
				default:
					close(started)
				}
				time.Sleep(50 * time.Millisecond)
			},
		}
	}, nil, 0)
	require.NoError(t, err)

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	done := make(chan StoreCacheRefreshResult, 1)
	go func() {
		done <- store.RefreshStoreCache(context.Background(), storeIDs[0])
	}()
	<-started
	require.NoError(t, store.Close())
	res := <-done
	require.False(t, res.Ready)
	closed := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Equal(t, []string{"store is closed"}, closed.Errors)
}

func TestStoreCacheRefreshKeepsFailureAfterCacheTTL(t *testing.T) {
	SetRegionCacheTTLWithJitter(1, 0)
	t.Cleanup(func() { SetRegionCacheTTLWithJitter(600, 60) })

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	res := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, res.Ready)
	require.Greater(t, res.Failed, 0)

	time.Sleep(1200 * time.Millisecond)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.Greater(t, status.Failed, 0, "TTL expiry must not report refresh ready: %+v", status)
	require.False(t, status.Ready)
}

func TestStoreCacheRefreshRetriesAfterCacheTTLAndConverges(t *testing.T) {
	SetRegionCacheTTLWithJitter(1, 0)
	t.Cleanup(func() { SetRegionCacheTTLWithJitter(600, 60) })

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)
	first := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, first.Ready)
	require.Greater(t, first.Failed, 0)

	time.Sleep(1200 * time.Millisecond)
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.Greater(t, status.Failed, 0)
	require.False(t, status.Ready)

	store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	cluster.ChangeLeader(regionID, peerIDs[1])
	second := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.Equal(t, 0, second.Remaining, "errors=%v updated=%d failed=%d scanned=%d", second.Errors, second.Updated, second.Failed, second.Scanned)
	require.Equal(t, 0, second.Failed)
	require.True(t, second.Ready, "expired failure must be retried and converge: %+v", second)
}

func TestStoreCacheRefreshResetClearsFailure(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	res := store.RefreshStoreCache(context.Background(), storeIDs[0])
	require.False(t, res.Ready)
	require.Greater(t, res.Failed, 0)

	store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	require.NoError(t, store.ResetStoreCacheRefresh(storeIDs[0]))
	status := store.GetStoreCacheStatus(storeIDs[0])
	require.Equal(t, 0, status.Failed)
	require.True(t, status.Ready)
	n := 0
	store.refreshTasks.Range(func(_, _ any) bool {
		n++
		return true
	})
	require.Zero(t, n)
}

func TestStoreCacheRefreshResetBusyLeavesRunningJob(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	var inFlight atomic.Int64
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				inFlight.Add(1)
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
				inFlight.Add(-1)
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	done := make(chan StoreCacheRefreshResult, 1)
	go func() {
		done <- store.RefreshStoreCache(context.Background(), storeIDs[0])
	}()
	<-started
	errCh := make(chan error, 1)
	go func() {
		errCh <- store.ResetStoreCacheRefresh(storeIDs[0])
	}()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrStoreCacheRefreshBusy)
	case <-time.After(time.Second):
		t.Fatal("reset waited on the running refresh")
	}
	require.Equal(t, int64(1), inFlight.Load())
	close(release)
	res := <-done
	require.False(t, res.Ready)
	require.Greater(t, res.Failed, 0)
}

func TestStoreCacheRefreshCancelledWaiterDoesNotReset(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	store, err := NewTestTiKVStore(client, pdClient, func(c Client) Client {
		return &refreshInterceptClient{
			Client: c,
			onGet: func() {
				select {
				case <-started:
				default:
					close(started)
				}
				<-release
			},
		}
	}, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(cluster, 3)
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
	_, err = store.GetRegionCache().LocateKey(bo, []byte("a"))
	require.NoError(t, err)

	done := make(chan StoreCacheRefreshResult, 1)
	go func() {
		done <- store.RefreshStoreCache(context.Background(), storeIDs[0])
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan StoreCacheRefreshResult, 1)
	go func() {
		waitDone <- store.RefreshStoreCache(ctx, storeIDs[0])
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	waited := <-waitDone
	require.False(t, waited.Ready)
	require.Contains(t, waited.Errors, context.Canceled.Error())
	require.ErrorIs(t, store.ResetStoreCacheRefresh(storeIDs[0]), ErrStoreCacheRefreshBusy)
	close(release)
	<-done
}

type refreshInterceptClient struct {
	Client
	onGet func()
	onReq func(req *tikvrpc.Request)
}

func (c *refreshInterceptClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdGet && c.onReq != nil {
		c.onReq(req)
	}
	if req.Type == tikvrpc.CmdGet && c.onGet != nil {
		c.onGet()
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}
