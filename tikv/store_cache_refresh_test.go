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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
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
