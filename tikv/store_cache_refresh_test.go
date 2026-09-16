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

type refreshInterceptClient struct {
	Client
	onGet func()
}

func (c *refreshInterceptClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdGet && c.onGet != nil {
		c.onGet()
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}
