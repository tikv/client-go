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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/testutils"
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
	require.Equal(t, 0, res.Remaining, "errors=%v updated=%d matched=%d", res.Errors, res.Updated, res.Matched)
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
