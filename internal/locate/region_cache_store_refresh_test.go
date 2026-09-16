// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package locate

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestSwitchWorkLeaderToPeerIfOnStoreUpdatesGlobalEpoch(t *testing.T) {
	peers := []*metapb.Peer{{Id: 1, StoreId: 1}, {Id: 2, StoreId: 2}, {Id: 3, StoreId: 3}}
	r := &Region{meta: &metapb.Region{Id: 1, Peers: peers}}
	rs := &regionStore{
		stores: []*Store{
			{storeID: 1, epoch: 11, storeType: tikvrpc.TiKV},
			{storeID: 2, epoch: 22, storeType: tikvrpc.TiFlash},
			{storeID: 3, epoch: 33, storeType: tikvrpc.TiKV},
		},
		storeEpochs: []uint32{11, 22, 30},
		workTiKVIdx: 0,
	}
	rs.accessIndex[tiKVOnly] = []int{0, 2}
	rs.accessIndex[tiFlashOnly] = []int{1}
	r.setStore(rs)

	applied, moved := r.switchWorkLeaderToPeerIfOnStore(peers[2], 1)
	require.True(t, applied)
	require.False(t, moved)
	got := r.getStore()
	require.Equal(t, AccessIndex(1), got.workTiKVIdx)
	require.Equal(t, uint32(33), got.storeEpochs[2], "epochs=%v", got.storeEpochs)
	require.Equal(t, uint32(22), got.storeEpochs[1])
}
