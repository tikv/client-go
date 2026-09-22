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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func BenchmarkCountWorkStoreMatches(b *testing.B) {
	b.ReportAllocs()
	c := &RegionCache{}
	c.mu.regions = make(map[RegionVerID]*Region, 10000)
	now := time.Now().Unix() + 3600
	for i := 0; i < 10000; i++ {
		st := &Store{storeID: 1, addr: "127.0.0.1:20160"}
		rs := &regionStore{
			stores:      []*Store{st},
			storeEpochs: []uint32{1},
			workTiKVIdx: 0,
		}
		rs.accessIndex[tiKVOnly] = []int{0}
		r := &Region{
			meta: &metapb.Region{
				Id:          uint64(i + 1),
				StartKey:    []byte{byte(i >> 8), byte(i)},
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
				Peers:       []*metapb.Peer{{Id: uint64(i + 1), StoreId: 1}},
			},
			ttl: now,
		}
		r.setStore(rs)
		c.mu.regions[r.VerID()] = r
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := c.CountWorkStoreMatches(1); n != 10000 {
			b.Fatalf("count=%d", n)
		}
	}
}

func TestClassifyWorkStoreNilStoreIsGone(t *testing.T) {
	c := &RegionCache{}
	c.mu.regions = make(map[RegionVerID]*Region)
	r := &Region{
		meta: &metapb.Region{
			Id:          1,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		},
		ttl: time.Now().Unix() + 3600,
	}
	c.mu.regions[r.VerID()] = r
	require.Equal(t, WorkStoreGone, c.ClassifyWorkStore(r.VerID(), 1))
	require.NotPanics(t, func() { _ = r.GetLeaderStoreID() })
}

func TestClassifyWorkStoreConcurrentInvalidate(t *testing.T) {
	c := &RegionCache{}
	c.mu.regions = make(map[RegionVerID]*Region)
	st := &Store{storeID: 1, addr: "127.0.0.1:20160"}
	rs := &regionStore{
		stores:      []*Store{st},
		storeEpochs: []uint32{1},
		workTiKVIdx: 0,
	}
	rs.accessIndex[tiKVOnly] = []int{0}
	r := &Region{
		meta: &metapb.Region{
			Id:          1,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		},
		ttl: time.Now().Unix() + 3600,
	}
	r.setStore(rs)
	c.mu.regions[r.VerID()] = r
	id := r.VerID()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			_ = c.ClassifyWorkStore(id, 1)
			_ = r.GetLeaderStoreID()
		}
	}()
	for i := 0; i < 1000; i++ {
		r.setStore(nil)
		r.setStore(rs)
	}
	<-done
}

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
