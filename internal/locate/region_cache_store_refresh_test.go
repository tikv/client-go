// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package locate

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func BenchmarkWorkStoreRangeStatus(b *testing.B) {
	for _, count := range []int{100, 10000, 100000} {
		b.Run(fmt.Sprintf("regions=%d", count), func(b *testing.B) {
			c := &RegionCache{}
			c.mu.sorted = NewSortedRegions(btreeDegree)
			ttl := time.Now().Unix() + 3600
			rs := &regionStore{stores: []*Store{{storeID: 2}}}
			rs.accessIndex[tiKVOnly] = []int{0}
			for i := 0; i < count; i++ {
				r := &Region{meta: &metapb.Region{
					Id:       uint64(i + 1),
					StartKey: []byte(fmt.Sprintf("%08d", i)),
					EndKey:   []byte(fmt.Sprintf("%08d", i+1)),
					Peers:    []*metapb.Peer{{Id: uint64(i + 1), StoreId: 2}},
				}, ttl: ttl}
				r.setStore(rs)
				c.mu.sorted.ReplaceOrInsert(r)
			}
			start := []byte(fmt.Sprintf("%08d", count/2))
			end := []byte(fmt.Sprintf("%08d", count/2+1))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !c.IsWorkStoreRangeResolved(start, end, 1) {
					b.Fatal("range unresolved")
				}
			}
		})
	}
}

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

func TestWorkStoreRangeStatus(t *testing.T) {
	type span struct {
		start, end string
		onStore    bool
		expired    bool
	}
	tests := []struct {
		name                string
		spans               []span
		start, end, covered string
		complete, resolved  bool
	}{
		{name: "empty cache", start: "b", end: "y", covered: "b"},
		{name: "start in hole", spans: []span{{start: "m", end: "z"}}, start: "b", end: "y", covered: "b"},
		{name: "merged region", spans: []span{{start: "a", end: "z"}}, start: "b", end: "y", covered: "z", complete: true, resolved: true},
		{name: "split regions", spans: []span{{start: "a", end: "m"}, {start: "m", end: "z"}}, start: "b", end: "y", covered: "z", complete: true, resolved: true},
		{name: "split sibling on store", spans: []span{{start: "a", end: "m"}, {start: "m", end: "z", onStore: true}}, start: "b", end: "y", covered: "z", complete: true},
		{name: "on store outside range", spans: []span{{start: "a", end: "m"}, {start: "m", end: "z", onStore: true}}, start: "b", end: "m", covered: "m", complete: true, resolved: true},
		{name: "interior hole", spans: []span{{start: "a", end: "m"}, {start: "n", end: "z"}}, start: "b", end: "y", covered: "m"},
		{name: "expired first", spans: []span{{start: "a", end: "m", expired: true}, {start: "m", end: "z"}}, start: "b", end: "y", covered: "b"},
		{name: "expired sibling", spans: []span{{start: "a", end: "m"}, {start: "m", end: "z", expired: true}}, start: "b", end: "y", covered: "m"},
		{name: "missing tail", spans: []span{{start: "a", end: "m"}}, start: "b", covered: "m"},
		{name: "infinite tail", spans: []span{{start: "a", end: "m"}, {start: "m"}}, start: "b", complete: true, resolved: true},
		{name: "whole keyspace", spans: []span{{end: "m"}, {start: "m"}}, complete: true, resolved: true},
		{name: "empty finite range", spans: []span{{start: "a", end: "z"}}, start: "b", end: "b", covered: "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &RegionCache{}
			c.mu.sorted = NewSortedRegions(btreeDegree)
			var regions []*Region
			for i, sp := range tt.spans {
				storeID := uint64(2)
				if sp.onStore {
					storeID = 1
				}
				ttl := time.Now().Unix() + 3600
				if sp.expired {
					ttl = time.Now().Unix() - 1
				}
				r := &Region{meta: &metapb.Region{
					Id: uint64(i + 1), StartKey: []byte(sp.start), EndKey: []byte(sp.end),
					Peers: []*metapb.Peer{{Id: uint64(i + 1), StoreId: storeID}},
				}, ttl: ttl}
				rs := &regionStore{stores: []*Store{{storeID: storeID}}}
				rs.accessIndex[tiKVOnly] = []int{0}
				r.setStore(rs)
				c.mu.sorted.ReplaceOrInsert(r)
				regions = append(regions, r)
			}
			ttls := make([]int64, len(regions))
			for i, r := range regions {
				ttls[i] = r.ttl
			}
			covered, complete, resolved := c.WorkStoreRangeStatus([]byte(tt.start), []byte(tt.end), 1)
			require.Equal(t, tt.covered, string(covered))
			require.Equal(t, tt.complete, complete)
			require.Equal(t, tt.resolved, resolved)
			require.Equal(t, tt.resolved, c.IsWorkStoreRangeResolved([]byte(tt.start), []byte(tt.end), 1))
			for i, r := range regions {
				require.Equal(t, ttls[i], r.ttl, "range checks must not renew TTL")
			}
			if len(covered) > 0 {
				covered[0] = '!'
				again, _, _ := c.WorkStoreRangeStatus([]byte(tt.start), []byte(tt.end), 1)
				require.Equal(t, tt.covered, string(again), "returned boundary must not alias cached keys")
			}
		})
	}
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
