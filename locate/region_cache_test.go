// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_cache_test.go
//

// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package locate

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/retry"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
)

func TestRegionCache(t *testing.T) {
	suite.Run(t, new(testRegionCacheSuite))
}

type testRegionCacheSuite struct {
	suite.Suite
	mvccStore mocktikv.MVCCStore
	cluster   *mocktikv.Cluster
	store1    uint64 // store1 is leader
	store2    uint64 // store2 is follower
	peer1     uint64 // peer1 is leader
	peer2     uint64 // peer2 is follower
	region1   uint64
	cache     *RegionCache
	bo        *retry.Backoffer
}

func (s *testRegionCacheSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.peer1 = peerIDs[0]
	s.peer2 = peerIDs[1]
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRegionCacheSuite) TearDownTest() {
	s.cache.Close()
	s.mvccStore.Close()
}

func (s *testRegionCacheSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRegionCacheSuite) checkCache(len int) {
	ts := time.Now().Unix()
	s.Equal(validRegions(s.cache.mu.regions, ts), len)
	s.Equal(validRegionsSearchedByVersions(s.cache.mu.latestVersions, s.cache.mu.regions, ts), len)
	s.Equal(validRegionsInBtree(s.cache.mu.sorted, ts), len)
}

func validRegionsSearchedByVersions(
	versions map[uint64]RegionVerID,
	regions map[RegionVerID]*Region,
	ts int64,
) (count int) {
	for _, ver := range versions {
		region, ok := regions[ver]
		if !ok || !region.checkRegionCacheTTL(ts) {
			continue
		}
		count++
	}
	return
}

func validRegions(regions map[RegionVerID]*Region, ts int64) (len int) {
	for _, region := range regions {
		if !region.checkRegionCacheTTL(ts) {
			continue
		}
		len++
	}
	return
}

func validRegionsInBtree(t *btree.BTree, ts int64) (len int) {
	t.Descend(func(item btree.Item) bool {
		r := item.(*btreeItem).cachedRegion
		if !r.checkRegionCacheTTL(ts) {
			return true
		}
		len++
		return true
	})
	return
}

func (s *testRegionCacheSuite) getRegion(key []byte) *Region {
	_, err := s.cache.LocateKey(s.bo, key)
	s.Nil(err)
	r := s.cache.searchCachedRegion(key, false)
	s.NotNil(r)
	return r
}

func (s *testRegionCacheSuite) getRegionWithEndKey(key []byte) *Region {
	_, err := s.cache.LocateEndKey(s.bo, key)
	s.Nil(err)
	r := s.cache.searchCachedRegion(key, true)
	s.NotNil(r)
	return r
}

func (s *testRegionCacheSuite) getAddr(key []byte, replicaRead kv.ReplicaReadType, seed uint32) string {
	loc, err := s.cache.LocateKey(s.bo, key)
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, replicaRead, seed)
	s.Nil(err)
	if ctx == nil {
		return ""
	}
	return ctx.Addr
}

func (s *testRegionCacheSuite) TestStoreLabels() {
	testcases := []struct {
		storeID uint64
	}{
		{
			storeID: s.store1,
		},
		{
			storeID: s.store2,
		},
	}
	for _, testcase := range testcases {
		s.T().Log(testcase.storeID)
		store := s.cache.getStoreByStoreID(testcase.storeID)
		_, err := store.initResolve(s.bo, s.cache)
		s.Nil(err)
		labels := []*metapb.StoreLabel{
			{
				Key:   "id",
				Value: fmt.Sprintf("%v", testcase.storeID),
			},
		}
		stores := s.cache.getStoresByLabels(labels)
		s.Equal(len(stores), 1)
		s.Equal(stores[0].labels, labels)
	}
}

func (s *testRegionCacheSuite) TestSimple() {
	seed := rand.Uint32()
	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))
	s.checkCache(1)
	s.Equal(r.GetMeta(), r.meta)
	s.Equal(r.GetLeaderPeerID(), r.meta.Peers[r.getStore().workTiKVIdx].Id)
	s.cache.mu.regions[r.VerID()].lastAccess = 0
	r = s.cache.searchCachedRegion([]byte("a"), true)
	s.Nil(r)
}

// TestResolveStateTransition verifies store's resolve state transition. For example,
// a newly added store is in unresolved state and will be resolved soon if it's an up store,
// or in tombstone state if it's a tombstone.
func (s *testRegionCacheSuite) TestResolveStateTransition() {
	cache := s.cache
	bo := retry.NewNoopBackoff(context.Background())

	// Check resolving normal stores. The resolve state should be resolved.
	for _, storeMeta := range s.cluster.GetAllStores() {
		store := cache.getStoreByStoreID(storeMeta.GetId())
		s.Equal(store.getResolveState(), unresolved)
		addr, err := store.initResolve(bo, cache)
		s.Nil(err)
		s.Equal(addr, storeMeta.GetAddress())
		s.Equal(store.getResolveState(), resolved)
	}

	waitResolve := func(s *Store) {
		for i := 0; i < 10; i++ {
			if s.getResolveState() != needCheck {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Mark the store needCheck. The resolve state should be resolved soon.
	store := cache.getStoreByStoreID(s.store1)
	store.markNeedCheck(cache.notifyCheckCh)
	waitResolve(store)
	s.Equal(store.getResolveState(), resolved)

	// Mark the store needCheck and it becomes a tombstone. The resolve state should be tombstone.
	s.cluster.MarkTombstone(s.store1)
	store.markNeedCheck(cache.notifyCheckCh)
	waitResolve(store)
	s.Equal(store.getResolveState(), tombstone)
	s.cluster.StartStore(s.store1)

	// Mark the store needCheck and it's deleted from PD. The resolve state should be tombstone.
	cache.clear()
	store = cache.getStoreByStoreID(s.store1)
	store.initResolve(bo, cache)
	s.Equal(store.getResolveState(), resolved)
	storeMeta := s.cluster.GetStore(s.store1)
	s.cluster.RemoveStore(s.store1)
	store.markNeedCheck(cache.notifyCheckCh)
	waitResolve(store)
	s.Equal(store.getResolveState(), tombstone)
	s.cluster.AddStore(storeMeta.GetId(), storeMeta.GetAddress(), storeMeta.GetLabels()...)

	// Mark the store needCheck and its address and labels are changed.
	// The resolve state should be deleted and a new store is added to the cache.
	cache.clear()
	store = cache.getStoreByStoreID(s.store1)
	store.initResolve(bo, cache)
	s.Equal(store.getResolveState(), resolved)
	s.cluster.UpdateStoreAddr(s.store1, store.addr+"0", &metapb.StoreLabel{Key: "k", Value: "v"})
	store.markNeedCheck(cache.notifyCheckCh)
	waitResolve(store)
	s.Equal(store.getResolveState(), deleted)
	newStore := cache.getStoreByStoreID(s.store1)
	s.Equal(newStore.getResolveState(), resolved)
	s.Equal(newStore.addr, store.addr+"0")
	s.Equal(newStore.labels, []*metapb.StoreLabel{{Key: "k", Value: "v"}})

	// Check initResolve()ing a tombstone store. The resolve state should be tombstone.
	cache.clear()
	s.cluster.MarkTombstone(s.store1)
	store = cache.getStoreByStoreID(s.store1)
	for i := 0; i < 2; i++ {
		addr, err := store.initResolve(bo, cache)
		s.Nil(err)
		s.Equal(addr, "")
		s.Equal(store.getResolveState(), tombstone)
	}
	s.cluster.StartStore(s.store1)
	cache.clear()

	// Check initResolve()ing a dropped store. The resolve state should be tombstone.
	cache.clear()
	storeMeta = s.cluster.GetStore(s.store1)
	s.cluster.RemoveStore(s.store1)
	store = cache.getStoreByStoreID(s.store1)
	for i := 0; i < 2; i++ {
		addr, err := store.initResolve(bo, cache)
		s.Nil(err)
		s.Equal(addr, "")
		s.Equal(store.getResolveState(), tombstone)
	}
	s.cluster.AddStore(storeMeta.GetId(), storeMeta.GetAddress(), storeMeta.GetLabels()...)
}

// TestFilterDownPeersOrPeersOnTombstoneOrDroppedStore verifies the RegionCache filter
// region's down peers and peers on tombstone or dropped stores. RegionCache shouldn't
// report errors in such cases if there are available peers.
func (s *testRegionCacheSuite) TestFilterDownPeersOrPeersOnTombstoneOrDroppedStores() {
	key := []byte("a")
	bo := retry.NewBackofferWithVars(context.Background(), 100, nil)

	verifyGetRPCCtx := func(meta *metapb.Region) {
		loc, err := s.cache.LocateKey(bo, key)
		s.NotNil(loc)
		s.Nil(err)
		ctx, err := s.cache.GetTiKVRPCContext(bo, loc.Region, kv.ReplicaReadLeader, 0)
		s.Nil(err)
		s.NotNil(ctx)
		s.Equal(ctx.Meta, meta)
		ctx, err = s.cache.GetTiKVRPCContext(bo, loc.Region, kv.ReplicaReadFollower, rand.Uint32())
		s.Nil(err)
		s.NotNil(ctx)
		s.Equal(ctx.Meta, meta)
	}

	// When all peers are normal, the cached region should contain all peers.
	reg, err := s.cache.findRegionByKey(bo, key, false)
	s.NotNil(reg)
	s.Nil(err)
	regInPD, _ := s.cluster.GetRegion(reg.GetID())
	s.Equal(reg.meta, regInPD)
	s.Equal(len(reg.meta.GetPeers()), len(reg.getStore().stores))
	verifyGetRPCCtx(reg.meta)
	s.checkCache(1)
	s.cache.clear()

	// Shouldn't contain the peer on the tombstone store.
	s.cluster.MarkTombstone(s.store1)
	reg, err = s.cache.findRegionByKey(bo, key, false)
	s.NotNil(reg)
	s.Nil(err)
	s.Equal(len(reg.meta.GetPeers()), len(regInPD.GetPeers())-1)
	s.Equal(len(reg.meta.GetPeers()), len(reg.getStore().stores))
	for _, peer := range reg.meta.GetPeers() {
		s.NotEqual(peer.GetStoreId(), s.store1)
	}
	for _, store := range reg.getStore().stores {
		s.NotEqual(store.storeID, s.store1)
	}
	verifyGetRPCCtx(reg.meta)
	s.checkCache(1)
	s.cache.clear()
	s.cluster.StartStore(s.store1)

	// Shouldn't contain the peer on the dropped store.
	store := s.cluster.GetStore(s.store1)
	s.cluster.RemoveStore(s.store1)
	reg, err = s.cache.findRegionByKey(bo, key, false)
	s.NotNil(reg)
	s.Nil(err)
	s.Equal(len(reg.meta.GetPeers()), len(regInPD.GetPeers())-1)
	s.Equal(len(reg.meta.GetPeers()), len(reg.getStore().stores))
	for _, peer := range reg.meta.GetPeers() {
		s.NotEqual(peer.GetStoreId(), s.store1)
	}
	for _, store := range reg.getStore().stores {
		s.NotEqual(store.storeID, s.store1)
	}
	verifyGetRPCCtx(reg.meta)
	s.checkCache(1)
	s.cache.clear()
	s.cluster.AddStore(store.GetId(), store.GetAddress(), store.GetLabels()...)

	// Report an error when there's no available peers.
	s.cluster.MarkTombstone(s.store1)
	s.cluster.MarkTombstone(s.store2)
	_, err = s.cache.findRegionByKey(bo, key, false)
	s.NotNil(err)
	s.Regexp(".*no available peers.", err.Error())
	s.cluster.StartStore(s.store1)
	s.cluster.StartStore(s.store2)
}

func (s *testRegionCacheSuite) TestUpdateLeader() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store2))
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store1))

	r = s.getRegionWithEndKey([]byte("z"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("z"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store2))
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store1))
}

func (s *testRegionCacheSuite) TestUpdateLeader2() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// new store3 becomes leader
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: peer3, StoreId: store3}, 0)

	// Store3 does not exist in cache, causes a reload from PD.
	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	follower := s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed)
	if seed%2 == 0 {
		s.Equal(follower, s.storeAddr(s.store2))
	} else {
		s.Equal(follower, s.storeAddr(store3))
	}
	follower2 := s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed+1)
	if (seed+1)%2 == 0 {
		s.Equal(follower2, s.storeAddr(s.store2))
	} else {
		s.Equal(follower2, s.storeAddr(store3))
	}
	s.NotEqual(follower, follower2)

	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader` again.
	s.cache.UpdateLeader(r.VerID(), &metapb.Peer{Id: peer3, StoreId: store3}, 0)
	r = s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(store3))
	follower = s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed)
	if seed%2 == 0 {
		s.Equal(follower, s.storeAddr(s.store1))
	} else {
		s.Equal(follower, s.storeAddr(s.store2))
	}
	follower2 = s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed+1)
	if (seed+1)%2 == 0 {
		s.Equal(follower2, s.storeAddr(s.store1))
	} else {
		s.Equal(follower2, s.storeAddr(s.store2))
	}
	s.NotEqual(follower, follower2)
}

func (s *testRegionCacheSuite) TestUpdateLeader3() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// store2 becomes leader
	s.cluster.ChangeLeader(s.region1, s.peer2)
	// store2 gone, store3 becomes leader
	s.cluster.RemoveStore(s.store2)
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader`(store2 is the leader)
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	// Store2 does not exist any more, causes a reload from PD.
	r := s.getRegion([]byte("a"))
	s.Nil(err)
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	// return resolved store2 address and send fail
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, seed)
	s.Nil(err)
	s.Equal(ctx.Addr, "store2")
	s.cache.OnSendFail(retry.NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
	s.cache.checkAndResolve(nil, func(*Store) bool { return true })
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)
	addr := s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0)
	s.Equal(addr, "")
	addr = s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0)
	s.Equal(addr, s.storeAddr(store3))

	addr = s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed)
	addr2 := s.getAddr([]byte("a"), kv.ReplicaReadFollower, seed+1)
	s.NotEqual(addr, s.storeAddr(store3))
	s.NotEqual(addr2, s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestSendFailedButLeaderNotChange() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// access 1 it will return NotLeader, leader back to 2 again
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestSendFailedInHibernateRegion() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	s.True(ctxFollower1.Peer.Id == s.peer1 || ctxFollower1.Peer.Id == peer3)
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// access 2, it's in hibernate and return 0 leader, so switch to 3
	s.cache.UpdateLeader(loc.Region, nil, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, peer3)

	// verify follower to be one of store1 and store2
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// again peer back to 1
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.cache.UpdateLeader(loc.Region, nil, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)

	// verify follower to be one of store2 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestSendFailInvalidateRegionsInSameStore() {
	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)
	loc2, err := s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc2.Region.id, region2)

	// Send fail on region1
	ctx, _ := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.checkCache(2)
	s.cache.OnSendFail(s.bo, ctx, false, errors.New("test error"))

	// Get region2 cache will get nil then reload.
	ctx2, err := s.cache.GetTiKVRPCContext(s.bo, loc2.Region, kv.ReplicaReadLeader, 0)
	s.Nil(ctx2)
	s.Nil(err)
}

func (s *testRegionCacheSuite) TestSendFailEnableForwarding() {
	s.cache.enableForwarding = true

	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	var storeState uint32 = uint32(unreachable)
	s.cache.testingKnobs.mockRequestLiveness = func(s *Store, bo *retry.Backoffer) livenessState {
		return livenessState(atomic.LoadUint32(&storeState))
	}

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)

	// Invoke OnSendFail so that the store will be marked as needForwarding
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.NotNil(ctx)
	s.cache.OnSendFail(s.bo, ctx, false, errors.New("test error"))

	// ...then on next retry, proxy will be used
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.NotNil(ctx)
	s.NotNil(ctx.ProxyStore)
	s.Equal(ctx.ProxyStore.storeID, s.store2)

	// Proxy will be also applied to other regions whose leader is on the store
	loc2, err := s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc2.Region.id, region2)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc2.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.NotNil(ctx)
	s.NotNil(ctx.ProxyStore)
	s.Equal(ctx.ProxyStore.storeID, s.store2)

	// Recover the store
	atomic.StoreUint32(&storeState, uint32(reachable))
	// The proxy should be unset after several retries
	for retry := 0; retry < 15; retry++ {
		ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
		s.Nil(err)
		if ctx.ProxyStore == nil {
			break
		}
		time.Sleep(time.Millisecond * 200)
	}
	s.Nil(ctx.ProxyStore)
}

func (s *testRegionCacheSuite) TestSendFailedInMultipleNode() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send 2 fail leader switch to 3
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, peer3)

	// verify follower to be one of store1 and store2
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	}
	s.True(ctxFollower1.Peer.Id == s.peer1 || ctxFollower1.Peer.Id == s.peer2)
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer1)
	} else {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	}
	s.Equal(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// 3 can be access, so switch to 1
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer1, StoreId: s.store1}, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)

	// verify follower to be one of store2 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	s.Nil(err)
	if seed%2 == 0 {
		s.Equal(ctxFollower1.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower1.Peer.Id, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	s.Nil(err)
	if (seed+1)%2 == 0 {
		s.Equal(ctxFollower2.Peer.Id, s.peer2)
	} else {
		s.Equal(ctxFollower2.Peer.Id, peer3)
	}
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestLabelSelectorTiKVPeer() {
	dc1Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-1",
		},
	}
	dc2Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-2",
		},
	}
	dc3Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-3",
		},
	}
	s.cluster.UpdateStoreLabels(s.store1, dc1Label)
	s.cluster.UpdateStoreLabels(s.store2, dc2Label)

	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.UpdateStoreLabels(store3, dc1Label)
	// Region have 3 peer, leader located in dc-1, followers located in dc-1, dc-2
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	seed := rand.Uint32()

	testcases := []struct {
		name               string
		t                  kv.ReplicaReadType
		labels             []*metapb.StoreLabel
		expectStoreIDRange map[uint64]struct{}
	}{
		{
			name:   "any Peer,located in dc-1",
			t:      kv.ReplicaReadMixed,
			labels: dc1Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
				store3:   {},
			},
		},
		{
			name:   "any Peer,located in dc-2",
			t:      kv.ReplicaReadMixed,
			labels: dc2Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store2: {},
			},
		},
		{
			name:   "only follower,located in dc-1",
			t:      kv.ReplicaReadFollower,
			labels: dc1Label,
			expectStoreIDRange: map[uint64]struct{}{
				store3: {},
			},
		},
		{
			name:   "only leader, shouldn't consider labels",
			t:      kv.ReplicaReadLeader,
			labels: dc2Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
			},
		},
		{
			name:   "no label matching, fallback to leader",
			t:      kv.ReplicaReadMixed,
			labels: dc3Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
			},
		},
	}

	for _, testcase := range testcases {
		s.T().Log(testcase.name)
		ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, testcase.t, seed, WithMatchLabels(testcase.labels))
		s.Nil(err)
		_, exist := testcase.expectStoreIDRange[ctx.Store.storeID]
		s.Equal(exist, true)
	}
}

func (s *testRegionCacheSuite) TestSplit() {
	seed := rand.Uint32()
	r := s.getRegion([]byte("x"))
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))

	// split to ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// tikv-server reports `NotInRegion`
	s.cache.InvalidateCachedRegion(r.VerID())
	s.checkCache(0)

	r = s.getRegion([]byte("x"))
	s.Equal(r.GetID(), region2)
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))
	s.checkCache(1)

	r = s.getRegionWithEndKey([]byte("m"))
	s.Equal(r.GetID(), s.region1)
	s.checkCache(2)
}

func (s *testRegionCacheSuite) TestMerge() {
	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	loc, err := s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc.Region.id, region2)

	// merge to single region
	s.cluster.Merge(s.region1, region2)

	// tikv-server reports `NotInRegion`
	s.cache.InvalidateCachedRegion(loc.Region)
	s.checkCache(0)

	loc, err = s.cache.LocateKey(s.bo, []byte("x"))
	s.Nil(err)
	s.Equal(loc.Region.id, s.region1)
	s.checkCache(1)
}

func (s *testRegionCacheSuite) TestReconnect() {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)

	// connect tikv-server failed, cause drop cache
	s.cache.InvalidateCachedRegion(loc.Region)

	r := s.getRegion([]byte("a"))
	s.NotNil(r)
	s.Equal(r.GetID(), s.region1)
	s.Equal(s.getAddr([]byte("a"), kv.ReplicaReadLeader, 0), s.storeAddr(s.store1))
	s.Equal(s.getAddr([]byte("x"), kv.ReplicaReadFollower, seed), s.storeAddr(s.store2))
	s.checkCache(1)
}

func (s *testRegionCacheSuite) TestRegionEpochAheadOfTiKV() {
	// Create a separated region cache to do this test.
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	cache := NewRegionCache(pdCli)
	defer cache.Close()

	region := createSampleRegion([]byte("k1"), []byte("k2"))
	region.meta.Id = 1
	region.meta.RegionEpoch = &metapb.RegionEpoch{Version: 10, ConfVer: 10}
	cache.insertRegionToCache(region)

	r1 := metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 9, ConfVer: 10}}
	r2 := metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 10, ConfVer: 9}}

	bo := retry.NewBackofferWithVars(context.Background(), 2000000, nil)

	_, err := cache.OnRegionEpochNotMatch(bo, &RPCContext{Region: region.VerID()}, []*metapb.Region{&r1})
	s.Nil(err)
	_, err = cache.OnRegionEpochNotMatch(bo, &RPCContext{Region: region.VerID()}, []*metapb.Region{&r2})
	s.Nil(err)
	s.Equal(bo.ErrorsNum(), 2)
}

func (s *testRegionCacheSuite) TestRegionEpochOnTiFlash() {
	// add store3 as tiflash
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store1), &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, peer3)

	// pre-load region cache
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)
	lctx, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(lctx.Peer.Id, peer3)

	// epoch-not-match on tiflash
	ctxTiFlash, err := s.cache.GetTiFlashRPCContext(s.bo, loc1.Region, true)
	s.Nil(err)
	s.Equal(ctxTiFlash.Peer.Id, s.peer1)
	ctxTiFlash.Peer.Role = metapb.PeerRole_Learner
	r := ctxTiFlash.Meta
	reqSend := NewRegionRequestSender(s.cache, nil)
	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{r}}}
	reqSend.onRegionError(s.bo, ctxTiFlash, nil, regionErr, nil)

	// check leader read should not go to tiflash
	lctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.NotEqual(lctx.Peer.Id, s.peer1)
}

func (s *testRegionCacheSuite) TestRegionDataNotReady() {
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)
	testcases := []struct {
		scope         string
		readType      kv.ReplicaReadType
		expectPeerID  uint64
		expectOptsLen int
		expectSeed    uint32
	}{
		{
			scope:         oracle.GlobalTxnScope,
			readType:      kv.ReplicaReadFollower,
			expectPeerID:  s.peer2,
			expectOptsLen: 1,
			expectSeed:    1,
		},
		{
			scope:         "local",
			readType:      kv.ReplicaReadFollower,
			expectPeerID:  s.peer2,
			expectOptsLen: 0,
			expectSeed:    1,
		},
	}

	for _, testcase := range testcases {
		fctx, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, testcase.readType, 0)
		s.Nil(err)
		s.Equal(fctx.Peer.Id, testcase.expectPeerID)
		reqSend := NewRegionRequestSender(s.cache, nil)
		regionErr := &errorpb.Error{DataIsNotReady: &errorpb.DataIsNotReady{}}
		var opts []StoreSelectorOption
		seed := uint32(0)
		s.bo.Reset()
		req := &tikvrpc.Request{TxnScope: testcase.scope, Context: kvrpcpb.Context{StaleRead: true}, ReplicaReadSeed: &seed}
		retry, err := reqSend.onRegionError(s.bo, fctx, req, regionErr, &opts)
		s.Nil(err)
		s.True(retry)
		s.Equal(len(opts), testcase.expectOptsLen)
		s.Equal(*req.GetReplicaReadSeed(), testcase.expectSeed)
	}
}

const regionSplitKeyFormat = "t%08d"

func createClusterWithStoresAndRegions(regionCnt, storeCount int) *mocktikv.Cluster {
	cluster := mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	_, _, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, storeCount)
	for i := 0; i < regionCnt; i++ {
		rawKey := []byte(fmt.Sprintf(regionSplitKeyFormat, i))
		ids := cluster.AllocIDs(4)
		// Make leaders equally distributed on the 3 stores.
		storeID := ids[0]
		peerIDs := ids[1:]
		leaderPeerID := peerIDs[i%3]
		cluster.SplitRaw(regionID, storeID, rawKey, peerIDs, leaderPeerID)
		regionID = ids[0]
	}
	return cluster
}

func loadRegionsToCache(cache *RegionCache, regionCnt int) {
	for i := 0; i < regionCnt; i++ {
		rawKey := []byte(fmt.Sprintf(regionSplitKeyFormat, i))
		cache.LocateKey(retry.NewBackofferWithVars(context.Background(), 1, nil), rawKey)
	}
}

func (s *testRegionCacheSuite) TestListRegionIDsInCache() {
	// ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	regionIDs, err := s.cache.ListRegionIDsInKeyRange(s.bo, []byte("a"), []byte("z"))
	s.Nil(err)
	s.Equal(regionIDs, []uint64{s.region1, region2})
	regionIDs, err = s.cache.ListRegionIDsInKeyRange(s.bo, []byte("m"), []byte("z"))
	s.Nil(err)
	s.Equal(regionIDs, []uint64{region2})

	regionIDs, err = s.cache.ListRegionIDsInKeyRange(s.bo, []byte("a"), []byte("m"))
	s.Nil(err)
	s.Equal(regionIDs, []uint64{s.region1, region2})
}

func (s *testRegionCacheSuite) TestScanRegions() {
	// Split at "a", "b", "c", "d"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'a' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	scannedRegions, err := s.cache.scanRegions(s.bo, []byte(""), nil, 100)
	s.Nil(err)
	s.Equal(len(scannedRegions), 5)
	for i := 0; i < 5; i++ {
		r := scannedRegions[i]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		s.Equal(r.meta.Id, regions[i])
		s.Equal(p.Id, peers[i][0])
	}

	scannedRegions, err = s.cache.scanRegions(s.bo, []byte("a"), nil, 3)
	s.Nil(err)
	s.Equal(len(scannedRegions), 3)
	for i := 1; i < 4; i++ {
		r := scannedRegions[i-1]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		s.Equal(r.meta.Id, regions[i])
		s.Equal(p.Id, peers[i][0])
	}

	scannedRegions, err = s.cache.scanRegions(s.bo, []byte("a1"), nil, 1)
	s.Nil(err)
	s.Equal(len(scannedRegions), 1)

	r0 := scannedRegions[0]
	_, p0, _, _ := r0.WorkStorePeer(r0.getStore())
	s.Equal(r0.meta.Id, regions[1])
	s.Equal(p0.Id, peers[1][0])

	// Test region with no leader
	s.cluster.GiveUpLeader(regions[1])
	s.cluster.GiveUpLeader(regions[3])
	scannedRegions, err = s.cache.scanRegions(s.bo, []byte(""), nil, 5)
	s.Nil(err)
	for i := 0; i < 3; i++ {
		r := scannedRegions[i]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		s.Equal(r.meta.Id, regions[i*2])
		s.Equal(p.Id, peers[i*2][0])
	}
}

func (s *testRegionCacheSuite) TestBatchLoadRegions() {
	// Split at "a", "b", "c", "d"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'a' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	testCases := []struct {
		startKey      []byte
		endKey        []byte
		limit         int
		expectKey     []byte
		expectRegions []uint64
	}{
		{[]byte(""), []byte("a"), 1, []byte("a"), []uint64{regions[0]}},
		{[]byte("a"), []byte("b1"), 2, []byte("c"), []uint64{regions[1], regions[2]}},
		{[]byte("a1"), []byte("d"), 2, []byte("c"), []uint64{regions[1], regions[2]}},
		{[]byte("c"), []byte("c1"), 2, nil, []uint64{regions[3]}},
		{[]byte("d"), nil, 2, nil, []uint64{regions[4]}},
	}

	for _, tc := range testCases {
		key, err := s.cache.BatchLoadRegionsFromKey(s.bo, tc.startKey, tc.limit)
		s.Nil(err)
		if tc.expectKey != nil {
			s.Equal(key, tc.expectKey)
		} else {
			s.Len(key, 0)
		}
		loadRegions, err := s.cache.BatchLoadRegionsWithKeyRange(s.bo, tc.startKey, tc.endKey, tc.limit)
		s.Nil(err)
		s.Len(loadRegions, len(tc.expectRegions))
		for i := range loadRegions {
			s.Equal(loadRegions[i].GetID(), tc.expectRegions[i])
		}
	}

	s.checkCache(len(regions))
}

func (s *testRegionCacheSuite) TestFollowerReadFallback() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be store2 and store3
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	s.Nil(err)
	s.Equal(ctxFollower1.Peer.Id, s.peer2)
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 1)
	s.Nil(err)
	s.Equal(ctxFollower2.Peer.Id, peer3)
	s.NotEqual(ctxFollower1.Peer.Id, ctxFollower2.Peer.Id)

	// send fail on store2, next follower read is going to fallback to store3
	s.cache.OnSendFail(s.bo, ctxFollower1, false, errors.New("test error"))
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, peer3)
}

func (s *testRegionCacheSuite) TestMixedReadFallback() {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer1)
	s.Equal(len(ctx.Meta.Peers), 3)

	// verify follower to be store1, store2 and store3
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 0)
	s.Nil(err)
	s.Equal(ctxFollower1.Peer.Id, s.peer1)

	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 1)
	s.Nil(err)
	s.Equal(ctxFollower2.Peer.Id, s.peer2)

	ctxFollower3, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 2)
	s.Nil(err)
	s.Equal(ctxFollower3.Peer.Id, peer3)

	// send fail on store2, next follower read is going to fallback to store3
	s.cache.OnSendFail(s.bo, ctxFollower1, false, errors.New("test error"))
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.Id, s.peer2)
}

func (s *testRegionCacheSuite) TestFollowerMeetEpochNotMatch() {
	// 3 nodes and no.1 is region1 leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)

	reqSend := NewRegionRequestSender(s.cache, nil)

	// follower read failed on store2
	followReqSeed := uint32(0)
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadFollower, followReqSeed)
	s.Nil(err)
	s.Equal(ctxFollower1.Peer.Id, s.peer2)
	s.Equal(ctxFollower1.Store.storeID, s.store2)

	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	reqSend.onRegionError(s.bo, ctxFollower1, &tikvrpc.Request{ReplicaReadSeed: &followReqSeed}, regionErr, nil)
	s.Equal(followReqSeed, uint32(1))

	regionErr = &errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}
	reqSend.onRegionError(s.bo, ctxFollower1, &tikvrpc.Request{ReplicaReadSeed: &followReqSeed}, regionErr, nil)
	s.Equal(followReqSeed, uint32(2))
}

func (s *testRegionCacheSuite) TestMixedMeetEpochNotMatch() {
	// 3 nodes and no.1 is region1 leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.Equal(loc1.Region.id, s.region1)

	reqSend := NewRegionRequestSender(s.cache, nil)

	// follower read failed on store1
	followReqSeed := uint32(0)
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadMixed, followReqSeed)
	s.Nil(err)
	s.Equal(ctxFollower1.Peer.Id, s.peer1)
	s.Equal(ctxFollower1.Store.storeID, s.store1)

	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	reqSend.onRegionError(s.bo, ctxFollower1, &tikvrpc.Request{ReplicaReadSeed: &followReqSeed}, regionErr, nil)
	s.Equal(followReqSeed, uint32(1))
}

func (s *testRegionCacheSuite) TestPeersLenChange() {
	// 2 peers [peer1, peer2] and let peer2 become leader
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.cache.UpdateLeader(loc.Region, &metapb.Peer{Id: s.peer2, StoreId: s.store2}, 0)

	// current leader is peer2 in [peer1, peer2]
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	s.Nil(err)
	s.Equal(ctx.Peer.StoreId, s.store2)

	// simulate peer1 became down in kv heartbeat and loaded before response back.
	cpMeta := &metapb.Region{
		Id:          ctx.Meta.Id,
		StartKey:    ctx.Meta.StartKey,
		EndKey:      ctx.Meta.EndKey,
		RegionEpoch: ctx.Meta.RegionEpoch,
		Peers:       make([]*metapb.Peer, len(ctx.Meta.Peers)),
	}
	copy(cpMeta.Peers, ctx.Meta.Peers)
	cpRegion := &pd.Region{
		Meta:      cpMeta,
		DownPeers: []*metapb.Peer{{Id: s.peer1, StoreId: s.store1}},
	}
	filterUnavailablePeers(cpRegion)
	region := &Region{meta: cpRegion.Meta}
	err = region.init(s.bo, s.cache)
	s.Nil(err)
	s.cache.insertRegionToCache(region)

	// OnSendFail should not panic
	s.cache.OnSendFail(retry.NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
}

func createSampleRegion(startKey, endKey []byte) *Region {
	return &Region{
		meta: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	}
}

func (s *testRegionCacheSuite) TestContains() {
	s.True(createSampleRegion(nil, nil).Contains([]byte{}))
	s.True(createSampleRegion(nil, nil).Contains([]byte{10}))
	s.False(createSampleRegion([]byte{10}, nil).Contains([]byte{}))
	s.False(createSampleRegion([]byte{10}, nil).Contains([]byte{9}))
	s.True(createSampleRegion([]byte{10}, nil).Contains([]byte{10}))
	s.True(createSampleRegion(nil, []byte{10}).Contains([]byte{}))
	s.True(createSampleRegion(nil, []byte{10}).Contains([]byte{9}))
	s.False(createSampleRegion(nil, []byte{10}).Contains([]byte{10}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{}))
	s.True(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{15}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{30}))
}

func (s *testRegionCacheSuite) TestContainsByEnd() {
	s.False(createSampleRegion(nil, nil).ContainsByEnd([]byte{}))
	s.True(createSampleRegion(nil, nil).ContainsByEnd([]byte{10}))
	s.False(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{}))
	s.False(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{10}))
	s.True(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{11}))
	s.False(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{}))
	s.True(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{10}))
	s.False(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{11}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{}))
	s.True(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{15}))
	s.False(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{30}))
}

func (s *testRegionCacheSuite) TestSwitchPeerWhenNoLeader() {
	var prevCtx *RPCContext
	for i := 0; i <= len(s.cluster.GetAllStores()); i++ {
		loc, err := s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
		s.Nil(err)
		if prevCtx == nil {
			s.Equal(i, 0)
		} else {
			s.NotEqual(ctx.AccessIdx, prevCtx.AccessIdx)
			s.NotEqual(ctx.Peer, prevCtx.Peer)
		}
		s.cache.InvalidateCachedRegionWithReason(loc.Region, NoLeader)
		s.Equal(s.cache.GetCachedRegionWithRLock(loc.Region).invalidReason, NoLeader)
		prevCtx = ctx
	}
}

func BenchmarkOnRequestFail(b *testing.B) {
	/*
			This benchmark simulate many concurrent requests call OnSendRequestFail method
			after failed on a store, validate that on this scene, requests don't get blocked on the
		    RegionCache lock.
	*/
	regionCnt, storeCount := 998, 3
	cluster := createClusterWithStoresAndRegions(regionCnt, storeCount)
	cache := NewRegionCache(mocktikv.NewPDClient(cluster))
	defer cache.Close()
	loadRegionsToCache(cache, regionCnt)
	bo := retry.NewBackofferWithVars(context.Background(), 1, nil)
	loc, err := cache.LocateKey(bo, []byte{})
	if err != nil {
		b.Fatal(err)
	}
	region := cache.getRegionByIDFromCache(loc.Region.id)
	b.ResetTimer()
	regionStore := region.getStore()
	store, peer, accessIdx, _ := region.WorkStorePeer(regionStore)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rpcCtx := &RPCContext{
				Region:     loc.Region,
				Meta:       region.meta,
				AccessIdx:  accessIdx,
				Peer:       peer,
				Store:      store,
				AccessMode: tiKVOnly,
			}
			r := cache.GetCachedRegionWithRLock(rpcCtx.Region)
			if r != nil {
				r.getStore().switchNextTiKVPeer(r, rpcCtx.AccessIdx)
			}
		}
	})
	if len(cache.mu.regions) != regionCnt*2/3 {
		b.Fatal(len(cache.mu.regions))
	}
}
