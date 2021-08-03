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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_request_test.go
//

// Copyright 2017 PingCAP, Inc.
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
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestRegionRequestToThreeStores(t *testing.T) {
	suite.Run(t, new(testRegionRequestToThreeStoresSuite))
}

type testRegionRequestToThreeStoresSuite struct {
	suite.Suite
	cluster             *mocktikv.Cluster
	storeIDs            []uint64
	peerIDs             []uint64
	regionID            uint64
	leaderPeer          uint64
	cache               *RegionCache
	bo                  *retry.Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           mocktikv.MVCCStore
}

func (s *testRegionRequestToThreeStoresSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	s.storeIDs, s.peerIDs, s.regionID, s.leaderPeer = mocktikv.BootstrapWithMultiStores(s.cluster, 3)
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestToThreeStoresSuite) TearDownTest() {
	s.cache.Close()
	s.mvccStore.Close()
}

func (s *testRegionRequestToThreeStoresSuite) TestGetRPCContext() {
	// Load the bootstrapped region into the cache.
	_, err := s.cache.BatchLoadRegionsFromKey(s.bo, []byte{}, 1)
	s.Nil(err)

	var seed uint32
	var regionID = RegionVerID{s.regionID, 0, 0}

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}, kv.ReplicaReadLeader, &seed)
	rpcCtx, err := s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	s.Nil(err)
	s.Equal(rpcCtx.Peer.Id, s.leaderPeer)

	req.ReplicaReadType = kv.ReplicaReadFollower
	rpcCtx, err = s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	s.Nil(err)
	s.NotEqual(rpcCtx.Peer.Id, s.leaderPeer)

	req.ReplicaReadType = kv.ReplicaReadMixed
	rpcCtx, err = s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	s.Nil(err)
	s.Equal(rpcCtx.Peer.Id, s.leaderPeer)

	seed = 1
	rpcCtx, err = s.regionRequestSender.getRPCContext(s.bo, req, regionID, tikvrpc.TiKV)
	s.Nil(err)
	s.NotEqual(rpcCtx.Peer.Id, s.leaderPeer)
}

func (s *testRegionRequestToThreeStoresSuite) TestStoreTokenLimit() {
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{}, kvrpcpb.Context{})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(region)
	oldStoreLimit := kv.StoreLimit.Load()
	kv.StoreLimit.Store(500)
	s.cache.getStoreByStoreID(s.storeIDs[0]).tokenCount.Store(500)
	// cause there is only one region in this cluster, regionID maps this leader.
	resp, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.NotNil(err)
	s.Nil(resp)
	e, ok := errors.Cause(err).(*tikverr.ErrTokenLimit)
	s.True(ok)
	s.Equal(e.StoreID, uint64(1))
	kv.StoreLimit.Store(oldStoreLimit)
}

func (s *testRegionRequestToThreeStoresSuite) TestSwitchPeerWhenNoLeader() {
	var leaderAddr string
	s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		if leaderAddr == "" {
			leaderAddr = addr
		}
		// Returns OK when switches to a different peer.
		if leaderAddr != addr {
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{
			RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}},
		}}, nil
	}}

	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

	bo := retry.NewBackofferWithVars(context.Background(), 5, nil)
	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	resp, err := s.regionRequestSender.SendReq(bo, req, loc.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp)
}

func (s *testRegionRequestToThreeStoresSuite) loadAndGetLeaderStore() (*Store, string) {
	region, err := s.regionRequestSender.regionCache.findRegionByKey(s.bo, []byte("a"), false)
	s.Nil(err)
	leaderStore, leaderPeer, _, _ := region.WorkStorePeer(region.getStore())
	s.Equal(leaderPeer.Id, s.leaderPeer)
	leaderAddr, err := s.regionRequestSender.regionCache.getStoreAddr(s.bo, region, leaderStore)
	s.Nil(err)
	return leaderStore, leaderAddr
}

func (s *testRegionRequestToThreeStoresSuite) TestForwarding() {
	s.regionRequestSender.regionCache.enableForwarding = true

	// First get the leader's addr from region cache
	leaderStore, leaderAddr := s.loadAndGetLeaderStore()

	bo := retry.NewBackoffer(context.Background(), 10000)

	// Simulate that the leader is network-partitioned but can be accessed by forwarding via a follower
	innerClient := s.regionRequestSender.client
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		if addr == leaderAddr {
			return nil, errors.New("simulated rpc error")
		}
		// MockTiKV doesn't support forwarding. Simulate forwarding here.
		if len(req.ForwardedHost) != 0 {
			addr = req.ForwardedHost
		}
		return innerClient.SendRequest(ctx, addr, req, timeout)
	}}
	var storeState uint32 = uint32(unreachable)
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness = func(s *Store, bo *retry.Backoffer) livenessState {
		return livenessState(atomic.LoadUint32(&storeState))
	}

	loc, err := s.regionRequestSender.regionCache.LocateKey(bo, []byte("k"))
	s.Nil(err)
	s.Equal(loc.Region.GetID(), s.regionID)
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v1"),
	})
	resp, ctx, err := s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr)
	s.Equal(resp.Resp.(*kvrpcpb.RawPutResponse).Error, "")
	s.Equal(ctx.Addr, leaderAddr)
	s.NotNil(ctx.ProxyStore)
	s.NotEqual(ctx.ProxyAddr, leaderAddr)
	s.Nil(err)

	// Simulate recovering to normal
	s.regionRequestSender.client = innerClient
	atomic.StoreUint32(&storeState, uint32(reachable))
	start := time.Now()
	for {
		if atomic.LoadInt32(&leaderStore.unreachable) == 0 {
			break
		}
		if time.Since(start) > 3*time.Second {
			s.FailNow("store didn't recover to normal in time")
		}
		time.Sleep(time.Millisecond * 200)
	}
	atomic.StoreUint32(&storeState, uint32(unreachable))

	req = tikvrpc.NewRequest(tikvrpc.CmdRawGet, &kvrpcpb.RawGetRequest{Key: []byte("k")})
	resp, ctx, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	regionErr, err = resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr)
	s.Equal(resp.Resp.(*kvrpcpb.RawGetResponse).Value, []byte("v1"))
	s.Nil(ctx.ProxyStore)

	// Simulate server down
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		if addr == leaderAddr || req.ForwardedHost == leaderAddr {
			return nil, errors.New("simulated rpc error")
		}

		// MockTiKV doesn't support forwarding. Simulate forwarding here.
		if len(req.ForwardedHost) != 0 {
			addr = req.ForwardedHost
		}
		return innerClient.SendRequest(ctx, addr, req, timeout)
	}}
	// The leader is changed after a store is down.
	newLeaderPeerID := s.peerIDs[0]
	if newLeaderPeerID == s.leaderPeer {
		newLeaderPeerID = s.peerIDs[1]
	}

	s.NotEqual(newLeaderPeerID, s.leaderPeer)
	s.cluster.ChangeLeader(s.regionID, newLeaderPeerID)

	req = tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v2"),
	})
	resp, ctx, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	regionErr, err = resp.GetRegionError()
	s.Nil(err)
	// After several retries, the region will be marked as needReload.
	// Then SendReqCtx will throw a pseudo EpochNotMatch to tell the caller to reload the region.
	s.NotNil(regionErr.EpochNotMatch)
	s.Nil(ctx)
	s.Equal(len(s.regionRequestSender.failStoreIDs), 0)
	s.Equal(len(s.regionRequestSender.failProxyStoreIDs), 0)
	region := s.regionRequestSender.regionCache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(region)
	s.True(region.checkNeedReload())

	loc, err = s.regionRequestSender.regionCache.LocateKey(bo, []byte("k"))
	s.Nil(err)
	req = tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v2"),
	})
	resp, ctx, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	regionErr, err = resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr)
	s.Equal(resp.Resp.(*kvrpcpb.RawPutResponse).Error, "")
	// Leader changed
	s.NotEqual(ctx.Store.storeID, leaderStore.storeID)
	s.Nil(ctx.ProxyStore)
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelector() {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)
	region := s.cache.GetCachedRegionWithRLock(regionLoc.Region)
	regionStore := region.getStore()

	// Create a fake region and change its leader to the last peer.
	regionStore = regionStore.clone()
	regionStore.workTiKVIdx = AccessIndex(len(regionStore.stores) - 1)
	sidx, _ := regionStore.accessStore(tiKVOnly, regionStore.workTiKVIdx)
	regionStore.stores[sidx].epoch++
	regionStore.storeEpochs[sidx]++
	// Add a TiFlash peer to the region.
	peer := &metapb.Peer{Id: s.cluster.AllocID(), StoreId: s.cluster.AllocID()}
	regionStore.accessIndex[tiFlashOnly] = append(regionStore.accessIndex[tiFlashOnly], len(regionStore.stores))
	regionStore.stores = append(regionStore.stores, &Store{storeID: peer.StoreId, storeType: tikvrpc.TiFlash})
	regionStore.storeEpochs = append(regionStore.storeEpochs, 0)

	region = &Region{
		meta: region.GetMeta(),
	}
	region.lastAccess = time.Now().Unix()
	region.meta.Peers = append(region.meta.Peers, peer)
	atomic.StorePointer(&region.store, unsafe.Pointer(regionStore))

	cache := NewRegionCache(s.cache.pdClient)
	defer cache.Close()
	cache.insertRegionToCache(region)

	// Verify creating the replicaSelector.
	replicaSelector, err := newReplicaSelector(cache, regionLoc.Region)
	s.NotNil(replicaSelector)
	s.Nil(err)
	s.Equal(replicaSelector.region, region)
	// Should only contains TiKV stores.
	s.Equal(len(replicaSelector.replicas), regionStore.accessStoreNum(tiKVOnly))
	s.Equal(len(replicaSelector.replicas), len(regionStore.stores)-1)
	s.IsType(accessKnownLeader{}, replicaSelector.state)

	// Verify that the store matches the peer and epoch.
	for _, replica := range replicaSelector.replicas {
		s.Equal(replica.store.storeID, replica.peer.GetStoreId())
		s.Equal(replica.peer, region.getPeerOnStore(replica.store.storeID))
		s.True(replica.attempts == 0)

		for i, store := range regionStore.stores {
			if replica.store == store {
				s.Equal(replica.epoch, regionStore.storeEpochs[i])
			}
		}
	}
	// Verify that the leader replica is at the head of replicas.
	leaderStore, leaderPeer, _, _ := region.WorkStorePeer(regionStore)
	leaderReplica := replicaSelector.replicas[0]
	s.Equal(leaderReplica.store, leaderStore)
	s.Equal(leaderReplica.peer, leaderPeer)

	/*
	assertRPCCtxEqual := func(rpcCtx *RPCContext, replica *replica) {
		s.Equal(rpcCtx.Store, replicaSelector.replicas[replicaSelector.nextReplicaIdx-1].store)
		s.Equal(rpcCtx.Peer, replicaSelector.replicas[replicaSelector.nextReplicaIdx-1].peer)
		s.Equal(rpcCtx.Addr, replicaSelector.replicas[replicaSelector.nextReplicaIdx-1].store.addr)
		s.Equal(rpcCtx.AccessMode, tiKVOnly)
	}

	// Verify the correctness of next()
	for i := 0; i < len(replicaSelector.replicas); i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		s.NotNil(rpcCtx)
		s.Nil(err)
		s.Equal(rpcCtx.Region, regionLoc.Region)
		s.Equal(rpcCtx.Meta, region.meta)
		replica := replicaSelector.replicas[replicaSelector.nextReplicaIdx-1]
		assertRPCCtxEqual(rpcCtx, replica)
		s.Equal(replica.attempts, 1)
		s.Equal(replicaSelector.nextReplicaIdx, i+1)
	}
	s.True(replicaSelector.isExhausted())
	rpcCtx, err := replicaSelector.next(s.bo)
	s.Nil(rpcCtx)
	s.Nil(err)
	// The region should be invalidated if runs out of all replicas.
	s.False(replicaSelector.region.isValid())

	region.lastAccess = time.Now().Unix()
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region)
	s.Nil(err)
	s.NotNil(replicaSelector)
	cache.testingKnobs.mockRequestLiveness = func(s *Store, bo *retry.Backoffer) livenessState {
		return reachable
	}
	for i := 0; i < maxReplicaAttempt; i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		s.NotNil(rpcCtx)
		s.Nil(err)
		nextIdx := replicaSelector.nextReplicaIdx
		// Verify that retry the same store if it's reachable.
		replicaSelector.onSendFailure(s.bo, nil)
		s.Equal(nextIdx, replicaSelector.nextReplicaIdx+1)
		s.Equal(replicaSelector.nextReplica().attempts, i+1)
	}
	// Verify the maxReplicaAttempt limit for each replica.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	assertRPCCtxEqual(rpcCtx, replicaSelector.replicas[1])
	s.Equal(replicaSelector.nextReplicaIdx, 2)

	// Verify updating leader.
	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	replicaSelector.next(s.bo)
	// The leader is the 3rd replica. After updating leader, it should be the next.
	leader := replicaSelector.replicas[2]
	replicaSelector.updateLeader(leader.peer)
	s.Equal(replicaSelector.nextReplica(), leader)
	s.Equal(replicaSelector.nextReplicaIdx, 1)
	rpcCtx, _ = replicaSelector.next(s.bo)
	assertRPCCtxEqual(rpcCtx, leader)
	// Verify the regionStore is updated and the workTiKVIdx points to the leader.
	regionStore = region.getStore()
	leaderStore, leaderPeer, _, _ = region.WorkStorePeer(regionStore)
	s.Equal(leaderStore, leader.store)
	s.Equal(leaderPeer, leader.peer)

	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	replicaSelector.next(s.bo)
	replicaSelector.next(s.bo)
	replicaSelector.next(s.bo)
	s.True(replicaSelector.isExhausted())
	// The leader is the 1st replica. After updating leader, it should be the next and
	// the currnet replica is skipped.
	leader = replicaSelector.replicas[0]
	replicaSelector.updateLeader(leader.peer)
	// The leader should be the next replica.
	s.Equal(replicaSelector.nextReplica(), leader)
	s.Equal(replicaSelector.nextReplicaIdx, 2)
	rpcCtx, _ = replicaSelector.next(s.bo)
	s.True(replicaSelector.isExhausted())
	assertRPCCtxEqual(rpcCtx, leader)
	// Verify the regionStore is updated and the workTiKVIdx points to the leader.
	regionStore = region.getStore()
	leaderStore, leaderPeer, _, _ = region.WorkStorePeer(regionStore)
	s.Equal(leaderStore, leader.store)
	s.Equal(leaderPeer, leader.peer)

	// Give the leader one more chance even if it exceeds the maxReplicaAttempt.
	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	leader = replicaSelector.replicas[0]
	leader.attempts = maxReplicaAttempt
	replicaSelector.updateLeader(leader.peer)
	s.Equal(leader.attempts, maxReplicaAttempt-1)
	rpcCtx, _ = replicaSelector.next(s.bo)
	assertRPCCtxEqual(rpcCtx, leader)
	s.Equal(leader.attempts, maxReplicaAttempt)

	// Invalidate the region if the leader is not in the region.
	region.lastAccess = time.Now().Unix()
	replicaSelector.updateLeader(&metapb.Peer{Id: s.cluster.AllocID(), StoreId: s.cluster.AllocID()})
	s.False(region.isValid())
	// Don't try next replica if the region is invalidated.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(rpcCtx)
	s.Nil(err)

	// Verify on send success.
	region.lastAccess = time.Now().Unix()
	replicaSelector, _ = newReplicaSelector(cache, regionLoc.Region)
	replicaSelector.next(s.bo)
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	replicaSelector.OnSendSuccess()
	// Verify the regionStore is updated and the workTiKVIdx points to the leader.
	leaderStore, leaderPeer, _, _ = region.WorkStorePeer(region.getStore())
	s.Equal(leaderStore, rpcCtx.Store)
	s.Equal(leaderPeer, rpcCtx.Peer)
	*/
}

//// TODO(youjiali1995): Remove duplicated tests. This test may be duplicated with other
//// tests but it's a dedicated one to test sending requests with the replica selector.
//func (s *testRegionRequestToThreeStoresSuite) TestSendReqWithReplicaSelector() {
//	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
//		Key:   []byte("key"),
//		Value: []byte("value"),
//	})
//	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
//	s.Nil(err)
//	s.NotNil(region)
//
//	reloadRegion := func() {
//		s.regionRequestSender.leaderReplicaSelector.region.invalidate(Other)
//		region, _ = s.cache.LocateRegionByID(s.bo, s.regionID)
//	}
//
//	hasFakeRegionError := func(resp *tikvrpc.Response) bool {
//		if resp == nil {
//			return false
//		}
//		regionErr, err := resp.GetRegionError()
//		if err != nil {
//			return false
//		}
//		return IsFakeRegionError(regionErr)
//	}
//
//	// Normal
//	bo := retry.NewBackoffer(context.Background(), -1)
//	sender := s.regionRequestSender
//	resp, err := sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.NotNil(resp)
//	s.True(bo.GetTotalBackoffTimes() == 0)
//
//	// Switch to the next Peer due to store failure and the leader is on the next peer.
//	bo = retry.NewBackoffer(context.Background(), -1)
//	s.cluster.ChangeLeader(s.regionID, s.peerIDs[1])
//	s.cluster.StopStore(s.storeIDs[0])
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.NotNil(resp)
//	s.Equal(sender.leaderReplicaSelector.nextReplicaIdx, 2)
//	s.True(bo.GetTotalBackoffTimes() == 1)
//	s.cluster.StartStore(s.storeIDs[0])
//
//	// Leader is updated because of send success, so no backoff.
//	bo = retry.NewBackoffer(context.Background(), -1)
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.NotNil(resp)
//	s.Equal(sender.leaderReplicaSelector.nextReplicaIdx, 1)
//	s.True(bo.GetTotalBackoffTimes() == 0)
//
//	// Switch to the next peer due to leader failure but the new leader is not elected.
//	// Region will be invalidated due to store epoch changed.
//	reloadRegion()
//	s.cluster.StopStore(s.storeIDs[1])
//	bo = retry.NewBackoffer(context.Background(), -1)
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.True(hasFakeRegionError(resp))
//	s.False(sender.leaderReplicaSelector.isExhausted())
//	s.Equal(bo.GetTotalBackoffTimes(), 1)
//	s.cluster.StartStore(s.storeIDs[1])
//
//	// Leader is changed. No backoff.
//	reloadRegion()
//	s.cluster.ChangeLeader(s.regionID, s.peerIDs[0])
//	bo = retry.NewBackoffer(context.Background(), -1)
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.NotNil(resp)
//	s.Equal(bo.GetTotalBackoffTimes(), 0)
//
//	// No leader. Backoff for each replica and runs out all replicas.
//	s.cluster.GiveUpLeader(s.regionID)
//	bo = retry.NewBackoffer(context.Background(), -1)
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.True(hasFakeRegionError(resp))
//	s.Equal(bo.GetTotalBackoffTimes(), 3)
//	s.True(sender.leaderReplicaSelector.isExhausted())
//	s.False(sender.leaderReplicaSelector.region.isValid())
//	s.cluster.ChangeLeader(s.regionID, s.peerIDs[0])
//
//	// The leader store is alive but can't provide service.
//	// Region will be invalidated due to running out of all replicas.
//	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness = func(s *Store, bo *retry.Backoffer) livenessState {
//		return reachable
//	}
//	reloadRegion()
//	s.cluster.StopStore(s.storeIDs[0])
//	bo = retry.NewBackoffer(context.Background(), -1)
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.True(hasFakeRegionError(resp))
//	s.True(sender.leaderReplicaSelector.isExhausted())
//	s.False(sender.leaderReplicaSelector.region.isValid())
//	s.Equal(bo.GetTotalBackoffTimes(), maxReplicaAttempt+2)
//	s.cluster.StartStore(s.storeIDs[0])
//
//	// Verify that retry the same replica when meets ServerIsBusy/MaxTimestampNotSynced/ReadIndexNotReady/ProposalInMergingMode.
//	for _, regionErr := range []*errorpb.Error{
//		// ServerIsBusy takes too much time to test.
//		// {ServerIsBusy: &errorpb.ServerIsBusy{}},
//		{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
//		{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}},
//		{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}},
//	} {
//		func() {
//			oc := sender.client
//			defer func() {
//				sender.client = oc
//			}()
//			s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
//				// Return the specific region error when accesses the leader.
//				if addr == s.cluster.GetStore(s.storeIDs[0]).Address {
//					return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: regionErr}}, nil
//				}
//				// Return the not leader error when accesses followers.
//				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{
//					NotLeader: &errorpb.NotLeader{
//						RegionId: region.Region.id, Leader: &metapb.Peer{Id: s.peerIDs[0], StoreId: s.storeIDs[0]},
//					}}}}, nil
//
//			}}
//			reloadRegion()
//			bo = retry.NewBackoffer(context.Background(), -1)
//			resp, err := sender.SendReq(bo, req, region.Region, time.Second)
//			s.Nil(err)
//			s.True(hasFakeRegionError(resp))
//			s.True(sender.leaderReplicaSelector.isExhausted())
//			s.False(sender.leaderReplicaSelector.region.isValid())
//			s.Equal(bo.GetTotalBackoffTimes(), maxReplicaAttempt+2)
//		}()
//	}
//
//	// Verify switch to the next peer immediately when meets StaleCommand.
//	reloadRegion()
//	func() {
//		oc := sender.client
//		defer func() {
//			sender.client = oc
//		}()
//		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
//			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}}}, nil
//		}}
//		reloadRegion()
//		bo = retry.NewBackoffer(context.Background(), -1)
//		resp, err := sender.SendReq(bo, req, region.Region, time.Second)
//		s.Nil(err)
//		s.True(hasFakeRegionError(resp))
//		s.True(sender.leaderReplicaSelector.isExhausted())
//		s.False(sender.leaderReplicaSelector.region.isValid())
//		s.Equal(bo.GetTotalBackoffTimes(), 0)
//	}()
//
//	// Verify don't invalidate region when meets unknown region errors.
//	reloadRegion()
//	func() {
//		oc := sender.client
//		defer func() {
//			sender.client = oc
//		}()
//		s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
//			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{Message: ""}}}, nil
//		}}
//		reloadRegion()
//		bo = retry.NewBackoffer(context.Background(), -1)
//		resp, err := sender.SendReq(bo, req, region.Region, time.Second)
//		s.Nil(err)
//		s.True(hasFakeRegionError(resp))
//		s.True(sender.leaderReplicaSelector.isExhausted())
//		s.False(sender.leaderReplicaSelector.region.isValid())
//		s.Equal(bo.GetTotalBackoffTimes(), 0)
//	}()
//
//	// Verify invalidate region when meets StoreNotMatch/RegionNotFound/EpochNotMatch/NotLeader and can't find the leader in region.
//	for i, regionErr := range []*errorpb.Error{
//		{StoreNotMatch: &errorpb.StoreNotMatch{}},
//		{RegionNotFound: &errorpb.RegionNotFound{}},
//		{EpochNotMatch: &errorpb.EpochNotMatch{}},
//		{NotLeader: &errorpb.NotLeader{Leader: &metapb.Peer{}}}} {
//		func() {
//			oc := sender.client
//			defer func() {
//				sender.client = oc
//			}()
//			s.regionRequestSender.client = &fnClient{func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
//				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: regionErr}}, nil
//
//			}}
//			reloadRegion()
//			bo = retry.NewBackoffer(context.Background(), -1)
//			resp, err := sender.SendReq(bo, req, region.Region, time.Second)
//
//			// Return a sendError when meets NotLeader and can't find the leader in the region.
//			if i == 3 {
//				s.Nil(err)
//				s.True(hasFakeRegionError(resp))
//			} else {
//				s.Nil(err)
//				s.NotNil(resp)
//				regionErr, _ := resp.GetRegionError()
//				s.NotNil(regionErr)
//			}
//			s.False(sender.leaderReplicaSelector.isExhausted())
//			s.False(sender.leaderReplicaSelector.region.isValid())
//			s.Equal(bo.GetTotalBackoffTimes(), 0)
//		}()
//	}
//
//	// Runs out of all replicas and then returns a send error.
//	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness = func(s *Store, bo *retry.Backoffer) livenessState {
//		return unreachable
//	}
//	reloadRegion()
//	for _, store := range s.storeIDs {
//		s.cluster.StopStore(store)
//	}
//	bo = retry.NewBackoffer(context.Background(), -1)
//	resp, err = sender.SendReq(bo, req, region.Region, time.Second)
//	s.Nil(err)
//	s.True(hasFakeRegionError(resp))
//	s.True(bo.GetTotalBackoffTimes() == 3)
//	s.False(sender.leaderReplicaSelector.region.isValid())
//	for _, store := range s.storeIDs {
//		s.cluster.StartStore(store)
//	}
//}