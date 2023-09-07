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
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
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
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster), apicodec.NewCodecV1(apicodec.ModeTxn)}
	s.cache = NewRegionCache(pdCli)
	s.bo = retry.NewNoopBackoff(context.Background())
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore, nil)
	s.regionRequestSender = NewRegionRequestSender(s.cache, client)
}

func (s *testRegionRequestToThreeStoresSuite) TearDownTest() {
	s.cache.Close()
	s.mvccStore.Close()
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
	resp, _, err := s.regionRequestSender.SendReq(s.bo, req, region.Region, time.Second)
	s.NotNil(err)
	s.Nil(resp)
	e, ok := errors.Cause(err).(*tikverr.ErrTokenLimit)
	s.True(ok)
	s.Equal(e.StoreID, uint64(1))
	kv.StoreLimit.Store(oldStoreLimit)
}

func (s *testRegionRequestToThreeStoresSuite) TestSwitchPeerWhenNoLeader() {
	var leaderAddr string
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
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
	resp, _, err := s.regionRequestSender.SendReq(bo, req, loc.Region, time.Second)
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
	var storeState = uint32(unreachable)
	tf := func(s *Store, bo *retry.Backoffer) livenessState {
		if s.addr == leaderAddr {
			return livenessState(atomic.LoadUint32(&storeState))
		}
		return reachable
	}
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(&tf))

	loc, err := s.regionRequestSender.regionCache.LocateKey(bo, []byte("k"))
	s.Nil(err)
	s.Equal(loc.Region.GetID(), s.regionID)
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v1"),
	})
	resp, ctx, _, err := s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
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
		if leaderStore.getLivenessState() == reachable {
			break
		}
		if time.Since(start) > 3*time.Second {
			s.FailNow("store didn't recover to normal in time")
		}
		time.Sleep(time.Millisecond * 200)
	}
	atomic.StoreUint32(&storeState, uint32(unreachable))

	req = tikvrpc.NewRequest(tikvrpc.CmdRawGet, &kvrpcpb.RawGetRequest{Key: []byte("k")})
	resp, ctx, _, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
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
	resp, ctx, _, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
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
	s.False(region.isValid())

	loc, err = s.regionRequestSender.regionCache.LocateKey(bo, []byte("k"))
	s.Nil(err)
	req = tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("k"),
		Value: []byte("v2"),
	})
	resp, ctx, _, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	regionErr, err = resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr)
	s.Equal(resp.Resp.(*kvrpcpb.RawPutResponse).Error, "")
	// Leader changed
	s.NotEqual(ctx.Store.storeID, leaderStore.storeID)
	s.Nil(ctx.ProxyStore)
}

func refreshEpochs(regionStore *regionStore) {
	for i, store := range regionStore.stores {
		regionStore.storeEpochs[i] = atomic.LoadUint32(&store.epoch)
	}
}

func refreshLivenessStates(regionStore *regionStore) {
	for _, store := range regionStore.stores {
		atomic.StoreUint32(&store.livenessState, uint32(reachable))
	}
}

func AssertRPCCtxEqual(s *testRegionRequestToThreeStoresSuite, rpcCtx *RPCContext, target *replica, proxy *replica) {
	s.Equal(rpcCtx.Store, target.store)
	s.Equal(rpcCtx.Peer, target.peer)
	s.Equal(rpcCtx.Addr, target.store.addr)
	s.Equal(rpcCtx.AccessMode, tiKVOnly)
	if proxy != nil {
		s.Equal(rpcCtx.ProxyStore, proxy.store)
		s.Equal(rpcCtx.ProxyAddr, proxy.store.addr)
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestLearnerReplicaSelector() {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)
	region := s.cache.GetCachedRegionWithRLock(regionLoc.Region)
	regionStore := region.getStore()
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{})

	// Create a fake region and change its leader to the last peer.
	regionStore = regionStore.clone()
	regionStore.workTiKVIdx = AccessIndex(len(regionStore.stores) - 1)
	sidx, _ := regionStore.accessStore(tiKVOnly, regionStore.workTiKVIdx)
	regionStore.stores[sidx].epoch++

	// Add a TiKV learner peer to the region.
	storeID := s.cluster.AllocID()
	s.cluster.AddStore(storeID, fmt.Sprintf("store%d", storeID))
	tikvLearner := &metapb.Peer{Id: s.cluster.AllocID(), StoreId: storeID, Role: metapb.PeerRole_Learner}
	tikvLearnerAccessIdx := len(regionStore.stores)
	regionStore.accessIndex[tiKVOnly] = append(regionStore.accessIndex[tiKVOnly], tikvLearnerAccessIdx)
	regionStore.stores = append(regionStore.stores, &Store{storeID: tikvLearner.StoreId})
	regionStore.storeEpochs = append(regionStore.storeEpochs, 0)

	region = &Region{
		meta: region.GetMeta(),
	}
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	region.meta.Peers = append(region.meta.Peers, tikvLearner)
	atomic.StorePointer(&region.store, unsafe.Pointer(regionStore))

	cache := NewRegionCache(s.cache.pdClient)
	defer cache.Close()
	cache.mu.Lock()
	cache.insertRegionToCache(region, true)
	cache.mu.Unlock()

	// Test accessFollower state with kv.ReplicaReadLearner request type.
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	refreshEpochs(regionStore)
	req.ReplicaReadType = kv.ReplicaReadLearner
	replicaSelector, err := newReplicaSelector(cache, regionLoc.Region, req)
	s.NotNil(replicaSelector)
	s.Nil(err)

	accessLearner, _ := replicaSelector.state.(*accessFollower)
	// Invalidate the region if the leader is not in the region.
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	rpcCtx, err := replicaSelector.next(s.bo)
	s.Nil(err)
	// Should switch to the next follower.
	s.Equal(AccessIndex(tikvLearnerAccessIdx), accessLearner.lastIdx)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[replicaSelector.targetIdx], nil)
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelector() {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)
	region := s.cache.GetCachedRegionWithRLock(regionLoc.Region)
	regionStore := region.getStore()
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{})

	// Create a fake region and change its leader to the last peer.
	regionStore = regionStore.clone()
	regionStore.workTiKVIdx = AccessIndex(len(regionStore.stores) - 1)
	sidx, _ := regionStore.accessStore(tiKVOnly, regionStore.workTiKVIdx)
	regionStore.stores[sidx].epoch++
	regionStore.storeEpochs[sidx]++
	// Add a TiFlash peer to the region.
	tiflash := &metapb.Peer{Id: s.cluster.AllocID(), StoreId: s.cluster.AllocID()}
	regionStore.accessIndex[tiFlashOnly] = append(regionStore.accessIndex[tiFlashOnly], len(regionStore.stores))
	regionStore.stores = append(regionStore.stores, &Store{storeID: tiflash.StoreId, storeType: tikvrpc.TiFlash})
	regionStore.storeEpochs = append(regionStore.storeEpochs, 0)

	region = &Region{
		meta: region.GetMeta(),
	}
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	region.meta.Peers = append(region.meta.Peers, tiflash)
	atomic.StorePointer(&region.store, unsafe.Pointer(regionStore))

	cache := NewRegionCache(s.cache.pdClient)
	defer cache.Close()
	cache.mu.Lock()
	cache.insertRegionToCache(region, true)
	cache.mu.Unlock()

	// Verify creating the replicaSelector.
	replicaSelector, err := newReplicaSelector(cache, regionLoc.Region, req)
	s.NotNil(replicaSelector)
	s.Nil(err)
	s.Equal(replicaSelector.region, region)
	// Should only contain TiKV stores.
	s.Equal(len(replicaSelector.replicas), regionStore.accessStoreNum(tiKVOnly))
	s.Equal(len(replicaSelector.replicas), len(regionStore.stores)-1)
	s.IsType(&accessKnownLeader{}, replicaSelector.state)

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

	// Test accessKnownLeader state
	s.IsType(&accessKnownLeader{}, replicaSelector.state)
	// Try the leader for maxReplicaAttempt times
	for i := 1; i <= maxReplicaAttempt; i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		s.Nil(err)
		AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[regionStore.workTiKVIdx], nil)
		s.IsType(&accessKnownLeader{}, replicaSelector.state)
		s.Equal(replicaSelector.replicas[regionStore.workTiKVIdx].attempts, i)
	}

	// After that it should switch to tryFollower
	for i := 0; i < len(replicaSelector.replicas)-1; i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		s.Nil(err)
		state, ok := replicaSelector.state.(*tryFollower)
		s.True(ok)
		s.Equal(regionStore.workTiKVIdx, state.leaderIdx)
		s.NotEqual(state.lastIdx, regionStore.workTiKVIdx)
		s.Equal(replicaSelector.targetIdx, state.lastIdx)
		AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[replicaSelector.targetIdx], nil)
		s.Equal(replicaSelector.targetReplica().attempts, 1)
	}
	// In tryFollower state, if all replicas are tried, nil RPCContext should be returned
	rpcCtx, err := replicaSelector.next(s.bo)
	s.Nil(err)
	s.Nil(rpcCtx)
	// The region should be invalidated
	s.False(replicaSelector.region.isValid())

	// Test switching to tryFollower if leader is unreachable
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.Nil(err)
	s.NotNil(replicaSelector)
	tf := func(s *Store, bo *retry.Backoffer) livenessState {
		return unreachable
	}
	cache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(&tf))
	s.IsType(&accessKnownLeader{}, replicaSelector.state)
	_, err = replicaSelector.next(s.bo)
	s.Nil(err)
	replicaSelector.onSendFailure(s.bo, nil)
	rpcCtx, err = replicaSelector.next(s.bo)
	s.NotNil(rpcCtx)
	s.Nil(err)
	s.IsType(&tryFollower{}, replicaSelector.state)
	s.NotEqual(replicaSelector.targetIdx, regionStore.workTiKVIdx)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.targetReplica(), nil)
	s.Equal(replicaSelector.targetReplica().attempts, 1)
	// If the NotLeader errors provides an unreachable leader, do not switch to it.
	replicaSelector.onNotLeader(s.bo, rpcCtx, &errorpb.NotLeader{
		RegionId: region.GetID(), Leader: &metapb.Peer{Id: s.peerIDs[regionStore.workTiKVIdx], StoreId: s.storeIDs[regionStore.workTiKVIdx]},
	})
	s.IsType(&tryFollower{}, replicaSelector.state)

	// If the leader is unreachable and forwarding is not enabled, just do not try
	// the unreachable leader.
	refreshEpochs(regionStore)
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.Nil(err)
	s.NotNil(replicaSelector)
	s.IsType(&accessKnownLeader{}, replicaSelector.state)
	// Now, livenessState is unreachable, so it will try a reachable follower instead of the unreachable leader.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	s.NotNil(rpcCtx)
	_, ok := replicaSelector.state.(*tryFollower)
	s.True(ok)
	s.NotEqual(regionStore.workTiKVIdx, replicaSelector.targetIdx)

	// Do not try to use proxy if livenessState is unknown instead of unreachable.
	refreshEpochs(regionStore)
	cache.enableForwarding = true
	tf = func(s *Store, bo *retry.Backoffer) livenessState {
		return unknown
	}
	cache.testingKnobs.mockRequestLiveness.Store(
		(*livenessFunc)(&tf))
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.Nil(err)
	s.NotNil(replicaSelector)
	s.Eventually(func() bool {
		return regionStore.stores[regionStore.workTiKVIdx].getLivenessState() == unknown
	}, 3*time.Second, 200*time.Millisecond)
	s.IsType(&accessKnownLeader{}, replicaSelector.state)
	// Now, livenessState is unknown. Even if forwarding is enabled, it should try followers
	// instead of using the proxy.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	s.NotNil(rpcCtx)
	_, ok = replicaSelector.state.(*tryFollower)
	s.True(ok)

	// Test switching to tryNewProxy if leader is unreachable and forwarding is enabled
	refreshEpochs(regionStore)
	cache.enableForwarding = true
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.Nil(err)
	s.NotNil(replicaSelector)
	tf = func(s *Store, bo *retry.Backoffer) livenessState {
		return unreachable
	}
	cache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(&tf))
	s.Eventually(func() bool {
		return regionStore.stores[regionStore.workTiKVIdx].getLivenessState() == unreachable
	}, 3*time.Second, 200*time.Millisecond)
	s.IsType(&accessKnownLeader{}, replicaSelector.state)
	// Now, livenessState is unreachable, so it will try a new proxy instead of the leader.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	s.NotNil(rpcCtx)
	state, ok := replicaSelector.state.(*tryNewProxy)
	s.True(ok)
	s.Equal(regionStore.workTiKVIdx, state.leaderIdx)
	s.Equal(AccessIndex(2), replicaSelector.targetIdx)
	s.NotEqual(AccessIndex(2), replicaSelector.proxyIdx)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.targetReplica(), replicaSelector.proxyReplica())
	s.Equal(replicaSelector.targetReplica().attempts, 1)
	s.Equal(replicaSelector.proxyReplica().attempts, 1)

	// When the current proxy node fails, it should try another one.
	lastProxy := replicaSelector.proxyIdx
	replicaSelector.onSendFailure(s.bo, nil)
	rpcCtx, err = replicaSelector.next(s.bo)
	s.NotNil(rpcCtx)
	s.Nil(err)
	state, ok = replicaSelector.state.(*tryNewProxy)
	s.True(ok)
	s.Equal(regionStore.workTiKVIdx, state.leaderIdx)
	s.Equal(AccessIndex(2), replicaSelector.targetIdx)
	s.NotEqual(lastProxy, replicaSelector.proxyIdx)
	s.Equal(replicaSelector.targetReplica().attempts, 2)
	s.Equal(replicaSelector.proxyReplica().attempts, 1)

	// Test proxy store is saves when proxy is enabled
	replicaSelector.onSendSuccess()
	regionStore = region.getStore()
	s.Equal(replicaSelector.proxyIdx, regionStore.proxyTiKVIdx)

	// Test initial state is accessByKnownProxy when proxyTiKVIdx is valid
	refreshEpochs(regionStore)
	cache.enableForwarding = true
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.Nil(err)
	s.NotNil(replicaSelector)
	state2, ok := replicaSelector.state.(*accessByKnownProxy)
	s.True(ok)
	s.Equal(regionStore.workTiKVIdx, state2.leaderIdx)
	_, err = replicaSelector.next(s.bo)
	s.Nil(err)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.targetReplica(), replicaSelector.proxyReplica())

	// Switch to tryNewProxy if the current proxy is not available
	replicaSelector.onSendFailure(s.bo, nil)
	s.IsType(&tryNewProxy{}, replicaSelector.state)
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.targetReplica(), replicaSelector.proxyReplica())
	s.Equal(regionStore.workTiKVIdx, state2.leaderIdx)
	s.Equal(AccessIndex(2), replicaSelector.targetIdx)
	s.NotEqual(regionStore.proxyTiKVIdx, replicaSelector.proxyIdx)
	s.Equal(replicaSelector.targetReplica().attempts, 2)
	s.Equal(replicaSelector.proxyReplica().attempts, 1)

	// Test accessFollower state with kv.ReplicaReadFollower request type.
	req = tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}, kv.ReplicaReadFollower, nil)
	refreshEpochs(regionStore)
	refreshLivenessStates(regionStore)
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.Nil(err)
	s.NotNil(replicaSelector)
	state3, ok := replicaSelector.state.(*accessFollower)
	s.True(ok)
	s.Equal(regionStore.workTiKVIdx, state3.leaderIdx)
	s.Equal(state3.lastIdx, AccessIndex(-1))

	lastIdx := AccessIndex(-1)
	for i := 0; i < regionStore.accessStoreNum(tiKVOnly)-1; i++ {
		rpcCtx, err := replicaSelector.next(s.bo)
		s.Nil(err)
		// Should switch to the next follower.
		s.NotEqual(lastIdx, state3.lastIdx)
		// Shouldn't access the leader if followers aren't exhausted.
		s.NotEqual(regionStore.workTiKVIdx, state3.lastIdx)
		s.Equal(replicaSelector.targetIdx, state3.lastIdx)
		AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[replicaSelector.targetIdx], nil)
		lastIdx = state3.lastIdx
	}
	// Fallback to the leader for 1 time
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	s.Equal(regionStore.workTiKVIdx, state3.lastIdx)
	s.Equal(replicaSelector.targetIdx, state3.lastIdx)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[regionStore.workTiKVIdx], nil)
	// All replicas are exhausted.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(rpcCtx)
	s.Nil(err)

	// Test accessFollower state filtering epoch-stale stores.
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	refreshEpochs(regionStore)
	// Mark all followers as stale.
	tiKVNum := regionStore.accessStoreNum(tiKVOnly)
	for i := 1; i < tiKVNum; i++ {
		regionStore.storeEpochs[(regionStore.workTiKVIdx+AccessIndex(i))%AccessIndex(tiKVNum)]++
	}
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.NotNil(replicaSelector)
	s.Nil(err)
	state3 = replicaSelector.state.(*accessFollower)
	// Should fallback to the leader immediately.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(err)
	s.Equal(regionStore.workTiKVIdx, state3.lastIdx)
	s.Equal(replicaSelector.targetIdx, state3.lastIdx)
	AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[regionStore.workTiKVIdx], nil)

	// Test accessFollower state filtering label-not-match stores.
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	refreshEpochs(regionStore)
	labels := []*metapb.StoreLabel{
		{
			Key:   "a",
			Value: "b",
		},
	}
	regionStore.workTiKVIdx = AccessIndex(0)
	accessIdx := AccessIndex(regionStore.accessStoreNum(tiKVOnly) - 1)
	_, store := regionStore.accessStore(tiKVOnly, accessIdx)
	store.labels = labels
	for i := 0; i < 5; i++ {
		replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req, WithMatchLabels(labels))
		s.NotNil(replicaSelector)
		s.Nil(err)
		rpcCtx, err = replicaSelector.next(s.bo)
		s.Nil(err)
		AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[accessIdx], nil)
	}

	// Test accessFollower state with leaderOnly option
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	refreshEpochs(regionStore)
	for i := 0; i < 5; i++ {
		replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req, WithLeaderOnly())
		s.NotNil(replicaSelector)
		s.Nil(err)
		rpcCtx, err = replicaSelector.next(s.bo)
		s.Nil(err)
		// Should always access the leader.
		AssertRPCCtxEqual(s, rpcCtx, replicaSelector.replicas[regionStore.workTiKVIdx], nil)
	}

	// Test accessFollower state with kv.ReplicaReadMixed request type.
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	refreshEpochs(regionStore)
	req.ReplicaReadType = kv.ReplicaReadMixed
	replicaSelector, err = newReplicaSelector(cache, regionLoc.Region, req)
	s.NotNil(replicaSelector)
	s.Nil(err)

	// Invalidate the region if the leader is not in the region.
	atomic.StoreInt64(&region.lastAccess, time.Now().Unix())
	replicaSelector.updateLeader(&metapb.Peer{Id: s.cluster.AllocID(), StoreId: s.cluster.AllocID()})
	s.False(region.isValid())
	// Don't try next replica if the region is invalidated.
	rpcCtx, err = replicaSelector.next(s.bo)
	s.Nil(rpcCtx)
	s.Nil(err)
}

// TODO(youjiali1995): Remove duplicated tests. This test may be duplicated with other
// tests but it's a dedicated one to test sending requests with the replica selector.
func (s *testRegionRequestToThreeStoresSuite) TestSendReqWithReplicaSelector() {
	req := tikvrpc.NewRequest(tikvrpc.CmdRawPut, &kvrpcpb.RawPutRequest{
		Key:   []byte("key"),
		Value: []byte("value"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(region)
	regionStore := s.cache.GetCachedRegionWithRLock(region.Region).getStore()
	s.NotNil(regionStore)

	reloadRegion := func() {
		s.regionRequestSender.replicaSelector.region.invalidate(Other)
		region, _ = s.cache.LocateRegionByID(s.bo, s.regionID)
		regionStore = s.cache.GetCachedRegionWithRLock(region.Region).getStore()
	}

	hasFakeRegionError := func(resp *tikvrpc.Response) bool {
		if resp == nil {
			return false
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return false
		}
		return IsFakeRegionError(regionErr)
	}

	// Normal
	bo := retry.NewBackoffer(context.Background(), -1)
	sender := s.regionRequestSender
	resp, _, err := sender.SendReq(bo, req, region.Region, client.ReadTimeoutShort)
	s.Nil(err)
	s.NotNil(resp)
	s.True(bo.GetTotalBackoffTimes() == 0)

	// Switch to the next Peer due to store failure and the leader is on the next peer.
	bo = retry.NewBackoffer(context.Background(), -1)
	s.cluster.ChangeLeader(s.regionID, s.peerIDs[1])
	s.cluster.StopStore(s.storeIDs[0])
	resp, _, err = sender.SendReq(bo, req, region.Region, client.ReadTimeoutShort)
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(sender.replicaSelector.targetIdx, AccessIndex(1))
	s.True(bo.GetTotalBackoffTimes() == 1)
	s.cluster.StartStore(s.storeIDs[0])
	atomic.StoreUint32(&regionStore.stores[0].livenessState, uint32(reachable))

	// Leader is updated because of send success, so no backoff.
	reloadRegion()
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, _, err = sender.SendReq(bo, req, region.Region, client.ReadTimeoutShort)
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(sender.replicaSelector.targetIdx, AccessIndex(1))
	s.True(bo.GetTotalBackoffTimes() == 0)

	// Switch to the next peer due to leader failure but the new leader is not elected.
	// Region will be invalidated due to store epoch changed.
	reloadRegion()
	s.cluster.StopStore(s.storeIDs[1])
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, _, err = sender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.True(hasFakeRegionError(resp))
	s.Equal(bo.GetTotalBackoffTimes(), 1)
	s.cluster.StartStore(s.storeIDs[1])
	atomic.StoreUint32(&regionStore.stores[1].livenessState, uint32(reachable))

	// Leader is changed. No backoff.
	reloadRegion()
	s.cluster.ChangeLeader(s.regionID, s.peerIDs[0])
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, _, err = sender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(bo.GetTotalBackoffTimes(), 0)

	// No leader. Backoff for each replica and runs out all replicas.
	s.cluster.GiveUpLeader(s.regionID)
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, _, err = sender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.True(hasFakeRegionError(resp))
	s.Equal(bo.GetTotalBackoffTimes(), 3)
	s.False(sender.replicaSelector.region.isValid())
	s.cluster.ChangeLeader(s.regionID, s.peerIDs[0])

	// The leader store is alive but can't provide service.

	tf := func(s *Store, bo *retry.Backoffer) livenessState {
		return reachable
	}
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(&tf))
	s.Eventually(func() bool {
		stores := s.regionRequestSender.replicaSelector.regionStore.stores
		return stores[0].getLivenessState() == reachable &&
			stores[1].getLivenessState() == reachable &&
			stores[2].getLivenessState() == reachable
	}, 3*time.Second, 200*time.Millisecond)
	// Region will be invalidated due to running out of all replicas.
	reloadRegion()
	s.cluster.StopStore(s.storeIDs[0])
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, _, err = sender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.True(hasFakeRegionError(resp))
	s.False(sender.replicaSelector.region.isValid())
	s.Equal(bo.GetTotalBackoffTimes(), maxReplicaAttempt+2)
	s.cluster.StartStore(s.storeIDs[0])

	// Verify that retry the same replica when meets ServerIsBusy/MaxTimestampNotSynced/ReadIndexNotReady/ProposalInMergingMode.
	for _, regionErr := range []*errorpb.Error{
		// ServerIsBusy takes too much time to test.
		// {ServerIsBusy: &errorpb.ServerIsBusy{}},
		{MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{}},
		{ReadIndexNotReady: &errorpb.ReadIndexNotReady{}},
		{ProposalInMergingMode: &errorpb.ProposalInMergingMode{}},
	} {
		func() {
			oc := sender.client
			defer func() {
				sender.client = oc
			}()
			s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
				// Return the specific region error when accesses the leader.
				if addr == s.cluster.GetStore(s.storeIDs[0]).Address {
					return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: regionErr}}, nil
				}
				// Return the not leader error when accesses followers.
				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{
					NotLeader: &errorpb.NotLeader{
						RegionId: region.Region.id, Leader: &metapb.Peer{Id: s.peerIDs[0], StoreId: s.storeIDs[0]},
					}}}}, nil

			}}
			reloadRegion()
			bo = retry.NewBackoffer(context.Background(), -1)
			resp, _, err := sender.SendReq(bo, req, region.Region, time.Second)
			s.Nil(err)
			s.True(hasFakeRegionError(resp))
			s.False(sender.replicaSelector.region.isValid())
			s.Equal(bo.GetTotalBackoffTimes(), maxReplicaAttempt+2)
		}()
	}

	// Verify switch to the next peer immediately when meets StaleCommand.
	reloadRegion()
	func() {
		oc := sender.client
		defer func() {
			sender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{StaleCommand: &errorpb.StaleCommand{}}}}, nil
		}}
		reloadRegion()
		bo = retry.NewBackoffer(context.Background(), -1)
		resp, _, err := sender.SendReq(bo, req, region.Region, time.Second)
		s.Nil(err)
		s.True(hasFakeRegionError(resp))
		s.False(sender.replicaSelector.region.isValid())
		s.Equal(bo.GetTotalBackoffTimes(), 0)
	}()

	// Verify don't invalidate region when meets unknown region errors.
	reloadRegion()
	func() {
		oc := sender.client
		defer func() {
			sender.client = oc
		}()
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: &errorpb.Error{Message: ""}}}, nil
		}}
		reloadRegion()
		bo = retry.NewBackoffer(context.Background(), -1)
		resp, _, err := sender.SendReq(bo, req, region.Region, time.Second)
		s.Nil(err)
		s.True(hasFakeRegionError(resp))
		s.False(sender.replicaSelector.region.isValid())
		s.Equal(bo.GetTotalBackoffTimes(), 0)
	}()

	// Verify invalidate region when meets StoreNotMatch/RegionNotFound/EpochNotMatch/NotLeader and can't find the leader in region.
	for i, regionErr := range []*errorpb.Error{
		{StoreNotMatch: &errorpb.StoreNotMatch{}},
		{RegionNotFound: &errorpb.RegionNotFound{}},
		{EpochNotMatch: &errorpb.EpochNotMatch{}},
		{NotLeader: &errorpb.NotLeader{Leader: &metapb.Peer{}}}} {
		func() {
			oc := sender.client
			defer func() {
				sender.client = oc
			}()
			s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
				return &tikvrpc.Response{Resp: &kvrpcpb.RawPutResponse{RegionError: regionErr}}, nil

			}}
			reloadRegion()
			bo = retry.NewBackoffer(context.Background(), -1)
			resp, _, err := sender.SendReq(bo, req, region.Region, time.Second)

			// Return a sendError when meets NotLeader and can't find the leader in the region.
			if i == 3 {
				s.Nil(err)
				s.True(hasFakeRegionError(resp))
			} else {
				s.Nil(err)
				s.NotNil(resp)
				regionErr, _ := resp.GetRegionError()
				s.NotNil(regionErr)
			}
			s.False(sender.replicaSelector.region.isValid())
			s.Equal(bo.GetTotalBackoffTimes(), 0)
		}()
	}

	// Runs out of all replicas and then returns a send error.
	tf = func(s *Store, bo *retry.Backoffer) livenessState {
		return unreachable
	}
	s.regionRequestSender.regionCache.testingKnobs.mockRequestLiveness.Store((*livenessFunc)(&tf))
	reloadRegion()
	for _, store := range s.storeIDs {
		s.cluster.StopStore(store)
	}
	bo = retry.NewBackoffer(context.Background(), -1)
	resp, _, err = sender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.True(hasFakeRegionError(resp))
	s.True(bo.GetTotalBackoffTimes() == 3)
	s.False(sender.replicaSelector.region.isValid())
	for _, store := range s.storeIDs {
		s.cluster.StartStore(store)
	}

	// Verify switch to the leader immediately when stale read requests with global txn scope meet region errors.
	s.cluster.ChangeLeader(region.Region.id, s.peerIDs[0])
	reloadRegion()
	req = tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
	req.ReadReplicaScope = oracle.GlobalTxnScope
	req.TxnScope = oracle.GlobalTxnScope
	for i := 0; i < 10; i++ {
		req.EnableStaleRead()
		// The request may be sent to the leader directly. We have to distinguish it.
		failureOnFollower := 0
		failureOnLeader := 0
		s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			if addr != s.cluster.GetStore(s.storeIDs[0]).Address {
				failureOnFollower++
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{}}}, nil
			} else if failureOnLeader == 0 && i%2 == 0 {
				failureOnLeader++
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{}}}, nil
			} else {
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{}}, nil
			}
		}}
		sender.SendReq(bo, req, region.Region, time.Second)
		state, ok := sender.replicaSelector.state.(*accessFollower)
		s.True(ok)
		s.True(failureOnFollower <= 1) // any retry should go to the leader, hence at most one failure on the follower allowed
		if failureOnFollower == 0 && failureOnLeader == 0 {
			// if the request goes to the leader and succeeds then it is executed as a StaleRead
			s.True(req.StaleRead)
		} else {
			// otherwise #leaderOnly flag should be set and retry request as a normal read
			s.True(state.option.leaderOnly)
			s.False(req.StaleRead)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestLoadBasedReplicaRead() {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)
	region := s.cache.GetCachedRegionWithRLock(regionLoc.Region)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{
		BusyThresholdMs: 50,
	})

	replicaSelector, err := newReplicaSelector(s.cache, regionLoc.Region, req)
	s.NotNil(replicaSelector)
	s.Nil(err)
	s.Equal(replicaSelector.region, region)
	s.IsType(&accessKnownLeader{}, replicaSelector.state)
	// The busyThreshold in replicaSelector should be initialized with the request context.
	s.Equal(replicaSelector.busyThreshold, 50*time.Millisecond)

	bo := retry.NewBackoffer(context.Background(), -1)
	rpcCtx, err := replicaSelector.next(bo)
	s.Nil(err)
	s.Equal(rpcCtx.Peer.Id, s.leaderPeer)

	// Receive a ServerIsBusy error
	replicaSelector.onServerIsBusy(bo, rpcCtx, req, &errorpb.ServerIsBusy{
		EstimatedWaitMs: 500,
	})

	rpcCtx, err = replicaSelector.next(bo)
	s.Nil(err)
	s.NotEqual(rpcCtx.Peer.Id, s.leaderPeer)
	s.IsType(&tryIdleReplica{}, replicaSelector.state)
	s.True(*rpcCtx.contextPatcher.replicaRead)
	lastPeerID := rpcCtx.Peer.Id

	replicaSelector.onServerIsBusy(bo, rpcCtx, req, &errorpb.ServerIsBusy{
		EstimatedWaitMs: 800,
	})

	rpcCtx, err = replicaSelector.next(bo)
	s.Nil(err)
	// Should choose a peer different from before
	s.NotEqual(rpcCtx.Peer.Id, s.leaderPeer)
	s.NotEqual(rpcCtx.Peer.Id, lastPeerID)
	s.IsType(&tryIdleReplica{}, replicaSelector.state)
	s.True(*rpcCtx.contextPatcher.replicaRead)

	// All peers are too busy
	replicaSelector.onServerIsBusy(bo, rpcCtx, req, &errorpb.ServerIsBusy{
		EstimatedWaitMs: 150,
	})
	lessBusyPeer := rpcCtx.Peer.Id

	// Then, send to the leader again with no threshold.
	rpcCtx, err = replicaSelector.next(bo)
	s.Nil(err)
	s.Equal(rpcCtx.Peer.Id, s.leaderPeer)
	s.IsType(&tryIdleReplica{}, replicaSelector.state)
	s.False(*rpcCtx.contextPatcher.replicaRead)
	s.Equal(*rpcCtx.contextPatcher.busyThreshold, time.Duration(0))

	time.Sleep(120 * time.Millisecond)

	// When there comes a new request, it should skip busy leader and choose a less busy store
	replicaSelector, err = newReplicaSelector(s.cache, regionLoc.Region, req)
	s.NotNil(replicaSelector)
	s.Nil(err)
	rpcCtx, err = replicaSelector.next(bo)
	s.Nil(err)
	s.Equal(rpcCtx.Peer.Id, lessBusyPeer)
	s.IsType(&tryIdleReplica{}, replicaSelector.state)
	s.True(*rpcCtx.contextPatcher.replicaRead)
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaReadWithFlashbackInProgress() {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)

	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		// Return serverIsBusy when accesses the leader with busy threshold.
		if addr == s.cluster.GetStore(s.storeIDs[0]).Address {
			if req.BusyThresholdMs > 0 {
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
					ServerIsBusy: &errorpb.ServerIsBusy{EstimatedWaitMs: 500},
				}}}, nil
			} else {
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte("value")}}, nil
			}
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
			FlashbackInProgress: &errorpb.FlashbackInProgress{
				RegionId: regionLoc.Region.GetID(),
			},
		}}}, nil
	}}

	reqs := []*tikvrpc.Request{
		tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, kvrpcpb.Context{
			BusyThresholdMs: 50,
		}),
		tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, kv.ReplicaReadFollower, nil),
	}

	for _, req := range reqs {
		bo := retry.NewBackoffer(context.Background(), -1)
		s.Nil(err)
		resp, retry, err := s.regionRequestSender.SendReq(bo, req, regionLoc.Region, time.Second)
		s.Nil(err)
		s.GreaterOrEqual(retry, 1)
		s.Equal(resp.Resp.(*kvrpcpb.GetResponse).Value, []byte("value"))
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestAccessFollowerAfter1TiKVDown() {
	var leaderAddr string
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		// Returns error when accesses non-leader.
		if leaderAddr != addr {
			return nil, context.DeadlineExceeded
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			Value: []byte("value"),
		}}, nil
	}}

	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key: []byte("key"),
	})
	req.ReplicaReadType = kv.ReplicaReadMixed

	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	region := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(region)
	regionStore := region.getStore()
	leaderAddr = regionStore.stores[regionStore.workTiKVIdx].addr
	s.NotEqual(leaderAddr, "")
	for i := 0; i < 10; i++ {
		bo := retry.NewBackofferWithVars(context.Background(), 100, nil)
		resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, loc.Region, client.ReadTimeoutShort, tikvrpc.TiKV)
		s.Nil(err)
		s.NotNil(resp)

		// Since send req to follower will receive error, then all follower will be marked as unreachable and epoch stale.
		allFollowerStoreEpochStale := true
		for i, store := range regionStore.stores {
			if i == int(regionStore.workTiKVIdx) {
				continue
			}
			if store.epoch == regionStore.storeEpochs[i] {
				allFollowerStoreEpochStale = false
				break
			} else {
				s.Equal(store.getLivenessState(), unreachable)
			}
		}
		if allFollowerStoreEpochStale {
			break
		}
	}

	// mock for GC leader reload all regions.
	bo := retry.NewBackofferWithVars(context.Background(), 10, nil)
	_, err = s.cache.BatchLoadRegionsWithKeyRange(bo, []byte(""), nil, 1)
	s.Nil(err)

	loc, err = s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	region = s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(region)
	regionStore = region.getStore()
	for i, store := range regionStore.stores {
		if i == int(regionStore.workTiKVIdx) {
			continue
		}
		// After reload region, the region epoch will be updated, but the store liveness state is still unreachable.
		s.Equal(store.epoch, regionStore.storeEpochs[i])
		s.Equal(store.getLivenessState(), unreachable)
	}

	for i := 0; i < 100; i++ {
		bo := retry.NewBackofferWithVars(context.Background(), 1, nil)
		resp, _, retryTimes, err := s.regionRequestSender.SendReqCtx(bo, req, loc.Region, client.ReadTimeoutShort, tikvrpc.TiKV)
		s.Nil(err)
		s.NotNil(resp)
		// since all follower'store is unreachable, the request will be sent to leader, the backoff times should be 0.
		s.Equal(0, bo.GetTotalBackoffTimes())
		s.Equal(0, retryTimes)
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestStaleReadFallback() {
	leaderStore, _ := s.loadAndGetLeaderStore()
	leaderLabel := []*metapb.StoreLabel{
		{
			Key:   "id",
			Value: strconv.FormatUint(leaderStore.StoreID(), 10),
		},
	}
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)
	value := []byte("value")
	isFirstReq := true

	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		select {
		case <-ctx.Done():
			return nil, errors.New("timeout")
		default:
		}
		// Return `DataIsNotReady` for the first time on leader.
		if isFirstReq {
			isFirstReq = false
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
				DataIsNotReady: &errorpb.DataIsNotReady{},
			}}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: value}}, nil
	}}

	region := s.cache.getRegionByIDFromCache(regionLoc.Region.GetID())
	s.True(region.isValid())

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, kv.ReplicaReadLeader, nil)
	req.ReadReplicaScope = oracle.GlobalTxnScope
	req.TxnScope = oracle.GlobalTxnScope
	req.EnableStaleRead()
	req.ReplicaReadType = kv.ReplicaReadMixed
	var ops []StoreSelectorOption
	ops = append(ops, WithMatchLabels(leaderLabel))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bo := retry.NewBackoffer(ctx, -1)
	s.Nil(err)
	resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, regionLoc.Region, time.Second, tikvrpc.TiKV, ops...)
	s.Nil(err)

	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Nil(regionErr)
	getResp, ok := resp.Resp.(*kvrpcpb.GetResponse)
	s.True(ok)
	s.Equal(getResp.Value, value)
}

func (s *testRegionRequestToThreeStoresSuite) TestSendReqFirstTimeout() {
	leaderAddr := ""
	reqTargetAddrs := make(map[string]struct{})
	s.regionRequestSender.RegionRequestRuntimeStats = NewRegionRequestRuntimeStats()
	bo := retry.NewBackoffer(context.Background(), 10000)
	mockRPCClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		reqTargetAddrs[addr] = struct{}{}
		if req.Context.MaxExecutionDurationMs < 10 {
			return nil, context.DeadlineExceeded
		}
		if addr != leaderAddr && !req.Context.ReplicaRead && !req.Context.StaleRead {
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte("value")}}, nil
	}}
	getLocFn := func() *KeyLocation {
		loc, err := s.regionRequestSender.regionCache.LocateKey(bo, []byte("a"))
		s.Nil(err)
		region := s.regionRequestSender.regionCache.GetCachedRegionWithRLock(loc.Region)
		leaderStore, _, _, _ := region.WorkStorePeer(region.getStore())
		leaderAddr, err = s.regionRequestSender.regionCache.getStoreAddr(s.bo, region, leaderStore)
		s.Nil(err)
		return loc
	}
	resetStats := func() {
		reqTargetAddrs = make(map[string]struct{})
		s.regionRequestSender = NewRegionRequestSender(s.cache, mockRPCClient)
		s.regionRequestSender.RegionRequestRuntimeStats = NewRegionRequestRuntimeStats()
	}

	//Test different read type.
	staleReadTypes := []bool{false, true}
	replicaReadTypes := []kv.ReplicaReadType{kv.ReplicaReadLeader, kv.ReplicaReadFollower, kv.ReplicaReadMixed}
	for _, staleRead := range staleReadTypes {
		for _, tp := range replicaReadTypes {
			log.Info("TestSendReqFirstTimeout", zap.Bool("stale-read", staleRead), zap.String("replica-read-type", tp.String()))
			resetStats()
			req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kvrpcpb.Context{})
			if staleRead {
				req.EnableStaleRead()
			} else {
				req.ReplicaRead = tp.IsFollowerRead()
				req.ReplicaReadType = tp
			}
			loc := getLocFn()
			resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Millisecond, tikvrpc.TiKV)
			s.Nil(err)
			regionErr, err := resp.GetRegionError()
			s.Nil(err)
			s.True(IsFakeRegionError(regionErr))
			s.Equal(1, len(s.regionRequestSender.Stats))
			if staleRead {
				rpcNum := s.regionRequestSender.Stats[tikvrpc.CmdGet].Count
				s.True(rpcNum == 1 || rpcNum == 2) // 1 rpc or 2 rpc
			} else {
				s.Equal(int64(3), s.regionRequestSender.Stats[tikvrpc.CmdGet].Count) // 3 rpc
				s.Equal(3, len(reqTargetAddrs))                                      // each rpc to a different store.
			}
			s.Equal(0, bo.GetTotalBackoffTimes()) // no backoff since fast retry.
			// warn: must rest MaxExecutionDurationMs before retry.
			resetStats()
			if staleRead {
				req.EnableStaleRead()
			} else {
				req.ReplicaRead = tp.IsFollowerRead()
				req.ReplicaReadType = tp
			}
			req.Context.MaxExecutionDurationMs = 0
			resp, _, _, err = s.regionRequestSender.SendReqCtx(bo, req, loc.Region, time.Second, tikvrpc.TiKV)
			s.Nil(err)
			regionErr, err = resp.GetRegionError()
			s.Nil(err)
			s.Nil(regionErr)
			s.Equal([]byte("value"), resp.Resp.(*kvrpcpb.GetResponse).Value)
			s.Equal(1, len(s.regionRequestSender.Stats))
			s.Equal(int64(1), s.regionRequestSender.Stats[tikvrpc.CmdGet].Count) // 1 rpc
			s.Equal(0, bo.GetTotalBackoffTimes())                                // no backoff since fast retry.
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestStaleReadFallback2Follower() {
	leaderStore, _ := s.loadAndGetLeaderStore()
	leaderLabel := []*metapb.StoreLabel{
		{
			Key:   "id",
			Value: strconv.FormatUint(leaderStore.StoreID(), 10),
		},
	}
	var followerID *uint64
	for _, storeID := range s.storeIDs {
		if storeID != leaderStore.storeID {
			id := storeID
			followerID = &id
			break
		}
	}
	s.NotNil(followerID)
	followerLabel := []*metapb.StoreLabel{
		{
			Key:   "id",
			Value: strconv.FormatUint(*followerID, 10),
		},
	}

	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)

	dataIsNotReady := false
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		select {
		case <-ctx.Done():
			return nil, errors.New("timeout")
		default:
		}
		if dataIsNotReady && req.StaleRead {
			dataIsNotReady = false
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
				DataIsNotReady: &errorpb.DataIsNotReady{},
			}}}, nil
		}
		if addr == leaderStore.addr {
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
				ServerIsBusy: &errorpb.ServerIsBusy{},
			}}}, nil
		}
		if !req.ReplicaRead {
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
				NotLeader: &errorpb.NotLeader{},
			}}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte(addr)}}, nil
	}}

	for _, localLeader := range []bool{true, false} {
		dataIsNotReady = true
		// data is not ready, then server is busy in the first round,
		// directly server is busy in the second round.
		for i := 0; i < 2; i++ {
			req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, kv.ReplicaReadLeader, nil)
			req.ReadReplicaScope = oracle.GlobalTxnScope
			req.TxnScope = oracle.GlobalTxnScope
			req.EnableStaleRead()
			req.ReplicaReadType = kv.ReplicaReadMixed
			var ops []StoreSelectorOption
			if localLeader {
				ops = append(ops, WithMatchLabels(leaderLabel))
			} else {
				ops = append(ops, WithMatchLabels(followerLabel))
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10000*time.Second)
			bo := retry.NewBackoffer(ctx, -1)
			s.Nil(err)
			resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, regionLoc.Region, time.Second, tikvrpc.TiKV, ops...)
			s.Nil(err)

			regionErr, err := resp.GetRegionError()
			s.Nil(err)
			s.Nil(regionErr)
			getResp, ok := resp.Resp.(*kvrpcpb.GetResponse)
			s.True(ok)
			if localLeader {
				s.NotEqual(getResp.Value, []byte("store"+leaderLabel[0].Value))
			} else {
				s.Equal(getResp.Value, []byte("store"+followerLabel[0].Value))
			}
			cancel()
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaReadFallbackToLeaderRegionError() {
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(regionLoc)

	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		select {
		case <-ctx.Done():
			return nil, errors.New("timeout")
		default:
		}
		// Return `mismatch peer id` when accesses the leader.
		if addr == s.cluster.GetStore(s.storeIDs[0]).Address {
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
				MismatchPeerId: &errorpb.MismatchPeerId{
					RequestPeerId: 1,
					StorePeerId:   2,
				},
			}}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
			DataIsNotReady: &errorpb.DataIsNotReady{},
		}}}, nil
	}}

	region := s.cache.getRegionByIDFromCache(regionLoc.Region.GetID())
	s.True(region.isValid())

	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, kv.ReplicaReadLeader, nil)
	req.ReadReplicaScope = oracle.GlobalTxnScope
	req.TxnScope = oracle.GlobalTxnScope
	req.EnableStaleRead()
	req.ReplicaReadType = kv.ReplicaReadFollower

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bo := retry.NewBackoffer(ctx, -1)
	s.Nil(err)
	resp, _, _, err := s.regionRequestSender.SendReqCtx(bo, req, regionLoc.Region, time.Second, tikvrpc.TiKV)
	s.Nil(err)
	regionErr, err := resp.GetRegionError()
	s.Nil(err)
	s.Equal(regionErrorToLabel(regionErr), "mismatch_peer_id")
	// return non-epoch-not-match region error and the upper layer can auto retry.
	s.Nil(regionErr.GetEpochNotMatch())
	// after region error returned, the region should be invalidated.
	s.False(region.isValid())
}

func (s *testRegionRequestToThreeStoresSuite) TestLogging() {
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key: []byte("key"),
	})
	region, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	s.NotNil(region)

	oc := s.regionRequestSender.client
	defer func() {
		s.regionRequestSender.client = oc
	}()

	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		response = &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}},
		}}
		return response, nil
	}}

	bo := retry.NewBackofferWithVars(context.Background(), 5, nil)
	resp, _, err := s.regionRequestSender.SendReq(bo, req, region.Region, time.Second)
	s.Nil(err)
	s.NotNil(resp)
	regionErr, _ := resp.GetRegionError()
	s.NotNil(regionErr)
}

func (s *testRegionRequestToThreeStoresSuite) TestRetryRequestSource() {
	leaderStore, _ := s.loadAndGetLeaderStore()
	regionLoc, err := s.cache.LocateRegionByID(s.bo, s.regionID)
	s.Nil(err)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key: []byte("key"),
	})
	req.InputRequestSource = "test"

	setReadType := func(req *tikvrpc.Request, readType string) {
		req.StaleRead = false
		req.ReplicaRead = false
		switch readType {
		case "leader":
			return
		case "follower":
			req.ReplicaRead = true
			req.ReplicaReadType = kv.ReplicaReadFollower
		case "stale_follower", "stale_leader":
			req.EnableStaleRead()
		default:
			panic("unreachable")
		}
	}

	setTargetReplica := func(selector *replicaSelector, readType string) {
		var leader bool
		switch readType {
		case "leader", "stale_leader":
			leader = true
		case "follower", "stale_follower":
			leader = false
		default:
			panic("unreachable")
		}
		for idx, replica := range selector.replicas {
			if replica.store.storeID == leaderStore.storeID && leader {
				selector.targetIdx = AccessIndex(idx)
				return
			}
			if replica.store.storeID != leaderStore.storeID && !leader {
				selector.targetIdx = AccessIndex(idx)
				return
			}
		}
		panic("unreachable")
	}

	firstReadReplicas := []string{"leader", "follower", "stale_follower", "stale_leader"}
	retryReadReplicas := []string{"leader", "follower"}
	for _, firstReplica := range firstReadReplicas {
		for _, retryReplica := range retryReadReplicas {
			bo := retry.NewBackoffer(context.Background(), -1)
			req.IsRetryRequest = false
			setReadType(req, firstReplica)
			replicaSelector, err := newReplicaSelector(s.cache, regionLoc.Region, req)
			s.Nil(err)
			setTargetReplica(replicaSelector, firstReplica)
			rpcCtx, err := replicaSelector.buildRPCContext(bo)
			s.Nil(err)
			replicaSelector.patchRequestSource(req, rpcCtx)
			s.Equal("test-"+firstReplica, req.RequestSource)

			// retry
			setReadType(req, retryReplica)
			replicaSelector, err = newReplicaSelector(s.cache, regionLoc.Region, req)
			s.Nil(err)
			setTargetReplica(replicaSelector, retryReplica)
			rpcCtx, err = replicaSelector.buildRPCContext(bo)
			s.Nil(err)
			req.IsRetryRequest = true
			replicaSelector.patchRequestSource(req, rpcCtx)
			s.Equal("test-retry_"+firstReplica+"_"+retryReplica, req.RequestSource)
		}
	}
}
