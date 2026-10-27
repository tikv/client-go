// Copyright 2026 TiKV Authors
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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

// witnessRoutingPDClient fails metadata requests after the test disables PD.
// Cached data-peer retries must not need either a Region or a Store lookup.
type witnessRoutingPDClient struct {
	pd.Client
	unavailable atomic.Bool
	failedCalls atomic.Int32
}

func (c *witnessRoutingPDClient) WithCallerComponent(caller.Component) pd.Client { return c }

func (c *witnessRoutingPDClient) checkAvailable() error {
	if c.unavailable.Load() {
		c.failedCalls.Add(1)
		return errors.New("PD unavailable")
	}
	return nil
}

func (c *witnessRoutingPDClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	if err := c.checkAvailable(); err != nil {
		return nil, err
	}
	return c.Client.GetRegion(ctx, key, opts...)
}

func (c *witnessRoutingPDClient) GetRegionByID(ctx context.Context, id uint64, opts ...opt.GetRegionOption) (*router.Region, error) {
	if err := c.checkAvailable(); err != nil {
		return nil, err
	}
	return c.Client.GetRegionByID(ctx, id, opts...)
}

func (c *witnessRoutingPDClient) GetStore(ctx context.Context, id uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
	if err := c.checkAvailable(); err != nil {
		return nil, err
	}
	return c.Client.GetStore(ctx, id, opts...)
}

func installWitnessLeader(s *testReplicaSelectorSuite, unresolvedWitness bool) (*Region, *metapb.Peer) {
	meta, _, _, _ := s.cluster.GetRegionByID(s.regionID)
	witness := meta.Peers[0]
	witness.IsWitness = true
	if unresolvedWitness {
		// No such Store exists in PD: resolving the witness would fail this test.
		witness.StoreId = s.cluster.AllocID()
	}
	r, err := newRegion(s.bo, s.cache, &router.Region{Meta: meta, Leader: witness})
	s.Require().NoError(err)
	s.cache.insertRegionToCache(r, true, true)
	return r, witness
}

func TestWitnessLeaderNeverReceivesDataRequests(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	r, witness := installWitnessLeader(s, true)
	s.Len(r.meta.Peers, 2)
	s.Equal(s.peerIDs[1], r.GetLeaderPeerID())
	_, resolved := s.cache.stores.get(witness.StoreId)
	s.False(resolved, "witness store must be filtered before resolution")

	for _, readType := range []kv.ReplicaReadType{
		kv.ReplicaReadLeader, kv.ReplicaReadFollower, kv.ReplicaReadMixed,
		kv.ReplicaReadLearner, kv.ReplicaReadPreferLeader,
	} {
		s.Run(readType.String(), func() {
			for seed := uint32(0); seed < 3; seed++ {
				ctx, err := s.cache.GetTiKVRPCContext(s.bo, r.VerID(), readType, seed)
				s.Require().NoError(err)
				s.Require().NotNil(ctx)
				s.False(ctx.Peer.IsWitness)
				s.NotEqual(witness.Id, ctx.Peer.Id)
			}
			req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")}, readType, nil)
			assertWitnessFreeRequest(s, r, req)
		})
	}
	staleReq := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
	staleReq.EnableStaleWithMixedReplicaRead()
	assertWitnessFreeRequest(s, r, staleReq)
	assertWitnessFreeRequest(s, r, tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{}))
}

func assertWitnessFreeRequest(s *testReplicaSelectorSuite, r *Region, req *tikvrpc.Request) {
	calls := 0
	rpcClient := &fnClient{fn: func(_ context.Context, addr string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		calls++
		s.False(req.Context.Peer.IsWitness)
		s.NotEqual(s.peerIDs[0], req.Context.Peer.Id)
		s.Contains([]string{"store2", "store3"}, addr)
		if req.Type == tikvrpc.CmdPrewrite {
			return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte("value")}}, nil
	}}
	sender := NewRegionRequestSender(s.cache, rpcClient, oracle.NoopReadTSValidator{})
	resp, _, _, err := sender.SendReqCtx(s.bo, req, r.VerID(), client.ReadTimeoutShort, tikvrpc.TiKV)
	s.NoError(err)
	s.Require().NotNil(resp)
	regionErr, err := resp.GetRegionError()
	s.NoError(err)
	s.Nil(regionErr)
	s.Equal(1, calls)
}

func TestWitnessLeaderHintsRetryDataPeerWithoutPD(t *testing.T) {
	for _, expires := range []bool{false, true} {
		name := "full-leader-recovers"
		if expires {
			name = "backoff-budget-expires"
		}
		t.Run(name, func(t *testing.T) {
			s := new(testReplicaSelectorSuite)
			s.SetupTest(t)
			defer s.TearDownTest()

			pdClient := &witnessRoutingPDClient{Client: s.cache.pdClient}
			s.cache.Close()
			s.cache = NewRegionCache(pdClient, RegionCacheNoHealthTick)
			r, witness := installWitnessLeader(s, false)
			pdClient.unavailable.Store(true)

			calls := 0
			fullLeader := r.meta.Peers[1]
			rpcClient := &fnClient{fn: func(_ context.Context, addr string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
				calls++
				s.False(req.Context.Peer.IsWitness)
				s.NotEqual(witness.Id, req.Context.Peer.Id)
				s.Less(calls, 10, "witness hints must be bounded by scheduling backoff")
				if calls <= 3 || expires {
					s.Equal("store2", addr)
					return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
						NotLeader: &errorpb.NotLeader{RegionId: r.GetID(), Leader: witness},
					}}}, nil
				}
				if calls == 4 {
					s.Equal("store2", addr)
					return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{RegionError: &errorpb.Error{
						NotLeader: &errorpb.NotLeader{RegionId: r.GetID(), Leader: fullLeader},
					}}}, nil
				}
				s.Equal("store3", addr)
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte("recovered")}}, nil
			}}
			sender := NewRegionRequestSender(s.cache, rpcClient, oracle.NoopReadTSValidator{})
			budget := 40000
			if expires {
				budget = 1
			}
			bo := retry.NewBackoffer(context.Background(), budget)
			req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
			resp, _, _, err := sender.SendReqCtx(bo, req, r.VerID(), client.ReadTimeoutShort, tikvrpc.TiKV)
			if expires {
				s.Error(err)
				s.Nil(resp)
				s.Equal(2, calls)
				s.Equal(1, bo.GetBackoffTimes()["regionScheduling"])
			} else {
				s.NoError(err)
				s.Require().NotNil(resp)
				s.Equal([]byte("recovered"), resp.Resp.(*kvrpcpb.GetResponse).Value)
				s.Equal(5, calls)
				s.Equal(3, bo.GetBackoffTimes()["regionScheduling"])
				s.Equal(fullLeader.Id, r.GetLeaderPeerID())
			}
			s.True(r.isValid())
			s.Zero(pdClient.failedCalls.Load(), "recovery retries must use cached Full peers")
		})
	}
}

func TestWitnessLeaderHintDoesNotResetOtherRetryBudgets(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	r, witness := installWitnessLeader(s, false)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	selector, err := newReplicaSelector(s.cache, r.VerID(), req)
	s.Require().NoError(err)
	ctx, err := selector.next(s.bo, req)
	s.Require().NoError(err)
	s.Require().NotNil(ctx)
	target := selector.target
	target.attempts = 4
	target.attemptedTime = time.Second
	target.addFlag(serverIsBusyFlag)
	bo := retry.NewBackoffer(context.Background(), 40000)
	shouldRetry, err := selector.onNotLeader(bo, ctx, &errorpb.NotLeader{Leader: witness})
	s.NoError(err)
	s.True(shouldRetry)
	s.Equal(3, target.attempts)
	s.Equal(time.Second, target.attemptedTime)
	s.True(target.hasFlag(serverIsBusyFlag))
	s.False(target.hasFlag(notLeaderFlag))
	s.Equal(1, bo.GetBackoffTimes()["regionScheduling"])

	// A previous NotLeader result must not be cleared by the recovery retry.
	target.addFlag(notLeaderFlag)
	target.attempts++
	shouldRetry, err = selector.onNotLeader(bo, ctx, &errorpb.NotLeader{Leader: witness})
	s.NoError(err)
	s.True(shouldRetry)
	s.Equal(3, target.attempts)
	s.Equal(time.Second, target.attemptedTime)
	s.True(target.hasFlag(serverIsBusyFlag))
	s.True(target.hasFlag(notLeaderFlag))
}

func TestWitnessLeaderHintRefreshesStaleDataPeerRole(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "selector", true: "legacy-cache"}[legacy], func(t *testing.T) {
			s := new(testReplicaSelectorSuite)
			s.SetupTest(t)
			defer s.TearDownTest()

			r := s.getRegion()
			witness := proto.Clone(r.meta.Peers[0]).(*metapb.Peer)
			witness.IsWitness = true
			s.False(r.switchWorkLeaderToPeer(witness))
			if legacy {
				s.cache.UpdateLeader(r.VerID(), witness, 0)
			} else {
				req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
				selector, err := newReplicaSelector(s.cache, r.VerID(), req)
				s.Require().NoError(err)
				ctx, err := selector.next(s.bo, req)
				s.Require().NoError(err)
				bo := retry.NewBackoffer(context.Background(), 40000)
				retry, err := selector.onNotLeader(bo, ctx, &errorpb.NotLeader{Leader: witness})
				s.NoError(err)
				s.True(retry)
				s.Equal(1, selector.target.attempts, "a newly identified witness must not become retryable")
			}
			s.False(r.isValid(), "the former data peer must be removed by a metadata refresh")

			// PD's refreshed metadata still reports the witness as the Raft leader.
			refreshed, _ := installWitnessLeader(s, false)
			ctx, err := s.cache.GetTiKVRPCContext(s.bo, refreshed.VerID(), kv.ReplicaReadLeader, 0)
			s.NoError(err)
			s.Require().NotNil(ctx)
			s.NotEqual(witness.Id, ctx.Peer.Id)
			s.False(ctx.Peer.IsWitness)
		})
	}
}

func TestWitnessLeaderCannotOverrideCachedDataLeader(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	r, witness := installWitnessLeader(s, false)
	dataLeader := r.GetLeaderPeerID()
	s.False(r.switchWorkLeaderToPeer(witness))
	s.cache.UpdateLeader(r.VerID(), witness, 0)
	s.True(r.isValid())
	s.Equal(dataLeader, r.GetLeaderPeerID())
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, r.VerID(), kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	bo := retry.NewBackoffer(context.Background(), 1)
	sender := NewRegionRequestSender(s.cache, nil, oracle.NoopReadTSValidator{})
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	shouldRetry, err := sender.onRegionError(bo, ctx, req, &errorpb.Error{NotLeader: &errorpb.NotLeader{Leader: witness}})
	s.NoError(err)
	s.True(shouldRetry)
	s.Equal(1, bo.GetBackoffTimes()["regionScheduling"])
	s.Equal(dataLeader, r.GetLeaderPeerID())

	// Reject an unflagged hint too if cached role metadata identifies a witness.
	r.meta.Peers[0].IsWitness = true
	unflagged := proto.Clone(r.meta.Peers[0]).(*metapb.Peer)
	unflagged.IsWitness = false
	s.False(r.switchWorkLeaderToPeer(unflagged))
}

func TestWitnessOnlyRegionHasNoDataRequestTarget(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	meta, leader, _, _ := s.cluster.GetRegionByID(s.regionID)
	for _, peer := range meta.Peers {
		peer.IsWitness = true
		peer.StoreId = s.cluster.AllocID()
	}
	leader.IsWitness = true
	r, err := newRegion(s.bo, s.cache, &router.Region{Meta: meta, Leader: leader})
	s.Nil(r)
	s.Require().Error(err)
	s.Contains(err.Error(), "no available peers")
}

func TestWitnessRegionWithOnlyColumnarDataPeerHasNoTiKVTarget(t *testing.T) {
	s := new(testReplicaSelectorSuite)
	s.SetupTest(t)
	defer s.TearDownTest()

	// Start with only one available Full peer and retain its in-flight context.
	s.cache.SetRegionCacheStore(s.storeIDs[2], "store3", "", tikvrpc.TiFlash, uint64(resolved), nil)
	oldMeta, leader, _, _ := s.cluster.GetRegionByID(s.regionID)
	oldMeta.Peers[2].Role = metapb.PeerRole_Learner
	oldRegion, err := newRegion(s.bo, s.cache, &router.Region{Meta: oldMeta, Leader: leader, DownPeers: oldMeta.Peers[1:]})
	s.Require().NoError(err)
	s.cache.insertRegionToCache(oldRegion, true, true)
	oldCtx, err := s.cache.GetTiKVRPCContext(s.bo, oldRegion.VerID(), kv.ReplicaReadLeader, 0)
	s.Require().NoError(err)
	s.Require().NotNil(oldCtx)
	s.Len(oldCtx.Meta.Peers, 1)

	meta, _, _, _ := s.cluster.GetRegionByID(s.regionID)
	witness := meta.Peers[0]
	witness.IsWitness = true
	meta.Peers[1].IsWitness = true
	meta.Peers[2].Role = metapb.PeerRole_Learner
	oldRegion.invalidate(NoLeader)
	r, err := newRegion(s.bo, s.cache, &router.Region{Meta: meta, Leader: witness})
	s.Require().NoError(err)
	s.cache.insertRegionToCache(r, true, true)
	s.False(r.switchWorkLeaderToPeer(r.meta.Peers[0]))
	// Legacy callbacks must also tolerate the loss of all TiKV candidates,
	// even though the filtered Region still has the same total peer count.
	s.cache.UpdateLeader(r.VerID(), nil, 0)
	s.cache.OnSendFail(s.bo, oldCtx, false, errors.New("Full peer became unavailable"))

	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{})
	selector, err := newReplicaSelector(s.cache, r.VerID(), req)
	s.NoError(err)
	s.Nil(selector)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, r.VerID(), kv.ReplicaReadLeader, 0)
	s.NoError(err)
	s.Nil(ctx)

	calls := 0
	rpcClient := &fnClient{fn: func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
		calls++
		return nil, errors.New("no TiKV peer can serve data")
	}}
	sender := NewRegionRequestSender(s.cache, rpcClient, oracle.NoopReadTSValidator{})
	resp, _, _, err := sender.SendReqCtx(s.bo, req, r.VerID(), client.ReadTimeoutShort, tikvrpc.TiKV)
	s.NoError(err)
	s.Require().NotNil(resp)
	regionErr, err := resp.GetRegionError()
	s.NoError(err)
	s.NotNil(regionErr)
	s.Zero(calls)
}
