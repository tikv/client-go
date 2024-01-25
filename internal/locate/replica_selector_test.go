package locate

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByLeader() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadLeader, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 13; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i <= 10 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			s.Equal(rpcCtx.Peer.Id, rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Store.storeID, rc.GetLeaderStoreID())
			target := selector.targetReplica()
			s.Equal(target.attempts, i)
			s.Equal(target.peer.Id, rc.GetLeaderPeerID())
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, false)
		} else if i <= 12 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.Equal(rpcCtx.Peer.Id, target.peer.Id)
			s.Equal(rpcCtx.Store.storeID, target.store.storeID)
			s.Equal(target.attempts, 1)
			s.NotEqual(target.peer.Id, lastPeerId)
			lastPeerId = target.peer.Id
			s.Equal(req.StaleRead, false) // stale read will always disable in follower.
			s.Equal(req.ReplicaRead, false)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByFollower() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadFollower, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i <= 2 {
			// try followers.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id != lastPeerId)
			s.True(target.peer.Id != rc.GetLeaderPeerID())
			s.Equal(target.peer.Id, rpcCtx.Peer.Id)
			lastPeerId = target.peer.Id
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i == 3 {
			// try leader.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id == rc.GetLeaderPeerID())
			s.Equal(target.peer.Id, rpcCtx.Peer.Id)
			lastPeerId = target.peer.Id
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByPreferLeader() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadPreferLeader, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i == 1 {
			// try leader first.
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id == rc.GetLeaderPeerID())
			s.Equal(target.peer.Id, rpcCtx.Peer.Id)
			lastPeerId = target.peer.Id
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i <= 3 {
			//try followers.
			s.NotNil(rpcCtx, fmt.Sprintf("i: %v", i))
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id != lastPeerId)
			s.True(target.peer.Id != rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, target.peer.Id)
			lastPeerId = target.peer.Id
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByMixed() {
	// case-1: mixed read, no label.
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	lastPeerId := uint64(0)
	for i := 1; i <= 4; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i <= 3 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id != lastPeerId)
			s.Equal(rpcCtx.Peer.Id, target.peer.Id)
			lastPeerId = target.peer.Id
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else {
			s.Nil(rpcCtx)
			s.Equal(rc.isValid(), false)
		}
	}
	// case-2: mixed read, with label.
	labels := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "us-west-1",
		},
	}

	for mi := range selector.getAllReplicas() {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
		replicas := selector.getAllReplicas()
		replicas[mi].store.labels = labels
		region, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		s.NotNil(region)
		selector, err = NewReplicaSelector(s.cache, region.Region, req, WithMatchLabels(labels))
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		leaderIdx := rc.getStore().workTiKVIdx
		leader := replicas[leaderIdx]
		matchReplica := replicas[mi]
		if leaderIdx != AccessIndex(mi) {
			// match label in follower
			for i := 1; i <= 6; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// first try the match-label peer.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, matchReplica.peer.Id)
					s.True(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i == 2 {
					// prefer leader in second try.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, leader.peer.Id)
					s.False(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i == 3 { // diff from v1
					// try last remain follower.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.NotEqual(target.peer.Id, matchReplica.peer.Id)
					s.NotEqual(target.peer.Id, leader.peer.Id)
					s.False(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		} else {
			// match label in leader
			for i := 1; i <= 5; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				//rc := s.cache.GetCachedRegionWithRLock(region.Region)
				//s.NotNil(rc)
				if i == 1 {
					// try leader
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, leader.peer.Id)
					s.True(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else if i <= 3 {
					// try follower
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.NotEqual(target.peer.Id, leader.peer.Id)
					s.False(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		}
		replicas[mi].store.labels = nil
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByStaleRead() {
	bo := retry.NewBackoffer(context.Background(), 1000)
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	req.EnableStaleWithMixedReplicaRead()
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	firstTryInLeader := false
	lastPeerId := uint64(0)
	for i := 1; i <= 5; i++ {
		rpcCtx, err := selector.next(bo, req)
		s.Nil(err)
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		if i == 1 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			firstTryInLeader = target.peer.Id == rc.GetLeaderPeerID()
			s.Equal(rpcCtx.Peer.Id, target.peer.Id)
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, true)
			s.Equal(req.ReplicaRead, false)
		} else if i == 2 {
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id == rc.GetLeaderPeerID())
			s.Equal(rpcCtx.Peer.Id, target.peer.Id)
			if firstTryInLeader {
				s.Equal(target.attempts, 2)
			} else {
				s.Equal(target.attempts, 1)
			}
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, false)
		} else if i == 3 { // diff from v1
			s.NotNil(rpcCtx)
			s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
			target := selector.targetReplica()
			s.True(target.peer.Id != rc.GetLeaderPeerID())
			s.NotEqual(rpcCtx.Peer.Id, lastPeerId)
			s.Equal(rpcCtx.Peer.Id, target.peer.Id)
			s.Equal(target.attempts, 1)
			s.Equal(req.StaleRead, false)
			s.Equal(req.ReplicaRead, true)
		} else if i == 4 {
			if firstTryInLeader {
				s.NotNil(rpcCtx)
				s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
				target := selector.targetReplica()
				s.True(target.peer.Id != rc.GetLeaderPeerID())
				s.NotEqual(rpcCtx.Peer.Id, lastPeerId)
				s.Equal(rpcCtx.Peer.Id, target.peer.Id)
				s.Equal(target.attempts, 1)
				s.Equal(req.StaleRead, false)
				s.Equal(req.ReplicaRead, true)
			} else {
				s.Nil(rpcCtx)
				s.Equal(rc.isValid(), false)
			}
		} else {
			s.Nil(rpcCtx)
		}
		if rpcCtx != nil {
			lastPeerId = rpcCtx.Peer.Id
		}
	}

	// test with label.
	labels := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "us-west-1",
		},
	}
	for mi := range selector.getAllReplicas() {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
		req.EnableStaleWithMixedReplicaRead()
		region, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		s.NotNil(region)
		selector, err = NewReplicaSelector(s.cache, region.Region, req, WithMatchLabels(labels))
		s.Nil(err)
		replicas := selector.getAllReplicas()
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		leaderIdx := rc.getStore().workTiKVIdx
		matchReplica := replicas[mi]
		matchReplica.store.labels = labels
		if leaderIdx != AccessIndex(mi) {
			// match label in follower
			for i := 1; i <= 6; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					// first try the match-label peer.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, matchReplica.peer.Id)
					s.NotEqual(target.peer.Id, rc.GetLeaderPeerID())
					s.True(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, true)
					s.Equal(req.ReplicaRead, false)
				} else if i == 2 {
					// retry in leader by leader read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, rc.GetLeaderPeerID())
					s.False(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i == 3 { // diff from v1
					// retry remain follower by follower read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.NotEqual(target.peer.Id, matchReplica.peer.Id)
					s.NotEqual(target.peer.Id, rc.GetLeaderPeerID())
					s.False(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		} else {
			// match label in leader
			for i := 1; i <= 5; i++ {
				rpcCtx, err := selector.next(bo, req)
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(region.Region)
				s.NotNil(rc)
				if i == 1 {
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, rc.GetLeaderPeerID())
					s.True(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, true)
					s.Equal(req.ReplicaRead, false)
				} else if i == 2 {
					// retry in leader without stale read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.Equal(target.peer.Id, rc.GetLeaderPeerID())
					s.True(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 2)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, false)
				} else if i <= 4 {
					// retry remain followers by follower read.
					s.NotNil(rpcCtx)
					s.Equal(rpcCtx.Peer.Role, metapb.PeerRole_Voter)
					target := selector.targetReplica()
					s.NotEqual(target.peer.Id, rc.GetLeaderPeerID())
					s.False(target.store.IsLabelsMatch(labels))
					s.Equal(rpcCtx.Peer.Id, target.peer.Id)
					s.Equal(target.attempts, 1)
					s.Equal(req.StaleRead, false)
					s.Equal(req.ReplicaRead, true)
				} else {
					s.Nil(rpcCtx)
					s.Equal(rc.isValid(), false)
				}
			}
		}
		matchReplica.store.labels = nil
	}
}

func (s *testRegionRequestToThreeStoresSuite) TestReplicaSelectorV2ByMixedCalculateScore() {
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
	region, err := s.cache.LocateKey(s.bo, []byte("a"))
	s.Nil(err)
	s.NotNil(region)
	selector, err := NewReplicaSelector(s.cache, region.Region, req)
	s.Nil(err)
	for i, r := range selector.getAllReplicas() {
		rc := s.cache.GetCachedRegionWithRLock(region.Region)
		s.NotNil(rc)
		isLeader := r.peer.Id == rc.GetLeaderPeerID()
		s.Equal(isLeader, AccessIndex(i) == rc.getStore().workTiKVIdx)
		strategy := ReplicaSelectMixedStrategy{}
		score := strategy.calculateScore(r, isLeader)
		s.Equal(r.store.isSlow(), false)
		if isLeader {
			s.Equal(score, 4)
		} else {
			s.Equal(score, 5)
		}
		r.store.slowScore.avgScore = slowScoreThreshold + 1
		s.Equal(r.store.isSlow(), true)
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 3)
		} else {
			s.Equal(score, 4)
		}
		strategy.tryLeader = true
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, 4)
		strategy.preferLeader = true
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 5)
		} else {
			s.Equal(score, 4)
		}
		strategy.learnerOnly = true
		strategy.tryLeader = false
		strategy.preferLeader = false
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, 3)
		labels := []*metapb.StoreLabel{
			{
				Key:   "zone",
				Value: "us-west-1",
			},
		}
		strategy.labels = labels
		score = strategy.calculateScore(r, isLeader)
		s.Equal(score, 0)

		strategy = ReplicaSelectMixedStrategy{
			tryLeader: true,
			labels:    labels,
		}
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 2)
		} else {
			s.Equal(score, 1)
		}

		strategy = ReplicaSelectMixedStrategy{
			preferLeader: true,
			labels:       labels,
		}
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 2)
		} else {
			s.Equal(score, 1)
		}
		r.store.labels = labels
		score = strategy.calculateScore(r, isLeader)
		if isLeader {
			s.Equal(score, 5)
		} else {
			s.Equal(score, 4)
		}
		r.store.labels = nil
	}
}
