package locate

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
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
	if !config.GetGlobalConfig().EnableReplicaSelectorV2 {
		return
	}
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

	for mi := range selector.getBaseReplicaSelector().replicas {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
		replicas := selector.getBaseReplicaSelector().replicas
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
	if !config.GetGlobalConfig().EnableReplicaSelectorV2 {
		return
	}
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
	for mi := range selector.getBaseReplicaSelector().replicas {
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("a")}, kv.ReplicaReadMixed, nil, kvrpcpb.Context{})
		req.EnableStaleWithMixedReplicaRead()
		region, err = s.cache.LocateKey(s.bo, []byte("a"))
		s.Nil(err)
		s.NotNil(region)
		selector, err = NewReplicaSelector(s.cache, region.Region, req, WithMatchLabels(labels))
		s.Nil(err)
		replicas := selector.getBaseReplicaSelector().replicas
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
	for i, r := range selector.getBaseReplicaSelector().replicas {
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

type RegionErrorType int

const (
	NotLeaderErr RegionErrorType = iota + 1
	RegionNotFoundErr
	KeyNotInRegionErr
	EpochNotMatchErr
	ServerIsBusyErr
	StaleCommandErr
	StoreNotMatchErr
	RaftEntryTooLargeErr
	MaxTimestampNotSyncedErr
	ReadIndexNotReadyErr
	ProposalInMergingModeErr
	DataIsNotReadyErr
	RegionNotInitializedErr
	DiskFullErr
	RecoveryInProgressErr
	FlashbackInProgressErr
	FlashbackNotPreparedErr
	IsWitnessErr
	MismatchPeerIdErr
	BucketVersionNotMatchErr
	// following error type is not region error.
	DeadLineExceededErr
)

func (tp RegionErrorType) GenRegionError() *errorpb.Error {
	err := &errorpb.Error{}
	switch tp {
	case NotLeaderErr:
		err.NotLeader = &errorpb.NotLeader{}
	case RegionNotFoundErr:
		err.RegionNotFound = &errorpb.RegionNotFound{}
	case KeyNotInRegionErr:
		err.KeyNotInRegion = &errorpb.KeyNotInRegion{}
	case EpochNotMatchErr:
		err.EpochNotMatch = &errorpb.EpochNotMatch{}
	case ServerIsBusyErr:
		err.ServerIsBusy = &errorpb.ServerIsBusy{}
	case StaleCommandErr:
		err.StaleCommand = &errorpb.StaleCommand{}
	case StoreNotMatchErr:
		err.StoreNotMatch = &errorpb.StoreNotMatch{}
	case RaftEntryTooLargeErr:
		err.RaftEntryTooLarge = &errorpb.RaftEntryTooLarge{}
	case MaxTimestampNotSyncedErr:
		err.MaxTimestampNotSynced = &errorpb.MaxTimestampNotSynced{}
	case ReadIndexNotReadyErr:
		err.ReadIndexNotReady = &errorpb.ReadIndexNotReady{}
	case ProposalInMergingModeErr:
		err.ProposalInMergingMode = &errorpb.ProposalInMergingMode{}
	case DataIsNotReadyErr:
		err.DataIsNotReady = &errorpb.DataIsNotReady{}
	case RegionNotInitializedErr:
		err.RegionNotInitialized = &errorpb.RegionNotInitialized{}
	case DiskFullErr:
		err.DiskFull = &errorpb.DiskFull{}
	case RecoveryInProgressErr:
		err.RecoveryInProgress = &errorpb.RecoveryInProgress{}
	case FlashbackInProgressErr:
		err.FlashbackInProgress = &errorpb.FlashbackInProgress{}
	case FlashbackNotPreparedErr:
		err.FlashbackNotPrepared = &errorpb.FlashbackNotPrepared{}
	case IsWitnessErr:
		err.IsWitness = &errorpb.IsWitness{}
	case MismatchPeerIdErr:
		err.MismatchPeerId = &errorpb.MismatchPeerId{}
	case BucketVersionNotMatchErr:
		err.BucketVersionNotMatch = &errorpb.BucketVersionNotMatch{}
	default:
		return nil
	}
	return err
}

func (tp RegionErrorType) GenError() (*errorpb.Error, error) {
	regionErr := tp.GenRegionError()
	if regionErr != nil {
		return regionErr, nil
	}
	switch tp {
	case DeadLineExceededErr:
		return nil, context.DeadlineExceeded
	}
	return nil, nil
}

func TestReplicaSelectorAccessPath(t *testing.T) {
	s := new(testRegionRequestToThreeStoresSuite)
	s.SetupTest()
	s.SetT(t)
	s.SetS(s)
	defer s.TearDownTest()

	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	r := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(r)
	s.Equal(r.GetLeaderStoreID(), uint64(1)) // leader in store1.
	s.Equal(r.getStore().stores[0].labels[0].Key, "id")
	s.Equal(r.getStore().stores[0].labels[0].Value, fmt.Sprintf("%v", r.getStore().stores[0].storeID))

	fakeRegionError := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}} // fake region error, since no replica is available.
	cases := []struct {
		staleRead           bool
		readType            kv.ReplicaReadType
		timeout             time.Duration
		label               *metapb.StoreLabel
		accessErr           []RegionErrorType
		access              []string
		accessInV1          []string
		respErr             string
		respRegionError     *errorpb.Error
		respRegionErrorInV1 *errorpb.Error
		backoffCnt          int
		backoffCntInV1      int
	}{
		// Test Leader read.
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
			},
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt: 1,
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt: 2,
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{NotLeaderErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
			},
			backoffCnt: 1,
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{NotLeaderErr, NotLeaderErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
			},
			backoffCnt: 2,
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{NotLeaderErr, NotLeaderErr, NotLeaderErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
			},
			backoffCnt:      3,
			respRegionError: fakeRegionError,
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{RaftEntryTooLargeErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
			},
			respErr: RaftEntryTooLargeErr.GenRegionError().String(),
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{FlashbackInProgressErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
			},
			respErr: "region 0 is in flashback progress, FlashbackStartTS is 0",
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{FlashbackNotPreparedErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
			},
			respErr: "region 0 is not prepared for the flashback",
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{DeadLineExceededErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
			},
			backoffCnt: 1,
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderErr, NotLeaderErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false", // is this expected, should retry with replica-read?
				"addr: store3, replica-read: false, stale-read: false", // ditto.
			},
			backoffCnt:      3,
			respRegionError: fakeRegionError,
		},
		{
			readType:  kv.ReplicaReadLeader,
			timeout:   time.Second,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
			},
			accessInV1: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:          2,
			backoffCntInV1:      2,
			respRegionError:     fakeRegionError,
			respRegionErrorInV1: fakeRegionError,
		},

		// Test follower Read.
		{
			readType:  kv.ReplicaReadFollower,
			accessErr: []RegionErrorType{},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
			},
		},
		{
			readType:  kv.ReplicaReadFollower,
			accessErr: []RegionErrorType{ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			readType:  kv.ReplicaReadFollower,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
				"addr: store1, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 2,
		},
		{
			readType:  kv.ReplicaReadFollower,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
				"addr: store1, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  3,
			respRegionError: fakeRegionError,
		},
		{
			readType:  kv.ReplicaReadFollower,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
				"addr: store1, replica-read: true, stale-read: false",
			},
			backoffCnt:     1,
			backoffCntInV1: 2,
		},
		{
			readType:  kv.ReplicaReadFollower,
			timeout:   time.Second,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
				"addr: store1, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		// Test mixed Read.
		{
			readType:  kv.ReplicaReadMixed,
			accessErr: []RegionErrorType{},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
			},
		},
		{
			readType:  kv.ReplicaReadMixed,
			accessErr: []RegionErrorType{ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			readType:  kv.ReplicaReadMixed,
			accessErr: []RegionErrorType{ServerIsBusyErr, StaleCommandErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			readType:  kv.ReplicaReadMixed,
			accessErr: []RegionErrorType{ServerIsBusyErr, StaleCommandErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  2,
			respRegionError: fakeRegionError,
		},
		{
			readType:  kv.ReplicaReadMixed,
			accessErr: []RegionErrorType{ServerIsBusyErr, RegionNotFoundErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  1,
			respRegionError: RegionNotFoundErr.GenRegionError(),
		},
		{
			readType:  kv.ReplicaReadMixed,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     1,
			backoffCntInV1: 2,
		},
		{
			readType:  kv.ReplicaReadMixed,
			timeout:   time.Second,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},

		// Test prefer-leader Read.
		{
			readType:  kv.ReplicaReadPreferLeader,
			accessErr: []RegionErrorType{},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
			},
		},
		{
			readType:  kv.ReplicaReadPreferLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			readType:  kv.ReplicaReadPreferLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr, StaleCommandErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			readType:  kv.ReplicaReadPreferLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr, StaleCommandErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  2,
			respRegionError: fakeRegionError,
		},
		{
			readType:  kv.ReplicaReadPreferLeader,
			accessErr: []RegionErrorType{ServerIsBusyErr, RegionNotFoundErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  1,
			respRegionError: RegionNotFoundErr.GenRegionError(),
		},
		{
			readType:  kv.ReplicaReadPreferLeader,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     1,
			backoffCntInV1: 2,
		},
		{
			readType:  kv.ReplicaReadPreferLeader,
			timeout:   time.Second,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},

		// stale read test.
		{
			staleRead: true,
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
			},
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
			},
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 0,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  2,
			respRegionError: fakeRegionError,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false", // is this expected? since store1(leader) is busy, then retry it again is expected?
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false", // is this expected? since store1(leader) is busy, then retry it again is expected?
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  2,
			respRegionError: fakeRegionError,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     1,
			backoffCntInV1: 2,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      1,
			backoffCntInV1:  3,
			respRegionError: fakeRegionError,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     1,
			backoffCntInV1: 2,
		},
		{
			staleRead: true,
			timeout:   time.Second,
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		// stale read test with label.
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "1"},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
			},
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
			},
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "3"},
			access: []string{
				"addr: store3, replica-read: false, stale-read: true",
			},
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
			},
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			accessInV1: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			accessInV1: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			accessInV1: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt: 1,
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DataIsNotReadyErr, RegionNotFoundErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
			},
			respRegionError: RegionNotFoundErr.GenRegionError(),
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			accessInV1: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
			},
			backoffCnt:     0,
			backoffCntInV1: 1,
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			accessInV1: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      0,
			backoffCntInV1:  2,
			respRegionError: fakeRegionError,
		},
		{
			staleRead: true,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DeadLineExceededErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			accessInV1: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt:     1,
			backoffCntInV1: 2,
		},
		{
			staleRead: true,
			timeout:   time.Second,
			label:     &metapb.StoreLabel{Key: "id", Value: "2"},
			accessErr: []RegionErrorType{DeadLineExceededErr, DeadLineExceededErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt: 0,
		},
	}

	retryRegionErrorTypes := []RegionErrorType{ServerIsBusyErr, StaleCommandErr, MaxTimestampNotSyncedErr, ProposalInMergingModeErr, ReadIndexNotReadyErr, RegionNotInitializedErr, DiskFullErr}
	for _, tp := range retryRegionErrorTypes {
		backoffCnt := 1
		switch tp {
		case StaleCommandErr:
			backoffCnt = 0
		}
		cases = append(cases, struct {
			staleRead           bool
			readType            kv.ReplicaReadType
			timeout             time.Duration
			label               *metapb.StoreLabel
			accessErr           []RegionErrorType
			access              []string
			accessInV1          []string
			respErr             string
			respRegionError     *errorpb.Error
			respRegionErrorInV1 *errorpb.Error
			backoffCnt          int
			backoffCntInV1      int
		}{
			accessErr: []RegionErrorType{tp},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt: backoffCnt,
		})
	}
	noRetryRegionErrorTypes := []RegionErrorType{RegionNotFoundErr, KeyNotInRegionErr, EpochNotMatchErr, StoreNotMatchErr, RecoveryInProgressErr, IsWitnessErr, MismatchPeerIdErr, BucketVersionNotMatchErr}
	for _, tp := range noRetryRegionErrorTypes {
		backoffCnt := 0
		switch tp {
		case RecoveryInProgressErr, IsWitnessErr:
			backoffCnt = 1
		}
		cases = append(cases, struct {
			staleRead           bool
			readType            kv.ReplicaReadType
			timeout             time.Duration
			label               *metapb.StoreLabel
			accessErr           []RegionErrorType
			access              []string
			accessInV1          []string
			respErr             string
			respRegionError     *errorpb.Error
			respRegionErrorInV1 *errorpb.Error
			backoffCnt          int
			backoffCntInV1      int
		}{
			accessErr: []RegionErrorType{tp},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
			},
			backoffCnt:      backoffCnt,
			respRegionError: tp.GenRegionError(),
		})
	}

	randIntn = func(n int) int {
		return 0
	}
	for i, ca := range cases {
		reachable.injectConstantLiveness(s.cache) // inject reachable liveness.
		msg := fmt.Sprintf("test case idx: %v, readType: %v, stale_read: %v, timeout: %v, label: %v, access_err: %v", i, ca.readType, ca.staleRead, ca.timeout, ca.label, ca.accessErr)
		access := []string{}
		fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			idx := len(access)
			access = append(access, fmt.Sprintf("addr: %v, replica-read: %v, stale-read: %v", addr, req.ReplicaRead, req.StaleRead))
			if idx < len(ca.accessErr) {
				regionErr, err := ca.accessErr[idx].GenError()
				if regionErr != nil {
					return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
						RegionError: regionErr,
					}}, nil
				}
				if err != nil {
					unreachable.injectConstantLiveness(s.cache) // inject unreachable liveness.
					return nil, err
				}
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
				Value: []byte("hello world"),
			}}, nil
		}}
		sender := NewRegionRequestSender(s.cache, fnClient)
		req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key: []byte("key"),
		})
		if ca.staleRead {
			req.EnableStaleWithMixedReplicaRead()
			req.ReadReplicaScope = oracle.GlobalTxnScope
			req.TxnScope = oracle.GlobalTxnScope
		} else {
			req.ReplicaReadType = ca.readType
			req.ReplicaRead = ca.readType.IsFollowerRead()
		}
		opts := []StoreSelectorOption{}
		if ca.label != nil {
			opts = append(opts, WithMatchLabels([]*metapb.StoreLabel{ca.label}))
		}

		loc, err := s.cache.LocateKey(s.bo, []byte("key"))
		s.Nil(err)

		// reset slow score, since serverIsBusyErr will mark the store is slow, and affect remaining test cases.
		stores := s.cache.GetAllStores()
		for _, store := range stores {
			store.slowScore = SlowScoreStat{}
			atomic.StoreUint32(&store.livenessState, uint32(reachable))
			store.setResolveState(resolved)
		}

		bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
		timeout := ca.timeout
		if timeout == 0 {
			timeout = client.ReadTimeoutShort
		}
		resp, _, _, err := sender.SendReqCtx(bo, req, loc.Region, timeout, tikvrpc.TiKV, opts...)
		caRespRegionError := ca.respRegionError
		if config.GetGlobalConfig().EnableReplicaSelectorV2 || len(ca.accessInV1) == 0 {
			s.Equal(ca.access, access, msg)
		} else {
			s.Equal(ca.accessInV1, access, msg)
			caRespRegionError = ca.respRegionErrorInV1
		}
		caBackoffCnt := ca.backoffCnt
		if !config.GetGlobalConfig().EnableReplicaSelectorV2 && ca.backoffCntInV1 > 0 {
			caBackoffCnt = ca.backoffCntInV1
		}
		s.Equal(caBackoffCnt, bo.GetTotalBackoffTimes(), msg)
		if ca.respErr == "" {
			s.Nil(err, msg)
			s.NotNil(resp, msg)
			regionErr, err := resp.GetRegionError()
			s.Nil(err, msg)
			if caRespRegionError == nil {
				s.Nil(regionErr, msg)
				s.Equal([]byte("hello world"), resp.Resp.(*kvrpcpb.GetResponse).Value, msg)
			} else {
				s.NotNil(regionErr, msg)
				s.Equal(caRespRegionError, regionErr, msg)
				if IsFakeRegionError(regionErr) {
					if ca.timeout == 0 {
						s.False(sender.replicaSelector.getBaseReplicaSelector().region.isValid(), msg) // region must be invalidated.
					} else {
						if config.GetGlobalConfig().EnableReplicaSelectorV2 {
							s.True(sender.replicaSelector.getBaseReplicaSelector().region.isValid(), msg)
						}
					}
				}
			}
		} else {
			s.NotNil(err, msg)
			s.Equal(ca.respErr, err.Error(), msg)
		}
		sender.replicaSelector.invalidateRegion() // invalidate region to reload.
	}
}

func BenchmarkReplicaSelector(b *testing.B) {
	s := new(testRegionRequestToThreeStoresSuite)
	s.SetupTest()
	defer s.TearDownTest()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableReplicaSelectorV2 = true
	})
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			Value: []byte("value"),
		}}, nil
	}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key: []byte("key"),
		})
		loc, err := s.cache.LocateKey(s.bo, []byte("key"))
		if err != nil {
			b.Fail()
		}
		sender := NewRegionRequestSender(s.cache, fnClient)
		bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
		sender.SendReqCtx(bo, req, loc.Region, client.ReadTimeoutShort, tikvrpc.TiKV)
	}
}
