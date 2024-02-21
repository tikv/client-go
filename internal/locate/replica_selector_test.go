package locate

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
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
	NotLeaderWithNewLeader2Err
	NotLeaderWithNewLeader3Err
	RegionNotFoundErr
	KeyNotInRegionErr
	EpochNotMatchErr
	ServerIsBusyErr
	ServerIsBusyWithEstimatedWaitMsErr
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
	RegionErrorTypeMax
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
	case ServerIsBusyWithEstimatedWaitMsErr:
		err.ServerIsBusy = &errorpb.ServerIsBusy{EstimatedWaitMs: 10}
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

func (tp RegionErrorType) Valid(addr string, req *tikvrpc.Request) bool {
	// leader-read.
	if req.StaleRead == false && req.ReplicaRead == false {
		switch tp {
		case DataIsNotReadyErr:
			// DataIsNotReadyErr only return when req is a stale read.
			return false
		}
	}
	// replica-read.
	if req.StaleRead == false && req.ReplicaRead == true {
		switch tp {
		case NotLeaderErr, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err:
			// NotLeaderErr will not return in replica read.
			return false
		case DataIsNotReadyErr:
			// DataIsNotReadyErr only return when req is a stale read.
			return false
		}
	}
	// stale-read.
	if req.StaleRead == true && req.ReplicaRead == false {
		switch tp {
		case NotLeaderErr, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err:
			// NotLeaderErr will not return in stale read.
			return false
		}
	}
	// store2 can't return a not leader error with new leader in store2.
	if addr == "store2" && tp == NotLeaderWithNewLeader2Err {
		return false
	}
	// ditto.
	if addr == "store3" && tp == NotLeaderWithNewLeader3Err {
		return false
	}
	return true
}

func (tp RegionErrorType) String() string {
	switch tp {
	case NotLeaderErr:
		return "NotLeaderErr"
	case NotLeaderWithNewLeader2Err:
		return "NotLeaderWithNewLeader2Err"
	case NotLeaderWithNewLeader3Err:
		return "NotLeaderWithNewLeader3Err"
	case RegionNotFoundErr:
		return "RegionNotFoundErr"
	case KeyNotInRegionErr:
		return "KeyNotInRegionErr"
	case EpochNotMatchErr:
		return "EpochNotMatchErr"
	case ServerIsBusyErr:
		return "ServerIsBusyErr"
	case ServerIsBusyWithEstimatedWaitMsErr:
		return "ServerIsBusyWithEstimatedWaitMsErr"
	case StaleCommandErr:
		return "StaleCommandErr"
	case StoreNotMatchErr:
		return "StoreNotMatchErr"
	case RaftEntryTooLargeErr:
		return "RaftEntryTooLargeErr"
	case MaxTimestampNotSyncedErr:
		return "MaxTimestampNotSyncedErr"
	case ReadIndexNotReadyErr:
		return "ReadIndexNotReadyErr"
	case ProposalInMergingModeErr:
		return "ProposalInMergingModeErr"
	case DataIsNotReadyErr:
		return "DataIsNotReadyErr"
	case RegionNotInitializedErr:
		return "RegionNotInitializedErr"
	case DiskFullErr:
		return "DiskFullErr"
	case RecoveryInProgressErr:
		return "RecoveryInProgressErr"
	case FlashbackInProgressErr:
		return "FlashbackInProgressErr"
	case FlashbackNotPreparedErr:
		return "FlashbackNotPreparedErr"
	case IsWitnessErr:
		return "IsWitnessErr"
	case MismatchPeerIdErr:
		return "MismatchPeerIdErr"
	case BucketVersionNotMatchErr:
		return "BucketVersionNotMatchErr"
	case DeadLineExceededErr:
		return "DeadLineExceededErr"
	default:
		return "unknown_" + strconv.Itoa(int(tp))
	}
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
			accessErr: []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
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
			accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
			},
			backoffCnt: 0, // no backoff
		},
		{
			readType:  kv.ReplicaReadLeader,
			accessErr: []RegionErrorType{NotLeaderWithNewLeader3Err, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
			},
			backoffCnt: 1,
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
			accessErr: []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
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
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
				"addr: store1, replica-read: true, stale-read: false",
			},
			backoffCnt:      1,
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
			accessErr: []RegionErrorType{ServerIsBusyWithEstimatedWaitMsErr, StaleCommandErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: true, stale-read: false",
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      1,
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
			backoffCnt: 0,
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
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr, ServerIsBusyErr},
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
			accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false", // is this expected? since store1(leader) is busy, then retry it again is expected?
				"addr: store2, replica-read: true, stale-read: false",
				"addr: store3, replica-read: true, stale-read: false",
			},
			backoffCnt:      1,
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
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader3Err},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
			},
			backoffCnt: 0,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader3Err, ServerIsBusyErr, NotLeaderWithNewLeader2Err, ServerIsBusyErr},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
			},
			backoffCnt: 2,
		},
		{
			staleRead: true,
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader2Err},
			access: []string{
				"addr: store1, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store2, replica-read: false, stale-read: false",
			},
			backoffCnt: 0,
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
			accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderWithNewLeader3Err, ServerIsBusyErr},
			access: []string{
				"addr: store2, replica-read: false, stale-read: true",
				"addr: store1, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
				"addr: store3, replica-read: false, stale-read: false",
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
			accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyWithEstimatedWaitMsErr, ServerIsBusyErr, ServerIsBusyErr},
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
			backoffCnt:      1,
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
		loc, err := s.cache.LocateKey(s.bo, []byte("key"))
		s.Nil(err)
		access := []string{}
		fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
			idx := len(access)
			access = append(access, fmt.Sprintf("addr: %v, replica-read: %v, stale-read: %v", addr, req.ReplicaRead, req.StaleRead))
			if idx < len(ca.accessErr) {
				var regionErr *errorpb.Error
				genNotLeaderErr := func(storeID uint64) *errorpb.Error {
					rc := s.cache.GetCachedRegionWithRLock(loc.Region)
					var peerInStore *metapb.Peer
					for _, peer := range rc.meta.Peers {
						if peer.StoreId == storeID {
							peerInStore = peer
							break
						}
					}
					return &errorpb.Error{
						NotLeader: &errorpb.NotLeader{
							RegionId: req.RegionId,
							Leader:   peerInStore,
						},
					}
				}
				switch ca.accessErr[idx] {
				case NotLeaderWithNewLeader2Err:
					regionErr = genNotLeaderErr(2)
				case NotLeaderWithNewLeader3Err:
					regionErr = genNotLeaderErr(3)
				default:
					regionErr, err = ca.accessErr[idx].GenError()
				}
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
		// reset slow score, since serverIsBusyErr will mark the store is slow, and affect remaining test cases.
		stores := s.cache.GetAllStores()
		for _, store := range stores {
			store.slowScore.resetSlowScore()
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

type replicaSelectorAccessPathCase struct {
	reqType          tikvrpc.CmdType
	readType         kv.ReplicaReadType
	staleRead        bool
	timeout          time.Duration
	label            *metapb.StoreLabel
	accessErr        []RegionErrorType
	accessErrInValid bool
	accessPathResult                   // use to record the execution result.
	expect           *accessPathResult //
}

type accessPathResult struct {
	accessPath      []string
	respErr         string
	respRegionError *errorpb.Error
	backoffCnt      int
	backoffDetail   []string
	regionIsValid   bool
}

func TestReplicaSelectorAccessPathByCase(t *testing.T) {
	s := new(testRegionRequestToThreeStoresSuite)
	s.SetupTest()
	s.SetT(t)
	s.SetS(s)
	defer s.TearDownTest()

	randIntn = func(n int) int { return 0 }                                       // mock randIntn to make test determinate.
	s.NoError(failpoint.Enable("tikvclient/fastBackoffBySkipSleep", `return`))    // skip backoff sleep to speed up test.
	s.NoError(failpoint.Enable("tikvclient/skipStoreCheckUntilHealth", `return`)) // skip store health check to stable the test.
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/fastBackoffBySkipSleep"))
		s.NoError(failpoint.Disable("tikvclient/skipStoreCheckUntilHealth"))
	}()

	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	r := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(r)
	// The following assumptions are made in the latter tests, which should be checked in advance:
	s.Equal(r.GetLeaderStoreID(), uint64(1)) // region's leader in store1.
	s.Equal(len(r.getStore().stores), 3)     // region has 3 peer(stores).
	for _, store := range r.getStore().stores {
		s.Equal(store.labels[0].Key, "id") // Each store has a label "id", and the value is the store's ID.
		s.Equal(store.labels[0].Value, fmt.Sprintf("%v", store.storeID))
	}

	fakeEpochNotMatch := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}} // fake region error, cause by no replica is available.
	runCaseAndCompare := func(ca replicaSelectorAccessPathCase) {
		ca2 := ca
		config.UpdateGlobal(func(conf *config.Config) {
			conf.EnableReplicaSelectorV2 = false
		})
		ca.run(s)
		if ca.accessErrInValid {
			// ignore this invalid case
			return
		}
		config.UpdateGlobal(func(conf *config.Config) {
			conf.EnableReplicaSelectorV2 = true
		})
		ca2.run(s)
		if ca2.expect != nil {
			msg := fmt.Sprintf("%v\n\n", ca2.Format())
			expect := ca2.expect
			result := ca2.accessPathResult
			s.Equal(expect.accessPath, result.accessPath, msg)
			s.Equal(expect.respErr, result.respErr, msg)
			s.Equal(expect.respRegionError, result.respRegionError, msg)
			s.Equal(expect.regionIsValid, result.regionIsValid, msg)
			s.Equal(expect.backoffCnt, result.backoffCnt, msg)
			s.Equal(expect.backoffDetail, result.backoffDetail, msg)
		}
		msg := fmt.Sprintf("v1:%v\nv2:%v\n\n", ca.Format(), ca2.Format())
		v1 := ca.accessPathResult
		v2 := ca2.accessPathResult
		s.Equal(v1.accessPath, v2.accessPath, msg)
		s.Equal(v1.respErr, v2.respErr, msg)
		s.Equal(v1.respRegionError, v2.respRegionError, msg)
		s.Equal(v1.regionIsValid, v2.regionIsValid, msg)
		if ca.readType == kv.ReplicaReadLeader { // only leader read backoff case match.
			s.Equal(v1.backoffCnt, v2.backoffCnt, msg)
			s.Equal(v1.backoffDetail, v2.backoffDetail, msg)
		}
	}

	var ca replicaSelectorAccessPathCase
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{DeadLineExceededErr, DiskFullErr, FlashbackNotPreparedErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "region 0 is not prepared for the flashback",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"tikvDiskFull+1"},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}"},
			respErr:         "region 0 is in flashback progress, FlashbackStartTS is 0",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	// Don't invalid region in tryFollowers, since leader meets deadlineExceededErr.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{NotLeaderErr, NotLeaderWithNewLeader3Err, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	// Don't invalid region in accessFollower, since leader meets deadlineExceededErr.
	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     nil,
		accessErr: []RegionErrorType{ServerIsBusyErr, ServerIsBusyErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: true, stale-read: false}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   0,
		label:     &metapb.StoreLabel{Key: "id", Value: "3"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, NotLeaderErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store3, replica-read: false, stale-read: true}",
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      1,
			backoffDetail:   []string{"regionScheduling+1"},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: false,
		timeout:   time.Second,
		label:     &metapb.StoreLabel{Key: "id", Value: "2"},
		accessErr: []RegionErrorType{FlashbackInProgressErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store1, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   time.Microsecond * 100,
		label:     &metapb.StoreLabel{Key: "id", Value: "1"},
		accessErr: []RegionErrorType{DataIsNotReadyErr, ServerIsBusyErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				// don't retry leader(store1), since leader won't return DataIsNotReadyErr, so retry it with leader-read may got NotLeaderErr.
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: nil,
			backoffCnt:      0,
			backoffDetail:   []string{},
			regionIsValid:   true,
		},
	}
	runCaseAndCompare(ca)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdPrewrite,
		readType:  kv.ReplicaReadLeader,
		staleRead: false,
		timeout:   0,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, NotLeaderErr, NotLeaderWithNewLeader2Err, ServerIsBusyWithEstimatedWaitMsErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store3, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}",
				"{addr: store2, replica-read: false, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      4,
			backoffDetail:   []string{"regionScheduling+1", "tikvRPC+2", "tikvServerBusy+1"},
			regionIsValid:   false,
		},
	}
	runCaseAndCompare(ca)
	s.False(ca.accessErrInValid)

	ca = replicaSelectorAccessPathCase{
		reqType:   tikvrpc.CmdGet,
		readType:  kv.ReplicaReadMixed,
		staleRead: true,
		timeout:   0,
		label:     nil,
		accessErr: []RegionErrorType{DeadLineExceededErr, RegionNotInitializedErr, DeadLineExceededErr},
		expect: &accessPathResult{
			accessPath: []string{
				"{addr: store1, replica-read: false, stale-read: true}",
				"{addr: store2, replica-read: true, stale-read: false}",
				"{addr: store3, replica-read: true, stale-read: false}"},
			respErr:         "",
			respRegionError: fakeEpochNotMatch,
			backoffCnt:      3,
			backoffDetail:   []string{"regionNotInitialized+1", "tikvRPC+2"},
			regionIsValid:   false,
		},
	}
	runCaseAndCompare(ca)
	s.False(ca.accessErrInValid)
}

func TestReplicaSelectorAccessPath2(t *testing.T) {
	s := new(testRegionRequestToThreeStoresSuite)
	s.SetupTest()
	s.SetT(t)
	s.SetS(s)
	defer s.TearDownTest()

	s.NoError(failpoint.Enable("tikvclient/fastBackoffBySkipSleep", `return`))
	s.NoError(failpoint.Enable("tikvclient/skipStoreCheckUntilHealth", `return`))
	defer func() {
		s.NoError(failpoint.Disable("tikvclient/fastBackoffBySkipSleep"))
		s.NoError(failpoint.Disable("tikvclient/skipStoreCheckUntilHealth"))
	}()

	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	r := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(r)
	s.Equal(r.GetLeaderStoreID(), uint64(1)) // leader in store1.
	s.Equal(r.getStore().stores[0].labels[0].Key, "id")
	s.Equal(r.getStore().stores[0].labels[0].Value, fmt.Sprintf("%v", r.getStore().stores[0].storeID))

	randIntn = func(n int) int {
		return 0
	}
	f, err := os.Create("access_path_diff.txt")
	s.Nil(err)
	w := bufio.NewWriter(f)
	defer func() {
		w.Flush()
		f.Close()
	}()

	totalValidCaseCount := 0
	runCaseAndCompare := func(ca replicaSelectorAccessPathCase) {
		ca2 := ca
		config.UpdateGlobal(func(conf *config.Config) {
			conf.EnableReplicaSelectorV2 = false
		})
		ca.run(s)
		if ca.accessErrInValid {
			// ignore this invalid case
			return
		}

		config.UpdateGlobal(func(conf *config.Config) {
			conf.EnableReplicaSelectorV2 = true
		})
		ca2.run(s)
		msg := fmt.Sprintf("v1:%v\nv2:%v\n\n", ca.Format(), ca2.Format())
		if !ca.Equal(&ca2.accessPathResult) {
			_, err = w.WriteString(msg)
			s.Nil(err)
		}
		s.Equal(ca.respErr, ca2.respErr, msg)
		//s.Equal(ca.accessPath, ca2.accessPath, msg)
		//s.Equal(ca.respRegionError, ca2.respRegionError, msg)
		//s.Equal(ca.regionIsValid, ca2.regionIsValid, msg)
		//if ca.readType == kv.ReplicaReadLeader { // only leader read backoff case match.
		//	s.Equal(ca.backoffDetail, ca2.backoffDetail, msg)
		//}
		totalValidCaseCount++
	}

	totalCaseCount := 0
	testCase := func(req tikvrpc.CmdType, readType kv.ReplicaReadType, staleRead bool, timeout time.Duration, label *metapb.StoreLabel) {
		isRead := isReadReq(req)
		accessErrGen := newAccessErrGenerator(isRead, staleRead)
		for {
			accessErr, done := accessErrGen.genAccessErr(staleRead)
			if done {
				break
			}
			ca := replicaSelectorAccessPathCase{
				reqType:   req,
				readType:  readType,
				staleRead: staleRead,
				timeout:   timeout,
				label:     label,
				accessErr: accessErr,
			}
			runCaseAndCompare(ca)
			totalCaseCount++
		}
	}

	testCase(tikvrpc.CmdPrewrite, kv.ReplicaReadLeader, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadFollower, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadPreferLeader, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadLeader, false, time.Second, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, false, time.Second, &metapb.StoreLabel{Key: "id", Value: "3"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, nil)
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, 0, &metapb.StoreLabel{Key: "id", Value: "3"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, &metapb.StoreLabel{Key: "id", Value: "1"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, &metapb.StoreLabel{Key: "id", Value: "2"})
	testCase(tikvrpc.CmdGet, kv.ReplicaReadMixed, true, time.Second, &metapb.StoreLabel{Key: "id", Value: "3"})

	logutil.BgLogger().Info("TestReplicaSelectorAccessPath2 finished",
		zap.Int("total-case", totalCaseCount),
		zap.Int("valid-case", totalValidCaseCount),
		zap.Int("skipped-case", totalCaseCount-totalValidCaseCount))
}

type accessErrGenerator struct {
	mode      int
	idx       int
	baseIdx   int
	allErrs   []RegionErrorType
	retryErrs []RegionErrorType
}

func newAccessErrGenerator(isRead, staleRead bool) *accessErrGenerator {
	filter := func(tp RegionErrorType) bool {
		// read request won't meet RaftEntryTooLargeErr.
		if isRead && tp == RaftEntryTooLargeErr {
			return false
		}
		if staleRead == false && tp == DataIsNotReadyErr {
			return false
		}
		// TODO: since v2 has come compatibility issue with v1, so skip FlashbackInProgressErr.
		if tp == FlashbackInProgressErr {
			return false
		}
		return true
	}
	allErrs := getAllRegionErrors(filter)
	retryableErr := getAllRegionErrors(func(tp RegionErrorType) bool {
		return filter(tp) && isRegionErrorRetryable(tp)
	})
	return &accessErrGenerator{
		idx:       0,
		mode:      0,
		allErrs:   allErrs,
		retryErrs: retryableErr,
	}
}

func getAllRegionErrors(filter func(errorType RegionErrorType) bool) []RegionErrorType {
	errs := make([]RegionErrorType, 0, int(RegionErrorTypeMax))
	for tp := NotLeaderErr; tp < RegionErrorTypeMax; tp++ {
		if filter != nil && filter(tp) == false {
			continue
		}
		errs = append(errs, tp)
	}
	return errs
}

func isRegionErrorRetryable(tp RegionErrorType) bool {
	switch tp {
	case NotLeaderErr, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err, ServerIsBusyErr, ServerIsBusyWithEstimatedWaitMsErr,
		StaleCommandErr, MaxTimestampNotSyncedErr, ReadIndexNotReadyErr, ProposalInMergingModeErr, DataIsNotReadyErr,
		RegionNotInitializedErr, FlashbackInProgressErr, DiskFullErr, DeadLineExceededErr:
		return true
	}
	return false
}

func (a *accessErrGenerator) genAccessErr(staleRead bool) ([]RegionErrorType, bool) {
	if a.mode == 0 {
		a.idx = 0
		a.mode = 1
		return nil, false
	}
	if a.mode == 1 {
		idx := a.idx
		a.idx++
		if a.idx >= len(a.allErrs) {
			a.idx = 0
			a.baseIdx = 0
			a.mode = 2
		}
		return []RegionErrorType{a.allErrs[idx]}, false
	}
	for a.mode <= 7 {
		allErrs := a.allErrs
		retryErrs := a.retryErrs
		if a.mode > 4 {
			// if mode > 4, reduce the error type to avoid generating too many combinations.
			allErrs = getAllRegionErrors(func(tp RegionErrorType) bool {
				if staleRead == false && tp == DataIsNotReadyErr {
					return false
				}
				switch tp {
				case NotLeaderErr, NotLeaderWithNewLeader2Err, NotLeaderWithNewLeader3Err, ServerIsBusyWithEstimatedWaitMsErr, DataIsNotReadyErr,
					RegionNotInitializedErr, DeadLineExceededErr:
					return true
				}
				return false
			})
			retryErrs = allErrs
		}
		errs := a.genAccessErrs(allErrs, retryErrs)
		if len(errs) > 0 {
			return errs, false
		}
		a.baseIdx = 0
		a.idx = 0
		a.mode++
	}
	return nil, true
}

func (a *accessErrGenerator) genAccessErrs(allErrs, retryErrs []RegionErrorType) []RegionErrorType {
	defer func() {
		a.baseIdx++
		if a.baseIdx >= len(allErrs) {
			a.baseIdx = 0
			a.idx++
		}
	}()
	mode := a.mode
	errs := make([]RegionErrorType, mode)
	errs[mode-1] = allErrs[a.baseIdx%len(allErrs)]
	value := a.idx
	for i := mode - 2; i >= 0; i-- {
		if i == 0 && value > len(retryErrs) {
			return nil
		}
		errs[i] = retryErrs[value%len(retryErrs)]
		value = value / len(retryErrs)
	}
	return errs
}

func (ca *replicaSelectorAccessPathCase) run(s *testRegionRequestToThreeStoresSuite) {
	reachable.injectConstantLiveness(s.cache) // inject reachable liveness.
	msg := ca.Format()
	access := []string{}
	fnClient := &fnClient{fn: func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (response *tikvrpc.Response, err error) {
		idx := len(access)
		access = append(access, fmt.Sprintf("{addr: %v, replica-read: %v, stale-read: %v}", addr, req.ReplicaRead, req.StaleRead))
		if idx < len(ca.accessErr) {
			if !ca.accessErr[idx].Valid(addr, req) {
				// mark this case is invalid. just ignore this case.
				ca.accessErrInValid = true
			} else {
				loc, err := s.cache.LocateKey(s.bo, []byte("key"))
				s.Nil(err)
				rc := s.cache.GetCachedRegionWithRLock(loc.Region)
				s.NotNil(rc)
				regionErr, err := ca.genAccessErr(s, ca.accessErr[idx], rc)
				if regionErr != nil {
					return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
						RegionError: regionErr,
					}}, nil
				}
				if err != nil {
					return nil, err
				}
			}
		}
		return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
			Value: []byte("hello world"),
		}}, nil
	}}
	sender := NewRegionRequestSender(s.cache, fnClient)
	var req *tikvrpc.Request
	switch ca.reqType {
	case tikvrpc.CmdGet:
		req = tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key: []byte("key"),
		})
	case tikvrpc.CmdPrewrite:
		req = tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	default:
		s.FailNow("unsupported reqType " + ca.reqType.String())
	}
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
	// reset slow score, since serverIsBusyErr will mark the store is slow, and affect remaining test cases.
	loc, err := s.cache.LocateKey(s.bo, []byte("key"))
	s.Nil(err)
	rc := s.cache.GetCachedRegionWithRLock(loc.Region)
	s.NotNil(rc)
	for _, store := range rc.getStore().stores {
		store.slowScore.resetSlowScore()
		atomic.StoreUint32(&store.livenessState, uint32(reachable))
		store.setResolveState(resolved)
	}

	bo := retry.NewBackofferWithVars(context.Background(), 40000, nil)
	timeout := ca.timeout
	if timeout == 0 {
		timeout = client.ReadTimeoutShort
	}
	resp, _, _, err := sender.SendReqCtx(bo, req, loc.Region, timeout, tikvrpc.TiKV, opts...)
	if err == nil {
		s.NotNil(resp, msg)
		regionErr, err := resp.GetRegionError()
		s.Nil(err, msg)
		ca.respRegionError = regionErr
	} else {
		ca.respErr = err.Error()
	}
	ca.accessPath = access
	ca.backoffCnt = bo.GetTotalBackoffTimes()
	detail := make([]string, 0, len(bo.GetBackoffTimes()))
	for tp, cnt := range bo.GetBackoffTimes() {
		detail = append(detail, fmt.Sprintf("%v+%v", tp, cnt))
	}
	sort.Strings(detail)
	ca.backoffDetail = detail
	ca.regionIsValid = sender.replicaSelector.getBaseReplicaSelector().region.isValid()
	sender.replicaSelector.invalidateRegion() // invalidate region to reload for next test case.
}

func (ca *replicaSelectorAccessPathCase) genAccessErr(s *testRegionRequestToThreeStoresSuite, accessErr RegionErrorType, r *Region) (regionErr *errorpb.Error, err error) {
	genNotLeaderErr := func(storeID uint64) *errorpb.Error {
		var peerInStore *metapb.Peer
		for _, peer := range r.meta.Peers {
			if peer.StoreId == storeID {
				peerInStore = peer
				break
			}
		}
		return &errorpb.Error{
			NotLeader: &errorpb.NotLeader{
				RegionId: r.meta.Id,
				Leader:   peerInStore,
			},
		}
	}
	switch accessErr {
	case NotLeaderWithNewLeader2Err:
		regionErr = genNotLeaderErr(2)
	case NotLeaderWithNewLeader3Err:
		regionErr = genNotLeaderErr(3)
	default:
		regionErr, err = accessErr.GenError()
	}
	if err != nil {
		// inject unreachable liveness.
		unreachable.injectConstantLiveness(s.cache)
	}
	return regionErr, err
}

func (c *accessPathResult) Equal(c2 *accessPathResult) bool {
	str1 := fmt.Sprintf("%v\n%v\n%v", c.accessPath, c.respRegionError, c.regionIsValid)
	str2 := fmt.Sprintf("%v\n%v\n%v", c2.accessPath, c2.respRegionError, c2.regionIsValid)
	return str1 == str2
}

func (c *replicaSelectorAccessPathCase) Format() string {
	label := ""
	if c.label != nil {
		label = fmt.Sprintf("%v->%v", c.label.Key, c.label.Value)
	}
	respRegionError := ""
	if c.respRegionError != nil {
		respRegionError = c.respRegionError.String()
	}
	accessErr := make([]string, len(c.accessErr))
	for i := range c.accessErr {
		accessErr[i] = c.accessErr[i].String()
	}
	return fmt.Sprintf("{\n"+
		"\treq: %v\n"+
		"\tread_type: %v\n"+
		"\tstale_read: %v\n"+
		"\ttimeout: %v\n"+
		"\tlabel: %v\n"+
		"\taccess_err: %v\n"+
		"\taccess_path: %v\n"+
		"\tresp_err: %v\n"+
		"\tresp_region_err: %v\n"+
		"\tbackoff_cnt: %v\n"+
		"\tbackoff_detail: %v\n"+
		"\tregion_is_valid: %v\n}",
		c.reqType, c.readType, c.staleRead, c.timeout, label, strings.Join(accessErr, ", "), strings.Join(c.accessPath, ", "),
		c.respErr, respRegionError, c.backoffCnt, strings.Join(c.backoffDetail, ", "), c.regionIsValid)
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
