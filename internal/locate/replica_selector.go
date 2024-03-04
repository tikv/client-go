// Copyright 2024 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_cache.go
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locate

import (
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type ReplicaSelector interface {
	next(bo *retry.Backoffer, req *tikvrpc.Request) (*RPCContext, error)
	targetReplica() *replica
	proxyReplica() *replica
	replicaType(rpcCtx *RPCContext) string
	String() string
	getBaseReplicaSelector() *baseReplicaSelector
	getLabels() []*metapb.StoreLabel
	onSendSuccess(req *tikvrpc.Request)
	onSendFailure(bo *retry.Backoffer, err error)
	invalidateRegion()
	// Following methods are used to handle region errors.
	onNotLeader(bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader) (shouldRetry bool, err error)
	onFlashbackInProgress(ctx *RPCContext, req *tikvrpc.Request) (handled bool)
	onDataIsNotReady()
	onServerIsBusy(bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy) (shouldRetry bool, err error)
	onReadReqConfigurableTimeout(req *tikvrpc.Request) bool
}

// NewReplicaSelector returns a new ReplicaSelector.
func NewReplicaSelector(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (ReplicaSelector, error) {
	if config.GetGlobalConfig().EnableReplicaSelectorV2 {
		return newReplicaSelectorV2(regionCache, regionID, req, opts...)
	}
	return newReplicaSelector(regionCache, regionID, req, opts...)
}

type replicaSelectorV2 struct {
	baseReplicaSelector
	replicaReadType kv.ReplicaReadType
	isStaleRead     bool
	isReadOnlyReq   bool
	option          storeSelectorOp
	target          *replica
	proxy           *replica
	attempts        int
}

func newReplicaSelectorV2(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (*replicaSelectorV2, error) {
	cachedRegion := regionCache.GetCachedRegionWithRLock(regionID)
	if cachedRegion == nil || !cachedRegion.isValid() {
		return nil, errors.New("cached region invalid")
	}
	replicas := buildTiKVReplicas(cachedRegion)
	option := storeSelectorOp{}
	for _, op := range opts {
		op(&option)
	}
	return &replicaSelectorV2{
		baseReplicaSelector: baseReplicaSelector{
			regionCache:   regionCache,
			region:        cachedRegion,
			replicas:      replicas,
			busyThreshold: time.Duration(req.BusyThresholdMs) * time.Millisecond,
		},
		replicaReadType: req.ReplicaReadType,
		isStaleRead:     req.StaleRead,
		isReadOnlyReq:   isReadReq(req.Type),
		option:          option,
		target:          nil,
		attempts:        0,
	}, nil
}

func (s *replicaSelectorV2) next(bo *retry.Backoffer, req *tikvrpc.Request) (rpcCtx *RPCContext, err error) {
	if !s.region.isValid() {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalid").Inc()
		return nil, nil
	}

	s.attempts++
	s.target = nil
	s.proxy = nil
	switch s.replicaReadType {
	case kv.ReplicaReadLeader:
		s.nextForReplicaReadLeader(req)
	default:
		s.nextForReplicaReadMixed(req)
	}
	if s.target == nil {
		return nil, nil
	}
	return s.buildRPCContext(bo, s.target, s.proxy)
}

func (s *replicaSelectorV2) nextForReplicaReadLeader(req *tikvrpc.Request) {
	if s.regionCache.enableForwarding {
		strategy := ReplicaSelectLeaderWithProxyStrategy{}
		s.target, s.proxy = strategy.next(s.replicas, s.region)
		if s.target != nil && s.proxy != nil {
			return
		}
	}
	leaderIdx := s.region.getStore().workTiKVIdx
	strategy := ReplicaSelectLeaderStrategy{leaderIdx: leaderIdx}
	s.target = strategy.next(s.replicas)
	if s.target != nil && s.busyThreshold > 0 && s.isReadOnlyReq && (s.target.store.EstimatedWaitTime() > s.busyThreshold || s.target.serverIsBusy) {
		// If the leader is busy in our estimation, try other idle replicas.
		// If other replicas are all busy, tryIdleReplica will try the leader again without busy threshold.
		mixedStrategy := ReplicaSelectMixedStrategy{leaderIdx: leaderIdx, busyThreshold: s.busyThreshold}
		idleTarget := mixedStrategy.next(s, s.region)
		if idleTarget != nil {
			s.target = idleTarget
			req.ReplicaRead = true
		} else {
			// No threshold if all peers are too busy, remove busy threshold and still use leader.
			s.busyThreshold = 0
			req.BusyThresholdMs = 0
			req.ReplicaRead = false
		}
	}
	if s.target != nil {
		return
	}
	mixedStrategy := ReplicaSelectMixedStrategy{leaderIdx: leaderIdx, leaderOnly: s.option.leaderOnly}
	s.target = mixedStrategy.next(s, s.region)
	if s.target != nil && s.isReadOnlyReq && s.replicas[leaderIdx].deadlineErrUsingConfTimeout {
		req.ReplicaRead = true
		req.StaleRead = false
	}
}

func (s *replicaSelectorV2) nextForReplicaReadMixed(req *tikvrpc.Request) {
	leaderIdx := s.region.getStore().workTiKVIdx
	if s.isStaleRead && s.attempts == 2 {
		// For stale read second retry, try leader by leader read.
		strategy := ReplicaSelectLeaderStrategy{leaderIdx: leaderIdx}
		s.target = strategy.next(s.replicas)
		if s.target != nil && !s.target.isExhausted(1, 0) {
			// For stale read, don't retry leader again if it is accessed at the first attempt.
			req.StaleRead = false
			req.ReplicaRead = false
			return
		}
	}
	preferLeader := req.ReplicaReadType == kv.ReplicaReadPreferLeader
	if s.attempts > 1 {
		if req.ReplicaReadType == kv.ReplicaReadMixed {
			// For mixed read retry, prefer retry leader first.
			preferLeader = true
		}
	}
	strategy := ReplicaSelectMixedStrategy{
		leaderIdx:    leaderIdx,
		tryLeader:    req.ReplicaReadType == kv.ReplicaReadMixed || req.ReplicaReadType == kv.ReplicaReadPreferLeader,
		preferLeader: preferLeader,
		leaderOnly:   s.option.leaderOnly,
		learnerOnly:  req.ReplicaReadType == kv.ReplicaReadLearner,
		labels:       s.option.labels,
		stores:       s.option.stores,
	}
	s.target = strategy.next(s, s.region)
	if s.target != nil {
		if s.isStaleRead && s.attempts == 1 {
			// stale-read request first access.
			if !s.target.store.IsLabelsMatch(s.option.labels) && s.target.peer.Id != s.region.GetLeaderPeerID() {
				// If the target replica's labels is not match and not leader, use replica read.
				// This is for compatible with old version.
				req.StaleRead = false
				req.ReplicaRead = true
			} else {
				// use stale read.
				req.StaleRead = true
				req.ReplicaRead = false
			}
		} else {
			// always use replica.
			req.StaleRead = false
			req.ReplicaRead = s.isReadOnlyReq
		}
	}
}

type ReplicaSelectLeaderStrategy struct {
	leaderIdx AccessIndex
}

func (s ReplicaSelectLeaderStrategy) next(replicas []*replica) *replica {
	leader := replicas[s.leaderIdx]
	if isLeaderCandidate(leader) {
		return leader
	}
	return nil
}

// check leader is candidate or not.
func isLeaderCandidate(leader *replica) bool {
	// If hibernate region is enabled and the leader is not reachable, the raft group
	// will not be wakened up and re-elect the leader until the follower receives
	// a request. So, before the new leader is elected, we should not send requests
	// to the unreachable old leader to avoid unnecessary timeout.
	// If leader.deadlineErrUsingConfTimeout is true, it means the leader is already tried and received deadline exceeded error, then don't retry it.
	if leader.store.getLivenessState() != reachable ||
		leader.isExhausted(maxReplicaAttempt, maxReplicaAttemptTime) ||
		leader.deadlineErrUsingConfTimeout ||
		leader.notLeader ||
		leader.isEpochStale() { // check leader epoch here, if leader.epoch staled, we can try other replicas. instead of buildRPCContext failed and invalidate region then retry.
		return false
	}
	return true
}

type ReplicaSelectMixedStrategy struct {
	leaderIdx     AccessIndex
	tryLeader     bool
	preferLeader  bool
	leaderOnly    bool
	learnerOnly   bool
	labels        []*metapb.StoreLabel
	stores        []uint64
	busyThreshold time.Duration
}

func (s *ReplicaSelectMixedStrategy) next(selector *replicaSelectorV2, region *Region) *replica {
	replicas := selector.replicas
	maxScoreIdxes := make([]int, 0, len(replicas))
	maxScore := -1
	reloadRegion := false
	//fmt.Printf("-------------begin------%#v---%v--\n", s, selector.replicaReadType)
	for i, r := range replicas {
		epochStale := r.isEpochStale()
		liveness := r.store.getLivenessState()
		isLeader := AccessIndex(i) == s.leaderIdx
		if epochStale && ((liveness == reachable && r.store.getResolveState() == resolved) || isLeader) {
			reloadRegion = true
		}
		if !s.isCandidate(r, isLeader, epochStale, liveness) {
			continue
		}
		score := s.calculateScore(r, isLeader)
		//fmt.Printf("replica store %v, score: %v  %v, %v-----------\n", r.store.storeID, score, isLeader, s.labels)
		if score > maxScore {
			maxScore = score
			maxScoreIdxes = append(maxScoreIdxes[:0], i)
		} else if score == maxScore && score > -1 {
			maxScoreIdxes = append(maxScoreIdxes, i)
		}
	}
	if reloadRegion {
		selector.region.setSyncFlags(needDelayedReloadPending)
	}
	if len(maxScoreIdxes) == 1 {
		idx := maxScoreIdxes[0]
		return replicas[idx]
	} else if len(maxScoreIdxes) > 1 {
		// if there are more than one replica with the same max score, we will randomly select one
		// todo: consider use store slow score to select a faster one.
		idx := maxScoreIdxes[randIntn(len(maxScoreIdxes))]
		return replicas[idx]
	}
	if s.busyThreshold > 0 {
		// when can't find an idle replica, no need to invalidate region.
		return nil
	}

	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
	for _, r := range replicas {
		if r.deadlineErrUsingConfTimeout {
			// when meet deadline exceeded error, do fast retry without invalidate region cache.
			return nil
		}
	}
	selector.invalidateRegion() // is exhausted, Is need to invalidate the region?
	return nil
}

func (s ReplicaSelectMixedStrategy) isCandidate(r *replica, isLeader bool, epochStale bool, liveness livenessState) bool {
	if epochStale || liveness == unreachable {
		// the replica is not available, skip it.
		return false
	}
	maxAttempt := 1
	if r.dataIsNotReady && !isLeader {
		// If the replica is failed by data not ready with stale read, we can retry it with replica-read.
		// 	after https://github.com/tikv/tikv/pull/15726, the leader will not return DataIsNotReady error,
		//	then no need to retry leader again, if you try it again, you may got a NotLeader error.
		maxAttempt = 2
	}
	if r.isExhausted(maxAttempt, 0) {
		// attempts is exhausted, skip it.
		return false
	}
	if s.leaderOnly && !isLeader {
		return false
	}
	if s.busyThreshold > 0 && (r.store.EstimatedWaitTime() > s.busyThreshold || r.serverIsBusy || isLeader) {
		return false
	}
	return true
}

const (
	// define the score of priority.
	scoreOfLabelMatch   = 3
	scoreOfPreferLeader = 2
	scoreOfNormalPeer   = 1
	scoreOfNotSlow      = 1
)

func (s *ReplicaSelectMixedStrategy) calculateScore(r *replica, isLeader bool) int {
	score := 0
	if isLeader {
		if s.preferLeader {
			score += scoreOfPreferLeader
		} else if s.tryLeader {
			if len(s.labels) > 0 {
				// when has match labels, prefer leader than not-matched peers.
				score += scoreOfPreferLeader
			} else {
				//fmt.Printf("store %v is normal peer , isleader: %v   %v------\n", r.store.storeID, isLeader, score)
				score += scoreOfNormalPeer
			}
		}
	} else {
		if s.learnerOnly {
			if r.peer.Role == metapb.PeerRole_Learner {
				//fmt.Printf("store %v is normal peer , isleader: %v   %v------\n", r.store.storeID, isLeader, score)
				score += scoreOfNormalPeer
			}
		} else {
			//fmt.Printf("store %v is normal peer , isleader: %v   %v------\n", r.store.storeID, isLeader, score)
			score += scoreOfNormalPeer
		}
	}
	if r.store.IsStoreMatch(s.stores) && r.store.IsLabelsMatch(s.labels) {
		score += scoreOfLabelMatch
	}
	if !r.store.healthStatus.IsSlow() {
		score += scoreOfNotSlow
		//fmt.Printf("store %v is not slow ------\n", r.store.storeID)
	}
	if score > 0 && r.attempts > 0 {
		// if the replica already tried, decrease the score.
		score = score - 1
	}
	return score
}

type ReplicaSelectLeaderWithProxyStrategy struct{}

func (s ReplicaSelectLeaderWithProxyStrategy) next(replicas []*replica, region *Region) (leader *replica, proxy *replica) {
	rs := region.getStore()
	leaderIdx := rs.workTiKVIdx
	leader = replicas[leaderIdx]
	if leader.store.getLivenessState() == reachable || leader.notLeader {
		// if leader's store is reachable, no need use proxy.
		rs.unsetProxyStoreIfNeeded(region)
		return nil, nil
	}
	proxyIdx := rs.proxyTiKVIdx
	if proxyIdx >= 0 && int(proxyIdx) < len(replicas) && s.isCandidate(replicas[proxyIdx], proxyIdx == leaderIdx) {
		return leader, replicas[proxyIdx]
	}

	for i, r := range replicas {
		if s.isCandidate(r, AccessIndex(i) == leaderIdx) {
			return leader, r
		}
	}
	return nil, nil
}

func (s ReplicaSelectLeaderWithProxyStrategy) isCandidate(r *replica, isLeader bool) bool {
	if isLeader ||
		r.isExhausted(1, 0) ||
		r.store.getLivenessState() != reachable ||
		r.isEpochStale() {
		// check epoch here, if epoch staled, we can try other replicas. instead of buildRPCContext failed and invalidate region then retry.
		return false
	}
	return true
}

func (s *replicaSelectorV2) onNotLeader(
	bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {
	if s.target != nil {
		s.target.notLeader = true
	}
	leaderIdx, err := s.baseReplicaSelector.onNotLeader(bo, ctx, notLeader)
	if err != nil {
		return false, err
	}
	if leaderIdx >= 0 {
		if isLeaderCandidate(s.replicas[leaderIdx]) {
			s.replicaReadType = kv.ReplicaReadLeader
		}
	}
	return true, nil
}

func (s *replicaSelectorV2) onFlashbackInProgress(ctx *RPCContext, req *tikvrpc.Request) bool {
	// if the failure is caused by replica read, we can retry it with leader safely.
	if req.ReplicaRead && s.target != nil && s.target.peer.Id != s.region.GetLeaderPeerID() {
		req.BusyThresholdMs = 0
		s.busyThreshold = 0
		ctx.contextPatcher.replicaRead = nil
		ctx.contextPatcher.busyThreshold = nil
		s.option.labels = nil // clear label, since we need to retry with leader.
		s.replicaReadType = kv.ReplicaReadLeader
		req.ReplicaReadType = kv.ReplicaReadLeader
		return true
	}
	return false
}

func (s *replicaSelectorV2) onDataIsNotReady() {
	if s.target != nil {
		s.target.dataIsNotReady = true
	}
}

func (s *replicaSelectorV2) onServerIsBusy(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy,
) (shouldRetry bool, err error) {
	if ctx != nil && ctx.Store != nil {
		ctx.Store.updateServerLoadStats(serverIsBusy.EstimatedWaitMs)
		if serverIsBusy.EstimatedWaitMs != 0 {
			if s.busyThreshold != 0 {
				// do not retry with batched coprocessor requests.
				// it'll be region misses if we send the tasks to replica.
				if req.Type == tikvrpc.CmdCop && len(req.Cop().Tasks) > 0 {
					return false, nil
				}
				if s.target != nil {
					s.target.serverIsBusy = true
				}
				return true, nil
			}
		} else if s.replicaReadType != kv.ReplicaReadLeader {
			// fast retry next follower
			return true, nil
		}
	}
	err = bo.Backoff(retry.BoTiKVServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *replicaSelectorV2) onReadReqConfigurableTimeout(req *tikvrpc.Request) bool {
	if isReadReqConfigurableTimeout(req) {
		if s.target != nil {
			s.target.deadlineErrUsingConfTimeout = true
		}
		return true
	}
	return false
}

func (s *replicaSelectorV2) onSendFailure(bo *retry.Backoffer, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	// todo: mark store need check and return to fast retry.
	target := s.target
	if s.proxy != nil {
		target = s.proxy
	}
	liveness := s.checkLiveness(bo, target)
	if s.replicaReadType == kv.ReplicaReadLeader && s.proxy == nil && s.target != nil && s.target.peer.Id == s.region.GetLeaderPeerID() &&
		liveness == unreachable && len(s.replicas) > 1 && s.regionCache.enableForwarding {
		// just return to use proxy.
		return
	}
	if liveness != reachable {
		s.invalidateReplicaStore(target, err)
	}
}

func (s *replicaSelectorV2) onSendSuccess(req *tikvrpc.Request) {
	if s.proxy != nil && s.target != nil {
		for idx, r := range s.replicas {
			if r.peer.Id == s.proxy.peer.Id {
				s.region.getStore().setProxyStoreIdx(s.region, AccessIndex(idx))
				break
			}
		}
	}
	if s.target != nil && s.target.peer.Id != s.region.GetLeaderPeerID() && req != nil && !req.StaleRead && !req.ReplicaRead {
		s.region.switchWorkLeaderToPeer(s.target.peer)
	}
}

func (s *replicaSelectorV2) targetReplica() *replica {
	return s.target
}

func (s *replicaSelectorV2) proxyReplica() *replica {
	return s.proxy
}

func (s *replicaSelectorV2) getLabels() []*metapb.StoreLabel {
	return s.option.labels
}

func (s *replicaSelectorV2) replicaType(_ *RPCContext) string {
	if s.target != nil {
		if s.target.peer.Id == s.region.GetLeaderPeerID() {
			return "leader"
		}
		return "follower"
	}
	return "unknown"
}

func (s *replicaSelectorV2) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("replicaSelectorV2{replicaReadType: %v, attempts: %v, %v}", s.replicaReadType.String(), s.attempts, s.baseReplicaSelector.String())
}
