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

package locate

import (
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type replicaSelector struct {
	baseReplicaSelector
	replicaReadType kv.ReplicaReadType
	isStaleRead     bool
	isReadOnlyReq   bool
	option          storeSelectorOp
	target          *replica
	proxy           *replica
	attempts        int
}

func newReplicaSelector(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (*replicaSelector, error) {
	cachedRegion := regionCache.GetCachedRegionWithRLock(regionID)
	if cachedRegion == nil || !cachedRegion.isValid() {
		return nil, errors.New("cached region invalid")
	}
	replicas := buildTiKVReplicas(cachedRegion)
	option := storeSelectorOp{}
	for _, op := range opts {
		op(&option)
	}
	return &replicaSelector{
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

func (s *replicaSelector) next(bo *retry.Backoffer, req *tikvrpc.Request) (rpcCtx *RPCContext, err error) {
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

func (s *replicaSelector) nextForReplicaReadLeader(req *tikvrpc.Request) {
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
		idleTarget := mixedStrategy.next(s)
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
	s.target = mixedStrategy.next(s)
	if s.target != nil && s.isReadOnlyReq && s.replicas[leaderIdx].deadlineErrUsingConfTimeout {
		req.ReplicaRead = true
		req.StaleRead = false
	}
}

func (s *replicaSelector) nextForReplicaReadMixed(req *tikvrpc.Request) {
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
	s.target = strategy.next(s)
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
	// If leader.notLeader is true, it means the leader is already tried and received not leader error, then don't retry it.
	if leader.store.getLivenessState() != reachable ||
		leader.isExhausted(maxReplicaAttempt, maxReplicaAttemptTime) ||
		leader.deadlineErrUsingConfTimeout ||
		leader.notLeader ||
		leader.isEpochStale() { // check leader epoch here, if leader.epoch staled, we can try other replicas. instead of buildRPCContext failed and invalidate region then retry.
		return false
	}
	return true
}

// ReplicaSelectMixedStrategy is used to select a replica by calculating a score for each replica, and then choose the one with the highest score.
// Attention, if you want the leader replica must be chosen in some case, you should use ReplicaSelectLeaderStrategy, instead of use ReplicaSelectMixedStrategy with preferLeader flag.
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

func (s *ReplicaSelectMixedStrategy) next(selector *replicaSelector) *replica {
	replicas := selector.replicas
	maxScoreIdxes := make([]int, 0, len(replicas))
	maxScore := -1
	reloadRegion := false
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
		// todo: consider use store statistics information to select a faster one.
		idx := maxScoreIdxes[randIntn(len(maxScoreIdxes))]
		return replicas[idx]
	}
	if s.busyThreshold > 0 {
		// when can't find an idle replica, no need to invalidate region.
		return nil
	}
	// when meet deadline exceeded error, do fast retry without invalidate region cache.
	if !hasDeadlineExceededError(selector.replicas) {
		selector.invalidateRegion()
	}
	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
	return nil
}

func (s *ReplicaSelectMixedStrategy) isCandidate(r *replica, isLeader bool, epochStale bool, liveness livenessState) bool {
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
	if s.preferLeader && r.store.healthStatus.IsSlow() && !isLeader {
		// This logic is used to compatible with old replica selector.
		// When use prefer-leader strategy, if the replica is slow, and it's not leader, then it is not candidate, skip it.
		return false
	}
	return true
}

const (
	// The definition of the score is:
	// MSB                                                                               LSB
	// [unused bits][1 bit: LabelMatches][1 bit: PreferLeader][2 bits: NormalPeer + NotSlow]
	flagLabelMatches = 1 << 4
	flagPreferLeader = 1 << 3
	flagNormalPeer   = 1 << 2
	flagNotSlow      = 1 << 1
	flagNotAttempt   = 1
)

// calculateScore calculates the score of the replica.
func (s *ReplicaSelectMixedStrategy) calculateScore(r *replica, isLeader bool) int {
	score := 0
	if r.store.IsStoreMatch(s.stores) && r.store.IsLabelsMatch(s.labels) {
		score |= flagLabelMatches
	}
	if isLeader {
		if s.preferLeader {
			// Following logic is used to compatible with old replica selector.
			// When use prefer-leader strategy, only prefer leader when the leader's store is not slow.
			if !r.store.healthStatus.IsSlow() {
				score |= flagPreferLeader
			} else {
				score |= flagNormalPeer
			}
		} else if s.tryLeader {
			if len(s.labels) > 0 {
				// When the leader has matching labels, prefer leader than other mismatching peers.
				score |= flagPreferLeader
			} else {
				score |= flagNormalPeer
			}
		}
	} else {
		if s.learnerOnly {
			if r.peer.Role == metapb.PeerRole_Learner {
				score |= flagNormalPeer
			}
		} else {
			score |= flagNormalPeer
		}
	}
	if !r.store.healthStatus.IsSlow() {
		score |= flagNotSlow
	}
	if r.attempts == 0 {
		score |= flagNotAttempt
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

func (s *replicaSelector) onNotLeader(
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

func (s *replicaSelector) onFlashbackInProgress(ctx *RPCContext, req *tikvrpc.Request) bool {
	// if the failure is caused by replica read, we can retry it with leader safely.
	if req.ReplicaRead && s.target != nil && s.target.peer.Id != s.region.GetLeaderPeerID() {
		req.BusyThresholdMs = 0
		s.busyThreshold = 0
		s.replicaReadType = kv.ReplicaReadLeader
		req.ReplicaReadType = kv.ReplicaReadLeader
		return true
	}
	return false
}

func (s *replicaSelector) onDataIsNotReady() {
	if s.target != nil {
		s.target.dataIsNotReady = true
	}
}

func (s *replicaSelector) onServerIsBusy(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy,
) (shouldRetry bool, err error) {
	var store *Store
	if ctx != nil && ctx.Store != nil {
		store = ctx.Store
		if serverIsBusy.EstimatedWaitMs != 0 {
			ctx.Store.updateServerLoadStats(serverIsBusy.EstimatedWaitMs)
			if s.busyThreshold != 0 && isReadReq(req.Type) {
				// do not retry with batched coprocessor requests.
				// it'll be region misses if we send the tasks to replica.
				if req.Type == tikvrpc.CmdCop && len(req.Cop().Tasks) > 0 {
					return false, nil
				}
				if s.target != nil {
					s.target.serverIsBusy = true
				}
			}
		} else {
			// Mark the server is busy (the next incoming READs could be redirected to expected followers.)
			ctx.Store.healthStatus.markAlreadySlow()
		}
	}
	backoffErr := errors.Errorf("server is busy, ctx: %v", ctx)
	if s.canFastRetry() {
		s.addPendingBackoff(store, retry.BoTiKVServerBusy, backoffErr)
		return true, nil
	}
	err = bo.Backoff(retry.BoTiKVServerBusy, backoffErr)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *replicaSelector) canFastRetry() bool {
	if s.replicaReadType == kv.ReplicaReadLeader {
		leaderIdx := s.region.getStore().workTiKVIdx
		leader := s.replicas[leaderIdx]
		if isLeaderCandidate(leader) && !leader.serverIsBusy {
			return false
		}
	}
	return true
}

func (s *replicaSelector) onReadReqConfigurableTimeout(req *tikvrpc.Request) bool {
	if isReadReqConfigurableTimeout(req) {
		if s.target != nil {
			s.target.deadlineErrUsingConfTimeout = true
		}
		return true
	}
	return false
}

func (s *replicaSelector) onSendFailure(bo *retry.Backoffer, err error) {
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

func (s *replicaSelector) onSendSuccess(req *tikvrpc.Request) {
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

func (s *replicaSelector) replicaType() string {
	if s.target != nil {
		if s.target.peer.Id == s.region.GetLeaderPeerID() {
			return "leader"
		}
		return "follower"
	}
	return "unknown"
}

func (s *replicaSelector) String() string {
	if s == nil {
		return ""
	}
	return fmt.Sprintf("replicaSelectorV2{replicaReadType: %v, attempts: %v, %v}", s.replicaReadType.String(), s.attempts, s.baseReplicaSelector.String())
}
