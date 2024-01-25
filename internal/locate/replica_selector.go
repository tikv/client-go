package locate

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"math/rand"
)

type ReplicaSelector interface {
	next(bo *retry.Backoffer, req *tikvrpc.Request) (*RPCContext, error)
	targetReplica() *replica
	proxyReplica() *replica
	getAllReplicas() []*replica
	replicaType(rpcCtx *RPCContext) string
	invalidateRegion()
	onSendSuccess(req *tikvrpc.Request)
	String() string
	onSendFailure(bo *retry.Backoffer, err error)
	// Following methods are used to handle region errors.
	onNotLeader(bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader) (shouldRetry bool, err error)
	onFlashbackInProgress()
	onDataIsNotReady()
	onServerIsBusy(bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy) (shouldRetry bool, err error)
	onReadReqConfigurableTimeout(req *tikvrpc.Request) bool
}

func NewReplicaSelector(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (ReplicaSelector, error) {
	//v2Enabled := false
	v2Enabled := true
	if v2Enabled && regionCache.enableForwarding == false && req.BusyThresholdMs == 0 {
		return newReplicaSelectorV2(regionCache, regionID, req, opts...)
	}
	return newReplicaSelector(regionCache, regionID, req, opts...)
}

type replicaSelectorV2 struct {
	baseReplicaSelector
	replicaReadType kv.ReplicaReadType
	isStaleRead     bool
	isReadOnlyReq   bool

	option   storeSelectorOp
	target   *replica
	attempts int
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

	isReadOnlyReq := false
	switch req.Type {
	case tikvrpc.CmdGet, tikvrpc.CmdBatchGet, tikvrpc.CmdScan,
		tikvrpc.CmdCop, tikvrpc.CmdBatchCop, tikvrpc.CmdCopStream:
		isReadOnlyReq = true
	}

	return &replicaSelectorV2{
		baseReplicaSelector: baseReplicaSelector{
			regionCache: regionCache,
			region:      cachedRegion,
			replicas:    replicas,
		},
		replicaReadType: req.ReplicaReadType,
		isStaleRead:     req.StaleRead,
		isReadOnlyReq:   isReadOnlyReq,
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
	switch s.replicaReadType {
	case kv.ReplicaReadLeader:
		strategy := ReplicaSelectLeaderStrategy{}
		s.target = strategy.next(s.replicas, s.region)
		if s.target == nil {
			strategy := ReplicaSelectMixedStrategy{}
			s.target = strategy.next(s, s.region)
		}
	default:
		if s.isStaleRead && s.attempts == 2 {
			// For stale read second retry, try leader by leader read.
			strategy := ReplicaSelectLeaderStrategy{}
			s.target = strategy.next(s.replicas, s.region)
			if s.target != nil {
				req.StaleRead = false
				req.ReplicaRead = false
			}
		}
		if s.target == nil {
			strategy := ReplicaSelectMixedStrategy{
				tryLeader:    req.ReplicaReadType == kv.ReplicaReadMixed || req.ReplicaReadType == kv.ReplicaReadPreferLeader,
				preferLeader: req.ReplicaReadType == kv.ReplicaReadPreferLeader,
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
	}
	if s.target == nil {
		return nil, nil
	}
	return s.buildRPCContext(bo, s.target)
}

type ReplicaSelectLeaderStrategy struct{}

func (s ReplicaSelectLeaderStrategy) next(replicas []*replica, region *Region) *replica {
	leader := replicas[region.getStore().workTiKVIdx]
	if leader.store.getLivenessState() == reachable && !leader.isExhausted(maxReplicaAttempt, maxReplicaAttemptTime) &&
		!leader.deadlineErrUsingConfTimeout && !leader.notLeader {
		if !leader.isEpochStale() { // check leader epoch here, if leader.epoch failed, we can try other replicas. instead of buildRPCContext failed and invalidate region then retry.
			return leader
		}
	}
	return nil
}

type ReplicaSelectMixedStrategy struct {
	tryLeader    bool
	preferLeader bool
	learnerOnly  bool
	labels       []*metapb.StoreLabel
	stores       []uint64
}

func (s *ReplicaSelectMixedStrategy) next(selector *replicaSelectorV2, region *Region) *replica {
	leaderIdx := region.getStore().workTiKVIdx
	replicas := selector.replicas
	maxScoreIdxes := make([]int, 0, len(replicas))
	maxScore := -1
	reloadRegion := false
	for i, r := range replicas {
		epochStale := r.isEpochStale()
		liveness := r.store.getLivenessState()
		if epochStale && ((liveness == reachable && r.store.getResolveState() == resolved) || AccessIndex(i) == leaderIdx) {
			reloadRegion = true
		}
		if epochStale || liveness == unreachable {
			// the replica is not available or attempts is exhausted, skip it.
			continue
		}
		maxAttempt := 1
		if r.dataIsNotReady {
			// If the replica is failed by data not ready with stale read, we can retry it with stale read.
			maxAttempt = 2
		}
		if r.isExhausted(maxAttempt, 0) {
			continue
		}
		score := s.calculateScore(r, AccessIndex(i) == leaderIdx)
		if score > maxScore {
			maxScore = score
			maxScoreIdxes = append(maxScoreIdxes[:0], i)
		} else if score == maxScore && score > -1 {
			maxScoreIdxes = append(maxScoreIdxes, i)
		}
	}
	if reloadRegion {
		selector.regionCache.scheduleReloadRegion(selector.region)
	}
	if len(maxScoreIdxes) == 1 {
		idx := maxScoreIdxes[0]
		return replicas[idx]
	} else if len(maxScoreIdxes) > 1 {
		// if there are more than one replica with the same max score, we will randomly select one
		// todo: use store slow score to select a faster one.
		idx := maxScoreIdxes[rand.Intn(len(maxScoreIdxes))]
		return replicas[idx]
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
				score += scoreOfNormalPeer
			}
		}
	} else {
		if s.learnerOnly {
			if r.peer.Role == metapb.PeerRole_Learner {
				score += scoreOfNormalPeer
			}
		} else {
			score += scoreOfNormalPeer
		}
	}
	if r.store.IsStoreMatch(s.stores) && r.store.IsLabelsMatch(s.labels) {
		score += scoreOfLabelMatch
	}
	if !r.store.isSlow() {
		score += scoreOfNotSlow
	}
	return score
}

func (s *replicaSelectorV2) buildRPCContext(bo *retry.Backoffer, r *replica) (*RPCContext, error) {
	return s.baseReplicaSelector.buildRPCContext(bo, r, nil)
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
		s.replicaReadType = kv.ReplicaReadLeader
	}
	return true, nil
}

func (s *replicaSelectorV2) onFlashbackInProgress() {}

func (s *replicaSelectorV2) onDataIsNotReady() {
	if s.target != nil {
		s.target.dataIsNotReady = true
	}
}

func (s *replicaSelectorV2) onServerIsBusy(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy,
) (shouldRetry bool, err error) {
	// todo: ?
	err = bo.Backoff(retry.BoTiKVServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *replicaSelectorV2) onReadReqConfigurableTimeout(req *tikvrpc.Request) bool {
	if isReadReqConfigurableTimeout(req) {
		if target := s.targetReplica(); target != nil {
			target.deadlineErrUsingConfTimeout = true
		}
		return true
	}
	return false
}

func (s *replicaSelectorV2) onSendFailure(bo *retry.Backoffer, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	// todo: mark store need check and return to fast retry.
	liveness := s.checkLiveness(bo, s.targetReplica())
	if liveness != reachable {
		s.invalidateReplicaStore(s.targetReplica(), err)
	}
}

func (s *replicaSelectorV2) onSendSuccess(req *tikvrpc.Request) {
	if s.target != nil && s.target.peer.Id != s.region.GetLeaderPeerID() && req != nil && req.StaleRead == false && req.ReplicaRead == false {
		s.region.switchWorkLeaderToPeer(s.target.peer)
	}
}

func (s *replicaSelectorV2) targetReplica() *replica {
	return s.target
}

func (s *replicaSelectorV2) proxyReplica() *replica {
	return nil
}

func (s *replicaSelectorV2) replicaType(rpcCtx *RPCContext) string {
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
	return fmt.Sprintf("replicaSelectorV2{replicaReadType: %v, %v}", s.replicaReadType.String(), s.baseReplicaSelector.String())
}
