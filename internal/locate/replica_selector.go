package locate

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
	"math/rand"
	"sync/atomic"
)

type ReplicaSelector interface {
	next(bo *retry.Backoffer, req *tikvrpc.Request) (*RPCContext, error)
	targetReplica() *replica
	proxyReplica() *replica
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
	replicaReadType kv.ReplicaReadType
	isStaleRead     bool
	isReadOnlyReq   bool

	regionCache *RegionCache
	region      *Region
	replicas    []*replica
	option      storeSelectorOp
	target      *replica
	attempts    int
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
		req.ReplicaReadType,
		req.StaleRead,
		isReadOnlyReq,
		regionCache,
		cachedRegion,
		replicas,
		option,
		nil,
		0,
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

const (
	maxLeaderReplicaAttempt   = 10
	maxFollowerReplicaAttempt = 1
)

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
	if r.isEpochStale() {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("stale_store").Inc()
		s.invalidateRegion()
		return nil, nil
	}
	rpcCtx := &RPCContext{
		Region:     s.region.VerID(),
		Meta:       s.region.meta,
		Peer:       r.peer,
		Store:      r.store,
		AccessMode: tiKVOnly,
		TiKVNum:    len(s.replicas),
	}
	// Set leader addr
	addr, err := s.regionCache.getStoreAddr(bo, s.region, r.store)
	if err != nil {
		return nil, err
	}
	if len(addr) == 0 {
		return nil, nil
	}
	rpcCtx.Addr = addr
	r.attempts++
	return rpcCtx, nil
}

func (s *replicaSelectorV2) onNotLeader(
	bo *retry.Backoffer, ctx *RPCContext, notLeader *errorpb.NotLeader,
) (shouldRetry bool, err error) {
	leader := notLeader.GetLeader()
	if s.target != nil {
		s.target.notLeader = true
	}
	if leader == nil {
		if err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("no leader, ctx: %v", ctx)); err != nil {
			return false, err
		}
	} else {
		s.updateLeader(leader)
	}
	return true, nil
}

func (s *replicaSelectorV2) updateLeader(leader *metapb.Peer) {
	if leader == nil {
		return
	}
	for _, replica := range s.replicas {
		if isSamePeer(replica.peer, leader) {
			// If hibernate region is enabled and the leader is not reachable, the raft group
			// will not be wakened up and re-elect the leader until the follower receives
			// a request. So, before the new leader is elected, we should not send requests
			// to the unreachable old leader to avoid unnecessary timeout.
			if replica.store.getLivenessState() != reachable {
				return
			}
			if replica.isExhausted(maxReplicaAttempt, maxReplicaAttemptTime) {
				// Give the replica one more chance and because each follower is tried only once,
				// it won't result in infinite retry.
				replica.attempts = maxReplicaAttempt - 1
				replica.attemptedTime = 0
			}
			replica.notLeader = false
			s.replicaReadType = kv.ReplicaReadLeader
			// Update the workTiKVIdx so that following requests can be sent to the leader immediately.
			if !s.region.switchWorkLeaderToPeer(leader) {
				panic("the store must exist")
			}
			logutil.BgLogger().Debug(
				"switch region leader to specific leader due to kv return NotLeader",
				zap.Uint64("regionID", s.region.GetID()),
				zap.Uint64("leaderStoreID", leader.GetStoreId()),
			)
			return
		}
	}
	// Invalidate the region since the new leader is not in the cached version.
	s.region.invalidate(StoreNotFound)
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
	if req.MaxExecutionDurationMs >= uint64(client.ReadTimeoutShort.Milliseconds()) {
		// Configurable timeout should less than `ReadTimeoutShort`.
		return false
	}
	switch req.Type {
	case tikvrpc.CmdGet, tikvrpc.CmdBatchGet, tikvrpc.CmdScan,
		tikvrpc.CmdCop, tikvrpc.CmdBatchCop, tikvrpc.CmdCopStream:
		if target := s.targetReplica(); target != nil {
			target.deadlineErrUsingConfTimeout = true
		}
		return true
	default:
		// Only work for read requests, return false for non-read requests.
		return false
	}
}

func (s *replicaSelectorV2) onSendFailure(bo *retry.Backoffer, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	// todo: mark store need check and return to fast retry.
	liveness := s.checkLiveness(bo, s.targetReplica())
	if liveness != reachable {
		s.invalidateReplicaStore(s.targetReplica(), err)
	}
}

func (s *replicaSelectorV2) invalidateReplicaStore(replica *replica, cause error) {
	store := replica.store
	if atomic.CompareAndSwapUint32(&store.epoch, replica.epoch, replica.epoch+1) {
		logutil.BgLogger().Info(
			"mark store's regions need be refill",
			zap.Uint64("id", store.storeID),
			zap.String("addr", store.addr),
			zap.Error(cause),
		)
		metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
		// schedule a store addr resolve.
		s.regionCache.markStoreNeedCheck(store)
		store.markAlreadySlow()
	}
}

func (s *replicaSelectorV2) checkLiveness(bo *retry.Backoffer, accessReplica *replica) livenessState {
	return accessReplica.store.requestLivenessAndStartHealthCheckLoopIfNeeded(bo, s.regionCache)
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

func (s *replicaSelectorV2) invalidateRegion() {
	if s.region != nil {
		s.region.invalidate(Other)
	}
}

func (s *replicaSelectorV2) String() string {
	var replicaStatus []string
	cacheRegionIsValid := "unknown"
	if s != nil {
		s.replicaReadType.String()
		if s.region != nil {
			if s.region.isValid() {
				cacheRegionIsValid = "true"
			} else {
				cacheRegionIsValid = "false"
			}
		}
		for _, replica := range s.replicas {
			replicaStatus = append(replicaStatus, fmt.Sprintf("peer: %v, store: %v, isEpochStale: %v, "+
				"attempts: %v, replica-epoch: %v, store-epoch: %v, store-state: %v, store-liveness-state: %v",
				replica.peer.GetId(),
				replica.store.storeID,
				replica.isEpochStale(),
				replica.attempts,
				replica.getEpoch(),
				atomic.LoadUint32(&replica.store.epoch),
				replica.store.getResolveState(),
				replica.store.getLivenessState(),
			))
		}
	}
	return fmt.Sprintf("replicaSelectorV2{replicaReadType: %v, cacheRegionIsValid: %v, replicaStatus: %v}", s.replicaReadType, cacheRegionIsValid, replicaStatus)
}
