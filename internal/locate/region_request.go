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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_request.go
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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pderr "github.com/tikv/pd/client/errs"
)

// shuttingDown is a flag to indicate tidb-server is exiting (Ctrl+C signal
// receved for example). If this flag is set, tikv client should not retry on
// network error because tidb-server expect tikv client to exit as soon as possible.
var shuttingDown uint32

// StoreShuttingDown atomically stores ShuttingDown into v.
func StoreShuttingDown(v uint32) {
	atomic.StoreUint32(&shuttingDown, v)
}

// LoadShuttingDown atomically loads ShuttingDown.
func LoadShuttingDown() uint32 {
	return atomic.LoadUint32(&shuttingDown)
}

// RegionRequestSender sends KV/Cop requests to tikv server. It handles network
// errors and some region errors internally.
//
// Typically, a KV/Cop request is bind to a region, all keys that are involved
// in the request should be located in the region.
// The sending process begins with looking for the address of leader store's
// address of the target region from cache, and the request is then sent to the
// destination tikv server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region
// merge, or region balance, tikv server may not able to process request and
// send back a RegionError.
// RegionRequestSender takes care of errors that does not relevant to region
// range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'. If fails to
// send the request to all replicas, a fake rregion error may be returned.
// Caller which receives the error should retry the request.
//
// For other region errors, since region range have changed, the request may need to
// split, so we simply return the error to caller.
type RegionRequestSender struct {
	regionCache       *RegionCache
	apiVersion        kvrpcpb.APIVersion
	client            client.Client
	storeAddr         string
	rpcError          error
	replicaSelector   *replicaSelector
	failStoreIDs      map[uint64]struct{}
	failProxyStoreIDs map[uint64]struct{}
	RegionRequestRuntimeStats
}

func (s *RegionRequestSender) String() string {
	return fmt.Sprintf("{rpcError:%v,replicaSelector: %v}", s.rpcError, s.replicaSelector.String())
}

// RegionRequestRuntimeStats records the runtime stats of send region requests.
type RegionRequestRuntimeStats struct {
	Stats map[tikvrpc.CmdType]*RPCRuntimeStats
}

// NewRegionRequestRuntimeStats returns a new RegionRequestRuntimeStats.
func NewRegionRequestRuntimeStats() RegionRequestRuntimeStats {
	return RegionRequestRuntimeStats{
		Stats: make(map[tikvrpc.CmdType]*RPCRuntimeStats),
	}
}

// RPCRuntimeStats indicates the RPC request count and consume time.
type RPCRuntimeStats struct {
	Count int64
	// Send region request consume time.
	Consume int64
}

// String implements fmt.Stringer interface.
func (r *RegionRequestRuntimeStats) String() string {
	var builder strings.Builder
	for k, v := range r.Stats {
		if builder.Len() > 0 {
			builder.WriteByte(',')
		}
		// append string: fmt.Sprintf("%s:{num_rpc:%v, total_time:%s}", k.String(), v.Count, util.FormatDuration(time.Duration(v.Consume))")
		builder.WriteString(k.String())
		builder.WriteString(":{num_rpc:")
		builder.WriteString(strconv.FormatInt(v.Count, 10))
		builder.WriteString(", total_time:")
		builder.WriteString(util.FormatDuration(time.Duration(v.Consume)))
		builder.WriteString("}")
	}
	return builder.String()
}

// Clone returns a copy of itself.
func (r *RegionRequestRuntimeStats) Clone() RegionRequestRuntimeStats {
	newRs := NewRegionRequestRuntimeStats()
	for cmd, v := range r.Stats {
		newRs.Stats[cmd] = &RPCRuntimeStats{
			Count:   v.Count,
			Consume: v.Consume,
		}
	}
	return newRs
}

// Merge merges other RegionRequestRuntimeStats.
func (r *RegionRequestRuntimeStats) Merge(rs RegionRequestRuntimeStats) {
	for cmd, v := range rs.Stats {
		stat, ok := r.Stats[cmd]
		if !ok {
			r.Stats[cmd] = &RPCRuntimeStats{
				Count:   v.Count,
				Consume: v.Consume,
			}
			continue
		}
		stat.Count += v.Count
		stat.Consume += v.Consume
	}
}

// RecordRegionRequestRuntimeStats records request runtime stats.
func RecordRegionRequestRuntimeStats(stats map[tikvrpc.CmdType]*RPCRuntimeStats, cmd tikvrpc.CmdType, d time.Duration) {
	stat, ok := stats[cmd]
	if !ok {
		stats[cmd] = &RPCRuntimeStats{
			Count:   1,
			Consume: int64(d),
		}
		return
	}
	stat.Count++
	stat.Consume += int64(d)
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client client.Client) *RegionRequestSender {
	return &RegionRequestSender{
		regionCache: regionCache,
		apiVersion:  regionCache.codec.GetAPIVersion(),
		client:      client,
	}
}

// GetRegionCache returns the region cache.
func (s *RegionRequestSender) GetRegionCache() *RegionCache {
	return s.regionCache
}

// GetClient returns the RPC client.
func (s *RegionRequestSender) GetClient() client.Client {
	return s.client
}

// SetStoreAddr specifies the dest store address.
func (s *RegionRequestSender) SetStoreAddr(addr string) {
	s.storeAddr = addr
}

// GetStoreAddr returns the dest store address.
func (s *RegionRequestSender) GetStoreAddr() string {
	return s.storeAddr
}

// GetRPCError returns the RPC error.
func (s *RegionRequestSender) GetRPCError() error {
	return s.rpcError
}

// SetRPCError rewrite the rpc error.
func (s *RegionRequestSender) SetRPCError(err error) {
	s.rpcError = err
}

// SendReq sends a request to tikv server. If fails to send the request to all replicas,
// a fake region error may be returned. Caller which receives the error should retry the request.
// It also returns the times of retries in RPC layer. A positive retryTimes indicates a possible undetermined error.
func (s *RegionRequestSender) SendReq(
	bo *retry.Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration,
) (*tikvrpc.Response, int, error) {
	resp, _, retryTimes, err := s.SendReqCtx(bo, req, regionID, timeout, tikvrpc.TiKV)
	return resp, retryTimes, err
}

type replica struct {
	store    *Store
	peer     *metapb.Peer
	epoch    uint32
	attempts int
	// deadlineErrUsingConfTimeout indicates the replica is already tried, but the received deadline exceeded error.
	deadlineErrUsingConfTimeout bool
}

func (r *replica) getEpoch() uint32 {
	return atomic.LoadUint32(&r.epoch)
}

func (r *replica) isEpochStale() bool {
	return r.epoch != atomic.LoadUint32(&r.store.epoch)
}

func (r *replica) isExhausted(maxAttempt int) bool {
	return r.attempts >= maxAttempt
}

type replicaSelector struct {
	regionCache *RegionCache
	region      *Region
	regionStore *regionStore
	replicas    []*replica
	labels      []*metapb.StoreLabel
	state       selectorState
	// replicas[targetIdx] is the replica handling the request this time
	targetIdx AccessIndex
	// replicas[proxyIdx] is the store used to redirect requests this time
	proxyIdx AccessIndex
	// TiKV can reject the request when its estimated wait duration exceeds busyThreshold.
	// Then, the client will receive a ServerIsBusy error and choose another replica to retry.
	busyThreshold time.Duration
}

func selectorStateToString(state selectorState) string {
	replicaSelectorState := "nil"
	if state != nil {
		switch state.(type) {
		case *accessKnownLeader:
			replicaSelectorState = "accessKnownLeader"
		case *accessFollower:
			replicaSelectorState = "accessFollower"
		case *accessByKnownProxy:
			replicaSelectorState = "accessByKnownProxy"
		case *tryFollower:
			replicaSelectorState = "tryFollower"
		case *tryNewProxy:
			replicaSelectorState = "tryNewProxy"
		case *invalidLeader:
			replicaSelectorState = "invalidLeader"
		case *invalidStore:
			replicaSelectorState = "invalidStore"
		case *stateBase:
			replicaSelectorState = "stateBase"
		case nil:
			replicaSelectorState = "nil"
		}
	}
	return replicaSelectorState
}

func (s *replicaSelector) String() string {
	var replicaStatus []string
	cacheRegionIsValid := "unknown"
	selectorStateStr := "nil"
	if s != nil {
		selectorStateStr = selectorStateToString(s.state)
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

	return fmt.Sprintf("replicaSelector{selectorStateStr: %v, cacheRegionIsValid: %v, replicaStatus: %v}", selectorStateStr, cacheRegionIsValid, replicaStatus)
}

// selectorState is the interface of states of the replicaSelector.
// Here is the main state transition diagram:
//
//                                    exceeding maxReplicaAttempt
//           +-------------------+   || RPC failure && unreachable && no forwarding
// +-------->+ accessKnownLeader +----------------+
// |         +------+------------+                |
// |                |                             |
// |                | RPC failure                 v
// |                | && unreachable        +-----+-----+
// |                | && enable forwarding  |tryFollower+------+
// |                |                       +-----------+      |
// | leader becomes v                                          | all followers
// | reachable +----+-------------+                            | are tried
// +-----------+accessByKnownProxy|                            |
// ^           +------+-----------+                            |
// |                  |                           +-------+    |
// |                  | RPC failure               |backoff+<---+
// | leader becomes   v                           +---+---+
// | reachable  +-----+-----+ all proxies are tried   ^
// +------------+tryNewProxy+-------------------------+
//              +-----------+

type selectorState interface {
	stateErrorHandle
	next(*retry.Backoffer, *replicaSelector) (*RPCContext, error)
	onSendSuccess(*replicaSelector)
	onSendFailure(*retry.Backoffer, *replicaSelector, error)
	onNoLeader(*replicaSelector)
}

type stateChanged struct{}

func (c stateChanged) Error() string {
	return "replicaSelector state changed"
}

type stateBase struct{}

func (s stateBase) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	return nil, nil
}

func (s stateBase) onSendSuccess(selector *replicaSelector) {
}

func (s stateBase) onSendFailure(backoffer *retry.Backoffer, selector *replicaSelector, err error) {
}

func (s stateBase) onNoLeader(selector *replicaSelector) {
}

// accessKnownLeader is the state where we are sending requests
// to the leader we suppose to be.
//
// After attempting maxReplicaAttempt times without success
// and without receiving new leader from the responses error,
// we should switch to tryFollower state.
type accessKnownLeader struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *accessKnownLeader) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	leader := selector.replicas[state.leaderIdx]
	liveness := leader.store.getLivenessState()
	if liveness == unreachable && selector.regionCache.enableForwarding {
		selector.state = &tryNewProxy{leaderIdx: state.leaderIdx}
		return nil, stateChanged{}
	}
	// If hibernate region is enabled and the leader is not reachable, the raft group
	// will not be wakened up and re-elect the leader until the follower receives
	// a request. So, before the new leader is elected, we should not send requests
	// to the unreachable old leader to avoid unnecessary timeout.
	if liveness != reachable || leader.isExhausted(maxReplicaAttempt) {
		selector.state = &tryFollower{leaderIdx: state.leaderIdx, lastIdx: state.leaderIdx, fromAccessKnownLeader: true}
		return nil, stateChanged{}
	}
	if selector.busyThreshold > 0 {
		// If the leader is busy in our estimation, change to tryIdleReplica state to try other replicas.
		// If other replicas are all busy, tryIdleReplica will try the leader again without busy threshold.
		leaderEstimated := selector.replicas[state.leaderIdx].store.EstimatedWaitTime()
		if leaderEstimated > selector.busyThreshold {
			selector.state = &tryIdleReplica{leaderIdx: state.leaderIdx}
			return nil, stateChanged{}
		}
	}
	selector.targetIdx = state.leaderIdx
	return selector.buildRPCContext(bo)
}

func (state *accessKnownLeader) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	liveness := selector.checkLiveness(bo, selector.targetReplica())
	// Only enable forwarding when unreachable to avoid using proxy to access a TiKV that cannot serve.
	if liveness == unreachable && len(selector.replicas) > 1 && selector.regionCache.enableForwarding {
		selector.state = &accessByKnownProxy{leaderIdx: state.leaderIdx}
		return
	}
	if liveness != reachable || selector.targetReplica().isExhausted(maxReplicaAttempt) {
		selector.state = &tryFollower{leaderIdx: state.leaderIdx, lastIdx: state.leaderIdx, fromAccessKnownLeader: true}
	}
	if liveness != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

func (state *accessKnownLeader) onNoLeader(selector *replicaSelector) {
	selector.state = &tryFollower{leaderIdx: state.leaderIdx, lastIdx: state.leaderIdx, fromAccessKnownLeader: true}
}

// tryFollower is the state where we cannot access the known leader
// but still try other replicas in case they have become the leader.
//
// In this state, a follower that is not tried will be used. If all
// followers are tried, we think we have exhausted the replicas.
// On sending failure in this state, if leader info is returned,
// the leader will be updated to replicas[0] and give it another chance.
type tryFollower struct {
	stateBase
	leaderIdx AccessIndex
	lastIdx   AccessIndex
	// fromAccessKnownLeader indicates whether the state is changed from `accessKnownLeader`.
	fromAccessKnownLeader bool
	labels                []*metapb.StoreLabel
}

func (state *tryFollower) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	hasDeadlineExceededErr := false
	filterReplicas := func(fn func(*replica) bool) (AccessIndex, *replica) {
		for i := 0; i < len(selector.replicas); i++ {
			idx := AccessIndex((int(state.lastIdx) + i) % len(selector.replicas))
			if idx == state.leaderIdx {
				continue
			}
			selectReplica := selector.replicas[idx]
			hasDeadlineExceededErr = hasDeadlineExceededErr || selectReplica.deadlineErrUsingConfTimeout
			if selectReplica.store.getLivenessState() != unreachable && !selectReplica.deadlineErrUsingConfTimeout &&
				fn(selectReplica) {
				return idx, selectReplica
			}
		}
		return -1, nil
	}

	if len(state.labels) > 0 {
		idx, selectReplica := filterReplicas(func(selectReplica *replica) bool {
			return selectReplica.store.IsLabelsMatch(state.labels)
		})
		if selectReplica != nil && idx >= 0 {
			state.lastIdx = idx
			selector.targetIdx = idx
		}
		// labels only take effect for first try.
		state.labels = nil
	}

	if selector.targetIdx < 0 {
		// Search replica that is not attempted from the last accessed replica
		idx, selectReplica := filterReplicas(func(selectReplica *replica) bool {
			return !selectReplica.isExhausted(1)
		})
		if selectReplica != nil && idx >= 0 {
			state.lastIdx = idx
			selector.targetIdx = idx
		}
	}

	// If all followers are tried and fail, backoff and retry.
	if selector.targetIdx < 0 {
		if hasDeadlineExceededErr {
			// when meet deadline exceeded error, do fast retry without invalidate region cache.
			return nil, nil
		}
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
		selector.invalidateRegion()
		return nil, nil
	}
	rpcCtx, err := selector.buildRPCContext(bo)
	if err != nil || rpcCtx == nil {
		return rpcCtx, err
	}
	if !state.fromAccessKnownLeader {
		replicaRead := true
		rpcCtx.contextPatcher.replicaRead = &replicaRead
	}
	staleRead := false
	rpcCtx.contextPatcher.staleRead = &staleRead
	return rpcCtx, nil
}

func (state *tryFollower) onSendSuccess(selector *replicaSelector) {
	if state.fromAccessKnownLeader {
		peer := selector.targetReplica().peer
		if !selector.region.switchWorkLeaderToPeer(peer) {
			logutil.BgLogger().Warn("the store must exist",
				zap.Uint64("store", peer.StoreId),
				zap.Uint64("peer", peer.Id))
		}
	}
}

func (state *tryFollower) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.targetReplica()) != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

// accessByKnownProxy is the state where we are sending requests through
// regionStore.proxyTiKVIdx as a proxy.
type accessByKnownProxy struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *accessByKnownProxy) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	leader := selector.replicas[state.leaderIdx]
	if leader.store.getLivenessState() == reachable {
		selector.regionStore.unsetProxyStoreIfNeeded(selector.region)
		selector.state = &accessKnownLeader{leaderIdx: state.leaderIdx}
		return nil, stateChanged{}
	}

	if selector.regionStore.proxyTiKVIdx >= 0 {
		selector.targetIdx = state.leaderIdx
		selector.proxyIdx = selector.regionStore.proxyTiKVIdx
		return selector.buildRPCContext(bo)
	}

	selector.state = &tryNewProxy{leaderIdx: state.leaderIdx}
	return nil, stateChanged{}
}

func (state *accessByKnownProxy) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	selector.state = &tryNewProxy{leaderIdx: state.leaderIdx}
	if selector.checkLiveness(bo, selector.proxyReplica()) != reachable {
		selector.invalidateReplicaStore(selector.proxyReplica(), cause)
	}
}

func (state *accessByKnownProxy) onNoLeader(selector *replicaSelector) {
	selector.state = &invalidLeader{}
}

// tryNewProxy is the state where we try to find a node from followers as proxy.
type tryNewProxy struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *tryNewProxy) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	leader := selector.replicas[state.leaderIdx]
	if leader.store.getLivenessState() == reachable {
		selector.regionStore.unsetProxyStoreIfNeeded(selector.region)
		selector.state = &accessKnownLeader{leaderIdx: state.leaderIdx}
		return nil, stateChanged{}
	}

	candidateNum := 0
	for idx, replica := range selector.replicas {
		if state.isCandidate(AccessIndex(idx), replica) {
			candidateNum++
		}
	}

	// If all followers are tried as a proxy and fail, mark the leader store invalid, then backoff and retry.
	if candidateNum == 0 {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
		selector.invalidateReplicaStore(leader, errors.Errorf("all followers are tried as proxy but fail"))
		selector.region.scheduleReload()
		return nil, nil
	}

	// Skip advanceCnt valid candidates to find a proxy peer randomly
	advanceCnt := rand.Intn(candidateNum)
	for idx, replica := range selector.replicas {
		if !state.isCandidate(AccessIndex(idx), replica) {
			continue
		}
		if advanceCnt == 0 {
			selector.targetIdx = state.leaderIdx
			selector.proxyIdx = AccessIndex(idx)
			break
		}
		advanceCnt--
	}
	return selector.buildRPCContext(bo)
}

func (state *tryNewProxy) isCandidate(idx AccessIndex, replica *replica) bool {
	// Try each peer only once
	return idx != state.leaderIdx && !replica.isExhausted(1)
}

func (state *tryNewProxy) onSendSuccess(selector *replicaSelector) {
	selector.regionStore.setProxyStoreIdx(selector.region, selector.proxyIdx)
}

func (state *tryNewProxy) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.proxyReplica()) != reachable {
		selector.invalidateReplicaStore(selector.proxyReplica(), cause)
	}
}

func (state *tryNewProxy) onNoLeader(selector *replicaSelector) {
	selector.state = &invalidLeader{}
}

// accessFollower is the state where we are sending requests to TiKV followers.
// If there is no suitable follower, requests will be sent to the leader as a fallback.
type accessFollower struct {
	stateBase
	// If tryLeader is true, the request can also be sent to the leader when !leader.isSlow()
	tryLeader   bool
	isStaleRead bool
	option      storeSelectorOp
	leaderIdx   AccessIndex
	lastIdx     AccessIndex
	learnerOnly bool
}

// Follower read will try followers first, if no follower is available, it will fallback to leader.
// Specially, for stale read, it tries local peer(can be either leader or follower), then use snapshot read in the leader,
// if the leader read receive server-is-busy and connection errors, the region cache is still valid,
// and the state will be changed to tryFollower, which will read by replica read.
func (state *accessFollower) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	replicaSize := len(selector.replicas)
	resetStaleRead := false
	if state.lastIdx < 0 {
		if state.tryLeader {
			state.lastIdx = AccessIndex(rand.Intn(replicaSize))
		} else {
			if replicaSize <= 1 {
				state.lastIdx = state.leaderIdx
			} else {
				// Randomly select a non-leader peer
				state.lastIdx = AccessIndex(rand.Intn(replicaSize - 1))
				if state.lastIdx >= state.leaderIdx {
					state.lastIdx++
				}
			}
		}
	} else {
		// Stale Read request will retry the leader only by using the WithLeaderOnly option.
		if state.isStaleRead {
			WithLeaderOnly()(&state.option)
			// retry on the leader should not use stale read flag to avoid possible DataIsNotReady error as it always can serve any read.
			resetStaleRead = true
		}
		state.lastIdx++
	}

	// If selector is under `ReplicaReadPreferLeader` mode, we should choose leader as high priority.
	if state.option.preferLeader {
		state.lastIdx = state.leaderIdx
	}
	var offset int
	if state.lastIdx >= 0 {
		offset = rand.Intn(replicaSize)
	}
	reloadRegion := false
	for i := 0; i < replicaSize && !state.option.leaderOnly; i++ {
		var idx AccessIndex
		if state.option.preferLeader {
			if i == 0 {
				idx = state.lastIdx
			} else {
				// randomly select next replica, but skip state.lastIdx
				if (i+offset)%replicaSize == 0 {
					offset++
				}
			}
		} else {
			idx = AccessIndex((int(state.lastIdx) + i) % replicaSize)
		}
		selectReplica := selector.replicas[idx]
		if state.isCandidate(idx, selectReplica) {
			state.lastIdx = idx
			selector.targetIdx = idx
			break
		}
		if selectReplica.isEpochStale() &&
			selectReplica.store.getResolveState() == resolved &&
			selectReplica.store.getLivenessState() == reachable {
			reloadRegion = true
		}
	}
	if reloadRegion {
		selector.regionCache.scheduleReloadRegion(selector.region)
	}
	// If there is no candidate, fallback to the leader.
	if selector.targetIdx < 0 {
		leader := selector.replicas[state.leaderIdx]
		leaderEpochStale := leader.isEpochStale()
		leaderUnreachable := leader.store.getLivenessState() != reachable
		leaderExhausted := state.IsLeaderExhausted(leader)
		leaderInvalid := leaderEpochStale || leaderUnreachable || leaderExhausted
		if len(state.option.labels) > 0 && !state.option.leaderOnly {
			logutil.Logger(bo.GetCtx()).Warn("unable to find a store with given labels",
				zap.Uint64("region", selector.region.GetID()),
				zap.Any("labels", state.option.labels))
		}
		if leaderInvalid || leader.deadlineErrUsingConfTimeout {
			logutil.Logger(bo.GetCtx()).Warn("unable to find valid leader",
				zap.Uint64("region", selector.region.GetID()),
				zap.Bool("epoch-stale", leaderEpochStale),
				zap.Bool("unreachable", leaderUnreachable),
				zap.Bool("exhausted", leaderExhausted),
				zap.Bool("kv-timeout", leader.deadlineErrUsingConfTimeout),
				zap.Bool("stale-read", state.isStaleRead))
			// In stale-read, the request will fallback to leader after the local follower failure.
			// If the leader is also unavailable, we can fallback to the follower and use replica-read flag again,
			// The remote follower not tried yet, and the local follower can retry without stale-read flag.
			// If leader tried and received deadline exceeded error, try follower.
			if state.isStaleRead || leader.deadlineErrUsingConfTimeout {
				selector.state = &tryFollower{
					leaderIdx: state.leaderIdx,
					lastIdx:   state.leaderIdx,
					labels:    state.option.labels,
				}
				if leaderEpochStale {
					selector.regionCache.scheduleReloadRegion(selector.region)
				}
				return nil, stateChanged{}
			}
			metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("exhausted").Inc()
			selector.invalidateRegion()
			return nil, nil
		}
		state.lastIdx = state.leaderIdx
		selector.targetIdx = state.leaderIdx
	}
	// Monitor the flows destination if selector is under `ReplicaReadPreferLeader` mode.
	if state.option.preferLeader {
		if selector.targetIdx != state.leaderIdx {
			selector.replicas[selector.targetIdx].store.recordReplicaFlowsStats(toFollower)
		} else {
			selector.replicas[selector.targetIdx].store.recordReplicaFlowsStats(toLeader)
		}
	}
	rpcCtx, err := selector.buildRPCContext(bo)
	if err != nil || rpcCtx == nil {
		return nil, err
	}
	if resetStaleRead {
		staleRead := false
		rpcCtx.contextPatcher.staleRead = &staleRead
	}
	return rpcCtx, nil
}

func (state *accessFollower) IsLeaderExhausted(leader *replica) bool {
	// Allow another extra retry for the following case:
	// 1. The stale read is enabled and leader peer is selected as the target peer at first.
	// 2. Data is not ready is returned from the leader peer.
	// 3. Stale read flag is removed and processing falls back to snapshot read on the leader peer.
	// 4. The leader peer should be retried again using snapshot read.
	if state.isStaleRead && state.option.leaderOnly {
		return leader.isExhausted(2)
	} else {
		return leader.isExhausted(1)
	}
}

func (state *accessFollower) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.targetReplica()) != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

func (state *accessFollower) isCandidate(idx AccessIndex, replica *replica) bool {
	// the epoch is staled or retry exhausted, or the store is unreachable.
	if replica.isEpochStale() || replica.isExhausted(1) || replica.store.getLivenessState() == unreachable || replica.deadlineErrUsingConfTimeout {
		return false
	}
	if state.option.leaderOnly {
		// The request can only be sent to the leader.
		return idx == state.leaderIdx
	}
	if !state.tryLeader && idx == state.leaderIdx {
		// The request cannot be sent to leader.
		return false
	}
	if state.learnerOnly {
		// The request can only be sent to the learner.
		return replica.peer.Role == metapb.PeerRole_Learner
	}
	// And If the leader store is abnormal to be accessed under `ReplicaReadPreferLeader` mode, we should choose other valid followers
	// as candidates to serve the Read request.
	if state.option.preferLeader && replica.store.isSlow() {
		return false
	}
	// Choose a replica with matched labels.
	return replica.store.IsStoreMatch(state.option.stores) && replica.store.IsLabelsMatch(state.option.labels)
}

// tryIdleReplica is the state where we find the leader is busy and retry the request using replica read.
type tryIdleReplica struct {
	stateBase
	leaderIdx AccessIndex
}

func (state *tryIdleReplica) next(bo *retry.Backoffer, selector *replicaSelector) (*RPCContext, error) {
	// Select a follower replica that has the lowest estimated wait duration
	minWait := time.Duration(math.MaxInt64)
	targetIdx := state.leaderIdx
	startIdx := rand.Intn(len(selector.replicas))
	for i := 0; i < len(selector.replicas); i++ {
		idx := (i + startIdx) % len(selector.replicas)
		r := selector.replicas[idx]
		// Don't choose leader again by default.
		if idx == int(state.leaderIdx) {
			continue
		}
		// Skip replicas that have been tried.
		if r.isExhausted(1) {
			continue
		}
		estimated := r.store.EstimatedWaitTime()
		if estimated > selector.busyThreshold {
			continue
		}
		if estimated < minWait {
			minWait = estimated
			targetIdx = AccessIndex(idx)
		}
		if minWait == 0 {
			break
		}
	}
	selector.targetIdx = targetIdx
	rpcCtx, err := selector.buildRPCContext(bo)
	if err != nil || rpcCtx == nil {
		return nil, err
	}
	replicaRead := targetIdx != state.leaderIdx
	rpcCtx.contextPatcher.replicaRead = &replicaRead
	if targetIdx == state.leaderIdx {
		// No threshold if all peers are too busy.
		selector.busyThreshold = 0
		rpcCtx.contextPatcher.busyThreshold = &selector.busyThreshold
	}
	return rpcCtx, nil
}

func (state *tryIdleReplica) onSendFailure(bo *retry.Backoffer, selector *replicaSelector, cause error) {
	if selector.checkLiveness(bo, selector.targetReplica()) != reachable {
		selector.invalidateReplicaStore(selector.targetReplica(), cause)
	}
}

type invalidStore struct {
	stateBase
}

func (state *invalidStore) next(_ *retry.Backoffer, _ *replicaSelector) (*RPCContext, error) {
	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalidStore").Inc()
	return nil, nil
}

// TODO(sticnarf): If using request forwarding and the leader is unknown, try other followers
// instead of just switching to this state to backoff and retry.
type invalidLeader struct {
	stateBase
}

func (state *invalidLeader) next(_ *retry.Backoffer, _ *replicaSelector) (*RPCContext, error) {
	metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalidLeader").Inc()
	return nil, nil
}

// newReplicaSelector creates a replicaSelector which selects replicas according to reqType and opts.
// opts is currently only effective for follower read.
func newReplicaSelector(
	regionCache *RegionCache, regionID RegionVerID, req *tikvrpc.Request, opts ...StoreSelectorOption,
) (*replicaSelector, error) {
	cachedRegion := regionCache.GetCachedRegionWithRLock(regionID)
	if cachedRegion == nil {
		return nil, errors.New("cached region not found")
	} else if cachedRegion.checkNeedReload() {
		return nil, errors.New("cached region need reload")
	} else if !cachedRegion.checkRegionCacheTTL(time.Now().Unix()) {
		return nil, errors.New("cached region ttl expired")
	}

	regionStore := cachedRegion.getStore()
	replicas := make([]*replica, 0, regionStore.accessStoreNum(tiKVOnly))
	for _, storeIdx := range regionStore.accessIndex[tiKVOnly] {
		replicas = append(
			replicas, &replica{
				store:    regionStore.stores[storeIdx],
				peer:     cachedRegion.meta.Peers[storeIdx],
				epoch:    regionStore.storeEpochs[storeIdx],
				attempts: 0,
			},
		)
	}

	option := storeSelectorOp{}
	for _, op := range opts {
		op(&option)
	}
	var state selectorState
	if !req.ReplicaReadType.IsFollowerRead() {
		if regionCache.enableForwarding && regionStore.proxyTiKVIdx >= 0 {
			state = &accessByKnownProxy{leaderIdx: regionStore.workTiKVIdx}
		} else {
			state = &accessKnownLeader{leaderIdx: regionStore.workTiKVIdx}
		}
	} else {
		if req.ReplicaReadType == kv.ReplicaReadPreferLeader {
			WithPerferLeader()(&option)
		}
		tryLeader := req.ReplicaReadType == kv.ReplicaReadMixed || req.ReplicaReadType == kv.ReplicaReadPreferLeader
		state = &accessFollower{
			tryLeader:   tryLeader,
			isStaleRead: req.StaleRead,
			option:      option,
			leaderIdx:   regionStore.workTiKVIdx,
			lastIdx:     -1,
			learnerOnly: req.ReplicaReadType == kv.ReplicaReadLearner,
		}
	}

	return &replicaSelector{
		regionCache,
		cachedRegion,
		regionStore,
		replicas,
		option.labels,
		state,
		-1,
		-1,
		time.Duration(req.BusyThresholdMs) * time.Millisecond,
	}, nil
}

const maxReplicaAttempt = 10

// next creates the RPCContext of the current candidate replica.
// It returns a SendError if runs out of all replicas or the cached region is invalidated.
func (s *replicaSelector) next(bo *retry.Backoffer) (rpcCtx *RPCContext, err error) {
	if !s.region.isValid() {
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("invalid").Inc()
		return nil, nil
	}

	s.targetIdx = -1
	s.proxyIdx = -1
	s.refreshRegionStore()
	for {
		rpcCtx, err = s.state.next(bo, s)
		if _, isStateChanged := err.(stateChanged); !isStateChanged {
			return
		}
	}
}

func (s *replicaSelector) targetReplica() *replica {
	if s.targetIdx >= 0 && int(s.targetIdx) < len(s.replicas) {
		return s.replicas[s.targetIdx]
	}
	return nil
}

func (s *replicaSelector) proxyReplica() *replica {
	if s.proxyIdx >= 0 && int(s.proxyIdx) < len(s.replicas) {
		return s.replicas[s.proxyIdx]
	}
	return nil
}

func (s *replicaSelector) refreshRegionStore() {
	oldRegionStore := s.regionStore
	newRegionStore := s.region.getStore()
	if oldRegionStore == newRegionStore {
		return
	}
	s.regionStore = newRegionStore

	// In the current implementation, if stores change, the address of it must change.
	// So we just compare the address here.
	// When stores change, we mark this replicaSelector as invalid to let the caller
	// recreate a new replicaSelector.
	if &oldRegionStore.stores != &newRegionStore.stores {
		s.state = &invalidStore{}
		return
	}

	// If leader has changed, it means a recent request succeeds an RPC
	// on the new leader.
	if oldRegionStore.workTiKVIdx != newRegionStore.workTiKVIdx {
		switch state := s.state.(type) {
		case *accessFollower:
			state.leaderIdx = newRegionStore.workTiKVIdx
		default:
			// Try the new leader and give it an addition chance if the
			// request is sent to the leader.
			newLeaderIdx := newRegionStore.workTiKVIdx
			s.state = &accessKnownLeader{leaderIdx: newLeaderIdx}
			if s.replicas[newLeaderIdx].attempts == maxReplicaAttempt {
				s.replicas[newLeaderIdx].attempts--
			}
		}
	}
}

func (s *replicaSelector) buildRPCContext(bo *retry.Backoffer) (*RPCContext, error) {
	targetReplica, proxyReplica := s.targetReplica(), s.proxyReplica()

	// Backoff and retry if no replica is selected or the selected replica is stale
	if targetReplica == nil || targetReplica.isEpochStale() ||
		(proxyReplica != nil && proxyReplica.isEpochStale()) {
		// TODO(youjiali1995): Is it necessary to invalidate the region?
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("stale_store").Inc()
		s.invalidateRegion()
		return nil, nil
	}

	rpcCtx := &RPCContext{
		Region:     s.region.VerID(),
		Meta:       s.region.meta,
		Peer:       targetReplica.peer,
		Store:      targetReplica.store,
		AccessMode: tiKVOnly,
		TiKVNum:    len(s.replicas),
	}

	// Set leader addr
	addr, err := s.regionCache.getStoreAddr(bo, s.region, targetReplica.store)
	if err != nil {
		return nil, err
	}
	if len(addr) == 0 {
		return nil, nil
	}
	rpcCtx.Addr = addr
	targetReplica.attempts++

	// Set proxy addr
	if proxyReplica != nil {
		addr, err = s.regionCache.getStoreAddr(bo, s.region, proxyReplica.store)
		if err != nil {
			return nil, err
		}
		if len(addr) == 0 {
			return nil, nil
		}
		rpcCtx.ProxyStore = proxyReplica.store
		rpcCtx.ProxyAddr = addr
		proxyReplica.attempts++
	}

	return rpcCtx, nil
}

func (s *replicaSelector) onSendFailure(bo *retry.Backoffer, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	s.state.onSendFailure(bo, s, err)
}

func (s *replicaSelector) onReadReqConfigurableTimeout(req *tikvrpc.Request) bool {
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
		if accessLeader, ok := s.state.(*accessKnownLeader); ok {
			// If leader return deadline exceeded error, we should try to access follower next time.
			s.state = &tryFollower{leaderIdx: accessLeader.leaderIdx, lastIdx: accessLeader.leaderIdx}
		}
		return true
	default:
		// Only work for read requests, return false for non-read requests.
		return false
	}
}

func (s *replicaSelector) checkLiveness(bo *retry.Backoffer, accessReplica *replica) livenessState {
	store := accessReplica.store
	liveness := store.requestLiveness(bo, s.regionCache)
	if liveness != reachable {
		store.startHealthCheckLoopIfNeeded(s.regionCache, liveness)
	}
	return liveness
}

func (s *replicaSelector) invalidateReplicaStore(replica *replica, cause error) {
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
		store.markNeedCheck(s.regionCache.notifyCheckCh)
		store.markAlreadySlow()
	}
}

func (s *replicaSelector) onSendSuccess() {
	s.state.onSendSuccess(s)
}

func (s *replicaSelector) onNotLeader(
	rpcCtx *RPCContext, notLeader *errorpb.NotLeader,
) error {
	leader := notLeader.GetLeader()
	if leader == nil {
		// The region may be during transferring leader.
		s.state.onNoLeader(s)
		return errors.Errorf("no leader, ctx: %v", rpcCtx)
	} else {
		s.updateLeader(notLeader.GetLeader())
	}
	return nil
}

// updateLeader updates the leader of the cached region.
// If the leader peer isn't found in the region, the region will be invalidated.
func (s *replicaSelector) updateLeader(leader *metapb.Peer) {
	if leader == nil {
		return
	}
	for i, replica := range s.replicas {
		if isSamePeer(replica.peer, leader) {
			// If hibernate region is enabled and the leader is not reachable, the raft group
			// will not be wakened up and re-elect the leader until the follower receives
			// a request. So, before the new leader is elected, we should not send requests
			// to the unreachable old leader to avoid unnecessary timeout.
			if replica.store.getLivenessState() != reachable {
				return
			}
			if replica.isExhausted(maxReplicaAttempt) {
				// Give the replica one more chance and because each follower is tried only once,
				// it won't result in infinite retry.
				replica.attempts = maxReplicaAttempt - 1
			}
			s.state = &accessKnownLeader{leaderIdx: AccessIndex(i)}
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

func (s *replicaSelector) onServerIsBusy(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy,
) (shouldRetry bool, err error) {
	if serverIsBusy.EstimatedWaitMs != 0 && ctx != nil && ctx.Store != nil {
		estimatedWait := time.Duration(serverIsBusy.EstimatedWaitMs) * time.Millisecond
		// Update the estimated wait time of the store.
		loadStats := &storeLoadStats{
			estimatedWait:     estimatedWait,
			waitTimeUpdatedAt: time.Now(),
		}
		ctx.Store.loadStats.Store(loadStats)

		if s.busyThreshold != 0 {
			// do not retry with batched coprocessor requests.
			// it'll be region misses if we send the tasks to replica.
			if req.Type == tikvrpc.CmdCop && len(req.Cop().Tasks) > 0 {
				return false, nil
			}
			switch state := s.state.(type) {
			case *accessKnownLeader:
				// Clear attempt history of the leader, so the leader can be accessed again.
				s.replicas[state.leaderIdx].attempts = 0
				s.state = &tryIdleReplica{leaderIdx: state.leaderIdx}
				return true, nil
			case *tryIdleReplica:
				if s.targetIdx != state.leaderIdx {
					return true, nil
				}
				// backoff if still receiving ServerIsBusy after accessing leader again
			}
		}
	} else if ctx != nil && ctx.Store != nil {
		// Mark the server is busy (the next incoming READs could be redirect
		// to expected followers. )
		ctx.Store.markAlreadySlow()
		if s.canFallback2Follower() {
			return true, nil
		}
	}
	err = bo.Backoff(retry.BoTiKVServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
	if err != nil {
		return false, err
	}
	return true, nil
}

// For some reasons, the leader is unreachable by now, try followers instead.
// the state is changed in accessFollower.next when leader is unavailable.
func (s *replicaSelector) canFallback2Follower() bool {
	if s == nil || s.state == nil {
		return false
	}
	state, ok := s.state.(*accessFollower)
	if !ok {
		return false
	}
	if !state.isStaleRead {
		return false
	}
	// can fallback to follower only when the leader is exhausted.
	return state.lastIdx == state.leaderIdx && state.IsLeaderExhausted(s.replicas[state.leaderIdx])
}

func (s *replicaSelector) invalidateRegion() {
	if s.region != nil {
		s.region.invalidate(Other)
	}
}

func (s *RegionRequestSender) getRPCContext(
	bo *retry.Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (*RPCContext, error) {
	switch et {
	case tikvrpc.TiKV:
		if s.replicaSelector == nil {
			selector, err := newReplicaSelector(s.regionCache, regionID, req, opts...)
			if selector == nil || err != nil {
				s.rpcError = err
				return nil, nil
			}
			s.replicaSelector = selector
		}
		return s.replicaSelector.next(bo)
	case tikvrpc.TiFlash:
		// Should ignore WN, because in disaggregated tiflash mode, TiDB will build rpcCtx itself.
		return s.regionCache.GetTiFlashRPCContext(bo, regionID, true, LabelFilterNoTiFlashWriteNode)
	case tikvrpc.TiDB:
		return &RPCContext{Addr: s.storeAddr}, nil
	case tikvrpc.TiFlashCompute:
		// In disaggregated tiflash mode, TiDB will build rpcCtx itself, so cannot reach here.
		return nil, errors.Errorf("should not reach here for disaggregated tiflash mode")
	default:
		return nil, errors.Errorf("unsupported storage type: %v", et)
	}
}

func (s *RegionRequestSender) reset() {
	s.replicaSelector = nil
	s.failStoreIDs = nil
	s.failProxyStoreIDs = nil
}

// IsFakeRegionError returns true if err is fake region error.
func IsFakeRegionError(err *errorpb.Error) bool {
	return err != nil && err.GetEpochNotMatch() != nil && len(err.GetEpochNotMatch().CurrentRegions) == 0
}

// SendReqCtx sends a request to tikv server and return response and RPCCtx of this RPC.
func (s *RegionRequestSender) SendReqCtx(
	bo *retry.Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	timeout time.Duration,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (
	resp *tikvrpc.Response,
	rpcCtx *RPCContext,
	retryTimes int,
	err error,
) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.SendReqCtx", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	if val, err := util.EvalFailpoint("tikvStoreSendReqResult"); err == nil {
		if s, ok := val.(string); ok {
			switch s {
			case "timeout":
				return nil, nil, 0, errors.New("timeout")
			case "GCNotLeader":
				if req.Type == tikvrpc.CmdGC {
					return &tikvrpc.Response{
						Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
					}, nil, 0, nil
				}
			case "PessimisticLockNotLeader":
				if req.Type == tikvrpc.CmdPessimisticLock {
					return &tikvrpc.Response{
						Resp: &kvrpcpb.PessimisticLockResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
					}, nil, 0, nil
				}
			case "GCServerIsBusy":
				if req.Type == tikvrpc.CmdGC {
					return &tikvrpc.Response{
						Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
					}, nil, 0, nil
				}
			case "busy":
				return &tikvrpc.Response{
					Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
				}, nil, 0, nil
			case "requestTiDBStoreError":
				if et == tikvrpc.TiDB {
					return nil, nil, 0, errors.WithStack(tikverr.ErrTiKVServerTimeout)
				}
			case "requestTiFlashError":
				if et == tikvrpc.TiFlash {
					return nil, nil, 0, errors.WithStack(tikverr.ErrTiFlashServerTimeout)
				}
			}
		}
	}

	// If the MaxExecutionDurationMs is not set yet, we set it to be the RPC timeout duration
	// so TiKV can give up the requests whose response TiDB cannot receive due to timeout.
	if req.Context.MaxExecutionDurationMs == 0 {
		req.Context.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
	}

	s.reset()
	retryTimes = 0
	defer func() {
		if retryTimes > 0 {
			metrics.TiKVRequestRetryTimesHistogram.Observe(float64(retryTimes))
		}
	}()

	var staleReadCollector *staleReadMetricsCollector
	if req.StaleRead {
		staleReadCollector = &staleReadMetricsCollector{}
		defer func() {
			if retryTimes == 0 {
				metrics.StaleReadHitCounter.Add(1)
			} else {
				metrics.StaleReadMissCounter.Add(1)
			}
		}()
	}

	totalErrors := make(map[string]int)
	for {
		if retryTimes > 0 {
			if retryTimes%100 == 0 {
				logutil.Logger(bo.GetCtx()).Warn(
					"retry",
					zap.Uint64("region", regionID.GetID()),
					zap.Int("times", retryTimes),
				)
			}
		}

		rpcCtx, err = s.getRPCContext(bo, req, regionID, et, opts...)
		if err != nil {
			return nil, nil, retryTimes, err
		}

		if _, err := util.EvalFailpoint("invalidCacheAndRetry"); err == nil {
			// cooperate with tikvclient/setGcResolveMaxBackoff
			if c := bo.GetCtx().Value("injectedBackoff"); c != nil {
				resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
				return resp, nil, retryTimes, err
			}
		}
		if rpcCtx == nil {
			// TODO(youjiali1995): remove it when using the replica selector for all requests.
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.

			// TODO: Change the returned error to something like "region missing in cache",
			// and handle this error like EpochNotMatch, which means to re-split the request and retry.
			s.logSendReqError(bo, "throwing pseudo region error due to no replica available", regionID, retryTimes, req, totalErrors)
			resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
			return resp, nil, retryTimes, err
		}

		var isLocalTraffic bool
		if staleReadCollector != nil && s.replicaSelector != nil {
			if target := s.replicaSelector.targetReplica(); target != nil {
				isLocalTraffic = target.store.IsLabelsMatch(s.replicaSelector.labels)
				staleReadCollector.onReq(req, isLocalTraffic)
			}
		}

		logutil.Eventf(bo.GetCtx(), "send %s request to region %d at %s", req.Type, regionID.id, rpcCtx.Addr)
		s.storeAddr = rpcCtx.Addr

		if _, err := util.EvalFailpoint("beforeSendReqToRegion"); err == nil {
			if hook := bo.GetCtx().Value("sendReqToRegionHook"); hook != nil {
				h := hook.(func(*tikvrpc.Request))
				h(req)
			}
		}

		rpcCtx.contextPatcher.applyTo(&req.Context)
		if req.InputRequestSource != "" && s.replicaSelector != nil {
			s.replicaSelector.patchRequestSource(req, rpcCtx)
		}
		if e := tikvrpc.SetContext(req, rpcCtx.Meta, rpcCtx.Peer); e != nil {
			return nil, nil, retryTimes, err
		}

		var retry bool
		resp, retry, err = s.sendReqToRegion(bo, rpcCtx, req, timeout)
		req.IsRetryRequest = true
		if err != nil {
			msg := fmt.Sprintf("send request failed, err: %v", err.Error())
			s.logSendReqError(bo, msg, regionID, retryTimes, req, totalErrors)
			return nil, nil, retryTimes, err
		}

		if _, err1 := util.EvalFailpoint("afterSendReqToRegion"); err1 == nil {
			if hook := bo.GetCtx().Value("sendReqToRegionFinishHook"); hook != nil {
				h := hook.(func(*tikvrpc.Request, *tikvrpc.Response, error))
				h(req, resp, err)
			}
		}

		// recheck whether the session/query is killed during the Next()
		boVars := bo.GetVars()
		if boVars != nil && boVars.Killed != nil && atomic.LoadUint32(boVars.Killed) == 1 {
			return nil, nil, retryTimes, errors.WithStack(tikverr.ErrQueryInterrupted)
		}
		if val, err := util.EvalFailpoint("mockRetrySendReqToRegion"); err == nil {
			if val.(bool) {
				retry = true
			}
		}
		if retry {
			retryTimes++
			continue
		}

		var regionErr *errorpb.Error
		regionErr, err = resp.GetRegionError()
		if err != nil {
			return nil, nil, retryTimes, err
		}
		if regionErr != nil {
			regionErrLogging := regionErrorToLogging(rpcCtx.Peer.GetId(), regionErr)
			totalErrors[regionErrLogging]++
			retry, err = s.onRegionError(bo, rpcCtx, req, regionErr, totalErrors)
			if err != nil {
				msg := fmt.Sprintf("send request on region error failed, err: %v", err.Error())
				s.logSendReqError(bo, msg, regionID, retryTimes, req, totalErrors)
				return nil, nil, retryTimes, err
			}
			if retry {
				retryTimes++
				continue
			}
			s.logSendReqError(bo, "send request meet region error without retry", regionID, retryTimes, req, totalErrors)
		} else {
			if s.replicaSelector != nil {
				s.replicaSelector.onSendSuccess()
			}
		}
		if staleReadCollector != nil {
			staleReadCollector.onResp(req.Type, resp, isLocalTraffic)
		}
		return resp, rpcCtx, retryTimes, nil
	}
}

func (s *RegionRequestSender) logSendReqError(bo *retry.Backoffer, msg string, regionID RegionVerID, retryTimes int, req *tikvrpc.Request, totalErrors map[string]int) {
	var totalErrorStr bytes.Buffer
	for err, cnt := range totalErrors {
		if totalErrorStr.Len() > 0 {
			totalErrorStr.WriteString(", ")
		}
		totalErrorStr.WriteString(err)
		totalErrorStr.WriteString(":")
		totalErrorStr.WriteString(strconv.Itoa(cnt))
	}
	logutil.Logger(bo.GetCtx()).Info(msg,
		zap.Uint64("req-ts", req.GetStartTS()),
		zap.String("req-type", req.Type.String()),
		zap.String("region", regionID.String()),
		zap.String("replica-read-type", req.ReplicaReadType.String()),
		zap.Bool("stale-read", req.StaleRead),
		zap.Stringer("request-sender", s),
		zap.Int("retry-times", retryTimes),
		zap.Int("total-backoff-ms", bo.GetTotalSleep()),
		zap.Int("total-backoff-times", bo.GetTotalBackoffTimes()),
		zap.Uint64("max-exec-timeout-ms", req.Context.MaxExecutionDurationMs),
		zap.String("total-region-errors", totalErrorStr.String()))
}

// RPCCancellerCtxKey is context key attach rpc send cancelFunc collector to ctx.
type RPCCancellerCtxKey struct{}

// RPCCanceller is rpc send cancelFunc collector.
type RPCCanceller struct {
	sync.Mutex
	allocID   int
	cancels   map[int]func()
	cancelled bool
}

// NewRPCanceller creates RPCCanceller with init state.
func NewRPCanceller() *RPCCanceller {
	return &RPCCanceller{cancels: make(map[int]func())}
}

// WithCancel generates new context with cancel func.
func (h *RPCCanceller) WithCancel(ctx context.Context) (context.Context, func()) {
	nctx, cancel := context.WithCancel(ctx)
	h.Lock()
	if h.cancelled {
		h.Unlock()
		cancel()
		return nctx, func() {}
	}
	id := h.allocID
	h.allocID++
	h.cancels[id] = cancel
	h.Unlock()
	return nctx, func() {
		cancel()
		h.Lock()
		delete(h.cancels, id)
		h.Unlock()
	}
}

// CancelAll cancels all inflight rpc context.
func (h *RPCCanceller) CancelAll() {
	h.Lock()
	for _, c := range h.cancels {
		c()
	}
	h.cancelled = true
	h.Unlock()
}

func fetchRespInfo(resp *tikvrpc.Response) string {
	var extraInfo string
	if resp == nil || resp.Resp == nil {
		extraInfo = "nil response"
	} else {
		regionErr, e := resp.GetRegionError()
		if e != nil {
			extraInfo = e.Error()
		} else if regionErr != nil {
			extraInfo = regionErr.String()
		} else if prewriteResp, ok := resp.Resp.(*kvrpcpb.PrewriteResponse); ok {
			extraInfo = prewriteResp.String()
		}
	}
	return extraInfo
}

func (s *RegionRequestSender) sendReqToRegion(
	bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request, timeout time.Duration,
) (resp *tikvrpc.Response, retry bool, err error) {
	// judge the store limit switch.
	if limit := kv.StoreLimit.Load(); limit > 0 {
		if err := s.getStoreToken(rpcCtx.Store, limit); err != nil {
			return nil, false, err
		}
		defer s.releaseStoreToken(rpcCtx.Store)
	}

	ctx := bo.GetCtx()
	if rawHook := ctx.Value(RPCCancellerCtxKey{}); rawHook != nil {
		var cancel context.CancelFunc
		ctx, cancel = rawHook.(*RPCCanceller).WithCancel(ctx)
		defer cancel()
	}

	// sendToAddr is the first target address that will receive the request. If proxy is used, sendToAddr will point to
	// the proxy that will forward the request to the final target.
	sendToAddr := rpcCtx.Addr
	if rpcCtx.ProxyStore == nil {
		req.ForwardedHost = ""
	} else {
		req.ForwardedHost = rpcCtx.Addr
		sendToAddr = rpcCtx.ProxyAddr
	}

	// Count the replica number as the RU cost factor.
	req.ReplicaNumber = 1
	if rpcCtx.Meta != nil && len(rpcCtx.Meta.GetPeers()) > 0 {
		req.ReplicaNumber = 0
		for _, peer := range rpcCtx.Meta.GetPeers() {
			role := peer.GetRole()
			if role == metapb.PeerRole_Voter || role == metapb.PeerRole_Learner {
				req.ReplicaNumber++
			}
		}
	}

	var sessionID uint64
	if v := bo.GetCtx().Value(util.SessionID); v != nil {
		sessionID = v.(uint64)
	}

	injectFailOnSend := false
	if val, e := util.EvalFailpoint("rpcFailOnSend"); e == nil {
		inject := true
		// Optional filters
		if s, ok := val.(string); ok {
			if s == "greengc" && !req.IsGreenGCRequest() {
				inject = false
			} else if s == "write" && !req.IsTxnWriteRequest() {
				inject = false
			}
		} else if sessionID == 0 {
			inject = false
		}

		if inject {
			logutil.Logger(ctx).Info(
				"[failpoint] injected RPC error on send", zap.Stringer("type", req.Type),
				zap.Stringer("req", req.Req.(fmt.Stringer)), zap.Stringer("ctx", &req.Context),
			)
			injectFailOnSend = true
			err = errors.New("injected RPC error on send")
		}
	}

	if !injectFailOnSend {
		start := time.Now()
		resp, err = s.client.SendRequest(ctx, sendToAddr, req, timeout)
		// Record timecost of external requests on related Store when `ReplicaReadMode == "PreferLeader"`.
		if rpcCtx.Store != nil && req.ReplicaReadType == kv.ReplicaReadPreferLeader && !util.IsInternalRequest(req.RequestSource) {
			rpcCtx.Store.recordSlowScoreStat(time.Since(start))
		}
		if s.Stats != nil {
			RecordRegionRequestRuntimeStats(s.Stats, req.Type, time.Since(start))
			if val, fpErr := util.EvalFailpoint("tikvStoreRespResult"); fpErr == nil {
				if val.(bool) {
					if req.Type == tikvrpc.CmdCop && bo.GetTotalSleep() == 0 {
						return &tikvrpc.Response{
							Resp: &coprocessor.Response{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}},
						}, false, nil
					}
				}
			}
		}

		if val, e := util.EvalFailpoint("rpcFailOnRecv"); e == nil {
			inject := true
			// Optional filters
			if s, ok := val.(string); ok {
				if s == "greengc" && !req.IsGreenGCRequest() {
					inject = false
				} else if s == "write" && !req.IsTxnWriteRequest() {
					inject = false
				}
			} else if sessionID == 0 {
				inject = false
			}

			if inject {
				logutil.Logger(ctx).Info(
					"[failpoint] injected RPC error on recv", zap.Stringer("type", req.Type),
					zap.Stringer("req", req.Req.(fmt.Stringer)), zap.Stringer("ctx", &req.Context),
					zap.Error(err), zap.String("extra response info", fetchRespInfo(resp)),
				)
				err = errors.New("injected RPC error on recv")
				resp = nil
			}
		}

		if val, e := util.EvalFailpoint("rpcContextCancelErr"); e == nil {
			if val.(bool) {
				ctx1, cancel := context.WithCancel(context.Background())
				cancel()
				<-ctx1.Done()
				ctx = ctx1
				err = ctx.Err()
				resp = nil
			}
		}
	}

	if rpcCtx.ProxyStore != nil {
		fromStore := strconv.FormatUint(rpcCtx.ProxyStore.storeID, 10)
		toStore := strconv.FormatUint(rpcCtx.Store.storeID, 10)
		result := "ok"
		if err != nil {
			result = "fail"
		}
		metrics.TiKVForwardRequestCounter.WithLabelValues(fromStore, toStore, req.Type.String(), result).Inc()
	}

	if err != nil {
		s.rpcError = err

		// Because in rpc logic, context.Cancel() will be transferred to rpcContext.Cancel error. For rpcContext cancel,
		// we need to retry the request. But for context cancel active, for example, limitExec gets the required rows,
		// we shouldn't retry the request, it will go to backoff and hang in retry logic.
		if ctx.Err() != nil && errors.Cause(ctx.Err()) == context.Canceled {
			return nil, false, errors.WithStack(ctx.Err())
		}

		if val, e := util.EvalFailpoint("noRetryOnRpcError"); e == nil {
			if val.(bool) {
				return nil, false, err
			}
		}
		if e := s.onSendFail(bo, rpcCtx, req, err); e != nil {
			return nil, false, err
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) getStoreToken(st *Store, limit int64) error {
	// Checking limit is not thread safe, preferring this for avoiding load in loop.
	count := st.tokenCount.Load()
	if count < limit {
		// Adding tokenCount is no thread safe, preferring this for avoiding check in loop.
		st.tokenCount.Add(1)
		return nil
	}
	metrics.TiKVStoreLimitErrorCounter.WithLabelValues(st.addr, strconv.FormatUint(st.storeID, 10)).Inc()
	return errors.WithStack(&tikverr.ErrTokenLimit{StoreID: st.storeID})
}

func (s *RegionRequestSender) releaseStoreToken(st *Store) {
	count := st.tokenCount.Load()
	// Decreasing tokenCount is no thread safe, preferring this for avoiding check in loop.
	if count > 0 {
		st.tokenCount.Sub(1)
		return
	}
	logutil.BgLogger().Warn("release store token failed, count equals to 0")
}

func (s *RegionRequestSender) onSendFail(bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, err error) error {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.onSendFail", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		return errors.WithStack(err)
	} else if LoadShuttingDown() > 0 {
		return errors.WithStack(tikverr.ErrTiDBShuttingDown)
	} else if isCauseByDeadlineExceeded(err) {
		if s.replicaSelector != nil && s.replicaSelector.onReadReqConfigurableTimeout(req) {
			return nil
		}
	}
	if status.Code(errors.Cause(err)) == codes.Canceled {
		select {
		case <-bo.GetCtx().Done():
			return errors.WithStack(err)
		default:
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when tikv is killed and exiting.
			// Backoff and retry in this case.
			logutil.Logger(bo.GetCtx()).Warn("receive a grpc cancel signal from remote", zap.Error(err))
		}
	}

	if ctx.Store != nil && ctx.Store.storeType == tikvrpc.TiFlashCompute {
		s.regionCache.InvalidateTiFlashComputeStoresIfGRPCError(err)
	} else if ctx.Meta != nil {
		if s.replicaSelector != nil {
			s.replicaSelector.onSendFailure(bo, err)
		} else {
			s.regionCache.OnSendFail(bo, ctx, s.NeedReloadRegion(ctx), err)
		}
	}

	// don't need to retry for ResourceGroup error
	if errors.Is(err, pderr.ErrClientResourceGroupThrottled) {
		return err
	}
	if errors.Is(err, pderr.ErrClientResourceGroupConfigUnavailable) {
		return err
	}
	var errGetResourceGroup *pderr.ErrClientGetResourceGroup
	if errors.As(err, &errGetResourceGroup) {
		return err
	}

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	if ctx.Store != nil && ctx.Store.storeType.IsTiFlashRelatedType() {
		err = bo.Backoff(
			retry.BoTiFlashRPC,
			errors.Errorf("send tiflash request error: %v, ctx: %v, try next peer later", err, ctx),
		)
	} else {
		err = bo.Backoff(
			retry.BoTiKVRPC,
			errors.Errorf("send tikv request error: %v, ctx: %v, try next peer later", err, ctx),
		)
	}
	return err
}

func isCauseByDeadlineExceeded(err error) bool {
	causeErr := errors.Cause(err)
	return causeErr == context.DeadlineExceeded || // batch-client will return this error.
		status.Code(causeErr) == codes.DeadlineExceeded // when batch-client is disabled, grpc will return this error.
}

// NeedReloadRegion checks is all peers has sent failed, if so need reload.
func (s *RegionRequestSender) NeedReloadRegion(ctx *RPCContext) (need bool) {
	if s.failStoreIDs == nil {
		s.failStoreIDs = make(map[uint64]struct{})
	}
	if s.failProxyStoreIDs == nil {
		s.failProxyStoreIDs = make(map[uint64]struct{})
	}
	s.failStoreIDs[ctx.Store.storeID] = struct{}{}
	if ctx.ProxyStore != nil {
		s.failProxyStoreIDs[ctx.ProxyStore.storeID] = struct{}{}
	}

	if ctx.AccessMode == tiKVOnly && len(s.failStoreIDs)+len(s.failProxyStoreIDs) >= ctx.TiKVNum {
		need = true
	} else if ctx.AccessMode == tiFlashOnly && len(s.failStoreIDs) >= len(ctx.Meta.Peers)-ctx.TiKVNum {
		need = true
	} else if len(s.failStoreIDs)+len(s.failProxyStoreIDs) >= len(ctx.Meta.Peers) {
		need = true
	}

	if need {
		s.failStoreIDs = nil
		s.failProxyStoreIDs = nil
	}
	return
}

// regionErrorToLogging constructs the logging content with extra information like returned leader peer id.
func regionErrorToLogging(peerID uint64, e *errorpb.Error) string {
	str := regionErrorToLabel(e)
	if e.GetNotLeader() != nil {
		notLeader := e.GetNotLeader()
		if notLeader.GetLeader() != nil {
			str = fmt.Sprintf("%v-%v", str, notLeader.GetLeader().GetId())
		} else {
			str = fmt.Sprintf("%v-nil", str)
		}
	}
	return fmt.Sprintf("%v-%v", peerID, str)
}

func regionErrorToLabel(e *errorpb.Error) string {
	if e.GetNotLeader() != nil {
		return "not_leader"
	} else if e.GetRegionNotFound() != nil {
		return "region_not_found"
	} else if e.GetKeyNotInRegion() != nil {
		return "key_not_in_region"
	} else if e.GetEpochNotMatch() != nil {
		return "epoch_not_match"
	} else if e.GetServerIsBusy() != nil {
		if strings.Contains(e.GetServerIsBusy().GetReason(), "deadline is exceeded") {
			return "deadline_exceeded"
		}
		return "server_is_busy"
	} else if e.GetStaleCommand() != nil {
		return "stale_command"
	} else if e.GetStoreNotMatch() != nil {
		return "store_not_match"
	} else if e.GetRaftEntryTooLarge() != nil {
		return "raft_entry_too_large"
	} else if e.GetMaxTimestampNotSynced() != nil {
		return "max_timestamp_not_synced"
	} else if e.GetReadIndexNotReady() != nil {
		return "read_index_not_ready"
	} else if e.GetProposalInMergingMode() != nil {
		return "proposal_in_merging_mode"
	} else if e.GetDataIsNotReady() != nil {
		return "data_is_not_ready"
	} else if e.GetRegionNotInitialized() != nil {
		return "region_not_initialized"
	} else if e.GetDiskFull() != nil {
		return "disk_full"
	} else if e.GetRecoveryInProgress() != nil {
		return "recovery_in_progress"
	} else if e.GetFlashbackInProgress() != nil {
		return "flashback_in_progress"
	} else if e.GetFlashbackNotPrepared() != nil {
		return "flashback_not_prepared"
	} else if e.GetIsWitness() != nil {
		return "peer_is_witness"
	} else if isDeadlineExceeded(e) {
		return "deadline_exceeded"
	} else if e.GetMismatchPeerId() != nil {
		return "mismatch_peer_id"
	} else if e.GetBucketVersionNotMatch() != nil {
		return "bucket_version_not_match"
	}
	return "unknown"
}

func isDeadlineExceeded(e *errorpb.Error) bool {
	return strings.Contains(e.GetMessage(), "Deadline is exceeded")
}

func (s *RegionRequestSender) onRegionError(
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, regionErr *errorpb.Error, totalErrors map[string]int,
) (shouldRetry bool, err error) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikv.onRegionError", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	// NOTE: Please add the region error handler in the same order of errorpb.Error.
	isInternal := false
	if req != nil {
		isInternal = util.IsInternalRequest(req.GetRequestSource())
	}

	handler := getRegionErrorHandler(regionErr)
	regionErrLabel := handler.label()
	if totalErrors != nil {
		totalErrors[regionErrLabel]++
	}
	metrics.TiKVRegionErrorCounter.WithLabelValues(regionErrLabel, strconv.FormatBool(isInternal)).Inc()

	retry := handler.handle(s, bo, ctx, req, regionErr)
	switch retry.retry {
	case ErrorRetryUninitialized:
		return false, errors.New("unreachable, retry uninitialized")
	case ErrorRetryImmediately:
		return true, nil
	case ErrorRetryWithBackoff:
		if err = bo.Backoff(retry.policy, retry.err); err != nil {
			return false, err
		}
		return true, nil
	case ErrorRetryInvalidateRegion:
		if retry.invalidateRegion == Ok {
			return false, errors.New("unreachable, invalidate region should not be Ok")
		}
		s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, retry.invalidateRegion)
		return false, nil
	case ErrorRetryInvalidateRegionAndBackoff:
		if retry.invalidateRegion == Ok {
			return false, errors.New("unreachable, invalidate region should not be Ok")
		}
		s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, retry.invalidateRegion)
		if err = bo.Backoff(retry.policy, retry.err); err != nil {
			return false, err
		}
		return false, nil
	case ErrorRetryRelocate:
		s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, retry.invalidateRegion)
		return false, nil
	case ErrorNoRetryReturnErr:
		return false, retry.err
	case ErrorNoRetryInvalidateRegionAndReturnErr:
		if retry.invalidateRegion == Ok {
			return false, errors.New("unreachable, invalidate region should not be Ok")
		}
		s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, retry.invalidateRegion)
		return false, retry.err
	default:
		return false, errors.Errorf("unreachable, invalid retry type %v", retry.retry)
	}
}

type staleReadMetricsCollector struct {
}

func (s *staleReadMetricsCollector) onReq(req *tikvrpc.Request, isLocalTraffic bool) {
	size := 0
	switch req.Type {
	case tikvrpc.CmdGet:
		size = req.Get().Size()
	case tikvrpc.CmdBatchGet:
		size = req.BatchGet().Size()
	case tikvrpc.CmdScan:
		size = req.Scan().Size()
	case tikvrpc.CmdCop:
		size = req.Cop().Size()
	default:
		// ignore non-read requests
		return
	}
	size += req.Context.Size()
	if isLocalTraffic {
		metrics.StaleReadLocalOutBytes.Add(float64(size))
		metrics.StaleReadReqLocalCounter.Add(1)
	} else {
		metrics.StaleReadRemoteOutBytes.Add(float64(size))
		metrics.StaleReadReqCrossZoneCounter.Add(1)
	}
}

func (s *staleReadMetricsCollector) onResp(tp tikvrpc.CmdType, resp *tikvrpc.Response, isLocalTraffic bool) {
	size := 0
	switch tp {
	case tikvrpc.CmdGet:
		size += resp.Resp.(*kvrpcpb.GetResponse).Size()
	case tikvrpc.CmdBatchGet:
		size += resp.Resp.(*kvrpcpb.BatchGetResponse).Size()
	case tikvrpc.CmdScan:
		size += resp.Resp.(*kvrpcpb.ScanResponse).Size()
	case tikvrpc.CmdCop:
		size += resp.Resp.(*coprocessor.Response).Size()
	default:
		// ignore non-read requests
		return
	}
	if isLocalTraffic {
		metrics.StaleReadLocalInBytes.Add(float64(size))
	} else {
		metrics.StaleReadRemoteInBytes.Add(float64(size))
	}
}

func (s *replicaSelector) replicaType(rpcCtx *RPCContext) string {
	leaderIdx := -1
	switch v := s.state.(type) {
	case *accessKnownLeader:
		return "leader"
	case *tryFollower:
		return "follower"
	case *accessFollower:
		leaderIdx = int(v.leaderIdx)
	case *tryIdleReplica:
		leaderIdx = int(v.leaderIdx)
	}
	if leaderIdx > -1 && rpcCtx != nil && rpcCtx.Peer != nil {
		for idx, replica := range s.replicas {
			if replica.peer.Id == rpcCtx.Peer.Id {
				if idx == leaderIdx {
					return "leader"
				}
				return "follower"
			}
		}
	}
	return "unknown"
}

func (s *replicaSelector) patchRequestSource(req *tikvrpc.Request, rpcCtx *RPCContext) {
	var sb strings.Builder
	defer func() {
		// TiKV does the limit control by the last part of the request source.
		sb.WriteByte('_')
		sb.WriteString(req.InputRequestSource)
		req.RequestSource = sb.String()
	}()

	replicaType := s.replicaType(rpcCtx)

	if req.IsRetryRequest {
		sb.WriteString("retry_")
		sb.WriteString(req.ReadType)
		sb.WriteByte('_')
		sb.WriteString(replicaType)
		return
	}
	if req.StaleRead {
		req.ReadType = "stale_" + replicaType
	} else {
		req.ReadType = replicaType
	}
	sb.WriteString(req.ReadType)
}

type regionErrorHandler int

const (
	regionErrorUnknown regionErrorHandler = iota
	regionErrorNotLeader
	regionErrorRegionNotFound
	regionErrorKeyNotInRegion
	regionErrorEpochNotMatch
	regionErrorServerIsBusy
	regionErrorStaleCommand
	regionErrorStoreNotMatch
	regionErrorRaftEntryTooLarge
	regionErrorMaxTimestampNotSynced
	regionErrorReadIndexNotReady
	regionErrorProposalInMergingMode
	regionErrorDataIsNotReady
	regionErrorRegionNotInitialized
	regionErrorDiskFull
	regionErrorRecoveryInProgress
	regionErrorFlashbackInProcess
	regionErrorFlashbackNotPrepared
	regionErrorIsWitness
	regionErrorMismatchPeerID
	regionErrorBucketVersionNotMatch
	regionErrorDeadlineExceeded
)

func getRegionErrorHandler(e *errorpb.Error) regionErrorHandler {
	if e.GetNotLeader() != nil {
		return regionErrorNotLeader
	} else if e.GetRegionNotFound() != nil {
		return regionErrorRegionNotFound
	} else if e.GetKeyNotInRegion() != nil {
		return regionErrorKeyNotInRegion
	} else if e.GetEpochNotMatch() != nil {
		return regionErrorEpochNotMatch
	} else if e.GetServerIsBusy() != nil {
		return regionErrorServerIsBusy
	} else if e.GetStaleCommand() != nil {
		return regionErrorStaleCommand
	} else if e.GetStoreNotMatch() != nil {
		return regionErrorStoreNotMatch
	} else if e.GetRaftEntryTooLarge() != nil {
		return regionErrorRaftEntryTooLarge
	} else if e.GetMaxTimestampNotSynced() != nil {
		return regionErrorMaxTimestampNotSynced
	} else if e.GetReadIndexNotReady() != nil {
		return regionErrorReadIndexNotReady
	} else if e.GetProposalInMergingMode() != nil {
		return regionErrorProposalInMergingMode
	} else if e.GetDataIsNotReady() != nil {
		return regionErrorDataIsNotReady
	} else if e.GetRegionNotInitialized() != nil {
		return regionErrorRegionNotInitialized
	} else if e.GetDiskFull() != nil {
		return regionErrorDiskFull
	} else if e.GetRecoveryInProgress() != nil {
		return regionErrorRecoveryInProgress
	} else if e.GetFlashbackInProgress() != nil {
		return regionErrorFlashbackInProcess
	} else if e.GetFlashbackNotPrepared() != nil {
		return regionErrorFlashbackNotPrepared
	} else if e.GetIsWitness() != nil {
		return regionErrorIsWitness
	} else if e.GetMismatchPeerId() != nil {
		return regionErrorMismatchPeerID
	} else if e.GetBucketVersionNotMatch() != nil {
		return regionErrorBucketVersionNotMatch
	} else if strings.Contains(e.GetMessage(), "Deadline is exceeded") {
		return regionErrorDeadlineExceeded
	}
	return regionErrorUnknown
}

func (r regionErrorHandler) label() string {
	switch r {
	case regionErrorNotLeader:
		return "not_leader"
	case regionErrorRegionNotFound:
		return "region_not_found"
	case regionErrorKeyNotInRegion:
		return "key_not_in_region"
	case regionErrorEpochNotMatch:
		return "epoch_not_match"
	case regionErrorServerIsBusy:
		return "server_is_busy"
	case regionErrorStaleCommand:
		return "stale_command"
	case regionErrorStoreNotMatch:
		return "store_not_match"
	case regionErrorRaftEntryTooLarge:
		return "raft_entry_too_large"
	case regionErrorMaxTimestampNotSynced:
		return "max_timestamp_not_synced"
	case regionErrorReadIndexNotReady:
		return "read_index_not_ready"
	case regionErrorProposalInMergingMode:
		return "proposal_in_merging_mode"
	case regionErrorDataIsNotReady:
		return "data_is_not_ready"
	case regionErrorRegionNotInitialized:
		return "region_not_initialized"
	case regionErrorDiskFull:
		return "disk_full"
	case regionErrorRecoveryInProgress:
		return "recovery_in_progress"
	case regionErrorFlashbackInProcess:
		return "flashback_in_process"
	case regionErrorFlashbackNotPrepared:
		return "flashback_not_prepared"
	case regionErrorIsWitness:
		return "is_witness"
	case regionErrorMismatchPeerID:
		return "mismatch_peer_id"
	case regionErrorBucketVersionNotMatch:
		return "bucket_version_not_match"
	case regionErrorDeadlineExceeded:
		return "deadline_exceeded"
	default:
		return "unknown"
	}
}

func (r regionErrorHandler) handle(s *RegionRequestSender, bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request,
	regionErr *errorpb.Error) ErrorRetryPolicy {
	ctx := bo.GetCtx()
	// TODO: remove this switch, the cases s.replicaSelector == nil should go another path.
	switch r {
	case regionErrorStaleCommand:
		staleCommand := regionErr.GetStaleCommand()
		return onStaleCommand(s, ctx, rpcCtx, req, staleCommand)
	default:
	}
	if s.replicaSelector == nil {
		return ErrorRetryPolicy{
			retry: ErrorNoRetryReturnErr,
			err:   errors.New("unreachable"),
		}
	}
	state := s.replicaSelector.state
	switch r {
	case regionErrorNotLeader:
		notLeader := regionErr.GetNotLeader()
		return state.onNotLeader(s, ctx, rpcCtx, req, notLeader)
	case regionErrorRegionNotFound:
		regionNotFound := regionErr.GetRegionNotFound()
		return state.onRegionNotFound(s, ctx, rpcCtx, req, regionNotFound)
	case regionErrorKeyNotInRegion:
		keyNotInRegion := regionErr.GetKeyNotInRegion()
		return state.onKeyNotInRegion(s, ctx, rpcCtx, req, keyNotInRegion)
	case regionErrorEpochNotMatch:
		epochNotMatch := regionErr.GetEpochNotMatch()
		return state.onEpochNotMatch(s, bo, rpcCtx, req, epochNotMatch)
	case regionErrorServerIsBusy:
		serverIsBusy := regionErr.GetServerIsBusy()
		return state.onServerIsBusy(s, bo, rpcCtx, req, serverIsBusy)
	case regionErrorStaleCommand:
	case regionErrorStoreNotMatch:
		storeNotMatch := regionErr.GetStoreNotMatch()
		return state.onStoreNotMatch(s, ctx, rpcCtx, req, storeNotMatch)
	case regionErrorRaftEntryTooLarge:
		raftEntryTooLarge := regionErr.GetRaftEntryTooLarge()
		return state.onRaftEntryTooLarge(s, ctx, rpcCtx, req, raftEntryTooLarge)
	case regionErrorMaxTimestampNotSynced:
		maxTimestampNotSynced := regionErr.GetMaxTimestampNotSynced()
		return state.onMaxTimestampNotSynced(s, ctx, rpcCtx, req, maxTimestampNotSynced)
	case regionErrorReadIndexNotReady:
		readIndexNotReady := regionErr.GetReadIndexNotReady()
		return state.onReadIndexNotReady(s, ctx, rpcCtx, req, readIndexNotReady)
	case regionErrorProposalInMergingMode:
		proposalInMergingMode := regionErr.GetProposalInMergingMode()
		return state.onProposalInMergingMode(s, ctx, rpcCtx, req, proposalInMergingMode)
	case regionErrorDataIsNotReady:
		dataIsNotReady := regionErr.GetDataIsNotReady()
		return state.onDataIsNotReady(s, ctx, rpcCtx, req, dataIsNotReady)
	case regionErrorRegionNotInitialized:
		regionNotInitialized := regionErr.GetRegionNotInitialized()
		return state.onRegionNotInitialized(s, ctx, rpcCtx, req, regionNotInitialized)
	case regionErrorDiskFull:
		diskFull := regionErr.GetDiskFull()
		return state.onDiskFull(s, ctx, rpcCtx, req, diskFull)
	case regionErrorRecoveryInProgress:
		recoveryInProgress := regionErr.GetRecoveryInProgress()
		return state.onRecoveryInProgress(s, ctx, rpcCtx, req, recoveryInProgress)
	case regionErrorFlashbackInProcess:
		flashbackInProcess := regionErr.GetFlashbackInProgress()
		return state.onFlashbackInProcess(s, ctx, rpcCtx, req, flashbackInProcess)
	case regionErrorFlashbackNotPrepared:
		flashbackNotPrepared := regionErr.GetFlashbackNotPrepared()
		return state.onFlashbackNotPrepared(s, ctx, rpcCtx, req, flashbackNotPrepared)
	case regionErrorIsWitness:
		isWitness := regionErr.GetIsWitness()
		return state.onIsWitness(s, ctx, rpcCtx, req, isWitness)
	case regionErrorMismatchPeerID:
		mismatchPeerId := regionErr.GetMismatchPeerId()
		return state.onMismatchPeerId(s, ctx, rpcCtx, req, mismatchPeerId)
	case regionErrorBucketVersionNotMatch:
		bucketVersionNotMatch := regionErr.GetBucketVersionNotMatch()
		return state.onBucketVersionNotMatch(s, ctx, rpcCtx, bucketVersionNotMatch)
	case regionErrorDeadlineExceeded:
		return onDeadlineExceeded(s, req)
	case regionErrorUnknown:
		return state.onUnknown(s, ctx, rpcCtx, req, regionErr)
	default:
	}
	logutil.Logger(bo.GetCtx()).Error("unreachable error handler for region error",
		zap.Stringer("regionErr", regionErr),
	)
	return ErrorRetryPolicy{
		retry:            ErrorNoRetryReturnErr,
		invalidateRegion: Ok,
		err:              errors.New("unknown error"),
		policy:           nil,
	}
}

type ErrorRetryMode int

const (
	ErrorRetryUninitialized ErrorRetryMode = iota // Do not use default value.
	ErrorRetryImmediately
	ErrorRetryWithBackoff
	ErrorRetryInvalidateRegion
	// ErrorRetryInvalidateRegionAndBackoff means for some reason, the backoff should be handled here.
	// The sender should be rebuilt after the region is invalidated, but if the backoff is failed, we can stop retrying from outside.
	ErrorRetryInvalidateRegionAndBackoff
	ErrorRetryRelocate
	ErrorNoRetryReturnErr
	ErrorNoRetryInvalidateRegionAndReturnErr
)

// ErrorRetryPolicy is actually an enum actions of the following process, the type is decided by ErrorRetryMode.
type ErrorRetryPolicy struct {
	retry            ErrorRetryMode
	invalidateRegion InvalidReason
	err              error
	policy           *retry.Config
}

type stateErrorHandle interface {
	onSendError(bo *retry.Backoffer, selector *replicaSelector, cause error) ErrorRetryPolicy
	onNotLeader(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, notLeader *errorpb.NotLeader) ErrorRetryPolicy
	onRegionNotFound(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, regionNotFound *errorpb.RegionNotFound) ErrorRetryPolicy
	onKeyNotInRegion(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, keyNotInRegion *errorpb.KeyNotInRegion) ErrorRetryPolicy
	onEpochNotMatch(s *RegionRequestSender, bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request, epochNotMatch *errorpb.EpochNotMatch) ErrorRetryPolicy
	onServerIsBusy(s *RegionRequestSender, bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request, serverIsBusy *errorpb.ServerIsBusy) ErrorRetryPolicy
	// TODO: onStaleCommand
	onStoreNotMatch(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, storeNotMatch *errorpb.StoreNotMatch) ErrorRetryPolicy
	onRaftEntryTooLarge(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, raftEntryTooLarge *errorpb.RaftEntryTooLarge) ErrorRetryPolicy
	onMaxTimestampNotSynced(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, maxTimestampNotSynced *errorpb.MaxTimestampNotSynced) ErrorRetryPolicy
	onReadIndexNotReady(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, readIndexNotReady *errorpb.ReadIndexNotReady) ErrorRetryPolicy
	onProposalInMergingMode(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, proposalInMergingMode *errorpb.ProposalInMergingMode) ErrorRetryPolicy
	onDataIsNotReady(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, dataIsNotReady *errorpb.DataIsNotReady) ErrorRetryPolicy
	onRegionNotInitialized(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, regionNotInitialized *errorpb.RegionNotInitialized) ErrorRetryPolicy
	onDiskFull(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, diskFull *errorpb.DiskFull) ErrorRetryPolicy
	onRecoveryInProgress(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, recoveryInProgress *errorpb.RecoveryInProgress) ErrorRetryPolicy
	onFlashbackInProcess(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, flashbackInProgress *errorpb.FlashbackInProgress) ErrorRetryPolicy
	onFlashbackNotPrepared(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, flashbackNotPrepared *errorpb.FlashbackNotPrepared) ErrorRetryPolicy
	onIsWitness(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, isWitness *errorpb.IsWitness) ErrorRetryPolicy
	onMismatchPeerId(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, mismatchPeerId *errorpb.MismatchPeerId) ErrorRetryPolicy
	onBucketVersionNotMatch(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, bucketVersionNotMatch *errorpb.BucketVersionNotMatch) ErrorRetryPolicy
	onUnknown(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, regionErr *errorpb.Error) ErrorRetryPolicy
}

var _ stateErrorHandle = stateBase{}

func (stateBase) onSendError(bo *retry.Backoffer, selector *replicaSelector, cause error) ErrorRetryPolicy {
	return ErrorRetryPolicy{}
}

func (stateBase) onNotLeader(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, notLeader *errorpb.NotLeader) ErrorRetryPolicy {
	// Retry if error is `NotLeader`.
	logutil.Logger(ctx).Debug(
		"tikv reports `NotLeader` retry later",
		zap.String("notLeader", notLeader.String()),
		zap.String("ctx", rpcCtx.String()),
	)

	policy := retry.BoRegionScheduling
	if s.replicaSelector != nil {
		err := s.replicaSelector.onNotLeader(rpcCtx, notLeader)
		if err == nil {
			return ErrorRetryPolicy{
				retry: ErrorRetryImmediately,
			}
		}
		return ErrorRetryPolicy{
			retry:  ErrorRetryWithBackoff,
			policy: policy,
			err:    err,
		}
	}
	if notLeader.GetLeader() == nil {
		// The peer doesn't know who is the current leader. Generally it's because
		// the Raft group is in an election, but it's possible that the peer is
		// isolated and removed from the Raft group. So it's necessary to reload
		// the region from PD.
		err := errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx)
		return ErrorRetryPolicy{
			retry:            ErrorRetryInvalidateRegion,
			invalidateRegion: NoLeader,
			policy:           policy,
			err:              err,
		}
	}
	// don't backoff if a new leader is returned.
	policy = nil
	s.regionCache.UpdateLeader(rpcCtx.Region, notLeader.GetLeader(), rpcCtx.AccessIdx)
	return ErrorRetryPolicy{
		retry: ErrorRetryImmediately,
	}
}

func (stateBase) onRegionNotFound(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, regionNotFound *errorpb.RegionNotFound) ErrorRetryPolicy {
	return ErrorRetryPolicy{
		retry:            ErrorRetryRelocate,
		invalidateRegion: RegionNotFound, // the earlier code use reason Other
	}
}

func (stateBase) onKeyNotInRegion(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, keyNotInRegion *errorpb.KeyNotInRegion) ErrorRetryPolicy {
	logutil.Logger(ctx).Error(
		"tikv reports `KeyNotInRegion`",
		zap.Stringer("req", req),
		zap.Stringer("ctx", rpcCtx),
	)
	return ErrorRetryPolicy{
		retry:            ErrorRetryRelocate,
		invalidateRegion: Other,
	}
}

func (stateBase) onEpochNotMatch(s *RegionRequestSender, bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request, epochNotMatch *errorpb.EpochNotMatch) ErrorRetryPolicy {
	logutil.Logger(bo.GetCtx()).Debug(
		"tikv reports `EpochNotMatch` retry later",
		zap.Stringer("EpochNotMatch", epochNotMatch),
		zap.Stringer("ctx", rpcCtx),
	)
	shouldRetry, err := s.regionCache.OnRegionEpochNotMatch(bo, rpcCtx, epochNotMatch.CurrentRegions)
	if err != nil {
		return ErrorRetryPolicy{
			retry: ErrorNoRetryReturnErr,
			err:   err,
		}
	}
	retry := ErrorRetryRelocate
	if shouldRetry {
		retry = ErrorRetryImmediately
	}
	return ErrorRetryPolicy{
		retry: retry,
	}
}

func (stateBase) onServerIsBusy(s *RegionRequestSender, bo *retry.Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request,
	serverIsBusy *errorpb.ServerIsBusy) ErrorRetryPolicy {
	if s.replicaSelector != nil {
		shouldRetry, err := s.replicaSelector.onServerIsBusy(bo, rpcCtx, req, serverIsBusy)
		if err != nil {
			return ErrorRetryPolicy{
				retry: ErrorNoRetryReturnErr,
				err:   err,
			}
		}
		retry := ErrorRetryRelocate
		if shouldRetry {
			retry = ErrorRetryImmediately
		}
		return ErrorRetryPolicy{
			retry: retry,
		}
	}
	logutil.Logger(bo.GetCtx()).Warn(
		"tikv reports `ServerIsBusy` retry later",
		zap.String("reason", serverIsBusy.GetReason()),
		zap.Stringer("ctx", rpcCtx),
	)
	err := errors.Errorf("server is busy, ctx: %v", rpcCtx)
	var policy *retry.Config
	if rpcCtx != nil && rpcCtx.Store != nil && rpcCtx.Store.storeType.IsTiFlashRelatedType() {
		policy = retry.BoTiFlashServerBusy
	} else {
		policy = retry.BoTiKVServerBusy
	}
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: policy,
		err:    err,
	}
}

func onStaleCommand(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, staleCommand *errorpb.StaleCommand) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", rpcCtx))
	if s.replicaSelector != nil {
		// Needn't backoff because the new leader should be elected soon
		// and the replicaSelector will try the next peer.
		return ErrorRetryPolicy{
			retry: ErrorRetryImmediately,
		}
	}
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: retry.BoStaleCmd,
		err:    errors.Errorf("stale command, ctx: %v", ctx),
	}
}

func (stateBase) onStoreNotMatch(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, storeNotMatch *errorpb.StoreNotMatch) ErrorRetryPolicy {
	// store not match
	logutil.Logger(ctx).Debug(
		"tikv reports `StoreNotMatch` retry later",
		zap.Stringer("storeNotMatch", storeNotMatch),
		zap.Stringer("ctx", rpcCtx),
	)
	rpcCtx.Store.markNeedCheck(s.regionCache.notifyCheckCh)
	// It's possible the address of store is not changed but the DNS resolves to a different address in k8s environment,
	// so we always reconnect in this case.
	s.client.CloseAddr(rpcCtx.Addr)
	return ErrorRetryPolicy{
		retry:            ErrorRetryRelocate,
		invalidateRegion: Other,
	}
}

func (stateBase) onRaftEntryTooLarge(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, raftEntryTooLarge *errorpb.RaftEntryTooLarge) ErrorRetryPolicy {
	logutil.Logger(ctx).Warn("tikv reports `RaftEntryTooLarge`", zap.Stringer("ctx", rpcCtx))
	return ErrorRetryPolicy{
		retry: ErrorNoRetryReturnErr,
		err:   errors.New(raftEntryTooLarge.String()),
	}
}

func (stateBase) onMaxTimestampNotSynced(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, maxTimestampNotSynced *errorpb.MaxTimestampNotSynced) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug("tikv reports `MaxTimestampNotSynced`", zap.Stringer("ctx", rpcCtx))
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: retry.BoMaxTsNotSynced,
		err:    errors.Errorf("max timestamp not synced, ctx: %v", rpcCtx),
	}
}

func (stateBase) onReadIndexNotReady(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, readIndexNotRead *errorpb.ReadIndexNotReady) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug(
		"tikv reports `ReadIndexNotReady` retry later",
		zap.Uint64("store-id", rpcCtx.Store.storeID),
		zap.Uint64("region-id", readIndexNotRead.GetRegionId()),
		zap.Stringer("ctx", rpcCtx),
	)
	// The region can't provide service until split or merge finished, so backoff.
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: retry.BoRegionScheduling,
		err:    errors.Errorf("read index not ready, ctx: %v", rpcCtx),
	}

}

func (stateBase) onProposalInMergingMode(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, proposalInMergingMode *errorpb.ProposalInMergingMode) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug("tikv reports `ProposalInMergingMode`", zap.Stringer("ctx", rpcCtx))
	// The region is merging and it can't provide service until merge finished, so backoff.
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: retry.BoRegionScheduling,
		err:    errors.Errorf("region is merging, ctx: %v", rpcCtx),
	}
}

func (stateBase) onDataIsNotReady(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, dataIsNotReady *errorpb.DataIsNotReady) ErrorRetryPolicy {
	logutil.Logger(ctx).Warn(
		"tikv reports `DataIsNotReady` retry later",
		zap.Uint64("store-id", rpcCtx.Store.storeID),
		zap.Uint64("peer-id", dataIsNotReady.GetPeerId()),
		zap.Uint64("region-id", dataIsNotReady.GetRegionId()),
		zap.Uint64("safe-ts", dataIsNotReady.GetSafeTs()),
		zap.Stringer("ctx", rpcCtx),
	)
	return ErrorRetryPolicy{
		retry: ErrorRetryImmediately,
	}
}

func (stateBase) onRegionNotInitialized(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, regionNotInitialized *errorpb.RegionNotInitialized) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug(
		"tikv reports `RegionNotInitialized` retry later",
		zap.Uint64("store-id", rpcCtx.Store.storeID),
		zap.Uint64("region-id", regionNotInitialized.GetRegionId()),
		zap.Stringer("ctx", rpcCtx),
	)
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: retry.BoMaxRegionNotInitialized,
		err:    errors.New("region not initialized"),
	}
}

func (stateBase) onDiskFull(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, diskFull *errorpb.DiskFull) ErrorRetryPolicy {
	return ErrorRetryPolicy{
		retry:  ErrorRetryWithBackoff,
		policy: retry.BoTiKVDiskFull,
		err:    errors.Errorf("tikv disk full: %v ctx: %v", diskFull.String(), rpcCtx.String()),
	}
}

func (stateBase) onRecoveryInProgress(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, recoveryInProgress *errorpb.RecoveryInProgress) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug("tikv reports `RecoveryInProgress`", zap.Stringer("ctx", rpcCtx))
	return ErrorRetryPolicy{
		retry:            ErrorRetryInvalidateRegionAndBackoff,
		policy:           retry.BoTiKVDiskFull,
		invalidateRegion: Other,
		err:              errors.Errorf("region recovery in progress, ctx: %v", rpcCtx),
	}
}

func (stateBase) onFlashbackInProcess(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, flashbackInProgress *errorpb.FlashbackInProgress) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug(
		"tikv reports `FlashbackInProgress`",
		zap.Stringer("req", req),
		zap.Stringer("ctx", rpcCtx),
	)
	if req != nil {
		// if the failure is caused by replica read, we can retry it with leader safely.
		if rpcCtx.contextPatcher.replicaRead != nil && *rpcCtx.contextPatcher.replicaRead {
			req.BusyThresholdMs = 0
			s.replicaSelector.busyThreshold = 0
			rpcCtx.contextPatcher.replicaRead = nil
			rpcCtx.contextPatcher.busyThreshold = nil
			return ErrorRetryPolicy{
				retry: ErrorRetryImmediately,
			}
		}
		if req.ReplicaReadType.IsFollowerRead() {
			s.replicaSelector = nil
			req.ReplicaReadType = kv.ReplicaReadLeader
			return ErrorRetryPolicy{
				retry: ErrorRetryImmediately,
			}
		}
	}
	return ErrorRetryPolicy{
		retry: ErrorNoRetryReturnErr,
		err: errors.Errorf(
			"region %d is in flashback progress, FlashbackStartTS is %d",
			flashbackInProgress.GetRegionId(), flashbackInProgress.GetFlashbackStartTs(),
		),
	}
}

func (stateBase) onFlashbackNotPrepared(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, flashbackNotPrepared *errorpb.FlashbackNotPrepared) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug(
		"tikv reports `FlashbackNotPrepared`",
		zap.Stringer("req", req),
		zap.Stringer("ctx", rpcCtx),
	)
	return ErrorRetryPolicy{
		retry: ErrorNoRetryReturnErr,
		err: errors.Errorf(
			"region %d is not prepared for the flashback",
			flashbackNotPrepared.GetRegionId(),
		),
	}
}

func (stateBase) onIsWitness(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, isWitness *errorpb.IsWitness) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug("tikv reports `IsWitness`", zap.Stringer("ctx", rpcCtx))
	return ErrorRetryPolicy{
		retry:            ErrorNoRetryInvalidateRegionAndReturnErr,
		invalidateRegion: Other,
		err:              errors.Errorf("is witness, ctx: %v", rpcCtx),
	}
}

func (stateBase) onMismatchPeerId(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, mismatchPeerId *errorpb.MismatchPeerId) ErrorRetryPolicy {
	logutil.Logger(ctx).Warn(
		"tikv reports `MismatchPeerId`, invalidate region cache",
		zap.Uint64("req peer id", mismatchPeerId.GetRequestPeerId()),
		zap.Uint64("store peer id", mismatchPeerId.GetStorePeerId()),
	)
	return ErrorRetryPolicy{
		retry:            ErrorRetryRelocate,
		invalidateRegion: Other,
	}
}

func (stateBase) onBucketVersionNotMatch(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, bucketVersionNotMatch *errorpb.BucketVersionNotMatch) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug(
		"tikv reports `BucketVersionNotMatch` retry later",
		zap.Uint64("latest bucket version", bucketVersionNotMatch.GetVersion()),
		zap.Uint64("request bucket version", rpcCtx.BucketVersion),
		zap.Stringer("ctx", rpcCtx),
	)
	// bucket version is not match, we should split this cop request again.
	s.regionCache.OnBucketVersionNotMatch(rpcCtx, bucketVersionNotMatch.Version, bucketVersionNotMatch.Keys)
	return ErrorRetryPolicy{
		retry: ErrorNoRetryReturnErr,
	}
}

func onDeadlineExceeded(s *RegionRequestSender, req *tikvrpc.Request) ErrorRetryPolicy {
	if s.replicaSelector != nil && s.replicaSelector.onReadReqConfigurableTimeout(req) {
		return ErrorRetryPolicy{
			retry: ErrorRetryImmediately,
		}
	}
	return ErrorRetryPolicy{
		retry: ErrorNoRetryReturnErr,
		err:   errors.New("deadline exceeded"),
	}
}

func (stateBase) onUnknown(s *RegionRequestSender, ctx context.Context, rpcCtx *RPCContext, req *tikvrpc.Request, regionErr *errorpb.Error) ErrorRetryPolicy {
	logutil.Logger(ctx).Debug(
		"tikv reports region failed",
		zap.Stringer("regionErr", regionErr),
		zap.Stringer("ctx", rpcCtx),
	)

	if s.replicaSelector != nil {
		// Try the next replica.
		return ErrorRetryPolicy{
			retry: ErrorRetryImmediately,
		}
	}

	// When the request is sent to TiDB, there is no region in the request, so the region id will be 0.
	// So when region id is 0, there is no business with region cache.
	if rpcCtx.Region.id != 0 {
		return ErrorRetryPolicy{
			retry:            ErrorRetryInvalidateRegion,
			invalidateRegion: Other,
		}
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	return ErrorRetryPolicy{
		retry: ErrorNoRetryReturnErr,
		err:   errors.Errorf("unknown error: %s", regionErr.GetMessage()),
	}
}
