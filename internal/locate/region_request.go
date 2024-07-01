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
	"context"
	"fmt"
	"maps"
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
	"github.com/tikv/pd/client/errs"
	pderr "github.com/tikv/pd/client/errs"
)

// shuttingDown is a flag to indicate tidb-server is exiting (Ctrl+C signal
// receved for example). If this flag is set, tikv client should not retry on
// network error because tidb-server expect tikv client to exit as soon as possible.
var shuttingDown uint32

// randIntn is only use for testing.
var randIntn = rand.Intn

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
	Stats             *RegionRequestRuntimeStats
	AccessStats       *ReplicaAccessStats
}

func (s *RegionRequestSender) String() string {
	if s.replicaSelector == nil {
		return fmt.Sprintf("{rpcError:%v, replicaSelector: <nil>}", s.rpcError)
	}
	return fmt.Sprintf("{rpcError:%v, replicaSelector: %v}", s.rpcError, s.replicaSelector.String())
}

// RegionRequestRuntimeStats records the runtime stats of send region requests.
type RegionRequestRuntimeStats struct {
	RPCStats map[tikvrpc.CmdType]*RPCRuntimeStats
	RequestErrorStats
}

// RequestErrorStats records the request error(region error and rpc error) count.
type RequestErrorStats struct {
	// ErrStats record the region error and rpc error, and their count.
	// Attention: avoid too many error types, ErrStats only record the first 16 different errors.
	ErrStats    map[string]int
	OtherErrCnt int
}

// NewRegionRequestRuntimeStats returns a new RegionRequestRuntimeStats.
func NewRegionRequestRuntimeStats() *RegionRequestRuntimeStats {
	return &RegionRequestRuntimeStats{
		RPCStats: make(map[tikvrpc.CmdType]*RPCRuntimeStats),
	}
}

// RPCRuntimeStats indicates the RPC request count and consume time.
type RPCRuntimeStats struct {
	Count int64
	// Send region request consume time.
	Consume int64
}

// RecordRPCRuntimeStats uses to record the rpc count and duration stats.
func (r *RegionRequestRuntimeStats) RecordRPCRuntimeStats(cmd tikvrpc.CmdType, d time.Duration) {
	stat, ok := r.RPCStats[cmd]
	if !ok {
		r.RPCStats[cmd] = &RPCRuntimeStats{
			Count:   1,
			Consume: int64(d),
		}
		return
	}
	stat.Count++
	stat.Consume += int64(d)
}

// RecordRPCErrorStats uses to record the request error(region error label and rpc error) info and count.
func (r *RequestErrorStats) RecordRPCErrorStats(errLabel string) {
	if r.ErrStats == nil {
		// lazy init to avoid unnecessary allocation.
		r.ErrStats = make(map[string]int)
	}
	if len(r.ErrStats) < 16 {
		// Avoid too many error.
		r.ErrStats[errLabel]++
	} else {
		r.OtherErrCnt++
	}
}

// getErrMsg returns error message. if the error has cause error, then return cause error message.
func getErrMsg(err error) string {
	if err == nil {
		return ""
	}
	if causeErr := errors.Cause(err); causeErr != nil {
		return causeErr.Error()
	}
	return err.Error()
}

// String implements fmt.Stringer interface.
func (r *RegionRequestRuntimeStats) String() string {
	if r == nil {
		return ""
	}
	var builder strings.Builder
	for k, v := range r.RPCStats {
		if builder.Len() > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(k.String())
		builder.WriteString(":{num_rpc:")
		builder.WriteString(strconv.FormatInt(v.Count, 10))
		builder.WriteString(", total_time:")
		builder.WriteString(util.FormatDuration(time.Duration(v.Consume)))
		builder.WriteString("}")
	}
	if errStatsStr := r.RequestErrorStats.String(); errStatsStr != "" {
		builder.WriteString(", rpc_errors:")
		builder.WriteString(errStatsStr)
	}
	return builder.String()
}

// String implements fmt.Stringer interface.
func (r *RequestErrorStats) String() string {
	if len(r.ErrStats) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.WriteString("{")
	for err, cnt := range r.ErrStats {
		if builder.Len() > 2 {
			builder.WriteString(", ")
		}
		builder.WriteString(err)
		builder.WriteString(":")
		builder.WriteString(strconv.Itoa(cnt))
	}
	if r.OtherErrCnt > 0 {
		builder.WriteString(", other_error:")
		builder.WriteString(strconv.Itoa(r.OtherErrCnt))
	}
	builder.WriteString("}")
	return builder.String()
}

// Clone returns a copy of itself.
func (r *RegionRequestRuntimeStats) Clone() *RegionRequestRuntimeStats {
	newRs := NewRegionRequestRuntimeStats()
	maps.Copy(newRs.RPCStats, r.RPCStats)
	if len(r.ErrStats) > 0 {
		newRs.ErrStats = make(map[string]int)
		maps.Copy(newRs.ErrStats, r.ErrStats)
		newRs.OtherErrCnt = r.OtherErrCnt
	}
	return newRs
}

// Merge merges other RegionRequestRuntimeStats.
func (r *RegionRequestRuntimeStats) Merge(rs *RegionRequestRuntimeStats) {
	if rs == nil {
		return
	}
	for cmd, v := range rs.RPCStats {
		stat, ok := r.RPCStats[cmd]
		if !ok {
			r.RPCStats[cmd] = &RPCRuntimeStats{
				Count:   v.Count,
				Consume: v.Consume,
			}
			continue
		}
		stat.Count += v.Count
		stat.Consume += v.Consume
	}
	if len(rs.ErrStats) > 0 {
		if r.ErrStats == nil {
			r.ErrStats = make(map[string]int)
		}
		for err, cnt := range rs.ErrStats {
			r.ErrStats[err] += cnt
		}
		r.OtherErrCnt += rs.OtherErrCnt
	}
}

// ReplicaAccessStats records the replica access info.
type ReplicaAccessStats struct {
	// AccessInfos records the access info
	AccessInfos []ReplicaAccessInfo
	// avoid to consume too much memory, after more than 5 records, count them by peerID in `OverflowAccessStat` map.
	OverflowAccessStat map[uint64]*RequestErrorStats
}

// ReplicaAccessInfo indicates the access path detail info of a request.
type ReplicaAccessInfo struct {
	Peer      uint64
	Store     uint64
	ReqReadTp ReqReadType
	Err       string
}

type ReqReadType byte

const (
	ReqLeader ReqReadType = iota
	ReqReplicaRead
	ReqStaleRead
)

func (s *ReplicaAccessStats) recordReplicaAccessInfo(staleRead, replicaRead bool, peerID, storeID uint64, err string) {
	if len(s.AccessInfos) < 5 {
		tp := ReqLeader
		if replicaRead {
			tp = ReqReplicaRead
		} else if staleRead {
			tp = ReqStaleRead
		}
		s.AccessInfos = append(s.AccessInfos, ReplicaAccessInfo{
			Peer:      peerID,
			Store:     storeID,
			ReqReadTp: tp,
			Err:       err,
		})
		return
	}
	if s.OverflowAccessStat == nil {
		s.OverflowAccessStat = make(map[uint64]*RequestErrorStats)
	}
	stat, ok := s.OverflowAccessStat[peerID]
	if !ok {
		stat = &RequestErrorStats{}
		s.OverflowAccessStat[peerID] = stat
	}
	stat.RecordRPCErrorStats(err)
}

// String implements fmt.Stringer interface.
func (s *ReplicaAccessStats) String() string {
	if s == nil {
		return ""
	}
	var builder strings.Builder
	for i, info := range s.AccessInfos {
		if i > 0 {
			builder.WriteString(", ")
		}
		switch info.ReqReadTp {
		case ReqLeader:
			builder.WriteString("{")
		case ReqReplicaRead:
			builder.WriteString("{replica_read, ")
		case ReqStaleRead:
			builder.WriteString("{stale_read, ")
		}
		builder.WriteString("peer:")
		builder.WriteString(strconv.FormatUint(info.Peer, 10))
		builder.WriteString(", store:")
		builder.WriteString(strconv.FormatUint(info.Store, 10))
		builder.WriteString(", err:")
		builder.WriteString(info.Err)
		builder.WriteString("}")
	}
	if len(s.OverflowAccessStat) > 0 {
		builder.WriteString(", overflow_count:{")
		cnt := 0
		for peerID, stat := range s.OverflowAccessStat {
			if stat == nil {
				continue
			}
			if cnt > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString("{peer:")
			builder.WriteString(strconv.FormatUint(peerID, 10))
			builder.WriteString(", error_stats:")
			builder.WriteString(stat.String())
			builder.WriteString("}")
			cnt++
		}
		builder.WriteString("}")
	}
	return builder.String()
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

// getClientExt returns the client with ClientExt interface.
// Return nil if the client does not implement ClientExt.
// Don't use in critical path.
func (s *RegionRequestSender) getClientExt() client.ClientExt {
	ext, _ := s.client.(client.ClientExt)
	return ext
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

func (s *RegionRequestSender) recordRPCAccessInfo(req *tikvrpc.Request, rpcCtx *RPCContext, err string) {
	if req == nil || rpcCtx == nil || rpcCtx.Peer == nil || rpcCtx.Store == nil {
		return
	}
	if s.AccessStats == nil {
		s.AccessStats = &ReplicaAccessStats{}
	}
	s.AccessStats.recordReplicaAccessInfo(req.StaleRead, req.ReplicaRead, rpcCtx.Peer.GetId(), rpcCtx.Store.storeID, err)
}

type replica struct {
	store         *Store
	peer          *metapb.Peer
	epoch         uint32
	attempts      int
	attemptedTime time.Duration
	flag          uint8
}

func (r *replica) getEpoch() uint32 {
	return atomic.LoadUint32(&r.epoch)
}

func (r *replica) isEpochStale() bool {
	return r.epoch != atomic.LoadUint32(&r.store.epoch)
}

func (r *replica) isExhausted(maxAttempt int, maxAttemptTime time.Duration) bool {
	return r.attempts >= maxAttempt || (maxAttemptTime > 0 && r.attemptedTime >= maxAttemptTime)
}

func (r *replica) onUpdateLeader() {
	if r.isExhausted(maxReplicaAttempt, maxReplicaAttemptTime) {
		// Give the replica one more chance and because each follower is tried only once,
		// it won't result in infinite retry.
		r.attempts = maxReplicaAttempt - 1
		r.attemptedTime = 0
	}
	r.deleteFlag(notLeaderFlag)
}

const (
	deadlineErrUsingConfTimeoutFlag uint8 = 1 << iota // deadlineErrUsingConfTimeoutFlag indicates the replica is already tried, but the received deadline exceeded error.
	dataIsNotReadyFlag                                // dataIsNotReadyFlag indicates the replica is already tried, but the received data is not ready error.
	notLeaderFlag                                     // notLeaderFlag indicates the replica is already tried, but the received not leader error.
	serverIsBusyFlag                                  // serverIsBusyFlag indicates the replica is already tried, but the received server is busy error.
)

func (r *replica) addFlag(flag uint8) {
	r.flag |= flag
}

func (r *replica) deleteFlag(flag uint8) {
	r.flag &= ^flag
}

func (r *replica) hasFlag(flag uint8) bool {
	return (r.flag & flag) > 0
}

type baseReplicaSelector struct {
	regionCache *RegionCache
	region      *Region
	replicas    []*replica
	// TiKV can reject the request when its estimated wait duration exceeds busyThreshold.
	// Then, the client will receive a ServerIsBusy error and choose another replica to retry.
	busyThreshold time.Duration
	// pendingBackoffs records the pending backoff by store_id for fast retry. Here are some examples to show how it works:
	// Example-1, fast retry and success:
	//      1. send req to store 1, got ServerIsBusy region error, record `store1 -> BoTiKVServerBusy` backoff in pendingBackoffs and fast retry next replica.
	//      2. retry in store2, and success.
	//         Since the request is success, we can skip the backoff and fast return result to user.
	// Example-2: fast retry different replicas but all failed:
	//      1. send req to store 1, got ServerIsBusy region error, record `store1 -> BoTiKVServerBusy` backoff in pendingBackoffs and fast retry next replica.
	//      2. send req to store 2, got ServerIsBusy region error, record `store2 -> BoTiKVServerBusy` backoff in pendingBackoffs and fast retry next replica.
	//      3. send req to store 3, got ServerIsBusy region error, record `store3 -> BoTiKVServerBusy` backoff in pendingBackoffs and fast retry next replica.
	//      4. no candidate since all stores are busy. But before return no candidate error to up layer, we need to call backoffOnNoCandidate function
	//         to apply a max pending backoff, the backoff is to avoid frequent access and increase the pressure on the cluster.
	// Example-3: fast retry same replica:
	//      1. send req to store 1, got ServerIsBusy region error, record `store1 -> BoTiKVServerBusy` backoff in pendingBackoffs and fast retry next replica.
	//      2. assume store 2 and store 3 are unreachable.
	//      3. re-send req to store 1 with replica-read. But before re-send to store1, we need to call backoffOnRetry function
	//         to apply pending BoTiKVServerBusy backoff, the backoff is to avoid frequent access and increase the pressure on the cluster.
	pendingBackoffs map[uint64]*backoffArgs
}

func (s *baseReplicaSelector) String() string {
	var replicaStatus []string
	cacheRegionIsValid := "unknown"
	if s != nil {
		if s.region != nil {
			if s.region.isValid() {
				cacheRegionIsValid = "true"
			} else {
				cacheRegionIsValid = "false"
			}
		}
		for _, replica := range s.replicas {
			replicaStatus = append(replicaStatus, fmt.Sprintf("peer: %v, store: %v, isEpochStale: %v, "+
				"attempts: %v, attempts_time: %v, replica-epoch: %v, store-epoch: %v, store-state: %v, store-liveness-state: %v",
				replica.peer.GetId(),
				replica.store.storeID,
				replica.isEpochStale(),
				replica.attempts,
				util.FormatDuration(replica.attemptedTime),
				replica.getEpoch(),
				atomic.LoadUint32(&replica.store.epoch),
				replica.store.getResolveState(),
				replica.store.getLivenessState(),
			))
		}
	}
	return fmt.Sprintf("cacheRegionIsValid: %v, replicaStatus: %v", cacheRegionIsValid, replicaStatus)
}

const (
	maxReplicaAttempt = 10
	// The maximum time to allow retrying sending requests after RPC failure. In case an RPC request fails after
	// timeout (there might be network issue or the TiKV node stuck), we use this to avoid retrying 10 times which may cost too much time.
	// For request using `client.ReadTimeoutShort` which is 30s, it might retry twice which costs 1min.
	maxReplicaAttemptTime = time.Second * 50
)

func (s *baseReplicaSelector) buildRPCContext(bo *retry.Backoffer, targetReplica, proxyReplica *replica) (*RPCContext, error) {
	// Backoff and retry if no replica is selected or the selected replica is stale
	if targetReplica == nil || targetReplica.isEpochStale() ||
		(proxyReplica != nil && proxyReplica.isEpochStale()) {
		// TODO(youjiali1995): Is it necessary to invalidate the region?
		metrics.TiKVReplicaSelectorFailureCounter.WithLabelValues("stale_store").Inc()
		s.invalidateRegion()
		return nil, nil
	}

	rpcCtx := &RPCContext{
		ClusterID:  s.regionCache.clusterID,
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

func (s *baseReplicaSelector) checkLiveness(bo *retry.Backoffer, accessReplica *replica) livenessState {
	return accessReplica.store.requestLivenessAndStartHealthCheckLoopIfNeeded(bo, s.regionCache.bg, s.regionCache.stores)
}

func (s *baseReplicaSelector) invalidateReplicaStore(replica *replica, cause error) {
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
		s.regionCache.stores.markStoreNeedCheck(store)
		store.healthStatus.markAlreadySlow()
	}
}

// updateLeader updates the leader of the cached region.
// If the leader peer isn't found in the region, the region will be invalidated.
// If switch to new leader successfully, returns the AccessIndex of the new leader in the replicas.
func (s *baseReplicaSelector) updateLeader(leader *metapb.Peer) int {
	if leader == nil {
		return -1
	}
	for i, replica := range s.replicas {
		if isSamePeer(replica.peer, leader) {
			// If hibernate region is enabled and the leader is not reachable, the raft group
			// will not be wakened up and re-elect the leader until the follower receives
			// a request. So, before the new leader is elected, we should not send requests
			// to the unreachable old leader to avoid unnecessary timeout.
			if replica.store.getLivenessState() != reachable {
				return -1
			}
			replica.onUpdateLeader()
			// Update the workTiKVIdx so that following requests can be sent to the leader immediately.
			if !s.region.switchWorkLeaderToPeer(leader) {
				panic("the store must exist")
			}
			logutil.BgLogger().Debug(
				"switch region leader to specific leader due to kv return NotLeader",
				zap.Uint64("regionID", s.region.GetID()),
				zap.Uint64("leaderStoreID", leader.GetStoreId()),
			)
			return i
		}
	}
	// Invalidate the region since the new leader is not in the cached version.
	s.region.invalidate(StoreNotFound)
	return -1
}

func (s *baseReplicaSelector) invalidateRegion() {
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
			if err != nil || selector == nil {
				return nil, err
			}
			s.replicaSelector = selector
		}
		return s.replicaSelector.next(bo, req)
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

const slowLogSendReqTime = 100 * time.Millisecond

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
	startTime := time.Now()
	startBackOff := bo.GetTotalSleep()
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
			if s.replicaSelector != nil {
				if err := s.replicaSelector.backoffOnNoCandidate(bo); err != nil {
					return nil, nil, retryTimes, err
				}
				if cost := time.Since(startTime); cost > slowLogSendReqTime || cost > timeout {
					s.logSendReqError(bo, "throwing pseudo region error due to no replica available", regionID, retryTimes, req, cost, bo.GetTotalSleep()-startBackOff, timeout)
				}
			}
			resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
			return resp, nil, retryTimes, err
		}

		var isLocalTraffic bool
		if staleReadCollector != nil && s.replicaSelector != nil && s.replicaSelector.target != nil {
			isLocalTraffic = s.replicaSelector.target.store.IsLabelsMatch(s.replicaSelector.option.labels)
			staleReadCollector.onReq(req, isLocalTraffic)
		}

		logutil.Eventf(bo.GetCtx(), "send %s request to region %d at %s", req.Type, regionID.id, rpcCtx.Addr)
		s.storeAddr = rpcCtx.Addr

		if _, err := util.EvalFailpoint("beforeSendReqToRegion"); err == nil {
			if hook := bo.GetCtx().Value("sendReqToRegionHook"); hook != nil {
				h := hook.(func(*tikvrpc.Request))
				h(req)
			}
		}

		req.Context.ClusterId = rpcCtx.ClusterID
		if req.InputRequestSource != "" && s.replicaSelector != nil {
			patchRequestSource(req, s.replicaSelector.replicaType())
		}
		if e := tikvrpc.SetContext(req, rpcCtx.Meta, rpcCtx.Peer); e != nil {
			return nil, nil, retryTimes, err
		}
		if s.replicaSelector != nil {
			if err := s.replicaSelector.backoffOnRetry(rpcCtx.Store, bo); err != nil {
				return nil, nil, retryTimes, err
			}
		}

		var retry bool
		resp, retry, err = s.sendReqToRegion(bo, rpcCtx, req, timeout)
		req.IsRetryRequest = true
		if err != nil {
			if cost := time.Since(startTime); cost > slowLogSendReqTime || cost > timeout {
				msg := fmt.Sprintf("send request failed, err: %v", err.Error())
				s.logSendReqError(bo, msg, regionID, retryTimes, req, cost, bo.GetTotalSleep()-startBackOff, timeout)
			}
			return nil, nil, retryTimes, err
		}

		if _, err1 := util.EvalFailpoint("afterSendReqToRegion"); err1 == nil {
			if hook := bo.GetCtx().Value("sendReqToRegionFinishHook"); hook != nil {
				h := hook.(func(*tikvrpc.Request, *tikvrpc.Response, error))
				h(req, resp, err)
			}
		}

		// recheck whether the session/query is killed during the Next()
		if err2 := bo.CheckKilled(); err2 != nil {
			return nil, nil, retryTimes, err2
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
			retry, err = s.onRegionError(bo, rpcCtx, req, regionErr)
			if err != nil {
				if cost := time.Since(startTime); cost > slowLogSendReqTime || cost > timeout {
					msg := fmt.Sprintf("send request on region error failed, err: %v", err.Error())
					s.logSendReqError(bo, msg, regionID, retryTimes, req, cost, bo.GetTotalSleep()-startBackOff, timeout)
				}
				return nil, nil, retryTimes, err
			}
			if retry {
				retryTimes++
				continue
			}
			if cost := time.Since(startTime); cost > slowLogSendReqTime || cost > timeout {
				s.logSendReqError(bo, "send request meet region error without retry", regionID, retryTimes, req, cost, bo.GetTotalSleep()-startBackOff, timeout)
			}
		} else {
			if s.replicaSelector != nil {
				s.replicaSelector.onSendSuccess(req)
			}
		}
		if staleReadCollector != nil {
			staleReadCollector.onResp(req.Type, resp, isLocalTraffic)
		}
		return resp, rpcCtx, retryTimes, nil
	}
}

func (s *RegionRequestSender) logSendReqError(bo *retry.Backoffer, msg string, regionID RegionVerID, retryTimes int, req *tikvrpc.Request, cost time.Duration, currentBackoffMs int, timeout time.Duration) {
	var builder strings.Builder
	// build the total round stats string.
	builder.WriteString("{total-backoff: ")
	builder.WriteString(util.FormatDuration(time.Duration(bo.GetTotalSleep() * int(time.Millisecond))))
	builder.WriteString(", total-backoff-times: ")
	builder.WriteString(strconv.Itoa(bo.GetTotalBackoffTimes()))
	if s.Stats != nil {
		builder.WriteString(", total-rpc: {")
		builder.WriteString(s.Stats.String())
		builder.WriteString("}")
	}
	builder.WriteString("}")
	totalRoundStats := builder.String()

	// build the current round stats string.
	builder.Reset()
	builder.WriteString("{time: ")
	builder.WriteString(util.FormatDuration(cost))
	builder.WriteString(", backoff: ")
	builder.WriteString(util.FormatDuration(time.Duration(currentBackoffMs * int(time.Millisecond))))
	builder.WriteString(", timeout: ")
	builder.WriteString(util.FormatDuration(timeout))
	builder.WriteString(", req-max-exec-timeout: ")
	builder.WriteString(util.FormatDuration(time.Duration(int64(req.Context.MaxExecutionDurationMs) * int64(time.Millisecond))))
	builder.WriteString(", retry-times: ")
	builder.WriteString(strconv.Itoa(retryTimes))
	if s.AccessStats != nil {
		builder.WriteString(", replica-access: {")
		builder.WriteString(s.AccessStats.String())
		builder.WriteString("}")
	}
	builder.WriteString("}")
	currentRoundStats := builder.String()
	logutil.Logger(bo.GetCtx()).Info(msg,
		zap.Uint64("req-ts", req.GetStartTS()),
		zap.String("req-type", req.Type.String()),
		zap.String("region", regionID.String()),
		zap.String("replica-read-type", req.ReplicaReadType.String()),
		zap.Bool("stale-read", req.StaleRead),
		zap.Stringer("request-sender", s),
		zap.String("total-round-stats", totalRoundStats),
		zap.String("current-round-stats", currentRoundStats))
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
		rpcDuration := time.Since(start)
		if s.replicaSelector != nil {
			recordAttemptedTime(s.replicaSelector, rpcDuration)
		}
		// Record timecost of external requests on related Store when `ReplicaReadMode == "PreferLeader"`.
		if rpcCtx.Store != nil && req.ReplicaReadType == kv.ReplicaReadPreferLeader && !util.IsInternalRequest(req.RequestSource) {
			rpcCtx.Store.healthStatus.recordClientSideSlowScoreStat(rpcDuration)
		}
		if s.Stats != nil {
			s.Stats.RecordRPCRuntimeStats(req.Type, rpcDuration)
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

		if _, e := util.EvalFailpoint("onRPCFinishedHook"); e == nil {
			if hook := bo.GetCtx().Value("onRPCFinishedHook"); hook != nil {
				h := hook.(func(*tikvrpc.Request, *tikvrpc.Response, error) (*tikvrpc.Response, error))
				resp, err = h(req, resp, err)
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
		if isRPCError(err) {
			s.rpcError = err
		}
		if s.Stats != nil {
			errStr := getErrMsg(err)
			s.Stats.RecordRPCErrorStats(errStr)
			s.recordRPCAccessInfo(req, rpcCtx, errStr)
		}
		// Because in rpc logic, context.Cancel() will be transferred to rpcContext.Cancel error. For rpcContext cancel,
		// we need to retry the request. But for context cancel active, for example, limitExec gets the required rows,
		// we shouldn't retry the request, it will go to backoff and hang in retry logic.
		if ctx.Err() != nil && errors.Cause(ctx.Err()) == context.Canceled {
			metrics.TiKVRPCErrorCounter.WithLabelValues("context-canceled", storeIDLabel(rpcCtx)).Inc()
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

func isRPCError(err error) bool {
	// exclude ErrClientResourceGroupThrottled
	return err != nil && errs.ErrClientResourceGroupThrottled.NotEqual(err)
}

func storeIDLabel(rpcCtx *RPCContext) string {
	if rpcCtx != nil && rpcCtx.Store != nil {
		return strconv.FormatUint(rpcCtx.Store.storeID, 10)
	}
	return "nil"
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
		st.tokenCount.Add(-1)
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
	storeLabel := storeIDLabel(ctx)
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		metrics.TiKVRPCErrorCounter.WithLabelValues("context-canceled", storeLabel).Inc()
		return errors.WithStack(err)
	} else if LoadShuttingDown() > 0 {
		metrics.TiKVRPCErrorCounter.WithLabelValues("shutting-down", storeLabel).Inc()
		return errors.WithStack(tikverr.ErrTiDBShuttingDown)
	} else if isCauseByDeadlineExceeded(err) {
		if s.replicaSelector != nil && s.replicaSelector.onReadReqConfigurableTimeout(req) {
			errLabel := "read-timeout-" + strconv.FormatUint(req.MaxExecutionDurationMs, 10) + "ms"
			metrics.TiKVRPCErrorCounter.WithLabelValues(errLabel, storeLabel).Inc()
			return nil
		}
	}
	if status.Code(errors.Cause(err)) == codes.Canceled {
		select {
		case <-bo.GetCtx().Done():
			metrics.TiKVRPCErrorCounter.WithLabelValues("grpc-canceled", storeLabel).Inc()
			return errors.WithStack(err)
		default:
			// If we don't cancel, but the error code is Canceled, it may be canceled by keepalive or gRPC remote.
			// For the case of canceled by keepalive, we need to re-establish the connection, otherwise following requests will always fail.
			// Canceled by gRPC remote may happen when tikv is killed and exiting.
			// Close the connection, backoff, and retry.
			logutil.Logger(bo.GetCtx()).Warn("receive a grpc cancel signal", zap.Error(err))
			var errConn *client.ErrConn
			if errors.As(err, &errConn) {
				if ext := s.getClientExt(); ext != nil {
					ext.CloseAddrVer(errConn.Addr, errConn.Ver)
				} else {
					s.client.CloseAddr(errConn.Addr)
				}
			}
		}
	}
	if errStr := getErrMsg(err); len(errStr) > 0 {
		metrics.TiKVRPCErrorCounter.WithLabelValues(getErrMsg(err), storeLabel).Inc()
	} else {
		metrics.TiKVRPCErrorCounter.WithLabelValues("unknown", storeLabel).Inc()
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

	if ctx.Store != nil && ctx.Store.storeType == tikvrpc.TiFlashCompute {
		s.regionCache.InvalidateTiFlashComputeStoresIfGRPCError(err)
	} else if ctx.Meta != nil {
		if s.replicaSelector != nil {
			s.replicaSelector.onSendFailure(bo, err)
		} else {
			s.regionCache.OnSendFail(bo, ctx, s.NeedReloadRegion(ctx), err)
		}
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
func regionErrorToLogging(e *errorpb.Error, errLabel string) string {
	str := errLabel
	if e.GetNotLeader() != nil {
		notLeader := e.GetNotLeader()
		if notLeader.GetLeader() != nil {
			str = fmt.Sprintf("%v_with_leader_%v", str, notLeader.GetLeader().GetId())
		} else {
			str = fmt.Sprintf("%v_with_no_leader", str)
		}
	}
	return str
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
	bo *retry.Backoffer, ctx *RPCContext, req *tikvrpc.Request, regionErr *errorpb.Error,
) (shouldRetry bool, err error) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikv.onRegionError", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	regionErrLabel := regionErrorToLabel(regionErr)
	metrics.TiKVRegionErrorCounter.WithLabelValues(regionErrLabel, storeIDLabel(ctx)).Inc()
	if s.Stats != nil {
		s.Stats.RecordRPCErrorStats(regionErrLabel)
		s.recordRPCAccessInfo(req, ctx, regionErrorToLogging(regionErr, regionErrLabel))
	}

	// NOTE: Please add the region error handler in the same order of errorpb.Error.
	if notLeader := regionErr.GetNotLeader(); notLeader != nil {
		// Retry if error is `NotLeader`.
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `NotLeader` retry later",
			zap.String("notLeader", notLeader.String()),
			zap.String("ctx", ctx.String()),
		)

		if s.replicaSelector != nil {
			return s.replicaSelector.onNotLeader(bo, ctx, notLeader)
		} else if notLeader.GetLeader() == nil {
			// The peer doesn't know who is the current leader. Generally it's because
			// the Raft group is in an election, but it's possible that the peer is
			// isolated and removed from the Raft group. So it's necessary to reload
			// the region from PD.
			s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, NoLeader)
			if err = bo.Backoff(
				retry.BoRegionScheduling,
				errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx),
			); err != nil {
				return false, err
			}
			return false, nil
		} else {
			// don't backoff if a new leader is returned.
			s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader(), ctx.AccessIdx)
			return true, nil
		}
	}

	// Retry it when tikv disk full happens.
	if diskFull := regionErr.GetDiskFull(); diskFull != nil {
		if err = bo.Backoff(
			retry.BoTiKVDiskFull,
			errors.Errorf("tikv disk full: %v ctx: %v", diskFull.String(), ctx.String()),
		); err != nil {
			return false, nil
		}
		return true, nil
	}

	if regionErr.GetRecoveryInProgress() != nil {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		logutil.Logger(bo.GetCtx()).Debug("tikv reports `RecoveryInProgress`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(retry.BoRegionRecoveryInProgress, errors.Errorf("region recovery in progress, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return false, nil
	}

	if regionErr.GetIsWitness() != nil {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		logutil.Logger(bo.GetCtx()).Debug("tikv reports `IsWitness`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(retry.BoIsWitness, errors.Errorf("is witness, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return false, nil
	}

	// Since we expect that the workload should be stopped during the flashback progress,
	// if a request meets the FlashbackInProgress error, it should stop retrying immediately
	// to avoid unnecessary backoff and potential unexpected data status to the user.
	if flashbackInProgress := regionErr.GetFlashbackInProgress(); flashbackInProgress != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `FlashbackInProgress`",
			zap.Stringer("req", req),
			zap.Stringer("ctx", ctx),
		)
		if req != nil && s.replicaSelector != nil && s.replicaSelector.onFlashbackInProgress(req) {
			return true, nil
		}
		return false, errors.Errorf(
			"region %d is in flashback progress, FlashbackStartTS is %d",
			flashbackInProgress.GetRegionId(), flashbackInProgress.GetFlashbackStartTs(),
		)
	}
	// This error means a second-phase flashback request is sent to a region that is not
	// prepared for the flashback before, it should stop retrying immediately to avoid
	// unnecessary backoff.
	if regionErr.GetFlashbackNotPrepared() != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `FlashbackNotPrepared`",
			zap.Stringer("req", req),
			zap.Stringer("ctx", ctx),
		)
		return false, errors.Errorf(
			"region %d is not prepared for the flashback",
			regionErr.GetFlashbackNotPrepared().GetRegionId(),
		)
	}

	// This peer is removed from the region. Invalidate the region since it's too stale.
	// if the region error is from follower, can we mark the peer unavailable and reload region asynchronously?
	if regionErr.GetRegionNotFound() != nil {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		return false, nil
	}

	if regionErr.GetKeyNotInRegion() != nil {
		logutil.Logger(bo.GetCtx()).Error("tikv reports `KeyNotInRegion`", zap.Stringer("req", req), zap.Stringer("ctx", ctx))
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		return false, nil
	}

	if epochNotMatch := regionErr.GetEpochNotMatch(); epochNotMatch != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `EpochNotMatch` retry later",
			zap.Stringer("EpochNotMatch", epochNotMatch),
			zap.Stringer("ctx", ctx),
		)
		retry, err := s.regionCache.OnRegionEpochNotMatch(bo, ctx, epochNotMatch.CurrentRegions)
		if !retry && s.replicaSelector != nil {
			s.replicaSelector.invalidateRegion()
		}
		return retry, err
	}

	if bucketVersionNotMatch := regionErr.GetBucketVersionNotMatch(); bucketVersionNotMatch != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `BucketVersionNotMatch` retry later",
			zap.Uint64("latest bucket version", bucketVersionNotMatch.GetVersion()),
			zap.Uint64("request bucket version", ctx.BucketVersion),
			zap.Stringer("ctx", ctx),
		)
		// bucket version is not match, we should split this cop request again.
		s.regionCache.OnBucketVersionNotMatch(ctx, bucketVersionNotMatch.Version, bucketVersionNotMatch.Keys)
		return false, nil
	}

	if serverIsBusy := regionErr.GetServerIsBusy(); serverIsBusy != nil {
		if s.replicaSelector != nil && strings.Contains(serverIsBusy.GetReason(), "deadline is exceeded") {
			if s.replicaSelector.onReadReqConfigurableTimeout(req) {
				return true, nil
			}
		}
		if s.replicaSelector != nil {
			return s.replicaSelector.onServerIsBusy(bo, ctx, req, serverIsBusy)
		}
		logutil.Logger(bo.GetCtx()).Warn(
			"tikv reports `ServerIsBusy` retry later",
			zap.String("reason", regionErr.GetServerIsBusy().GetReason()),
			zap.Stringer("ctx", ctx),
		)
		if ctx != nil && ctx.Store != nil && ctx.Store.storeType.IsTiFlashRelatedType() {
			err = bo.Backoff(retry.BoTiFlashServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		} else {
			err = bo.Backoff(retry.BoTiKVServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// StaleCommand error indicates the request is sent to the old leader and its term is changed.
	// We can't know whether the request is committed or not, so it's an undetermined error too,
	// but we don't handle it now.
	if regionErr.GetStaleCommand() != nil {
		logutil.Logger(bo.GetCtx()).Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", ctx))
		if s.replicaSelector != nil {
			// Needn't backoff because the new leader should be elected soon
			// and the replicaSelector will try the next peer.
		} else {
			err = bo.Backoff(retry.BoStaleCmd, errors.Errorf("stale command, ctx: %v", ctx))
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	if storeNotMatch := regionErr.GetStoreNotMatch(); storeNotMatch != nil {
		// store not match
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `StoreNotMatch` retry later",
			zap.Stringer("storeNotMatch", storeNotMatch),
			zap.Stringer("ctx", ctx),
		)
		s.regionCache.stores.markStoreNeedCheck(ctx.Store)
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		// It's possible the address of store is not changed but the DNS resolves to a different address in k8s environment,
		// so we always reconnect in this case.
		s.client.CloseAddr(ctx.Addr)
		return false, nil
	}

	if regionErr.GetRaftEntryTooLarge() != nil {
		logutil.Logger(bo.GetCtx()).Warn("tikv reports `RaftEntryTooLarge`", zap.Stringer("ctx", ctx))
		return false, errors.New(regionErr.String())
	}

	if regionErr.GetMaxTimestampNotSynced() != nil {
		logutil.Logger(bo.GetCtx()).Debug("tikv reports `MaxTimestampNotSynced`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(retry.BoMaxTsNotSynced, errors.Errorf("max timestamp not synced, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// A read request may be sent to a peer which has not been initialized yet, we should retry in this case.
	if regionErr.GetRegionNotInitialized() != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `RegionNotInitialized` retry later",
			zap.Uint64("store-id", ctx.Store.storeID),
			zap.Uint64("region-id", regionErr.GetRegionNotInitialized().GetRegionId()),
			zap.Stringer("ctx", ctx),
		)
		err = bo.Backoff(retry.BoMaxRegionNotInitialized, errors.Errorf("region not initialized"))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// The read-index can't be handled timely because the region is splitting or merging.
	if regionErr.GetReadIndexNotReady() != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `ReadIndexNotReady` retry later",
			zap.Uint64("store-id", ctx.Store.storeID),
			zap.Uint64("region-id", regionErr.GetRegionNotInitialized().GetRegionId()),
			zap.Stringer("ctx", ctx),
		)
		// The region can't provide service until split or merge finished, so backoff.
		err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("read index not ready, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	if regionErr.GetProposalInMergingMode() != nil {
		logutil.Logger(bo.GetCtx()).Debug("tikv reports `ProposalInMergingMode`", zap.Stringer("ctx", ctx))
		// The region is merging and it can't provide service until merge finished, so backoff.
		err = bo.Backoff(retry.BoRegionScheduling, errors.Errorf("region is merging, ctx: %v", ctx))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// A stale read request may be sent to a peer which the data is not ready yet, we should retry in this case.
	// This error is specific to stale read and the target replica is randomly selected. If the request is sent
	// to the leader, the data must be ready, so we don't backoff here.
	if regionErr.GetDataIsNotReady() != nil {
		logutil.Logger(bo.GetCtx()).Debug(
			"tikv reports `DataIsNotReady` retry later",
			zap.Uint64("store-id", ctx.Store.storeID),
			zap.Uint64("peer-id", regionErr.GetDataIsNotReady().GetPeerId()),
			zap.Uint64("region-id", regionErr.GetDataIsNotReady().GetRegionId()),
			zap.Uint64("safe-ts", regionErr.GetDataIsNotReady().GetSafeTs()),
			zap.Stringer("ctx", ctx),
		)
		if s.replicaSelector != nil {
			s.replicaSelector.onDataIsNotReady()
		}
		if !req.IsGlobalStaleRead() {
			// only backoff local stale reads as global should retry immediately against the leader as a normal read
			err = bo.Backoff(retry.BoMaxDataNotReady, errors.New("data is not ready"))
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	if isDeadlineExceeded(regionErr) && s.replicaSelector != nil && s.replicaSelector.onReadReqConfigurableTimeout(req) {
		return true, nil
	}

	if mismatch := regionErr.GetMismatchPeerId(); mismatch != nil {
		logutil.Logger(bo.GetCtx()).Warn(
			"tikv reports `MismatchPeerId`, invalidate region cache",
			zap.Uint64("req peer id", mismatch.GetRequestPeerId()),
			zap.Uint64("store peer id", mismatch.GetStorePeerId()),
		)
		if s.replicaSelector != nil {
			s.replicaSelector.invalidateRegion()
		}
		return false, nil
	}

	logutil.Logger(bo.GetCtx()).Debug(
		"tikv reports region failed",
		zap.Stringer("regionErr", regionErr),
		zap.Stringer("ctx", ctx),
	)

	if s.replicaSelector != nil {
		// Try the next replica.
		return true, nil
	}

	// When the request is sent to TiDB, there is no region in the request, so the region id will be 0.
	// So when region id is 0, there is no business with region cache.
	if ctx.Region.id != 0 {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	return false, nil
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

func patchRequestSource(req *tikvrpc.Request, replicaType string) {
	var sb strings.Builder
	defer func() {
		// TiKV does the limit control by the last part of the request source.
		sb.WriteByte('_')
		sb.WriteString(req.InputRequestSource)
		req.RequestSource = sb.String()
	}()

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

func recordAttemptedTime(s *replicaSelector, duration time.Duration) {
	if s.target != nil {
		s.target.attemptedTime += duration
	}
	if s.proxy != nil {
		s.proxy.attemptedTime += duration
	}
}

type backoffArgs struct {
	cfg *retry.Config
	err error
}

// addPendingBackoff adds pending backoff for the store.
func (s *baseReplicaSelector) addPendingBackoff(store *Store, cfg *retry.Config, err error) {
	storeId := uint64(0)
	if store != nil {
		storeId = store.storeID
	}
	if s.pendingBackoffs == nil {
		s.pendingBackoffs = make(map[uint64]*backoffArgs)
	}
	s.pendingBackoffs[storeId] = &backoffArgs{cfg, err}
}

// backoffOnRetry apply pending backoff on the store when retry in this store.
func (s *baseReplicaSelector) backoffOnRetry(store *Store, bo *retry.Backoffer) error {
	storeId := uint64(0)
	if store != nil {
		storeId = store.storeID
	}
	args, ok := s.pendingBackoffs[storeId]
	if !ok {
		return nil
	}
	delete(s.pendingBackoffs, storeId)
	return bo.Backoff(args.cfg, args.err)
}

// backoffOnNoCandidate apply the largest base pending backoff when no candidate.
func (s *baseReplicaSelector) backoffOnNoCandidate(bo *retry.Backoffer) error {
	var args *backoffArgs
	for _, pbo := range s.pendingBackoffs {
		if args == nil || args.cfg.Base() < pbo.cfg.Base() {
			args = pbo
		}
	}
	if args == nil {
		return nil
	}
	return bo.Backoff(args.cfg, args.err)
}
