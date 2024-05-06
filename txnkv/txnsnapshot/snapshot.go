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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/snapshot.go
//

// Copyright 2015 PingCAP, Inc.
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

package txnsnapshot

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/unionstore"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	// DefaultScanBatchSize is the default scan batch size.
	DefaultScanBatchSize = 256
	batchGetSize         = 5120
	maxTimestamp         = math.MaxUint64
)

// IsoLevel is the transaction's isolation level.
type IsoLevel kvrpcpb.IsolationLevel

const (
	// SI stands for 'snapshot isolation'.
	SI IsoLevel = IsoLevel(kvrpcpb.IsolationLevel_SI)
	// RC stands for 'read committed'.
	RC IsoLevel = IsoLevel(kvrpcpb.IsolationLevel_RC)
	// RCCheckTS stands for 'read consistency' with ts check.
	RCCheckTS IsoLevel = IsoLevel(kvrpcpb.IsolationLevel_RCCheckTS)
)

// ToPB converts isolation level to wire type.
func (l IsoLevel) ToPB() kvrpcpb.IsolationLevel {
	return kvrpcpb.IsolationLevel(l)
}

type kvstore interface {
	CheckVisibility(startTime uint64) error
	// GetRegionCache gets the RegionCache.
	GetRegionCache() *locate.RegionCache
	GetLockResolver() *txnlock.LockResolver
	GetTiKVClient() (client client.Client)

	// SendReq sends a request to TiKV.
	SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
}

// ReplicaReadAdjuster is a function that adjust the StoreSelectorOption and ReplicaReadType
// based on the keys count for BatchPointGet and PointGet
type ReplicaReadAdjuster func(int) (locate.StoreSelectorOption, kv.ReplicaReadType)

// KVSnapshot implements the tidbkv.Snapshot interface.
type KVSnapshot struct {
	store           kvstore
	version         uint64
	isolationLevel  IsoLevel
	priority        txnutil.Priority
	notFillCache    bool
	keyOnly         bool
	vars            *kv.Variables
	replicaReadSeed uint32
	resolvedLocks   util.TSSet
	committedLocks  util.TSSet
	scanBatchSize   int
	readTimeout     time.Duration

	// Cache the result of Get and BatchGet.
	// The invariance is that calling Get or BatchGet multiple times using the same start ts,
	// the result should not change.
	// NOTE: This representation here is different from the Get and BatchGet API.
	// cached use len(value)=0 to represent a key-value entry doesn't exist (a reliable truth from TiKV).
	// In the BatchGet API, it use no key-value entry to represent non-exist.
	// It's OK as long as there are no zero-byte values in the protocol.
	mu struct {
		sync.RWMutex
		hitCnt           int64
		cached           map[string][]byte
		cachedSize       int
		stats            *SnapshotRuntimeStats
		replicaRead      kv.ReplicaReadType
		taskID           uint64
		isStaleness      bool
		busyThreshold    time.Duration
		readReplicaScope string
		// replicaReadAdjuster check and adjust the replica read type and store match labels.
		replicaReadAdjuster ReplicaReadAdjuster
		// MatchStoreLabels indicates the labels the store should be matched
		matchStoreLabels []*metapb.StoreLabel
		// resourceGroupTag is use to set the kv request resource group tag.
		resourceGroupTag []byte
		// resourceGroupTagger is use to set the kv request resource group tag if resourceGroupTag is nil.
		resourceGroupTagger tikvrpc.ResourceGroupTagger
		// interceptor is used to decorate the RPC request logic related to the snapshot.
		interceptor interceptor.RPCInterceptor
		// resourceGroupName is used to bind the request to specified resource group.
		resourceGroupName string
	}
	sampleStep uint32
	*util.RequestSource
	isPipelined bool
}

// NewTiKVSnapshot creates a snapshot of an TiKV store.
func NewTiKVSnapshot(store kvstore, ts uint64, replicaReadSeed uint32) *KVSnapshot {
	// Sanity check for snapshot version.
	if ts >= math.MaxInt64 && ts != math.MaxUint64 {
		err := errors.Errorf("try to get snapshot with a large ts %d", ts)
		panic(err)
	}
	return &KVSnapshot{
		store:           store,
		version:         ts,
		scanBatchSize:   DefaultScanBatchSize,
		priority:        txnutil.PriorityNormal,
		vars:            kv.DefaultVars,
		replicaReadSeed: replicaReadSeed,
		RequestSource:   &util.RequestSource{},
	}
}

const batchGetMaxBackoff = 20000

// SetSnapshotTS resets the timestamp for reads.
func (s *KVSnapshot) SetSnapshotTS(ts uint64) {
	// Sanity check for snapshot version.
	if ts >= math.MaxInt64 && ts != math.MaxUint64 {
		err := errors.Errorf("try to get snapshot with a large ts %d", ts)
		panic(err)
	}
	// Invalidate cache if the snapshotTS change!
	s.version = ts
	s.mu.Lock()
	s.mu.cached = nil
	s.mu.Unlock()
	// And also remove the minCommitTS pushed information.
	s.resolvedLocks = util.TSSet{}
}

// IsInternal returns if the KvSnapshot is used by internal executions.
func (s *KVSnapshot) IsInternal() bool {
	return util.IsRequestSourceInternal(s.RequestSource)
}

// BatchGet gets all the keys' value from kv-server and returns a map contains key/value pairs.
// The map will not contain nonexistent keys.
// NOTE: Don't modify keys. Some codes rely on the order of keys.
func (s *KVSnapshot) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	return s.BatchGetWithTier(ctx, keys, BatchGetSnapshotTier)
}

// BatchGet tiers indicate the read tier of the batch get request.
// BatchGet read keys in regions. The keys location and region error retry mechanism are shared.
const (
	// BatchGetSnapshotTier indicates the batch get reads from a snapshot.
	BatchGetSnapshotTier = 1 << iota
	// BatchGetBufferTier indicates the batch get reads from the pipelined flushed buffer, only read locks in the current txn.
	// this only works when the txn is created with a pipelined memdb, unless an error will be returned.
	BatchGetBufferTier
)

// BatchGetWithTier gets all the keys' value from kv-server with given tier and returns a map contains key/value pairs.
func (s *KVSnapshot) BatchGetWithTier(ctx context.Context, keys [][]byte, readTier int) (map[string][]byte, error) {
	// Check the cached value first.
	m := make(map[string][]byte)
	s.mu.RLock()
	if s.mu.cached != nil && readTier == BatchGetSnapshotTier {
		tmp := make([][]byte, 0, len(keys))
		for _, key := range keys {
			if val, ok := s.mu.cached[string(key)]; ok {
				atomic.AddInt64(&s.mu.hitCnt, 1)
				if len(val) > 0 {
					m[string(key)] = val
				}
			} else {
				tmp = append(tmp, key)
			}
		}
		keys = tmp
	}
	s.mu.RUnlock()

	if len(keys) == 0 {
		return m, nil
	}

	ctx = context.WithValue(ctx, retry.TxnStartKey, s.version)
	if ctx.Value(util.RequestSourceKey) == nil {
		ctx = context.WithValue(ctx, util.RequestSourceKey, *s.RequestSource)
	}
	bo := retry.NewBackofferWithVars(ctx, batchGetMaxBackoff, s.vars)
	s.mu.RLock()
	if s.mu.interceptor != nil {
		// User has called snapshot.SetRPCInterceptor() to explicitly set an interceptor, we
		// need to bind it to ctx so that the internal client can perceive and execute
		// it before initiating an RPC request.
		bo.SetCtx(interceptor.WithRPCInterceptor(bo.GetCtx(), s.mu.interceptor))
	}
	s.mu.RUnlock()
	// Create a map to collect key-values from region servers.
	var mu sync.Mutex
	err := s.batchGetKeysByRegions(bo, keys, readTier, func(k, v []byte) {
		// when read buffer tier, empty value means a delete record, should also collect it.
		if len(v) == 0 && readTier != BatchGetBufferTier {
			return
		}

		mu.Lock()
		m[string(k)] = v
		mu.Unlock()
	})
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, err
	}

	err = s.store.CheckVisibility(s.version)
	if err != nil {
		return nil, err
	}

	if readTier != BatchGetSnapshotTier {
		return m, nil
	}

	// Update the cache.
	s.UpdateSnapshotCache(keys, m)

	return m, nil
}

type batchKeys struct {
	region locate.RegionVerID
	keys   [][]byte
}

func (b *batchKeys) relocate(bo *retry.Backoffer, c *locate.RegionCache) (bool, error) {
	loc, err := c.LocateKey(bo, b.keys[0])
	if err != nil {
		return false, err
	}
	// keys is not in order, so we have to iterate all keys.
	for i := 1; i < len(b.keys); i++ {
		if !loc.Contains(b.keys[i]) {
			return false, nil
		}
	}
	b.region = loc.Region
	return true, nil
}

// appendBatchKeysBySize appends keys to b. It may split the keys to make
// sure each batch's size does not exceed the limit.
func appendBatchKeysBySize(b []batchKeys, region locate.RegionVerID, keys [][]byte, sizeFn func([]byte) int, limit int) []batchKeys {
	var start, end int
	for start = 0; start < len(keys); start = end {
		var size int
		for end = start; end < len(keys) && size < limit; end++ {
			size += sizeFn(keys[end])
		}
		b = append(b, batchKeys{
			region: region,
			keys:   keys[start:end],
		})
	}
	return b
}

func (s *KVSnapshot) batchGetKeysByRegions(bo *retry.Backoffer, keys [][]byte, readTier int, collectF func(k, v []byte)) error {
	defer func(start time.Time) {
		if s.IsInternal() {
			metrics.TxnCmdHistogramWithBatchGetInternal.Observe(time.Since(start).Seconds())
		} else {
			metrics.TxnCmdHistogramWithBatchGetGeneral.Observe(time.Since(start).Seconds())
		}
	}(time.Now())
	groups, _, err := s.store.GetRegionCache().GroupKeysByRegion(bo, keys, nil)
	if err != nil {
		return err
	}

	if s.IsInternal() {
		metrics.TxnRegionsNumHistogramWithSnapshotInternal.Observe(float64(len(groups)))
	} else {
		metrics.TxnRegionsNumHistogramWithSnapshot.Observe(float64(len(groups)))
	}

	var batches []batchKeys
	for id, g := range groups {
		batches = appendBatchKeysBySize(batches, id, g, func([]byte) int { return 1 }, batchGetSize)
	}

	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return s.batchGetSingleRegion(bo, batches[0], readTier, collectF)
	}
	ch := make(chan error)
	for _, batch1 := range batches {
		batch := batch1
		go func() {
			backoffer, cancel := bo.Fork()
			defer cancel()
			ch <- s.batchGetSingleRegion(backoffer, batch, readTier, collectF)
		}()
	}
	for i := 0; i < len(batches); i++ {
		if e := <-ch; e != nil {
			logutil.BgLogger().Debug("snapshot BatchGetWithTier failed",
				zap.Error(e),
				zap.Uint64("txnStartTS", s.version))
			err = errors.WithStack(e)
		}
	}
	return err
}

func (s *KVSnapshot) buildBatchGetRequest(keys [][]byte, busyThresholdMs int64, readTier int) (*tikvrpc.Request, error) {
	ctx := kvrpcpb.Context{
		Priority:         s.priority.ToPB(),
		NotFillCache:     s.notFillCache,
		TaskId:           s.mu.taskID,
		ResourceGroupTag: s.mu.resourceGroupTag,
		IsolationLevel:   s.isolationLevel.ToPB(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: s.mu.resourceGroupName,
		},
		BusyThresholdMs: uint32(busyThresholdMs),
	}
	switch readTier {
	case BatchGetSnapshotTier:
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdBatchGet, &kvrpcpb.BatchGetRequest{
			Keys:    keys,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, ctx)
		return req, nil
	case BatchGetBufferTier:
		if !s.isPipelined {
			return nil, errors.New("only snapshot with pipelined dml can read from buffer")
		}
		req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdBufferBatchGet, &kvrpcpb.BufferBatchGetRequest{
			Keys:    keys,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, ctx)
		return req, nil
	default:
		return nil, errors.Errorf("unknown read tier %d", readTier)
	}
}

func (s *KVSnapshot) batchGetSingleRegion(bo *retry.Backoffer, batch batchKeys, readTier int, collectF func(k, v []byte)) error {
	cli := NewClientHelper(s.store, &s.resolvedLocks, &s.committedLocks, false)
	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = locate.NewRegionRequestRuntimeStats()
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}
	isStaleness := s.mu.isStaleness
	busyThresholdMs := s.mu.busyThreshold.Milliseconds()
	s.mu.RUnlock()

	pending := batch.keys
	var resolvingRecordToken *int
	useConfigurableKVTimeout := true
	// the states in request need to keep when retry request.
	var readType string
	for {
		s.mu.RLock()
		req, err := s.buildBatchGetRequest(pending, busyThresholdMs, readTier)
		if err != nil {
			return err
		}
		req.InputRequestSource = s.GetRequestSource()
		if readType != "" {
			req.ReadType = readType
			req.IsRetryRequest = true
		}
		if s.mu.resourceGroupTag == nil && s.mu.resourceGroupTagger != nil {
			s.mu.resourceGroupTagger(req)
		}
		scope := s.mu.readReplicaScope
		matchStoreLabels := s.mu.matchStoreLabels
		replicaAdjuster := s.mu.replicaReadAdjuster
		s.mu.RUnlock()
		req.TxnScope = scope
		req.ReadReplicaScope = scope
		if isStaleness {
			req.EnableStaleWithMixedReplicaRead()
		}
		timeout := client.ReadTimeoutMedium
		if useConfigurableKVTimeout && s.readTimeout > 0 {
			useConfigurableKVTimeout = false
			timeout = s.readTimeout
		}
		req.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
		ops := make([]locate.StoreSelectorOption, 0, 2)
		if len(matchStoreLabels) > 0 {
			ops = append(ops, locate.WithMatchLabels(matchStoreLabels))
		}
		if req.ReplicaReadType.IsFollowerRead() && replicaAdjuster != nil {
			op, readType := replicaAdjuster(len(pending))
			if op != nil {
				ops = append(ops, op)
			}
			req.ReplicaReadType = readType
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, batch.region, timeout, tikvrpc.TiKV, "", ops...)
		if err != nil {
			return err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return err
		}
		readType = req.ReadType
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return err
				}
			}
			same, err := batch.relocate(bo, cli.regionCache)
			if err != nil {
				return err
			}
			if same {
				continue
			}
			return s.batchGetKeysByRegions(bo, pending, readTier, collectF)
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		var (
			lockedKeys [][]byte
			locks      []*txnlock.Lock

			keyErr  *kvrpcpb.KeyError
			pairs   []*kvrpcpb.KvPair
			details *kvrpcpb.ExecDetailsV2
		)
		switch v := resp.Resp.(type) {
		case *kvrpcpb.BatchGetResponse:
			keyErr = v.GetError()
			pairs = v.Pairs
			details = v.GetExecDetailsV2()
		case *kvrpcpb.BufferBatchGetResponse:
			keyErr = v.GetError()
			pairs = v.Pairs
			details = v.GetExecDetailsV2()
		default:
			return errors.Errorf("unknown response %T", v)
		}
		if keyErr != nil {
			// If a response-level error happens, skip reading pairs.
			lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				return err
			}
			lockedKeys = append(lockedKeys, lock.Key)
			locks = append(locks, lock)
		} else {
			for _, pair := range pairs {
				keyErr := pair.GetError()
				if keyErr == nil {
					collectF(pair.GetKey(), pair.GetValue())
					continue
				}
				lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
				if err != nil {
					return err
				}
				lockedKeys = append(lockedKeys, lock.Key)
				locks = append(locks, lock)
			}
		}
		if details != nil {
			readKeys := len(pairs)
			var readTime float64
			if timeDetail := details.GetTimeDetailV2(); timeDetail != nil {
				readTime = float64(timeDetail.GetKvReadWallTimeNs()) / 1000000000.
			} else if timeDetail := details.GetTimeDetail(); timeDetail != nil {
				readTime = float64(timeDetail.GetKvReadWallTimeMs()) / 1000.
			}
			readSize := float64(details.GetScanDetailV2().GetProcessedVersionsSize())
			metrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
			s.mergeExecDetail(details)
		}
		if len(lockedKeys) > 0 {
			if resolvingRecordToken == nil {
				token := cli.RecordResolvingLocks(locks, s.version)
				resolvingRecordToken = &token
				defer cli.ResolveLocksDone(s.version, *resolvingRecordToken)
			} else {
				cli.UpdateResolvingLocks(locks, s.version, *resolvingRecordToken)
			}
			// we need to read from leader after resolving the lock.
			if isStaleness {
				isStaleness = false
				busyThresholdMs = 0
			}
			resolveLocksOpts := txnlock.ResolveLocksOptions{
				CallerStartTS: s.version,
				Locks:         locks,
				Detail:        s.getResolveLockDetail(),
			}
			resolveLocksRes, err := cli.ResolveLocksWithOpts(bo, resolveLocksOpts)
			msBeforeExpired := resolveLocksRes.TTL
			if err != nil {
				return err
			}
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.Errorf("BatchGetWithTier lockedKeys: %d", len(lockedKeys)))
				if err != nil {
					return err
				}
			}
			// Only reduce pending keys when there is no response-level error. Otherwise,
			// lockedKeys may be incomplete.
			if keyErr == nil {
				pending = lockedKeys
			}
			continue
		}
		return nil
	}
}

const getMaxBackoff = 20000

// Get gets the value for key k from snapshot.
func (s *KVSnapshot) Get(ctx context.Context, k []byte) ([]byte, error) {
	defer func(start time.Time) {
		if s.IsInternal() {
			metrics.TxnCmdHistogramWithGetInternal.Observe(time.Since(start).Seconds())
		} else {
			metrics.TxnCmdHistogramWithGetGeneral.Observe(time.Since(start).Seconds())
		}
	}(time.Now())

	s.mu.RLock()
	// Check the cached values first.
	if s.mu.cached != nil {
		if value, ok := s.mu.cached[string(k)]; ok {
			atomic.AddInt64(&s.mu.hitCnt, 1)
			s.mu.RUnlock()
			if len(value) == 0 {
				return nil, tikverr.ErrNotExist
			}
			return value, nil
		}
	}
	if _, err := util.EvalFailpoint("snapshot-get-cache-fail"); err == nil {
		if ctx.Value("TestSnapshotCache") != nil {
			s.mu.RUnlock()
			panic("cache miss")
		}
	}
	ctx = context.WithValue(ctx, retry.TxnStartKey, s.version)
	if ctx.Value(util.RequestSourceKey) == nil {
		ctx = context.WithValue(ctx, util.RequestSourceKey, *s.RequestSource)
	}
	bo := retry.NewBackofferWithVars(ctx, getMaxBackoff, s.vars)
	if s.mu.interceptor != nil {
		// User has called snapshot.SetRPCInterceptor() to explicitly set an interceptor, we
		// need to bind it to ctx so that the internal client can perceive and execute
		// it before initiating an RPC request.
		bo.SetCtx(interceptor.WithRPCInterceptor(bo.GetCtx(), s.mu.interceptor))
	}
	s.mu.RUnlock()
	val, err := s.get(ctx, bo, k)
	s.recordBackoffInfo(bo)
	if err != nil {
		return nil, err
	}
	err = s.store.CheckVisibility(s.version)
	if err != nil {
		return nil, err
	}
	// Update the cache.
	s.UpdateSnapshotCache([][]byte{k}, map[string][]byte{string(k): val})
	if len(val) == 0 {
		return nil, tikverr.ErrNotExist
	}
	return val, nil
}

func (s *KVSnapshot) get(ctx context.Context, bo *retry.Backoffer, k []byte) ([]byte, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvSnapshot.get", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		opentracing.ContextWithSpan(ctx, span1)
	}

	cli := NewClientHelper(s.store, &s.resolvedLocks, &s.committedLocks, true)
	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = locate.NewRegionRequestRuntimeStats()
		defer func() {
			s.mergeRegionRequestStats(cli.Stats)
		}()
	}
	req := tikvrpc.NewReplicaReadRequest(tikvrpc.CmdGet,
		&kvrpcpb.GetRequest{
			Key:     k,
			Version: s.version,
		}, s.mu.replicaRead, &s.replicaReadSeed, kvrpcpb.Context{
			Priority:         s.priority.ToPB(),
			NotFillCache:     s.notFillCache,
			TaskId:           s.mu.taskID,
			ResourceGroupTag: s.mu.resourceGroupTag,
			IsolationLevel:   s.isolationLevel.ToPB(),
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: s.mu.resourceGroupName,
			},
			BusyThresholdMs: uint32(s.mu.busyThreshold.Milliseconds()),
		})
	req.InputRequestSource = s.GetRequestSource()
	if s.mu.resourceGroupTag == nil && s.mu.resourceGroupTagger != nil {
		s.mu.resourceGroupTagger(req)
	}
	isStaleness := s.mu.isStaleness
	matchStoreLabels := s.mu.matchStoreLabels
	scope := s.mu.readReplicaScope
	replicaAdjuster := s.mu.replicaReadAdjuster
	s.mu.RUnlock()
	req.TxnScope = scope
	req.ReadReplicaScope = scope
	var ops []locate.StoreSelectorOption
	if isStaleness {
		req.EnableStaleWithMixedReplicaRead()
	}
	if len(matchStoreLabels) > 0 {
		ops = append(ops, locate.WithMatchLabels(matchStoreLabels))
	}
	if req.ReplicaReadType.IsFollowerRead() && replicaAdjuster != nil {
		op, readType := replicaAdjuster(1)
		if op != nil {
			ops = append(ops, op)
		}
		req.ReplicaReadType = readType
	}

	var firstLock *txnlock.Lock
	var resolvingRecordToken *int
	useConfigurableKVTimeout := true
	for {
		util.EvalFailpoint("beforeSendPointGet")
		loc, err := s.store.GetRegionCache().LocateKey(bo, k)
		if err != nil {
			return nil, err
		}
		timeout := client.ReadTimeoutShort
		if useConfigurableKVTimeout && s.readTimeout > 0 {
			useConfigurableKVTimeout = false
			timeout = s.readTimeout
		}
		req.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
		resp, _, _, err := cli.SendReqCtx(bo, req, loc.Region, timeout, tikvrpc.TiKV, "", ops...)
		if err != nil {
			return nil, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, err
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		if resp.Resp == nil {
			return nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdGetResp := resp.Resp.(*kvrpcpb.GetResponse)
		if cmdGetResp.ExecDetailsV2 != nil {
			readKeys := len(cmdGetResp.Value)
			var readTime float64
			if timeDetail := cmdGetResp.ExecDetailsV2.GetTimeDetailV2(); timeDetail != nil {
				readTime = float64(timeDetail.GetKvReadWallTimeNs()) / 1000000000.
			} else if timeDetail := cmdGetResp.ExecDetailsV2.GetTimeDetail(); timeDetail != nil {
				readTime = float64(timeDetail.GetKvReadWallTimeMs()) / 1000.
			}
			readSize := float64(cmdGetResp.ExecDetailsV2.GetScanDetailV2().GetProcessedVersionsSize())
			metrics.ObserveReadSLI(uint64(readKeys), readTime, readSize)
			s.mergeExecDetail(cmdGetResp.ExecDetailsV2)
		}
		val := cmdGetResp.GetValue()
		if keyErr := cmdGetResp.GetError(); keyErr != nil {
			lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				return nil, err
			}
			if firstLock == nil {
				// we need to read from leader after resolving the lock.
				if isStaleness {
					req.DisableStaleReadMeetLock()
					req.BusyThresholdMs = 0
				}
				firstLock = lock
			} else if s.version == maxTimestamp && firstLock.TxnID != lock.TxnID {
				// If it is an autocommit point get, it needs to be blocked only
				// by the first lock it meets. During retries, if the encountered
				// lock is different from the first one, we can omit it.
				cli.resolvedLocks.Put(lock.TxnID)
				continue
			}
			locks := []*txnlock.Lock{lock}
			if resolvingRecordToken == nil {
				token := cli.RecordResolvingLocks(locks, s.version)
				resolvingRecordToken = &token
				defer cli.ResolveLocksDone(s.version, *resolvingRecordToken)
			} else {
				cli.UpdateResolvingLocks(locks, s.version, *resolvingRecordToken)
			}
			resolveLocksOpts := txnlock.ResolveLocksOptions{
				CallerStartTS: s.version,
				Locks:         locks,
				Detail:        s.getResolveLockDetail(),
			}
			resolveLocksRes, err := cli.ResolveLocksWithOpts(bo, resolveLocksOpts)
			if err != nil {
				return nil, err
			}
			msBeforeExpired := resolveLocksRes.TTL
			if msBeforeExpired > 0 {
				err = bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.New(keyErr.String()))
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		return val, nil
	}
}

func (s *KVSnapshot) mergeExecDetail(detail *kvrpcpb.ExecDetailsV2) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if detail == nil || s.mu.stats == nil {
		return
	}
	if s.mu.stats.resolveLockDetail == nil {
		s.mu.stats.resolveLockDetail = &util.ResolveLockDetail{}
	}
	if s.mu.stats.scanDetail == nil {
		s.mu.stats.scanDetail = &util.ScanDetail{
			ResolveLock: s.mu.stats.resolveLockDetail,
		}
	}
	if s.mu.stats.timeDetail == nil {
		s.mu.stats.timeDetail = &util.TimeDetail{}
	}
	s.mu.stats.scanDetail.MergeFromScanDetailV2(detail.ScanDetailV2)
	s.mu.stats.timeDetail.MergeFromTimeDetail(detail.TimeDetailV2, detail.TimeDetail)
}

// Iter return a list of key-value pair after `k`.
func (s *KVSnapshot) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error) {
	scanner, err := newScanner(s, k, upperBound, s.scanBatchSize, false)
	return scanner, err
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (s *KVSnapshot) IterReverse(k, lowerBound []byte) (unionstore.Iterator, error) {
	scanner, err := newScanner(s, lowerBound, k, s.scanBatchSize, true)
	return scanner, err
}

// SetNotFillCache indicates whether tikv should skip filling cache when
// loading data.
func (s *KVSnapshot) SetNotFillCache(b bool) {
	s.notFillCache = b
}

// SetKeyOnly indicates if tikv can return only keys.
func (s *KVSnapshot) SetKeyOnly(b bool) {
	s.keyOnly = b
}

// SetScanBatchSize sets the scan batchSize used to scan data from tikv.
func (s *KVSnapshot) SetScanBatchSize(batchSize int) {
	s.scanBatchSize = batchSize
}

// SetReplicaRead sets up the replica read type.
func (s *KVSnapshot) SetReplicaRead(readType kv.ReplicaReadType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.replicaRead = readType
}

// SetIsolationLevel sets the isolation level used to scan data from tikv.
func (s *KVSnapshot) SetIsolationLevel(level IsoLevel) {
	s.isolationLevel = level
}

// SetSampleStep skips 'step - 1' number of keys after each returned key.
func (s *KVSnapshot) SetSampleStep(step uint32) {
	s.sampleStep = step
}

// SetPriority sets the priority for tikv to execute commands.
func (s *KVSnapshot) SetPriority(pri txnutil.Priority) {
	s.priority = pri
}

// SetTaskID marks current task's unique ID to allow TiKV to schedule
// tasks more fairly.
func (s *KVSnapshot) SetTaskID(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.taskID = id
}

// SetRuntimeStats sets the stats to collect runtime statistics.
// Set it to nil to clear stored stats.
func (s *KVSnapshot) SetRuntimeStats(stats *SnapshotRuntimeStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.stats = stats
}

// SetTxnScope is same as SetReadReplicaScope, keep it in order to keep compatible for now.
func (s *KVSnapshot) SetTxnScope(scope string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.readReplicaScope = scope
}

// SetReadReplicaScope set read replica scope
func (s *KVSnapshot) SetReadReplicaScope(scope string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.readReplicaScope = scope
}

// SetReplicaReadAdjuster set replica read adjust function
func (s *KVSnapshot) SetReplicaReadAdjuster(f ReplicaReadAdjuster) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.replicaReadAdjuster = f
}

// SetLoadBasedReplicaReadThreshold sets the TiKV wait duration threshold of
// enabling replica read automatically
func (s *KVSnapshot) SetLoadBasedReplicaReadThreshold(busyThreshold time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if busyThreshold <= 0 || busyThreshold.Milliseconds() > math.MaxUint32 {
		s.mu.busyThreshold = 0
	} else {
		s.mu.busyThreshold = busyThreshold
	}
}

// SetIsStalenessReadOnly indicates whether the transaction is staleness read only transaction
func (s *KVSnapshot) SetIsStalenessReadOnly(b bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.isStaleness = b
}

// SetMatchStoreLabels sets up labels to filter target stores.
func (s *KVSnapshot) SetMatchStoreLabels(labels []*metapb.StoreLabel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.matchStoreLabels = labels
}

// SetResourceGroupTag sets resource group tag of the kv request.
func (s *KVSnapshot) SetResourceGroupTag(tag []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.resourceGroupTag = tag
}

// SetResourceGroupTagger sets resource group tagger of the kv request.
// Before sending the request, if resourceGroupTag is not nil, use
// resourceGroupTag directly, otherwise use resourceGroupTagger.
func (s *KVSnapshot) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.resourceGroupTagger = tagger
}

// SetRPCInterceptor sets interceptor.RPCInterceptor for the snapshot.
// interceptor.RPCInterceptor will be executed before each RPC request is initiated.
// Note that SetRPCInterceptor will replace the previously set interceptor.
func (s *KVSnapshot) SetRPCInterceptor(it interceptor.RPCInterceptor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.interceptor = it
}

// AddRPCInterceptor adds an interceptor, the order of addition is the order of execution.
// the chained interceptors will be dedupcated by its name.
func (s *KVSnapshot) AddRPCInterceptor(it interceptor.RPCInterceptor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.interceptor == nil {
		s.mu.interceptor = it
		return
	}
	s.mu.interceptor = interceptor.ChainRPCInterceptors(s.mu.interceptor, it)
}

// SetResourceGroupName set resource group name of the kv request.
func (s *KVSnapshot) SetResourceGroupName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.resourceGroupName = name
}

// SnapCacheHitCount gets the snapshot cache hit count. Only for test.
func (s *KVSnapshot) SnapCacheHitCount() int {
	return int(atomic.LoadInt64(&s.mu.hitCnt))
}

// SnapCacheSize gets the snapshot cache size. Only for test.
func (s *KVSnapshot) SnapCacheSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mu.cached)
}

// SnapCache gets the copy of snapshot cache. Only for test.
func (s *KVSnapshot) SnapCache() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string][]byte, len(s.mu.cached))
	for k, v := range s.mu.cached {
		cp[k] = v
	}
	return cp
}

// UpdateSnapshotCache sets the values of cache, for further fast read with same keys.
func (s *KVSnapshot) UpdateSnapshotCache(keys [][]byte, m map[string][]byte) {
	// s.version == math.MaxUint64 is used in special transaction, which always read the latest data.
	// do not cache it to avoid anomaly.
	if s.version == math.MaxUint64 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.cached == nil {
		s.mu.cached = make(map[string][]byte, min(len(keys), 8))
	}
	for _, key := range keys {
		val := m[string(key)]
		s.mu.cachedSize += len(key) + len(val)
		s.mu.cachedSize -= len(s.mu.cached[string(key)])
		s.mu.cached[string(key)] = val
	}

	const cachedSizeLimit = 10 << 30
	if s.mu.cachedSize >= cachedSizeLimit {
		for k, v := range s.mu.cached {
			if _, needed := m[k]; needed {
				continue
			}
			delete(s.mu.cached, k)
			s.mu.cachedSize -= len(k) + len(v)
			if s.mu.cachedSize < cachedSizeLimit {
				break
			}
		}
	}
}

// CleanCache cleans the cache for given keys. Only for test.
func (s *KVSnapshot) CleanCache(keys [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		s.mu.cachedSize -= len(key)
		s.mu.cachedSize -= len(s.mu.cached[string(key)])
		delete(s.mu.cached, string(key))
	}
}

// SetVars sets variables to the transaction.
func (s *KVSnapshot) SetVars(vars *kv.Variables) {
	s.vars = vars
}

func (s *KVSnapshot) recordBackoffInfo(bo *retry.Backoffer) {
	s.mu.RLock()
	if s.mu.stats == nil || bo.GetTotalSleep() == 0 {
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.stats == nil {
		return
	}
	if s.mu.stats.backoffSleepMS == nil {
		s.mu.stats.backoffSleepMS = bo.GetBackoffSleepMS()
		s.mu.stats.backoffTimes = bo.GetBackoffTimes()
		return
	}
	for k, v := range bo.GetBackoffSleepMS() {
		s.mu.stats.backoffSleepMS[k] += v
	}
	for k, v := range bo.GetBackoffTimes() {
		s.mu.stats.backoffTimes[k] += v
	}
}

func (s *KVSnapshot) mergeRegionRequestStats(rpcStats *locate.RegionRequestRuntimeStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.stats == nil {
		return
	}
	if s.mu.stats.rpcStats == nil {
		s.mu.stats.rpcStats = rpcStats
		return
	}
	s.mu.stats.rpcStats.Merge(rpcStats)
}

// SetKVReadTimeout sets timeout for individual KV read operations under this snapshot
func (s *KVSnapshot) SetKVReadTimeout(readTimeout time.Duration) {
	s.readTimeout = readTimeout
}

// GetKVReadTimeout returns timeout for individual KV read operations under this snapshot or 0 if timeout is not set
func (s *KVSnapshot) GetKVReadTimeout() time.Duration {
	return s.readTimeout
}

func (s *KVSnapshot) getResolveLockDetail() *util.ResolveLockDetail {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.stats == nil {
		return nil
	}
	return s.mu.stats.resolveLockDetail
}

// SetPipelined sets the snapshot to pipelined mode.
func (s *KVSnapshot) SetPipelined(ts uint64) {
	s.isPipelined = true
	// In pipelined mode, some locks are flushed into stores during the execution.
	// If a read request encounters these pipelined locks, it'll be a situation where lock.ts == start_ts.
	// In order to allow the snapshot to proceed normally, we need to skip these locks.
	// Otherwise, the transaction will attempt to resolve its own lock, leading to a mutual wait with the primary key TTL.
	// Currently, we skip these locks by resolvedLocks mechanism.
	s.resolvedLocks.Put(ts)
}

// SnapshotRuntimeStats records the runtime stats of snapshot.
type SnapshotRuntimeStats struct {
	rpcStats          *locate.RegionRequestRuntimeStats
	backoffSleepMS    map[string]int
	backoffTimes      map[string]int
	scanDetail        *util.ScanDetail
	timeDetail        *util.TimeDetail
	resolveLockDetail *util.ResolveLockDetail
}

// Clone implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Clone() *SnapshotRuntimeStats {
	newRs := SnapshotRuntimeStats{}
	if rs.rpcStats != nil {
		newRs.rpcStats = rs.rpcStats.Clone()
	}
	if len(rs.backoffSleepMS) > 0 {
		newRs.backoffSleepMS = make(map[string]int)
		newRs.backoffTimes = make(map[string]int)
		for k, v := range rs.backoffSleepMS {
			newRs.backoffSleepMS[k] += v
		}
		for k, v := range rs.backoffTimes {
			newRs.backoffTimes[k] += v
		}
	}

	if rs.scanDetail != nil {
		newRs.scanDetail = rs.scanDetail
	}

	if rs.timeDetail != nil {
		newRs.timeDetail = rs.timeDetail
	}

	if rs.resolveLockDetail != nil {
		newRs.resolveLockDetail = rs.resolveLockDetail
	}

	return &newRs
}

// Merge implements the RuntimeStats interface.
func (rs *SnapshotRuntimeStats) Merge(other *SnapshotRuntimeStats) {
	if other.rpcStats != nil {
		if rs.rpcStats == nil {
			rs.rpcStats = locate.NewRegionRequestRuntimeStats()
		}
		rs.rpcStats.Merge(other.rpcStats)
	}
	if len(other.backoffSleepMS) > 0 {
		if rs.backoffSleepMS == nil {
			rs.backoffSleepMS = make(map[string]int)
		}
		if rs.backoffTimes == nil {
			rs.backoffTimes = make(map[string]int)
		}
		for k, v := range other.backoffSleepMS {
			rs.backoffSleepMS[k] += v
		}
		for k, v := range other.backoffTimes {
			rs.backoffTimes[k] += v
		}
	}
}

// String implements fmt.Stringer interface.
func (rs *SnapshotRuntimeStats) String() string {
	var buf bytes.Buffer
	if rs.rpcStats != nil {
		buf.WriteString(rs.rpcStats.String())
	}
	for k, v := range rs.backoffTimes {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		ms := rs.backoffSleepMS[k]
		d := time.Duration(ms) * time.Millisecond
		buf.WriteString(fmt.Sprintf("%s_backoff:{num:%d, total_time:%s}", k, v, util.FormatDuration(d)))
	}
	timeDetail := rs.timeDetail.String()
	if timeDetail != "" {
		buf.WriteString(", ")
		buf.WriteString(timeDetail)
	}
	scanDetail := rs.scanDetail.String()
	if scanDetail != "" {
		buf.WriteString(", ")
		buf.WriteString(scanDetail)
	}
	return buf.String()
}

// GetCmdRPCCount returns the count of the corresponding kind of rpc requests
func (rs *SnapshotRuntimeStats) GetCmdRPCCount(cmd tikvrpc.CmdType) int64 {
	if rs.rpcStats == nil || len(rs.rpcStats.RPCStats) == 0 {
		return 0
	}

	stats, ok := rs.rpcStats.RPCStats[cmd]
	if !ok {
		return 0
	}

	return stats.Count
}
