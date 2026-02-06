// Copyright 2025 TiKV Authors
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
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
)

type poolWrapper struct {
	pool interface{ Go(func()) error }
}

func (p *poolWrapper) Go(f func()) {
	err := p.pool.Go(f)
	if err != nil {
		// fallback to native go
		go f()
	}
}

func (s *KVSnapshot) asyncBatchGetByRegions(
	bo *retry.Backoffer,
	batches []batchKeys,
	readTier int,
	opt kv.BatchGetOptions,
	collectF func(k []byte, v kv.ValueEntry),
) (err error) {
	var (
		runloop   = async.NewRunLoop()
		completed = 0
	)
	runloop.Pool = &poolWrapper{pool: s.store}
	forkedBo, cancel := bo.Fork()
	defer cancel()
	for i, batch1 := range batches {
		var backoffer *retry.Backoffer
		if i == len(batches)-1 {
			backoffer = forkedBo
		} else {
			backoffer = forkedBo.Clone()
		}
		batch := batch1
		s.tryBatchGetSingleRegionUsingAsyncAPI(backoffer, batch, readTier, opt, collectF, async.NewCallback(runloop, func(_ struct{}, e error) {
			// The callback is designed to be executed in the runloop's goroutine thus it should be safe to update the
			// following variables without locks.
			completed++
			if e != nil {
				logutil.BgLogger().Debug("snapshot BatchGetWithTier failed",
					zap.Error(e),
					zap.Uint64("txnStartTS", s.version))
				err = errors.WithStack(e)
			}
		}))
	}
	for completed < len(batches) {
		if _, e := runloop.Exec(bo.GetCtx()); e != nil {
			err = errors.WithStack(e)
			break
		}
	}
	return err
}

func (s *KVSnapshot) tryBatchGetSingleRegionUsingAsyncAPI(
	bo *retry.Backoffer,
	batch batchKeys,
	readTier int,
	opt kv.BatchGetOptions,
	collectF func(k []byte, v kv.ValueEntry),
	cb async.Callback[struct{}],
) {
	cli := NewClientHelper(s.store, &s.resolvedLocks, &s.committedLocks, false)
	s.mu.RLock()
	if s.mu.stats != nil {
		cli.Stats = locate.NewRegionRequestRuntimeStats()
		cb.Inject(func(res struct{}, err error) (struct{}, error) {
			s.mergeRegionRequestStats(cli.Stats)
			return res, err
		})
	}
	req, err := s.buildBatchGetRequest(batch.keys, s.mu.busyThreshold.Milliseconds(), readTier, opt)
	if err != nil {
		s.mu.RUnlock()
		cb.Invoke(struct{}{}, err)
		return
	}
	req.InputRequestSource = s.GetRequestSource()
	if s.mu.resourceGroupTag == nil && s.mu.resourceGroupTagger != nil {
		s.mu.resourceGroupTagger(req)
	}
	isStaleness := s.mu.isStaleness
	readReplicaScope := s.mu.readReplicaScope
	matchStoreLabels := s.mu.matchStoreLabels
	replicaReadAdjuster := s.mu.replicaReadAdjuster
	s.mu.RUnlock()

	req.TxnScope = readReplicaScope
	req.ReadReplicaScope = readReplicaScope
	if isStaleness {
		req.EnableStaleWithMixedReplicaRead()
	}

	timeout := client.ReadTimeoutMedium
	if s.readTimeout > 0 {
		timeout = s.readTimeout
	}
	req.MaxExecutionDurationMs = uint64(timeout.Milliseconds())

	ops := make([]locate.StoreSelectorOption, 0, 2)
	if len(matchStoreLabels) > 0 {
		ops = append(ops, locate.WithMatchLabels(matchStoreLabels))
	}
	if req.ReplicaReadType.IsFollowerRead() && replicaReadAdjuster != nil {
		op, readType := replicaReadAdjuster(len(batch.keys))
		if op != nil {
			ops = append(ops, op)
		}
		req.ReplicaReadType = readType
	}

	onResp := func(resp *tikvrpc.ResponseExt, err error) {
		if err != nil {
			cb.Invoke(struct{}{}, err)
			metrics.AsyncBatchGetCounterWithOtherError.Inc()
			return
		}

		regionErr, err := resp.GetRegionError()
		if err != nil {
			cb.Invoke(struct{}{}, err)
			metrics.AsyncBatchGetCounterWithOtherError.Inc()
			return
		}
		if regionErr != nil {
			cb.Executor().Go(func() {
				growStackForBatchGetWorker()
				err := s.retryBatchGetSingleRegionAfterAsyncAPI(bo, cli, batch, readTier, req.ReadType, regionErr, nil, opt, collectF)
				cb.Schedule(struct{}{}, err)
			})
			metrics.AsyncBatchGetCounterWithRegionError.Inc()
			return
		}

		lockInfo, err := collectBatchGetResponseData(&resp.Response, collectF, s.mergeExecDetail)
		if err != nil {
			cb.Invoke(struct{}{}, err)
			metrics.AsyncBatchGetCounterWithOtherError.Inc()
			return
		}
		if len(lockInfo.lockedKeys) > 0 {
			cb.Executor().Go(func() {
				growStackForBatchGetWorker()
				err := s.retryBatchGetSingleRegionAfterAsyncAPI(bo, cli, batch, readTier, req.ReadType, nil, lockInfo, opt, collectF)
				cb.Schedule(struct{}{}, err)
			})
			metrics.AsyncBatchGetCounterWithLockError.Inc()
			return
		}

		cb.Invoke(struct{}{}, nil)
		metrics.AsyncBatchGetCounterWithOK.Inc()
	}

	cli.SendReqAsync(bo, req, batch.region, timeout, async.NewCallback(cb.Executor(), onResp), ops...)
}

func (s *KVSnapshot) retryBatchGetSingleRegionAfterAsyncAPI(
	bo *retry.Backoffer,
	cli *ClientHelper,
	batch batchKeys,
	readTier int,
	readType string,
	regionErr *errorpb.Error,
	lockInfo *batchGetLockInfo,
	opt kv.BatchGetOptions,
	collectF func(k []byte, v kv.ValueEntry),
) error {
	var (
		resolvingRecordToken  *int
		readAfterResolveLocks bool
	)
	for {
		if regionErr != nil {
			retriable, err := s.handleBatchGetRegionError(bo, &batch, cli.regionCache, regionErr)
			if err != nil {
				return err
			}
			if !retriable {
				return s.batchGetKeysByRegions(bo, batch.keys, readTier, false, opt, collectF)
			}
		} else if lockInfo != nil && len(lockInfo.lockedKeys) > 0 {
			if resolvingRecordToken == nil {
				token := cli.RecordResolvingLocks(lockInfo.locks, s.version)
				resolvingRecordToken = &token
				defer cli.ResolveLocksDone(s.version, *resolvingRecordToken)
			} else {
				cli.UpdateResolvingLocks(lockInfo.locks, s.version, *resolvingRecordToken)
			}
			readAfterResolveLocks = true
			if err := s.handleBatchGetLocks(bo, lockInfo, cli); err != nil {
				return err
			}
			// Only reduce pending keys when there is no response-level error. Otherwise,
			// lockedKeys may be incomplete.
			if lockInfo.keyErr == nil {
				batch.keys = lockInfo.lockedKeys
			}
		}

		s.mu.RLock()
		isStaleness := s.mu.isStaleness
		busyThresholdMs := s.mu.busyThreshold.Milliseconds()
		if isStaleness && readAfterResolveLocks {
			isStaleness = false
			busyThresholdMs = 0
		}
		req, err := s.buildBatchGetRequest(batch.keys, busyThresholdMs, readTier, opt)
		if err != nil {
			s.mu.RUnlock()
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
		readReplicaScope := s.mu.readReplicaScope
		matchStoreLabels := s.mu.matchStoreLabels
		replicaReadAdjuster := s.mu.replicaReadAdjuster
		s.mu.RUnlock()

		req.TxnScope = readReplicaScope
		req.ReadReplicaScope = readReplicaScope
		if isStaleness {
			req.EnableStaleWithMixedReplicaRead()
		}
		timeout := client.ReadTimeoutMedium
		req.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
		ops := make([]locate.StoreSelectorOption, 0, 2)
		if len(matchStoreLabels) > 0 {
			ops = append(ops, locate.WithMatchLabels(matchStoreLabels))
		}
		if req.ReplicaReadType.IsFollowerRead() && replicaReadAdjuster != nil {
			op, readType := replicaReadAdjuster(len(batch.keys))
			if op != nil {
				ops = append(ops, op)
			}
			req.ReplicaReadType = readType
		}
		resp, _, _, err := cli.SendReqCtx(bo, req, batch.region, timeout, tikvrpc.TiKV, "", ops...)
		if err != nil {
			return err
		}
		regionErr, err = resp.GetRegionError()
		if err != nil {
			return err
		}
		readType = req.ReadType
		if regionErr != nil {
			continue
		}
		lockInfo, err = collectBatchGetResponseData(resp, collectF, s.mergeExecDetail)
		if err != nil {
			return err
		}
		if len(lockInfo.lockedKeys) > 0 {
			continue
		}
		return nil
	}
}
