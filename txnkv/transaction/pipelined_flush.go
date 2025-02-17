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

package transaction

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// PipelinedRequestSource is the source of the Flush & ResolveLock requests in a txn with pipelined memdb.
// txn.GetRequestSource may cause data race because the upper layer may edit the source while the flush requests are built in background.
// So we use the fixed source from the upper layer to avoid the data race.
// This also distinguishes the resource usage between p-DML(pipelined DML) and other small DMLs.
const PipelinedRequestSource = "external_pdml"

type actionPipelinedFlush struct {
	generation uint64
}

var _ twoPhaseCommitAction = actionPipelinedFlush{}

func (action actionPipelinedFlush) String() string {
	return "pipelined_flush"
}

func (action actionPipelinedFlush) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return nil
}

func (c *twoPhaseCommitter) buildPipelinedFlushRequest(batch batchMutations, generation uint64) *tikvrpc.Request {
	m := batch.mutations
	mutations := make([]*kvrpcpb.Mutation, m.Len())

	for i := 0; i < m.Len(); i++ {
		assertion := kvrpcpb.Assertion_None
		if m.IsAssertExists(i) {
			assertion = kvrpcpb.Assertion_Exist
		}
		if m.IsAssertNotExist(i) {
			assertion = kvrpcpb.Assertion_NotExist
		}
		mutations[i] = &kvrpcpb.Mutation{
			Op:        m.GetOp(i),
			Key:       m.GetKey(i),
			Value:     m.GetValue(i),
			Assertion: assertion,
		}
	}

	minCommitTS := c.startTS + 1

	req := &kvrpcpb.FlushRequest{
		Mutations:      mutations,
		PrimaryKey:     c.primary(),
		StartTs:        c.startTS,
		MinCommitTs:    minCommitTS,
		Generation:     generation,
		LockTtl:        max(defaultLockTTL, ManagedLockTTL),
		AssertionLevel: c.txn.assertionLevel,
	}

	r := tikvrpc.NewRequest(
		tikvrpc.CmdFlush, req, kvrpcpb.Context{
			Priority:               c.priority,
			SyncLog:                c.syncLog,
			ResourceGroupTag:       c.resourceGroupTag,
			DiskFullOpt:            c.txn.diskFullOpt,
			TxnSource:              c.txn.txnSource,
			MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds()),
			RequestSource:          PipelinedRequestSource,
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: c.resourceGroupName,
			},
		},
	)
	if c.resourceGroupTag == nil && c.resourceGroupTagger != nil {
		c.resourceGroupTagger(r)
	}
	return r
}

func (action actionPipelinedFlush) handleSingleBatch(
	c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations,
) (err error) {
	if len(c.primaryKey) == 0 {
		logutil.Logger(bo.GetCtx()).Error(
			"[pipelined dml] primary key should be set before pipelined flush",
			zap.Uint64("startTS", c.startTS),
			zap.Uint64("generation", action.generation),
		)
		return errors.New("[pipelined dml] primary key should be set before pipelined flush")
	}

	tBegin := time.Now()
	attempts := 0

	req := c.buildPipelinedFlushRequest(batch, action.generation)
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), c.store.GetOracle())
	var resolvingRecordToken *int

	for {
		attempts++
		reqBegin := time.Now()
		if reqBegin.Sub(tBegin) > slowRequestThreshold {
			logutil.Logger(bo.GetCtx()).Warn(
				"[pipelined dml] slow pipelined flush request",
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("generation", action.generation),
				zap.Stringer("region", &batch.region),
				zap.Int("attempts", attempts),
			)
			tBegin = time.Now()
		}
		resp, _, err := sender.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
		// Unexpected error occurs, return it
		if err != nil {
			return err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return err
		}
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
			if regionErr.GetDiskFull() != nil {
				storeIds := regionErr.GetDiskFull().GetStoreId()
				desc := " "
				for _, i := range storeIds {
					desc += strconv.FormatUint(i, 10) + " "
				}

				logutil.Logger(bo.GetCtx()).Error(
					"Request failed cause of TiKV disk full",
					zap.String("store_id", desc),
					zap.String("reason", regionErr.GetDiskFull().GetReason()),
				)

				return errors.New(regionErr.String())
			}
			same, err := batch.relocate(bo, c.store.GetRegionCache())
			if err != nil {
				return err
			}
			if same {
				continue
			}
			err = c.doActionOnMutations(bo, actionPipelinedFlush{generation: action.generation}, batch.mutations)
			return err
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		flushResp := resp.Resp.(*kvrpcpb.FlushResponse)
		keyErrs := flushResp.GetErrors()
		if len(keyErrs) == 0 {
			// Clear the RPC Error since the request is evaluated successfully.
			sender.SetRPCError(nil)

			// Update CommitDetails
			reqDuration := time.Since(reqBegin)
			c.getDetail().MergeFlushReqDetails(
				reqDuration,
				batch.region.GetID(),
				sender.GetStoreAddr(),
				flushResp.ExecDetailsV2,
			)

			if batch.isPrimary {
				// start keepalive after primary key is written.
				c.run(c, nil, true)
			}
			return nil
		}
		locks := make([]*txnlock.Lock, 0, len(keyErrs))

		logged := make(map[uint64]struct{}, 1)
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
				return c.extractKeyExistsErr(e)
			}

			// Extract lock from key error
			lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
			if err1 != nil {
				return err1
			}
			if _, ok := logged[lock.TxnID]; !ok {
				logutil.Logger(bo.GetCtx()).Info(
					"[pipelined dml] flush encounters lock. "+
						"More locks belonging to the same transaction may be omitted",
					zap.Uint64("txnID", c.startTS),
					zap.Uint64("generation", action.generation),
					zap.Stringer("lock", lock),
				)
				logged[lock.TxnID] = struct{}{}
			}
			// If an optimistic transaction encounters a lock with larger TS, this transaction will certainly
			// fail due to a WriteConflict error. So we can construct and return an error here early.
			// Pessimistic transactions don't need such an optimization. If this key needs a pessimistic lock,
			// TiKV will return a PessimisticLockNotFound error directly if it encounters a different lock. Otherwise,
			// TiKV returns lock.TTL = 0, and we still need to resolve the lock.
			if lock.TxnID > c.startTS && !c.isPessimistic {
				metrics.LockResolverCountWithWriteConflict.Inc()
				return tikverr.NewErrWriteConflictWithArgs(
					c.startTS,
					lock.TxnID,
					0,
					lock.Key,
					kvrpcpb.WriteConflict_Optimistic,
				)
			}
			locks = append(locks, lock)
		}
		if resolvingRecordToken == nil {
			token := c.store.GetLockResolver().RecordResolvingLocks(locks, c.startTS)
			resolvingRecordToken = &token
			defer c.store.GetLockResolver().ResolveLocksDone(c.startTS, *resolvingRecordToken)
		} else {
			c.store.GetLockResolver().UpdateResolvingLocks(locks, c.startTS, *resolvingRecordToken)
		}
		resolveLockOpts := txnlock.ResolveLocksOptions{
			CallerStartTS: c.startTS,
			Locks:         locks,
			Detail:        &c.getDetail().ResolveLock,
		}
		resolveLockRes, err := c.store.GetLockResolver().ResolveLocksWithOpts(bo, resolveLockOpts)
		if err != nil {
			return err
		}
		msBeforeExpired := resolveLockRes.TTL
		if msBeforeExpired > 0 {
			err = bo.BackoffWithCfgAndMaxSleep(
				retry.BoTxnLock,
				int(msBeforeExpired),
				errors.Errorf("[pipelined dml] flush lockedKeys: %d", len(locks)),
			)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Warn(
					"[pipelined dml] backoff failed during flush",
					zap.Error(err),
					zap.Uint64("startTS", c.startTS),
					zap.Uint64("generation", action.generation),
				)
				return err
			}
		}
	}
}

func (c *twoPhaseCommitter) pipelinedFlushMutations(bo *retry.Backoffer, mutations CommitterMutations, generation uint64) error {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.pipelinedFlushMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	return c.doActionOnMutations(bo, actionPipelinedFlush{generation}, mutations)
}

func (c *twoPhaseCommitter) commitFlushedMutations(bo *retry.Backoffer) error {
	logutil.Logger(bo.GetCtx()).Info(
		"[pipelined dml] start to commit transaction",
		zap.Int("keys", c.txn.GetMemBuffer().Len()),
		zap.Duration("flush_wait_duration", c.txn.GetMemBuffer().GetMetrics().WaitDuration),
		zap.Duration("total_duration", c.txn.GetMemBuffer().GetMetrics().TotalDuration),
		zap.Uint64("memdb traversal cache hit", c.txn.GetMemBuffer().GetMetrics().MemDBHitCount),
		zap.Uint64("memdb traversal cache miss", c.txn.GetMemBuffer().GetMetrics().MemDBMissCount),
		zap.String("size", units.HumanSize(float64(c.txn.GetMemBuffer().Size()))),
		zap.Uint64("startTS", c.startTS),
	)
	commitTS, err := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
	if err != nil {
		logutil.Logger(bo.GetCtx()).Warn("[pipelined dml] commit transaction get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS),
		)
		return err
	}
	atomic.StoreUint64(&c.commitTS, commitTS)

	if _, err := util.EvalFailpoint("pipelinedCommitFail"); err == nil {
		return errors.New("pipelined DML commit failed")
	}

	primaryMutation := NewPlainMutations(1)
	primaryMutation.Push(c.pipelinedCommitInfo.primaryOp, c.primaryKey, nil, false, false, false, false)
	if err = c.commitMutations(bo, &primaryMutation); err != nil {
		return errors.Trace(err)
	}
	c.mu.Lock()
	c.mu.committed = true
	c.mu.Unlock()
	logutil.Logger(bo.GetCtx()).Info(
		"[pipelined dml] transaction is committed",
		zap.Uint64("startTS", c.startTS),
		zap.Uint64("commitTS", commitTS),
	)
	broadcastToAllStores(
		c.txn,
		c.store,
		retry.NewBackofferWithVars(
			bo.GetCtx(),
			broadcastMaxBackoff,
			c.txn.vars,
		),
		&kvrpcpb.TxnStatus{
			StartTs:     c.startTS,
			MinCommitTs: c.minCommitTSMgr.get(),
			CommitTs:    commitTS,
			RolledBack:  false,
			IsCompleted: false,
		},
		c.resourceGroupName,
		c.resourceGroupTag,
	)

	if _, err := util.EvalFailpoint("pipelinedSkipResolveLock"); err == nil {
		return nil
	}

	// async resolve the rest locks.
	commitBo := retry.NewBackofferWithVars(c.store.Ctx(), CommitSecondaryMaxBackoff, c.txn.vars)
	c.resolveFlushedLocks(commitBo, c.pipelinedCommitInfo.pipelinedStart, c.pipelinedCommitInfo.pipelinedEnd, true)
	return nil
}

// buildPipelinedResolveHandler returns a function which resolves all locks for the given region.
// If the region cache is stale, it reloads the region info and resolve the rest ranges.
// The function also count resolved regions.
func (c *twoPhaseCommitter) buildPipelinedResolveHandler(commit bool, resolved *atomic.Uint64) (rangetask.TaskHandler, error) {
	commitVersion := uint64(0)
	if commit {
		commitVersion = atomic.LoadUint64(&c.commitTS)
		if commitVersion == 0 {
			return nil, errors.New("commitTS is 0")
		}
	}
	maxBackOff := cleanupMaxBackoff
	if commit {
		maxBackOff = CommitSecondaryMaxBackoff
	}
	regionCache := c.store.GetRegionCache()
	// the handler function runs in a different goroutine, should copy the required values before it to avoid race.
	kvContext := &kvrpcpb.Context{
		Priority:         c.priority,
		SyncLog:          c.syncLog,
		ResourceGroupTag: c.resourceGroupTag,
		DiskFullOpt:      c.diskFullOpt,
		TxnSource:        c.txnSource,
		RequestSource:    PipelinedRequestSource,
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	}
	return func(ctx context.Context, r kv.KeyRange) (rangetask.TaskStat, error) {
		start := r.StartKey
		res := rangetask.TaskStat{}
		for {
			lreq := &kvrpcpb.ResolveLockRequest{
				StartVersion:  c.startTS,
				CommitVersion: commitVersion,
			}
			req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, lreq, *proto.Clone(kvContext).(*kvrpcpb.Context))
			bo := retry.NewBackoffer(ctx, maxBackOff)
			loc, err := regionCache.LocateKey(bo, start)
			if err != nil {
				return res, err
			}
			resp, err := c.store.SendReq(bo, req, loc.Region, client.MaxWriteExecutionTime)
			if err != nil {
				err = bo.Backoff(retry.BoRegionMiss, err)
				if err != nil {
					logutil.Logger(bo.GetCtx()).Error("send resolve lock request error", zap.Error(err))
					return res, err
				}
				continue
			}
			regionErr, err := resp.GetRegionError()
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("get region error failed", zap.Error(err))
				return res, err
			}
			if regionErr != nil {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					logutil.Logger(bo.GetCtx()).Error("send resolve lock get region error", zap.Error(err))
					return res, err
				}
				continue
			}
			if resp.Resp == nil {
				logutil.Logger(bo.GetCtx()).Error("send resolve lock response body missing", zap.Error(errors.WithStack(tikverr.ErrBodyMissing)))
				return res, err
			}
			cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
			if keyErr := cmdResp.GetError(); keyErr != nil {
				err = errors.Errorf("unexpected resolve err: %s", keyErr)
				logutil.BgLogger().Error("resolveLock error", zap.Error(err))
				return res, err
			}
			resolved.Add(1)
			res.CompletedRegions++
			if loc.EndKey == nil || bytes.Compare(loc.EndKey, r.EndKey) >= 0 {
				return res, nil
			}
			start = loc.EndKey
		}
	}, nil
}

// resolveFlushedLocks resolves all locks in the given range [start, end) with the given status.
// The resolve process is running in another goroutine so this function won't block.
func (c *twoPhaseCommitter) resolveFlushedLocks(bo *retry.Backoffer, start, end []byte, commit bool) {
	var resolved atomic.Uint64
	handler, err := c.buildPipelinedResolveHandler(commit, &resolved)
	commitTs := uint64(0)
	if commit {
		commitTs = atomic.LoadUint64(&c.commitTS)
	}
	if err != nil {
		logutil.Logger(bo.GetCtx()).Error(
			"[pipelined dml] build buildPipelinedResolveHandler error",
			zap.Error(err),
			zap.Uint64("resolved regions", resolved.Load()),
			zap.Uint64("startTS", c.startTS),
			zap.Uint64("commitTS", commitTs),
		)
		return
	}

	status := "rollback"
	if commit {
		status = "commit"
	}

	runner := rangetask.NewRangeTaskRunnerWithID(
		fmt.Sprintf("pipelined-dml-%s", status),
		fmt.Sprintf("pipelined-dml-%s-%d", status, c.startTS),
		c.store,
		c.txn.pipelinedResolveLockConcurrency,
		handler,
	)
	runner.SetStatLogInterval(30 * time.Second)

	c.txn.spawnWithStorePool(func() {
		if err = runner.RunOnRange(bo.GetCtx(), start, end); err != nil {
			logutil.Logger(bo.GetCtx()).Error("[pipelined dml] resolve flushed locks failed",
				zap.String("txn-status", status),
				zap.Uint64("resolved regions", resolved.Load()),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("commitTS", commitTs),
				zap.Uint64("session", c.sessionID),
				zap.Error(err),
			)
		} else {
			logutil.Logger(bo.GetCtx()).Info("[pipelined dml] resolve flushed locks done",
				zap.String("txn-status", status),
				zap.Uint64("resolved regions", resolved.Load()),
				zap.Uint64("startTS", c.startTS),
				zap.Uint64("commitTS", commitTs),
				zap.Uint64("session", c.sessionID),
			)

			// wait a while before notifying txn_status_cache to evict the txn,
			// which tolerates slow followers and avoids the situation that the
			// txn is evicted before the follower catches up.
			time.Sleep(broadcastGracePeriod)

			broadcastToAllStores(
				c.txn,
				c.store,
				retry.NewBackofferWithVars(
					bo.GetCtx(),
					broadcastMaxBackoff,
					c.txn.vars,
				),
				&kvrpcpb.TxnStatus{
					StartTs:     c.startTS,
					MinCommitTs: 0,
					CommitTs:    commitTs,
					RolledBack:  !commit,
					IsCompleted: true,
				},
				c.resourceGroupName,
				c.resourceGroupTag,
			)
		}
	})
}
