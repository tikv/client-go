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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// BuildTxnFileMaxBackoff is max sleep time to build TxnFile.
	BuildTxnFileMaxBackoff = atomicutil.NewUint64(60000)
)

const PreSplitRegionChunks = 4

type txnFileCtx struct {
	slice txnChunkSlice
}

type chunkBatch struct {
	txnChunkSlice
	region    *locate.KeyLocation
	isPrimary bool
}

func (b chunkBatch) String() string {
	return fmt.Sprintf("chunkBatch{region: %s, isPrimary: %t, txnChunkSlice: %s}", b.region.String(), b.isPrimary, b.txnChunkSlice.String())
}

type txnChunkSlice struct {
	chunkIDs    []uint64
	chunkRanges []txnChunkRange
}

func (s txnChunkSlice) String() string {
	slice := make([]string, len(s.chunkRanges))
	for i, ran := range s.chunkRanges {
		slice[i] = fmt.Sprintf("txnChunkSlice{%v: [%s, %s]}", s.chunkIDs[i], kv.StrKey(ran.smallest), kv.StrKey(ran.biggest))
	}
	return fmt.Sprintf("[%s]", strings.Join(slice, ", "))
}

func (s *txnChunkSlice) Smallest() []byte {
	if len(s.chunkRanges) == 0 {
		return nil
	}
	return s.chunkRanges[0].smallest
}

func (s *txnChunkSlice) Biggest() []byte {
	if len(s.chunkRanges) == 0 {
		return nil
	}
	return s.chunkRanges[len(s.chunkRanges)-1].biggest
}

func (cs *txnChunkSlice) appendSlice(other *txnChunkSlice) {
	if cs.len() == 0 {
		cs.chunkIDs = append(cs.chunkIDs, other.chunkIDs...)
		cs.chunkRanges = append(cs.chunkRanges, other.chunkRanges...)
		return
	}
	lastChunkRange := cs.chunkRanges[len(cs.chunkIDs)-1]
	for i, chunkRange := range other.chunkRanges {
		if bytes.Compare(lastChunkRange.smallest, chunkRange.smallest) < 0 {
			cs.chunkIDs = append(cs.chunkIDs, other.chunkIDs[i:]...)
			cs.chunkRanges = append(cs.chunkRanges, other.chunkRanges[i:]...)
			break
		}
	}
}

func (cs *txnChunkSlice) append(chunkID uint64, chunkRange txnChunkRange) {
	cs.chunkIDs = append(cs.chunkIDs, chunkID)
	cs.chunkRanges = append(cs.chunkRanges, chunkRange)
}

func (cs *txnChunkSlice) len() int {
	return len(cs.chunkIDs)
}

func (cs *txnChunkSlice) groupToBatches(primary []byte, c *locate.RegionCache, bo *retry.Backoffer) ([]chunkBatch, error) {
	batchMap := make(map[locate.RegionVerID]*chunkBatch)
	for i, chunkRange := range cs.chunkRanges {
		regions, err := chunkRange.getOverlapRegions(c, bo)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		for _, r := range regions {
			if batchMap[r.Region] == nil {
				batchMap[r.Region] = &chunkBatch{
					region: r,
				}
			}
			batchMap[r.Region].append(cs.chunkIDs[i], chunkRange)
		}
	}

	batches := make([]chunkBatch, 0, len(batchMap))
	picked := false // Pick the batch with primary and put it at the first.
	for _, batch := range batchMap {
		if primary != nil && !picked && batch.region.Contains(primary) {
			if len(batches) > 0 {
				batches[0], *batch = *batch, batches[0]
			}
			picked = true
		}

		batches = append(batches, *batch)
	}

	if primary != nil && !picked {
		logutil.Logger(bo.GetCtx()).Error("txn file primary not in any batch",
			zap.String("primary", kv.StrKey(primary)),
			zap.Stringer("txnChunkSlice", cs),
			zap.Stringers("batches", batches),
		)
		return nil, fmt.Errorf("txn file primary not in any batch")
	}

	logutil.Logger(bo.GetCtx()).Debug("txn file group to batches", zap.Stringers("batches", batches))
	return batches, nil
}

type txnChunkRange struct {
	smallest []byte
	biggest  []byte
}

func newTxnChunkRange(smallest []byte, biggest []byte) txnChunkRange {
	return txnChunkRange{
		smallest: smallest,
		biggest:  biggest,
	}
}

func (r *txnChunkRange) getOverlapRegions(c *locate.RegionCache, bo *retry.Backoffer) ([]*locate.KeyLocation, error) {
	regions := make([]*locate.KeyLocation, 0)
	startKey := r.smallest
	for bytes.Compare(startKey, r.biggest) <= 0 {
		loc, err := c.LocateKey(bo, startKey)
		if err != nil {
			logutil.Logger(bo.GetCtx()).Error("locate key failed", zap.Error(err), zap.String("startKey", kv.StrKey(startKey)))
			return nil, errors.Wrap(err, "locate key failed")
		}
		regions = append(regions, loc)
		if len(loc.EndKey) == 0 {
			break
		}
		startKey = loc.EndKey
	}
	return regions, nil
}

type txnFileAction interface {
	executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error)
	onPrimarySuccess(c *twoPhaseCommitter)
	extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError
	asyncExecuteSecondaries() bool
	String() string
}

type txnFilePrewriteAction struct{}

var _ txnFileAction = (*txnFilePrewriteAction)(nil)

func (a txnFilePrewriteAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	primaryLock := c.txnFileCtx.slice.chunkRanges[0].smallest
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		StartVersion:   c.startTS,
		PrimaryLock:    primaryLock,
		LockTtl:        c.lockTTL,
		MaxCommitTs:    c.maxCommitTS,
		AssertionLevel: kvrpcpb.AssertionLevel_Off,
		TxnFileChunks:  batch.chunkIDs,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		TxnSource:              c.txnSource,
		MaxExecutionDurationMs: uint64(client.ReadTimeoutMedium.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	})
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	var resolvingRecordToken *int
	for {
		resp, _, err := sender.SendReq(bo, req, batch.region.Region, client.ReadTimeoutMedium)
		if err != nil {
			return nil, err
		}
		if resp.Resp == nil {
			return nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		prewriteResp := resp.Resp.(*kvrpcpb.PrewriteResponse)
		keyErrs := prewriteResp.GetErrors()
		if len(keyErrs) == 0 {
			return resp, nil
		}
		var locks []*txnlock.Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
				return nil, c.extractKeyExistsErr(e)
			}

			// Extract lock from key error
			lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
			if err1 != nil {
				return nil, err1
			}
			logutil.Logger(bo.GetCtx()).Info(
				"prewrite txn file encounters lock",
				zap.Uint64("session", c.sessionID),
				zap.Uint64("txnID", c.startTS),
				zap.Stringer("lock", lock),
			)
			// If an optimistic transaction encounters a lock with larger TS, this transaction will certainly
			// fail due to a WriteConflict error. So we can construct and return an error here early.
			// Pessimistic transactions don't need such an optimization. If this key needs a pessimistic lock,
			// TiKV will return a PessimisticLockNotFound error directly if it encounters a different lock. Otherwise,
			// TiKV returns lock.TTL = 0, and we still need to resolve the lock.
			if lock.TxnID > c.startTS {
				return nil, tikverr.NewErrWriteConflictWithArgs(
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
			return nil, err
		}
		msBeforeExpired := resolveLockRes.TTL
		if msBeforeExpired > 0 {
			err = bo.BackoffWithCfgAndMaxSleep(
				retry.BoTxnLock,
				int(msBeforeExpired),
				errors.Errorf("2PC txn file prewrite lockedKeys: %d", len(locks)),
			)
			if err != nil {
				return nil, err
			}
		}
	}
}

func (a txnFilePrewriteAction) onPrimarySuccess(c *twoPhaseCommitter) {
	c.run(c, nil)
}

func (a txnFilePrewriteAction) extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError {
	prewriteResp, _ := resp.Resp.(*kvrpcpb.PrewriteResponse)
	errs := prewriteResp.GetErrors()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (a txnFilePrewriteAction) asyncExecuteSecondaries() bool {
	return false
}

func (a txnFilePrewriteAction) String() string {
	return "txnFilePrewrite"
}

type txnFileCommitAction struct {
	commitTS uint64
}

var _ txnFileAction = (*txnFileCommitAction)(nil)

func (a txnFileCommitAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	keys := make([][]byte, 0, batch.len())
	for _, chunkRange := range batch.chunkRanges {
		keys = append(keys, chunkRange.smallest)
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
		Keys:          keys, // set keys to help detect duplicated request.
		StartVersion:  c.startTS,
		CommitVersion: a.commitTS,
		IsTxnFile:     true,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		TxnSource:              c.txnSource,
		MaxExecutionDurationMs: uint64(client.ReadTimeoutMedium.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	})
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	for {
		resp, _, err := sender.SendReq(bo, req, batch.region.Region, client.ReadTimeoutMedium)
		if batch.isPrimary && sender.GetRPCError() != nil {
			c.setUndeterminedErr(errors.WithStack(sender.GetRPCError()))
		}
		// Unexpected error occurs, return it.
		if err != nil {
			return nil, err
		}
		if resp.Resp == nil {
			return nil, errors.WithStack(tikverr.ErrBodyMissing)
		}
		commitResp := resp.Resp.(*kvrpcpb.CommitResponse)
		if keyErr := commitResp.GetError(); keyErr != nil {
			if rejected := keyErr.GetCommitTsExpired(); rejected != nil {
				logutil.Logger(bo.GetCtx()).Info("2PC commitTS rejected by TiKV, retry with a newer commitTS",
					zap.Uint64("txnStartTS", c.startTS),
					zap.Stringer("info", logutil.Hex(rejected)))

				// Do not retry for a txn which has a too large MinCommitTs
				// 3600000 << 18 = 943718400000
				if rejected.MinCommitTs-rejected.AttemptedCommitTs > 943718400000 {
					return nil, errors.Errorf("2PC MinCommitTS is too large, we got MinCommitTS: %d, and AttemptedCommitTS: %d",
						rejected.MinCommitTs, rejected.AttemptedCommitTs)
				}

				// Update commit ts and retry.
				commitTS, err1 := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
				if err1 != nil {
					logutil.Logger(bo.GetCtx()).Warn("2PC get commitTS failed",
						zap.Error(err1),
						zap.Uint64("txnStartTS", c.startTS))
					return nil, err1
				}

				c.mu.Lock()
				c.commitTS = commitTS
				c.mu.Unlock()
				// Update the commitTS of the request and retry.
				req.Commit().CommitVersion = commitTS
				continue
			}
			return nil, tikverr.ExtractKeyErr(keyErr)
		}
		return resp, nil
	}
}

func (a txnFileCommitAction) onPrimarySuccess(c *twoPhaseCommitter) {
	c.mu.Lock()
	c.mu.committed = true
	c.mu.Unlock()
}

func (a txnFileCommitAction) extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError {
	commitResp, _ := resp.Resp.(*kvrpcpb.CommitResponse)
	return commitResp.GetError()
}

func (a txnFileCommitAction) asyncExecuteSecondaries() bool {
	return true
}

func (a txnFileCommitAction) String() string {
	return "txnFileCommit"
}

type txnFileRollbackAction struct{}

var _ txnFileAction = (*txnFileRollbackAction)(nil)

func (a txnFileRollbackAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	keys := make([][]byte, 0, batch.len())
	for _, chunkRange := range batch.chunkRanges {
		keys = append(keys, chunkRange.smallest)
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, &kvrpcpb.BatchRollbackRequest{
		Keys:         keys,
		StartVersion: c.startTS,
		IsTxnFile:    true,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       c.resourceGroupTag,
		DiskFullOpt:            c.diskFullOpt,
		TxnSource:              c.txnSource,
		MaxExecutionDurationMs: uint64(client.ReadTimeoutShort.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: c.resourceGroupName,
		},
	})
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
	resp, _, err1 := sender.SendReq(bo, req, batch.region.Region, client.ReadTimeoutShort)
	if err1 != nil {
		return nil, err1
	}
	return resp, nil
}

func (a txnFileRollbackAction) onPrimarySuccess(_ *twoPhaseCommitter) {
}

func (a txnFileRollbackAction) extractKeyError(resp *tikvrpc.Response) *kvrpcpb.KeyError {
	rollbackResp, _ := resp.Resp.(*kvrpcpb.BatchRollbackResponse)
	return rollbackResp.GetError()
}

func (a txnFileRollbackAction) asyncExecuteSecondaries() bool {
	return true
}

func (a txnFileRollbackAction) String() string {
	return "txnFileRollback"
}

func (c *twoPhaseCommitter) executeTxnFile(ctx context.Context) (err error) {
	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			if c.txnFileCtx.slice.len() > 0 {
				err1 := c.executeTxnFileAction(retry.NewBackofferWithVars(ctx, int(PrewriteMaxBackoff.Load()), c.txn.vars), c.txnFileCtx.slice, txnFileRollbackAction{})
				if err1 != nil {
					logutil.BgLogger().Error("executeTxnFile: rollback on error failed", zap.Error(err1))
				}
			}
			metrics.TwoPCTxnCounterError.Inc()
		} else {
			metrics.TwoPCTxnCounterOk.Inc()
		}
		c.txn.commitTS = c.commitTS

		logutil.Logger(ctx).Info("execute txn file finished", zap.Uint64("startTS", c.startTS), zap.Uint64("commitTS", c.commitTS), zap.Error(err))
	}()

	logutil.Logger(ctx).Info("execute txn file", zap.Uint64("startTS", c.startTS))

	buildBo := retry.NewBackofferWithVars(ctx, int(BuildTxnFileMaxBackoff.Load()), c.txn.vars)
	if err = c.buildTxnFiles(buildBo, c.mutations); err != nil {
		logutil.Logger(ctx).Error("build txn files failed", zap.Error(err))
		return
	}
	if errSplit := c.preSplitTxnFileRegions(buildBo); errSplit != nil {
		logutil.Logger(ctx).Warn("txn file pre split regions failed", zap.Error(errSplit))
	}

	prewriteBo := retry.NewBackofferWithVars(ctx, int(PrewriteMaxBackoff.Load()), c.txn.vars)
	logutil.Logger(ctx).Info("execute txn file prewrite", zap.Uint64("startTS", c.startTS))
	err = c.executeTxnFileAction(prewriteBo, c.txnFileCtx.slice, txnFilePrewriteAction{})
	logutil.Logger(ctx).Info("execute txn file prewrite finished", zap.Uint64("startTS", c.startTS), zap.Error(err))
	if err != nil {
		return
	}

	commitBo := retry.NewBackofferWithVars(ctx, int(CommitMaxBackoff), c.txn.vars)
	c.commitTS, err = c.store.GetTimestampWithRetry(commitBo, c.txn.GetScope())
	if err != nil {
		return
	}
	logutil.Logger(ctx).Info("execute txn file commit", zap.Uint64("startTS", c.startTS))
	err = c.executeTxnFileAction(commitBo, c.txnFileCtx.slice, txnFileCommitAction{commitTS: c.commitTS})
	logutil.Logger(ctx).Info("execute txn file commit finished", zap.Uint64("startTS", c.startTS), zap.Error(err))
	return
}

func (c *twoPhaseCommitter) executeTxnFileSlice(bo *retry.Backoffer, chunkSlice txnChunkSlice, batches []chunkBatch, action txnFileAction, primaryRegion *locate.KeyLocation) (txnChunkSlice, error) {
	isBatchExecuted := func(batch *chunkBatch) bool {
		return primaryRegion != nil &&
			bytes.Compare(primaryRegion.StartKey, batch.Smallest()) <= 0 &&
			(len(primaryRegion.EndKey) == 0 || bytes.Compare(batch.Biggest(), primaryRegion.EndKey) < 0)
	}

	var err error
	var regionErrChunks txnChunkSlice

	if batches == nil {
		batches, err = chunkSlice.groupToBatches(nil, c.store.GetRegionCache(), bo)
		if err != nil {
			return regionErrChunks, errors.Wrap(err, "group to batches failed")
		}
	}

	for _, batch := range batches {
		if isBatchExecuted(&batch) {
			continue
		}

		logutil.Logger(bo.GetCtx()).Info("txn file execute secondary batch", zap.Uint64("startTS", c.startTS),
			zap.Any("batch", batch),
			zap.Stringer("action", action),
		)
		resp, err1 := action.executeBatch(c, bo, batch)
		logutil.Logger(bo.GetCtx()).Info("txn file execute secondary batch finished", zap.Uint64("startTS", c.startTS),
			zap.Any("batch", batch),
			zap.Stringer("action", action), zap.Error(err1),
		)
		if err1 != nil {
			return regionErrChunks, err1
		}
		if keyErr := action.extractKeyError(resp); keyErr != nil {
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
				return regionErrChunks, c.extractKeyExistsErr(e)
			}
			lock, err2 := txnlock.ExtractLockFromKeyErr(keyErr)
			if err2 != nil {
				return regionErrChunks, err2
			}
			if lock.TxnID > c.startTS {
				return regionErrChunks, tikverr.NewErrWriteConflictWithArgs(
					c.startTS,
					lock.TxnID,
					0,
					lock.Key,
					kvrpcpb.WriteConflict_Optimistic,
				)
			}
		}
		regionErr, err1 := resp.GetRegionError()
		if err1 != nil {
			return regionErrChunks, err1
		}
		if regionErr != nil {
			regionErrChunks.appendSlice(&batch.txnChunkSlice)
			continue
		}
	}
	return regionErrChunks, nil
}

func (c *twoPhaseCommitter) executeTxnFileSliceWithRetry(bo *retry.Backoffer, chunkSlice txnChunkSlice, batches []chunkBatch, action txnFileAction, primaryRegion *locate.KeyLocation) error {
	currentChunks := chunkSlice
	currentBatches := batches
	for {
		var regionErrChunks txnChunkSlice
		regionErrChunks, err := c.executeTxnFileSlice(bo, currentChunks, currentBatches, action, primaryRegion)
		if err != nil {
			return errors.WithStack(err)
		}
		if regionErrChunks.len() == 0 {
			return nil
		}
		currentChunks = regionErrChunks
		currentBatches = nil
		err = bo.Backoff(retry.BoRegionMiss, errors.Errorf("txn file region miss"))
		if err != nil {
			return errors.WithStack(err)
		}
	}
}

func (c *twoPhaseCommitter) executeTxnFilePrimaryBatch(bo *retry.Backoffer, firstBatch chunkBatch, action txnFileAction) (regionErr *errorpb.Error, err error) {
	if !firstBatch.region.Contains(c.primary()) {
		logutil.Logger(bo.GetCtx()).Error("primary out of first batch",
			zap.Uint64("startTS", c.startTS),
			zap.String("primary", kv.StrKey(c.primary())),
			zap.Stringer("action", action),
			zap.Stringer("first batch", firstBatch),
		)
		return nil, fmt.Errorf("primary out of first batch, primary: %s, first batch: %v", kv.StrKey(c.primary()), firstBatch)
	}

	firstBatch.isPrimary = true
	logutil.Logger(bo.GetCtx()).Info("txn file execute primary batch", zap.Uint64("startTS", c.startTS),
		zap.Any("batch", firstBatch), zap.String("primary", kv.StrKey(c.primary())),
		zap.Stringer("action", action),
	)
	resp, err := action.executeBatch(c, bo, firstBatch)
	logutil.Logger(bo.GetCtx()).Info("txn file execute primary batch finished", zap.Uint64("startTS", c.startTS),
		zap.Any("batch", firstBatch), zap.String("primary", kv.StrKey(c.primary())),
		zap.Stringer("action", action), zap.Error(err),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	regionErr, err = resp.GetRegionError()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if regionErr != nil {
		return regionErr, nil
	}
	action.onPrimarySuccess(c)
	return nil, nil
}

func (c *twoPhaseCommitter) executeTxnFileAction(bo *retry.Backoffer, chunkSlice txnChunkSlice, action txnFileAction) error {
	for {
		batches, err := chunkSlice.groupToBatches(c.primary(), c.store.GetRegionCache(), bo)
		if err != nil {
			return errors.Wrap(err, "group to batches failed")
		}

		regionErr, err := c.executeTxnFilePrimaryBatch(bo, batches[0], action)
		if err != nil {
			return errors.WithStack(err)
		}
		if regionErr != nil {
			errBo := bo.Backoff(retry.BoRegionMiss, errors.Wrap(errors.New(regionErr.String()), "txn file execute primary batch failed"))
			if errBo != nil {
				return errors.WithStack(errBo)
			}
			continue
		}

		primaryRegion := batches[0].region
		secondaries := batches[1:]
		if len(secondaries) == 0 {
			return nil
		}
		var emptySlice txnChunkSlice
		if !action.asyncExecuteSecondaries() {
			return c.executeTxnFileSliceWithRetry(bo, emptySlice, secondaries, action, primaryRegion)
		}

		c.store.WaitGroup().Add(1)
		errGo := c.store.Go(func() {
			defer c.store.WaitGroup().Done()
			logutil.Logger(bo.GetCtx()).Info("txn file async execute secondaries", zap.Uint64("startTS", c.startTS),
				zap.Stringer("action", action),
			)
			err := c.executeTxnFileSliceWithRetry(bo, emptySlice, secondaries, action, primaryRegion)
			logutil.Logger(bo.GetCtx()).Info("txn file async execute secondaries finished", zap.Uint64("startTS", c.startTS),
				zap.Stringer("action", action), zap.Error(err),
			)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Warn("txn file async execute secondaries failed",
					zap.Uint64("startTS", c.startTS), zap.Stringer("action", action), zap.Error(err))
			}
		})
		if errGo != nil {
			c.store.WaitGroup().Done()
			logutil.Logger(bo.GetCtx()).Warn("fail to create goroutine",
				zap.Uint64("startTS", c.startTS), zap.Stringer("action", action), zap.Error(errGo))
		}
		return nil
	}
}

func (c *twoPhaseCommitter) buildTxnFiles(bo *retry.Backoffer, mutations CommitterMutations) error {
	maxTxnChunkSize := int(config.GetGlobalConfig().TiKVClient.TxnChunkMaxSize)
	capacity := c.txn.Size() + c.txn.Len()*7 + 4
	if capacity > maxTxnChunkSize {
		capacity = maxTxnChunkSize
	}

	writer, err := newChunkWriterClient()
	if err != nil {
		return errors.Wrap(err, "new chunk writer client failed")
	}

	totalSize := 0
	buf := make([]byte, 0, capacity)
	chunkSmallest := mutations.GetKey(0)
	for i := 0; i < mutations.Len(); i++ {
		key := mutations.GetKey(i)
		op := mutations.GetOp(i)
		val := mutations.GetValue(i)
		entrySize := 2 + len(key) + 1 + 4 + len(val)
		if len(buf) > 0 && len(buf)+entrySize+4 > cap(buf) {
			totalSize += len(buf)
			chunkID, err := c.buildTxnFile(bo, writer, buf)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("build txn file failed", zap.Error(err))
				return errors.Wrap(err, "build txn file failed")
			}
			ran := newTxnChunkRange(chunkSmallest, mutations.GetKey(i-1))
			c.txnFileCtx.slice.append(chunkID, ran)
			chunkSmallest = key
			buf = buf[:0]
		}
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		buf = append(buf, byte(op))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	}
	if len(buf) > 0 {
		totalSize += len(buf)
		chunkID, err := c.buildTxnFile(bo, writer, buf)
		if err != nil {
			logutil.Logger(bo.GetCtx()).Error("build txn file failed", zap.Error(err))
			return errors.Wrap(err, "build txn file failed")
		}
		ran := newTxnChunkRange(chunkSmallest, mutations.GetKey(mutations.Len()-1))
		c.txnFileCtx.slice.append(chunkID, ran)
	}
	logutil.Logger(bo.GetCtx()).Info("build txn files",
		zap.Uint64("startTS", c.startTS),
		zap.Int("mutationsLen", mutations.Len()),
		zap.Int("totalChunksSize", totalSize),
		zap.Any("chunkIDs", c.txnFileCtx.slice.chunkIDs))
	return nil
}

func (c *twoPhaseCommitter) buildTxnFile(bo *retry.Backoffer, writer *chunkWriterClient, buf []byte) (uint64, error) {
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	hash.Write(buf)
	crc := hash.Sum32()
	buf = binary.LittleEndian.AppendUint32(buf, crc)

	for {
		req, err := http.NewRequestWithContext(bo.GetCtx(), "POST", writer.serviceAddr, bytes.NewReader(buf))
		if err != nil {
			return 0, errors.WithStack(err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := writer.cli.Do(req)
		if err != nil {
			logutil.Logger(bo.GetCtx()).Warn("build txn file request failed", zap.Error(err), zap.String("addr", writer.serviceAddr))
			err = bo.Backoff(retry.BoTiKVRPC, errors.WithMessage(err, "build txn file request failed"))
			if err != nil {
				return 0, errors.WithStack(err)
			}
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			var bodyStr string
			if data, err := io.ReadAll(resp.Body); err == nil {
				bodyStr = string(data)
			}
			logutil.Logger(bo.GetCtx()).Warn("build txn file service error", zap.String("http status", resp.Status), zap.String("body", bodyStr))
			err = bo.Backoff(retry.BoTiKVServerBusy, errors.WithMessagef(err, "build txn file service error, http status %s", resp.Status))
			if err != nil {
				return 0, errors.WithStack(err)
			}
			continue
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		v := struct {
			ChunkId uint64 `json:"chunk_id"`
		}{}
		if err = json.Unmarshal(data, &v); err != nil {
			return 0, errors.Wrapf(err, "unmarshal response %s", string(data))
		}
		logutil.Logger(bo.GetCtx()).Debug("build txn file", zap.Int("size", len(buf)), zap.Uint64("chunkId", v.ChunkId))
		return v.ChunkId, nil
	}
}

func (c *twoPhaseCommitter) useTxnFile() bool {
	conf := config.GetGlobalConfig()
	if c.txn == nil || c.txn.isPessimistic || c.txn.isInternal() || uint64(c.txn.GetMemBuffer().Size()) < conf.TiKVClient.TxnFileMinMutationSize {
		return false
	}
	return len(conf.TiKVClient.TxnChunkWriterAddr) > 0
}

func (c *twoPhaseCommitter) preSplitTxnFileRegions(bo *retry.Backoffer) error {
	batches, err := c.txnFileCtx.slice.groupToBatches(nil, c.store.GetRegionCache(), bo)
	if err != nil {
		return errors.Wrap(err, "group to batches failed")
	}
	var splitKeys [][]byte
	for _, batch := range batches {
		if batch.len() > PreSplitRegionChunks {
			for i := PreSplitRegionChunks; i < batch.len(); i += PreSplitRegionChunks {
				splitKeys = append(splitKeys, batch.chunkRanges[i].smallest)
			}
		}
	}
	if len(splitKeys) == 0 {
		return nil
	}
	_, err = c.store.SplitRegions(bo.GetCtx(), splitKeys, false, nil)
	return errors.Wrap(err, "pre split regions failed")
}

type chunkWriterClient struct {
	cli         *http.Client
	serviceAddr string
}

func newChunkWriterClient() (*chunkWriterClient, error) {
	var (
		cfg     = config.GetGlobalConfig()
		scheme  = "http://"
		timeout = time.Duration(BuildTxnFileMaxBackoff.Load()) * time.Millisecond
		client  = &http.Client{Timeout: timeout}
	)
	if len(cfg.Security.ClusterSSLCA) != 0 {
		tlsConfig, err := cfg.Security.ToTLSConfig()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		scheme = "https://"
		client.Transport = &http.Transport{
			TLSClientConfig:   tlsConfig,
			ForceAttemptHTTP2: true,
		}
	}
	serviceAddr := fmt.Sprintf("%s%s/txn_chunk", scheme, cfg.TiKVClient.TxnChunkWriterAddr)
	return &chunkWriterClient{client, serviceAddr}, nil
}
