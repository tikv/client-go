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
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/resourcecontrol"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	// BuildTxnFileMaxBackoff is max sleep time (in millisecond) to build TxnFile.
	BuildTxnFileMaxBackoff = atomicutil.NewUint64(60000)

	buildChunkErrMsg string = "txn file: build chunk failed"
)

const (
	PreSplitRegionChunks = 4

	// MaxTxnChunkSizeInParallel is the max parallel size when prewrite/commit txn chunks.
	MaxTxnChunkSizeInParallel uint64 = 4 << 30 // 4GB
)

type txnFileCtx struct {
	slice txnChunkSlice
}

type chunkBatch struct {
	txnChunkSlice
	region     *locate.KeyLocation
	sampleKeys [][]byte
	isPrimary  bool
}

func (b chunkBatch) String() string {
	return fmt.Sprintf("chunkBatch{region: %d, isPrimary: %t, txnChunkSlice: %v}",
		b.region.Region.GetID(), b.isPrimary, b.txnChunkSlice.chunkIDs)
}

func (b *chunkBatch) getSampleKeys() [][]byte {
	return b.sampleKeys
}

// txnChunkSlice should be sorted by txnChunkRange.smallest and no overlapping.
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
	cs.chunkIDs = append(cs.chunkIDs, other.chunkIDs...)
	cs.chunkRanges = append(cs.chunkRanges, other.chunkRanges...)
}

func (cs *txnChunkSlice) append(chunkID uint64, chunkRange txnChunkRange) {
	cs.chunkIDs = append(cs.chunkIDs, chunkID)
	cs.chunkRanges = append(cs.chunkRanges, chunkRange)
}

func (cs *txnChunkSlice) Len() int {
	return len(cs.chunkIDs)
}

func (cs *txnChunkSlice) Swap(i, j int) {
	cs.chunkIDs[i], cs.chunkIDs[j] = cs.chunkIDs[j], cs.chunkIDs[i]
	cs.chunkRanges[i], cs.chunkRanges[j] = cs.chunkRanges[j], cs.chunkRanges[i]
}

func (cs *txnChunkSlice) Less(i, j int) bool {
	return bytes.Compare(cs.chunkRanges[i].smallest, cs.chunkRanges[j].smallest) < 0
}

func (cs *txnChunkSlice) sortAndDedup() {
	if len(cs.chunkIDs) <= 1 {
		return
	}

	sort.Sort(cs)

	newIDs := cs.chunkIDs[:1]
	newRanges := cs.chunkRanges[:1]
	for i := 1; i < len(cs.chunkIDs); i++ {
		if cs.chunkIDs[i] != newIDs[len(newIDs)-1] {
			newIDs = append(newIDs, cs.chunkIDs[i])
			newRanges = append(newRanges, cs.chunkRanges[i])
		}
	}
	cs.chunkIDs = newIDs
	cs.chunkRanges = newRanges
}

// []chunkBatch is sorted by region.StartKey.
// Note: regions may be overlapping.
func (cs *txnChunkSlice) groupToBatches(c *locate.RegionCache, bo *retry.Backoffer, mutations CommitterMutations) ([]chunkBatch, error) {
	// Do not use `locate.RegionVerID` as map key to avoid grouping chunks to different batches when `confVer` changes.
	type batchMapKey struct {
		regionID  uint64
		regionVer uint64
	}
	batchMap := make(map[batchMapKey]*chunkBatch)
	for i, chunkRange := range cs.chunkRanges {
		chunkID := cs.chunkIDs[i]

		regions, firstKeys, err := chunkRange.getOverlapRegions(c, bo, mutations)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		for j, r := range regions {
			firstKey := firstKeys[j]

			bk := batchMapKey{regionID: r.Region.GetID(), regionVer: r.Region.GetVer()}
			if batchMap[bk] == nil {
				batchMap[bk] = &chunkBatch{
					region:     r,
					sampleKeys: make([][]byte, 0, 1),
				}
			}

			batch := batchMap[bk]
			batch.append(chunkID, chunkRange)
			if len(firstKey) > 0 {
				batch.sampleKeys = append(batch.sampleKeys, firstKey)
			}
		}
	}

	batches := make([]chunkBatch, 0, len(batchMap))
	for _, batch := range batchMap {
		batches = append(batches, *batch)
	}
	sort.Slice(batches, func(i, j int) bool {
		// Sort by chunks first, and then by region, to make sure that primary key is in the first batch:
		// 1. Different batches may contain the same chunks.
		// 2. Different batches may have regions with same start key (if region merge happens during grouping).
		// 3. Bigger batches may have regions with smaller start key (if region merge happens during grouping).
		cmp := bytes.Compare(batches[i].Smallest(), batches[j].Smallest())
		if cmp == 0 {
			return bytes.Compare(batches[i].region.StartKey, batches[j].region.StartKey) < 0
		}
		return cmp < 0
	})

	logutil.Logger(bo.GetCtx()).Debug("txn file group to batches", zap.Stringers("batches", batches))
	return batches, nil
}

type txnChunkRange struct {
	smallest []byte
	biggest  []byte
}

func (r txnChunkRange) String() string {
	return fmt.Sprintf("txnChunkRange[%s,%s]", kv.StrKey(r.smallest), kv.StrKey(r.biggest))
}

func newTxnChunkRange(smallest []byte, biggest []byte) txnChunkRange {
	return txnChunkRange{
		smallest: smallest,
		biggest:  biggest,
	}
}

func (r *txnChunkRange) getOverlapRegions(c *locate.RegionCache, bo *retry.Backoffer, mutations CommitterMutations) ([]*locate.KeyLocation, [][]byte, error) {
	regions := make([]*locate.KeyLocation, 0)
	firstKeys := make([][]byte, 0)
	startKey := r.smallest
	exclusiveBiggest := kv.NextKey(r.biggest)
	for bytes.Compare(startKey, r.biggest) <= 0 {
		loc, err := c.LocateKey(bo, startKey)
		if err != nil {
			logutil.Logger(bo.GetCtx()).Error("locate key failed", zap.Error(err), zap.String("startKey", kv.StrKey(startKey)))
			return nil, nil, errors.Wrap(err, "locate key failed")
		}
		firstKey, ok := MutationsHasDataInRange(
			mutations,
			util.GetMaxStartKey(r.smallest, loc.StartKey),
			util.GetMinEndKey(exclusiveBiggest, loc.EndKey),
		)
		if ok {
			regions = append(regions, loc)
			firstKeys = append(firstKeys, firstKey)
		}
		if len(loc.EndKey) == 0 {
			break
		}
		startKey = loc.EndKey
	}
	return regions, firstKeys, nil
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
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), c.store.GetOracle())
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

type txnFileCommitAction struct{}

var _ txnFileAction = (*txnFileCommitAction)(nil)

func (a txnFileCommitAction) executeBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch chunkBatch) (*tikvrpc.Response, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
		Keys:          batch.getSampleKeys(), // To help detect duplicated request.
		StartVersion:  c.startTS,
		CommitVersion: c.commitTS,
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
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), c.store.GetOracle())
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
	req := tikvrpc.NewRequest(tikvrpc.CmdBatchRollback, &kvrpcpb.BatchRollbackRequest{
		Keys:         batch.getSampleKeys(), // To help detect duplicated request.
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
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), c.store.GetOracle())
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

type step struct {
	name string
	dur  time.Duration
}

func (s step) String() string {
	return fmt.Sprintf("%s:%s", s.name, s.dur.String())
}

func (c *twoPhaseCommitter) executeTxnFile(ctx context.Context) (err error) {
	if val, err := util.EvalFailpoint("injectErrorOnExecTxnFile"); err == nil {
		errVal := val.(string)
		if errVal == "writeConflict" {
			err = tikverr.NewErrWriteConflictWithArgs(
				c.startTS,
				c.startTS+1,
				0,
				c.primaryKey,
				kvrpcpb.WriteConflict_Optimistic,
			)
		} else {
			err = errors.New("injected error in executeTxnFile: " + errVal)
		}
		return err
	}

	start := time.Now()
	steps := make([]step, 0)
	stepDone := func(name string) {
		now := time.Now()
		s := step{name: name, dur: now.Sub(start)}
		steps = append(steps, s)
		start = now
	}

	defer func() {
		// Always clean up all written keys if the txn does not commit.
		c.mu.RLock()
		committed := c.mu.committed
		undetermined := c.mu.undeterminedErr != nil
		c.mu.RUnlock()
		if !committed && !undetermined {
			if c.txnFileCtx.slice.Len() > 0 {
				err1 := c.executeTxnFileAction(retry.NewBackofferWithVars(ctx, int(CommitMaxBackoff), c.txn.vars), c.txnFileCtx.slice, txnFileRollbackAction{})
				if err1 != nil {
					logutil.Logger(ctx).Error("txn file: rollback on error failed", zap.Error(err1))
				}
			}
			c.reportFailureMetrics()
		} else {
			c.reportSuccessMetrics(steps)
		}
		c.txn.commitTS = c.commitTS

		logutil.Logger(ctx).Info("execute txn file finished",
			zap.Uint64("startTS", c.startTS),
			zap.Uint64("commitTS", c.commitTS),
			zap.Error(err),
			zap.String("requestSource", c.txn.GetRequestSource()),
			zap.Stringers("steps", steps))
	}()

	logutil.Logger(ctx).Debug("execute txn file", zap.Uint64("startTS", c.startTS))

	buildBo := retry.NewBackofferWithVars(ctx, int(BuildTxnFileMaxBackoff.Load()), c.txn.vars)

	rcInterceptor := client.ResourceControlInterceptor.Load()
	var ruDetails *util.RUDetails
	if detail := ctx.Value(util.RUDetailsCtxKey); detail != nil {
		ruDetails = detail.(*util.RUDetails)
	}
	reqInfo, err := c.beforeExecuteTxnFile(buildBo, rcInterceptor, ruDetails)
	if err != nil {
		return
	}

	err = c.buildTxnFiles(buildBo, c.mutations)
	stepDone("build")
	if err != nil {
		return
	}

	err = c.preSplitTxnFileRegions(buildBo)
	stepDone("pre-split")
	if err != nil {
		return
	}

	prewriteBo := retry.NewBackofferWithVars(ctx, int(PrewriteMaxBackoff.Load()), c.txn.vars)
	err = c.executeTxnFileAction(prewriteBo, c.txnFileCtx.slice, txnFilePrewriteAction{})
	stepDone("prewrite")
	if err != nil {
		return
	}

	commitBo := retry.NewBackofferWithVars(ctx, int(CommitMaxBackoff), c.txn.vars)
	c.commitTS, err = c.store.GetTimestampWithRetry(commitBo, c.txn.GetScope())
	if err != nil {
		return
	}
	err = c.executeTxnFileAction(commitBo, c.txnFileCtx.slice, txnFileCommitAction{})
	stepDone("commit")
	if err != nil {
		return
	}

	err = c.afterExecuteTxnFile(rcInterceptor, reqInfo, ruDetails)
	return
}

func (c *twoPhaseCommitter) executeTxnFileSlice(bo *retry.Backoffer, chunkSlice txnChunkSlice, batches []chunkBatch, action txnFileAction) (txnChunkSlice, error) {
	var err error
	var regionErrChunks txnChunkSlice

	chunksCount := chunkSlice.Len()
	if batches == nil {
		batches, err = chunkSlice.groupToBatches(c.store.GetRegionCache(), bo, c.mutations)
		if err != nil {
			return regionErrChunks, errors.Wrap(err, "txn file: group to batches failed")
		}
	}

	if len(batches) == 1 {
		regionErrSlice, err := c.executeTxnFileSliceSingleBatch(bo, batches[0], action)
		if err != nil {
			return regionErrChunks, err
		} else if regionErrSlice != nil {
			regionErrChunks.appendSlice(regionErrSlice)
		}
		return regionErrChunks, nil
	}

	type result struct {
		regionErrSlice *txnChunkSlice
		err            error
	}
	ch := make(chan result, len(batches))

	exitCh := make(chan struct{})
	defer close(exitCh)

	bo, cancel := bo.Fork()
	defer cancel()

	rateLim := len(batches)
	cnf := config.GetGlobalConfig()
	if rateLim > cnf.CommitterConcurrency {
		rateLim = cnf.CommitterConcurrency
	}
	maxChunksInParallel := int(MaxTxnChunkSizeInParallel / cnf.TiKVClient.TxnChunkMaxSize) // 32 by default
	if chunksCount > maxChunksInParallel {
		rateLim = maxChunksInParallel
	}
	rateLimiter := util.NewRateLimit(rateLim)

	for _, batch := range batches {
		batch := batch
		bo := bo.Clone()
		if exit := rateLimiter.GetToken(exitCh); !exit {
			go func() {
				defer rateLimiter.PutToken()
				regionErrSlice, err := c.executeTxnFileSliceSingleBatch(bo, batch, action)
				ch <- result{regionErrSlice, err}
			}()
		}
	}

	for i := 0; i < len(batches); i++ {
		r := <-ch
		if r.err != nil {
			return regionErrChunks, r.err
		} else if r.regionErrSlice != nil {
			regionErrChunks.appendSlice(r.regionErrSlice)
		}
	}
	regionErrChunks.sortAndDedup()
	return regionErrChunks, nil
}

func (c *twoPhaseCommitter) executeTxnFileSliceSingleBatch(bo *retry.Backoffer, batch chunkBatch, action txnFileAction) (*txnChunkSlice, error) {
	resp, err1 := action.executeBatch(c, bo, batch)
	logutil.Logger(bo.GetCtx()).Debug("txn file: execute batch finished",
		zap.Uint64("startTS", c.startTS),
		zap.Any("batch", batch),
		zap.Stringer("action", action),
		zap.Error(err1))
	if err1 != nil {
		return nil, err1
	}
	if keyErr := action.extractKeyError(resp); keyErr != nil {
		if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
			e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
			return nil, c.extractKeyExistsErr(e)
		}
		lock, err2 := txnlock.ExtractLockFromKeyErr(keyErr)
		if err2 != nil {
			return nil, err2
		}
		if lock.TxnID > c.startTS {
			return nil, tikverr.NewErrWriteConflictWithArgs(
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
		return nil, err1
	}
	if regionErr != nil {
		logutil.Logger(bo.GetCtx()).Debug("txn file: execute batch failed, region error",
			zap.Uint64("startTS", c.startTS),
			zap.Stringer("action", action),
			zap.Any("batch", batch),
			zap.Stringer("regionErr", regionErr))
		return &batch.txnChunkSlice, nil
	}
	return nil, nil
}

func (c *twoPhaseCommitter) executeTxnFileSliceWithRetry(bo *retry.Backoffer, chunkSlice txnChunkSlice, batches []chunkBatch, action txnFileAction) error {
	currentChunks := chunkSlice
	currentBatches := batches
	for {
		var regionErrChunks txnChunkSlice
		regionErrChunks, err := c.executeTxnFileSlice(bo, currentChunks, currentBatches, action)
		if err != nil {
			return errors.WithStack(err)
		}
		if regionErrChunks.Len() == 0 {
			return nil
		}
		logutil.Logger(bo.GetCtx()).Debug("txn file meet region errors", zap.Stringer("regionErrChunks", regionErrChunks))
		currentChunks = regionErrChunks
		currentBatches = nil
		err = bo.Backoff(retry.BoRegionMiss, errors.Errorf("txn file: execute failed, region miss"))
		if err != nil {
			return errors.WithStack(err)
		}
	}
}

func (c *twoPhaseCommitter) executeTxnFilePrimaryBatch(bo *retry.Backoffer, firstBatch chunkBatch, action txnFileAction) (regionErr *errorpb.Error, err error) {
	if !firstBatch.region.Contains(c.primary()) {
		logutil.Logger(bo.GetCtx()).Error("txn file: primary out of first batch",
			zap.Uint64("startTS", c.startTS),
			zap.String("primary", kv.StrKey(c.primary())),
			zap.Stringer("action", action),
			zap.Stringer("first batch", firstBatch))
		return nil, fmt.Errorf("txn file: primary out of first batch")
	}

	firstBatch.isPrimary = true
	resp, err := action.executeBatch(c, bo, firstBatch)
	logutil.Logger(bo.GetCtx()).Debug("txn file: execute primary batch finished",
		zap.Uint64("startTS", c.startTS),
		zap.String("primary", kv.StrKey(c.primary())),
		zap.Stringer("action", action),
		zap.Stringer("batch", firstBatch),
		zap.Error(err))
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
		batches, err := chunkSlice.groupToBatches(c.store.GetRegionCache(), bo, c.mutations)
		if err != nil {
			return errors.Wrap(err, "txn file: group to batches failed")
		}

		regionErr, err := c.executeTxnFilePrimaryBatch(bo, batches[0], action)
		if err != nil {
			return errors.WithStack(err)
		}
		if regionErr != nil {
			errBo := bo.Backoff(retry.BoRegionMiss, errors.Wrap(errors.New(regionErr.String()), "txn file: execute primary batch failed"))
			if errBo != nil {
				return errors.WithStack(errBo)
			}
			continue
		}

		secondaries := batches[1:]
		if len(secondaries) == 0 {
			return nil
		}
		if !action.asyncExecuteSecondaries() {
			return c.executeTxnFileSliceWithRetry(bo, chunkSlice, secondaries, action)
		}

		c.store.WaitGroup().Add(1)
		errGo := c.store.Go(func() {
			defer c.store.WaitGroup().Done()
			err := c.executeTxnFileSliceWithRetry(bo, chunkSlice, secondaries, action)
			logutil.Logger(bo.GetCtx()).Debug("txn file: async execute secondaries finished",
				zap.Uint64("startTS", c.startTS),
				zap.Stringer("action", action),
				zap.Error(err))
			if err != nil {
				logutil.Logger(bo.GetCtx()).Warn("txn file: async execute secondaries failed",
					zap.Uint64("startTS", c.startTS), zap.Stringer("action", action), zap.Error(err))
			}
		})
		if errGo != nil {
			c.store.WaitGroup().Done()
			logutil.Logger(bo.GetCtx()).Warn("txn file: create goroutine failed",
				zap.Uint64("startTS", c.startTS),
				zap.Stringer("action", action),
				zap.Error(errGo))
		}
		return nil
	}
}

func (c *twoPhaseCommitter) buildTxnFiles(bo *retry.Backoffer, mutations CommitterMutations) error {
	bo, cancel := bo.Fork()
	defer cancel()

	cfg := config.GetGlobalConfig()
	maxTxnChunkSize := int(cfg.TiKVClient.TxnChunkMaxSize)
	capacity := c.txn.Size() + c.txn.Len()*7 + 4
	totalChunks := (capacity + maxTxnChunkSize - 1) / maxTxnChunkSize
	if capacity > maxTxnChunkSize {
		capacity = maxTxnChunkSize
	}
	concurrency := int(cfg.TiKVClient.TxnChunkWriterConcurrency)
	if concurrency > totalChunks {
		concurrency = totalChunks
	}

	writer, err := newChunkWriterClient(c.getKeyspaceID())
	if err != nil {
		return errors.Wrap(err, "new chunk writer client failed")
	}

	results := make([]buildChunkResult, 0, totalChunks)
	resultCh := make(chan buildChunkResult, concurrency)

	totalSize := 0
	inflightChunks := 0
	buf := make([]byte, 0, capacity)
	chunkSmallest := mutations.GetKey(0)
	for i := 0; i < mutations.Len(); i++ {
		key := mutations.GetKey(i)
		op := mutations.GetOp(i)
		val := mutations.GetValue(i)
		entrySize := 2 + len(key) + 1 + 4 + len(val)
		if len(buf) > 0 && len(buf)+entrySize+4 > cap(buf) {
			totalSize += len(buf)
			inflightChunks += 1
			ran := newTxnChunkRange(chunkSmallest, mutations.GetKey(i-1))
			writer.asyncBuildChunk(bo, buf, ran, resultCh)
			chunkSmallest = key
			buf = make([]byte, 0, capacity)

			if inflightChunks >= concurrency {
				r := <-resultCh
				if r.err != nil {
					logutil.Logger(bo.GetCtx()).Error(buildChunkErrMsg, zap.Error(err))
					return errors.Wrap(r.err, buildChunkErrMsg)
				}
				results = append(results, r)
				inflightChunks -= 1
			}
		}
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		buf = append(buf, byte(op))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(val)))
		buf = append(buf, val...)
	}
	if len(buf) > 0 {
		totalSize += len(buf)
		inflightChunks += 1
		ran := newTxnChunkRange(chunkSmallest, mutations.GetKey(mutations.Len()-1))
		writer.asyncBuildChunk(bo, buf, ran, resultCh)
	}

	for i := 0; i < inflightChunks; i++ {
		r := <-resultCh
		if r.err != nil {
			logutil.Logger(bo.GetCtx()).Error(buildChunkErrMsg, zap.Error(err))
			return errors.Wrap(r.err, buildChunkErrMsg)
		}
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool {
		return bytes.Compare(results[i].chunkRange.smallest, results[j].chunkRange.smallest) < 0
	})
	for _, r := range results {
		c.txnFileCtx.slice.append(r.chunkId, r.chunkRange)
	}

	logutil.Logger(bo.GetCtx()).Info("build txn files",
		zap.Uint64("startTS", c.startTS),
		zap.Int("mutationsLen", mutations.Len()),
		zap.Int("totalChunksSize", totalSize),
		zap.Any("chunkIDs", c.txnFileCtx.slice.chunkIDs))
	return nil
}

func (c *twoPhaseCommitter) getKeyspaceID() apicodec.KeyspaceID {
	return c.store.GetRegionCache().Codec().GetKeyspaceID()
}

func (c *twoPhaseCommitter) useTxnFile(ctx context.Context) (bool, error) {
	if c.txn == nil || c.txn.vars.DisableTxnFile {
		return false, nil
	}
	conf := config.GetGlobalConfig()
	minMutationSize := c.txn.vars.TxnFileMinMutationSize
	if minMutationSize == 0 {
		minMutationSize = conf.TiKVClient.TxnFileMinMutationSize
	}
	if c.txn.isInternal() {
		// Relax the requirement for internal requests.
		minMutationSize = minMutationSize / 2
	}

	if c.txn.isPessimistic ||
		len(conf.TiKVClient.TxnChunkWriterAddr) == 0 ||
		uint64(c.txn.GetMemBuffer().Size()) < minMutationSize ||
		!IsRequestSourceUseTxnFile(c.txn.RequestSource, conf) {
		return false, nil
	}

	logutil.Logger(ctx).Debug("transaction use txn file",
		zap.Uint64("startTS", c.startTS),
		zap.Int("size", c.txn.GetMemBuffer().Size()),
		zap.Int("len", c.mutations.Len()),
		zap.String("reqSource", c.txn.GetRequestSource()),
	)
	return true, nil
}

// IsRequestSourceUseTxnFile checks if the request source is allowed to use file-based txn on the configuration.
func IsRequestSourceUseTxnFile(reqSource *util.RequestSource, conf *config.Config) bool {
	if reqSource.RequestSourceInternal {
		return slices.Contains(
			conf.TiKVClient.TxnFileRequestSourceWhitelist,
			reqSource.RequestSourceType,
		)
	}
	return true
}

func (c *twoPhaseCommitter) preSplitTxnFileRegions(bo *retry.Backoffer) error {
	batches, err := c.txnFileCtx.slice.groupToBatches(c.store.GetRegionCache(), bo, c.mutations)
	if err != nil {
		return errors.Wrap(err, "group to batches failed")
	}
	var splitKeys [][]byte
	for _, batch := range batches {
		if batch.Len() > PreSplitRegionChunks {
			for i := PreSplitRegionChunks; i < batch.Len(); i += PreSplitRegionChunks {
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

func (c *twoPhaseCommitter) beforeExecuteTxnFile(
	bo *retry.Backoffer,
	rcInterceptor *resourceControlClient.ResourceGroupKVInterceptor,
	ruDetails *util.RUDetails,
) (*resourcecontrol.RequestInfo, error) {
	if rcInterceptor == nil {
		return nil, nil
	}

	ctx := bo.GetCtx()
	regionCache := c.store.GetRegionCache()

	var region *locate.Region
	for {
		loc, err := regionCache.LocateKey(bo, c.primary())
		if err != nil {
			return nil, errors.Wrap(err, "failed to locate primary")
		}

		region = regionCache.GetCachedRegionWithRLock(loc.Region)
		if region != nil {
			break
		}

		err = bo.Backoff(retry.BoRegionMiss, errors.New("cached region not found"))
		if err != nil {
			logutil.Logger(ctx).Error("txn file: cached region not found",
				zap.String("key", kv.StrKey(c.primary())),
				zap.Stringer("loc", loc))
			return nil, errors.WithStack(err)
		}
	}

	var replicaNumber int64 = 0
	for _, peer := range region.GetMeta().GetPeers() {
		if peer.GetRole() == metapb.PeerRole_Voter {
			replicaNumber++
		}
	}

	writeBytes := int64(c.txn.Size())
	discountRatio := config.GetGlobalConfig().TiKVClient.TxnFileRUDiscountRatio
	if discountRatio > 0.0 && discountRatio < 1.0 {
		writeBytes = int64(float64(writeBytes) * discountRatio)
	}

	reqInfo := resourcecontrol.NewRequestInfo(
		writeBytes,
		region.GetLeaderStoreID(),
		replicaNumber,
		false,
	)

	consumption, _ /* penalty */, waitDuration, _ /* priority */, err := (*rcInterceptor).OnRequestWait(ctx, c.resourceGroupName, reqInfo)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if ruDetails != nil {
		ruDetails.Update(consumption, waitDuration)
	}

	return reqInfo, nil
}

func (c *twoPhaseCommitter) afterExecuteTxnFile(rcInterceptor *resourceControlClient.ResourceGroupKVInterceptor, reqInfo *resourcecontrol.RequestInfo, ruDetails *util.RUDetails) error {
	if rcInterceptor == nil {
		return nil
	}

	respInfo := &resourcecontrol.ResponseInfo{}
	consumption, err := (*rcInterceptor).OnResponse(c.resourceGroupName, reqInfo, respInfo)
	if err != nil {
		return errors.WithStack(err)
	}
	if ruDetails != nil {
		ruDetails.Update(consumption, time.Duration(0))
	}

	return nil
}

func (c *twoPhaseCommitter) reportSuccessMetrics(steps []step) {
	metrics.TwoPCTxnCounterOk.Inc()
	metrics.TxnFileRequestsOk.Inc()

	mutationBytes := c.txn.GetMemBuffer().Size()
	var dur time.Duration
	for _, step := range steps {
		dur += step.dur
	}

	if c.txn.isInternal() {
		metrics.TxnFileWriteBytesInternal.Add(float64(mutationBytes))
		metrics.TxnFileMutationSizeInternal.Observe(float64(mutationBytes))
		metrics.TxnFileDurationInternal.Observe(dur.Seconds())
	} else {
		metrics.TxnFileWriteBytesGeneral.Add(float64(mutationBytes))
		metrics.TxnFileMutationSizeGeneral.Observe(float64(mutationBytes))
		metrics.TxnFileDurationGeneral.Observe(dur.Seconds())
	}
}

func (c *twoPhaseCommitter) reportFailureMetrics() {
	metrics.TwoPCTxnCounterError.Inc()
	metrics.TxnFileRequestsError.Inc()
}

var (
	once sync.Once
	cli  *http.Client
)

func getHTTPClient() *http.Client {
	once.Do(func() {
		timeout := time.Duration(BuildTxnFileMaxBackoff.Load()) * time.Millisecond
		cli = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
			},
		}
	})
	return cli
}

type chunkWriterClient struct {
	cli         *http.Client
	serviceAddr string
}

func newChunkWriterClient(keyspaceID apicodec.KeyspaceID) (*chunkWriterClient, error) {
	var (
		cfg    = config.GetGlobalConfig()
		scheme = "http://"
		client = getHTTPClient()
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
	serviceAddr := fmt.Sprintf("%s%s/txn_chunk?keyspace_id=%v", scheme, cfg.TiKVClient.TxnChunkWriterAddr, keyspaceID)
	return &chunkWriterClient{client, serviceAddr}, nil
}

func (w *chunkWriterClient) buildChunk(bo *retry.Backoffer, buf []byte) (uint64, error) {
	hash := crc32.New(crc32.MakeTable(crc32.IEEE))
	hash.Write(buf)
	crc := hash.Sum32()
	buf = binary.LittleEndian.AppendUint32(buf, crc)

	data, err := w.request(bo, "POST", buf)
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

func (w *chunkWriterClient) request(bo *retry.Backoffer, method string, data []byte) ([]byte, error) {
	ctx := bo.GetCtx()
	for {
		if ctx.Err() != nil {
			return nil, errors.WithStack(ctx.Err())
		}

		var body io.Reader
		if len(data) > 0 {
			body = bytes.NewReader(data)
		}

		req, err := http.NewRequestWithContext(ctx, method, w.serviceAddr, body)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/octet-stream")
		}

		resp, err := w.cli.Do(req)
		if err != nil {
			logutil.Logger(ctx).Warn("request failed", zap.Error(err), zap.String("addr", w.serviceAddr))
			err = bo.Backoff(retry.BoTiKVRPC, errors.WithMessage(err, "request failed"))
			if err != nil {
				return nil, errors.WithStack(err)
			}
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			var bodyStr string
			if data, err := io.ReadAll(resp.Body); err == nil {
				bodyStr = string(data)
			}
			logutil.Logger(ctx).Warn("service error", zap.String("http status", resp.Status), zap.String("body", bodyStr))
			err = bo.Backoff(retry.BoTiKVServerBusy, fmt.Errorf("service error, http status %s", resp.Status))
			if err != nil {
				return nil, errors.WithStack(err)
			}
			continue
		}
		data, err := io.ReadAll(resp.Body)
		return data, errors.WithStack(err)
	}
}

type buildChunkResult struct {
	chunkId    uint64
	chunkRange txnChunkRange
	err        error
}

func (w *chunkWriterClient) asyncBuildChunk(bo *retry.Backoffer, buf []byte, chunkRange txnChunkRange, resultCh chan<- buildChunkResult) {
	go func() {
		chunkId, err := w.buildChunk(bo.Clone(), buf)
		resultCh <- buildChunkResult{chunkId, chunkRange, err}
	}()
}
