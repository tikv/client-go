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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/prewrite.go
//

// Copyright 2020 PingCAP, Inc.
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
	"encoding/hex"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

type actionPrewrite struct {
	retry         bool
	isInternal    bool
	hasRpcRetries bool // RPC tried more than once => we need to set is_retry_request
}

var _ twoPhaseCommitAction = actionPrewrite{}

func (action actionPrewrite) String() string {
	return "prewrite"
}

func (action actionPrewrite) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	if action.isInternal {
		return metrics.TxnRegionsNumHistogramPrewriteInternal
	}
	return metrics.TxnRegionsNumHistogramPrewrite
}

func (c *twoPhaseCommitter) buildPrewriteRequest(batch batchMutations, txnSize uint64) *tikvrpc.Request {
	m := batch.mutations
	mutations := make([]*kvrpcpb.Mutation, m.Len())
	pessimisticActions := make([]kvrpcpb.PrewriteRequest_PessimisticAction, m.Len())
	var forUpdateTSConstraints []*kvrpcpb.PrewriteRequest_ForUpdateTSConstraint

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
		if m.IsPessimisticLock(i) {
			pessimisticActions[i] = kvrpcpb.PrewriteRequest_DO_PESSIMISTIC_CHECK
		} else if m.NeedConstraintCheckInPrewrite(i) {
			pessimisticActions[i] = kvrpcpb.PrewriteRequest_DO_CONSTRAINT_CHECK
		} else {
			pessimisticActions[i] = kvrpcpb.PrewriteRequest_SKIP_PESSIMISTIC_CHECK
		}

		if c.forUpdateTSConstraints != nil {
			if actualLockForUpdateTS, ok := c.forUpdateTSConstraints[string(mutations[i].Key)]; ok {
				forUpdateTSConstraints = append(forUpdateTSConstraints, &kvrpcpb.PrewriteRequest_ForUpdateTSConstraint{
					Index:               uint32(i),
					ExpectedForUpdateTs: actualLockForUpdateTS,
				})
			}
		}
	}
	c.mu.Lock()
	minCommitTS := c.minCommitTSMgr.get()
	c.mu.Unlock()
	if c.forUpdateTS > 0 && c.forUpdateTS >= minCommitTS {
		minCommitTS = c.forUpdateTS + 1
	} else if c.startTS >= minCommitTS {
		minCommitTS = c.startTS + 1
	}

	if val, err := util.EvalFailpoint("mockZeroCommitTS"); err == nil {
		// Should be val.(uint64) but failpoint doesn't support that.
		if tmp, ok := val.(int); ok && uint64(tmp) == c.startTS {
			minCommitTS = 0
		}
	}

	ttl := c.lockTTL

	// ref: https://github.com/pingcap/tidb/issues/33641
	// we make the TTL satisfy the following condition:
	// 	max_commit_ts.physical < start_ts.physical + TTL < (current_ts of some check_txn_status that wants to resolve this lock).physical
	// and we have:
	//  current_ts <= max_ts <= min_commit_ts of this lock
	// such that
	// 	max_commit_ts < min_commit_ts, so if this lock is resolved, it will be forced to fall back to normal 2PC, thus resolving the issue.
	if c.isAsyncCommit() && c.isPessimistic {
		safeTTL := uint64(oracle.ExtractPhysical(c.maxCommitTS)-oracle.ExtractPhysical(c.startTS)) + 1
		if safeTTL > ttl {
			ttl = safeTTL
		}
	}

	if c.sessionID > 0 {
		if _, err := util.EvalFailpoint("twoPCShortLockTTL"); err == nil {
			ttl = 1
			keys := make([]string, 0, len(mutations))
			for _, m := range mutations {
				keys = append(keys, hex.EncodeToString(m.Key))
			}
			logutil.BgLogger().Info(
				"[failpoint] injected lock ttl = 1 on prewrite",
				zap.Uint64("txnStartTS", c.startTS), zap.Strings("keys", keys),
			)
		}
	}

	assertionLevel := c.txn.assertionLevel
	if _, err := util.EvalFailpoint("assertionSkipCheckFromPrewrite"); err == nil {
		assertionLevel = kvrpcpb.AssertionLevel_Off
	}

	req := &kvrpcpb.PrewriteRequest{
		Mutations:              mutations,
		PrimaryLock:            c.primary(),
		StartVersion:           c.startTS,
		LockTtl:                ttl,
		PessimisticActions:     pessimisticActions,
		ForUpdateTs:            c.forUpdateTS,
		TxnSize:                txnSize,
		MinCommitTs:            minCommitTS,
		MaxCommitTs:            c.maxCommitTS,
		AssertionLevel:         assertionLevel,
		ForUpdateTsConstraints: forUpdateTSConstraints,
	}

	if _, err := util.EvalFailpoint("invalidMaxCommitTS"); err == nil {
		if req.MaxCommitTs > 0 {
			req.MaxCommitTs = minCommitTS - 1
		}
	}

	if c.isAsyncCommit() {
		if batch.isPrimary {
			req.Secondaries = c.asyncSecondaries()
		}
		req.UseAsyncCommit = true
	}

	if c.isOnePC() {
		req.TryOnePc = true
	}

	r := tikvrpc.NewRequest(
		tikvrpc.CmdPrewrite, req, kvrpcpb.Context{
			Priority:               c.priority,
			SyncLog:                c.syncLog,
			ResourceGroupTag:       c.resourceGroupTag,
			DiskFullOpt:            c.diskFullOpt,
			TxnSource:              c.txnSource,
			MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds()),
			RequestSource:          c.txn.GetRequestSource(),
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

func (action actionPrewrite) handleSingleBatch(
	c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations,
) (err error) {
	// WARNING: This function only tries to send a single request to a single region, so it don't
	// need to unset the `useOnePC` flag when it fails. A special case is that when TiKV returns
	// regionErr, it's uncertain if the request will be splitted into multiple and sent to multiple
	// regions. It invokes `prewriteMutations` recursively here, and the number of batches will be
	// checked there.

	if err = action.handleSingleBatchFailpoint(c, bo, batch); err != nil {
		return err
	}

	handler := action.newSingleBatchPrewriteReqHandler(c, batch, bo)

	var retryable bool
	for {
		// It will return false if the request is success or meet an unretryable error.
		// otherwise if the error is retryable, it will return true.
		retryable, err = handler.sendReqAndCheck()
		if !retryable {
			handler.drop(err)
			return err
		}
	}
}

// handleSingleBatchFailpoint is used to inject errors for test.
func (action actionPrewrite) handleSingleBatchFailpoint(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	if c.sessionID <= 0 {
		return nil
	}
	if batch.isPrimary {
		if _, err := util.EvalFailpoint("prewritePrimaryFail"); err == nil {
			// Delay to avoid cancelling other normally ongoing prewrite requests.
			time.Sleep(time.Millisecond * 50)
			logutil.Logger(bo.GetCtx()).Info(
				"[failpoint] injected error on prewriting primary batch",
				zap.Uint64("txnStartTS", c.startTS),
			)
			return errors.New("injected error on prewriting primary batch")
		}
		util.EvalFailpoint("prewritePrimary") // for other failures like sleep or pause
		return nil
	}

	if _, err := util.EvalFailpoint("prewriteSecondaryFail"); err == nil {
		// Delay to avoid cancelling other normally ongoing prewrite requests.
		time.Sleep(time.Millisecond * 50)
		logutil.Logger(bo.GetCtx()).Info(
			"[failpoint] injected error on prewriting secondary batch",
			zap.Uint64("txnStartTS", c.startTS),
		)
		return errors.New("injected error on prewriting secondary batch")
	}
	util.EvalFailpoint("prewriteSecondary") // for other failures like sleep or pause
	// concurrent failpoint sleep doesn't work as expected. So we need a separate fail point.
	// `1*sleep()` can block multiple concurrent threads that meet the failpoint.
	if val, err := util.EvalFailpoint("prewriteSecondarySleep"); err == nil {
		time.Sleep(time.Millisecond * time.Duration(val.(int)))
	}
	return nil
}

func (c *twoPhaseCommitter) prewriteMutations(bo *retry.Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.prewriteMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	// `doActionOnMutations` will unset `useOnePC` if the mutations is splitted into multiple batches.
	return c.doActionOnMutations(bo, actionPrewrite{isInternal: c.txn.isInternal()}, mutations)
}

// prewrite1BatchReqHandler is used to handle 1 singleBatch prewrite request.
type prewrite1BatchReqHandler struct {
	committer            *twoPhaseCommitter
	action               *actionPrewrite
	req                  *tikvrpc.Request
	batch                batchMutations
	bo                   *retry.Backoffer
	sender               *locate.RegionRequestSender
	resolvingRecordToken *int
	attempts             int
	// begin is the time when the first request is sent,
	// it will be reset once the total duration exceeds the slowRequestThreshold
	// It's used to log slow prewrite requests.
	begin time.Time
}

func (action actionPrewrite) newSingleBatchPrewriteReqHandler(c *twoPhaseCommitter, batch batchMutations, bo *retry.Backoffer) *prewrite1BatchReqHandler {
	txnSize := uint64(c.regionTxnSize[batch.region.GetID()])
	// When we retry because of a region miss, we don't know the transaction size. We set the transaction size here
	// to MaxUint64 to avoid unexpected "resolve lock lite".
	if action.retry {
		txnSize = math.MaxUint64
	}
	req := c.buildPrewriteRequest(batch, txnSize)
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), c.store.GetOracle())
	return &prewrite1BatchReqHandler{
		action:               &action,
		req:                  req,
		committer:            c,
		batch:                batch,
		bo:                   bo,
		sender:               sender,
		begin:                time.Now(),
		attempts:             0,
		resolvingRecordToken: nil,
	}
}

// drop is called when the prewrite request is finished. It checks the error and updates the commit details.
func (handler *prewrite1BatchReqHandler) drop(err error) {
	if err != nil {
		// If we fail to receive response for async commit prewrite, it will be undetermined whether this
		// transaction has been successfully committed.
		// If prewrite has been cancelled, all ongoing prewrite RPCs will become errors, we needn't set undetermined
		// errors.
		if (handler.committer.isAsyncCommit() || handler.committer.isOnePC()) && handler.sender.GetRPCError() != nil && atomic.LoadUint32(&handler.committer.prewriteCancelled) == 0 {
			handler.committer.setUndeterminedErr(handler.sender.GetRPCError())
		}
	}
	if handler.resolvingRecordToken != nil {
		handler.committer.store.GetLockResolver().ResolveLocksDone(handler.committer.startTS, *handler.resolvingRecordToken)
	}
}

func (handler *prewrite1BatchReqHandler) beforeSend(reqBegin time.Time) {
	handler.attempts++
	if handler.action.hasRpcRetries {
		handler.req.IsRetryRequest = true
	}
	if handler.attempts == 1 {
		return
	}
	if reqBegin.Sub(handler.begin) > slowRequestThreshold {
		logutil.BgLogger().Warn(
			"slow prewrite request",
			zap.Uint64("startTS", handler.committer.startTS),
			zap.Stringer("region", &handler.batch.region),
			zap.Int("attempts", handler.attempts),
		)
		handler.begin = reqBegin
	}
}

// sendAndCheckReq sends the prewrite request to the TiKV server and check the response.
// If the TiKV server returns a retryable error, the function returns true.
// If the TiKV server returns ok or a non-retryable error, the function returns false.
func (handler *prewrite1BatchReqHandler) sendReqAndCheck() (retryable bool, err error) {
	reqBegin := time.Now()
	handler.beforeSend(reqBegin)
	resp, retryTimes, err := handler.sender.SendReq(handler.bo, handler.req, handler.batch.region, client.ReadTimeoutShort)
	// Unexpected error occurs, return it directly.
	if err != nil {
		return false, err
	}
	if retryTimes > 0 {
		handler.action.hasRpcRetries = true
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return false, err
	}
	if regionErr != nil {
		return handler.handleRegionErr(regionErr)
	}

	if resp.Resp == nil {
		return false, errors.WithStack(tikverr.ErrBodyMissing)
	}
	prewriteResp := resp.Resp.(*kvrpcpb.PrewriteResponse)
	keyErrs := prewriteResp.GetErrors()
	if len(keyErrs) == 0 {
		return false, handler.handleSingleBatchSucceed(reqBegin, prewriteResp)
	}

	locks, e := handler.extractKeyErrs(keyErrs)
	if e != nil {
		return false, e
	}

	if err := handler.resolveLocks(locks); err != nil {
		return false, err
	}
	return true, nil

}

// handleRegionErr handles region errors when sending the prewrite request.
// If the region error is EpochNotMatch and the data is still in the same region, return with retrable true.
// Otherwise, the function returns with retryable false if the region error is not retryable:
//  1. The region epoch is changed and the data is not in the same region any more:
//     doActionOnMutations directly and return retryable false regardless of success or failure.
//  2. Other region errors.
func (handler *prewrite1BatchReqHandler) handleRegionErr(regionErr *errorpb.Error) (retryable bool, err error) {
	// For other region error and the fake region error, backoff because
	// there's something wrong.
	// For the real EpochNotMatch error, don't backoff.
	if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
		err := handler.bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return false, err
		}
	}
	if regionErr.GetDiskFull() != nil {
		storeIds := regionErr.GetDiskFull().GetStoreId()
		desc := " "
		for _, i := range storeIds {
			desc += strconv.FormatUint(i, 10) + " "
		}

		logutil.Logger(handler.bo.GetCtx()).Error(
			"Request failed cause of TiKV disk full",
			zap.String("store_id", desc),
			zap.String("reason", regionErr.GetDiskFull().GetReason()),
		)

		return false, errors.New(regionErr.String())
	}

	// for the real EpochNotMatch error, relocate the region.
	same, err := handler.batch.relocate(handler.bo, handler.committer.store.GetRegionCache())
	if err != nil {
		return false, err
	}
	if same {
		return true, nil
	}
	err = handler.committer.doActionOnMutations(handler.bo, actionPrewrite{true, handler.action.isInternal, handler.action.hasRpcRetries}, handler.batch.mutations)
	return false, err
}

// extractKeyErrs extracts locks from key errors.
func (handler *prewrite1BatchReqHandler) extractKeyErrs(keyErrs []*kvrpcpb.KeyError) ([]*txnlock.Lock, error) {
	var locks []*txnlock.Lock
	logged := make(map[uint64]struct{})
	for _, keyErr := range keyErrs {
		// Check already exists error
		if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
			e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
			return nil, handler.committer.extractKeyExistsErr(e)
		}

		// Extract lock from key error
		lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
		if err1 != nil {
			return nil, err1
		}
		if _, ok := logged[lock.TxnID]; !ok {
			logutil.BgLogger().Info(
				"prewrite encounters lock. "+
					"More locks belonging to the same transaction may be omitted",
				zap.Uint64("session", handler.committer.sessionID),
				zap.Uint64("txnID", handler.committer.startTS),
				zap.Stringer("lock", lock),
				zap.Stringer("policy", handler.committer.txn.prewriteEncounterLockPolicy),
			)
			logged[lock.TxnID] = struct{}{}
		}
		// If an optimistic transaction encounters a lock with larger TS, this transaction will certainly
		// fail due to a WriteConflict error. So we can construct and return an error here early.
		// Pessimistic transactions don't need such an optimization. If this key needs a pessimistic lock,
		// TiKV will return a PessimisticLockNotFound error directly if it encounters a different lock. Otherwise,
		// TiKV returns lock.TTL = 0, and we still need to resolve the lock.
		if (lock.TxnID > handler.committer.startTS && !handler.committer.isPessimistic) ||
			handler.committer.txn.prewriteEncounterLockPolicy == NoResolvePolicy {
			metrics.LockResolverCountWithWriteConflict.Inc()
			return nil, tikverr.NewErrWriteConflictWithArgs(
				handler.committer.startTS,
				lock.TxnID,
				0,
				lock.Key,
				kvrpcpb.WriteConflict_Optimistic,
			)
		}
		locks = append(locks, lock)
	}
	return locks, nil
}

// resolveLocks resolves locks.
func (handler *prewrite1BatchReqHandler) resolveLocks(locks []*txnlock.Lock) error {
	if handler.resolvingRecordToken == nil {
		token := handler.committer.store.GetLockResolver().RecordResolvingLocks(locks, handler.committer.startTS)
		handler.resolvingRecordToken = &token
	} else {
		handler.committer.store.GetLockResolver().UpdateResolvingLocks(locks, handler.committer.startTS, *handler.resolvingRecordToken)
	}

	resolveLockOpts := txnlock.ResolveLocksOptions{
		CallerStartTS:            handler.committer.startTS,
		Locks:                    locks,
		Detail:                   &handler.committer.getDetail().ResolveLock,
		PessimisticRegionResolve: true,
	}
	resolveLockRes, err := handler.committer.store.GetLockResolver().ResolveLocksWithOpts(handler.bo, resolveLockOpts)
	if err != nil {
		return err
	}
	msBeforeExpired := resolveLockRes.TTL
	if msBeforeExpired > 0 {
		err = handler.bo.BackoffWithCfgAndMaxSleep(
			retry.BoTxnLock,
			int(msBeforeExpired),
			errors.Errorf("2PC prewrite lockedKeys: %d", len(locks)),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// handleSingleBatchSucceed handles the response when the prewrite request is successful.
func (handler *prewrite1BatchReqHandler) handleSingleBatchSucceed(reqBegin time.Time, prewriteResp *kvrpcpb.PrewriteResponse) error {
	// Clear the RPC Error since the request is evaluated successfully.
	handler.sender.SetRPCError(nil)

	// Update CommitDetails
	reqDuration := time.Since(reqBegin)
	handler.committer.getDetail().MergePrewriteReqDetails(
		reqDuration,
		handler.batch.region.GetID(),
		handler.sender.GetStoreAddr(),
		prewriteResp.ExecDetailsV2,
	)

	if handler.batch.isPrimary {
		// After writing the primary key, if the size of the transaction is larger than 32M,
		// start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
		// In this case 1PC is not expected to be used, but still check it for safety.
		if int64(handler.committer.txnSize) > config.GetGlobalConfig().TiKVClient.TTLRefreshedTxnSize &&
			prewriteResp.OnePcCommitTs == 0 {
			handler.committer.ttlManager.run(handler.committer, nil, false)
		}
	}
	if handler.committer.isOnePC() {
		if prewriteResp.OnePcCommitTs == 0 {
			if prewriteResp.MinCommitTs != 0 {
				return errors.New("MinCommitTs must be 0 when 1pc falls back to 2pc")
			}
			logutil.Logger(handler.bo.GetCtx()).Warn(
				"1pc failed and fallbacks to normal commit procedure",
				zap.Uint64("startTS", handler.committer.startTS),
			)
			metrics.OnePCTxnCounterFallback.Inc()
			handler.committer.setOnePC(false)
			handler.committer.setAsyncCommit(false)
		} else {
			// For 1PC, there's no racing to access `onePCCommitTS` so it's safe
			// not to lock the mutex.
			if handler.committer.onePCCommitTS != 0 {
				logutil.Logger(handler.bo.GetCtx()).Fatal(
					"one pc happened multiple times",
					zap.Uint64("startTS", handler.committer.startTS),
				)
			}
			handler.committer.onePCCommitTS = prewriteResp.OnePcCommitTs
		}
		return nil
	} else if prewriteResp.OnePcCommitTs != 0 {
		logutil.Logger(handler.bo.GetCtx()).Fatal(
			"tikv committed a non-1pc transaction with 1pc protocol",
			zap.Uint64("startTS", handler.committer.startTS),
		)
	}
	if handler.committer.isAsyncCommit() {
		// 0 if the min_commit_ts is not ready or any other reason that async
		// commit cannot proceed. The client can then fallback to normal way to
		// continue committing the transaction if prewrite are all finished.
		if prewriteResp.MinCommitTs == 0 {
			if handler.committer.testingKnobs.noFallBack {
				return nil
			}
			logutil.Logger(handler.bo.GetCtx()).Warn(
				"async commit cannot proceed since the returned minCommitTS is zero, "+
					"fallback to normal path", zap.Uint64("startTS", handler.committer.startTS),
			)
			handler.committer.setAsyncCommit(false)
		} else {
			handler.committer.mu.Lock()
			if prewriteResp.MinCommitTs > handler.committer.minCommitTSMgr.get() {
				handler.committer.minCommitTSMgr.tryUpdate(prewriteResp.MinCommitTs, twoPCAccess)
			}
			handler.committer.mu.Unlock()
		}
	}
	return nil
}
