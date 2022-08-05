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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/pessimistic.go
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
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

type actionPessimisticLock struct {
	*kv.LockCtx
	lockWaitMode kvrpcpb.PessimisticWaitLockMode
}
type actionPessimisticRollback struct{}

var (
	_ twoPhaseCommitAction = actionPessimisticLock{}
	_ twoPhaseCommitAction = actionPessimisticRollback{}
)

func (actionPessimisticLock) String() string {
	return "pessimistic_lock"
}

func (actionPessimisticLock) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramPessimisticLock
}

func (actionPessimisticRollback) String() string {
	return "pessimistic_rollback"
}

func (actionPessimisticRollback) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return metrics.TxnRegionsNumHistogramPessimisticRollback
}

type diagnosticContext struct {
	resolvingRecordToken *int
	sender               *locate.RegionRequestSender
	reqDuration          time.Duration
}

func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	convertMutationsToPb := func(committerMutations CommitterMutations) []*kvrpcpb.Mutation {
		mutations := make([]*kvrpcpb.Mutation, committerMutations.Len())
		for i := 0; i < committerMutations.Len(); i++ {
			mut := &kvrpcpb.Mutation{
				Op:  kvrpcpb.Op_PessimisticLock,
				Key: committerMutations.GetKey(i),
			}
			if c.txn.us.HasPresumeKeyNotExists(committerMutations.GetKey(i)) || (c.doingAmend && committerMutations.GetOp(i) == kvrpcpb.Op_Insert) {
				mut.Assertion = kvrpcpb.Assertion_NotExist
			}
			mutations[i] = mut
		}
		return mutations
	}

	m := batch.mutations
	mutations := convertMutationsToPb(m)
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticLock, &kvrpcpb.PessimisticLockRequest{
		Mutations:      mutations,
		PrimaryLock:    c.primary(),
		StartVersion:   c.startTS,
		ForUpdateTs:    c.forUpdateTS,
		IsFirstLock:    c.isFirstLock,
		WaitTimeout:    action.LockWaitTime(),
		ReturnValues:   action.ReturnValues,
		CheckExistence: action.CheckExistence,
		MinCommitTs:    c.forUpdateTS + 1,
		WaitLockMode:   action.lockWaitMode,
	}, kvrpcpb.Context{
		Priority:               c.priority,
		SyncLog:                c.syncLog,
		ResourceGroupTag:       action.LockCtx.ResourceGroupTag,
		MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds()),
		RequestSource:          c.txn.GetRequestSource(),
	})
	if action.LockCtx.ResourceGroupTag == nil && action.LockCtx.ResourceGroupTagger != nil {
		req.ResourceGroupTag = action.LockCtx.ResourceGroupTagger(req.Req.(*kvrpcpb.PessimisticLockRequest))
	}
	lockWaitStartTime := action.WaitStartTime
	diagCtx := diagnosticContext{}
	defer func() {
		if diagCtx.resolvingRecordToken != nil {
			c.store.GetLockResolver().ResolveLocksDone(c.startTS, *diagCtx.resolvingRecordToken)
		}
	}()
	for {
		// if lockWaitTime set, refine the request `WaitTimeout` field based on timeout limit
		if action.LockWaitTime() > 0 && action.LockWaitTime() != kv.LockAlwaysWait {
			timeLeft := action.LockWaitTime() - (time.Since(lockWaitStartTime)).Milliseconds()
			if timeLeft <= 0 {
				req.PessimisticLock().WaitTimeout = kv.LockNoWait
			} else {
				req.PessimisticLock().WaitTimeout = timeLeft
			}
		}
		elapsed := uint64(time.Since(c.txn.startTime) / time.Millisecond)
		ttl := elapsed + atomic.LoadUint64(&ManagedLockTTL)
		if _, err := util.EvalFailpoint("shortPessimisticLockTTL"); err == nil {
			ttl = 1
			keys := make([]string, 0, len(mutations))
			for _, m := range mutations {
				keys = append(keys, hex.EncodeToString(m.Key))
			}
			logutil.BgLogger().Info("[failpoint] injected lock ttl = 1 on pessimistic lock",
				zap.Uint64("txnStartTS", c.startTS), zap.Strings("keys", keys))
		}
		req.PessimisticLock().LockTtl = ttl
		if _, err := util.EvalFailpoint("PessimisticLockErrWriteConflict"); err == nil {
			time.Sleep(300 * time.Millisecond)
			return errors.WithStack(&tikverr.ErrWriteConflict{WriteConflict: nil})
		}
		sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())
		startTime := time.Now()
		resp, err := sender.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
		diagCtx.reqDuration = time.Since(startTime)
		diagCtx.sender = sender
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCTime, int64(diagCtx.reqDuration))
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCCount, 1)
		}
		if err != nil {
			return err
		}

		if action.lockWaitMode == kvrpcpb.PessimisticWaitLockMode_RetryFirst {
			finished, err := action.handlePessimisticLockResponseRetryFirstMode(c, bo, &batch, mutations, resp, &diagCtx)
			if err != nil {
				return err
			}
			if finished {
				return nil
			}
		} else if action.lockWaitMode == kvrpcpb.PessimisticWaitLockMode_LockFirst {
			finished, retryMutations, err := action.handlePessimisticLockResponseLockFirstMode(c, bo, &batch, mutations, resp, &diagCtx)
			if err != nil {
				return err
			}
			if finished {
				return nil
			}

			if retryMutations != nil && retryMutations.Len() < batch.mutations.Len() {
				// The request may be partially succeeded. Replace the request with remaining mutations.
				// The primary lock (if it's in this request) must have been succeeded.
				batch = batchMutations{
					region:       batch.region,
					mutations:    retryMutations,
					isPrimary:    false,
					primaryIndex: -1,
				}
				m = batch.mutations
				mutations = convertMutationsToPb(m)
				req.PessimisticLock().Mutations = mutations
			}
		}

		// Handle the killed flag when waiting for the pessimistic lock.
		// When a txn runs into LockKeys() and backoff here, it has no chance to call
		// executor.Next() and check the killed flag.
		if action.Killed != nil {
			// Do not reset the killed flag here!
			// actionPessimisticLock runs on each region parallelly, we have to consider that
			// the error may be dropped.
			if atomic.LoadUint32(action.Killed) == 1 {
				return errors.WithStack(tikverr.ErrQueryInterrupted)
			}
		}
	}
}

func (action actionPessimisticLock) handlePessimisticLockResponseRetryFirstMode(c *twoPhaseCommitter, bo *retry.Backoffer, batch *batchMutations, mutationsPb []*kvrpcpb.Mutation, resp *tikvrpc.Response, diagCtx *diagnosticContext) (finished bool, err error) {
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return true, err
	}
	if regionErr != nil {
		// For other region error and the fake region error, backoff because
		// there's something wrong.
		// For the real EpochNotMatch error, don't backoff.
		if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return true, err
			}
		}
		same, err := batch.relocate(bo, c.store.GetRegionCache())
		if err != nil {
			return true, err
		}
		if same {
			return false, nil
		}
		err = c.pessimisticLockMutations(bo, action.LockCtx, action.lockWaitMode, batch.mutations)
		return true, err
	}
	if resp.Resp == nil {
		return true, errors.WithStack(tikverr.ErrBodyMissing)
	}
	lockResp := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
	if len(lockResp.Results) != 0 {
		// We use old protocol in this mode. The `Result` field should not be used.
		return true, errors.New("Pessimistic lock response corrupted")
	}
	keyErrs := lockResp.GetErrors()
	if len(keyErrs) == 0 {
		action.LockCtx.Stats.MergeReqDetails(diagCtx.reqDuration, batch.region.GetID(), diagCtx.sender.GetStoreAddr(), lockResp.ExecDetailsV2)

		if batch.isPrimary {
			// After locking the primary key, we should protect the primary lock from expiring
			// now in case locking the remaining keys take a long time.
			c.run(c, action.LockCtx)
		}

		// Handle the case that the TiKV's version is too old and doesn't support `CheckExistence`.
		// If `CheckExistence` is set, `ReturnValues` is not set and `CheckExistence` is not supported, skip
		// retrieving value totally (indicated by `skipRetrievingValue`) to avoid panicking.
		skipRetrievingValue := !action.ReturnValues && action.CheckExistence && len(lockResp.NotFounds) == 0

		if (action.ReturnValues || action.CheckExistence) && !skipRetrievingValue {
			action.ValuesLock.Lock()
			for i, mutation := range mutationsPb {
				var value []byte
				if action.ReturnValues {
					value = lockResp.Values[i]
				}
				var exists = !lockResp.NotFounds[i]
				action.Values[string(mutation.Key)] = kv.ReturnedValue{
					Value:  value,
					Exists: exists,
				}
			}
			action.ValuesLock.Unlock()
		}
		return true, nil
	}
	var locks []*txnlock.Lock
	skipResolvingLocks := true
	for _, keyErr := range keyErrs {
		// Check already exists error
		if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
			e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
			return true, c.extractKeyExistsErr(e)
		}
		if deadlock := keyErr.Deadlock; deadlock != nil {
			return true, errors.WithStack(&tikverr.ErrDeadlock{Deadlock: deadlock})
		}

		// Extract lock from key error
		lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
		if err1 != nil {
			return true, err1
		}
		locks = append(locks, lock)
		if !keyErr.GetLocked().GetSkipResolvingLock() {
			skipResolvingLocks = false
		}
	}

	if skipResolvingLocks {
		// Retry immediately.
		return false, nil
	}

	// Because we already waited on tikv, no need to Backoff here.
	// tikv default will wait 3s(also the maximum wait value) when lock error occurs
	if diagCtx.resolvingRecordToken == nil {
		token := c.store.GetLockResolver().RecordResolvingLocks(locks, c.startTS)
		diagCtx.resolvingRecordToken = &token
	} else {
		c.store.GetLockResolver().UpdateResolvingLocks(locks, c.startTS, *diagCtx.resolvingRecordToken)
	}
	resolveLockOpts := txnlock.ResolveLocksOptions{
		CallerStartTS: 0,
		Locks:         locks,
		Detail:        &action.LockCtx.Stats.ResolveLock,
	}
	resolveLockRes, err := c.store.GetLockResolver().ResolveLocksWithOpts(bo, resolveLockOpts)
	if err != nil {
		return true, err
	}

	// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
	// the pessimistic lock. We should return acquire fail with nowait set or timeout error if necessary.
	if resolveLockRes.TTL > 0 {
		if action.LockWaitTime() == kv.LockNoWait {
			return true, errors.WithStack(tikverr.ErrLockAcquireFailAndNoWaitSet)
		} else if action.LockWaitTime() == kv.LockAlwaysWait {
			// do nothing but keep wait
		} else {
			// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
			if time.Since(action.WaitStartTime).Milliseconds() >= action.LockWaitTime() {
				return true, errors.WithStack(tikverr.ErrLockWaitTimeout)
			}
		}
		if action.LockCtx.PessimisticLockWaited != nil {
			atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
		}
	}

	return false, nil
}

func (action actionPessimisticLock) handlePessimisticLockResponseLockFirstMode(c *twoPhaseCommitter, bo *retry.Backoffer, batch *batchMutations, mutationsPb []*kvrpcpb.Mutation, resp *tikvrpc.Response, diagCtx *diagnosticContext) (finished bool, retryMutations CommitterMutations, err error) {
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return true, nil, err
	}
	if resp.Resp == nil {
		return true, nil, errors.WithStack(tikverr.ErrBodyMissing)
	}
	lockResp := resp.Resp.(*kvrpcpb.PessimisticLockResponse)
	failedMutations := NewPlainMutations(0)
	keyErrs := lockResp.GetErrors()

	if batch.isPrimary && len(lockResp.Results) > 0 && lockResp.Results[batch.primaryIndex].Type != kvrpcpb.PessimisticLockKeyResultType_Failed {
		// After locking the primary key, we should protect the primary lock from expiring
		// now in case locking the remaining keys take a long time.
		c.run(c, action.LockCtx)
	}

	for i, res := range lockResp.Results {
		switch res.Type {
		case kvrpcpb.PessimisticLockKeyResultType_Empty:
			// Do nothing
		case kvrpcpb.PessimisticLockKeyResultType_Value:
			action.ValuesLock.Lock()
			action.Values[string(mutationsPb[i].Key)] = kv.ReturnedValue{
				Value:  res.Value,
				Exists: res.Existence,
			}
			action.ValuesLock.Unlock()
		case kvrpcpb.PessimisticLockKeyResultType_Existence:
			action.ValuesLock.Lock()
			action.Values[string(mutationsPb[i].Key)] = kv.ReturnedValue{
				Exists: res.Existence,
			}
			action.ValuesLock.Unlock()
		case kvrpcpb.PessimisticLockKeyResultType_LockedWithConflict:
			action.ValuesLock.Lock()
			action.Values[string(mutationsPb[i].Key)] = kv.ReturnedValue{
				Value:                res.Value,
				Exists:               res.Existence,
				LockedWithConflictTS: res.LockedWithConflictTs,
			}
			if res.LockedWithConflictTs > action.MaxLockedWithConflictTS {
				action.MaxLockedWithConflictTS = res.LockedWithConflictTs
			}
			action.ValuesLock.Unlock()
		case kvrpcpb.PessimisticLockKeyResultType_Failed:
			action.ValuesLock.Lock()
			action.Values[string(mutationsPb[i].Key)] = kv.ReturnedValue{
				LockStatusUncertain: true,
			}
			action.ValuesLock.Unlock()
			failedMutations.AppendMutation(batch.mutations.GetMutation(i))
		default:
			panic("unreachable")
		}
	}

	if len(lockResp.Results) > 0 && failedMutations.Len() < len(lockResp.Results) {
		// Can we do it like this?
		action.LockCtx.Stats.MergeReqDetails(diagCtx.reqDuration, batch.region.GetID(), diagCtx.sender.GetStoreAddr(), lockResp.ExecDetailsV2)
	}

	// The primary must be locked first, otherwise there should be some bug in the implementation.
	if batch.isPrimary && failedMutations.Len() < len(lockResp.Results) && lockResp.Results[batch.primaryIndex].Type == kvrpcpb.PessimisticLockKeyResultType_Failed {
		return true, nil, errors.New("Pessimistic lock response corrupted")
	}

	var locks []*txnlock.Lock
	skipResolvingLocks := true
	for _, keyErr := range keyErrs {
		// Check already exists error
		if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
			e := &tikverr.ErrKeyExist{AlreadyExist: alreadyExist}
			return true, nil, c.extractKeyExistsErr(e)
		}
		if deadlock := keyErr.Deadlock; deadlock != nil {
			return true, nil, errors.WithStack(&tikverr.ErrDeadlock{Deadlock: deadlock})
		}

		// Extract lock from key error
		lock, err1 := txnlock.ExtractLockFromKeyErr(keyErr)
		if err1 != nil {
			return true, nil, err1
		}
		locks = append(locks, lock)
		if !keyErr.GetLocked().GetSkipResolvingLock() {
			skipResolvingLocks = false
		}
	}

	if regionErr != nil {
		// For other region error and the fake region error, backoff because
		// there's something wrong.
		// For the real EpochNotMatch error, don't backoff.
		if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return true, nil, err
			}
		}
		same, err := batch.relocate(bo, c.store.GetRegionCache())
		if err != nil {
			return true, nil, err
		}
		retryMutations = &failedMutations
		if len(lockResp.Results) == 0 {
			retryMutations = batch.mutations
		}
		if same {
			return false, retryMutations, nil
		}
		err = c.pessimisticLockMutations(bo, action.LockCtx, action.lockWaitMode, retryMutations)
		return true, nil, err
	}

	if failedMutations.Len() > 0 {
		if len(locks) > 0 {
			if skipResolvingLocks {
				// Retry immediately.
				return false, &failedMutations, nil
			}
			// Because we already waited on tikv, no need to Backoff here.
			// tikv default will wait 3s(also the maximum wait value) when lock error occurs
			if diagCtx.resolvingRecordToken == nil {
				token := c.store.GetLockResolver().RecordResolvingLocks(locks, c.startTS)
				diagCtx.resolvingRecordToken = &token
			} else {
				c.store.GetLockResolver().UpdateResolvingLocks(locks, c.startTS, *diagCtx.resolvingRecordToken)
			}
			resolveLockOpts := txnlock.ResolveLocksOptions{
				CallerStartTS: 0,
				Locks:         locks,
				Detail:        &action.LockCtx.Stats.ResolveLock,
			}
			resolveLockRes, err := c.store.GetLockResolver().ResolveLocksWithOpts(bo, resolveLockOpts)
			if err != nil {
				return true, nil, err
			}

			// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
			// the pessimistic lock. We should return acquire fail with nowait set or timeout error if necessary.
			if resolveLockRes.TTL > 0 {
				if action.LockWaitTime() == kv.LockNoWait {
					return true, nil, errors.WithStack(tikverr.ErrLockAcquireFailAndNoWaitSet)
				} else if action.LockWaitTime() == kv.LockAlwaysWait {
					// do nothing but keep wait
				} else {
					// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
					if time.Since(action.WaitStartTime).Milliseconds() >= action.LockWaitTime() {
						return true, nil, errors.WithStack(tikverr.ErrLockWaitTimeout)
					}
				}
				if action.LockCtx.PessimisticLockWaited != nil {
					atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
				}
			}
			return false, &failedMutations, nil
		}

		return true, nil, errors.New("Pessimistic lock response corrupted")
	}

	if regionErr != nil || len(locks) != 0 {
		return true, nil, errors.New("Pessimistic lock response corrupted")
	}

	// If the Results in response, there must be either some unretryable error in keyErrs or some region error, therefore
	// the function must have returned.
	if len(lockResp.Results) == 0 {
		return true, nil, errors.New("Pessimistic lock response corrupted")
	}

	return true, nil, nil
}

func (actionPessimisticRollback) handleSingleBatch(c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations) error {
	forUpdateTS := c.forUpdateTS
	if c.maxLockedWithConflictTS > forUpdateTS {
		forUpdateTS = c.maxLockedWithConflictTS
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, &kvrpcpb.PessimisticRollbackRequest{
		StartVersion: c.startTS,
		ForUpdateTs:  forUpdateTS,
		Keys:         batch.mutations.GetKeys(),
	})
	req.RequestSource = util.RequestSourceFromCtx(bo.GetCtx())
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, err := c.store.SendReq(bo, req, batch.region, client.ReadTimeoutShort)

	if err != nil {
		return err
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	if regionErr != nil {
		err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return err
		}
		return c.pessimisticRollbackMutations(bo, batch.mutations)
	}
	return nil
}

func (c *twoPhaseCommitter) pessimisticLockMutations(bo *retry.Backoffer, lockCtx *kv.LockCtx, lockWaitMode kvrpcpb.PessimisticWaitLockMode, mutations CommitterMutations) error {
	if c.sessionID > 0 {
		if val, err := util.EvalFailpoint("beforePessimisticLock"); err == nil {
			// Pass multiple instructions in one string, delimited by commas, to trigger multiple behaviors, like
			// `return("delay,fail")`. Then they will be executed sequentially at once.
			if v, ok := val.(string); ok {
				for _, action := range strings.Split(v, ",") {
					if action == "delay" {
						duration := time.Duration(rand.Int63n(int64(time.Second) * 5))
						logutil.Logger(bo.GetCtx()).Info("[failpoint] injected delay at pessimistic lock",
							zap.Uint64("txnStartTS", c.startTS), zap.Duration("duration", duration))
						time.Sleep(duration)
					} else if action == "fail" {
						logutil.Logger(bo.GetCtx()).Info("[failpoint] injected failure at pessimistic lock",
							zap.Uint64("txnStartTS", c.startTS))
						return errors.New("injected failure at pessimistic lock")
					}
				}
			}
		}
	}
	return c.doActionOnMutations(bo, actionPessimisticLock{LockCtx: lockCtx, lockWaitMode: lockWaitMode}, mutations)
}

func (c *twoPhaseCommitter) pessimisticRollbackMutations(bo *retry.Backoffer, mutations CommitterMutations) error {
	return c.doActionOnMutations(bo, actionPessimisticRollback{}, mutations)
}
