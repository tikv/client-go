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

package txnlock

import (
	"bytes"
	"container/list"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// ResolvedCacheSize is max number of cached txn status.
const ResolvedCacheSize = 2048

const (
	getTxnStatusMaxBackoff     = 20000
	asyncResolveLockMaxBackoff = 40000
)

type storage interface {
	// GetRegionCache gets the RegionCache.
	GetRegionCache() *locate.RegionCache
	// SendReq sends a request to TiKV.
	SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error)
	// GetOracle gets a timestamp oracle client.
	GetOracle() oracle.Oracle
}

// LockResolver resolves locks and also caches resolved txn status.
type LockResolver struct {
	store                    storage
	resolveLockLiteThreshold uint64
	mu                       struct {
		sync.RWMutex
		// These two fields is used to tracking lock resolving information
		// currentStartTS -> caller token -> resolving locks
		resolving map[uint64][][]Lock
		// currentStartTS -> concurrency resolving lock process in progress
		// use concurrency counting here to speed up checking
		// whether we can free the resource used in `resolving`
		resolvingConcurrency map[uint64]int
		// resolved caches resolved txns (FIFO, txn id -> txnStatus).
		resolved       map[uint64]TxnStatus
		recentResolved *list.List
	}
	testingKnobs struct {
		meetLock func(locks []*Lock)
	}

	// LockResolver may have some goroutines resolving locks in the background.
	// The Cancel function is to cancel these goroutines for passing goleak test.
	asyncResolveCtx    context.Context
	asyncResolveCancel func()
}

// ResolvingLock stands for current resolving locks' information
type ResolvingLock struct {
	TxnID     uint64
	LockTxnID uint64
	Key       []byte
	Primary   []byte
}

// NewLockResolver creates a new LockResolver instance.
func NewLockResolver(store storage) *LockResolver {
	r := &LockResolver{
		store:                    store,
		resolveLockLiteThreshold: config.GetGlobalConfig().TiKVClient.ResolveLockLiteThreshold,
	}
	r.mu.resolved = make(map[uint64]TxnStatus)
	r.mu.resolving = make(map[uint64][][]Lock)
	r.mu.resolvingConcurrency = make(map[uint64]int)
	r.mu.recentResolved = list.New()
	r.asyncResolveCtx, r.asyncResolveCancel = context.WithCancel(context.Background())
	return r
}

// Close cancels all background goroutines.
func (lr *LockResolver) Close() {
	lr.asyncResolveCancel()
}

// TxnStatus represents a txn's final status. It should be Lock or Commit or Rollback.
type TxnStatus struct {
	ttl         uint64
	commitTS    uint64
	action      kvrpcpb.Action
	primaryLock *kvrpcpb.LockInfo
}

// IsCommitted returns true if the txn's final status is Commit.
func (s TxnStatus) IsCommitted() bool { return s.ttl == 0 && s.commitTS > 0 }

// IsRolledBack returns true if the txn's final status is rolled back.
func (s TxnStatus) IsRolledBack() bool { return s.ttl == 0 && s.commitTS == 0 }

// CommitTS returns the txn's commitTS. It is valid iff `IsCommitted` is true.
func (s TxnStatus) CommitTS() uint64 { return s.commitTS }

// TTL returns the TTL of the transaction if the transaction is still alive.
func (s TxnStatus) TTL() uint64 { return s.ttl }

// Action returns what the CheckTxnStatus request have done to the transaction.
func (s TxnStatus) Action() kvrpcpb.Action { return s.action }

// StatusCacheable checks whether the transaction status is certain.True will be
// returned if its status is certain:
//
//	If transaction is already committed, the result could be cached.
//	Otherwise:
//	  If l.LockType is pessimistic lock type:
//	      - if its primary lock is pessimistic too, the check txn status result should not be cached.
//	      - if its primary lock is prewrite lock type, the check txn status could be cached.
//	  If l.lockType is prewrite lock type:
//	      - always cache the check txn status result.
//
// For prewrite locks, their primary keys should ALWAYS be the correct one and will NOT change.
func (s TxnStatus) StatusCacheable() bool {
	if s.IsCommitted() {
		return true
	}
	if s.ttl == 0 {
		if s.action == kvrpcpb.Action_NoAction ||
			s.action == kvrpcpb.Action_LockNotExistRollback ||
			s.action == kvrpcpb.Action_TTLExpireRollback {
			return true
		}
	}
	return false
}

// Lock represents a lock from tikv server.
type Lock struct {
	Key             []byte
	Primary         []byte
	TxnID           uint64
	TTL             uint64
	TxnSize         uint64
	LockType        kvrpcpb.Op
	UseAsyncCommit  bool
	LockForUpdateTS uint64
	MinCommitTS     uint64
}

func (l *Lock) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("key: ")
	buf.WriteString(hex.EncodeToString(l.Key))
	buf.WriteString(", primary: ")
	buf.WriteString(hex.EncodeToString(l.Primary))
	return fmt.Sprintf("%s, txnStartTS: %d, lockForUpdateTS:%d, minCommitTs:%d, ttl: %d, type: %s, UseAsyncCommit: %t, txnSize: %d",
		buf.String(), l.TxnID, l.LockForUpdateTS, l.MinCommitTS, l.TTL, l.LockType, l.UseAsyncCommit, l.TxnSize)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:             l.GetKey(),
		Primary:         l.GetPrimaryLock(),
		TxnID:           l.GetLockVersion(),
		TTL:             l.GetLockTtl(),
		TxnSize:         l.GetTxnSize(),
		LockType:        l.LockType,
		UseAsyncCommit:  l.UseAsyncCommit,
		LockForUpdateTS: l.LockForUpdateTs,
		MinCommitTS:     l.MinCommitTs,
	}
}

func (lr *LockResolver) saveResolved(txnID uint64, status TxnStatus) {
	lr.mu.Lock()
	defer lr.mu.Unlock()

	if _, ok := lr.mu.resolved[txnID]; ok {
		return
	}
	lr.mu.resolved[txnID] = status
	lr.mu.recentResolved.PushBack(txnID)
	if len(lr.mu.resolved) > ResolvedCacheSize {
		front := lr.mu.recentResolved.Front()
		delete(lr.mu.resolved, front.Value.(uint64))
		lr.mu.recentResolved.Remove(front)
	}
}

func (lr *LockResolver) getResolved(txnID uint64) (TxnStatus, bool) {
	lr.mu.RLock()
	defer lr.mu.RUnlock()

	s, ok := lr.mu.resolved[txnID]
	return s, ok
}

// BatchResolveLocks resolve locks in a batch.
// Used it in gcworker only!
func (lr *LockResolver) BatchResolveLocks(bo *retry.Backoffer, locks []*Lock, loc locate.RegionVerID) (bool, error) {
	if len(locks) == 0 {
		return true, nil
	}

	metrics.LockResolverCountWithBatchResolve.Inc()

	// The GCWorker kill all ongoing transactions, because it must make sure all
	// locks have been cleaned before GC.
	expiredLocks := locks

	txnInfos := make(map[uint64]uint64)
	startTime := time.Now()
	for _, l := range expiredLocks {
		logutil.Logger(bo.GetCtx()).Debug("BatchResolveLocks handling lock", zap.Stringer("lock", l))

		if _, ok := txnInfos[l.TxnID]; ok {
			continue
		}
		metrics.LockResolverCountWithExpired.Inc()

		// Use currentTS = math.MaxUint64 means rollback the txn, no matter the lock is expired or not!
		status, err := lr.getTxnStatus(bo, l.TxnID, l.Primary, 0, math.MaxUint64, true, false, l)
		if err != nil {
			return false, err
		}

		if l.LockType == kvrpcpb.Op_PessimisticLock {
			// BatchResolveLocks forces resolving the locks ignoring whether whey are expired.
			// For pessimistic locks, committing them makes no sense, but it won't affect transaction
			// correctness if we always roll back them.
			// Pessimistic locks needs special handling logic because their primary may not point
			// to the real primary of that transaction, and their state cannot be put in `txnInfos`.
			// (see: https://github.com/pingcap/tidb/issues/42937).
			//
			// `resolvePessimisticLock` should be called after calling `getTxnStatus`.
			// See: https://github.com/pingcap/tidb/issues/45134
			err := lr.resolvePessimisticLock(bo, l)
			if err != nil {
				return false, err
			}
			continue
		}

		// If the transaction uses async commit, CheckTxnStatus will reject rolling back the primary lock.
		// Then we need to check the secondary locks to determine the final status of the transaction.
		if status.primaryLock != nil && status.primaryLock.UseAsyncCommit {
			resolveData, err := lr.checkAllSecondaries(bo, l, &status)
			if err == nil {
				txnInfos[l.TxnID] = resolveData.commitTs
				continue
			}
			if _, ok := errors.Cause(err).(*nonAsyncCommitLock); ok {
				status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary, 0, math.MaxUint64, true, true, l)
				if err != nil {
					return false, err
				}
			} else {
				return false, err
			}
		}

		if status.ttl > 0 {
			logutil.BgLogger().Error("BatchResolveLocks fail to clean locks, this result is not expected!")
			return false, errors.New("TiDB ask TiKV to rollback locks but it doesn't, the protocol maybe wrong")
		}

		txnInfos[l.TxnID] = status.commitTS
	}
	logutil.BgLogger().Info("BatchResolveLocks: lookup txn status",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of txn", len(txnInfos)))

	listTxnInfos := make([]*kvrpcpb.TxnInfo, 0, len(txnInfos))
	for txnID, status := range txnInfos {
		listTxnInfos = append(listTxnInfos, &kvrpcpb.TxnInfo{
			Txn:    txnID,
			Status: status,
		})
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{TxnInfos: listTxnInfos},
		kvrpcpb.Context{
			// TODO: how to pass the `start_ts`	here?
			RequestSource: util.RequestSourceFromCtx(bo.GetCtx()),
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: util.ResourceGroupNameFromCtx(bo.GetCtx()),
			},
		},
	)
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	startTime = time.Now()
	resp, err := lr.store.SendReq(bo, req, loc, client.ReadTimeoutShort)
	if err != nil {
		return false, err
	}

	regionErr, err := resp.GetRegionError()
	if err != nil {
		return false, err
	}

	if regionErr != nil {
		err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return false, err
		}
		return false, nil
	}

	if resp.Resp == nil {
		return false, errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
	if keyErr := cmdResp.GetError(); keyErr != nil {
		return false, errors.Errorf("unexpected resolve err: %s", keyErr)
	}

	logutil.BgLogger().Info("BatchResolveLocks: resolve locks in a batch",
		zap.Duration("cost time", time.Since(startTime)),
		zap.Int("num of locks", len(expiredLocks)))
	return true, nil
}

// ResolveLocksOptions is the options struct for calling resolving lock.
type ResolveLocksOptions struct {
	CallerStartTS uint64
	Locks         []*Lock
	Lite          bool
	ForRead       bool
	Detail        *util.ResolveLockDetail
}

// ResolveLockResult is the result struct for resolving lock.
type ResolveLockResult struct {
	TTL         int64
	IgnoreLocks []uint64
	AccessLocks []uint64
}

// ResolveLocksWithOpts wraps ResolveLocks and ResolveLocksForRead, which extract the parameters into structs for better extension.
func (lr *LockResolver) ResolveLocksWithOpts(bo *retry.Backoffer, opts ResolveLocksOptions) (ResolveLockResult, error) {
	return lr.resolveLocks(bo, opts)
}

// ResolveLocks tries to resolve Locks. The resolving process is in 3 steps:
//  1. Use the `lockTTL` to pick up all expired locks. Only locks that are too
//     old are considered orphan locks and will be handled later. If all locks
//     are expired then all locks will be resolved so the returned `ok` will be
//     true, otherwise caller should sleep a while before retry.
//  2. For each lock, query the primary key to get txn(which left the lock)'s
//     commit status.
//  3. Send `ResolveLock` cmd to the lock's region to resolve all locks belong to
//     the same transaction.
func (lr *LockResolver) ResolveLocks(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock) (int64, error) {
	opts := ResolveLocksOptions{
		CallerStartTS: callerStartTS,
		Locks:         locks,
	}
	res, err := lr.resolveLocks(bo, opts)
	return res.TTL, err
}

// ResolveLocksForRead is essentially the same as ResolveLocks, except with some optimizations for read.
// Read operations needn't wait for resolve secondary locks and can read through(the lock's transaction is committed
// and its commitTS is less than or equal to callerStartTS) or ignore(the lock's transaction is rolled back or its minCommitTS is pushed) the lock .
func (lr *LockResolver) ResolveLocksForRead(bo *retry.Backoffer, callerStartTS uint64, locks []*Lock, lite bool) (int64, []uint64 /* canIgnore */, []uint64 /* canAccess */, error) {
	opts := ResolveLocksOptions{
		CallerStartTS: callerStartTS,
		Locks:         locks,
		Lite:          lite,
		ForRead:       true,
	}
	res, err := lr.resolveLocks(bo, opts)
	return res.TTL, res.IgnoreLocks, res.AccessLocks, err
}

// RecordResolvingLocks records a txn which startTS is callerStartTS tries to resolve locks
// Call this when start trying to resolve locks
// Return a token which is used to call ResolvingLocksDone
func (lr *LockResolver) RecordResolvingLocks(locks []*Lock, callerStartTS uint64) int {
	resolving := make([]Lock, 0, len(locks))
	for _, lock := range locks {
		resolving = append(resolving, *lock)
	}
	lr.mu.Lock()
	lr.mu.resolvingConcurrency[callerStartTS]++
	token := len(lr.mu.resolving[callerStartTS])
	lr.mu.resolving[callerStartTS] = append(lr.mu.resolving[callerStartTS], resolving)
	lr.mu.Unlock()
	return token
}

// UpdateResolvingLocks update the lock resoling information of the txn `callerStartTS`
func (lr *LockResolver) UpdateResolvingLocks(locks []*Lock, callerStartTS uint64, token int) {
	resolving := make([]Lock, 0, len(locks))
	for _, lock := range locks {
		resolving = append(resolving, *lock)
	}
	lr.mu.Lock()
	lr.mu.resolving[callerStartTS][token] = resolving
	lr.mu.Unlock()
}

// ResolveLocksDone will remove resolving locks information related with callerStartTS
func (lr *LockResolver) ResolveLocksDone(callerStartTS uint64, token int) {
	lr.mu.Lock()
	lr.mu.resolving[callerStartTS][token] = nil
	lr.mu.resolvingConcurrency[callerStartTS]--
	if lr.mu.resolvingConcurrency[callerStartTS] == 0 {
		delete(lr.mu.resolving, callerStartTS)
		delete(lr.mu.resolvingConcurrency, callerStartTS)
	}
	lr.mu.Unlock()
}

func (lr *LockResolver) resolveLocks(bo *retry.Backoffer, opts ResolveLocksOptions) (ResolveLockResult, error) {
	callerStartTS, locks, forRead, lite, detail := opts.CallerStartTS, opts.Locks, opts.ForRead, opts.Lite, opts.Detail
	if lr.testingKnobs.meetLock != nil {
		lr.testingKnobs.meetLock(locks)
	}
	var msBeforeTxnExpired txnExpireTime
	if len(locks) == 0 {
		return ResolveLockResult{
			TTL: msBeforeTxnExpired.value(),
		}, nil
	}
	metrics.LockResolverCountWithResolve.Inc()
	// This is the origin resolve lock time.
	// TODO(you06): record the more details and calculate the total time by calculating the sum of details.
	if detail != nil {
		startTime := time.Now()
		defer func() {
			atomic.AddInt64(&detail.ResolveLockTime, int64(time.Since(startTime)))
		}()
	}

	// TxnID -> []Region, record resolved Regions.
	// TODO: Maybe put it in LockResolver and share by all txns.
	cleanTxns := make(map[uint64]map[locate.RegionVerID]struct{})
	var resolve func(*Lock, bool) (TxnStatus, error)
	resolve = func(l *Lock, forceSyncCommit bool) (TxnStatus, error) {
		status, err := lr.getTxnStatusFromLock(bo, l, callerStartTS, forceSyncCommit, detail)

		if _, ok := errors.Cause(err).(primaryMismatch); ok {
			if l.LockType != kvrpcpb.Op_PessimisticLock {
				logutil.BgLogger().Info("unexpected primaryMismatch error occurred on a non-pessimistic lock", zap.Stringer("lock", l), zap.Error(err))
				return TxnStatus{}, err
			}
			// Pessimistic rollback the pessimistic lock as it points to an invalid primary.
			status, err = TxnStatus{}, nil
		} else if err != nil {
			return TxnStatus{}, err
		}
		if status.ttl != 0 {
			return status, nil
		}

		// If the lock is committed or rollbacked, resolve lock.
		metrics.LockResolverCountWithExpired.Inc()
		cleanRegions, exists := cleanTxns[l.TxnID]
		if !exists {
			cleanRegions = make(map[locate.RegionVerID]struct{})
			cleanTxns[l.TxnID] = cleanRegions
		}
		if status.primaryLock != nil && status.primaryLock.UseAsyncCommit && !forceSyncCommit {
			// resolveAsyncCommitLock will resolve all locks of the transaction, so we needn't resolve
			// it again if it has been resolved once.
			if exists {
				return status, nil
			}
			// status of async-commit transaction is determined by resolveAsyncCommitLock.
			status, err = lr.resolveAsyncCommitLock(bo, l, status, forRead)
			if _, ok := errors.Cause(err).(*nonAsyncCommitLock); ok {
				status, err = resolve(l, true)
			}
			return status, err
		}
		if l.LockType == kvrpcpb.Op_PessimisticLock {
			// pessimistic locks don't block read so it needn't be async.
			err = lr.resolvePessimisticLock(bo, l)
		} else {
			if forRead {
				asyncCtx := context.WithValue(lr.asyncResolveCtx, util.RequestSourceKey, bo.GetCtx().Value(util.RequestSourceKey))
				asyncBo := retry.NewBackoffer(asyncCtx, asyncResolveLockMaxBackoff)
				go func() {
					// Pass an empty cleanRegions here to avoid data race and
					// let `reqCollapse` deduplicate identical resolve requests.
					err := lr.resolveLock(asyncBo, l, status, lite, map[locate.RegionVerID]struct{}{})
					if err != nil {
						logutil.BgLogger().Info("failed to resolve lock asynchronously",
							zap.String("lock", l.String()), zap.Uint64("commitTS", status.CommitTS()), zap.Error(err))
					}
				}()
			} else {
				err = lr.resolveLock(bo, l, status, lite, cleanRegions)
			}
		}
		return status, err
	}

	var canIgnore, canAccess []uint64
	for _, l := range locks {
		status, err := resolve(l, false)
		if err != nil {
			msBeforeTxnExpired.update(0)
			return ResolveLockResult{
				TTL: msBeforeTxnExpired.value(),
			}, err
		}
		if !forRead {
			if status.ttl != 0 {
				metrics.LockResolverCountWithNotExpired.Inc()
				msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, status.ttl, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
				msBeforeTxnExpired.update(msBeforeLockExpired)
				continue
			}
		}
		if status.action == kvrpcpb.Action_MinCommitTSPushed || status.IsRolledBack() ||
			(status.IsCommitted() && status.CommitTS() > callerStartTS) {
			if canIgnore == nil {
				canIgnore = make([]uint64, 0, len(locks))
			}
			canIgnore = append(canIgnore, l.TxnID)
		} else if status.IsCommitted() && status.CommitTS() <= callerStartTS {
			if canAccess == nil {
				canAccess = make([]uint64, 0, len(locks))
			}
			canAccess = append(canAccess, l.TxnID)
		} else {
			metrics.LockResolverCountWithNotExpired.Inc()
			msBeforeLockExpired := lr.store.GetOracle().UntilExpired(l.TxnID, status.ttl, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			msBeforeTxnExpired.update(msBeforeLockExpired)
		}
	}
	if msBeforeTxnExpired.value() > 0 {
		metrics.LockResolverCountWithWaitExpired.Inc()
	}
	return ResolveLockResult{
		TTL:         msBeforeTxnExpired.value(),
		IgnoreLocks: canIgnore,
		AccessLocks: canAccess,
	}, nil
}

// Resolving returns the locks' information we are resolving currently.
func (lr *LockResolver) Resolving() []ResolvingLock {
	result := []ResolvingLock{}
	lr.mu.RLock()
	defer lr.mu.RUnlock()
	for txnID, items := range lr.mu.resolving {
		for _, item := range items {
			for _, lock := range item {
				result = append(result, ResolvingLock{
					TxnID:     txnID,
					LockTxnID: lock.TxnID,
					Key:       lock.Key,
					Primary:   lock.Primary,
				})
			}
		}
	}
	return result
}

type txnExpireTime struct {
	initialized bool
	txnExpire   int64
}

func (t *txnExpireTime) update(lockExpire int64) {
	if lockExpire <= 0 {
		lockExpire = 0
	}
	if !t.initialized {
		t.txnExpire = lockExpire
		t.initialized = true
		return
	}
	if lockExpire < t.txnExpire {
		t.txnExpire = lockExpire
	}
}

func (t *txnExpireTime) value() int64 {
	if !t.initialized {
		return 0
	}
	return t.txnExpire
}

// GetTxnStatus queries tikv-server for a txn's status (commit/rollback).
// If the primary key is still locked, it will launch a Rollback to abort it.
// To avoid unnecessarily aborting too many txns, it is wiser to wait a few
// seconds before calling it after Prewrite.
func (lr *LockResolver) GetTxnStatus(txnID uint64, callerStartTS uint64, primary []byte) (TxnStatus, error) {
	var status TxnStatus
	bo := retry.NewBackoffer(context.Background(), getTxnStatusMaxBackoff)
	currentTS, err := lr.store.GetOracle().GetLowResolutionTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	if err != nil {
		return status, err
	}
	return lr.getTxnStatus(bo, txnID, primary, callerStartTS, currentTS, true, false, nil)
}

func (lr *LockResolver) getTxnStatusFromLock(bo *retry.Backoffer, l *Lock, callerStartTS uint64, forceSyncCommit bool, detail *util.ResolveLockDetail) (TxnStatus, error) {
	var currentTS uint64
	var err error
	var status TxnStatus

	if l.TTL == 0 {
		// NOTE: l.TTL = 0 is a special protocol!!!
		// When the pessimistic txn prewrite meets locks of a txn, it should resolve the lock **unconditionally**.
		// In this case, TiKV use lock TTL = 0 to notify TiDB, and TiDB should resolve the lock!
		// Set currentTS to max uint64 to make the lock expired.
		currentTS = math.MaxUint64
	} else {
		currentTS, err = lr.store.GetOracle().GetLowResolutionTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			return TxnStatus{}, err
		}
	}

	rollbackIfNotExist := false
	if _, err := util.EvalFailpoint("getTxnStatusDelay"); err == nil {
		time.Sleep(100 * time.Millisecond)
	}
	for {
		status, err = lr.getTxnStatus(bo, l.TxnID, l.Primary, callerStartTS, currentTS, rollbackIfNotExist, forceSyncCommit, l)
		if err == nil {
			return status, nil
		}
		// If the error is something other than txnNotFoundErr, throw the error (network
		// unavailable, tikv down, backoff timeout etc) to the caller.
		if _, ok := errors.Cause(err).(txnNotFoundErr); !ok {
			return TxnStatus{}, err
		}

		if _, err := util.EvalFailpoint("txnNotFoundRetTTL"); err == nil {
			return TxnStatus{ttl: l.TTL, action: kvrpcpb.Action_NoAction}, nil
		}

		if lr.store.GetOracle().UntilExpired(l.TxnID, l.TTL, &oracle.Option{TxnScope: oracle.GlobalTxnScope}) <= 0 {
			logutil.Logger(bo.GetCtx()).Warn("lock txn not found, lock has expired",
				zap.Uint64("CallerStartTs", callerStartTS),
				zap.Stringer("lock str", l))
			if l.LockType == kvrpcpb.Op_PessimisticLock {
				if _, err := util.EvalFailpoint("txnExpireRetTTL"); err == nil {
					return TxnStatus{action: kvrpcpb.Action_LockNotExistDoNothing},
						errors.New("error txn not found and lock expired")
				}
			}
			// For pessimistic lock resolving, if the primary lock does not exist and rollbackIfNotExist is true,
			// The Action_LockNotExistDoNothing will be returned as the status.
			rollbackIfNotExist = true
			continue
		} else {
			// For the Rollback statement from user, the pessimistic locks will be rollbacked and the primary key in store
			// has no related information. There are possibilities that some other transactions do checkTxnStatus on these
			// locks and they will be blocked ttl time, so let the transaction retries to do pessimistic lock if txn not found
			// and the lock does not expire yet.
			if l.LockType == kvrpcpb.Op_PessimisticLock {
				return TxnStatus{ttl: l.TTL}, nil
			}
		}

		// Handle txnNotFound error.
		// getTxnStatus() returns it when the secondary locks exist while the primary lock doesn't.
		// This is likely to happen in the concurrently prewrite when secondary regions
		// success before the primary region.
		if err := bo.Backoff(retry.BoTxnNotFound, err); err != nil {
			logutil.Logger(bo.GetCtx()).Warn("getTxnStatusFromLock backoff fail", zap.Error(err))
			return TxnStatus{}, err
		}
	}
}

type txnNotFoundErr struct {
	*kvrpcpb.TxnNotFound
}

func (e txnNotFoundErr) Error() string {
	return e.TxnNotFound.String()
}

type primaryMismatch struct {
	currentLock *kvrpcpb.LockInfo
}

func (e primaryMismatch) Error() string {
	return "primary mismatch, current lock: " + e.currentLock.String()
}

// getTxnStatus sends the CheckTxnStatus request to the TiKV server.
// When rollbackIfNotExist is false, the caller should be careful with the txnNotFoundErr error.
func (lr *LockResolver) getTxnStatus(bo *retry.Backoffer, txnID uint64, primary []byte,
	callerStartTS, currentTS uint64, rollbackIfNotExist bool, forceSyncCommit bool, lockInfo *Lock) (TxnStatus, error) {
	if s, ok := lr.getResolved(txnID); ok {
		return s, nil
	}

	metrics.LockResolverCountWithQueryTxnStatus.Inc()

	// CheckTxnStatus may meet the following cases:
	// 1. LOCK
	// 1.1 Lock expired -- orphan lock, fail to update TTL, crash recovery etc.
	// 1.2 Lock TTL -- active transaction holding the lock.
	// 2. NO LOCK
	// 2.1 Txn Committed
	// 2.2 Txn Rollbacked -- rollback itself, rollback by others, GC tomb etc.
	// 2.3 No lock -- pessimistic lock rollback, concurrence prewrite.

	var status TxnStatus
	resolvingPessimisticLock := lockInfo != nil && lockInfo.LockType == kvrpcpb.Op_PessimisticLock
	req := tikvrpc.NewRequest(tikvrpc.CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:               primary,
		LockTs:                   txnID,
		CallerStartTs:            callerStartTS,
		CurrentTs:                currentTS,
		RollbackIfNotExist:       rollbackIfNotExist,
		ForceSyncCommit:          forceSyncCommit,
		ResolvingPessimisticLock: resolvingPessimisticLock,
		VerifyIsPrimary:          true,
	}, kvrpcpb.Context{
		RequestSource: util.RequestSourceFromCtx(bo.GetCtx()),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: util.ResourceGroupNameFromCtx(bo.GetCtx()),
		},
	})
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, primary)
		if err != nil {
			return status, err
		}
		req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
		resp, err := lr.store.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
		if err != nil {
			return status, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return status, err
		}
		if regionErr != nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return status, err
			}
			continue
		}
		if resp.Resp == nil {
			return status, errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.CheckTxnStatusResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			txnNotFound := keyErr.GetTxnNotFound()
			if txnNotFound != nil {
				return status, txnNotFoundErr{txnNotFound}
			}

			if p := keyErr.GetPrimaryMismatch(); p != nil && resolvingPessimisticLock {
				err = primaryMismatch{currentLock: p.GetLockInfo()}
				logutil.BgLogger().Info("getTxnStatus was called on secondary lock", zap.Error(err))
				return status, err
			}

			err = errors.Errorf("unexpected err: %s, tid: %v", keyErr, txnID)
			logutil.BgLogger().Error("getTxnStatus error", zap.Error(err))
			return status, err
		}
		status.action = cmdResp.Action
		status.primaryLock = cmdResp.LockInfo

		if status.primaryLock != nil && status.primaryLock.UseAsyncCommit && !forceSyncCommit {
			if !lr.store.GetOracle().IsExpired(txnID, cmdResp.LockTtl, &oracle.Option{TxnScope: oracle.GlobalTxnScope}) {
				status.ttl = cmdResp.LockTtl
			}
		} else if cmdResp.LockTtl != 0 {
			status.ttl = cmdResp.LockTtl
		} else {
			if cmdResp.CommitVersion == 0 {
				metrics.LockResolverCountWithQueryTxnStatusRolledBack.Inc()
			} else {
				metrics.LockResolverCountWithQueryTxnStatusCommitted.Inc()
			}

			status.commitTS = cmdResp.CommitVersion
			if status.StatusCacheable() {
				lr.saveResolved(txnID, status)
			}
		}

		return status, nil
	}
}

// asyncResolveData is data contributed by multiple goroutines when resolving locks using the async commit protocol. All
// data should be protected by the mutex field.
type asyncResolveData struct {
	mutex sync.Mutex
	// If any key has been committed (missingLock is true), then this is the commit ts. In that case, all locks should
	// be committed with the same commit timestamp. If no locks have been committed (missingLock is false), then we will
	// use max(all min commit ts) from all locks; i.e., it is the commit ts we should use. Note that a secondary lock's
	// commit ts may or may not be the same as the primary lock's min commit ts.
	commitTs    uint64
	keys        [][]byte
	missingLock bool
}

type nonAsyncCommitLock struct{}

func (*nonAsyncCommitLock) Error() string {
	return "CheckSecondaryLocks receives a non-async-commit lock"
}

// addKeys adds the keys from locks to data, keeping other fields up to date. startTS and commitTS are for the
// transaction being resolved.
//
// In the async commit protocol when checking locks, we send a list of keys to check and get back a list of locks. There
// will be a lock for every key which is locked. If there are fewer locks than keys, then a lock is missing because it
// has been committed, rolled back, or was never locked.
//
// In this function, locks is the list of locks, and expected is the number of keys. asyncResolveData.missingLock will be
// set to true if the lengths don't match. If the lengths do match, then the locks are added to asyncResolveData.locks
// and will need to be resolved by the caller.
func (data *asyncResolveData) addKeys(locks []*kvrpcpb.LockInfo, expected int, startTS uint64, commitTS uint64) error {
	data.mutex.Lock()
	defer data.mutex.Unlock()

	// Check locks to see if any have been committed or rolled back.
	if len(locks) < expected {
		logutil.BgLogger().Debug("addKeys: lock has been committed or rolled back", zap.Uint64("commit ts", commitTS), zap.Uint64("start ts", startTS))
		// A lock is missing - the transaction must either have been rolled back or committed.
		if !data.missingLock {
			// commitTS == 0 => lock has been rolled back.
			if commitTS != 0 && commitTS < data.commitTs {
				return errors.Errorf("commit TS must be greater or equal to min commit TS: commit ts: %v, min commit ts: %v", commitTS, data.commitTs)
			}
			data.commitTs = commitTS
		}
		data.missingLock = true

		if data.commitTs != commitTS {
			return errors.Errorf("commit TS mismatch in async commit recovery: %v and %v", data.commitTs, commitTS)
		}

		// We do not need to resolve the remaining locks because TiKV will have resolved them as appropriate.
		return nil
	}

	logutil.BgLogger().Debug("addKeys: all locks present", zap.Uint64("start ts", startTS))
	// Save all locks to be resolved.
	for _, lockInfo := range locks {
		if lockInfo.LockVersion != startTS {
			err := errors.Errorf("unexpected timestamp, expected: %v, found: %v", startTS, lockInfo.LockVersion)
			logutil.BgLogger().Error("addLocks error", zap.Error(err))
			return err
		}
		if !lockInfo.UseAsyncCommit {
			return &nonAsyncCommitLock{}
		}
		if !data.missingLock && lockInfo.MinCommitTs > data.commitTs {
			data.commitTs = lockInfo.MinCommitTs
		}
		data.keys = append(data.keys, lockInfo.Key)
	}

	return nil
}

func (lr *LockResolver) checkSecondaries(bo *retry.Backoffer, txnID uint64, curKeys [][]byte, curRegionID locate.RegionVerID, shared *asyncResolveData) error {
	checkReq := &kvrpcpb.CheckSecondaryLocksRequest{
		Keys:         curKeys,
		StartVersion: txnID,
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdCheckSecondaryLocks, checkReq, kvrpcpb.Context{
		RequestSource: util.RequestSourceFromCtx(bo.GetCtx()),
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: util.ResourceGroupNameFromCtx(bo.GetCtx()),
		},
	})
	metrics.LockResolverCountWithQueryCheckSecondaryLocks.Inc()
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, err := lr.store.SendReq(bo, req, curRegionID, client.ReadTimeoutShort)
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

		logutil.BgLogger().Debug("checkSecondaries: region error, regrouping", zap.Uint64("txn id", txnID), zap.Uint64("region", curRegionID.GetID()))

		// If regions have changed, then we might need to regroup the keys. Since this should be rare and for the sake
		// of simplicity, we will resolve regions sequentially.
		regions, _, err := lr.store.GetRegionCache().GroupKeysByRegion(bo, curKeys, nil)
		if err != nil {
			return err
		}
		for regionID, keys := range regions {
			// Recursion will terminate because the resolve request succeeds or the Backoffer reaches its limit.
			if err = lr.checkSecondaries(bo, txnID, keys, regionID, shared); err != nil {
				return err
			}
		}
		return nil
	}
	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}

	checkResp := resp.Resp.(*kvrpcpb.CheckSecondaryLocksResponse)
	return shared.addKeys(checkResp.Locks, len(curKeys), txnID, checkResp.CommitTs)
}

// resolveAsyncResolveData resolves all locks in an async-commit transaction according to the status.
func (lr *LockResolver) resolveAsyncResolveData(bo *retry.Backoffer, l *Lock, status TxnStatus, data *asyncResolveData) error {
	util.EvalFailpoint("resolveAsyncResolveData")

	keysByRegion, _, err := lr.store.GetRegionCache().GroupKeysByRegion(bo, data.keys, nil)
	if err != nil {
		return err
	}
	errChan := make(chan error, len(keysByRegion))
	// Resolve every lock in the transaction.
	for region, locks := range keysByRegion {
		curLocks := locks
		curRegion := region
		resolveBo, cancel := bo.Fork()
		defer cancel()

		go func() {
			errChan <- lr.resolveRegionLocks(resolveBo, l, curRegion, curLocks, status)
		}()
	}

	var errs []string
	for range keysByRegion {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	if len(errs) > 0 {
		return errors.Errorf("async commit recovery (sending ResolveLock) finished with errors: %v", errs)
	}

	return nil
}

// resolveLockAsync resolves l assuming it was locked using the async commit protocol.
func (lr *LockResolver) resolveAsyncCommitLock(bo *retry.Backoffer, l *Lock, status TxnStatus, asyncResolveAll bool) (TxnStatus, error) {
	metrics.LockResolverCountWithResolveAsync.Inc()

	resolveData, err := lr.checkAllSecondaries(bo, l, &status)
	if err != nil {
		return TxnStatus{}, err
	}
	resolveData.keys = append(resolveData.keys, l.Primary)

	status.commitTS = resolveData.commitTs
	if status.StatusCacheable() {
		lr.saveResolved(l.TxnID, status)
	}

	logutil.BgLogger().Info("resolve async commit", zap.Uint64("startTS", l.TxnID), zap.Uint64("commitTS", status.commitTS))
	if asyncResolveAll {
		asyncBo := retry.NewBackoffer(lr.asyncResolveCtx, asyncResolveLockMaxBackoff)
		go func() {
			err := lr.resolveAsyncResolveData(asyncBo, l, status, resolveData)
			if err != nil {
				logutil.BgLogger().Info("failed to resolve async-commit locks asynchronously",
					zap.Uint64("startTS", l.TxnID), zap.Uint64("commitTS", status.CommitTS()), zap.Error(err))
			}
		}()
	} else {
		err = lr.resolveAsyncResolveData(bo, l, status, resolveData)
	}
	return status, err
}

// checkAllSecondaries checks the secondary locks of an async commit transaction to find out the final
// status of the transaction
func (lr *LockResolver) checkAllSecondaries(bo *retry.Backoffer, l *Lock, status *TxnStatus) (*asyncResolveData, error) {
	regions, _, err := lr.store.GetRegionCache().GroupKeysByRegion(bo, status.primaryLock.Secondaries, nil)
	if err != nil {
		return nil, err
	}

	shared := asyncResolveData{
		mutex:       sync.Mutex{},
		commitTs:    status.primaryLock.MinCommitTs,
		keys:        [][]byte{},
		missingLock: false,
	}

	errChan := make(chan error, len(regions))
	for regionID, keys := range regions {
		curRegionID := regionID
		curKeys := keys
		checkBo, cancel := bo.Fork()
		defer cancel()

		go func() {
			errChan <- lr.checkSecondaries(checkBo, l.TxnID, curKeys, curRegionID, &shared)
		}()
	}

	for range regions {
		err := <-errChan
		if err != nil {
			return nil, err
		}
	}

	return &shared, nil
}

// resolveRegionLocks is essentially the same as resolveLock, but we resolve all keys in the same region at the same time.
func (lr *LockResolver) resolveRegionLocks(bo *retry.Backoffer, l *Lock, region locate.RegionVerID, keys [][]byte, status TxnStatus) error {
	lreq := &kvrpcpb.ResolveLockRequest{
		StartVersion: l.TxnID,
	}
	if status.IsCommitted() {
		lreq.CommitVersion = status.CommitTS()
	}
	lreq.Keys = keys
	req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, lreq, kvrpcpb.Context{
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: util.ResourceGroupNameFromCtx(bo.GetCtx()),
		},
	})
	req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
	resp, err := lr.store.SendReq(bo, req, region, client.ReadTimeoutShort)
	if err != nil {
		return err
	}

	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	if regionErr != nil {
		err := bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return err
		}

		logutil.BgLogger().Info("resolveRegionLocks region error, regrouping", zap.String("lock", l.String()), zap.Uint64("region", region.GetID()))

		// Regroup locks.
		regions, _, err := lr.store.GetRegionCache().GroupKeysByRegion(bo, keys, nil)
		if err != nil {
			return err
		}
		for regionID, keys := range regions {
			// Recursion will terminate because the resolve request succeeds or the Backoffer reaches its limit.
			if err = lr.resolveRegionLocks(bo, l, regionID, keys, status); err != nil {
				return err
			}
		}
		return nil
	}
	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
	if keyErr := cmdResp.GetError(); keyErr != nil {
		err = errors.Errorf("unexpected resolve err: %s, lock: %v", keyErr, l)
		logutil.BgLogger().Error("resolveLock error", zap.Error(err))
	}

	return nil
}

func (lr *LockResolver) resolveLock(bo *retry.Backoffer, l *Lock, status TxnStatus, lite bool, cleanRegions map[locate.RegionVerID]struct{}) error {
	util.EvalFailpoint("resolveLock")

	metrics.LockResolverCountWithResolveLocks.Inc()
	resolveLite := lite || l.TxnSize < lr.resolveLockLiteThreshold
	// The lock has been resolved by getTxnStatusFromLock.
	if resolveLite && bytes.Equal(l.Key, l.Primary) {
		return nil
	}
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return err
		}
		if _, ok := cleanRegions[loc.Region]; ok {
			return nil
		}
		lreq := &kvrpcpb.ResolveLockRequest{
			StartVersion: l.TxnID,
		}
		if status.IsCommitted() {
			lreq.CommitVersion = status.CommitTS()
		} else {
			logutil.BgLogger().Info("resolveLock rollback", zap.String("lock", l.String()))
		}

		if resolveLite {
			// Only resolve specified keys when it is a small transaction,
			// prevent from scanning the whole region in this case.
			metrics.LockResolverCountWithResolveLockLite.Inc()
			lreq.Keys = [][]byte{l.Key}
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, lreq, kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: util.ResourceGroupNameFromCtx(bo.GetCtx()),
			},
		})
		req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
		req.RequestSource = util.RequestSourceFromCtx(bo.GetCtx())
		resp, err := lr.store.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
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
			continue
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected resolve err: %s, lock: %v", keyErr, l)
			logutil.BgLogger().Error("resolveLock error", zap.Error(err))
			return err
		}
		if !resolveLite {
			cleanRegions[loc.Region] = struct{}{}
		}
		return nil
	}
}

// resolvePessimisticLock handles pessimistic locks after checking txn status.
// Note that this function assumes `CheckTxnStatus` is done (or `getTxnStatusFromLock` has been called) on the lock.
func (lr *LockResolver) resolvePessimisticLock(bo *retry.Backoffer, l *Lock) error {
	metrics.LockResolverCountWithResolveLocks.Inc()
	// The lock has been resolved by getTxnStatusFromLock.
	if bytes.Equal(l.Key, l.Primary) {
		return nil
	}
	for {
		loc, err := lr.store.GetRegionCache().LocateKey(bo, l.Key)
		if err != nil {
			return err
		}
		forUpdateTS := l.LockForUpdateTS
		if forUpdateTS == 0 {
			forUpdateTS = math.MaxUint64
		}
		pessimisticRollbackReq := &kvrpcpb.PessimisticRollbackRequest{
			StartVersion: l.TxnID,
			ForUpdateTs:  forUpdateTS,
			Keys:         [][]byte{l.Key},
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, pessimisticRollbackReq, kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: util.ResourceGroupNameFromCtx(bo.GetCtx()),
			},
		})
		req.MaxExecutionDurationMs = uint64(client.MaxWriteExecutionTime.Milliseconds())
		resp, err := lr.store.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
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
			continue
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		cmdResp := resp.Resp.(*kvrpcpb.PessimisticRollbackResponse)
		if keyErr := cmdResp.GetErrors(); len(keyErr) > 0 {
			err = errors.Errorf("unexpected resolve pessimistic lock err: %s, lock: %v", keyErr[0], l)
			logutil.Logger(bo.GetCtx()).Error("resolveLock error", zap.Error(err))
			return err
		}
		return nil
	}
}
