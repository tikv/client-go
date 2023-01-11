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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/txn.go
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

package transaction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/internal/unionstore"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/txnkv/txnutil"
	"github.com/tikv/client-go/v2/util"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// MaxTxnTimeUse is the max time a Txn may use (in ms) from its begin to commit.
// We use it to abort the transaction to guarantee GC worker will not influence it.
const MaxTxnTimeUse = 24 * 60 * 60 * 1000

type tempLockBufferEntry struct {
	HasReturnValue    bool
	HasCheckExistence bool
	Value             tikv.ReturnedValue
}

// trySkipLockingOnRetry checks if the key can be skipped in an aggressive locking context, and performs necessary
// changes to the entry. Returns whether the key can be skipped.
func (e *tempLockBufferEntry) trySkipLockingOnRetry(returnValue bool, checkExistence bool) bool {
	if e.Value.LockedWithConflictTS != 0 {
		// Clear its LockedWithConflictTS field as if it's locked in normal way.
		e.Value.LockedWithConflictTS = 0
	} else {
		// If we require more information than those we already got during last attempt, we need to lock it again.
		if !e.HasReturnValue && returnValue {
			return false
		}
		if !returnValue && !e.HasCheckExistence && checkExistence {
			return false
		}
	}
	if !returnValue {
		e.HasReturnValue = false
		e.Value.Value = nil
	}
	if !checkExistence {
		e.HasCheckExistence = false
		e.Value.Exists = true
	}
	return true
}

// TxnOptions indicates the option when beginning a transaction.
// TxnOptions are set by the TxnOption values passed to Begin
type TxnOptions struct {
	TxnScope string
	StartTS  *uint64
}

// KVTxn contains methods to interact with a TiKV transaction.
type KVTxn struct {
	snapshot  *txnsnapshot.KVSnapshot
	us        *unionstore.KVUnionStore
	store     kvstore // for connection to region.
	startTS   uint64
	startTime time.Time // Monotonic timestamp for recording txn time consuming.
	commitTS  uint64
	mu        sync.Mutex // For thread-safe LockKeys function.
	setCnt    int64
	vars      *tikv.Variables
	committer *twoPhaseCommitter
	lockedCnt int

	valid bool

	// schemaVer is the infoSchema fetched at startTS.
	schemaVer SchemaVer
	// commitCallback is called after current transaction gets committed
	commitCallback func(info string, err error)

	binlog                  BinlogExecutor
	schemaLeaseChecker      SchemaLeaseChecker
	syncLog                 bool
	priority                txnutil.Priority
	isPessimistic           bool
	enableAsyncCommit       bool
	enable1PC               bool
	causalConsistency       bool
	scope                   string
	kvFilter                KVFilter
	resourceGroupTag        []byte
	resourceGroupTagger     tikvrpc.ResourceGroupTagger // use this when resourceGroupTag is nil
	diskFullOpt             kvrpcpb.DiskFullOpt
	txnSource               uint64
	commitTSUpperBoundCheck func(uint64) bool
	// interceptor is used to decorate the RPC request logic related to the txn.
	interceptor    interceptor.RPCInterceptor
	assertionLevel kvrpcpb.AssertionLevel
	*util.RequestSource
	// resourceGroupName is the name of tenent resource group.
	resourceGroupName string

	aggressiveLockingContext *aggressiveLockingContext
	aggressiveLockingDirty   bool
}

// NewTiKVTxn creates a new KVTxn.
func NewTiKVTxn(store kvstore, snapshot *txnsnapshot.KVSnapshot, startTS uint64, options *TxnOptions) (*KVTxn, error) {
	cfg := config.GetGlobalConfig()
	newTiKVTxn := &KVTxn{
		snapshot:          snapshot,
		us:                unionstore.NewUnionStore(snapshot),
		store:             store,
		startTS:           startTS,
		startTime:         time.Now(),
		valid:             true,
		vars:              tikv.DefaultVars,
		scope:             options.TxnScope,
		enableAsyncCommit: cfg.EnableAsyncCommit,
		enable1PC:         cfg.Enable1PC,
		diskFullOpt:       kvrpcpb.DiskFullOpt_NotAllowedOnFull,
		RequestSource:     snapshot.RequestSource,
	}
	return newTiKVTxn, nil
}

// SetSuccess is used to probe if kv variables are set or not. It is ONLY used in test cases.
var SetSuccess = *atomicutil.NewBool(false)

// SetVars sets variables to the transaction.
func (txn *KVTxn) SetVars(vars *tikv.Variables) {
	txn.vars = vars
	txn.snapshot.SetVars(vars)
	if val, err := util.EvalFailpoint("probeSetVars"); err == nil {
		if val.(bool) {
			SetSuccess.Store(true)
		}
	}
}

// GetVars gets variables from the transaction.
func (txn *KVTxn) GetVars() *tikv.Variables {
	return txn.vars
}

// Get implements transaction interface.
func (txn *KVTxn) Get(ctx context.Context, k []byte) ([]byte, error) {
	ret, err := txn.us.Get(ctx, k)
	if tikverr.IsErrNotFound(err) {
		return nil, err
	}
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// BatchGet gets kv from the memory buffer of statement and transaction, and the kv storage.
// Do not use len(value) == 0 or value == nil to represent non-exist.
// If a key doesn't exist, there shouldn't be any corresponding entry in the result map.
func (txn *KVTxn) BatchGet(ctx context.Context, keys [][]byte) (map[string][]byte, error) {
	return NewBufferBatchGetter(txn.GetMemBuffer(), txn.GetSnapshot()).BatchGet(ctx, keys)
}

// Set sets the value for key k as v into kv store.
// v must NOT be nil or empty, otherwise it returns ErrCannotSetNilValue.
func (txn *KVTxn) Set(k []byte, v []byte) error {
	txn.setCnt++
	return txn.us.GetMemBuffer().Set(k, v)
}

// String implements fmt.Stringer interface.
func (txn *KVTxn) String() string {
	res := fmt.Sprintf("%d", txn.StartTS())
	if txn.aggressiveLockingContext != nil {
		res += fmt.Sprintf(" (aggressiveLocking: prev %d keys, current %d keys)", len(txn.aggressiveLockingContext.lastRetryUnnecessaryLocks), len(txn.aggressiveLockingContext.currentLockedKeys))
	}
	return res
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (txn *KVTxn) Iter(k []byte, upperBound []byte) (unionstore.Iterator, error) {
	return txn.us.Iter(k, upperBound)
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
func (txn *KVTxn) IterReverse(k []byte) (unionstore.Iterator, error) {
	return txn.us.IterReverse(k)
}

// Delete removes the entry for key k from kv store.
func (txn *KVTxn) Delete(k []byte) error {
	return txn.us.GetMemBuffer().Delete(k)
}

// SetSchemaLeaseChecker sets a hook to check schema version.
func (txn *KVTxn) SetSchemaLeaseChecker(checker SchemaLeaseChecker) {
	txn.schemaLeaseChecker = checker
}

// EnableForceSyncLog indicates tikv to always sync log for the transaction.
func (txn *KVTxn) EnableForceSyncLog() {
	txn.syncLog = true
}

// SetPessimistic indicates if the transaction should use pessimictic lock.
func (txn *KVTxn) SetPessimistic(b bool) {
	txn.isPessimistic = b
}

// SetSchemaVer updates schema version to validate transaction.
func (txn *KVTxn) SetSchemaVer(schemaVer SchemaVer) {
	txn.schemaVer = schemaVer
}

// SetPriority sets the priority for both write and read.
func (txn *KVTxn) SetPriority(pri txnutil.Priority) {
	txn.priority = pri
	txn.GetSnapshot().SetPriority(pri)
}

// SetResourceGroupTag sets the resource tag for both write and read.
func (txn *KVTxn) SetResourceGroupTag(tag []byte) {
	txn.resourceGroupTag = tag
	txn.GetSnapshot().SetResourceGroupTag(tag)
}

// SetResourceGroupTagger sets the resource tagger for both write and read.
// Before sending the request, if resourceGroupTag is not nil, use
// resourceGroupTag directly, otherwise use resourceGroupTagger.
func (txn *KVTxn) SetResourceGroupTagger(tagger tikvrpc.ResourceGroupTagger) {
	txn.resourceGroupTagger = tagger
	txn.GetSnapshot().SetResourceGroupTagger(tagger)
}

// SetResourceGroupName set resource group name for both read and write.
func (txn *KVTxn) SetResourceGroupName(name string) {
	txn.resourceGroupName = name
	txn.GetSnapshot().SetResourceGroupName(name)
}

// SetRPCInterceptor sets interceptor.RPCInterceptor for the transaction and its related snapshot.
// interceptor.RPCInterceptor will be executed before each RPC request is initiated.
// Note that SetRPCInterceptor will replace the previously set interceptor.
func (txn *KVTxn) SetRPCInterceptor(it interceptor.RPCInterceptor) {
	txn.interceptor = it
	txn.GetSnapshot().SetRPCInterceptor(it)
}

// AddRPCInterceptor adds an interceptor, the order of addition is the order of execution.
func (txn *KVTxn) AddRPCInterceptor(it interceptor.RPCInterceptor) {
	if txn.interceptor == nil {
		txn.SetRPCInterceptor(it)
		return
	}
	txn.interceptor = interceptor.ChainRPCInterceptors(txn.interceptor, it)
	txn.GetSnapshot().AddRPCInterceptor(it)
}

// SetCommitCallback sets up a function that will be called when the transaction
// is finished.
func (txn *KVTxn) SetCommitCallback(f func(string, error)) {
	txn.commitCallback = f
}

// SetEnableAsyncCommit indicates if the transaction will try to use async commit.
func (txn *KVTxn) SetEnableAsyncCommit(b bool) {
	txn.enableAsyncCommit = b
}

// SetEnable1PC indicates that the transaction will try to use 1 phase commit(which should be faster).
// 1PC does not work if the keys to update in the current txn are in multiple regions.
func (txn *KVTxn) SetEnable1PC(b bool) {
	txn.enable1PC = b
}

// SetCausalConsistency indicates if the transaction does not need to
// guarantee linearizability. Default value is false which means
// linearizability is guaranteed.
func (txn *KVTxn) SetCausalConsistency(b bool) {
	txn.causalConsistency = b
}

// SetScope sets the geographical scope of the transaction.
func (txn *KVTxn) SetScope(scope string) {
	txn.scope = scope
}

// SetKVFilter sets the filter to ignore key-values in memory buffer.
func (txn *KVTxn) SetKVFilter(filter KVFilter) {
	txn.kvFilter = filter
}

// SetCommitTSUpperBoundCheck provide a way to restrict the commit TS upper bound.
// The 2PC processing will pass the commitTS for the checker function, if the function
// returns false, the 2PC processing will abort.
func (txn *KVTxn) SetCommitTSUpperBoundCheck(f func(commitTS uint64) bool) {
	txn.commitTSUpperBoundCheck = f
}

// SetDiskFullOpt sets whether current operation is allowed in each TiKV disk usage level.
func (txn *KVTxn) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	txn.diskFullOpt = level
}

// SetTxnSource sets the source of the transaction.
func (txn *KVTxn) SetTxnSource(txnSource uint64) {
	txn.txnSource = txnSource
}

// GetDiskFullOpt gets the options of current operation in each TiKV disk usage level.
func (txn *KVTxn) GetDiskFullOpt() kvrpcpb.DiskFullOpt {
	return txn.diskFullOpt
}

// ClearDiskFullOpt clears the options of current operation in each tikv disk usage level.
func (txn *KVTxn) ClearDiskFullOpt() {
	txn.diskFullOpt = kvrpcpb.DiskFullOpt_NotAllowedOnFull
}

// SetAssertionLevel sets how strict the assertions in the transaction should be.
func (txn *KVTxn) SetAssertionLevel(assertionLevel kvrpcpb.AssertionLevel) {
	txn.assertionLevel = assertionLevel
}

// IsPessimistic returns true if it is pessimistic.
func (txn *KVTxn) IsPessimistic() bool {
	return txn.isPessimistic
}

// IsCasualConsistency returns if the transaction allows linearizability
// inconsistency.
func (txn *KVTxn) IsCasualConsistency() bool {
	return txn.causalConsistency
}

// GetScope returns the geographical scope of the transaction.
func (txn *KVTxn) GetScope() string {
	return txn.scope
}

// Commit commits the transaction operations to KV store.
func (txn *KVTxn) Commit(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikvTxn.Commit", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	defer trace.StartRegion(ctx, "CommitTxn").End()

	if !txn.valid {
		return tikverr.ErrInvalidTxn
	}
	defer txn.close()

	ctx = context.WithValue(ctx, util.RequestSourceKey, *txn.RequestSource)

	if txn.aggressiveLockingContext != nil {
		if len(txn.aggressiveLockingContext.currentLockedKeys) != 0 {
			return errors.New("trying to commit transaction when aggressive locking is pending")
		}
		txn.CancelAggressiveLocking(ctx)
	}

	if val, err := util.EvalFailpoint("mockCommitError"); err == nil && val.(bool) {
		if _, err := util.EvalFailpoint("mockCommitErrorOpt"); err == nil {
			failpoint.Disable("tikvclient/mockCommitErrorOpt")
			return errors.New("mock commit error")
		}
	}

	start := time.Now()
	defer func() { metrics.TxnCmdHistogramWithCommit.Observe(time.Since(start).Seconds()) }()

	// sessionID is used for log.
	var sessionID uint64
	val := ctx.Value(util.SessionID)
	if val != nil {
		sessionID = val.(uint64)
	}

	if txn.interceptor != nil {
		// User has called txn.SetRPCInterceptor() to explicitly set an interceptor, we
		// need to bind it to ctx so that the internal client can perceive and execute
		// it before initiating an RPC request.
		ctx = interceptor.WithRPCInterceptor(ctx, txn.interceptor)
	}

	var err error
	// If the txn use pessimistic lock, committer is initialized.
	committer := txn.committer
	if committer == nil {
		committer, err = newTwoPhaseCommitter(txn, sessionID)
		if err != nil {
			return err
		}
		txn.committer = committer
	}

	txn.committer.SetDiskFullOpt(txn.diskFullOpt)
	txn.committer.SetTxnSource(txn.txnSource)

	defer committer.ttlManager.close()

	initRegion := trace.StartRegion(ctx, "InitKeys")
	err = committer.initKeysAndMutations(ctx)
	initRegion.End()
	if err != nil {
		if txn.IsPessimistic() {
			txn.asyncPessimisticRollback(ctx, committer.mutations.GetKeys(), txn.committer.forUpdateTS)
		}
		return err
	}
	if committer.mutations.Len() == 0 {
		return nil
	}

	defer func() {
		detail := committer.getDetail()
		detail.Mu.Lock()
		metrics.TiKVTxnCommitBackoffSeconds.Observe(float64(detail.Mu.CommitBackoffTime) / float64(time.Second))
		metrics.TiKVTxnCommitBackoffCount.Observe(float64(len(detail.Mu.PrewriteBackoffTypes) + len(detail.Mu.CommitBackoffTypes)))
		detail.Mu.Unlock()

		ctxValue := ctx.Value(util.CommitDetailCtxKey)
		if ctxValue != nil {
			commitDetail := ctxValue.(**util.CommitDetails)
			if *commitDetail != nil {
				(*commitDetail).TxnRetry++
			} else {
				*commitDetail = detail
			}
		}
	}()
	// latches disabled
	// pessimistic transaction should also bypass latch.
	if txn.store.TxnLatches() == nil || txn.IsPessimistic() {
		err = committer.execute(ctx)
		if val == nil || sessionID > 0 {
			txn.onCommitted(err)
		}
		logutil.Logger(ctx).Debug("[kv] txnLatches disabled, 2pc directly", zap.Error(err))
		return err
	}

	// latches enabled
	// for transactions which need to acquire latches
	start = time.Now()
	lock := txn.store.TxnLatches().Lock(committer.startTS, committer.mutations.GetKeys())
	commitDetail := committer.getDetail()
	commitDetail.LocalLatchTime = time.Since(start)
	if commitDetail.LocalLatchTime > 0 {
		metrics.TiKVLocalLatchWaitTimeHistogram.Observe(commitDetail.LocalLatchTime.Seconds())
	}
	defer txn.store.TxnLatches().UnLock(lock)
	if lock.IsStale() {
		return &tikverr.ErrWriteConflictInLatch{StartTS: txn.startTS}
	}
	err = committer.execute(ctx)
	if val == nil || sessionID > 0 {
		txn.onCommitted(err)
	}
	if err == nil {
		lock.SetCommitTS(committer.commitTS)
	}
	logutil.Logger(ctx).Debug("[kv] txnLatches enabled while txn retryable", zap.Error(err))
	return err
}

func (txn *KVTxn) close() {
	txn.valid = false
	txn.ClearDiskFullOpt()
}

// Rollback undoes the transaction operations to KV store.
func (txn *KVTxn) Rollback() error {
	if !txn.valid {
		return tikverr.ErrInvalidTxn
	}

	if txn.aggressiveLockingContext != nil {
		if len(txn.aggressiveLockingContext.currentLockedKeys) != 0 {
			txn.close()
			return errors.New("trying to rollback transaction when aggressive locking is pending")
		}
		txn.CancelAggressiveLocking(context.Background())
	}

	start := time.Now()
	// Clean up pessimistic lock.
	if txn.IsPessimistic() && txn.committer != nil {
		err := txn.rollbackPessimisticLocks()
		txn.committer.ttlManager.close()
		if err != nil {
			logutil.BgLogger().Error(err.Error())
		}
	}
	txn.close()
	logutil.BgLogger().Debug("[kv] rollback txn", zap.Uint64("txnStartTS", txn.StartTS()))
	metrics.TxnCmdHistogramWithRollback.Observe(time.Since(start).Seconds())
	return nil
}

func (txn *KVTxn) rollbackPessimisticLocks() error {
	if txn.lockedCnt == 0 {
		return nil
	}
	ctx := context.WithValue(context.Background(), util.RequestSourceKey, *txn.RequestSource)
	bo := retry.NewBackofferWithVars(ctx, cleanupMaxBackoff, txn.vars)
	if txn.interceptor != nil {
		// User has called txn.SetRPCInterceptor() to explicitly set an interceptor, we
		// need to bind it to ctx so that the internal client can perceive and execute
		// it before initiating an RPC request.
		bo.SetCtx(interceptor.WithRPCInterceptor(bo.GetCtx(), txn.interceptor))
	}
	keys := txn.collectLockedKeys()
	return txn.committer.pessimisticRollbackMutations(bo, &PlainMutations{keys: keys})
}

func (txn *KVTxn) collectLockedKeys() [][]byte {
	keys := make([][]byte, 0, txn.lockedCnt)
	buf := txn.GetMemBuffer()
	var err error
	for it := buf.IterWithFlags(nil, nil); it.Valid(); err = it.Next() {
		_ = err
		if it.Flags().HasLocked() {
			keys = append(keys, it.Key())
		}
	}
	return keys
}

// TxnInfo is used to keep track the info of a committed transaction (mainly for diagnosis and testing)
type TxnInfo struct {
	TxnScope            string `json:"txn_scope"`
	StartTS             uint64 `json:"start_ts"`
	CommitTS            uint64 `json:"commit_ts"`
	TxnCommitMode       string `json:"txn_commit_mode"`
	AsyncCommitFallback bool   `json:"async_commit_fallback"`
	OnePCFallback       bool   `json:"one_pc_fallback"`
	ErrMsg              string `json:"error,omitempty"`
}

func (txn *KVTxn) onCommitted(err error) {
	if txn.commitCallback != nil {
		isAsyncCommit := txn.committer.isAsyncCommit()
		isOnePC := txn.committer.isOnePC()

		commitMode := "2pc"
		if isOnePC {
			commitMode = "1pc"
		} else if isAsyncCommit {
			commitMode = "async_commit"
		}

		info := TxnInfo{
			TxnScope:            txn.GetScope(),
			StartTS:             txn.startTS,
			CommitTS:            txn.commitTS,
			TxnCommitMode:       commitMode,
			AsyncCommitFallback: txn.committer.hasTriedAsyncCommit && !isAsyncCommit,
			OnePCFallback:       txn.committer.hasTriedOnePC && !isOnePC,
		}
		if err != nil {
			info.ErrMsg = err.Error()
		}
		infoStr, err2 := json.Marshal(info)
		_ = err2
		txn.commitCallback(string(infoStr), err)
	}
}

// LockKeysWithWaitTime tries to lock the entries with the keys in KV store.
// lockWaitTime in ms, 0 means nowait lock.
func (txn *KVTxn) LockKeysWithWaitTime(ctx context.Context, lockWaitTime int64, keysInput ...[]byte) (err error) {
	forUpdateTs := txn.startTS
	if txn.IsPessimistic() {
		bo := retry.NewBackofferWithVars(context.Background(), TsoMaxBackoff, nil)
		forUpdateTs, err = txn.store.GetTimestampWithRetry(bo, txn.scope)
		if err != nil {
			return err
		}
	}
	lockCtx := tikv.NewLockCtx(forUpdateTs, lockWaitTime, time.Now())

	return txn.LockKeys(ctx, lockCtx, keysInput...)
}

// StartAggressiveLocking makes the transaction enters aggressive locking state.
//
// Aggressive locking refers to the behavior that when a DML in a pessimistic transaction encounters write conflict,
// do not pessimistic-rollback them immediately; instead, keep the already-acquired locks and retry the statement.
// In this way, during retry, if it needs to acquire the same locks that was acquired in the previous execution, the
// lock RPC can be skipped. After finishing the execution, if some of the locks that were acquired in the previous
// execution but not needed in the current retried execution, they will be released.
//
// In aggressive locking state, keys locked by `LockKeys` will be recorded to a separated buffer. For `LockKeys`
// invocations that involves only one key, the pessimistic lock request will be performed in ForceLock mode
// (kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeForceLock).
func (txn *KVTxn) StartAggressiveLocking() {
	if txn.aggressiveLockingContext != nil {
		panic("Trying to start aggressive locking while it's already started")
	}
	txn.aggressiveLockingContext = &aggressiveLockingContext{
		lastRetryUnnecessaryLocks: nil,
		currentLockedKeys:         make(map[string]tempLockBufferEntry),
		startTime:                 time.Now(),
	}
}

// RetryAggressiveLocking tells the transaction that the current statement will be retried (ends the current attempt
// and starts the next attempt).
// If some keys are already locked during the aggressive locking, after calling this function, these locks will then be
// regarded as being acquired in the previous attempt.
// If some locks is acquired in the previous attempt but not needed in the current attempt, after calling this function,
// these locks will then be released.
func (txn *KVTxn) RetryAggressiveLocking(ctx context.Context) {
	if txn.aggressiveLockingContext == nil {
		panic("Trying to retry aggressive locking while it's not started")
	}
	txn.cleanupAggressiveLockingRedundantLocks(ctx)
	if txn.aggressiveLockingContext.assignedPrimaryKey {
		txn.resetPrimary()
		txn.aggressiveLockingContext.assignedPrimaryKey = false
	}

	txn.aggressiveLockingContext.lastPrimaryKey = txn.aggressiveLockingContext.primaryKey
	txn.aggressiveLockingContext.primaryKey = nil

	txn.aggressiveLockingContext.lastAttemptStartTime = txn.aggressiveLockingContext.startTime
	txn.aggressiveLockingContext.startTime = time.Now()

	txn.aggressiveLockingContext.lastRetryUnnecessaryLocks = txn.aggressiveLockingContext.currentLockedKeys
	txn.aggressiveLockingContext.currentLockedKeys = make(map[string]tempLockBufferEntry)
}

// CancelAggressiveLocking cancels the current aggressive locking state. All pessimistic locks that were acquired
// during the aggressive locking state will be rolled back by PessimisticRollback.
func (txn *KVTxn) CancelAggressiveLocking(ctx context.Context) {
	if txn.aggressiveLockingContext == nil {
		panic("Trying to cancel aggressive locking while it's not started")
	}
	txn.cleanupAggressiveLockingRedundantLocks(context.Background())
	if txn.aggressiveLockingContext.assignedPrimaryKey {
		txn.resetPrimary()
		txn.aggressiveLockingContext.assignedPrimaryKey = false
	}

	keys := make([][]byte, 0, len(txn.aggressiveLockingContext.currentLockedKeys))
	for key := range txn.aggressiveLockingContext.currentLockedKeys {
		keys = append(keys, []byte(key))
	}
	if len(keys) != 0 {
		// The committer must have been initialized if some keys are locked
		forUpdateTS := txn.committer.forUpdateTS
		if txn.aggressiveLockingContext.maxLockedWithConflictTS > forUpdateTS {
			forUpdateTS = txn.aggressiveLockingContext.maxLockedWithConflictTS
		}
		txn.asyncPessimisticRollback(context.Background(), keys, forUpdateTS)
		txn.lockedCnt -= len(keys)
	}
	txn.aggressiveLockingContext = nil
}

// DoneAggressiveLocking finishes the current aggressive locking. The locked keys will be moved to the membuffer as if
// these keys are locked in nomral way. If there's any unneeded locks, they will be released.
func (txn *KVTxn) DoneAggressiveLocking(ctx context.Context) {
	if txn.aggressiveLockingContext == nil {
		panic("Trying to finish aggressive locking while it's not started")
	}
	txn.cleanupAggressiveLockingRedundantLocks(context.Background())
	memBuffer := txn.GetMemBuffer()
	for key, entry := range txn.aggressiveLockingContext.currentLockedKeys {
		setValExists := tikv.SetKeyLockedValueExists
		if (entry.HasCheckExistence || entry.HasReturnValue) && !entry.Value.Exists {
			setValExists = tikv.SetKeyLockedValueNotExists
		}
		memBuffer.UpdateFlags([]byte(key), tikv.SetKeyLocked, tikv.DelNeedCheckExists, setValExists)
	}
	if txn.aggressiveLockingContext.maxLockedWithConflictTS > 0 {
		// There are some keys locked so the committer must have been created.
		if txn.aggressiveLockingContext.maxLockedWithConflictTS > txn.committer.maxLockedWithConflictTS {
			txn.committer.maxLockedWithConflictTS = txn.aggressiveLockingContext.maxLockedWithConflictTS
		}
	}
	txn.aggressiveLockingContext = nil
}

// IsInAggressiveLockingMode checks if the transaction is currently in aggressive locking mode.
func (txn *KVTxn) IsInAggressiveLockingMode() bool {
	return txn.aggressiveLockingContext != nil
}

// IsInAggressiveLockingStage checks if a key is locked during the current aggressive locking stage.
func (txn *KVTxn) IsInAggressiveLockingStage(key []byte) bool {
	if txn.aggressiveLockingContext != nil {
		_, ok := txn.aggressiveLockingContext.currentLockedKeys[string(key)]
		return ok
	}
	return false
}

func (txn *KVTxn) mayAggressiveLockingLastLockedKeysExpire() bool {
	ttl := atomic.LoadUint64(&ManagedLockTTL)
	return ttl <= math.MaxInt64 &&
		time.Since(txn.aggressiveLockingContext.lastAttemptStartTime).Milliseconds() >= int64(ttl)
}

func (txn *KVTxn) cleanupAggressiveLockingRedundantLocks(ctx context.Context) {
	if len(txn.aggressiveLockingContext.lastRetryUnnecessaryLocks) == 0 {
		return
	}
	keys := make([][]byte, 0, len(txn.aggressiveLockingContext.lastRetryUnnecessaryLocks))
	for keyStr := range txn.aggressiveLockingContext.lastRetryUnnecessaryLocks {
		key := []byte(keyStr)
		keys = append(keys, key)
	}
	if len(keys) != 0 {
		// The committer must have been initialized if some keys are locked
		forUpdateTS := txn.committer.forUpdateTS
		if txn.aggressiveLockingContext.maxLockedWithConflictTS > forUpdateTS {
			forUpdateTS = txn.aggressiveLockingContext.maxLockedWithConflictTS
		}
		txn.asyncPessimisticRollback(ctx, keys, forUpdateTS)
		txn.lockedCnt -= len(keys)
	}
}

func (txn *KVTxn) filterAggressiveLockedKeys(lockCtx *tikv.LockCtx, allKeys [][]byte) ([][]byte, error) {
	// In aggressive locking mode, we can skip locking if all of these conditions are met:
	// * The primary is unchanged during the current aggressive locking (which means primary is already set
	//   before the current aggressive locking or the selected primary is the same as that selected during the
	//   previous attempt).
	// * The key is already locked in the previous attempt.
	// * The time since last attempt is short enough so that the locks we acquired during last attempt is
	//   unlikely to be resolved by other transactions.

	// In case primary is not assigned in this phase, or primary is already set but unchanged, we don't need
	// to update the locks.
	canTrySkip := !txn.aggressiveLockingContext.assignedPrimaryKey || bytes.Equal(txn.aggressiveLockingContext.lastPrimaryKey, txn.aggressiveLockingContext.primaryKey)

	// Do not preallocate since in most cases the keys need to lock doesn't change during pessimistic-retry.
	keys := make([][]byte, 0)
	for _, k := range allKeys {
		keyStr := string(k)
		if lastResult, ok := txn.aggressiveLockingContext.lastRetryUnnecessaryLocks[keyStr]; ok {
			if lockCtx.ForUpdateTS < lastResult.Value.LockedWithConflictTS {
				// This should be an unreachable path.
				return nil, errors.Errorf("Txn %v Retrying aggressive locking with ForUpdateTS (%v) less than previous LockedWithConflictTS (%v)", txn.StartTS(), lockCtx.ForUpdateTS, lastResult.Value.LockedWithConflictTS)
			}
			delete(txn.aggressiveLockingContext.lastRetryUnnecessaryLocks, keyStr)
			if canTrySkip &&
				lastResult.trySkipLockingOnRetry(lockCtx.ReturnValues, lockCtx.CheckExistence) &&
				!txn.mayAggressiveLockingLastLockedKeysExpire() {
				// We can skip locking it since it's already locked during last attempt to aggressive locking, and
				// we already have the information that we need.
				if lockCtx.Values != nil {
					lockCtx.Values[keyStr] = lastResult.Value
				}
				txn.aggressiveLockingContext.currentLockedKeys[keyStr] = lastResult
				continue
			}
		}
		keys = append(keys, k)
	}

	return keys, nil
}

// LockKeys tries to lock the entries with the keys in KV store.
// lockCtx is the context for lock, lockCtx.lockWaitTime in ms
func (txn *KVTxn) LockKeys(ctx context.Context, lockCtx *tikv.LockCtx, keysInput ...[]byte) error {
	return txn.lockKeys(ctx, lockCtx, nil, keysInput...)
}

// LockKeysFunc tries to lock the entries with the keys in KV store.
// lockCtx is the context for lock, lockCtx.lockWaitTime in ms
// fn is a function which run before the lock is released.
func (txn *KVTxn) LockKeysFunc(ctx context.Context, lockCtx *tikv.LockCtx, fn func(), keysInput ...[]byte) error {
	return txn.lockKeys(ctx, lockCtx, fn, keysInput...)
}

func (txn *KVTxn) lockKeys(ctx context.Context, lockCtx *tikv.LockCtx, fn func(), keysInput ...[]byte) error {
	if txn.interceptor != nil {
		// User has called txn.SetRPCInterceptor() to explicitly set an interceptor, we
		// need to bind it to ctx so that the internal client can perceive and execute
		// it before initiating an RPC request.
		ctx = interceptor.WithRPCInterceptor(ctx, txn.interceptor)
	}

	ctx = context.WithValue(ctx, util.RequestSourceKey, *txn.RequestSource)
	// Exclude keys that are already locked.
	var err error
	keys := make([][]byte, 0, len(keysInput))
	startTime := time.Now()
	txn.mu.Lock()
	defer txn.mu.Unlock()
	defer func() {
		metrics.TxnCmdHistogramWithLockKeys.Observe(time.Since(startTime).Seconds())
		if err == nil {
			if lockCtx.PessimisticLockWaited != nil {
				if atomic.LoadInt32(lockCtx.PessimisticLockWaited) > 0 {
					timeWaited := time.Since(lockCtx.WaitStartTime)
					atomic.StoreInt64(lockCtx.LockKeysDuration, int64(timeWaited))
					metrics.TiKVPessimisticLockKeysDuration.Observe(timeWaited.Seconds())
				}
			}
		}
		if lockCtx.LockKeysCount != nil {
			*lockCtx.LockKeysCount += int32(len(keys))
		}
		if lockCtx.Stats != nil {
			lockCtx.Stats.TotalTime = time.Since(startTime)
			ctxValue := ctx.Value(util.LockKeysDetailCtxKey)
			if ctxValue != nil {
				lockKeysDetail := ctxValue.(**util.LockKeysDetails)
				*lockKeysDetail = lockCtx.Stats
			}
		}
	}()
	defer func() {
		if fn != nil {
			fn()
		}
	}()

	if !txn.IsPessimistic() && txn.aggressiveLockingContext != nil {
		return errors.New("trying to perform aggressive locking in optimistic transaction")
	}

	memBuf := txn.us.GetMemBuffer()
	// Avoid data race with concurrent updates to the memBuf
	memBuf.RLock()
	for _, key := range keysInput {
		// The value of lockedMap is only used by pessimistic transactions.
		var valueExist, locked, checkKeyExists bool
		if flags, err := memBuf.GetFlags(key); err == nil {
			locked = flags.HasLocked()
			valueExist = flags.HasLockedValueExists()
			checkKeyExists = flags.HasNeedCheckExists()
		}
		// If the key is locked in the current aggressive locking stage, override the information in memBuf.
		isInLastAggressiveLockingStage := false
		if txn.aggressiveLockingContext != nil {
			if entry, ok := txn.aggressiveLockingContext.currentLockedKeys[string(key)]; ok {
				locked = true
				valueExist = entry.Value.Exists
			} else if entry, ok := txn.aggressiveLockingContext.lastRetryUnnecessaryLocks[string(key)]; ok {
				locked = true
				valueExist = entry.Value.Exists
				isInLastAggressiveLockingStage = true
			}
		}

		if !locked || isInLastAggressiveLockingStage {
			// Locks acquired in the previous aggressive locking stage might need to be updated later in
			// `filterAggressiveLockedKeys`.
			keys = append(keys, key)
		}
		if locked && txn.IsPessimistic() {
			if checkKeyExists && valueExist {
				alreadyExist := kvrpcpb.AlreadyExist{Key: key}
				e := &tikverr.ErrKeyExist{AlreadyExist: &alreadyExist}
				memBuf.RUnlock()
				return txn.committer.extractKeyExistsErr(e)
			}
		}
		if lockCtx.ReturnValues && locked {
			keyStr := string(key)
			// An already locked key can not return values, we add an entry to let the caller get the value
			// in other ways.
			lockCtx.Values[keyStr] = tikv.ReturnedValue{AlreadyLocked: true}
		}
	}
	memBuf.RUnlock()

	if len(keys) == 0 {
		return nil
	}
	if lockCtx.LockOnlyIfExists {
		if !lockCtx.ReturnValues {
			return &tikverr.ErrLockOnlyIfExistsNoReturnValue{
				StartTS:     txn.startTS,
				ForUpdateTs: lockCtx.ForUpdateTS,
				LockKey:     keys[0],
			}
		}
		// It can't transform LockOnlyIfExists mode to normal mode. If so, it can add a lock to a key
		// which doesn't exist in tikv. TiDB should ensure that primary key must be set when it sends
		// a LockOnlyIfExists pessmistic lock request.
		if (txn.committer == nil || txn.committer.primaryKey == nil) && len(keys) > 1 {
			return &tikverr.ErrLockOnlyIfExistsNoPrimaryKey{
				StartTS:     txn.startTS,
				ForUpdateTs: lockCtx.ForUpdateTS,
				LockKey:     keys[0],
			}
		}
	}
	keys = deduplicateKeys(keys)
	checkedExistence := false
	var assignedPrimaryKey bool
	if txn.IsPessimistic() && lockCtx.ForUpdateTS > 0 {
		if txn.committer == nil {
			// sessionID is used for log.
			var sessionID uint64
			var err error
			val := ctx.Value(util.SessionID)
			if val != nil {
				sessionID = val.(uint64)
			}
			txn.committer, err = newTwoPhaseCommitter(txn, sessionID)
			if err != nil {
				return err
			}
		}
		if txn.committer.primaryKey == nil {
			assignedPrimaryKey = true
			txn.selectPrimaryForPessimisticLock(keys)
		}

		txn.committer.forUpdateTS = lockCtx.ForUpdateTS

		allKeys := keys

		// If aggressive locking is enabled and we don't need to update the primary for all locks, we can avoid sending
		// RPC to those already locked keys.
		if txn.aggressiveLockingContext != nil {
			keys, err = txn.filterAggressiveLockedKeys(lockCtx, allKeys)
			if err != nil {
				return err
			}

			if len(keys) == 0 {
				return nil
			}
		}

		lockWaitMode := kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeNormal
		if txn.aggressiveLockingContext != nil && len(keys) == 1 && !lockCtx.LockOnlyIfExists {
			lockWaitMode = kvrpcpb.PessimisticLockWakeUpMode_WakeUpModeForceLock
		}
		lockCtx.Stats = &util.LockKeysDetails{
			LockKeys:    int32(len(keys)),
			ResolveLock: util.ResolveLockDetail{},
		}
		bo := retry.NewBackofferWithVars(ctx, pessimisticLockMaxBackoff, txn.vars)
		// If the number of keys greater than 1, it can be on different region,
		// concurrently execute on multiple regions may lead to deadlock.
		txn.committer.isFirstLock = txn.lockedCnt == 0 && len(keys) == 1
		err = txn.committer.pessimisticLockMutations(bo, lockCtx, lockWaitMode, &PlainMutations{keys: keys})
		if lockCtx.Stats != nil && bo.GetTotalSleep() > 0 {
			atomic.AddInt64(&lockCtx.Stats.BackoffTime, int64(bo.GetTotalSleep())*int64(time.Millisecond))
			lockCtx.Stats.Mu.Lock()
			lockCtx.Stats.Mu.BackoffTypes = append(lockCtx.Stats.Mu.BackoffTypes, bo.GetTypes()...)
			lockCtx.Stats.Mu.Unlock()
		}
		if lockCtx.Killed != nil {
			// If the kill signal is received during waiting for pessimisticLock,
			// pessimisticLockKeys would handle the error but it doesn't reset the flag.
			// We need to reset the killed flag here.
			atomic.CompareAndSwapUint32(lockCtx.Killed, 1, 0)
		}
		if txn.aggressiveLockingContext != nil {
			if txn.aggressiveLockingContext.maxLockedWithConflictTS < lockCtx.MaxLockedWithConflictTS {
				txn.aggressiveLockingContext.maxLockedWithConflictTS = lockCtx.MaxLockedWithConflictTS
			}
		}
		if err != nil {
			var unmarkKeys [][]byte
			// Avoid data race with concurrent updates to the memBuf
			memBuf.RLock()
			for _, key := range keys {
				if txn.us.HasPresumeKeyNotExists(key) {
					unmarkKeys = append(unmarkKeys, key)
				}
			}
			memBuf.RUnlock()
			for _, key := range unmarkKeys {
				txn.us.UnmarkPresumeKeyNotExists(key)
			}
			keyMayBeLocked := !(tikverr.IsErrWriteConflict(err) || tikverr.IsErrKeyExist(err))
			// If there is only 1 key and lock fails, no need to do pessimistic rollback.
			if len(keys) > 1 || keyMayBeLocked {
				dl, isDeadlock := errors.Cause(err).(*tikverr.ErrDeadlock)
				if isDeadlock {
					if hashInKeys(dl.DeadlockKeyHash, keys) {
						dl.IsRetryable = true
					}
					if lockCtx.OnDeadlock != nil {
						// Call OnDeadlock before pessimistic rollback.
						lockCtx.OnDeadlock(dl)
					}
				}

				// TODO: It's possible that there are some locks successfully locked with conflict but the client didn't
				//   receive the response due to RPC error. In case the lost LockedWithConflictTS > any received
				//   LockedWithConflictTS, it might not be successfully rolled back here.
				rollbackForUpdateTS := lockCtx.ForUpdateTS
				if lockCtx.MaxLockedWithConflictTS > rollbackForUpdateTS {
					rollbackForUpdateTS = lockCtx.MaxLockedWithConflictTS
				}
				wg := txn.asyncPessimisticRollback(ctx, allKeys, rollbackForUpdateTS)
				txn.lockedCnt -= len(allKeys) - len(keys)
				if txn.aggressiveLockingContext != nil {
					for _, k := range allKeys {
						delete(txn.aggressiveLockingContext.currentLockedKeys, string(k))
					}
				}

				if isDeadlock {
					logutil.Logger(ctx).Debug("deadlock error received", zap.Uint64("startTS", txn.startTS), zap.Stringer("deadlockInfo", dl))
					if dl.IsRetryable {
						// Wait for the pessimistic rollback to finish before we retry the statement.
						wg.Wait()
						// Sleep a little, wait for the other transaction that blocked by this transaction to acquire the lock.
						time.Sleep(time.Millisecond * 5)
						if _, err := util.EvalFailpoint("SingleStmtDeadLockRetrySleep"); err == nil {
							time.Sleep(300 * time.Millisecond)
						}
					}
				}
			}
			if assignedPrimaryKey {
				// unset the primary key and stop heartbeat if we assigned primary key when failed to lock it.
				txn.resetPrimary()
			}
			return err
		}

		if lockCtx.CheckExistence {
			checkedExistence = true
		}
	}
	if assignedPrimaryKey && lockCtx.LockOnlyIfExists {
		if len(keys) != 1 {
			panic("LockOnlyIfExists only assigns the primary key when locking only one key")
		}
		txn.unsetPrimaryKeyIfNeeded(lockCtx)
	}
	skipedLockKeys := 0
	for _, key := range keys {
		valExists := true
		// PointGet and BatchPointGet will return value in pessimistic lock response, the value may not exist.
		// For other lock modes, the locked key values always exist.
		keyStr := string(key)
		var val tikv.ReturnedValue
		var ok bool
		if val, ok = lockCtx.Values[keyStr]; ok {
			if lockCtx.ReturnValues || checkedExistence || val.LockedWithConflictTS != 0 {
				if !val.Exists {
					valExists = false
				}
			}
		}
		if txn.aggressiveLockingContext != nil {
			txn.aggressiveLockingContext.currentLockedKeys[keyStr] = tempLockBufferEntry{
				HasReturnValue:    lockCtx.ReturnValues,
				HasCheckExistence: lockCtx.CheckExistence,
				Value:             val,
			}
			txn.aggressiveLockingDirty = true
		} else {
			setValExists := tikv.SetKeyLockedValueExists
			if !valExists {
				setValExists = tikv.SetKeyLockedValueNotExists
			}
			// TODO: Fix the calculation when aggressive-locking is active
			if lockCtx.LockOnlyIfExists && !valExists {
				skipedLockKeys++
				continue
			}
			memBuf.UpdateFlags(key, tikv.SetKeyLocked, tikv.DelNeedCheckExists, setValExists)
		}
	}
	txn.lockedCnt += len(keys) - skipedLockKeys
	return nil
}

func (txn *KVTxn) resetPrimary() {
	txn.committer.primaryKey = nil
	txn.committer.ttlManager.reset()
}

func (txn *KVTxn) selectPrimaryForPessimisticLock(sortedKeys [][]byte) {
	if txn.aggressiveLockingContext != nil {
		lastPrimaryKey := txn.aggressiveLockingContext.lastPrimaryKey
		if lastPrimaryKey != nil {
			foundIdx := sort.Search(len(sortedKeys), func(i int) bool {
				return bytes.Compare(sortedKeys[i], lastPrimaryKey) >= 0
			})
			if foundIdx < len(sortedKeys) && bytes.Equal(sortedKeys[foundIdx], lastPrimaryKey) {
				// The last selected primary is included in the current list of keys we are going to lock. Select it
				// as the primary.
				txn.committer.primaryKey = sortedKeys[foundIdx]
				txn.aggressiveLockingContext.assignedPrimaryKey = true
				txn.aggressiveLockingContext.primaryKey = sortedKeys[foundIdx]
			} else {
				txn.committer.primaryKey = sortedKeys[0]
				txn.aggressiveLockingContext.assignedPrimaryKey = true
				txn.aggressiveLockingContext.primaryKey = sortedKeys[0]
			}
		} else {
			txn.committer.primaryKey = sortedKeys[0]
			txn.aggressiveLockingContext.assignedPrimaryKey = true
			txn.aggressiveLockingContext.primaryKey = sortedKeys[0]
		}
	} else {
		txn.committer.primaryKey = sortedKeys[0]
	}
}

type aggressiveLockingContext struct {
	lastRetryUnnecessaryLocks map[string]tempLockBufferEntry
	lastPrimaryKey            []byte
	lastAttemptStartTime      time.Time

	currentLockedKeys       map[string]tempLockBufferEntry
	maxLockedWithConflictTS uint64
	assignedPrimaryKey      bool
	primaryKey              []byte
	startTime               time.Time
}

// unsetPrimaryKeyIfNeed is used to unset primary key of the transaction after performing LockOnlyIfExists.
// When locking only one key with LockOnlyIfExists flag, the key will be selected as primary if
// it's the first lock of the transaction. If the key doesn't exist on TiKV, the key won't be
// locked, in which case we should unset the primary of the transaction.
// The caller must ensure the conditions below:
// (1) only one key to be locked (2) primary is not selected before (2) with LockOnlyIfExists
func (txn *KVTxn) unsetPrimaryKeyIfNeeded(lockCtx *tikv.LockCtx) {
	if val, ok := lockCtx.Values[string(txn.committer.primaryKey)]; ok {
		if !val.Exists {
			txn.committer.primaryKey = nil
			txn.committer.ttlManager.reset()
		}
	}
}

// deduplicateKeys deduplicate the keys, it use sort instead of map to avoid memory allocation.
func deduplicateKeys(keys [][]byte) [][]byte {
	if len(keys) == 1 {
		return keys
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	deduped := keys[:1]
	for i := 1; i < len(keys); i++ {
		if !bytes.Equal(deduped[len(deduped)-1], keys[i]) {
			deduped = append(deduped, keys[i])
		}
	}
	return deduped
}

const pessimisticRollbackMaxBackoff = 20000

// asyncPessimisticRollback rollbacks pessimistic locks of the current transaction on the specified keys asynchronously.
// Pessimistic locks on specified keys with its forUpdateTS <= specifiedForUpdateTS will be unlocked. If 0 is passed
// to specifiedForUpdateTS, the current forUpdateTS of the current transaction will be used.
func (txn *KVTxn) asyncPessimisticRollback(ctx context.Context, keys [][]byte, specifiedForUpdateTS uint64) *sync.WaitGroup {
	// Clone a new committer for execute in background.
	committer := &twoPhaseCommitter{
		store:       txn.committer.store,
		sessionID:   txn.committer.sessionID,
		startTS:     txn.committer.startTS,
		forUpdateTS: txn.committer.forUpdateTS,
		primaryKey:  txn.committer.primaryKey,
	}
	if specifiedForUpdateTS > committer.forUpdateTS {
		committer.forUpdateTS = specifiedForUpdateTS
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		if val, err := util.EvalFailpoint("beforeAsyncPessimisticRollback"); err == nil {
			if s, ok := val.(string); ok {
				if s == "skip" {
					logutil.Logger(ctx).Info("[failpoint] injected skip async pessimistic rollback",
						zap.Uint64("txnStartTS", txn.startTS))
					wg.Done()
					return
				} else if s == "delay" {
					duration := time.Duration(rand.Int63n(int64(time.Second) * 2))
					logutil.Logger(ctx).Info("[failpoint] injected delay before async pessimistic rollback",
						zap.Uint64("txnStartTS", txn.startTS), zap.Duration("duration", duration))
					time.Sleep(duration)
				}
			}
		}

		err := committer.pessimisticRollbackMutations(retry.NewBackofferWithVars(ctx, pessimisticRollbackMaxBackoff, txn.vars), &PlainMutations{keys: keys})
		if err != nil {
			logutil.Logger(ctx).Warn("[kv] pessimisticRollback failed.", zap.Error(err))
		}
		wg.Done()
	}()
	return wg
}

func hashInKeys(deadlockKeyHash uint64, keys [][]byte) bool {
	for _, key := range keys {
		if farm.Fingerprint64(key) == deadlockKeyHash {
			return true
		}
	}
	return false
}

// IsReadOnly checks if the transaction has only performed read operations.
func (txn *KVTxn) IsReadOnly() bool {
	return !(txn.us.GetMemBuffer().Dirty() || txn.aggressiveLockingDirty)
}

// StartTS returns the transaction start timestamp.
func (txn *KVTxn) StartTS() uint64 {
	return txn.startTS
}

// Valid returns if the transaction is valid.
// A transaction become invalid after commit or rollback.
func (txn *KVTxn) Valid() bool {
	return txn.valid
}

// Len returns the number of entries in the DB.
func (txn *KVTxn) Len() int {
	return txn.us.GetMemBuffer().Len()
}

// Size returns sum of keys and values length.
func (txn *KVTxn) Size() int {
	return txn.us.GetMemBuffer().Size()
}

// Reset reset the Transaction to initial states.
func (txn *KVTxn) Reset() {
	txn.us.GetMemBuffer().Reset()
}

// GetUnionStore returns the UnionStore binding to this transaction.
func (txn *KVTxn) GetUnionStore() *unionstore.KVUnionStore {
	return txn.us
}

// GetMemBuffer return the MemBuffer binding to this transaction.
func (txn *KVTxn) GetMemBuffer() *unionstore.MemDB {
	return txn.us.GetMemBuffer()
}

// GetSnapshot returns the Snapshot binding to this transaction.
func (txn *KVTxn) GetSnapshot() *txnsnapshot.KVSnapshot {
	return txn.snapshot
}

// SetBinlogExecutor sets the method to perform binlong synchronization.
func (txn *KVTxn) SetBinlogExecutor(binlog BinlogExecutor) {
	txn.binlog = binlog
	if txn.committer != nil {
		txn.committer.binlog = binlog
	}
}

// GetClusterID returns store's cluster id.
func (txn *KVTxn) GetClusterID() uint64 {
	return txn.store.GetClusterID()
}

// SetMemoryFootprintChangeHook sets the hook function that is triggered when memdb grows
func (txn *KVTxn) SetMemoryFootprintChangeHook(hook func(uint64)) {
	txn.us.GetMemBuffer().SetMemoryFootprintChangeHook(hook)
}

// Mem returns the current memory footprint
func (txn *KVTxn) Mem() uint64 {
	return txn.us.GetMemBuffer().Mem()
}

// SetRequestSourceInternal sets the scope of the request source.
func (txn *KVTxn) SetRequestSourceInternal(internal bool) {
	txn.RequestSource.SetRequestSourceInternal(internal)
}

// SetRequestSourceType sets the type of the request source.
func (txn *KVTxn) SetRequestSourceType(tp string) {
	txn.RequestSource.SetRequestSourceType(tp)
}
