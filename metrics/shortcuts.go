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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/metrics/shortcuts.go
//

// Copyright 2021 PingCAP, Inc.
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

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Shortcuts for performance improvement.
var (
	TxnCmdHistogramWithCommitInternal   prometheus.Observer
	TxnCmdHistogramWithCommitGeneral    prometheus.Observer
	TxnCmdHistogramWithRollbackInternal prometheus.Observer
	TxnCmdHistogramWithRollbackGeneral  prometheus.Observer
	TxnCmdHistogramWithBatchGetInternal prometheus.Observer
	TxnCmdHistogramWithBatchGetGeneral  prometheus.Observer
	TxnCmdHistogramWithGetInternal      prometheus.Observer
	TxnCmdHistogramWithGetGeneral       prometheus.Observer
	TxnCmdHistogramWithLockKeysInternal prometheus.Observer
	TxnCmdHistogramWithLockKeysGeneral  prometheus.Observer

	RawkvCmdHistogramWithGet           prometheus.Observer
	RawkvCmdHistogramWithBatchGet      prometheus.Observer
	RawkvCmdHistogramWithBatchPut      prometheus.Observer
	RawkvCmdHistogramWithDelete        prometheus.Observer
	RawkvCmdHistogramWithBatchDelete   prometheus.Observer
	RawkvCmdHistogramWithRawScan       prometheus.Observer
	RawkvCmdHistogramWithRawReversScan prometheus.Observer
	RawkvSizeHistogramWithKey          prometheus.Observer
	RawkvSizeHistogramWithValue        prometheus.Observer
	RawkvCmdHistogramWithRawChecksum   prometheus.Observer

	BackoffHistogramRPC                      prometheus.Observer
	BackoffHistogramLock                     prometheus.Observer
	BackoffHistogramLockFast                 prometheus.Observer
	BackoffHistogramPD                       prometheus.Observer
	BackoffHistogramRegionMiss               prometheus.Observer
	BackoffHistogramRegionScheduling         prometheus.Observer
	BackoffHistogramServerBusy               prometheus.Observer
	BackoffHistogramTiKVDiskFull             prometheus.Observer
	BackoffHistogramRegionRecoveryInProgress prometheus.Observer
	BackoffHistogramStaleCmd                 prometheus.Observer
	BackoffHistogramDataNotReady             prometheus.Observer
	BackoffHistogramIsWitness                prometheus.Observer
	BackoffHistogramEmpty                    prometheus.Observer

	TxnRegionsNumHistogramWithSnapshotInternal         prometheus.Observer
	TxnRegionsNumHistogramWithSnapshot                 prometheus.Observer
	TxnRegionsNumHistogramPrewriteInternal             prometheus.Observer
	TxnRegionsNumHistogramPrewrite                     prometheus.Observer
	TxnRegionsNumHistogramCommitInternal               prometheus.Observer
	TxnRegionsNumHistogramCommit                       prometheus.Observer
	TxnRegionsNumHistogramCleanupInternal              prometheus.Observer
	TxnRegionsNumHistogramCleanup                      prometheus.Observer
	TxnRegionsNumHistogramPessimisticLockInternal      prometheus.Observer
	TxnRegionsNumHistogramPessimisticLock              prometheus.Observer
	TxnRegionsNumHistogramPessimisticRollbackInternal  prometheus.Observer
	TxnRegionsNumHistogramPessimisticRollback          prometheus.Observer
	TxnRegionsNumHistogramWithCoprocessorInternal      prometheus.Observer
	TxnRegionsNumHistogramWithCoprocessor              prometheus.Observer
	TxnRegionsNumHistogramWithBatchCoprocessorInternal prometheus.Observer
	TxnRegionsNumHistogramWithBatchCoprocessor         prometheus.Observer

	TxnWriteKVCountHistogramInternal prometheus.Observer
	TxnWriteKVCountHistogramGeneral  prometheus.Observer
	TxnWriteSizeHistogramInternal    prometheus.Observer
	TxnWriteSizeHistogramGeneral     prometheus.Observer

	LockResolverCountWithBatchResolve             prometheus.Counter
	LockResolverCountWithExpired                  prometheus.Counter
	LockResolverCountWithNotExpired               prometheus.Counter
	LockResolverCountWithWaitExpired              prometheus.Counter
	LockResolverCountWithResolve                  prometheus.Counter
	LockResolverCountWithResolveForWrite          prometheus.Counter
	LockResolverCountWithResolveAsync             prometheus.Counter
	LockResolverCountWithWriteConflict            prometheus.Counter
	LockResolverCountWithQueryTxnStatus           prometheus.Counter
	LockResolverCountWithQueryTxnStatusCommitted  prometheus.Counter
	LockResolverCountWithQueryTxnStatusRolledBack prometheus.Counter
	LockResolverCountWithQueryCheckSecondaryLocks prometheus.Counter
	LockResolverCountWithResolveLocks             prometheus.Counter
	LockResolverCountWithResolveLockLite          prometheus.Counter

	RegionCacheCounterWithInvalidateRegionFromCacheOK prometheus.Counter
	RegionCacheCounterWithSendFail                    prometheus.Counter
	RegionCacheCounterWithGetRegionByIDOK             prometheus.Counter
	RegionCacheCounterWithGetRegionByIDError          prometheus.Counter
	RegionCacheCounterWithGetCacheMissOK              prometheus.Counter
	RegionCacheCounterWithGetCacheMissError           prometheus.Counter
	RegionCacheCounterWithScanRegionsOK               prometheus.Counter
	RegionCacheCounterWithScanRegionsError            prometheus.Counter
	RegionCacheCounterWithGetStoreOK                  prometheus.Counter
	RegionCacheCounterWithGetStoreError               prometheus.Counter
	RegionCacheCounterWithInvalidateStoreRegionsOK    prometheus.Counter

	LoadRegionCacheHistogramWhenCacheMiss  prometheus.Observer
	LoadRegionCacheHistogramWithRegions    prometheus.Observer
	LoadRegionCacheHistogramWithRegionByID prometheus.Observer
	LoadRegionCacheHistogramWithGetStore   prometheus.Observer

	TxnHeartBeatHistogramOK    prometheus.Observer
	TxnHeartBeatHistogramError prometheus.Observer

	StatusCountWithOK    prometheus.Counter
	StatusCountWithError prometheus.Counter

	SecondaryLockCleanupFailureCounterCommit   prometheus.Counter
	SecondaryLockCleanupFailureCounterRollback prometheus.Counter

	TwoPCTxnCounterOk    prometheus.Counter
	TwoPCTxnCounterError prometheus.Counter

	AsyncCommitTxnCounterOk    prometheus.Counter
	AsyncCommitTxnCounterError prometheus.Counter

	OnePCTxnCounterOk       prometheus.Counter
	OnePCTxnCounterError    prometheus.Counter
	OnePCTxnCounterFallback prometheus.Counter

	BatchRecvHistogramOK    prometheus.Observer
	BatchRecvHistogramError prometheus.Observer

	PrewriteAssertionUsageCounterNone     prometheus.Counter
	PrewriteAssertionUsageCounterExist    prometheus.Counter
	PrewriteAssertionUsageCounterNotExist prometheus.Counter
	PrewriteAssertionUsageCounterUnknown  prometheus.Counter

	AggressiveLockedKeysNew                prometheus.Counter
	AggressiveLockedKeysDerived            prometheus.Counter
	AggressiveLockedKeysLockedWithConflict prometheus.Counter
	AggressiveLockedKeysNonForceLock       prometheus.Counter

	StaleReadHitCounter  prometheus.Counter
	StaleReadMissCounter prometheus.Counter

	StaleReadReqLocalCounter     prometheus.Counter
	StaleReadReqCrossZoneCounter prometheus.Counter

	StaleReadLocalInBytes   prometheus.Counter
	StaleReadLocalOutBytes  prometheus.Counter
	StaleReadRemoteInBytes  prometheus.Counter
	StaleReadRemoteOutBytes prometheus.Counter
)

func initShortcuts() {
	TxnCmdHistogramWithCommitInternal = TiKVTxnCmdHistogram.WithLabelValues(LblCommit, LblInternal)
	TxnCmdHistogramWithCommitGeneral = TiKVTxnCmdHistogram.WithLabelValues(LblCommit, LblGeneral)
	TxnCmdHistogramWithRollbackInternal = TiKVTxnCmdHistogram.WithLabelValues(LblRollback, LblInternal)
	TxnCmdHistogramWithRollbackGeneral = TiKVTxnCmdHistogram.WithLabelValues(LblRollback, LblGeneral)
	TxnCmdHistogramWithBatchGetInternal = TiKVTxnCmdHistogram.WithLabelValues(LblBatchGet, LblInternal)
	TxnCmdHistogramWithBatchGetGeneral = TiKVTxnCmdHistogram.WithLabelValues(LblBatchGet, LblGeneral)
	TxnCmdHistogramWithGetInternal = TiKVTxnCmdHistogram.WithLabelValues(LblGet, LblInternal)
	TxnCmdHistogramWithGetGeneral = TiKVTxnCmdHistogram.WithLabelValues(LblGet, LblGeneral)
	TxnCmdHistogramWithLockKeysInternal = TiKVTxnCmdHistogram.WithLabelValues(LblLockKeys, LblInternal)
	TxnCmdHistogramWithLockKeysGeneral = TiKVTxnCmdHistogram.WithLabelValues(LblLockKeys, LblGeneral)

	RawkvCmdHistogramWithGet = TiKVRawkvCmdHistogram.WithLabelValues("get")
	RawkvCmdHistogramWithBatchGet = TiKVRawkvCmdHistogram.WithLabelValues("batch_get")
	RawkvCmdHistogramWithBatchPut = TiKVRawkvCmdHistogram.WithLabelValues("batch_put")
	RawkvCmdHistogramWithDelete = TiKVRawkvCmdHistogram.WithLabelValues("delete")
	RawkvCmdHistogramWithBatchDelete = TiKVRawkvCmdHistogram.WithLabelValues("batch_delete")
	RawkvCmdHistogramWithRawScan = TiKVRawkvCmdHistogram.WithLabelValues("raw_scan")
	RawkvCmdHistogramWithRawReversScan = TiKVRawkvCmdHistogram.WithLabelValues("raw_reverse_scan")
	RawkvSizeHistogramWithKey = TiKVRawkvSizeHistogram.WithLabelValues("key")
	RawkvSizeHistogramWithValue = TiKVRawkvSizeHistogram.WithLabelValues("value")
	RawkvCmdHistogramWithRawChecksum = TiKVRawkvSizeHistogram.WithLabelValues("raw_checksum")

	BackoffHistogramRPC = TiKVBackoffHistogram.WithLabelValues("tikvRPC")
	BackoffHistogramLock = TiKVBackoffHistogram.WithLabelValues("txnLock")
	BackoffHistogramLockFast = TiKVBackoffHistogram.WithLabelValues("tikvLockFast")
	BackoffHistogramPD = TiKVBackoffHistogram.WithLabelValues("pdRPC")
	BackoffHistogramRegionMiss = TiKVBackoffHistogram.WithLabelValues("regionMiss")
	BackoffHistogramRegionScheduling = TiKVBackoffHistogram.WithLabelValues("regionScheduling")
	BackoffHistogramServerBusy = TiKVBackoffHistogram.WithLabelValues("serverBusy")
	BackoffHistogramTiKVDiskFull = TiKVBackoffHistogram.WithLabelValues("tikvDiskFull")
	BackoffHistogramRegionRecoveryInProgress = TiKVBackoffHistogram.WithLabelValues("regionRecoveryInProgress")
	BackoffHistogramStaleCmd = TiKVBackoffHistogram.WithLabelValues("staleCommand")
	BackoffHistogramDataNotReady = TiKVBackoffHistogram.WithLabelValues("dataNotReady")
	BackoffHistogramIsWitness = TiKVBackoffHistogram.WithLabelValues("isWitness")
	BackoffHistogramEmpty = TiKVBackoffHistogram.WithLabelValues("")

	TxnRegionsNumHistogramWithSnapshotInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("snapshot", LblInternal)
	TxnRegionsNumHistogramWithSnapshot = TiKVTxnRegionsNumHistogram.WithLabelValues("snapshot", LblGeneral)
	TxnRegionsNumHistogramPrewriteInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_prewrite", LblInternal)
	TxnRegionsNumHistogramPrewrite = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_prewrite", LblGeneral)
	TxnRegionsNumHistogramCommitInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_commit", LblInternal)
	TxnRegionsNumHistogramCommit = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_commit", LblGeneral)
	TxnRegionsNumHistogramCleanupInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_cleanup", LblInternal)
	TxnRegionsNumHistogramCleanup = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_cleanup", LblGeneral)
	TxnRegionsNumHistogramPessimisticLockInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_pessimistic_lock", LblInternal)
	TxnRegionsNumHistogramPessimisticLock = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_pessimistic_lock", LblGeneral)
	TxnRegionsNumHistogramPessimisticRollbackInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_pessimistic_rollback", LblInternal)
	TxnRegionsNumHistogramPessimisticRollback = TiKVTxnRegionsNumHistogram.WithLabelValues("2pc_pessimistic_rollback", LblGeneral)
	TxnRegionsNumHistogramWithCoprocessorInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("coprocessor", LblInternal)
	TxnRegionsNumHistogramWithCoprocessor = TiKVTxnRegionsNumHistogram.WithLabelValues("batch_coprocessor", LblGeneral)
	TxnRegionsNumHistogramWithBatchCoprocessorInternal = TiKVTxnRegionsNumHistogram.WithLabelValues("coprocessor", LblInternal)
	TxnRegionsNumHistogramWithBatchCoprocessor = TiKVTxnRegionsNumHistogram.WithLabelValues("batch_coprocessor", LblGeneral)
	TxnWriteKVCountHistogramInternal = TiKVTxnWriteKVCountHistogram.WithLabelValues(LblInternal)
	TxnWriteKVCountHistogramGeneral = TiKVTxnWriteKVCountHistogram.WithLabelValues(LblGeneral)
	TxnWriteSizeHistogramInternal = TiKVTxnWriteSizeHistogram.WithLabelValues(LblInternal)
	TxnWriteSizeHistogramGeneral = TiKVTxnWriteSizeHistogram.WithLabelValues(LblGeneral)

	LockResolverCountWithBatchResolve = TiKVLockResolverCounter.WithLabelValues("batch_resolve")
	LockResolverCountWithExpired = TiKVLockResolverCounter.WithLabelValues("expired")
	LockResolverCountWithNotExpired = TiKVLockResolverCounter.WithLabelValues("not_expired")
	LockResolverCountWithWaitExpired = TiKVLockResolverCounter.WithLabelValues("wait_expired")
	LockResolverCountWithResolve = TiKVLockResolverCounter.WithLabelValues("resolve")
	LockResolverCountWithResolveForWrite = TiKVLockResolverCounter.WithLabelValues("resolve_for_write")
	LockResolverCountWithResolveAsync = TiKVLockResolverCounter.WithLabelValues("resolve_async_commit")
	LockResolverCountWithWriteConflict = TiKVLockResolverCounter.WithLabelValues("write_conflict")
	LockResolverCountWithQueryTxnStatus = TiKVLockResolverCounter.WithLabelValues("query_txn_status")
	LockResolverCountWithQueryTxnStatusCommitted = TiKVLockResolverCounter.WithLabelValues("query_txn_status_committed")
	LockResolverCountWithQueryTxnStatusRolledBack = TiKVLockResolverCounter.WithLabelValues("query_txn_status_rolled_back")
	LockResolverCountWithQueryCheckSecondaryLocks = TiKVLockResolverCounter.WithLabelValues("query_check_secondary_locks")
	LockResolverCountWithResolveLocks = TiKVLockResolverCounter.WithLabelValues("query_resolve_locks")
	LockResolverCountWithResolveLockLite = TiKVLockResolverCounter.WithLabelValues("query_resolve_lock_lite")

	RegionCacheCounterWithInvalidateRegionFromCacheOK = TiKVRegionCacheCounter.WithLabelValues("invalidate_region_from_cache", "ok")
	RegionCacheCounterWithSendFail = TiKVRegionCacheCounter.WithLabelValues("send_fail", "ok")
	RegionCacheCounterWithGetRegionByIDOK = TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "ok")
	RegionCacheCounterWithGetRegionByIDError = TiKVRegionCacheCounter.WithLabelValues("get_region_by_id", "err")
	RegionCacheCounterWithGetCacheMissOK = TiKVRegionCacheCounter.WithLabelValues("get_region_when_miss", "ok")
	RegionCacheCounterWithGetCacheMissError = TiKVRegionCacheCounter.WithLabelValues("get_region_when_miss", "err")
	RegionCacheCounterWithScanRegionsOK = TiKVRegionCacheCounter.WithLabelValues("scan_regions", "ok")
	RegionCacheCounterWithScanRegionsError = TiKVRegionCacheCounter.WithLabelValues("scan_regions", "err")
	RegionCacheCounterWithGetStoreOK = TiKVRegionCacheCounter.WithLabelValues("get_store", "ok")
	RegionCacheCounterWithGetStoreError = TiKVRegionCacheCounter.WithLabelValues("get_store", "err")
	RegionCacheCounterWithInvalidateStoreRegionsOK = TiKVRegionCacheCounter.WithLabelValues("invalidate_store_regions", "ok")

	LoadRegionCacheHistogramWhenCacheMiss = TiKVLoadRegionCacheHistogram.WithLabelValues("get_region_when_miss")
	LoadRegionCacheHistogramWithRegionByID = TiKVLoadRegionCacheHistogram.WithLabelValues("get_region_by_id")
	LoadRegionCacheHistogramWithRegions = TiKVLoadRegionCacheHistogram.WithLabelValues("scan_regions")
	LoadRegionCacheHistogramWithGetStore = TiKVLoadRegionCacheHistogram.WithLabelValues("get_store")

	TxnHeartBeatHistogramOK = TiKVTxnHeartBeatHistogram.WithLabelValues("ok")
	TxnHeartBeatHistogramError = TiKVTxnHeartBeatHistogram.WithLabelValues("err")

	StatusCountWithOK = TiKVStatusCounter.WithLabelValues("ok")
	StatusCountWithError = TiKVStatusCounter.WithLabelValues("err")

	SecondaryLockCleanupFailureCounterCommit = TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("commit")
	SecondaryLockCleanupFailureCounterRollback = TiKVSecondaryLockCleanupFailureCounter.WithLabelValues("rollback")

	TwoPCTxnCounterOk = TiKVTwoPCTxnCounter.WithLabelValues("ok")
	TwoPCTxnCounterError = TiKVTwoPCTxnCounter.WithLabelValues("err")

	AsyncCommitTxnCounterOk = TiKVAsyncCommitTxnCounter.WithLabelValues("ok")
	AsyncCommitTxnCounterError = TiKVAsyncCommitTxnCounter.WithLabelValues("err")

	OnePCTxnCounterOk = TiKVOnePCTxnCounter.WithLabelValues("ok")
	OnePCTxnCounterError = TiKVOnePCTxnCounter.WithLabelValues("err")
	OnePCTxnCounterFallback = TiKVOnePCTxnCounter.WithLabelValues("fallback")

	BatchRecvHistogramOK = TiKVBatchRecvLatency.WithLabelValues("ok")
	BatchRecvHistogramError = TiKVBatchRecvLatency.WithLabelValues("err")

	PrewriteAssertionUsageCounterNone = TiKVPrewriteAssertionUsageCounter.WithLabelValues("none")
	PrewriteAssertionUsageCounterExist = TiKVPrewriteAssertionUsageCounter.WithLabelValues("exist")
	PrewriteAssertionUsageCounterNotExist = TiKVPrewriteAssertionUsageCounter.WithLabelValues("not-exist")
	PrewriteAssertionUsageCounterUnknown = TiKVPrewriteAssertionUsageCounter.WithLabelValues("unknown")

	// Counts new locks trying to acquire inside an aggressive locking stage.
	AggressiveLockedKeysNew = TiKVAggressiveLockedKeysCounter.WithLabelValues("new")
	// Counts locks trying to acquire inside an aggressive locking stage, but it's already locked in the previous
	// aggressive locking stage (before the latest invocation to `RetryAggressiveLocking`), in which case the lock
	// can be *derived* from the previous stage and no RPC is needed for the key.
	AggressiveLockedKeysDerived = TiKVAggressiveLockedKeysCounter.WithLabelValues("derived")
	// Counts locks that's forced acquired ignoring the WriteConflict.
	AggressiveLockedKeysLockedWithConflict = TiKVAggressiveLockedKeysCounter.WithLabelValues("locked_with_conflict")
	// Counts locks that's acquired within an aggressive locking stage, but with force-lock disabled (by passing
	// `WakeUpMode = PessimisticLockWakeUpMode_WakeUpModeNormal`, which will disable `allow_lock_with_conflict` in
	// TiKV).
	AggressiveLockedKeysNonForceLock = TiKVAggressiveLockedKeysCounter.WithLabelValues("non_force_lock")

	StaleReadHitCounter = TiKVStaleReadCounter.WithLabelValues("hit")
	StaleReadMissCounter = TiKVStaleReadCounter.WithLabelValues("miss")

	StaleReadReqLocalCounter = TiKVStaleReadReqCounter.WithLabelValues("local")
	StaleReadReqCrossZoneCounter = TiKVStaleReadReqCounter.WithLabelValues("cross-zone")

	StaleReadLocalInBytes = TiKVStaleReadBytes.WithLabelValues("local", "in")
	StaleReadLocalOutBytes = TiKVStaleReadBytes.WithLabelValues("local", "out")
	StaleReadRemoteInBytes = TiKVStaleReadBytes.WithLabelValues("cross-zone", "in")
	StaleReadRemoteOutBytes = TiKVStaleReadBytes.WithLabelValues("cross-zone", "out")
}
