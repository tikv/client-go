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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/util/execdetails.go
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

package util

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
	uatomic "go.uber.org/atomic"
)

type commitDetailCtxKeyType struct{}
type lockKeysDetailCtxKeyType struct{}
type execDetailsCtxKeyType struct{}
type ruDetailsCtxKeyType struct{}
type traceExecDetailsCtxKeyType struct{}

var (
	// CommitDetailCtxKey presents CommitDetail info key in context.
	CommitDetailCtxKey = commitDetailCtxKeyType{}

	// LockKeysDetailCtxKey presents LockKeysDetail info key in context.
	LockKeysDetailCtxKey = lockKeysDetailCtxKeyType{}

	// ExecDetailsKey presents ExecDetail info key in context.
	ExecDetailsKey = execDetailsCtxKeyType{}

	// ruDetailsCtxKey presents RUDetals info key in context.
	RUDetailsCtxKey = ruDetailsCtxKeyType{}

	// traceExecDetailsKey is a context key whose value indicates whether to add ExecDetails to trace.
	traceExecDetailsKey = traceExecDetailsCtxKeyType{}
)

// ContextWithTraceExecDetails returns a context with trace-exec-details enabled
func ContextWithTraceExecDetails(ctx context.Context) context.Context {
	return context.WithValue(ctx, traceExecDetailsKey, struct{}{})
}

// TraceExecDetailsEnabled checks whether trace-exec-details enabled
func TraceExecDetailsEnabled(ctx context.Context) bool {
	return ctx.Value(traceExecDetailsKey) != nil
}

// TiKVExecDetails is the detail execution information at TiKV side.
type TiKVExecDetails struct {
	TimeDetail  *TimeDetail
	ScanDetail  *ScanDetail
	WriteDetail *WriteDetail
}

// NewTiKVExecDetails creates a TiKVExecDetails from a kvproto ExecDetailsV2.
func NewTiKVExecDetails(pb *kvrpcpb.ExecDetailsV2) TiKVExecDetails {
	if pb == nil {
		return TiKVExecDetails{}
	}
	td := &TimeDetail{}
	td.MergeFromTimeDetail(pb.TimeDetailV2, pb.TimeDetail)
	sd := &ScanDetail{}
	sd.MergeFromScanDetailV2(pb.ScanDetailV2)
	wd := &WriteDetail{}
	wd.MergeFromWriteDetailPb(pb.WriteDetail)
	return TiKVExecDetails{
		TimeDetail:  td,
		ScanDetail:  sd,
		WriteDetail: wd,
	}
}

func (ed *TiKVExecDetails) String() string {
	if ed == nil {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if ed.TimeDetail != nil {
		buf.WriteString(ed.TimeDetail.String())
	}
	if ed.ScanDetail != nil {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(ed.ScanDetail.String())
	}
	if ed.WriteDetail != nil {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(ed.WriteDetail.String())
	}
	return buf.String()
}

func cloneRUV2(ru *kvrpcpb.RUV2) *kvrpcpb.RUV2 {
	if ru == nil {
		return nil
	}
	cloned := *ru
	if ru.ExecutorInputs != nil {
		execInputs := *ru.ExecutorInputs
		cloned.ExecutorInputs = &execInputs
	}
	return &cloned
}

func mergeRUV2(dst, src *kvrpcpb.RUV2) {
	if dst == nil || src == nil {
		return
	}
	dst.KvEngineCacheMiss += src.KvEngineCacheMiss
	dst.CoprocessorExecutorIterations += src.CoprocessorExecutorIterations
	dst.CoprocessorResponseBytes += src.CoprocessorResponseBytes
	dst.RaftstoreStoreWriteTriggerWbBytes += src.RaftstoreStoreWriteTriggerWbBytes
	dst.StorageProcessedKeysBatchGet += src.StorageProcessedKeysBatchGet
	dst.StorageProcessedKeysGet += src.StorageProcessedKeysGet
	dst.ReadRpcCount += src.ReadRpcCount
	dst.WriteRpcCount += src.WriteRpcCount
	if src.ExecutorInputs != nil {
		if dst.ExecutorInputs == nil {
			dst.ExecutorInputs = &kvrpcpb.ExecutorInputs{}
		}
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchTableScan += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchTableScan
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSelection += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSelection
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchTopN += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchTopN
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchLimit += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchLimit
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr
		dst.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr += src.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr
	}
}

// ReqDetailInfo contains diagnose information about `TiKVExecDetails`, region, store and backoff.
type ReqDetailInfo struct {
	ReqTotalTime time.Duration
	Region       uint64
	StoreAddr    string
	ExecDetails  TiKVExecDetails
}

// CommitTSLagDetails contain the detail when the commit timestamp
// from PD lags the expected ts set by `SetCommitWaitUntilTSO`.
type CommitTSLagDetails struct {
	// WaitTime indicates the total wait time for the lagged PD TSO exceeds `WaitUntilTS`.
	WaitTime time.Duration
	// BackoffCnt indicates the backoff count to wait the lagged PD TSO exceeds `WaitUntilTS`.
	BackoffCnt int
	// FirstLagTS indicates the first fetched TSO that lags behind `WaitUntilTS`.
	FirstLagTS uint64
	// WaitUntilTS indicates the min timestamp of the commit ts, the txn should wait PD TSO to exceeds this value.
	WaitUntilTS uint64
}

// Merge merges CommitTSLagDetails with another one
func (d *CommitTSLagDetails) Merge(other *CommitTSLagDetails) {
	if other == nil || other.FirstLagTS <= 0 {
		// other.FirstLagTS <= 0 indicates no lag happen, do not need to merge the new details.
		return
	}
	d.WaitTime += other.WaitTime
	d.BackoffCnt += other.BackoffCnt
	// For sample, we use the last lag timestamps after merge
	d.FirstLagTS = other.FirstLagTS
	d.WaitUntilTS = other.WaitUntilTS
}

// CommitDetails contains commit detail information.
type CommitDetails struct {
	GetCommitTsTime        time.Duration
	GetLatestTsTime        time.Duration
	LagDetails             CommitTSLagDetails
	PrewriteTime           time.Duration
	WaitPrewriteBinlogTime time.Duration
	CommitTime             time.Duration
	LocalLatchTime         time.Duration
	Mu                     struct {
		sync.Mutex
		// The total backoff time used in both the prewrite and commit phases.
		CommitBackoffTime    int64
		PrewriteBackoffTypes []string
		CommitBackoffTypes   []string
		// The prewrite requests are executed concurrently so the slowest request information would be recorded.
		SlowestPrewrite ReqDetailInfo
		// It's recorded only when the commit mode is 2pc.
		CommitPrimary ReqDetailInfo
	}
	WriteKeys         int
	WriteSize         int
	PrewriteRegionNum int32
	TxnRetry          int
	ResolveLock       ResolveLockDetail
	PrewriteReqNum    int
}

// Merge merges commit details into itself.
func (cd *CommitDetails) Merge(other *CommitDetails) {
	cd.GetCommitTsTime += other.GetCommitTsTime
	cd.GetLatestTsTime += other.GetLatestTsTime
	cd.PrewriteTime += other.PrewriteTime
	cd.LagDetails.Merge(&other.LagDetails)
	cd.WaitPrewriteBinlogTime += other.WaitPrewriteBinlogTime
	cd.CommitTime += other.CommitTime
	cd.LocalLatchTime += other.LocalLatchTime
	cd.ResolveLock.ResolveLockTime += other.ResolveLock.ResolveLockTime
	cd.WriteKeys += other.WriteKeys
	cd.WriteSize += other.WriteSize
	cd.PrewriteRegionNum += other.PrewriteRegionNum
	cd.TxnRetry += other.TxnRetry
	cd.Mu.CommitBackoffTime += other.Mu.CommitBackoffTime

	cd.Mu.PrewriteBackoffTypes = append(cd.Mu.PrewriteBackoffTypes, other.Mu.PrewriteBackoffTypes...)
	if cd.Mu.SlowestPrewrite.ReqTotalTime < other.Mu.SlowestPrewrite.ReqTotalTime {
		cd.Mu.SlowestPrewrite = other.Mu.SlowestPrewrite
	}

	cd.Mu.CommitBackoffTypes = append(cd.Mu.CommitBackoffTypes, other.Mu.CommitBackoffTypes...)
	if cd.Mu.CommitPrimary.ReqTotalTime < other.Mu.CommitPrimary.ReqTotalTime {
		cd.Mu.CommitPrimary = other.Mu.CommitPrimary
	}
}

// MergePrewriteReqDetails merges prewrite related ExecDetailsV2 into the current CommitDetails.
func (cd *CommitDetails) MergePrewriteReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2) {
	if cd == nil {
		return
	}
	cd.Mu.Lock()
	defer cd.Mu.Unlock()
	if reqDuration > cd.Mu.SlowestPrewrite.ReqTotalTime {
		cd.Mu.SlowestPrewrite.ReqTotalTime = reqDuration
		cd.Mu.SlowestPrewrite.Region = regionID
		cd.Mu.SlowestPrewrite.StoreAddr = addr
		cd.Mu.SlowestPrewrite.ExecDetails = NewTiKVExecDetails(execDetails)
	}
}

// MergeCommitReqDetails merges commit related ExecDetailsV2 into the current CommitDetails.
func (cd *CommitDetails) MergeCommitReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2) {
	if cd == nil {
		return
	}
	cd.Mu.Lock()
	defer cd.Mu.Unlock()
	if reqDuration > cd.Mu.CommitPrimary.ReqTotalTime {
		cd.Mu.CommitPrimary.ReqTotalTime = reqDuration
		cd.Mu.CommitPrimary.Region = regionID
		cd.Mu.CommitPrimary.StoreAddr = addr
		cd.Mu.CommitPrimary.ExecDetails = NewTiKVExecDetails(execDetails)
	}
}

func (cd *CommitDetails) MergeFlushReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2) {
	// leave it empty for now
}

// Clone returns a deep copy of itself.
func (cd *CommitDetails) Clone() *CommitDetails {
	commit := &CommitDetails{
		GetCommitTsTime:        cd.GetCommitTsTime,
		GetLatestTsTime:        cd.GetLatestTsTime,
		LagDetails:             cd.LagDetails,
		PrewriteTime:           cd.PrewriteTime,
		WaitPrewriteBinlogTime: cd.WaitPrewriteBinlogTime,
		CommitTime:             cd.CommitTime,
		LocalLatchTime:         cd.LocalLatchTime,
		WriteKeys:              cd.WriteKeys,
		WriteSize:              cd.WriteSize,
		PrewriteRegionNum:      cd.PrewriteRegionNum,
		TxnRetry:               cd.TxnRetry,
		ResolveLock:            cd.ResolveLock,
	}
	commit.Mu.CommitBackoffTime = cd.Mu.CommitBackoffTime
	commit.Mu.PrewriteBackoffTypes = append([]string{}, cd.Mu.PrewriteBackoffTypes...)
	commit.Mu.CommitBackoffTypes = append([]string{}, cd.Mu.CommitBackoffTypes...)
	commit.Mu.SlowestPrewrite = cd.Mu.SlowestPrewrite
	commit.Mu.CommitPrimary = cd.Mu.CommitPrimary
	return commit
}

// LockKeysDetails contains pessimistic lock keys detail information.
type LockKeysDetails struct {
	TotalTime                  time.Duration
	RegionNum                  int32
	LockKeys                   int32
	AggressiveLockNewCount     int
	AggressiveLockDerivedCount int
	LockedWithConflictCount    int
	ResolveLock                ResolveLockDetail
	BackoffTime                int64
	Mu                         struct {
		sync.Mutex
		BackoffTypes        []string
		SlowestReqTotalTime time.Duration
		SlowestRegion       uint64
		SlowestStoreAddr    string
		SlowestExecDetails  TiKVExecDetails
	}
	LockRPCTime  int64
	LockRPCCount int64
	RetryCount   int
}

// Merge merges lock keys execution details into self.
func (ld *LockKeysDetails) Merge(lockKey *LockKeysDetails) {
	ld.TotalTime += lockKey.TotalTime
	ld.RegionNum += lockKey.RegionNum
	ld.LockKeys += lockKey.LockKeys
	ld.AggressiveLockNewCount += lockKey.AggressiveLockNewCount
	ld.AggressiveLockDerivedCount += lockKey.AggressiveLockDerivedCount
	ld.LockedWithConflictCount += lockKey.LockedWithConflictCount
	ld.ResolveLock.ResolveLockTime += lockKey.ResolveLock.ResolveLockTime
	ld.BackoffTime += lockKey.BackoffTime
	ld.LockRPCTime += lockKey.LockRPCTime
	ld.LockRPCCount += lockKey.LockRPCCount
	ld.Mu.BackoffTypes = append(ld.Mu.BackoffTypes, lockKey.Mu.BackoffTypes...)
	ld.RetryCount++
	if ld.Mu.SlowestReqTotalTime < lockKey.Mu.SlowestReqTotalTime {
		ld.Mu.SlowestReqTotalTime = lockKey.Mu.SlowestReqTotalTime
		ld.Mu.SlowestRegion = lockKey.Mu.SlowestRegion
		ld.Mu.SlowestStoreAddr = lockKey.Mu.SlowestStoreAddr
		ld.Mu.SlowestExecDetails = lockKey.Mu.SlowestExecDetails
	}
}

// MergeReqDetails merges ExecDetailsV2 into the current LockKeysDetails.
func (ld *LockKeysDetails) MergeReqDetails(reqDuration time.Duration, regionID uint64, addr string, execDetails *kvrpcpb.ExecDetailsV2) {
	if ld == nil {
		return
	}
	ld.Mu.Lock()
	defer ld.Mu.Unlock()
	if reqDuration > ld.Mu.SlowestReqTotalTime {
		ld.Mu.SlowestReqTotalTime = reqDuration
		ld.Mu.SlowestRegion = regionID
		ld.Mu.SlowestStoreAddr = addr
		ld.Mu.SlowestExecDetails = NewTiKVExecDetails(execDetails)
	}
}

// Clone returns a deep copy of itself.
func (ld *LockKeysDetails) Clone() *LockKeysDetails {
	lock := &LockKeysDetails{
		TotalTime:                  ld.TotalTime,
		RegionNum:                  ld.RegionNum,
		LockKeys:                   ld.LockKeys,
		AggressiveLockNewCount:     ld.AggressiveLockNewCount,
		AggressiveLockDerivedCount: ld.AggressiveLockDerivedCount,
		LockedWithConflictCount:    ld.LockedWithConflictCount,
		BackoffTime:                ld.BackoffTime,
		LockRPCTime:                ld.LockRPCTime,
		LockRPCCount:               ld.LockRPCCount,
		RetryCount:                 ld.RetryCount,
		ResolveLock:                ld.ResolveLock,
	}
	lock.Mu.BackoffTypes = append([]string{}, ld.Mu.BackoffTypes...)
	lock.Mu.SlowestReqTotalTime = ld.Mu.SlowestReqTotalTime
	lock.Mu.SlowestRegion = ld.Mu.SlowestRegion
	lock.Mu.SlowestStoreAddr = ld.Mu.SlowestStoreAddr
	lock.Mu.SlowestExecDetails = ld.Mu.SlowestExecDetails
	return lock
}

// ExecDetails contains execution detail info.
type ExecDetails struct {
	BackoffCount       int64
	BackoffDuration    int64
	WaitKVRespDuration int64
	WaitPDRespDuration int64
	TrafficDetails
}

// TrafficDetails contains traffic detail info.
type TrafficDetails struct {
	UnpackedBytesSentKVTotal          int64
	UnpackedBytesReceivedKVTotal      int64
	UnpackedBytesSentKVCrossZone      int64
	UnpackedBytesReceivedKVCrossZone  int64
	UnpackedBytesSentMPPTotal         int64
	UnpackedBytesReceivedMPPTotal     int64
	UnpackedBytesSentMPPCrossZone     int64
	UnpackedBytesReceivedMPPCrossZone int64
}

// FormatDuration uses to format duration, this function will prune precision before format duration.
// Pruning precision is for human readability. The prune rule is:
//  1. if the duration was less than 1us, return the original string.
//  2. readable value >=10, keep 1 decimal, otherwise, keep 2 decimal. such as:
//     9.412345ms  -> 9.41ms
//     10.412345ms -> 10.4ms
//     5.999s      -> 6s
//     100.45µs    -> 100.5µs
func FormatDuration(d time.Duration) string {
	if d <= time.Microsecond {
		return d.String()
	}
	unit := getUnit(d)
	if unit == time.Nanosecond {
		return d.String()
	}
	integer := (d / unit) * unit
	decimal := float64(d%unit) / float64(unit)
	if d < 10*unit {
		decimal = math.Round(decimal*100) / 100
	} else {
		decimal = math.Round(decimal*10) / 10
	}
	d = integer + time.Duration(decimal*float64(unit))
	return d.String()
}

func getUnit(d time.Duration) time.Duration {
	if d >= time.Second {
		return time.Second
	} else if d >= time.Millisecond {
		return time.Millisecond
	} else if d >= time.Microsecond {
		return time.Microsecond
	}
	return time.Nanosecond
}

// PoolTaskDetails aggregates scheduling and execution details reported by
// read-pool tasks.
type PoolTaskDetails struct {
	// TaskCount is the number of read-pool tasks whose details were reported.
	TaskCount uint64 `json:"task_count"`
	// PollCount is the sum of reported Future::poll counts.
	PollCount uint64 `json:"poll_count"`
	// MaxPollCount is the maximum reported Future::poll count in one sample.
	MaxPollCount uint64 `json:"max_poll_count"`
	// MinPollCount is the minimum reported Future::poll count in one sample.
	MinPollCount uint64 `json:"min_poll_count"`
	// DispatchCount is the sum of reported worker dispatch counts.
	DispatchCount uint64 `json:"dispatch_count"`
	// MaxDispatchCount is the maximum reported worker dispatch count in one sample.
	MaxDispatchCount uint64 `json:"max_dispatch_count"`
	// MinDispatchCount is the minimum reported worker dispatch count in one sample.
	MinDispatchCount uint64 `json:"min_dispatch_count"`
	// TotalWallTime is the sum of reported pool-task wall-time snapshots.
	TotalWallTime time.Duration `json:"total_wall_time"`
	// TaskWallTimeSampleCount is the number of samples with reported pool-task wall time.
	TaskWallTimeSampleCount uint64 `json:"task_wall_time_sample_count"`
	// MaxTaskWallTime is the maximum reported pool-task wall time in one sample.
	MaxTaskWallTime time.Duration `json:"max_task_wall_time"`
	// MinTaskWallTime is the minimum reported pool-task wall time in one sample.
	MinTaskWallTime time.Duration `json:"min_task_wall_time"`
	// TotalQueueWaitTime is the sum of reported ready-queue wait times.
	TotalQueueWaitTime time.Duration `json:"total_queue_wait_time"`
	// MaxQueueWaitTime is the maximum ready-queue wait time.
	MaxQueueWaitTime time.Duration `json:"max_queue_wait_time"`
	// MinQueueWaitTime is the minimum ready-queue wait time among recorded samples.
	MinQueueWaitTime time.Duration `json:"min_queue_wait_time"`
	// TotalWakeWaitTime is the sum of reported times from a pending poll to rescheduling.
	TotalWakeWaitTime time.Duration `json:"total_wake_wait_time"`
	// MaxWakeWaitTime is the maximum time from a pending poll to rescheduling.
	MaxWakeWaitTime time.Duration `json:"max_wake_wait_time"`
	// MinWakeWaitTime is the minimum time from a pending poll to rescheduling among recorded samples.
	MinWakeWaitTime time.Duration `json:"min_wake_wait_time"`
	// FairQueueSampleCount is the number of recorded fair-queue wait samples.
	FairQueueSampleCount uint64 `json:"fair_queue_sample_count"`
	// TotalFairQueueWaitedTaskSlices is the sum of reported task slices dispatched
	// ahead of the sampled tasks while they waited in the fair queue.
	TotalFairQueueWaitedTaskSlices uint64 `json:"total_fair_queue_waited_task_slices"`
	// MaxFairQueueWaitedTaskSlices is the maximum number of task slices dispatched
	// ahead of a sampled task during one fair-queue wait.
	MaxFairQueueWaitedTaskSlices uint64 `json:"max_fair_queue_waited_task_slices"`
	// MinFairQueueWaitedTaskSlices is the minimum number of task slices dispatched
	// ahead of a sampled task during one fair-queue wait.
	MinFairQueueWaitedTaskSlices uint64 `json:"min_fair_queue_waited_task_slices"`
	// PollCPUTime is the sum of reported thread CPU time consumed by Future::poll calls.
	PollCPUTime time.Duration `json:"poll_cpu_time"`
	// MaxPollCPUTime is the maximum thread CPU time consumed by one Future::poll call.
	MaxPollCPUTime time.Duration `json:"max_poll_cpu_time"`
	// MinPollCPUTime is the minimum thread CPU time consumed by one Future::poll call.
	MinPollCPUTime time.Duration `json:"min_poll_cpu_time"`
	// PollWallTime is the sum of reported wall time consumed by Future::poll calls.
	PollWallTime time.Duration `json:"poll_wall_time"`
	// MinPollWallTime is the minimum wall time consumed by one Future::poll call.
	MinPollWallTime time.Duration `json:"min_poll_wall_time"`
	// MaxPollWallTime is the maximum wall time consumed by one Future::poll call.
	MaxPollWallTime time.Duration `json:"max_poll_wall_time"`
}

// MergeFromPB merges one response's protobuf details into the aggregate.
func (d *PoolTaskDetails) MergeFromPB(details *kvrpcpb.PoolTaskDetails) {
	if d == nil || details == nil {
		return
	}
	hadPollSamples := d.PollCount > 0
	hadQueueWaitSamples := d.TotalQueueWaitTime > 0
	hadWakeWaitSamples := d.TotalWakeWaitTime > 0
	hadFairQueueSamples := d.FairQueueSampleCount > 0
	hadTaskWallTimeSamples := d.TaskWallTimeSampleCount > 0
	hadTasks := d.TaskCount > 0

	d.TaskCount++
	pollCount := details.GetPollCount()
	d.PollCount += pollCount
	d.MaxPollCount = max(d.MaxPollCount, pollCount)
	d.MinPollCount = mergePoolTaskMinimum(d.MinPollCount, pollCount, hadTasks)
	dispatchCount := details.GetDispatchCount()
	d.DispatchCount += dispatchCount
	d.MaxDispatchCount = max(d.MaxDispatchCount, dispatchCount)
	d.MinDispatchCount = mergePoolTaskMinimum(d.MinDispatchCount, dispatchCount, hadTasks)
	taskWallTime := time.Duration(details.GetTotalWallNanos())
	d.TotalWallTime += taskWallTime
	if taskWallTime > 0 {
		d.TaskWallTimeSampleCount++
		d.MaxTaskWallTime = max(d.MaxTaskWallTime, taskWallTime)
		d.MinTaskWallTime = mergePoolTaskMinimum(d.MinTaskWallTime, taskWallTime, hadTaskWallTimeSamples)
	}

	d.TotalQueueWaitTime += time.Duration(details.GetTotalQueueWaitNanos())
	d.MaxQueueWaitTime = max(d.MaxQueueWaitTime, time.Duration(details.GetMaxQueueWaitNanos()))
	if details.GetTotalQueueWaitNanos() > 0 {
		d.MinQueueWaitTime = mergePoolTaskMinimum(d.MinQueueWaitTime, time.Duration(details.GetMinQueueWaitNanos()), hadQueueWaitSamples)
	}

	d.TotalWakeWaitTime += time.Duration(details.GetTotalWakeWaitNanos())
	d.MaxWakeWaitTime = max(d.MaxWakeWaitTime, time.Duration(details.GetMaxWakeWaitNanos()))
	if details.GetTotalWakeWaitNanos() > 0 {
		d.MinWakeWaitTime = mergePoolTaskMinimum(d.MinWakeWaitTime, time.Duration(details.GetMinWakeWaitNanos()), hadWakeWaitSamples)
	}

	if details.GetFairQueueEnabled() {
		d.FairQueueSampleCount += dispatchCount
		d.TotalFairQueueWaitedTaskSlices += details.GetTotalFairQueueWaitedTaskSlices()
		d.MaxFairQueueWaitedTaskSlices = max(d.MaxFairQueueWaitedTaskSlices, details.GetMaxFairQueueWaitedTaskSlices())
		d.MinFairQueueWaitedTaskSlices = mergePoolTaskMinimum(
			d.MinFairQueueWaitedTaskSlices,
			details.GetMinFairQueueWaitedTaskSlices(),
			hadFairQueueSamples,
		)
	}

	d.PollCPUTime += time.Duration(details.GetPollCpuNanos())
	d.MaxPollCPUTime = max(d.MaxPollCPUTime, time.Duration(details.GetMaxPollCpuNanos()))
	d.PollWallTime += time.Duration(details.GetPollWallNanos())
	d.MaxPollWallTime = max(d.MaxPollWallTime, time.Duration(details.GetMaxPollWallNanos()))
	if details.GetPollCount() > 0 {
		d.MinPollCPUTime = mergePoolTaskMinimum(d.MinPollCPUTime, time.Duration(details.GetMinPollCpuNanos()), hadPollSamples)
		d.MinPollWallTime = mergePoolTaskMinimum(d.MinPollWallTime, time.Duration(details.GetMinPollWallNanos()), hadPollSamples)
	}
}

// Merge merges another aggregate into d.
func (d *PoolTaskDetails) Merge(other *PoolTaskDetails) {
	if d == nil || other == nil || other.Empty() {
		return
	}
	hadPollSamples := d.PollCount > 0
	hadQueueWaitSamples := d.TotalQueueWaitTime > 0
	hadWakeWaitSamples := d.TotalWakeWaitTime > 0
	hadFairQueueSamples := d.FairQueueSampleCount > 0
	hadTaskWallTimeSamples := d.TaskWallTimeSampleCount > 0
	hadTasks := d.TaskCount > 0

	d.TaskCount += other.TaskCount
	d.PollCount += other.PollCount
	d.MaxPollCount = max(d.MaxPollCount, other.MaxPollCount)
	d.MinPollCount = mergePoolTaskMinimum(d.MinPollCount, other.MinPollCount, hadTasks)
	d.DispatchCount += other.DispatchCount
	d.MaxDispatchCount = max(d.MaxDispatchCount, other.MaxDispatchCount)
	d.MinDispatchCount = mergePoolTaskMinimum(d.MinDispatchCount, other.MinDispatchCount, hadTasks)
	d.TotalWallTime += other.TotalWallTime
	d.TaskWallTimeSampleCount += other.TaskWallTimeSampleCount
	d.MaxTaskWallTime = max(d.MaxTaskWallTime, other.MaxTaskWallTime)
	if other.TotalWallTime > 0 {
		d.MinTaskWallTime = mergePoolTaskMinimum(d.MinTaskWallTime, other.MinTaskWallTime, hadTaskWallTimeSamples)
	}
	d.TotalQueueWaitTime += other.TotalQueueWaitTime
	d.MaxQueueWaitTime = max(d.MaxQueueWaitTime, other.MaxQueueWaitTime)
	if other.TotalQueueWaitTime > 0 {
		d.MinQueueWaitTime = mergePoolTaskMinimum(d.MinQueueWaitTime, other.MinQueueWaitTime, hadQueueWaitSamples)
	}
	d.TotalWakeWaitTime += other.TotalWakeWaitTime
	d.MaxWakeWaitTime = max(d.MaxWakeWaitTime, other.MaxWakeWaitTime)
	if other.TotalWakeWaitTime > 0 {
		d.MinWakeWaitTime = mergePoolTaskMinimum(d.MinWakeWaitTime, other.MinWakeWaitTime, hadWakeWaitSamples)
	}
	d.FairQueueSampleCount += other.FairQueueSampleCount
	d.TotalFairQueueWaitedTaskSlices += other.TotalFairQueueWaitedTaskSlices
	d.MaxFairQueueWaitedTaskSlices = max(d.MaxFairQueueWaitedTaskSlices, other.MaxFairQueueWaitedTaskSlices)
	if other.FairQueueSampleCount > 0 {
		d.MinFairQueueWaitedTaskSlices = mergePoolTaskMinimum(
			d.MinFairQueueWaitedTaskSlices,
			other.MinFairQueueWaitedTaskSlices,
			hadFairQueueSamples,
		)
	}
	d.PollCPUTime += other.PollCPUTime
	d.MaxPollCPUTime = max(d.MaxPollCPUTime, other.MaxPollCPUTime)
	d.PollWallTime += other.PollWallTime
	d.MaxPollWallTime = max(d.MaxPollWallTime, other.MaxPollWallTime)
	if other.PollCount > 0 {
		d.MinPollCPUTime = mergePoolTaskMinimum(d.MinPollCPUTime, other.MinPollCPUTime, hadPollSamples)
		d.MinPollWallTime = mergePoolTaskMinimum(d.MinPollWallTime, other.MinPollWallTime, hadPollSamples)
	}
}

// Clone returns an independent copy of d.
func (d *PoolTaskDetails) Clone() *PoolTaskDetails {
	if d == nil {
		return nil
	}
	clone := *d
	return &clone
}

// Empty reports whether no pool-task details were collected.
func (d *PoolTaskDetails) Empty() bool {
	return d == nil || d.TaskCount == 0
}

// String returns a compact human-readable representation of the aggregate.
func (d *PoolTaskDetails) String() string {
	if d.Empty() {
		return ""
	}
	var buf strings.Builder
	buf.WriteString("{tasks:")
	buf.WriteString(strconv.FormatUint(d.TaskCount, 10))
	writePoolTaskCountStats(&buf, "poll_count", d.PollCount, d.TaskCount, d.MaxPollCount, d.MinPollCount)
	writePoolTaskCountStats(&buf, "dispatch_count", d.DispatchCount, 0, d.MaxDispatchCount, d.MinDispatchCount)
	writePoolTaskTimeStats(
		&buf,
		"task_wall_time",
		d.TotalWallTime,
		d.TaskWallTimeSampleCount,
		d.MaxTaskWallTime,
		d.MinTaskWallTime,
	)
	writePoolTaskTimeStats(
		&buf,
		"queue_wait",
		d.TotalQueueWaitTime,
		d.DispatchCount,
		d.MaxQueueWaitTime,
		d.MinQueueWaitTime,
	)
	wakeWaitCount := uint64(0)
	if d.DispatchCount > d.TaskCount {
		// Each task has one initial dispatch, which
		// has no preceding pending-to-wake interval.
		wakeWaitCount = d.DispatchCount - d.TaskCount
	}
	writePoolTaskTimeStats(
		&buf,
		"wake_wait",
		d.TotalWakeWaitTime,
		wakeWaitCount,
		d.MaxWakeWaitTime,
		d.MinWakeWaitTime,
	)
	buf.WriteString(", fair_queue:{enabled:")
	buf.WriteString(strconv.FormatBool(d.FairQueueSampleCount > 0))
	buf.WriteString(", waited_task_slices:{total:")
	buf.WriteString(strconv.FormatUint(d.TotalFairQueueWaitedTaskSlices, 10))
	if d.FairQueueSampleCount > 0 {
		buf.WriteString(", avg:")
		buf.WriteString(formatPoolTaskAverage(d.TotalFairQueueWaitedTaskSlices, d.FairQueueSampleCount))
	}
	buf.WriteString(", max:")
	buf.WriteString(strconv.FormatUint(d.MaxFairQueueWaitedTaskSlices, 10))
	buf.WriteString(", min:")
	buf.WriteString(strconv.FormatUint(d.MinFairQueueWaitedTaskSlices, 10))
	buf.WriteString("}}")
	writePoolTaskTimeStats(&buf, "poll_cpu", d.PollCPUTime, d.PollCount, d.MaxPollCPUTime, d.MinPollCPUTime)
	writePoolTaskTimeStats(&buf, "poll_wall", d.PollWallTime, d.PollCount, d.MaxPollWallTime, d.MinPollWallTime)
	buf.WriteByte('}')
	return buf.String()
}

func writePoolTaskCountStats(
	buf *strings.Builder,
	name string,
	total uint64,
	averageDivisor uint64,
	maxCount uint64,
	minCount uint64,
) {
	buf.WriteString(", ")
	buf.WriteString(name)
	buf.WriteString(":{total:")
	buf.WriteString(strconv.FormatUint(total, 10))
	if averageDivisor > 0 {
		buf.WriteString(", avg:")
		buf.WriteString(formatPoolTaskAverage(total, averageDivisor))
	}
	buf.WriteString(", max:")
	buf.WriteString(strconv.FormatUint(maxCount, 10))
	buf.WriteString(", min:")
	buf.WriteString(strconv.FormatUint(minCount, 10))
	buf.WriteByte('}')
}

func formatPoolTaskAverage(total, count uint64) string {
	average := strconv.FormatFloat(float64(total)/float64(count), 'f', 2, 64)
	average = strings.TrimRight(average, "0")
	return strings.TrimRight(average, ".")
}

func writePoolTaskTimeStats(
	buf *strings.Builder,
	name string,
	total time.Duration,
	sampleCount uint64,
	maxTime time.Duration,
	minTime time.Duration,
) {
	if total == 0 {
		return
	}
	buf.WriteString(", ")
	buf.WriteString(name)
	buf.WriteString(":{total:")
	buf.WriteString(FormatDuration(total))
	if sampleCount > 0 {
		buf.WriteString(", avg:")
		buf.WriteString(FormatDuration(total / time.Duration(sampleCount)))
	}
	buf.WriteString(", max:")
	buf.WriteString(FormatDuration(maxTime))
	buf.WriteString(", min:")
	buf.WriteString(FormatDuration(minTime))
	buf.WriteByte('}')
}

func mergePoolTaskMinimum[T time.Duration | uint64](current, candidate T, hasCurrent bool) T {
	if !hasCurrent || candidate < current {
		return candidate
	}
	return current
}

// ScanDetail contains coprocessor scan detail information.
type ScanDetail struct {
	// TotalKeys is the approximate number of MVCC keys meet during scanning. It includes
	// deleted versions, but does not include RocksDB tombstone keys.
	TotalKeys int64
	// ProcessedKeys is the number of user keys scanned from the storage.
	// It does not include deleted version or RocksDB tombstone keys.
	// For Coprocessor requests, it includes keys that has been filtered out by Selection.
	ProcessedKeys int64
	// Number of bytes of user key-value pairs scanned from the storage, i.e.
	// total size of data returned from MVCC layer.
	ProcessedKeysSize int64
	// RocksdbDeleteSkippedCount is the total number of deletes and single deletes skipped over during
	// iteration, i.e. how many RocksDB tombstones are skipped.
	RocksdbDeleteSkippedCount uint64
	// RocksdbKeySkippedCount it the total number of internal keys skipped over during iteration.
	RocksdbKeySkippedCount uint64
	// RocksdbBlockCacheHitCount is the total number of RocksDB block cache hits.
	RocksdbBlockCacheHitCount uint64
	// RocksdbBlockReadCount is the total number of block reads (with IO).
	RocksdbBlockReadCount uint64
	// RocksdbBlockReadByte is the total number of bytes from block reads.
	RocksdbBlockReadByte uint64
	// RocksdbBlockReadDuration is the total time used for block reads.
	RocksdbBlockReadDuration time.Duration
	// GetSnapshotDuration is the time spent getting an engine snapshot.
	GetSnapshotDuration time.Duration
	// IaCacheHitCount is the total number of IA segment cache hits.
	IaCacheHitCount uint64
	// IaRemoteReadSegmentCount is the total number of IA remote segment reads.
	IaRemoteReadSegmentCount uint64
	// IaRemoteReadSegmentBytes is the total number of logical bytes returned from IA remote segment reads.
	IaRemoteReadSegmentBytes uint64
	// IaRemoteReadSegmentDuration is the total time spent serving IA remote segment reads.
	IaRemoteReadSegmentDuration time.Duration
}

// Merge merges scan detail execution details into self.
func (sd *ScanDetail) Merge(scanDetail *ScanDetail) {
	atomic.AddInt64(&sd.TotalKeys, scanDetail.TotalKeys)
	atomic.AddInt64(&sd.ProcessedKeys, scanDetail.ProcessedKeys)
	atomic.AddInt64(&sd.ProcessedKeysSize, scanDetail.ProcessedKeysSize)
	atomic.AddUint64(&sd.RocksdbDeleteSkippedCount, scanDetail.RocksdbDeleteSkippedCount)
	atomic.AddUint64(&sd.RocksdbKeySkippedCount, scanDetail.RocksdbKeySkippedCount)
	atomic.AddUint64(&sd.RocksdbBlockCacheHitCount, scanDetail.RocksdbBlockCacheHitCount)
	atomic.AddUint64(&sd.RocksdbBlockReadCount, scanDetail.RocksdbBlockReadCount)
	atomic.AddUint64(&sd.RocksdbBlockReadByte, scanDetail.RocksdbBlockReadByte)
	atomic.AddInt64((*int64)(&sd.RocksdbBlockReadDuration), int64(scanDetail.RocksdbBlockReadDuration))
	atomic.AddInt64((*int64)(&sd.GetSnapshotDuration), int64(scanDetail.GetSnapshotDuration))
	atomic.AddUint64(&sd.IaCacheHitCount, scanDetail.IaCacheHitCount)
	atomic.AddUint64(&sd.IaRemoteReadSegmentCount, scanDetail.IaRemoteReadSegmentCount)
	atomic.AddUint64(&sd.IaRemoteReadSegmentBytes, scanDetail.IaRemoteReadSegmentBytes)
	atomic.AddInt64((*int64)(&sd.IaRemoteReadSegmentDuration), int64(scanDetail.IaRemoteReadSegmentDuration))
}

var zeroScanDetail = ScanDetail{}

// String implements the fmt.Stringer interface.
func (sd *ScanDetail) String() string {
	if sd == nil || *sd == zeroScanDetail {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	buf.WriteString("scan_detail: {")
	if sd.ProcessedKeys > 0 {
		buf.WriteString("total_process_keys: ")
		buf.WriteString(strconv.FormatInt(sd.ProcessedKeys, 10))
		buf.WriteString(", ")
	}
	if sd.ProcessedKeysSize > 0 {
		buf.WriteString("total_process_keys_size: ")
		buf.WriteString(strconv.FormatInt(sd.ProcessedKeysSize, 10))
		buf.WriteString(", ")
	}
	if sd.TotalKeys > 0 {
		buf.WriteString("total_keys: ")
		buf.WriteString(strconv.FormatInt(sd.TotalKeys, 10))
		buf.WriteString(", ")
	}
	if sd.GetSnapshotDuration > 0 {
		buf.WriteString("get_snapshot_time: ")
		buf.WriteString(FormatDuration(sd.GetSnapshotDuration))
		buf.WriteString(", ")
	}
	if sd.IaCacheHitCount > 0 || sd.IaRemoteReadSegmentCount > 0 || sd.IaRemoteReadSegmentBytes > 0 || sd.IaRemoteReadSegmentDuration > 0 {
		buf.WriteString("ia: {")
		if sd.IaCacheHitCount > 0 {
			buf.WriteString("cache_hit_count: ")
			buf.WriteString(strconv.FormatUint(sd.IaCacheHitCount, 10))
			buf.WriteString(", ")
		}
		if sd.IaRemoteReadSegmentCount > 0 {
			buf.WriteString("remote_read_segment_count: ")
			buf.WriteString(strconv.FormatUint(sd.IaRemoteReadSegmentCount, 10))
			buf.WriteString(", ")
		}
		if sd.IaRemoteReadSegmentBytes > 0 {
			buf.WriteString("remote_read_segment_bytes: ")
			buf.WriteString(FormatBytes(int64(sd.IaRemoteReadSegmentBytes)))
			buf.WriteString(", ")
		}
		if sd.IaRemoteReadSegmentDuration > 0 {
			buf.WriteString("remote_read_segment_wait_time: ")
			buf.WriteString(FormatDuration(sd.IaRemoteReadSegmentDuration))
			buf.WriteString(", ")
		}
		if buf.Bytes()[buf.Len()-2] == ',' {
			buf.Truncate(buf.Len() - 2)
		}
		buf.WriteString("}, ")
	}
	buf.WriteString("rocksdb: {")
	if sd.RocksdbDeleteSkippedCount > 0 {
		buf.WriteString("delete_skipped_count: ")
		buf.WriteString(strconv.FormatUint(sd.RocksdbDeleteSkippedCount, 10))
		buf.WriteString(", ")
	}
	if sd.RocksdbKeySkippedCount > 0 {
		buf.WriteString("key_skipped_count: ")
		buf.WriteString(strconv.FormatUint(sd.RocksdbKeySkippedCount, 10))
		buf.WriteString(", ")
	}
	buf.WriteString("block: {")
	if sd.RocksdbBlockCacheHitCount > 0 {
		buf.WriteString("cache_hit_count: ")
		buf.WriteString(strconv.FormatUint(sd.RocksdbBlockCacheHitCount, 10))
		buf.WriteString(", ")
	}
	if sd.RocksdbBlockReadCount > 0 {
		buf.WriteString("read_count: ")
		buf.WriteString(strconv.FormatUint(sd.RocksdbBlockReadCount, 10))
		buf.WriteString(", ")
	}
	if sd.RocksdbBlockReadByte > 0 {
		buf.WriteString("read_byte: ")
		buf.WriteString(FormatBytes(int64(sd.RocksdbBlockReadByte)))
		buf.WriteString(", ")
	}
	if sd.RocksdbBlockReadDuration > 0 {
		buf.WriteString("read_time: ")
		buf.WriteString(FormatDuration(sd.RocksdbBlockReadDuration))
	}
	if buf.Bytes()[buf.Len()-2] == ',' {
		buf.Truncate(buf.Len() - 2)
	}
	buf.WriteString("}}}")
	return buf.String()
}

// MergeFromScanDetailV2 merges scan detail from pb into itself.
func (sd *ScanDetail) MergeFromScanDetailV2(scanDetail *kvrpcpb.ScanDetailV2) {
	if scanDetail != nil {
		sd.TotalKeys += int64(scanDetail.TotalVersions)
		sd.ProcessedKeys += int64(scanDetail.ProcessedVersions)
		sd.ProcessedKeysSize += int64(scanDetail.ProcessedVersionsSize)
		sd.RocksdbDeleteSkippedCount += scanDetail.RocksdbDeleteSkippedCount
		sd.RocksdbKeySkippedCount += scanDetail.RocksdbKeySkippedCount
		sd.RocksdbBlockCacheHitCount += scanDetail.RocksdbBlockCacheHitCount
		sd.RocksdbBlockReadCount += scanDetail.RocksdbBlockReadCount
		sd.RocksdbBlockReadByte += scanDetail.RocksdbBlockReadByte
		sd.RocksdbBlockReadDuration += time.Duration(scanDetail.RocksdbBlockReadNanos) * time.Nanosecond
		sd.GetSnapshotDuration += time.Duration(scanDetail.GetSnapshotNanos) * time.Nanosecond
		sd.IaCacheHitCount += scanDetail.IaCacheHitCount
		sd.IaRemoteReadSegmentCount += scanDetail.IaRemoteReadSegmentCount
		sd.IaRemoteReadSegmentBytes += scanDetail.IaRemoteReadSegmentBytes
		sd.IaRemoteReadSegmentDuration += time.Duration(scanDetail.IaRemoteReadSegmentNanos) * time.Nanosecond
	}
}

// WriteDetail contains the detailed time breakdown of a write operation.
type WriteDetail struct {
	// StoreBatchWaitDuration is the wait duration in the store loop.
	StoreBatchWaitDuration time.Duration
	// ProposeSendWaitDuration is the duration before sending proposal to peers.
	ProposeSendWaitDuration time.Duration
	// PersistLogDuration is the total time spent on persisting the log.
	PersistLogDuration time.Duration
	// RaftDbWriteLeaderWaitDuration is the wait time until the Raft log write leader begins to write.
	RaftDbWriteLeaderWaitDuration time.Duration
	// RaftDbSyncLogDuration is the time spent on synchronizing the Raft log to the disk.
	RaftDbSyncLogDuration time.Duration
	// RaftDbWriteMemtableDuration is the time spent on writing the Raft log to the Raft memtable.
	RaftDbWriteMemtableDuration time.Duration
	// CommitLogDuration is the time waiting for peers to confirm the proposal (counting from the instant when the leader sends the proposal message).
	CommitLogDuration time.Duration
	// ApplyBatchWaitDuration is the wait duration in the apply loop.
	ApplyBatchWaitDuration time.Duration
	// ApplyLogDuration is the total time spend to applying the log.
	ApplyLogDuration time.Duration
	// ApplyMutexLockDuration is the wait time until the KV RocksDB lock is acquired.
	ApplyMutexLockDuration time.Duration
	// ApplyWriteLeaderWaitDuration is the wait time until becoming the KV RocksDB write leader.
	ApplyWriteLeaderWaitDuration time.Duration
	// ApplyWriteWalDuration is the time spent on writing the KV DB WAL to the disk.
	ApplyWriteWalDuration time.Duration
	// ApplyWriteMemtableNanos is the time spent on writing to the memtable of the KV RocksDB.
	ApplyWriteMemtableDuration time.Duration
	// SchedulerLatchWaitDuration is the time spent on waiting for acquiring latch in the scheduler layer.
	SchedulerLatchWaitDuration time.Duration
	// SchedulerProcessDuration is the time spent on processing for the write command in the scheduler layer.
	SchedulerProcessDuration time.Duration
	// SchedulerThrottleDuration is the time spent on waiting due to throttled in the scheduler layer.
	SchedulerThrottleDuration time.Duration
	// SchedulerPessimisticLockWaitDuration is the time spent on waiting for pessimistic locks in the scheduler layer.
	SchedulerPessimisticLockWaitDuration time.Duration
}

// MergeFromWriteDetailPb merges WriteDetail protobuf into the current WriteDetail
func (wd *WriteDetail) MergeFromWriteDetailPb(pb *kvrpcpb.WriteDetail) {
	if pb != nil {
		wd.StoreBatchWaitDuration += time.Duration(pb.StoreBatchWaitNanos) * time.Nanosecond
		wd.ProposeSendWaitDuration += time.Duration(pb.ProposeSendWaitNanos) * time.Nanosecond
		wd.PersistLogDuration += time.Duration(pb.PersistLogNanos) * time.Nanosecond
		wd.RaftDbWriteLeaderWaitDuration += time.Duration(pb.RaftDbWriteLeaderWaitNanos) * time.Nanosecond
		wd.RaftDbSyncLogDuration += time.Duration(pb.RaftDbSyncLogNanos) * time.Nanosecond
		wd.RaftDbWriteMemtableDuration += time.Duration(pb.RaftDbWriteMemtableNanos) * time.Nanosecond
		wd.CommitLogDuration += time.Duration(pb.CommitLogNanos) * time.Nanosecond
		wd.ApplyBatchWaitDuration += time.Duration(pb.ApplyBatchWaitNanos) * time.Nanosecond
		wd.ApplyLogDuration += time.Duration(pb.ApplyLogNanos) * time.Nanosecond
		wd.ApplyMutexLockDuration += time.Duration(pb.ApplyMutexLockNanos) * time.Nanosecond
		wd.ApplyWriteLeaderWaitDuration += time.Duration(pb.ApplyWriteLeaderWaitNanos) * time.Nanosecond
		wd.ApplyWriteWalDuration += time.Duration(pb.ApplyWriteWalNanos) * time.Nanosecond
		wd.ApplyWriteMemtableDuration += time.Duration(pb.ApplyWriteMemtableNanos) * time.Nanosecond
		wd.SchedulerLatchWaitDuration += time.Duration(pb.LatchWaitNanos) * time.Nanosecond
		wd.SchedulerProcessDuration += time.Duration(pb.ProcessNanos) * time.Nanosecond
		wd.SchedulerThrottleDuration += time.Duration(pb.ThrottleNanos) * time.Nanosecond
		wd.SchedulerPessimisticLockWaitDuration += time.Duration(pb.PessimisticLockWaitNanos) * time.Nanosecond
	}
}

// Merge merges another WriteDetail protobuf into self.
func (wd *WriteDetail) Merge(writeDetail *WriteDetail) {
	atomic.AddInt64((*int64)(&wd.StoreBatchWaitDuration), int64(writeDetail.StoreBatchWaitDuration))
	atomic.AddInt64((*int64)(&wd.ProposeSendWaitDuration), int64(writeDetail.ProposeSendWaitDuration))
	atomic.AddInt64((*int64)(&wd.PersistLogDuration), int64(writeDetail.PersistLogDuration))
	atomic.AddInt64((*int64)(&wd.RaftDbWriteLeaderWaitDuration), int64(writeDetail.RaftDbWriteLeaderWaitDuration))
	atomic.AddInt64((*int64)(&wd.RaftDbSyncLogDuration), int64(writeDetail.RaftDbSyncLogDuration))
	atomic.AddInt64((*int64)(&wd.RaftDbWriteMemtableDuration), int64(writeDetail.RaftDbWriteMemtableDuration))
	atomic.AddInt64((*int64)(&wd.CommitLogDuration), int64(writeDetail.CommitLogDuration))
	atomic.AddInt64((*int64)(&wd.ApplyBatchWaitDuration), int64(writeDetail.ApplyBatchWaitDuration))
	atomic.AddInt64((*int64)(&wd.ApplyLogDuration), int64(writeDetail.ApplyLogDuration))
	atomic.AddInt64((*int64)(&wd.ApplyMutexLockDuration), int64(writeDetail.ApplyMutexLockDuration))
	atomic.AddInt64((*int64)(&wd.ApplyWriteLeaderWaitDuration), int64(writeDetail.ApplyWriteLeaderWaitDuration))
	atomic.AddInt64((*int64)(&wd.ApplyWriteWalDuration), int64(writeDetail.ApplyWriteWalDuration))
	atomic.AddInt64((*int64)(&wd.ApplyWriteMemtableDuration), int64(writeDetail.ApplyWriteMemtableDuration))
	atomic.AddInt64((*int64)(&wd.SchedulerLatchWaitDuration), int64(writeDetail.SchedulerLatchWaitDuration))
	atomic.AddInt64((*int64)(&wd.SchedulerProcessDuration), int64(writeDetail.SchedulerProcessDuration))
	atomic.AddInt64((*int64)(&wd.SchedulerThrottleDuration), int64(writeDetail.SchedulerThrottleDuration))
	atomic.AddInt64((*int64)(&wd.SchedulerPessimisticLockWaitDuration), int64(writeDetail.SchedulerPessimisticLockWaitDuration))
}

var zeroWriteDetail = WriteDetail{}

func (wd *WriteDetail) String() string {
	if wd == nil || *wd == zeroWriteDetail {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 64))
	buf.WriteString("write_detail: {")
	buf.WriteString("store_batch_wait: ")
	buf.WriteString(FormatDuration(wd.StoreBatchWaitDuration))
	buf.WriteString(", propose_send_wait: ")
	buf.WriteString(FormatDuration(wd.ProposeSendWaitDuration))
	buf.WriteString(", persist_log: {total: ")
	buf.WriteString(FormatDuration(wd.PersistLogDuration))
	buf.WriteString(", write_leader_wait: ")
	buf.WriteString(FormatDuration(wd.RaftDbWriteLeaderWaitDuration))
	buf.WriteString(", sync_log: ")
	buf.WriteString(FormatDuration(wd.RaftDbSyncLogDuration))
	buf.WriteString(", write_memtable: ")
	buf.WriteString(FormatDuration(wd.RaftDbWriteMemtableDuration))
	buf.WriteString("}, commit_log: ")
	buf.WriteString(FormatDuration(wd.CommitLogDuration))
	buf.WriteString(", apply_batch_wait: ")
	buf.WriteString(FormatDuration(wd.ApplyBatchWaitDuration))
	buf.WriteString(", apply: {total:")
	buf.WriteString(FormatDuration(wd.ApplyLogDuration))
	buf.WriteString(", mutex_lock: ")
	buf.WriteString(FormatDuration(wd.ApplyMutexLockDuration))
	buf.WriteString(", write_leader_wait: ")
	buf.WriteString(FormatDuration(wd.ApplyWriteLeaderWaitDuration))
	buf.WriteString(", write_wal: ")
	buf.WriteString(FormatDuration(wd.ApplyWriteWalDuration))
	buf.WriteString(", write_memtable: ")
	buf.WriteString(FormatDuration(wd.ApplyWriteMemtableDuration))
	buf.WriteString("}, scheduler: {process: ")
	buf.WriteString(FormatDuration(wd.SchedulerProcessDuration))
	if wd.SchedulerLatchWaitDuration > 0 {
		buf.WriteString(", latch_wait: ")
		buf.WriteString(FormatDuration(wd.SchedulerLatchWaitDuration))
	}
	if wd.SchedulerPessimisticLockWaitDuration > 0 {
		buf.WriteString(", pessimistic_lock_wait: ")
		buf.WriteString(FormatDuration(wd.SchedulerPessimisticLockWaitDuration))
	}
	if wd.SchedulerThrottleDuration > 0 {
		buf.WriteString(", throttle: ")
		buf.WriteString(FormatDuration(wd.SchedulerThrottleDuration))
	}
	buf.WriteString("}}")
	return buf.String()
}

// TimeDetail contains coprocessor time detail information.
type TimeDetail struct {
	// Off-cpu and on-cpu wall time elapsed to actually process the request payload. It does not
	// include `wait_wall_time`.
	// This field is very close to the CPU time in most cases. Some wait time spend in RocksDB
	// cannot be excluded for now, like Mutex wait time, which is included in this field, so that
	// this field is called wall time instead of CPU time.
	ProcessTime time.Duration
	// Time elapsed when a coprocessor task yields itself.
	SuspendTime time.Duration
	// Off-cpu wall time elapsed in TiKV side. Usually this includes queue waiting time and
	// other kind of waits in series.
	WaitTime time.Duration
	// KvReadWallTime is the time used in KV Scan/Get. For get/batch_get,
	// this is total duration, which is almost the same with grpc duration.
	KvReadWallTime time.Duration
	// KvGrpcProcessTime is the time used in TiKV gRPC request processing,
	// measured from receiving the request to the start of handling the request.
	KvGrpcProcessTime time.Duration
	// KvGrpcWaitTime is the time used in TiKV gRPC response waiting, measured
	// from when the response is ready to when sending begins.
	KvGrpcWaitTime time.Duration
	// TotalRPCWallTime is Total wall clock time spent on this RPC in TiKV.
	TotalRPCWallTime time.Duration
}

// String implements the fmt.Stringer interface.
func (td *TimeDetail) String() string {
	if td == nil {
		return ""
	}
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	if td.ProcessTime > 0 {
		buf.WriteString("total_process_time: ")
		buf.WriteString(FormatDuration(td.ProcessTime))
	}
	if td.SuspendTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("total_suspend_time: ")
		buf.WriteString(FormatDuration(td.SuspendTime))
	}
	if td.WaitTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("total_wait_time: ")
		buf.WriteString(FormatDuration(td.WaitTime))
	}
	if td.KvReadWallTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("total_kv_read_wall_time: ")
		buf.WriteString(FormatDuration(td.KvReadWallTime))
	}
	if td.KvGrpcProcessTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("tikv_grpc_process_time: ")
		buf.WriteString(FormatDuration(td.KvGrpcProcessTime))
	}
	if td.KvGrpcWaitTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("tikv_grpc_wait_time: ")
		buf.WriteString(FormatDuration(td.KvGrpcWaitTime))
	}
	if td.TotalRPCWallTime > 0 {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("tikv_wall_time: ")
		buf.WriteString(FormatDuration(td.TotalRPCWallTime))
	}
	if buf.Len() == 0 {
		return ""
	}
	return "time_detail: {" + buf.String() + "}"
}

// Merge merges the time detail into itself.
// Note this function could be called concurrently.
func (td *TimeDetail) Merge(detail *TimeDetail) {
	if detail != nil {
		atomic.AddInt64((*int64)(&td.ProcessTime), int64(detail.ProcessTime))
		atomic.AddInt64((*int64)(&td.SuspendTime), int64(detail.SuspendTime))
		atomic.AddInt64((*int64)(&td.WaitTime), int64(detail.WaitTime))
		atomic.AddInt64((*int64)(&td.KvReadWallTime), int64(detail.KvReadWallTime))
		atomic.AddInt64((*int64)(&td.KvGrpcProcessTime), int64(detail.KvGrpcProcessTime))
		atomic.AddInt64((*int64)(&td.KvGrpcWaitTime), int64(detail.KvGrpcWaitTime))
		atomic.AddInt64((*int64)(&td.TotalRPCWallTime), int64(detail.TotalRPCWallTime))
	}
}

// MergeFromTimeDetail merges time detail from pb into itself.
func (td *TimeDetail) MergeFromTimeDetail(timeDetailV2 *kvrpcpb.TimeDetailV2, timeDetail *kvrpcpb.TimeDetail) {
	if timeDetailV2 != nil {
		td.WaitTime += time.Duration(timeDetailV2.WaitWallTimeNs) * time.Nanosecond
		td.ProcessTime += time.Duration(timeDetailV2.ProcessWallTimeNs) * time.Nanosecond
		td.SuspendTime += time.Duration(timeDetailV2.ProcessSuspendWallTimeNs) * time.Nanosecond
		td.KvReadWallTime += time.Duration(timeDetailV2.KvReadWallTimeNs) * time.Nanosecond
		td.KvGrpcProcessTime += time.Duration(timeDetailV2.KvGrpcProcessTimeNs) * time.Nanosecond
		td.KvGrpcWaitTime += time.Duration(timeDetailV2.KvGrpcWaitTimeNs) * time.Nanosecond
		td.TotalRPCWallTime += time.Duration(timeDetailV2.TotalRpcWallTimeNs) * time.Nanosecond
	} else if timeDetail != nil {
		td.WaitTime += time.Duration(timeDetail.WaitWallTimeMs) * time.Millisecond
		td.ProcessTime += time.Duration(timeDetail.ProcessWallTimeMs) * time.Millisecond
		td.KvReadWallTime += time.Duration(timeDetail.KvReadWallTimeMs) * time.Millisecond
		td.TotalRPCWallTime += time.Duration(timeDetail.TotalRpcWallTimeNs) * time.Nanosecond
	}
}

// ResolveLockDetail contains the resolve lock detail information.
type ResolveLockDetail struct {
	// ResolveLockTime is the total duration of resolving lock.
	ResolveLockTime int64
	// TODO(you06): add more details of resolving locks.
}

// Merge merges resolve lock detail details into self.
func (rd *ResolveLockDetail) Merge(resolveLock *ResolveLockDetail) {
	rd.ResolveLockTime += resolveLock.ResolveLockTime
}

// RUDetails contains RU detail info.
type RUDetails struct {
	readRU         *uatomic.Float64
	writeRU        *uatomic.Float64
	ruWaitDuration *uatomic.Duration
	// tiflashRU stores RRU+WRU of Tiflash.
	tiflashRU *uatomic.Float64
	// tikvRUV2 stores TiKV RU v2 value in scaled units.
	tikvRUV2 *uatomic.Float64
	// rawRUV2Mu protects rawRUV2, which accumulates raw TiKV RU v2 counters
	// for TiDB to drain incrementally into statement-level RUV2 metrics.
	rawRUV2Mu sync.Mutex
	// rawRUV2 stores pending raw TiKV RU v2 counters since the last DrainRUV2 call.
	rawRUV2 *kvrpcpb.RUV2
	// calculationMu protects statement-level RU v1 formula inputs.
	calculationMu      sync.Mutex
	calculation        *resourceControlClient.RUCalculation
	calculationInvalid bool
}

// NewRUDetails creates a new RUDetails.
func NewRUDetails() *RUDetails {
	return &RUDetails{
		readRU:         uatomic.NewFloat64(0),
		writeRU:        uatomic.NewFloat64(0),
		ruWaitDuration: uatomic.NewDuration(0),
		tiflashRU:      uatomic.NewFloat64(0),
		tikvRUV2:       uatomic.NewFloat64(0),
	}
}

// NewRUDetails creates a new RUDetails with specifical values.
// This function is used in tidb's unit test.
func NewRUDetailsWith(rru, wru float64, waitDur time.Duration) *RUDetails {
	return &RUDetails{
		readRU:         uatomic.NewFloat64(rru),
		writeRU:        uatomic.NewFloat64(wru),
		ruWaitDuration: uatomic.NewDuration(waitDur),
		tiflashRU:      uatomic.NewFloat64(0),
		tikvRUV2:       uatomic.NewFloat64(0),
	}
}

// Clone implements the RuntimeStats interface.
func (rd *RUDetails) Clone() *RUDetails {
	cloned := &RUDetails{
		readRU:         uatomic.NewFloat64(rd.readRU.Load()),
		writeRU:        uatomic.NewFloat64(rd.writeRU.Load()),
		ruWaitDuration: uatomic.NewDuration(rd.ruWaitDuration.Load()),
		tiflashRU:      uatomic.NewFloat64(rd.tiflashRU.Load()),
		tikvRUV2:       uatomic.NewFloat64(rd.tikvRUV2.Load()),
	}
	rd.rawRUV2Mu.Lock()
	cloned.rawRUV2 = cloneRUV2(rd.rawRUV2)
	rd.rawRUV2Mu.Unlock()
	calculation, exists, invalid := rd.ruCalculationState()
	cloned.calculationInvalid = invalid
	if exists {
		cloned.calculation = &calculation
	}
	return cloned
}

// Merge implements the RuntimeStats interface.
func (rd *RUDetails) Merge(other *RUDetails) {
	rd.readRU.Add(other.readRU.Load())
	rd.writeRU.Add(other.writeRU.Load())
	rd.ruWaitDuration.Add(other.ruWaitDuration.Load())
	rd.tiflashRU.Add(other.tiflashRU.Load())
	rd.tikvRUV2.Add(other.tikvRUV2.Load())
	rd.AddRUV2(other.getRawRUV2())
	calculation, exists, invalid := other.ruCalculationState()
	if invalid {
		rd.invalidateRUCalculation()
	} else if exists {
		rd.AddRUCalculation(calculation)
	}
}

// String implements fmt.Stringer interface.
func (rd *RUDetails) String() string {
	return fmt.Sprintf(
		"RRU:%f, WRU:%f, WaitDuration:%v",
		rd.readRU.Load(),
		rd.writeRU.Load(),
		rd.ruWaitDuration.Load(),
	)
}

// RRU returns the read RU.
func (rd *RUDetails) RRU() float64 {
	return rd.readRU.Load()
}

// WRU returns the write RU.
func (rd *RUDetails) WRU() float64 {
	return rd.writeRU.Load()
}

// RUWaitDuration returns the time duration waiting for available RU.
func (rd *RUDetails) RUWaitDuration() time.Duration {
	return rd.ruWaitDuration.Load()
}

// TiflashRU returns the Tiflash RU (RRU+WRU) accumulated in the client.
func (rd *RUDetails) TiflashRU() float64 {
	return rd.tiflashRU.Load()
}

// TiKVRUV2 returns the TiKV RU v2 value accumulated in the client.
func (rd *RUDetails) TiKVRUV2() float64 {
	return rd.tikvRUV2.Load()
}

// AddTiKVRUV2 adds a delta (scaled) to the accumulated TiKV RU v2 value.
func (rd *RUDetails) AddTiKVRUV2(delta float64) {
	if rd == nil || delta == 0 {
		return
	}
	rd.tikvRUV2.Add(delta)
}

func (rd *RUDetails) getRawRUV2() *kvrpcpb.RUV2 {
	if rd == nil {
		return nil
	}
	rd.rawRUV2Mu.Lock()
	defer rd.rawRUV2Mu.Unlock()
	return cloneRUV2(rd.rawRUV2)
}

// AddRUV2 accumulates raw TiKV RU v2 counters in RUDetails.
func (rd *RUDetails) AddRUV2(delta *kvrpcpb.RUV2) {
	if rd == nil || delta == nil {
		return
	}
	rd.rawRUV2Mu.Lock()
	defer rd.rawRUV2Mu.Unlock()
	if rd.rawRUV2 == nil {
		rd.rawRUV2 = cloneRUV2(delta)
		return
	}
	mergeRUV2(rd.rawRUV2, delta)
}

// DrainRUV2 returns the accumulated raw TiKV RU v2 counters and clears them.
func (rd *RUDetails) DrainRUV2() *kvrpcpb.RUV2 {
	if rd == nil {
		return nil
	}
	rd.rawRUV2Mu.Lock()
	defer rd.rawRUV2Mu.Unlock()
	drained := cloneRUV2(rd.rawRUV2)
	rd.rawRUV2 = nil
	return drained
}

// Update updates the RU runtime stats with the given consumption info.
func (rd *RUDetails) Update(consumption *rmpb.Consumption, waitDuration time.Duration) {
	if rd == nil || consumption == nil {
		return
	}
	rd.readRU.Add(consumption.RRU)
	rd.writeRU.Add(consumption.WRU)
	rd.ruWaitDuration.Add(waitDuration)
}

// AddRUCalculation adds RU v1 formula inputs collected from PD. Calculations
// with different factor snapshots cannot be represented by one formula, so
// the detail is discarded in that case.
func (rd *RUDetails) AddRUCalculation(delta resourceControlClient.RUCalculation) {
	if rd == nil {
		return
	}
	rd.calculationMu.Lock()
	defer rd.calculationMu.Unlock()
	if rd.calculationInvalid {
		return
	}
	if rd.calculation == nil {
		calculation := delta
		rd.calculation = &calculation
		return
	}
	if rd.calculation.Factors != delta.Factors {
		rd.calculation = nil
		rd.calculationInvalid = true
		return
	}
	rd.calculation.Add(delta)
}

// RUCalculation returns the statement's RU v1 calculation, whether one was
// collected, and whether all collected inputs used the same factor snapshot.
func (rd *RUDetails) RUCalculation() (resourceControlClient.RUCalculation, bool, bool) {
	calculation, exists, invalid := rd.ruCalculationState()
	return calculation, exists, !invalid
}

func (rd *RUDetails) ruCalculationState() (
	resourceControlClient.RUCalculation, bool, bool,
) {
	if rd == nil {
		return resourceControlClient.RUCalculation{}, false, false
	}
	rd.calculationMu.Lock()
	defer rd.calculationMu.Unlock()
	if rd.calculationInvalid {
		return resourceControlClient.RUCalculation{}, false, true
	}
	if rd.calculation == nil {
		return resourceControlClient.RUCalculation{}, false, false
	}
	return *rd.calculation, true, false
}

func (rd *RUDetails) invalidateRUCalculation() {
	if rd == nil {
		return
	}
	rd.calculationMu.Lock()
	rd.calculation = nil
	rd.calculationInvalid = true
	rd.calculationMu.Unlock()
}

// UpdateTiFlash updates the Tiflash RU (RRU+WRU) with the given consumption info.
func (rd *RUDetails) UpdateTiFlash(consumption *rmpb.Consumption) {
	if rd == nil || consumption == nil {
		return
	}
	rd.readRU.Add(consumption.RRU)
	rd.writeRU.Add(consumption.WRU)
	rd.tiflashRU.Add(consumption.RRU + consumption.WRU)
}
