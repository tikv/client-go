// Copyright 2026 TiKV Authors
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLockKeysDetailsMerge(t *testing.T) {
	a := &LockKeysDetails{
		TotalTime:                  10 * time.Millisecond,
		RegionNum:                  2,
		LockKeys:                   5,
		AggressiveLockNewCount:     1,
		AggressiveLockDerivedCount: 2,
		LockedWithConflictCount:    3,
		ResolveLock:                ResolveLockDetail{ResolveLockTime: 100},
		BackoffTime:                200,
		LockRPCTime:                300,
		LockRPCCount:               4,
		RetryCount:                 1,
	}
	a.Mu.BackoffTypes = []string{"txnLock"}
	a.Mu.SlowestReqTotalTime = 5 * time.Millisecond
	a.Mu.SlowestRegion = 10
	a.Mu.SlowestStoreAddr = "store1"

	b := &LockKeysDetails{
		TotalTime:                  20 * time.Millisecond,
		RegionNum:                  3,
		LockKeys:                   7,
		AggressiveLockNewCount:     4,
		AggressiveLockDerivedCount: 5,
		LockedWithConflictCount:    6,
		ResolveLock:                ResolveLockDetail{ResolveLockTime: 150},
		BackoffTime:                250,
		LockRPCTime:                350,
		LockRPCCount:               5,
		RetryCount:                 2,
	}
	b.Mu.BackoffTypes = []string{"regionMiss"}
	b.Mu.SlowestReqTotalTime = 8 * time.Millisecond
	b.Mu.SlowestRegion = 20
	b.Mu.SlowestStoreAddr = "store2"

	a.Merge(b)

	assert.Equal(t, 30*time.Millisecond, a.TotalTime)
	assert.Equal(t, int32(5), a.RegionNum)
	assert.Equal(t, int32(12), a.LockKeys)
	assert.Equal(t, 5, a.AggressiveLockNewCount)
	assert.Equal(t, 7, a.AggressiveLockDerivedCount)
	assert.Equal(t, 9, a.LockedWithConflictCount)
	assert.Equal(t, int64(250), a.ResolveLock.ResolveLockTime)
	assert.Equal(t, int64(450), a.BackoffTime)
	assert.Equal(t, int64(650), a.LockRPCTime)
	assert.Equal(t, int64(9), a.LockRPCCount)
	assert.Equal(t, 2, a.RetryCount) // RetryCount is incremented by 1 per Merge call
	assert.Equal(t, []string{"txnLock", "regionMiss"}, a.Mu.BackoffTypes)
	// b has a slower request, so a should adopt b's slowest info
	assert.Equal(t, 8*time.Millisecond, a.Mu.SlowestReqTotalTime)
	assert.Equal(t, uint64(20), a.Mu.SlowestRegion)
	assert.Equal(t, "store2", a.Mu.SlowestStoreAddr)
}

func TestLockKeysDetailsMergeSlowestNotReplaced(t *testing.T) {
	a := &LockKeysDetails{}
	a.Mu.SlowestReqTotalTime = 10 * time.Millisecond
	a.Mu.SlowestRegion = 1
	a.Mu.SlowestStoreAddr = "store1"

	b := &LockKeysDetails{}
	b.Mu.SlowestReqTotalTime = 5 * time.Millisecond
	b.Mu.SlowestRegion = 2
	b.Mu.SlowestStoreAddr = "store2"

	a.Merge(b)

	// a already has the slower request, should keep its own info
	assert.Equal(t, 10*time.Millisecond, a.Mu.SlowestReqTotalTime)
	assert.Equal(t, uint64(1), a.Mu.SlowestRegion)
	assert.Equal(t, "store1", a.Mu.SlowestStoreAddr)
}

func TestLockKeysDetailsClone(t *testing.T) {
	orig := &LockKeysDetails{
		TotalTime:                  10 * time.Millisecond,
		RegionNum:                  2,
		LockKeys:                   5,
		AggressiveLockNewCount:     1,
		AggressiveLockDerivedCount: 2,
		LockedWithConflictCount:    3,
		ResolveLock:                ResolveLockDetail{ResolveLockTime: 100},
		BackoffTime:                200,
		LockRPCTime:                300,
		LockRPCCount:               4,
		RetryCount:                 1,
	}
	orig.Mu.BackoffTypes = []string{"txnLock", "regionMiss"}
	orig.Mu.SlowestReqTotalTime = 5 * time.Millisecond
	orig.Mu.SlowestRegion = 10
	orig.Mu.SlowestStoreAddr = "store1"

	cloned := orig.Clone()

	// Verify all fields are equal
	assert.Equal(t, orig.TotalTime, cloned.TotalTime)
	assert.Equal(t, orig.RegionNum, cloned.RegionNum)
	assert.Equal(t, orig.LockKeys, cloned.LockKeys)
	assert.Equal(t, orig.AggressiveLockNewCount, cloned.AggressiveLockNewCount)
	assert.Equal(t, orig.AggressiveLockDerivedCount, cloned.AggressiveLockDerivedCount)
	assert.Equal(t, orig.LockedWithConflictCount, cloned.LockedWithConflictCount)
	assert.Equal(t, orig.ResolveLock, cloned.ResolveLock)
	assert.Equal(t, orig.BackoffTime, cloned.BackoffTime)
	assert.Equal(t, orig.LockRPCTime, cloned.LockRPCTime)
	assert.Equal(t, orig.LockRPCCount, cloned.LockRPCCount)
	assert.Equal(t, orig.RetryCount, cloned.RetryCount)
	assert.Equal(t, orig.Mu.BackoffTypes, cloned.Mu.BackoffTypes)
	assert.Equal(t, orig.Mu.SlowestReqTotalTime, cloned.Mu.SlowestReqTotalTime)
	assert.Equal(t, orig.Mu.SlowestRegion, cloned.Mu.SlowestRegion)
	assert.Equal(t, orig.Mu.SlowestStoreAddr, cloned.Mu.SlowestStoreAddr)

	// Verify deep copy: modifying cloned slice should not affect original
	cloned.Mu.BackoffTypes = append(cloned.Mu.BackoffTypes, "extra")
	assert.Len(t, orig.Mu.BackoffTypes, 2)

	cloned.TotalTime = 999 * time.Millisecond
	assert.Equal(t, 10*time.Millisecond, orig.TotalTime)
}

func TestCommitDetailsMerge(t *testing.T) {
	a := &CommitDetails{
		GetCommitTsTime:        10 * time.Millisecond,
		GetLatestTsTime:        5 * time.Millisecond,
		PrewriteTime:           20 * time.Millisecond,
		WaitPrewriteBinlogTime: 3 * time.Millisecond,
		CommitTime:             15 * time.Millisecond,
		LocalLatchTime:         2 * time.Millisecond,
		WriteKeys:              100,
		WriteSize:              2000,
		PrewriteRegionNum:      4,
		TxnRetry:               1,
		ResolveLock:            ResolveLockDetail{ResolveLockTime: 50},
	}
	a.Mu.CommitBackoffTime = 100
	a.Mu.PrewriteBackoffTypes = []string{"txnLock"}
	a.Mu.CommitBackoffTypes = []string{"regionMiss"}
	a.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 1, StoreAddr: "s1"}
	a.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 3 * time.Millisecond, Region: 2, StoreAddr: "s2"}

	b := &CommitDetails{
		GetCommitTsTime:        12 * time.Millisecond,
		GetLatestTsTime:        6 * time.Millisecond,
		PrewriteTime:           25 * time.Millisecond,
		WaitPrewriteBinlogTime: 4 * time.Millisecond,
		CommitTime:             18 * time.Millisecond,
		LocalLatchTime:         3 * time.Millisecond,
		WriteKeys:              150,
		WriteSize:              3000,
		PrewriteRegionNum:      5,
		TxnRetry:               2,
		ResolveLock:            ResolveLockDetail{ResolveLockTime: 60},
	}
	b.Mu.CommitBackoffTime = 200
	b.Mu.PrewriteBackoffTypes = []string{"tikvRPC"}
	b.Mu.CommitBackoffTypes = []string{"txnLock"}
	b.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 8 * time.Millisecond, Region: 10, StoreAddr: "s10"}
	b.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 6 * time.Millisecond, Region: 20, StoreAddr: "s20"}

	a.Merge(b)

	assert.Equal(t, 22*time.Millisecond, a.GetCommitTsTime)
	assert.Equal(t, 11*time.Millisecond, a.GetLatestTsTime)
	assert.Equal(t, 45*time.Millisecond, a.PrewriteTime)
	assert.Equal(t, 7*time.Millisecond, a.WaitPrewriteBinlogTime)
	assert.Equal(t, 33*time.Millisecond, a.CommitTime)
	assert.Equal(t, 5*time.Millisecond, a.LocalLatchTime)
	assert.Equal(t, 250, a.WriteKeys)
	assert.Equal(t, 5000, a.WriteSize)
	assert.Equal(t, int32(9), a.PrewriteRegionNum)
	assert.Equal(t, 3, a.TxnRetry)
	assert.Equal(t, int64(110), a.ResolveLock.ResolveLockTime)
	assert.Equal(t, int64(300), a.Mu.CommitBackoffTime)
	assert.Equal(t, []string{"txnLock", "tikvRPC"}, a.Mu.PrewriteBackoffTypes)
	assert.Equal(t, []string{"regionMiss", "txnLock"}, a.Mu.CommitBackoffTypes)
	// b has slower prewrite and commit, so a should adopt b's info
	assert.Equal(t, 8*time.Millisecond, a.Mu.SlowestPrewrite.ReqTotalTime)
	assert.Equal(t, uint64(10), a.Mu.SlowestPrewrite.Region)
	assert.Equal(t, "s10", a.Mu.SlowestPrewrite.StoreAddr)
	assert.Equal(t, 6*time.Millisecond, a.Mu.CommitPrimary.ReqTotalTime)
	assert.Equal(t, uint64(20), a.Mu.CommitPrimary.Region)
	assert.Equal(t, "s20", a.Mu.CommitPrimary.StoreAddr)
}

func TestCommitDetailsMergeSlowestNotReplaced(t *testing.T) {
	a := &CommitDetails{}
	a.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 10 * time.Millisecond, Region: 1}
	a.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 10 * time.Millisecond, Region: 2}

	b := &CommitDetails{}
	b.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 3}
	b.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 4}

	a.Merge(b)

	assert.Equal(t, uint64(1), a.Mu.SlowestPrewrite.Region)
	assert.Equal(t, uint64(2), a.Mu.CommitPrimary.Region)
}

func TestCommitDetailsClone(t *testing.T) {
	orig := &CommitDetails{
		GetCommitTsTime:        10 * time.Millisecond,
		GetLatestTsTime:        5 * time.Millisecond,
		PrewriteTime:           20 * time.Millisecond,
		WaitPrewriteBinlogTime: 3 * time.Millisecond,
		CommitTime:             15 * time.Millisecond,
		LocalLatchTime:         2 * time.Millisecond,
		WriteKeys:              100,
		WriteSize:              2000,
		PrewriteRegionNum:      4,
		TxnRetry:               1,
		ResolveLock:            ResolveLockDetail{ResolveLockTime: 50},
	}
	orig.Mu.CommitBackoffTime = 100
	orig.Mu.PrewriteBackoffTypes = []string{"txnLock", "regionMiss"}
	orig.Mu.CommitBackoffTypes = []string{"tikvRPC"}
	orig.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 1, StoreAddr: "s1"}
	orig.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 3 * time.Millisecond, Region: 2, StoreAddr: "s2"}

	cloned := orig.Clone()

	assert.Equal(t, orig.GetCommitTsTime, cloned.GetCommitTsTime)
	assert.Equal(t, orig.GetLatestTsTime, cloned.GetLatestTsTime)
	assert.Equal(t, orig.PrewriteTime, cloned.PrewriteTime)
	assert.Equal(t, orig.WaitPrewriteBinlogTime, cloned.WaitPrewriteBinlogTime)
	assert.Equal(t, orig.CommitTime, cloned.CommitTime)
	assert.Equal(t, orig.LocalLatchTime, cloned.LocalLatchTime)
	assert.Equal(t, orig.WriteKeys, cloned.WriteKeys)
	assert.Equal(t, orig.WriteSize, cloned.WriteSize)
	assert.Equal(t, orig.PrewriteRegionNum, cloned.PrewriteRegionNum)
	assert.Equal(t, orig.TxnRetry, cloned.TxnRetry)
	assert.Equal(t, orig.ResolveLock, cloned.ResolveLock)
	assert.Equal(t, orig.Mu.CommitBackoffTime, cloned.Mu.CommitBackoffTime)
	assert.Equal(t, orig.Mu.PrewriteBackoffTypes, cloned.Mu.PrewriteBackoffTypes)
	assert.Equal(t, orig.Mu.CommitBackoffTypes, cloned.Mu.CommitBackoffTypes)
	assert.Equal(t, orig.Mu.SlowestPrewrite, cloned.Mu.SlowestPrewrite)
	assert.Equal(t, orig.Mu.CommitPrimary, cloned.Mu.CommitPrimary)

	// Verify deep copy: modifying cloned slices should not affect original
	cloned.Mu.PrewriteBackoffTypes = append(cloned.Mu.PrewriteBackoffTypes, "extra")
	assert.Len(t, orig.Mu.PrewriteBackoffTypes, 2)

	cloned.Mu.CommitBackoffTypes = append(cloned.Mu.CommitBackoffTypes, "extra")
	assert.Len(t, orig.Mu.CommitBackoffTypes, 1)

	cloned.GetCommitTsTime = 999 * time.Millisecond
	assert.Equal(t, 10*time.Millisecond, orig.GetCommitTsTime)
}

func TestScanDetailMerge(t *testing.T) {
	a := &ScanDetail{
		TotalKeys:                 100,
		ProcessedKeys:             50,
		ProcessedKeysSize:         1000,
		RocksdbDeleteSkippedCount: 10,
		RocksdbKeySkippedCount:    20,
		RocksdbBlockCacheHitCount: 30,
		RocksdbBlockReadCount:     40,
		RocksdbBlockReadByte:      5000,
		RocksdbBlockReadDuration:  1 * time.Millisecond,
		GetSnapshotDuration:       2 * time.Millisecond,
	}
	b := &ScanDetail{
		TotalKeys:                 200,
		ProcessedKeys:             80,
		ProcessedKeysSize:         2000,
		RocksdbDeleteSkippedCount: 15,
		RocksdbKeySkippedCount:    25,
		RocksdbBlockCacheHitCount: 35,
		RocksdbBlockReadCount:     45,
		RocksdbBlockReadByte:      6000,
		RocksdbBlockReadDuration:  3 * time.Millisecond,
		GetSnapshotDuration:       4 * time.Millisecond,
	}

	a.Merge(b)

	assert.Equal(t, int64(300), a.TotalKeys)
	assert.Equal(t, int64(130), a.ProcessedKeys)
	assert.Equal(t, int64(3000), a.ProcessedKeysSize)
	assert.Equal(t, uint64(25), a.RocksdbDeleteSkippedCount)
	assert.Equal(t, uint64(45), a.RocksdbKeySkippedCount)
	assert.Equal(t, uint64(65), a.RocksdbBlockCacheHitCount)
	assert.Equal(t, uint64(85), a.RocksdbBlockReadCount)
	assert.Equal(t, uint64(11000), a.RocksdbBlockReadByte)
	assert.Equal(t, 4*time.Millisecond, a.RocksdbBlockReadDuration)
	assert.Equal(t, 6*time.Millisecond, a.GetSnapshotDuration)
}

func TestWriteDetailMerge(t *testing.T) {
	a := &WriteDetail{
		StoreBatchWaitDuration:        1 * time.Millisecond,
		ProposeSendWaitDuration:       2 * time.Millisecond,
		PersistLogDuration:            3 * time.Millisecond,
		RaftDbWriteLeaderWaitDuration: 4 * time.Millisecond,
		RaftDbSyncLogDuration:         5 * time.Millisecond,
		RaftDbWriteMemtableDuration:   6 * time.Millisecond,
		CommitLogDuration:             7 * time.Millisecond,
		ApplyBatchWaitDuration:        8 * time.Millisecond,
		ApplyLogDuration:              9 * time.Millisecond,
		ApplyMutexLockDuration:        10 * time.Millisecond,
		ApplyWriteLeaderWaitDuration:  11 * time.Millisecond,
		ApplyWriteWalDuration:         12 * time.Millisecond,
		ApplyWriteMemtableDuration:    13 * time.Millisecond,
	}
	b := &WriteDetail{
		StoreBatchWaitDuration:        1 * time.Millisecond,
		ProposeSendWaitDuration:       2 * time.Millisecond,
		PersistLogDuration:            3 * time.Millisecond,
		RaftDbWriteLeaderWaitDuration: 4 * time.Millisecond,
		RaftDbSyncLogDuration:         5 * time.Millisecond,
		RaftDbWriteMemtableDuration:   6 * time.Millisecond,
		CommitLogDuration:             7 * time.Millisecond,
		ApplyBatchWaitDuration:        8 * time.Millisecond,
		ApplyLogDuration:              9 * time.Millisecond,
		ApplyMutexLockDuration:        10 * time.Millisecond,
		ApplyWriteLeaderWaitDuration:  11 * time.Millisecond,
		ApplyWriteWalDuration:         12 * time.Millisecond,
		ApplyWriteMemtableDuration:    13 * time.Millisecond,
	}

	a.Merge(b)

	assert.Equal(t, 2*time.Millisecond, a.StoreBatchWaitDuration)
	assert.Equal(t, 4*time.Millisecond, a.ProposeSendWaitDuration)
	assert.Equal(t, 6*time.Millisecond, a.PersistLogDuration)
	assert.Equal(t, 8*time.Millisecond, a.RaftDbWriteLeaderWaitDuration)
	assert.Equal(t, 10*time.Millisecond, a.RaftDbSyncLogDuration)
	assert.Equal(t, 12*time.Millisecond, a.RaftDbWriteMemtableDuration)
	assert.Equal(t, 14*time.Millisecond, a.CommitLogDuration)
	assert.Equal(t, 16*time.Millisecond, a.ApplyBatchWaitDuration)
	assert.Equal(t, 18*time.Millisecond, a.ApplyLogDuration)
	assert.Equal(t, 20*time.Millisecond, a.ApplyMutexLockDuration)
	assert.Equal(t, 22*time.Millisecond, a.ApplyWriteLeaderWaitDuration)
	assert.Equal(t, 24*time.Millisecond, a.ApplyWriteWalDuration)
	assert.Equal(t, 26*time.Millisecond, a.ApplyWriteMemtableDuration)
}

func TestTimeDetailMerge(t *testing.T) {
	a := &TimeDetail{
		ProcessTime:      10 * time.Millisecond,
		SuspendTime:      2 * time.Millisecond,
		WaitTime:         5 * time.Millisecond,
		KvReadWallTime:   3 * time.Millisecond,
		TotalRPCWallTime: 20 * time.Millisecond,
	}
	b := &TimeDetail{
		ProcessTime:      15 * time.Millisecond,
		SuspendTime:      3 * time.Millisecond,
		WaitTime:         7 * time.Millisecond,
		KvReadWallTime:   4 * time.Millisecond,
		TotalRPCWallTime: 30 * time.Millisecond,
	}

	a.Merge(b)

	assert.Equal(t, 25*time.Millisecond, a.ProcessTime)
	assert.Equal(t, 5*time.Millisecond, a.SuspendTime)
	assert.Equal(t, 12*time.Millisecond, a.WaitTime)
	assert.Equal(t, 7*time.Millisecond, a.KvReadWallTime)
	assert.Equal(t, 50*time.Millisecond, a.TotalRPCWallTime)
}

func TestTimeDetailMergeNil(t *testing.T) {
	a := &TimeDetail{
		ProcessTime: 10 * time.Millisecond,
	}
	a.Merge(nil)
	assert.Equal(t, 10*time.Millisecond, a.ProcessTime)
}
