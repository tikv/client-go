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

package txnsnapshot

import (
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func newSnapshotWithRuntimeStats(stats *SnapshotRuntimeStats) *KVSnapshot {
	snapshot := &KVSnapshot{}
	snapshot.SetRuntimeStats(stats)
	return snapshot
}

func TestSnapshotRuntimeStatsPointResponseStats(t *testing.T) {
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)

	pointStats := stats.GetPointResponseStats()
	require.True(t, pointStats.IsValid())
	require.False(t, pointStats.ScanDetailComplete())
	require.False(t, pointStats.PayloadComplete())
	require.Equal(t, PointResponseStats{}, pointStats)

	// Merging a standalone ExecDetails value preserves its existing diagnostic
	// purpose and does not fabricate point-response coverage.
	snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions: 9,
	}})
	require.Equal(t, PointResponseStats{}, stats.GetPointResponseStats())
	require.Contains(t, stats.String(), "total_keys: 9")

	// Missing ExecDetailsV2 differs from a present, zero-valued ScanDetailV2.
	snapshot.mergePointResponse(nil, 0, true)
	snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{}, 7, true)
	snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{}}, 0, true)
	snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:            11,
		ProcessedVersions:        7,
		ProcessedVersionsSize:    70,
		RocksdbBlockReadByte:     99,
		IaRemoteReadSegmentBytes: 101,
	}}, 13, true)

	pointStats = stats.GetPointResponseStats()
	require.True(t, pointStats.IsValid())
	require.False(t, pointStats.ScanDetailComplete())
	require.True(t, pointStats.PayloadComplete())
	require.Equal(t, PointReadScanDetail{
		TotalKeys:         11,
		ProcessedKeys:     7,
		ProcessedKeysSize: 70,
	}, pointStats.ScanDetail)
	require.Equal(t, uint64(20), pointStats.PayloadBytes)

	// The getter returns an independent value snapshot.
	pointStats.ScanDetail.TotalKeys = 1000
	require.Equal(t, int64(11), stats.GetPointResponseStats().ScanDetail.TotalKeys)
}

func TestSnapshotRuntimeStatsPointResponseCloneAndMerge(t *testing.T) {
	source := &SnapshotRuntimeStats{}
	sourceSnapshot := newSnapshotWithRuntimeStats(source)
	sourceSnapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:         5,
		ProcessedVersions:     3,
		ProcessedVersionsSize: 30,
	}}, 7, true)
	sourceSnapshot.mergePointResponse(nil, 0, true)

	clone := source.Clone()
	sourceSnapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:         7,
		ProcessedVersions:     4,
		ProcessedVersionsSize: 40,
	}}, 11, true)

	cloneStats := clone.GetPointResponseStats()
	require.Equal(t, PointReadScanDetail{
		TotalKeys:         5,
		ProcessedKeys:     3,
		ProcessedKeysSize: 30,
	}, cloneStats.ScanDetail)
	require.Equal(t, uint64(7), cloneStats.PayloadBytes)
	require.True(t, cloneStats.IsValid())
	require.False(t, cloneStats.ScanDetailComplete())
	require.True(t, cloneStats.PayloadComplete())

	target := &SnapshotRuntimeStats{}
	target.Merge(clone)
	target.Merge(source)
	targetStats := target.GetPointResponseStats()
	require.Equal(t, PointReadScanDetail{
		TotalKeys:         17,
		ProcessedKeys:     10,
		ProcessedKeysSize: 100,
	}, targetStats.ScanDetail)
	require.Equal(t, uint64(25), targetStats.PayloadBytes)
	require.True(t, targetStats.IsValid())
	require.False(t, targetStats.ScanDetailComplete())
	require.True(t, targetStats.PayloadComplete())

	// Merging with itself snapshots the source before acquiring the target lock.
	target.Merge(target)
	require.Equal(t, uint64(50), target.GetPointResponseStats().PayloadBytes)
	require.Equal(t, cloneStats, clone.GetPointResponseStats())
}

func TestSnapshotRuntimeStatsPointResponseInvalid(t *testing.T) {
	var nilStats *SnapshotRuntimeStats
	require.False(t, nilStats.GetPointResponseStats().IsValid())
	require.False(t, nilStats.GetPointResponseStats().ScanDetailComplete())
	require.False(t, nilStats.GetPointResponseStats().PayloadComplete())

	stats := &SnapshotRuntimeStats{}
	newSnapshotWithRuntimeStats(stats).mergePointResponse(nil, 0, false)
	require.False(t, stats.GetPointResponseStats().IsValid())
	require.False(t, stats.Clone().GetPointResponseStats().IsValid())
	merged := &SnapshotRuntimeStats{}
	merged.Merge(stats)
	require.False(t, merged.GetPointResponseStats().IsValid())
}

func TestCollectBatchGetResponseDataPointResponseStats(t *testing.T) {
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)
	collect := func(resp any) (*batchGetLockInfo, error) {
		return collectBatchGetResponseData(
			&tikvrpc.Response{Resp: resp},
			func([]byte, kv.ValueEntry) {},
			snapshot.mergePointResponse,
		)
	}

	// A nil recorder keeps the normal response parsing path but performs no
	// point-response accounting.
	_, err := collectBatchGetResponseData(
		&tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{}},
		func([]byte, kv.ValueEntry) {},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, PointResponseStats{}, stats.GetPointResponseStats())

	_, err = collectBatchGetResponseData(
		&tikvrpc.Response{}, func([]byte, kv.ValueEntry) {}, snapshot.mergePointResponse,
	)
	require.Error(t, err)

	_, err = collect(&kvrpcpb.BatchGetResponse{})
	require.NoError(t, err)
	_, err = collect(&kvrpcpb.BatchGetResponse{
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{}},
	})
	require.NoError(t, err)

	lockInfo, err := collect(&kvrpcpb.BatchGetResponse{
		Error: &kvrpcpb.KeyError{Locked: testLockInfo("k")},
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions: 2, ProcessedVersions: 1, ProcessedVersionsSize: 10,
		}},
	})
	require.NoError(t, err)
	require.Len(t, lockInfo.lockedKeys, 1)

	// The successful retry is a separate recognized response.
	_, err = collect(&kvrpcpb.BatchGetResponse{
		Pairs: []*kvrpcpb.KvPair{
			{Key: []byte("aa"), Value: []byte("bbb")},
			{Error: &kvrpcpb.KeyError{Locked: testLockInfo("locked")}},
		},
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions: 3, ProcessedVersions: 2, ProcessedVersionsSize: 20,
		}},
	})
	require.NoError(t, err)
	_, err = collect(&kvrpcpb.BufferBatchGetResponse{
		Pairs: []*kvrpcpb.KvPair{{Key: []byte("c"), Value: []byte("dd")}},
	})
	require.NoError(t, err)
	_, err = collect(&kvrpcpb.GetResponse{})
	require.Error(t, err)

	pointStats := stats.GetPointResponseStats()
	require.True(t, pointStats.IsValid())
	require.False(t, pointStats.ScanDetailComplete())
	require.True(t, pointStats.PayloadComplete())
	require.Equal(t, PointReadScanDetail{
		TotalKeys: 5, ProcessedKeys: 3, ProcessedKeysSize: 30,
	}, pointStats.ScanDetail)
	require.Equal(t, uint64(8), pointStats.PayloadBytes)
}

func testLockInfo(key string) *kvrpcpb.LockInfo {
	return &kvrpcpb.LockInfo{
		PrimaryLock: []byte(key),
		LockVersion: 1,
		Key:         []byte(key),
		LockTtl:     1,
		TxnSize:     1,
		LockType:    kvrpcpb.Op_Put,
	}
}

func TestSnapshotRuntimeStatsConcurrentPointResponseAccess(t *testing.T) {
	const (
		writers       = 8
		responsesEach = 100
		readers       = 4
		readsEach     = 200
	)
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)
	start := make(chan struct{})

	var readerWG sync.WaitGroup
	for range readers {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			<-start
			for range readsEach {
				stats.GetPointResponseStats()
				clone := stats.Clone()
				merged := &SnapshotRuntimeStats{}
				merged.Merge(clone)
				_ = merged.String()
			}
		}()
	}

	var writerWG sync.WaitGroup
	errCh := make(chan error, writers)
	for range writers {
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			<-start
			for i := range responsesEach {
				response := &kvrpcpb.BatchGetResponse{
					Pairs: []*kvrpcpb.KvPair{{Key: []byte("k"), Value: []byte("vv")}},
				}
				if i%2 == 0 {
					response.ExecDetailsV2 = &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
						TotalVersions: 1, ProcessedVersions: 2, ProcessedVersionsSize: 3,
					}}
				}
				_, err := collectBatchGetResponseData(
					&tikvrpc.Response{Resp: response},
					func([]byte, kv.ValueEntry) {},
					snapshot.mergePointResponse,
				)
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	close(start)
	writerWG.Wait()
	readerWG.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	pointStats := stats.GetPointResponseStats()
	require.Equal(t, PointReadScanDetail{
		TotalKeys:         writers * responsesEach / 2,
		ProcessedKeys:     writers * responsesEach,
		ProcessedKeysSize: writers * responsesEach * 3 / 2,
	}, pointStats.ScanDetail)
	require.Equal(t, uint64(writers*responsesEach*3), pointStats.PayloadBytes)
	require.True(t, pointStats.IsValid())
	require.False(t, pointStats.ScanDetailComplete())
	require.True(t, pointStats.PayloadComplete())
}
