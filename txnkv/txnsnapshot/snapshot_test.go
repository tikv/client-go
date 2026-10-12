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
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

func newSnapshotWithRuntimeStats(stats *SnapshotRuntimeStats) *KVSnapshot {
	snapshot := &KVSnapshot{}
	snapshot.SetRuntimeStats(stats)
	return snapshot
}

func TestSnapshotRuntimeStatsGetScanDetail(t *testing.T) {
	var nilStats *SnapshotRuntimeStats
	require.Nil(t, nilStats.GetScanDetail())

	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)
	require.Equal(t, &util.ScanDetail{}, stats.GetScanDetail())

	detail := &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:             11,
		ProcessedVersions:         7,
		ProcessedVersionsSize:     70,
		RocksdbDeleteSkippedCount: 2,
		RocksdbKeySkippedCount:    3,
		RocksdbBlockCacheHitCount: 5,
		RocksdbBlockReadCount:     6,
		RocksdbBlockReadByte:      99,
		RocksdbBlockReadNanos:     13,
		GetSnapshotNanos:          17,
		IaCacheHitCount:           19,
		IaRemoteReadSegmentCount:  23,
		IaRemoteReadSegmentBytes:  101,
		IaRemoteReadSegmentNanos:  29,
	}}
	snapshot.mergePointResponse(detail, 0)
	snapshot.mergePointResponse(nil, 0)
	snapshot.mergePointResponse(detail, 0)
	want := &util.ScanDetail{
		TotalKeys:                   22,
		ProcessedKeys:               14,
		ProcessedKeysSize:           140,
		RocksdbDeleteSkippedCount:   4,
		RocksdbKeySkippedCount:      6,
		RocksdbBlockCacheHitCount:   10,
		RocksdbBlockReadCount:       12,
		RocksdbBlockReadByte:        198,
		RocksdbBlockReadDuration:    26 * time.Nanosecond,
		GetSnapshotDuration:         34 * time.Nanosecond,
		IaCacheHitCount:             38,
		IaRemoteReadSegmentCount:    46,
		IaRemoteReadSegmentBytes:    202,
		IaRemoteReadSegmentDuration: 58 * time.Nanosecond,
	}
	require.Equal(t, want, stats.GetScanDetail())

	// Modifying the returned detail must not alter the runtime statistics.
	copy := stats.GetScanDetail()
	*copy = util.ScanDetail{}
	require.Equal(t, want, stats.GetScanDetail())

	// Subsequent responses must not change an earlier copy or a cloned stats instance.
	copy = stats.GetScanDetail()
	clone := stats.Clone()
	snapshot.mergePointResponse(detail, 0)
	require.Equal(t, want, copy)
	require.Equal(t, want, clone.GetScanDetail())
	require.Equal(t, uint64(69), stats.GetScanDetail().IaRemoteReadSegmentCount)

	merged := &SnapshotRuntimeStats{}
	merged.Merge(clone)
	merged.Merge(clone)
	require.Equal(t, uint64(92), merged.GetScanDetail().IaRemoteReadSegmentCount)
	require.Equal(t, uint64(404), merged.GetScanDetail().IaRemoteReadSegmentBytes)
	require.Equal(t, 116*time.Nanosecond, merged.GetScanDetail().IaRemoteReadSegmentDuration)
	require.Equal(t, want, clone.GetScanDetail())
}

func TestSnapshotRuntimeStatsPointResponseStats(t *testing.T) {
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)

	pointStats := stats.GetPointResponseStats()
	require.True(t, pointStats.IsValid())
	require.False(t, pointStats.ScanDetailComplete())
	require.False(t, pointStats.PayloadComplete())
	require.Equal(t, PointResponseStats{}, pointStats)

	// Missing ExecDetailsV2 differs from a present, zero-valued ScanDetailV2.
	snapshot.mergePointResponse(nil, 0)
	snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{}, 7)
	snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{}}, 0)
	snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:            11,
		ProcessedVersions:        7,
		ProcessedVersionsSize:    70,
		RocksdbBlockReadByte:     99,
		IaRemoteReadSegmentBytes: 101,
	}}, 13)

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

func TestSnapshotRuntimeStatsStandaloneScanDetailDoesNotEstablishPointCoverage(t *testing.T) {
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)

	// Standalone execution details remain visible through the legacy diagnostic
	// scan detail, but do not establish point-response coverage.
	snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions: 9,
	}})
	pointStats := stats.GetPointResponseStats()
	require.True(t, pointStats.IsValid())
	require.False(t, pointStats.ScanDetailComplete())
	require.False(t, pointStats.PayloadComplete())
	require.Equal(t, PointResponseStats{}, pointStats)
	require.Contains(t, stats.String(), "total_keys: 9")
}

func TestSnapshotRuntimeStatsPointResponseCloneAndMerge(t *testing.T) {
	source := &SnapshotRuntimeStats{}
	sourceSnapshot := newSnapshotWithRuntimeStats(source)
	sourceSnapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:         5,
		ProcessedVersions:     3,
		ProcessedVersionsSize: 30,
	}}, 7)
	sourceSnapshot.mergePointResponse(nil, 0)

	clone := source.Clone()
	sourceSnapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:         7,
		ProcessedVersions:     4,
		ProcessedVersionsSize: 40,
	}}, 11)

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

	// Self-merge doubles the accumulated values without changing the source clone.
	target.Merge(target)
	targetStats = target.GetPointResponseStats()
	require.Equal(t, int64(34), targetStats.ScanDetail.TotalKeys)
	require.Equal(t, uint64(50), targetStats.PayloadBytes)
	require.Equal(t, cloneStats, clone.GetPointResponseStats())
}

func TestSnapshotRuntimeStatsPointResponseInvalid(t *testing.T) {
	var nilStats *SnapshotRuntimeStats
	require.False(t, nilStats.GetPointResponseStats().IsValid())
	require.False(t, nilStats.GetPointResponseStats().ScanDetailComplete())
	require.False(t, nilStats.GetPointResponseStats().PayloadComplete())
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

func TestSnapshotRuntimeStatsConcurrentPointResponseWrites(t *testing.T) {
	const (
		writers       = 8
		responsesEach = 100
	)
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)
	start := make(chan struct{})

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
