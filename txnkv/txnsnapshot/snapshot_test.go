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
	"github.com/tikv/client-go/v2/util"
)

func newSnapshotWithRuntimeStats(stats *SnapshotRuntimeStats) *KVSnapshot {
	snapshot := &KVSnapshot{}
	snapshot.SetRuntimeStats(stats)
	return snapshot
}

func TestSnapshotRuntimeStatsPointResponseStats(t *testing.T) {
	t.Run("aggregate projection", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			scanDetail util.ScanDetail
			extra      pointReadResponseExtraStats
		}{
			{
				name:       "no response with nonzero scan detail",
				scanDetail: util.ScanDetail{TotalKeys: 5, ProcessedKeys: 3, ProcessedKeysSize: 30},
			},
			{
				name:  "complete zero response",
				extra: pointReadResponseExtraStats{seenResponse: true},
			},
			{
				name:       "complete aggregate",
				scanDetail: util.ScanDetail{TotalKeys: 5, ProcessedKeys: 3, ProcessedKeysSize: 30},
				extra:      pointReadResponseExtraStats{payloadBytes: 7, seenResponse: true},
			},
			{
				name:       "missing scan detail aggregate",
				scanDetail: util.ScanDetail{TotalKeys: 5, ProcessedKeys: 3, ProcessedKeysSize: 30},
				extra:      pointReadResponseExtraStats{payloadBytes: 7, seenResponse: true, missingScanDetail: true},
			},
			{
				name:       "negative counters and limits",
				scanDetail: util.ScanDetail{TotalKeys: -1, ProcessedKeys: -1 << 63, ProcessedKeysSize: 1<<63 - 1},
				extra:      pointReadResponseExtraStats{payloadBytes: ^uint64(0), seenResponse: true},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				stats := &SnapshotRuntimeStats{scanDetail: tc.scanDetail, pointResponseExtra: tc.extra}
				expected := getPointResponseStatsBeforeProjection(stats)
				actual := stats.GetPointResponseStats()
				require.Equal(t, expected, actual)
				if !tc.extra.seenResponse {
					require.Equal(t, PointResponseStats{}, actual)
				}
				require.Equal(t, tc.scanDetail, stats.scanDetail)
				require.Equal(t, tc.extra, stats.pointResponseExtra)
				actual.Invalidate()
				actual.ScanDetail.TotalKeys++
				require.Equal(t, expected, stats.GetPointResponseStats(), "the projected value is independent")
			})
		}
	})

	t.Run("payload sum overflow", func(t *testing.T) {
		stats := &SnapshotRuntimeStats{}
		snapshot := newSnapshotWithRuntimeStats(stats)
		snapshot.mergePointResponse(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions: 5, ProcessedVersions: 3, ProcessedVersionsSize: 30,
		}}, ^uint64(0)-1)
		snapshot.mergePointResponse(nil, 5)
		actual := stats.GetPointResponseStats()
		require.Equal(t, getPointResponseStatsBeforeProjection(stats), actual)
		require.Equal(t, uint64(3), actual.PayloadBytes)
		require.True(t, actual.PayloadComplete())
		require.False(t, actual.ScanDetailComplete())
	})

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

func TestSnapshotRuntimeStatsGetResponse(t *testing.T) {
	detail := &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions: 2, ProcessedVersions: 1, ProcessedVersionsSize: 5,
	}}
	for _, mode := range []struct {
		name    string
		enabled bool
	}{{name: "enabled", enabled: true}, {name: "disabled"}} {
		t.Run(mode.name, func(t *testing.T) {
			for _, tc := range []struct {
				name         string
				response     *kvrpcpb.GetResponse
				payloadBytes uint64
			}{
				{name: "value", response: &kvrpcpb.GetResponse{Value: []byte("value"), ExecDetailsV2: detail}, payloadBytes: 5},
				{name: "not found", response: &kvrpcpb.GetResponse{ExecDetailsV2: detail}},
				{name: "response error", response: &kvrpcpb.GetResponse{
					Value: []byte("ignored"), Error: &kvrpcpb.KeyError{Abort: "error"}, ExecDetailsV2: detail,
				}},
				{name: "missing execution detail", response: &kvrpcpb.GetResponse{Value: []byte("value")}, payloadBytes: 5},
				{name: "missing scan detail", response: &kvrpcpb.GetResponse{ExecDetailsV2: &kvrpcpb.ExecDetailsV2{}}},
				{name: "zero scan detail", response: &kvrpcpb.GetResponse{ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
					ScanDetailV2: &kvrpcpb.ScanDetailV2{},
				}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var stats *SnapshotRuntimeStats
					if mode.enabled {
						stats = &SnapshotRuntimeStats{}
					}
					snapshot := newSnapshotWithRuntimeStats(stats)
					snapshot.mergeGetResponse(tc.response)
					if !mode.enabled {
						require.Nil(t, snapshot.mu.stats)
						return
					}
					var expected PointResponseStats
					expected.RecordResponse(tc.response.ExecDetailsV2.GetScanDetailV2(), tc.payloadBytes)
					require.Equal(t, expected, stats.GetPointResponseStats())
				})
			}
		})
	}
}

func BenchmarkSnapshotGetResponseRecording(b *testing.B) {
	response := &kvrpcpb.GetResponse{
		Value: make([]byte, 128),
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
			ScanDetailV2: &kvrpcpb.ScanDetailV2{TotalVersions: 1, ProcessedVersions: 1, ProcessedVersionsSize: 128},
		},
	}
	for _, mode := range []struct {
		name    string
		enabled bool
	}{{name: "enabled", enabled: true}, {name: "disabled"}} {
		b.Run(mode.name, func(b *testing.B) {
			var stats *SnapshotRuntimeStats
			if mode.enabled {
				stats = &SnapshotRuntimeStats{}
			}
			snapshot := newSnapshotWithRuntimeStats(stats)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				snapshot.mergeGetResponse(response)
			}
			b.StopTimer()
			if mode.enabled {
				pointStats := stats.GetPointResponseStats()
				require.Equal(b, uint64(b.N)*128, pointStats.PayloadBytes)
				require.Equal(b, int64(b.N), pointStats.ScanDetail.TotalKeys)
			}
		})
	}
}

// Keep the old getter and projection separate, as in the production baseline,
// so the benchmark compares the same entry point without indirect function calls.
func getPointResponseStatsBeforeProjection(rs *SnapshotRuntimeStats) PointResponseStats {
	if rs == nil {
		var stats PointResponseStats
		stats.Invalidate()
		return stats
	}
	return buildPointResponseStatsBeforeProjection(rs.pointResponseExtra, &rs.scanDetail)
}

func buildPointResponseStatsBeforeProjection(s pointReadResponseExtraStats, scanDetail *util.ScanDetail) PointResponseStats {
	if !s.seenResponse {
		return PointResponseStats{}
	}
	stats := PointResponseStats{
		ScanDetail: PointReadScanDetail{
			TotalKeys:         scanDetail.TotalKeys,
			ProcessedKeys:     scanDetail.ProcessedKeys,
			ProcessedKeysSize: scanDetail.ProcessedKeysSize,
		},
		PayloadBytes: s.payloadBytes,
	}
	coverageScanDetail := &kvrpcpb.ScanDetailV2{}
	if s.missingScanDetail {
		coverageScanDetail = nil
	}
	stats.RecordResponse(coverageScanDetail, 0)
	return stats
}

var benchmarkPointResponseStatsSink PointResponseStats

func BenchmarkSnapshotPointResponseStatsProjection(b *testing.B) {
	complete := &SnapshotRuntimeStats{}
	newSnapshotWithRuntimeStats(complete).mergePointResponse(&kvrpcpb.ExecDetailsV2{
		ScanDetailV2: &kvrpcpb.ScanDetailV2{TotalVersions: 11, ProcessedVersions: 7, ProcessedVersionsSize: 70},
	}, 128)
	missing := complete.Clone()
	newSnapshotWithRuntimeStats(missing).mergePointResponse(nil, 17)
	for _, tc := range []struct {
		name  string
		stats *SnapshotRuntimeStats
	}{
		{name: "complete", stats: complete},
		{name: "missing", stats: missing},
	} {
		b.Run(tc.name, func(b *testing.B) {
			expected := getPointResponseStatsBeforeProjection(tc.stats)
			b.Run("before", func(b *testing.B) {
				var result PointResponseStats
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					result = getPointResponseStatsBeforeProjection(tc.stats)
				}
				b.StopTimer()
				benchmarkPointResponseStatsSink = result
				require.Equal(b, expected, result)
			})
			b.Run("aggregate", func(b *testing.B) {
				var result PointResponseStats
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					result = tc.stats.GetPointResponseStats()
				}
				b.StopTimer()
				benchmarkPointResponseStatsSink = result
				require.Equal(b, expected, result)
			})
		})
	}
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
	require.Equal(t, getPointResponseStatsBeforeProjection(nilStats), nilStats.GetPointResponseStats())
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
