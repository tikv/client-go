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

package client

import (
	"context"
	"strconv"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/metrics"
	"google.golang.org/grpc/stats"
)

func resetBatchCommandsStatsMetrics() {
	metrics.TiKVBatchCommandsSendTime.Reset()
	metrics.TiKVBatchCommandsRecvTime.Reset()
	metrics.TiKVBatchCommandsSendWireBytes.Reset()
	metrics.TiKVBatchCommandsRecvWireBytes.Reset()
	metrics.TiKVBatchCommandsDelayedPick.Reset()
	metrics.TiKVBatchCommandsTransparentRetry.Reset()
}

func readGauge(t *testing.T, metric interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()

	pb := &dto.Metric{}
	require.NoError(t, metric.Write(pb))
	return pb.GetGauge().GetValue()
}

func readCounter(t *testing.T, metric interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()

	pb := &dto.Metric{}
	require.NoError(t, metric.Write(pb))
	return pb.GetCounter().GetValue()
}

func TestBatchClientStatsMonitorTagRPCUsesGlobalStreamID(t *testing.T) {
	resetBatchCommandsStatsMetrics()
	t.Cleanup(resetBatchCommandsStatsMetrics)

	ctx1 := (&batchClientStatsMonitor{addr: "tikv-1"}).TagRPC(context.Background(), &stats.RPCTagInfo{
		FullMethodName: batchCommandsMethod,
	})
	ctx2 := (&batchClientStatsMonitor{addr: "tikv-1"}).TagRPC(context.Background(), &stats.RPCTagInfo{
		FullMethodName: batchCommandsMethod,
	})

	id1, ok1 := ctx1.Value(batchClientStreamKey{}).(uint64)
	id2, ok2 := ctx2.Value(batchClientStreamKey{}).(uint64)
	require.True(t, ok1)
	require.True(t, ok2)
	require.NotZero(t, id1)
	require.NotZero(t, id2)
	require.NotEqual(t, id1, id2)
}

func TestBatchClientStatsMonitorHandleRPC(t *testing.T) {
	resetBatchCommandsStatsMetrics()
	t.Cleanup(resetBatchCommandsStatsMetrics)

	addr := "tikv-1"
	monitor := &batchClientStatsMonitor{addr: addr}
	ctx := monitor.TagRPC(context.Background(), &stats.RPCTagInfo{
		FullMethodName: batchCommandsMethod,
	})
	id, ok := ctx.Value(batchClientStreamKey{}).(uint64)
	require.True(t, ok)

	monitor.HandleRPC(ctx, &stats.DelayedPickComplete{})
	monitor.HandleRPC(ctx, &stats.Begin{IsTransparentRetryAttempt: true})

	sentTime := time.Unix(1710000000, 500000000)
	recvTime := time.Unix(1710000001, 250000000)
	monitor.HandleRPC(ctx, &stats.OutPayload{
		SentTime:   sentTime,
		WireLength: 11,
	})
	monitor.HandleRPC(ctx, &stats.InPayload{
		RecvTime:   recvTime,
		WireLength: 13,
	})

	streamID := strconv.FormatUint(id, 10)
	require.Equal(t,
		float64(sentTime.UnixNano())/float64(time.Second),
		readGauge(t, metrics.TiKVBatchCommandsSendTime.WithLabelValues(addr, streamID)),
	)
	require.Equal(t,
		float64(recvTime.UnixNano())/float64(time.Second),
		readGauge(t, metrics.TiKVBatchCommandsRecvTime.WithLabelValues(addr, streamID)),
	)
	require.Equal(t, 11.0, readCounter(t, metrics.TiKVBatchCommandsSendWireBytes.WithLabelValues(addr)))
	require.Equal(t, 13.0, readCounter(t, metrics.TiKVBatchCommandsRecvWireBytes.WithLabelValues(addr)))
	require.Equal(t, 1.0, readCounter(t, metrics.TiKVBatchCommandsDelayedPick.WithLabelValues(addr)))
	require.Equal(t, 1.0, readCounter(t, metrics.TiKVBatchCommandsTransparentRetry.WithLabelValues(addr)))

	monitor.HandleRPC(ctx, &stats.End{})
	require.False(t, metrics.TiKVBatchCommandsSendTime.DeleteLabelValues(addr, streamID))
	require.False(t, metrics.TiKVBatchCommandsRecvTime.DeleteLabelValues(addr, streamID))
	require.Equal(t, 11.0, readCounter(t, metrics.TiKVBatchCommandsSendWireBytes.WithLabelValues(addr)))
	require.Equal(t, 13.0, readCounter(t, metrics.TiKVBatchCommandsRecvWireBytes.WithLabelValues(addr)))
}
