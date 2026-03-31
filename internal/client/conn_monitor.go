// Copyright 2025 TiKV Authors
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/stats"
)

type monitoredConn struct {
	*grpc.ClientConn
	Name string
}

func (c *monitoredConn) Close() error {
	if c.ClientConn != nil {
		err := c.ClientConn.Close()
		logutil.BgLogger().Debug("close gRPC connection", zap.String("target", c.Name), zap.Error(err))
		return err
	}
	return nil
}

type connMonitor struct {
	m        sync.Map
	loopOnce sync.Once
	stopOnce sync.Once
	stop     chan struct{}
}

func (c *connMonitor) AddConn(conn *monitoredConn) {
	c.m.Store(conn.Name, conn)
}

func (c *connMonitor) RemoveConn(conn *monitoredConn) {
	c.m.Delete(conn.Name)
	for state := connectivity.Idle; state <= connectivity.Shutdown; state++ {
		metrics.TiKVGrpcConnectionState.WithLabelValues(conn.Name, conn.Target(), state.String()).Set(0)
	}
}

func (c *connMonitor) Start() {
	c.loopOnce.Do(
		func() {
			c.stop = make(chan struct{})
			go c.start()
		},
	)
}

func (c *connMonitor) Stop() {
	c.stopOnce.Do(
		func() {
			if c.stop != nil {
				close(c.stop)
			}
		},
	)
}

func (c *connMonitor) start() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.m.Range(func(_, value interface{}) bool {
				conn := value.(*monitoredConn)
				nowState := conn.GetState()
				for state := connectivity.Idle; state <= connectivity.Shutdown; state++ {
					if state == nowState {
						metrics.TiKVGrpcConnectionState.WithLabelValues(conn.Name, conn.Target(), nowState.String()).Set(1)
					} else {
						metrics.TiKVGrpcConnectionState.WithLabelValues(conn.Name, conn.Target(), state.String()).Set(0)
					}
				}
				return true
			})
		case <-c.stop:
			return
		}
	}
}

const batchCommandsMethod = "/tikvpb.Tikv/BatchCommands"

type batchClientStreamKey struct{}

var globalBatchClientStreamID uint64

type batchClientStreamMetrics struct {
	inited sync.Once

	sendTime prometheus.Gauge
	recvTime prometheus.Gauge
}

func (m *batchClientStreamMetrics) init(addr string, id uint64) {
	m.inited.Do(func() {
		streamID := strconv.FormatUint(id, 10)
		m.sendTime = metrics.TiKVBatchCommandsSendTime.WithLabelValues(addr, streamID)
		m.recvTime = metrics.TiKVBatchCommandsRecvTime.WithLabelValues(addr, streamID)
	})
}

func (m *batchClientStreamMetrics) onOutPayload(ev *stats.OutPayload) {
	m.sendTime.Set(float64(ev.SentTime.UnixNano()) / float64(time.Second))
}

func (m *batchClientStreamMetrics) onInPayload(ev *stats.InPayload) {
	m.recvTime.Set(float64(ev.RecvTime.UnixNano()) / float64(time.Second))
}

func (m *batchClientStreamMetrics) cleanUp(addr string, id uint64) {
	streamID := strconv.FormatUint(id, 10)
	metrics.TiKVBatchCommandsSendTime.DeleteLabelValues(addr, streamID)
	metrics.TiKVBatchCommandsRecvTime.DeleteLabelValues(addr, streamID)
}

type batchClientTargetMetrics struct {
	inited sync.Once

	sendWireBytes    prometheus.Counter
	recvWireBytes    prometheus.Counter
	delayedPick      prometheus.Counter
	transparentRetry prometheus.Counter
}

func (m *batchClientTargetMetrics) init(addr string) {
	m.inited.Do(func() {
		m.sendWireBytes = metrics.TiKVBatchCommandsSendWireBytes.WithLabelValues(addr)
		m.recvWireBytes = metrics.TiKVBatchCommandsRecvWireBytes.WithLabelValues(addr)
		m.delayedPick = metrics.TiKVBatchCommandsDelayedPick.WithLabelValues(addr)
		m.transparentRetry = metrics.TiKVBatchCommandsTransparentRetry.WithLabelValues(addr)
	})
}

func (m *batchClientTargetMetrics) onOutPayload(ev *stats.OutPayload) {
	m.sendWireBytes.Add(float64(ev.WireLength))
}

func (m *batchClientTargetMetrics) onInPayload(ev *stats.InPayload) {
	m.recvWireBytes.Add(float64(ev.WireLength))
}

func (m *batchClientTargetMetrics) onDelayedPickComplete(*stats.DelayedPickComplete) {
	m.delayedPick.Inc()
}

func (m *batchClientTargetMetrics) onBegin(ev *stats.Begin) {
	if ev.IsTransparentRetryAttempt {
		m.transparentRetry.Inc()
	}
}

type batchClientStatsMonitor struct {
	addr          string
	targetMetrics batchClientTargetMetrics
	streamMetrics sync.Map
}

func (m *batchClientStatsMonitor) getStreamMetrics(id uint64) *batchClientStreamMetrics {
	mm, _ := m.streamMetrics.LoadOrStore(id, &batchClientStreamMetrics{})
	return mm.(*batchClientStreamMetrics)
}

// TagConn implements [stats.Handler].
func (m *batchClientStatsMonitor) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn implements [stats.Handler].
func (m *batchClientStatsMonitor) HandleConn(context.Context, stats.ConnStats) {
}

// TagRPC implements [stats.Handler].
func (m *batchClientStatsMonitor) TagRPC(ctx context.Context, i *stats.RPCTagInfo) context.Context {
	if i.FullMethodName != batchCommandsMethod {
		return ctx
	}
	return context.WithValue(ctx, batchClientStreamKey{}, atomic.AddUint64(&globalBatchClientStreamID, 1))
}

// HandleRPC implements [stats.Handler].
func (m *batchClientStatsMonitor) HandleRPC(ctx context.Context, s stats.RPCStats) {
	id, ok := ctx.Value(batchClientStreamKey{}).(uint64)
	if !ok {
		return
	}
	switch ev := s.(type) {
	case *stats.OutPayload:
		m.targetMetrics.init(m.addr)
		m.targetMetrics.onOutPayload(ev)

		streamMetrics := m.getStreamMetrics(id)
		streamMetrics.init(m.addr, id)
		streamMetrics.onOutPayload(ev)

	case *stats.InPayload:
		m.targetMetrics.init(m.addr)
		m.targetMetrics.onInPayload(ev)

		streamMetrics := m.getStreamMetrics(id)
		streamMetrics.init(m.addr, id)
		streamMetrics.onInPayload(ev)

	case *stats.DelayedPickComplete:
		m.targetMetrics.init(m.addr)
		m.targetMetrics.onDelayedPickComplete(ev)

	case *stats.Begin:
		m.targetMetrics.init(m.addr)
		m.targetMetrics.onBegin(ev)

	case *stats.End:
		mm, ok := m.streamMetrics.LoadAndDelete(id)
		if !ok {
			return
		}
		mm.(*batchClientStreamMetrics).cleanUp(m.addr, id)
	}
}
