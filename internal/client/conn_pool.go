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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/keepalive"
)

type connPool struct {
	// The target host.
	target string
	// version of the connection pool, increase by 1 when reconnect.
	ver uint64

	index uint32
	// streamTimeout binds with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease
	dialTimeout   time.Duration
	conns         []*monitoredConn
	// batchConn is not null when batch is enabled.
	*batchConn
	done chan struct{}

	monitor *connMonitor

	metrics struct {
		rpcLatHistByStore prometheus.ObserverVec
		rpcLatHistByType  *rpcMetrics
		rpcSrcLatSum      sync.Map
		rpcNetLatExternal prometheus.Observer
		rpcNetLatInternal prometheus.Observer
	}
}

func newConnPool(maxSize uint, addr string, ver uint64, security config.Security,
	idleNotify *uint32, enableBatch bool, dialTimeout time.Duration, m *connMonitor, eventListener *atomic.Pointer[ClientEventListener], opts []grpc.DialOption) (*connPool, error) {
	a := &connPool{
		ver:           ver,
		index:         0,
		conns:         make([]*monitoredConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),
		done:          make(chan struct{}),
		dialTimeout:   dialTimeout,
		monitor:       m,
	}
	a.metrics.rpcLatHistByStore = metrics.TiKVSendReqHistogramByStore.MustCurryWith(prometheus.Labels{metrics.LblStore: addr})
	a.metrics.rpcLatHistByType = deriveRPCMetrics(metrics.TiKVSendReqHistogramByType)
	a.metrics.rpcNetLatExternal = metrics.TiKVRPCNetLatencyHistogram.WithLabelValues(addr, "false")
	a.metrics.rpcNetLatInternal = metrics.TiKVRPCNetLatencyHistogram.WithLabelValues(addr, "true")
	if err := a.Init(addr, security, idleNotify, enableBatch, eventListener, opts...); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connPool) monitoredDial(ctx context.Context, connName, target string, opts ...grpc.DialOption) (conn *monitoredConn, err error) {
	conn = &monitoredConn{
		Name: connName,
	}
	conn.ClientConn, err = grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, err
	}
	a.monitor.AddConn(conn)
	return conn, nil
}

func (a *connPool) Init(addr string, security config.Security, idleNotify *uint32, enableBatch bool, eventListener *atomic.Pointer[ClientEventListener], opts ...grpc.DialOption) error {
	a.target = addr

	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err != nil {
			return errors.WithStack(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	cfg := config.GetGlobalConfig()
	var (
		unaryInterceptor  grpc.UnaryClientInterceptor
		streamInterceptor grpc.StreamClientInterceptor
	)
	if cfg.OpenTracingEnable {
		unaryInterceptor = grpc_opentracing.UnaryClientInterceptor()
		streamInterceptor = grpc_opentracing.StreamClientInterceptor()
	}

	allowBatch := (cfg.TiKVClient.MaxBatchSize > 0) && enableBatch
	if allowBatch {
		a.batchConn = newBatchConn(uint(len(a.conns)), cfg.TiKVClient.MaxBatchSize, idleNotify)
		a.batchConn.initMetrics(a.target)
	}
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	for i := range a.conns {
		ctx, cancel := context.WithTimeout(context.Background(), a.dialTimeout)
		var callOptions []grpc.CallOption
		callOptions = append(callOptions, grpc.MaxCallRecvMsgSize(MaxRecvMsgSize))
		if cfg.TiKVClient.GrpcCompressionType == gzip.Name {
			callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
		}

		opts = append([]grpc.DialOption{
			opt,
			grpc.WithInitialWindowSize(cfg.TiKVClient.GrpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(cfg.TiKVClient.GrpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithDefaultCallOptions(callOptions...),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond, // Default was 1s.
					Multiplier: 1.6,                    // Default
					Jitter:     0.2,                    // Default
					MaxDelay:   3 * time.Second,        // Default was 120s.
				},
				MinConnectTimeout: a.dialTimeout,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(keepAlive) * time.Second,
				Timeout: cfg.TiKVClient.GetGrpcKeepAliveTimeout(),
			}),
		}, opts...)
		if cfg.TiKVClient.GrpcSharedBufferPool {
			opts = append(opts, experimental.WithRecvBufferPool(grpc.NewSharedBufferPool()))
		}
		conn, err := a.monitoredDial(
			ctx,
			fmt.Sprintf("%s-%d", a.target, i),
			addr,
			opts...,
		)

		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.WithStack(err)
		}
		a.conns[i] = conn

		if allowBatch {
			batchClient := &batchCommandsClient{
				target:           a.target,
				conn:             conn.ClientConn,
				forwardedClients: make(map[string]*batchCommandsStream),
				batched:          sync.Map{},
				epoch:            0,
				closed:           0,
				tikvClientCfg:    cfg.TiKVClient,
				tikvLoad:         &a.tikvTransportLayerLoad,
				dialTimeout:      a.dialTimeout,
				tryLock:          tryLock{sync.NewCond(new(sync.Mutex)), false},
				eventListener:    eventListener,
				metrics:          &a.batchConn.metrics,
			}
			batchClient.maxConcurrencyRequestLimit.Store(cfg.TiKVClient.MaxConcurrencyRequestLimit)
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
		}
	}
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout, a.done)
	if allowBatch {
		go a.batchSendLoop(cfg.TiKVClient)
	}

	return nil
}

func (a *connPool) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.conns))
	return a.conns[next].ClientConn
}

func (a *connPool) Close() {
	if a.batchConn != nil {
		a.batchConn.Close()
	}

	for _, c := range a.conns {
		if c != nil {
			err := c.Close()
			tikverr.Log(err)
			if err == nil {
				a.monitor.RemoveConn(c)
			}
		}
	}

	close(a.done)
}

func (a *connPool) updateRPCMetrics(req *tikvrpc.Request, resp *tikvrpc.Response, latency time.Duration) {
	seconds := latency.Seconds()
	stale := req.GetStaleRead()
	source := req.GetRequestSource()
	internal := util.IsInternalRequest(req.GetRequestSource())

	a.metrics.rpcLatHistByType.get(req.Type, stale, internal).Observe(seconds)
	a.metrics.rpcLatHistByStore.With(prometheus.Labels{
		metrics.LblStaleRead: strconv.FormatBool(stale),
		metrics.LblScope:     strconv.FormatBool(internal)}).Observe(seconds)

	srcLatSum, ok := a.metrics.rpcSrcLatSum.Load(source)
	if !ok {
		srcLatSum = deriveRPCMetrics(metrics.TiKVSendReqSummary.MustCurryWith(
			prometheus.Labels{metrics.LblStore: a.target, metrics.LblSource: source}))
		a.metrics.rpcSrcLatSum.Store(source, srcLatSum)
	}
	srcLatSum.(*rpcMetrics).get(req.Type, stale, internal).Observe(seconds)

	if execDetail := resp.GetExecDetailsV2(); execDetail != nil {
		var totalRpcWallTimeNs uint64
		if execDetail.TimeDetailV2 != nil {
			totalRpcWallTimeNs = execDetail.TimeDetailV2.TotalRpcWallTimeNs
		} else if execDetail.TimeDetail != nil {
			totalRpcWallTimeNs = execDetail.TimeDetail.TotalRpcWallTimeNs
		}
		if totalRpcWallTimeNs > 0 {
			lat := latency - time.Duration(totalRpcWallTimeNs)
			if internal {
				a.metrics.rpcNetLatInternal.Observe(lat.Seconds())
			} else {
				a.metrics.rpcNetLatExternal.Observe(lat.Seconds())
			}
		}
	}
}
