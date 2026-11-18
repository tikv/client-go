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
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"
)

type connPool struct {
	// The target host.
	target string
	// version of the connection pool, increase by 1 when reconnect.
	ver uint64
	// keyspaceID is used to identify the keyspace of the request.
	keyspaceID apicodec.KeyspaceID

	index uint32
	// streamTimeout binds with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease
	dialTimeout   time.Duration
	conns         []*monitoredConn
	// batchConn is not null when batch is enabled.
	*batchConn
	done      chan struct{}
	closeOnce sync.Once
	// users includes admitted requests and non-batch streams that outlive them.
	users     atomic.Int64
	draining  atomic.Bool
	drained   chan struct{}
	drainOnce sync.Once

	monitor *connMonitor

	metrics atomic.Pointer[storeMetrics]
}

func newConnPool(ctx context.Context, maxSize uint, addr string, ver uint64, keyspaceID apicodec.KeyspaceID, security config.Security,
	idleNotify *uint32, enableBatch bool, dialTimeout time.Duration, m *connMonitor, eventListener *atomic.Pointer[ClientEventListener], opts []grpc.DialOption) (*connPool, error) {
	a := &connPool{
		ver:           ver,
		index:         0,
		keyspaceID:    keyspaceID,
		conns:         make([]*monitoredConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),
		done:          make(chan struct{}),
		drained:       make(chan struct{}),
		dialTimeout:   dialTimeout,
		monitor:       m,
	}
	if err := a.Init(ctx, addr, security, idleNotify, enableBatch, eventListener, opts...); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *connPool) monitoredDial(ctx context.Context, connName, target string, opts ...grpc.DialOption) (conn *monitoredConn, err error) {
	conn = &monitoredConn{
		Name: connName,
	}
	//nolint:staticcheck // SA1019: ignore deprecation warning
	conn.ClientConn, err = grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, err
	}
	a.monitor.AddConn(conn)
	return conn, nil
}

func (a *connPool) Init(ctx context.Context, addr string, security config.Security, idleNotify *uint32, enableBatch bool, eventListener *atomic.Pointer[ClientEventListener], opts ...grpc.DialOption) error {
	a.target = addr

	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	tlsConfig, err := security.ToKVServerTLSConfig()
	if err != nil {
		return errors.WithStack(err)
	}
	if tlsConfig != nil {
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
		a.initMetrics(a.target)
	}
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	for i := range a.conns {
		dialCtx, cancel := context.WithTimeout(ctx, a.dialTimeout)
		var callOptions []grpc.CallOption
		callOptions = append(callOptions, grpc.MaxCallRecvMsgSize(MaxRecvMsgSize))
		if cfg.TiKVClient.GrpcCompressionType == gzip.Name {
			callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
		}

		// Don't remove this until we no longer use sharedBytes in tipb and kvproto.
		callOptions = append(callOptions, grpc.ForceCodec(&legacyCodec{}))

		dialOpts := append([]grpc.DialOption{
			opt,
			grpc.WithInitialWindowSize(cfg.TiKVClient.GrpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(cfg.TiKVClient.GrpcInitialConnWindowSize),
			grpc.WithUnaryInterceptor(unaryInterceptor),
			grpc.WithStreamInterceptor(streamInterceptor),
			grpc.WithChainStreamInterceptor(a.trackStream),
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
		if !cfg.TiKVClient.GrpcSharedBufferPool {
			dialOpts = append(dialOpts, experimental.WithBufferPool(mem.NopBufferPool{}))
		}
		conn, err := a.monitoredDial(
			dialCtx,
			fmt.Sprintf("%s-%d", a.target, i),
			addr,
			dialOpts...,
		)

		cancel()
		if err != nil {
			// Cleanup if the initialization fails.
			a.Close()
			return errors.WithStack(err)
		}
		a.conns[i] = conn

		if allowBatch {
			connIdx := strconv.Itoa(i)
			batchClient := &batchCommandsClient{
				target:           a.target,
				connIdx:          connIdx,
				keyspaceID:       a.keyspaceID,
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
				metrics:          initBatchCommandsClientMetrics(a.target, connIdx),
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
	a.closeOnce.Do(a.close)
}

func (a *connPool) close() {
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

func (a *connPool) release() {
	if a.users.Add(-1) == 0 && a.draining.Load() {
		a.drainOnce.Do(func() { close(a.drained) })
	}
}

// retire is called after removing the pool from admission, under RPCClient's lock.
func (a *connPool) retire() {
	a.draining.Store(true)
	if a.users.Load() == 0 {
		a.drainOnce.Do(func() { close(a.drained) })
	}
}

func (a *connPool) trackStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// BatchCommands is a persistent transport; its logical requests are counted
	// at admission. Counting the stream itself would prevent it ever draining.
	if method == "/tikvpb.Tikv/BatchCommands" {
		return streamer(ctx, desc, cc, method, opts...)
	}
	a.users.Add(1)
	release := sync.OnceFunc(a.release)
	opts = append(opts, grpc.OnFinish(func(error) { release() }))
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		release()
	}
	return stream, err
}

func (a *connPool) getStoreMetrics(storeID uint64) *storeMetrics {
	m := a.metrics.Load()
	if m == nil || m.storeID != storeID {
		// The client selects a connPool by addr via RPCClient.getConnPool, so it's possible that the storeID of the
		// selected connPool is not the same as the storeID in req.Context. We need to create a new storeMetrics for the
		// new storeID. Note that connPool.metrics just works as a cache, the metric data is stored in corresponding
		// MetricVec, so it's ok to overwrite it here.
		m = newStoreMetrics(storeID)
		a.metrics.Store(m)
	}
	return m
}
