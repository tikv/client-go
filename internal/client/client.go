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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/client.go
//

// Copyright 2016 PingCAP, Inc.
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

// Package client provides tcp connection to kvserver.
package client

import (
	"context"
	"fmt"
	"io"
	"math"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// MaxRecvMsgSize set max gRPC receive message size received from server. If any message size is larger than
// current value, an error will be reported from gRPC.
var MaxRecvMsgSize = math.MaxInt64 - 1

// Timeout durations.
const (
	dialTimeout       = 5 * time.Second
	ReadTimeoutShort  = 30 * time.Second // For requests that read/write several key-values.
	ReadTimeoutMedium = 60 * time.Second // For requests that may need scan region.

	// MaxWriteExecutionTime is the MaxExecutionDurationMs field for write requests.
	// Because the last deadline check is before proposing, let us give it 10 more seconds
	// after proposing.
	MaxWriteExecutionTime = ReadTimeoutShort - 10*time.Second
)

// Grpc window size
const (
	GrpcInitialWindowSize     = 1 << 30
	GrpcInitialConnWindowSize = 1 << 30
)

// forwardMetadataKey is the key of gRPC metadata which represents a forwarded request.
const forwardMetadataKey = "tikv-forwarded-host"

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client interface {
	// Close should release all data.
	Close() error
	// CloseAddr closes gRPC connections to the address. It will reconnect the next time it's used.
	CloseAddr(addr string) error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

type connArray struct {
	// The target host.
	target string

	index uint32
	v     []*monitoredConn
	// streamTimeout binds with a background goroutine to process coprocessor streaming timeout.
	streamTimeout chan *tikvrpc.Lease
	dialTimeout   time.Duration
	// batchConn is not null when batch is enabled.
	*batchConn
	done chan struct{}

	monitor *connMonitor
}

func newConnArray(maxSize uint, addr string, security config.Security,
	idleNotify *uint32, enableBatch bool, dialTimeout time.Duration, m *connMonitor, opts []grpc.DialOption) (*connArray, error) {
	a := &connArray{
		index:         0,
		v:             make([]*monitoredConn, maxSize),
		streamTimeout: make(chan *tikvrpc.Lease, 1024),
		done:          make(chan struct{}),
		dialTimeout:   dialTimeout,
		monitor:       m,
	}
	if err := a.Init(addr, security, idleNotify, enableBatch, opts...); err != nil {
		return nil, err
	}
	return a, nil
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

type monitoredConn struct {
	*grpc.ClientConn
	Name string
}

func (a *connArray) monitoredDial(ctx context.Context, connName, target string, opts ...grpc.DialOption) (conn *monitoredConn, err error) {
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

func (c *monitoredConn) Close() error {
	if c.ClientConn != nil {
		return c.ClientConn.Close()
	}
	return nil
}

func (a *connArray) Init(addr string, security config.Security, idleNotify *uint32, enableBatch bool, opts ...grpc.DialOption) error {
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
		a.batchConn = newBatchConn(uint(len(a.v)), cfg.TiKVClient.MaxBatchSize, idleNotify)
		a.pendingRequests = metrics.TiKVBatchPendingRequests.WithLabelValues(a.target)
		a.batchSize = metrics.TiKVBatchRequests.WithLabelValues(a.target)
	}
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout
	for i := range a.v {
		ctx, cancel := context.WithTimeout(context.Background(), a.dialTimeout)
		var callOptions []grpc.CallOption
		callOptions = append(callOptions, grpc.MaxCallRecvMsgSize(MaxRecvMsgSize))
		if cfg.TiKVClient.GrpcCompressionType == gzip.Name {
			callOptions = append(callOptions, grpc.UseCompressor(gzip.Name))
		}

		opts = append([]grpc.DialOption{
			opt,
			grpc.WithInitialWindowSize(GrpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(GrpcInitialConnWindowSize),
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
				Timeout: time.Duration(keepAliveTimeout) * time.Second,
			}),
		}, opts...)

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
		a.v[i] = conn

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
			}
			a.batchCommandsClients = append(a.batchCommandsClients, batchClient)
		}
	}
	go tikvrpc.CheckStreamTimeoutLoop(a.streamTimeout, a.done)
	if allowBatch {
		go a.batchSendLoop(cfg.TiKVClient)
	}

	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next].ClientConn
}

func (a *connArray) Close() {
	if a.batchConn != nil {
		a.batchConn.Close()
	}

	for _, c := range a.v {
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

type option struct {
	gRPCDialOptions []grpc.DialOption
	security        config.Security
	dialTimeout     time.Duration
	codec           apicodec.Codec
}

// Opt is the option for the client.
type Opt func(*option)

// WithSecurity is used to set the security config.
func WithSecurity(security config.Security) Opt {
	return func(c *option) {
		c.security = security
	}
}

// WithGRPCDialOptions is used to set the grpc.DialOption.
func WithGRPCDialOptions(grpcDialOptions ...grpc.DialOption) Opt {
	return func(c *option) {
		c.gRPCDialOptions = grpcDialOptions
	}
}

// WithCodec is used to set RPCClient's codec.
func WithCodec(codec apicodec.Codec) Opt {
	return func(c *option) {
		c.codec = codec
	}
}

// RPCClient is RPC client struct.
// TODO: Add flow control between RPC clients in TiDB ond RPC servers in TiKV.
// Since we use shared client connection to communicate to the same TiKV, it's possible
// that there are too many concurrent requests which overload the service of TiKV.
type RPCClient struct {
	sync.RWMutex

	conns  map[string]*connArray
	option *option

	idleNotify uint32

	// Periodically check whether there is any connection that is idle and then close and remove these connections.
	// Implement background cleanup.
	isClosed bool

	connMonitor *connMonitor
}

// NewRPCClient creates a client that manages connections and rpc calls with tikv-servers.
func NewRPCClient(opts ...Opt) *RPCClient {
	cli := &RPCClient{
		conns: make(map[string]*connArray),
		option: &option{
			dialTimeout: dialTimeout,
		},
		connMonitor: &connMonitor{},
	}
	for _, opt := range opts {
		opt(cli.option)
	}
	cli.connMonitor.Start()
	return cli
}

func (c *RPCClient) getConnArray(addr string, enableBatch bool, opt ...func(cfg *config.TiKVClient)) (*connArray, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	array, ok := c.conns[addr]
	c.RUnlock()
	if !ok {
		var err error
		array, err = c.createConnArray(addr, enableBatch, opt...)
		if err != nil {
			return nil, err
		}
	}

	// An idle connArray will not change to active again, this avoid the race condition
	// that recycling idle connection close an active connection unexpectedly (idle -> active).
	if array.batchConn != nil && array.isIdle() {
		return nil, errors.Errorf("rpcClient is idle")
	}

	return array, nil
}

func (c *RPCClient) createConnArray(addr string, enableBatch bool, opts ...func(cfg *config.TiKVClient)) (*connArray, error) {
	c.Lock()
	defer c.Unlock()
	array, ok := c.conns[addr]
	if !ok {
		var err error
		client := config.GetGlobalConfig().TiKVClient
		for _, opt := range opts {
			opt(&client)
		}
		array, err = newConnArray(
			client.GrpcConnectionCount,
			addr,
			c.option.security,
			&c.idleNotify,
			enableBatch,
			c.option.dialTimeout,
			c.connMonitor,
			c.option.gRPCDialOptions)

		if err != nil {
			return nil, err
		}
		c.conns[addr] = array
	}
	return array, nil
}

func (c *RPCClient) closeConns() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, array := range c.conns {
			array.Close()
		}
	}
	c.Unlock()
}

var (
	sendReqHistCache       sync.Map
	sendReqCounterCache    sync.Map
	rpcNetLatencyHistCache sync.Map
)

type sendReqHistCacheKey struct {
	tp         tikvrpc.CmdType
	id         uint64
	staleRad   bool
	isInternal bool
}

type sendReqCounterCacheKey struct {
	sendReqHistCacheKey
	requestSource string
}

type rpcNetLatencyCacheKey struct {
	storeID    uint64
	isInternal bool
}

type sendReqCounterCacheValue struct {
	counter     prometheus.Counter
	timeCounter prometheus.Counter
}

func (c *RPCClient) updateTiKVSendReqHistogram(req *tikvrpc.Request, resp *tikvrpc.Response, start time.Time, staleRead bool) {
	elapsed := time.Since(start)
	secs := elapsed.Seconds()
	storeID := req.Context.GetPeer().GetStoreId()
	isInternal := util.IsInternalRequest(req.GetRequestSource())

	histKey := sendReqHistCacheKey{
		req.Type,
		storeID,
		staleRead,
		isInternal,
	}
	counterKey := sendReqCounterCacheKey{
		histKey,
		req.GetRequestSource(),
	}

	reqType := req.Type.String()
	var storeIDStr string

	hist, ok := sendReqHistCache.Load(histKey)
	if !ok {
		if len(storeIDStr) == 0 {
			storeIDStr = strconv.FormatUint(storeID, 10)
		}
		hist = metrics.TiKVSendReqHistogram.WithLabelValues(reqType, storeIDStr,
			strconv.FormatBool(staleRead), strconv.FormatBool(isInternal))
		sendReqHistCache.Store(histKey, hist)
	}
	counter, ok := sendReqCounterCache.Load(counterKey)
	if !ok {
		if len(storeIDStr) == 0 {
			storeIDStr = strconv.FormatUint(storeID, 10)
		}
		counter = sendReqCounterCacheValue{
			metrics.TiKVSendReqCounter.WithLabelValues(reqType, storeIDStr, strconv.FormatBool(staleRead),
				counterKey.requestSource, strconv.FormatBool(isInternal)),
			metrics.TiKVSendReqTimeCounter.WithLabelValues(reqType, storeIDStr,
				strconv.FormatBool(staleRead), counterKey.requestSource, strconv.FormatBool(isInternal)),
		}
		sendReqCounterCache.Store(counterKey, counter)
	}

	hist.(prometheus.Observer).Observe(secs)
	counter.(sendReqCounterCacheValue).counter.Inc()
	counter.(sendReqCounterCacheValue).timeCounter.Add(secs)

	if execDetail := resp.GetExecDetailsV2(); execDetail != nil {
		var totalRpcWallTimeNs uint64
		if execDetail.TimeDetailV2 != nil {
			totalRpcWallTimeNs = execDetail.TimeDetailV2.TotalRpcWallTimeNs
		} else if execDetail.TimeDetail != nil {
			totalRpcWallTimeNs = execDetail.TimeDetail.TotalRpcWallTimeNs
		}
		if totalRpcWallTimeNs > 0 {
			cacheKey := rpcNetLatencyCacheKey{
				storeID,
				isInternal,
			}
			latHist, ok := rpcNetLatencyHistCache.Load(cacheKey)
			if !ok {
				if len(storeIDStr) == 0 {
					storeIDStr = strconv.FormatUint(storeID, 10)
				}
				latHist = metrics.TiKVRPCNetLatencyHistogram.WithLabelValues(storeIDStr, strconv.FormatBool(isInternal))
				rpcNetLatencyHistCache.Store(cacheKey, latHist)
			}
			latency := elapsed - time.Duration(totalRpcWallTimeNs)*time.Nanosecond
			latHist.(prometheus.Observer).Observe(latency.Seconds())
		}
	}
}

func (c *RPCClient) sendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, err error) {
	var spanRPC opentracing.Span
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		spanRPC = span.Tracer().StartSpan(fmt.Sprintf("rpcClient.SendRequest, region ID: %d, type: %s", req.RegionId, req.Type), opentracing.ChildOf(span.Context()))
		defer spanRPC.Finish()
		ctx = opentracing.ContextWithSpan(ctx, spanRPC)
	}

	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		go c.recycleIdleConnArray()
	}

	// TiDB will not send batch commands to TiFlash, to resolve the conflict with Batch Cop Request.
	// tiflash/tiflash_mpp/tidb don't use BatchCommand.
	enableBatch := req.StoreTp == tikvrpc.TiKV
	connArray, err := c.getConnArray(addr, enableBatch)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	staleRead := req.GetStaleRead()
	defer func() {
		stmtExec := ctx.Value(util.ExecDetailsKey)
		if stmtExec != nil {
			detail := stmtExec.(*util.ExecDetails)
			atomic.AddInt64(&detail.WaitKVRespDuration, int64(time.Since(start)))
		}
		c.updateTiKVSendReqHistogram(req, resp, start, staleRead)

		if spanRPC != nil && util.TraceExecDetailsEnabled(ctx) {
			if si := buildSpanInfoFromResp(resp); si != nil {
				si.addTo(spanRPC, start)
			}
		}
	}()

	// TiDB RPC server supports batch RPC, but batch connection will send heart beat, It's not necessary since
	// request to TiDB is not high frequency.
	if config.GetGlobalConfig().TiKVClient.MaxBatchSize > 0 && enableBatch {
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			defer trace.StartRegion(ctx, req.Type.String()).End()
			return sendBatchRequest(ctx, addr, req.ForwardedHost, connArray.batchConn, batchReq, timeout)
		}
	}

	clientConn := connArray.Get()
	if state := clientConn.GetState(); state == connectivity.TransientFailure {
		storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
		metrics.TiKVGRPCConnTransientFailureCounter.WithLabelValues(addr, storeID).Inc()
	}

	if req.IsDebugReq() {
		client := debugpb.NewDebugClient(clientConn)
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return tikvrpc.CallDebugRPC(ctx1, client, req)
	}

	client := tikvpb.NewTikvClient(clientConn)

	// Set metadata for request forwarding. Needn't forward DebugReq.
	if req.ForwardedHost != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, forwardMetadataKey, req.ForwardedHost)
	}
	switch req.Type {
	case tikvrpc.CmdBatchCop:
		return c.getBatchCopStreamResponse(ctx, client, req, timeout, connArray)
	case tikvrpc.CmdCopStream:
		return c.getCopStreamResponse(ctx, client, req, timeout, connArray)
	case tikvrpc.CmdMPPConn:
		return c.getMPPStreamResponse(ctx, client, req, timeout, connArray)
	}
	// Or else it's a unary call.
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return tikvrpc.CallRPC(ctx1, client, req)
}

// SendRequest sends a Request to server and receives Response.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// In unit test, the option or codec may be nil. Here should skip the encode/decode process.
	if c.option == nil || c.option.codec == nil {
		return c.sendRequest(ctx, addr, req, timeout)
	}

	codec := c.option.codec
	req, err := codec.EncodeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.sendRequest(ctx, addr, req, timeout)
	if err != nil {
		return nil, err
	}
	return codec.DecodeResponse(req, resp)
}

func (c *RPCClient) getCopStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connArray *connArray) (*tikvrpc.Response, error) {
	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in copStream.Lease.Cancel call this cancel at copStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
		return nil, err
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	copStream := resp.Resp.(*tikvrpc.CopStreamResponse)
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *coprocessor.Response
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.WithStack(err)
		}
		logutil.BgLogger().Debug("copstream returns nothing for the request.")
	}
	copStream.Response = first
	return resp, nil

}

func (c *RPCClient) getBatchCopStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connArray *connArray) (*tikvrpc.Response, error) {
	// Coprocessor streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in copStream.Lease.Cancel call this cancel at copStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
		return nil, err
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	copStream := resp.Resp.(*tikvrpc.BatchCopStreamResponse)
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *coprocessor.BatchResponse
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.WithStack(err)
		}
		logutil.BgLogger().Debug("batch copstream returns nothing for the request.")
	}
	copStream.BatchResponse = first
	return resp, nil
}

func (c *RPCClient) getMPPStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connArray *connArray) (*tikvrpc.Response, error) {
	// MPP streaming request.
	// Use context to support timeout for grpc streaming client.
	ctx1, cancel := context.WithCancel(ctx)
	// Should NOT call defer cancel() here because it will cancel further stream.Recv()
	// We put it in copStream.Lease.Cancel call this cancel at copStream.Close
	// TODO: add unit test for SendRequest.
	resp, err := tikvrpc.CallRPC(ctx1, client, req)
	if err != nil {
		cancel()
		return nil, err
	}

	// Put the lease object to the timeout channel, so it would be checked periodically.
	copStream := resp.Resp.(*tikvrpc.MPPStreamResponse)
	copStream.Timeout = timeout
	copStream.Lease.Cancel = cancel
	connArray.streamTimeout <- &copStream.Lease

	// Read the first streaming response to get CopStreamResponse.
	// This can make error handling much easier, because SendReq() retry on
	// region error automatically.
	var first *mpp.MPPDataPacket
	first, err = copStream.Recv()
	if err != nil {
		if errors.Cause(err) != io.EOF {
			return nil, errors.WithStack(err)
		}
	}
	copStream.MPPDataPacket = first
	return resp, nil
}

// Close closes all connections.
func (c *RPCClient) Close() error {
	// TODO: add a unit test for SendRequest After Closed
	c.closeConns()
	c.connMonitor.Stop()
	return nil
}

// CloseAddr closes gRPC connections to the address.
func (c *RPCClient) CloseAddr(addr string) error {
	c.Lock()
	conn, ok := c.conns[addr]
	if ok {
		delete(c.conns, addr)
		logutil.BgLogger().Debug("close connection", zap.String("target", addr))
	}
	c.Unlock()

	if conn != nil {
		conn.Close()
	}
	return nil
}

type spanInfo struct {
	name     string
	dur      uint64
	async    bool
	children []spanInfo
}

func (si *spanInfo) calcDur() uint64 {
	if si.dur == 0 {
		for _, child := range si.children {
			if child.async {
				// TODO: Here we just skip the duration of async process, however there might be a sync point before a
				// specified span, in which case we should take the max(main_routine_duration, async_span_duration).
				// It's OK for now, since only pesist-log is marked as async, whose duration is typically smaller than
				// commit-log.
				continue
			}
			si.dur += child.calcDur()
		}
	}
	return si.dur
}

func (si *spanInfo) addTo(parent opentracing.Span, start time.Time) time.Time {
	if parent == nil {
		return start
	}
	dur := si.calcDur()
	if dur == 0 {
		return start
	}
	end := start.Add(time.Duration(dur) * time.Nanosecond)
	tracer := parent.Tracer()
	span := tracer.StartSpan(si.name, opentracing.ChildOf(parent.Context()), opentracing.StartTime(start))
	t := start
	for _, child := range si.children {
		t = child.addTo(span, t)
	}
	span.FinishWithOptions(opentracing.FinishOptions{FinishTime: end})
	if si.async {
		span.SetTag("async", "true")
		return start
	}
	return end
}

func (si *spanInfo) printTo(out io.StringWriter) {
	out.WriteString(si.name)
	if si.async {
		out.WriteString("'")
	}
	if si.dur > 0 {
		out.WriteString("[")
		out.WriteString(time.Duration(si.dur).String())
		out.WriteString("]")
	}
	if len(si.children) > 0 {
		out.WriteString("{")
		for _, child := range si.children {
			out.WriteString(" ")
			child.printTo(out)
		}
		out.WriteString(" }")
	}
}

func (si *spanInfo) String() string {
	buf := new(strings.Builder)
	si.printTo(buf)
	return buf.String()
}

func buildSpanInfoFromResp(resp *tikvrpc.Response) *spanInfo {
	details := resp.GetExecDetailsV2()
	if details == nil {
		return nil
	}

	td := details.TimeDetailV2
	tdOld := details.TimeDetail
	sd := details.ScanDetailV2
	wd := details.WriteDetail

	if td == nil && tdOld == nil {
		return nil
	}

	var spanRPC, spanWait, spanProcess spanInfo
	if td != nil {
		spanRPC = spanInfo{name: "tikv.RPC", dur: td.TotalRpcWallTimeNs}
		spanWait = spanInfo{name: "tikv.Wait", dur: td.WaitWallTimeNs}
		spanProcess = spanInfo{name: "tikv.Process", dur: td.ProcessWallTimeNs}
	} else if tdOld != nil {
		// TimeDetail is deprecated, will be removed in future version.
		spanRPC = spanInfo{name: "tikv.RPC", dur: tdOld.TotalRpcWallTimeNs}
		spanWait = spanInfo{name: "tikv.Wait", dur: tdOld.WaitWallTimeMs * uint64(time.Millisecond)}
		spanProcess = spanInfo{name: "tikv.Process", dur: tdOld.ProcessWallTimeMs * uint64(time.Millisecond)}
	}

	if sd != nil {
		spanWait.children = append(spanWait.children, spanInfo{name: "tikv.GetSnapshot", dur: sd.GetSnapshotNanos})
		if wd == nil {
			spanProcess.children = append(spanProcess.children, spanInfo{name: "tikv.RocksDBBlockRead", dur: sd.RocksdbBlockReadNanos})
		}
	}

	spanRPC.children = append(spanRPC.children, spanWait, spanProcess)
	if td != nil {
		spanSuspend := spanInfo{name: "tikv.Suspend", dur: td.ProcessSuspendWallTimeNs}
		spanRPC.children = append(spanRPC.children, spanSuspend)
	}

	if wd != nil {
		spanAsyncWrite := spanInfo{
			name: "tikv.AsyncWrite",
			children: []spanInfo{
				{name: "tikv.StoreBatchWait", dur: wd.StoreBatchWaitNanos},
				{name: "tikv.ProposeSendWait", dur: wd.ProposeSendWaitNanos},
				{name: "tikv.PersistLog", dur: wd.PersistLogNanos, async: true, children: []spanInfo{
					{name: "tikv.RaftDBWriteWait", dur: wd.RaftDbWriteLeaderWaitNanos}, // MutexLock + WriteLeader
					{name: "tikv.RaftDBWriteWAL", dur: wd.RaftDbSyncLogNanos},
					{name: "tikv.RaftDBWriteMemtable", dur: wd.RaftDbWriteMemtableNanos},
				}},
				{name: "tikv.CommitLog", dur: wd.CommitLogNanos},
				{name: "tikv.ApplyBatchWait", dur: wd.ApplyBatchWaitNanos},
				{name: "tikv.ApplyLog", dur: wd.ApplyLogNanos, children: []spanInfo{
					{name: "tikv.ApplyMutexLock", dur: wd.ApplyMutexLockNanos},
					{name: "tikv.ApplyWriteLeaderWait", dur: wd.ApplyWriteLeaderWaitNanos},
					{name: "tikv.ApplyWriteWAL", dur: wd.ApplyWriteWalNanos},
					{name: "tikv.ApplyWriteMemtable", dur: wd.ApplyWriteMemtableNanos},
				}},
			},
		}
		spanRPC.children = append(spanRPC.children, spanAsyncWrite)
	}

	return &spanRPC
}
