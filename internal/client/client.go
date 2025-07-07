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

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
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
	// SetEventListener registers an event listener for the Client instance. If it's called more than once, the
	// previously set one will be replaced.
	SetEventListener(listener ClientEventListener)
}

// ClientEventListener is a listener to handle events produced by `Client`.
type ClientEventListener interface {
	// OnHealthFeedback is called when `Client` receives a response that carries the HealthFeedback information.
	OnHealthFeedback(feedback *kvrpcpb.HealthFeedback)
}

// ClientExt is a client has extended interfaces.
type ClientExt interface {
	// CloseAddrVer closes gRPC connections to the address with additional `ver` parameter.
	// Each new connection will have an incremented `ver` value, and attempts to close a previous `ver` will be ignored.
	// Passing `math.MaxUint64` as the `ver` parameter will forcefully close all connections to the address.
	CloseAddrVer(addr string, ver uint64) error
}

// ErrConn wraps error with target address and version of the connection.
type ErrConn struct {
	Err  error
	Addr string
	Ver  uint64
}

func (e *ErrConn) Error() string {
	return fmt.Sprintf("[%s](%d) %s", e.Addr, e.Ver, e.Err.Error())
}

func (e *ErrConn) Cause() error {
	return e.Err
}

func (e *ErrConn) Unwrap() error {
	return e.Err
}

func WrapErrConn(err error, pool *connPool) error {
	if err == nil {
		return nil
	}
	return &ErrConn{
		Err:  err,
		Addr: pool.target,
		Ver:  pool.ver,
	}
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

	connPools map[string]*connPool
	vers      map[string]uint64
	option    *option

	idleNotify uint32

	// Periodically check whether there is any connection that is idle and then close and remove these connections.
	// Implement background cleanup.
	isClosed bool

	connMonitor *connMonitor

	eventListener *atomic.Pointer[ClientEventListener]
}

var _ Client = &RPCClient{}

// NewRPCClient creates a client that manages connections and rpc calls with tikv-servers.
func NewRPCClient(opts ...Opt) *RPCClient {
	cli := &RPCClient{
		connPools: make(map[string]*connPool),
		vers:      make(map[string]uint64),
		option: &option{
			dialTimeout: dialTimeout,
		},
		connMonitor:   &connMonitor{},
		eventListener: new(atomic.Pointer[ClientEventListener]),
	}
	for _, opt := range opts {
		opt(cli.option)
	}
	cli.connMonitor.Start()
	return cli
}

func (c *RPCClient) getConnPool(addr string, enableBatch bool, opt ...func(cfg *config.TiKVClient)) (*connPool, error) {
	c.RLock()
	if c.isClosed {
		c.RUnlock()
		return nil, errors.Errorf("rpcClient is closed")
	}
	pool, ok := c.connPools[addr]
	c.RUnlock()
	if !ok {
		var err error
		pool, err = c.createConnPool(addr, enableBatch, opt...)
		if err != nil {
			return nil, err
		}
	}

	// An idle connPool will not change to active again, this avoid the race condition
	// that recycling idle connection close an active connection unexpectedly (idle -> active).
	if pool.batchConn != nil && pool.isIdle() {
		return nil, errors.Errorf("rpcClient is idle")
	}

	return pool, nil
}

func (c *RPCClient) createConnPool(addr string, enableBatch bool, opts ...func(cfg *config.TiKVClient)) (*connPool, error) {
	c.Lock()
	defer c.Unlock()
	pool, ok := c.connPools[addr]
	if !ok {
		var err error
		client := config.GetGlobalConfig().TiKVClient
		for _, opt := range opts {
			opt(&client)
		}
		ver := c.vers[addr] + 1
		pool, err = newConnPool(
			client.GrpcConnectionCount,
			addr,
			ver,
			c.option.security,
			&c.idleNotify,
			enableBatch,
			c.option.dialTimeout,
			c.connMonitor,
			c.eventListener,
			c.option.gRPCDialOptions)

		if err != nil {
			return nil, err
		}
		c.connPools[addr] = pool
		c.vers[addr] = ver
	}
	return pool, nil
}

func (c *RPCClient) closeConns() {
	c.Lock()
	if !c.isClosed {
		c.isClosed = true
		// close all connections
		for _, pool := range c.connPools {
			pool.Close()
		}
	}
	c.Unlock()
}

func (c *RPCClient) recycleIdleConnArray() {
	start := time.Now()

	var addrs []string
	var vers []uint64
	c.RLock()
	for _, conn := range c.connPools {
		if conn.batchConn != nil && conn.isIdle() {
			addrs = append(addrs, conn.target)
			vers = append(vers, conn.ver)
		}
	}
	c.RUnlock()

	for i, addr := range addrs {
		c.CloseAddrVer(addr, vers[i])
	}

	metrics.TiKVBatchClientRecycle.Observe(time.Since(start).Seconds())
}

func (c *RPCClient) sendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, err error) {
	tikvrpc.AttachContext(req, req.Context)

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
	connPool, err := c.getConnPool(addr, enableBatch)
	if err != nil {
		return nil, err
	}

	wrapErrConn := func(resp *tikvrpc.Response, err error) (*tikvrpc.Response, error) {
		return resp, WrapErrConn(err, connPool)
	}

	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		connPool.updateRPCMetrics(req, resp, elapsed)

		if spanRPC != nil && util.TraceExecDetailsEnabled(ctx) {
			if si := buildSpanInfoFromResp(resp); si != nil {
				si.addTo(spanRPC, start)
			}
		}
	}()

	// TiDB RPC server supports batch RPC, but batch connection will send heart beat, It's not necessary since
	// request to TiDB is not high frequency.
	pri := req.GetResourceControlContext().GetOverridePriority()
	if config.GetGlobalConfig().TiKVClient.MaxBatchSize > 0 && enableBatch {
		if batchReq := req.ToBatchCommandsRequest(); batchReq != nil {
			defer trace.StartRegion(ctx, req.Type.String()).End()
			return wrapErrConn(sendBatchRequest(ctx, addr, req.ForwardedHost, connPool.batchConn, batchReq, timeout, pri))
		}
	}

	clientConn := connPool.Get()
	if state := clientConn.GetState(); state == connectivity.TransientFailure {
		storeID := strconv.FormatUint(req.Context.GetPeer().GetStoreId(), 10)
		metrics.TiKVGRPCConnTransientFailureCounter.WithLabelValues(addr, storeID).Inc()
	}

	if req.IsDebugReq() {
		client := debugpb.NewDebugClient(clientConn)
		ctx1, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return wrapErrConn(tikvrpc.CallDebugRPC(ctx1, client, req))
	}

	client := tikvpb.NewTikvClient(clientConn)

	// Set metadata for request forwarding. Needn't forward DebugReq.
	if req.ForwardedHost != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, forwardMetadataKey, req.ForwardedHost)
	}
	switch req.Type {
	case tikvrpc.CmdBatchCop:
		return wrapErrConn(c.getBatchCopStreamResponse(ctx, client, req, timeout, connPool))
	case tikvrpc.CmdCopStream:
		return wrapErrConn(c.getCopStreamResponse(ctx, client, req, timeout, connPool))
	case tikvrpc.CmdMPPConn:
		return wrapErrConn(c.getMPPStreamResponse(ctx, client, req, timeout, connPool))
	}
	// Or else it's a unary call.
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return wrapErrConn(tikvrpc.CallRPC(ctx1, client, req))
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

func (c *RPCClient) getCopStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connPool *connPool) (*tikvrpc.Response, error) {
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
	connPool.streamTimeout <- &copStream.Lease

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

func (c *RPCClient) getBatchCopStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connPool *connPool) (*tikvrpc.Response, error) {
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
	connPool.streamTimeout <- &copStream.Lease

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

func (c *RPCClient) getMPPStreamResponse(ctx context.Context, client tikvpb.TikvClient, req *tikvrpc.Request, timeout time.Duration, connPool *connPool) (*tikvrpc.Response, error) {
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
	connPool.streamTimeout <- &copStream.Lease

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
	return c.CloseAddrVer(addr, math.MaxUint64)
}

func (c *RPCClient) CloseAddrVer(addr string, ver uint64) error {
	c.Lock()
	if c.isClosed {
		c.Unlock()
		return nil
	}
	pool, ok := c.connPools[addr]
	if ok {
		if pool.ver <= ver {
			delete(c.connPools, addr)
			logutil.BgLogger().Debug("close connection", zap.String("target", addr), zap.Uint64("ver", ver), zap.Uint64("pool.ver", pool.ver))
		} else {
			logutil.BgLogger().Debug("ignore close connection", zap.String("target", addr), zap.Uint64("ver", ver), zap.Uint64("pool.ver", pool.ver))
			pool = nil
		}
	}
	c.Unlock()

	if pool != nil {
		pool.Close()
	}
	return nil
}

// SetEventListener registers an event listener for the Client instance. If it's called more than once, the
// previously set one will be replaced.
func (c *RPCClient) SetEventListener(listener ClientEventListener) {
	c.eventListener.Store(&listener)
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

func deriveRPCMetrics(root prometheus.ObserverVec) *rpcMetrics {
	return &rpcMetrics{
		root:        root,
		latGet:      root.With(prometheus.Labels{metrics.LblType: tikvrpc.CmdGet.String(), metrics.LblStaleRead: "false", metrics.LblScope: "false"}),
		latCop:      root.With(prometheus.Labels{metrics.LblType: tikvrpc.CmdCop.String(), metrics.LblStaleRead: "false", metrics.LblScope: "false"}),
		latBatchGet: root.With(prometheus.Labels{metrics.LblType: tikvrpc.CmdBatchGet.String(), metrics.LblStaleRead: "false", metrics.LblScope: "false"}),
	}
}

type rpcMetrics struct {
	root prometheus.ObserverVec

	// static metrics
	latGet      prometheus.Observer
	latCop      prometheus.Observer
	latBatchGet prometheus.Observer

	latOther sync.Map
}

func (m *rpcMetrics) get(cmd tikvrpc.CmdType, stale bool, internal bool) prometheus.Observer {
	if !stale && !internal {
		switch cmd {
		case tikvrpc.CmdGet:
			return m.latGet
		case tikvrpc.CmdCop:
			return m.latCop
		case tikvrpc.CmdBatchGet:
			return m.latBatchGet
		}
	}
	key := uint64(cmd)
	if stale {
		key |= 1 << 16
	}
	if internal {
		key |= 1 << 17
	}
	lat, ok := m.latOther.Load(key)
	if !ok {
		lat = m.root.With(prometheus.Labels{
			metrics.LblType:      cmd.String(),
			metrics.LblStaleRead: strconv.FormatBool(stale),
			metrics.LblScope:     strconv.FormatBool(internal),
		})
		m.latOther.Store(key, lat)
	}
	return lat.(prometheus.Observer)
}
