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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/client_test.go
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

package client

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

func TestConn(t *testing.T) {
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
	})()

	client := NewRPCClient()
	defer client.Close()

	addr := "127.0.0.1:6379"
	conn1, err := client.getConnArray(addr, true)
	assert.Nil(t, err)

	conn2, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	assert.False(t, conn2.Get() == conn1.Get())

	ver := conn2.ver
	assert.Nil(t, client.CloseAddrVer(addr, ver-1))
	_, ok := client.conns[addr]
	assert.True(t, ok)
	assert.Nil(t, client.CloseAddrVer(addr, ver))
	_, ok = client.conns[addr]
	assert.False(t, ok)

	conn3, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	assert.NotNil(t, conn3)
	assert.Equal(t, ver+1, conn3.ver)

	client.Close()
	conn4, err := client.getConnArray(addr, true)
	assert.NotNil(t, err)
	assert.Nil(t, conn4)
}

func TestGetConnAfterClose(t *testing.T) {
	client := NewRPCClient()
	defer client.Close()

	addr := "127.0.0.1:6379"
	connArray, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	assert.Nil(t, client.CloseAddr(addr))
	conn := connArray.Get()
	state := conn.GetState()
	assert.True(t, state == connectivity.Shutdown)
}

func TestCancelTimeoutRetErr(t *testing.T) {
	req := new(tikvpb.BatchCommandsRequest_Request)
	a := newBatchConn(1, 1, nil)

	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	_, err := sendBatchRequest(ctx, "", "", a, req, 2*time.Second, 0)
	assert.Equal(t, errors.Cause(err), context.Canceled)

	_, err = sendBatchRequest(context.Background(), "", "", a, req, 0, 0)
	assert.Equal(t, errors.Cause(err), context.DeadlineExceeded)
}

func TestSendWhenReconnect(t *testing.T) {
	server, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	restoreFn := config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxConcurrencyRequestLimit = 10000
	})

	rpcClient := NewRPCClient()
	defer func() {
		rpcClient.Close()
		restoreFn()
	}()
	addr := server.Addr()
	conn, err := rpcClient.getConnArray(addr, true)
	assert.Nil(t, err)

	// Suppose all connections are re-establishing.
	for _, client := range conn.batchConn.batchCommandsClients {
		client.lockForRecreate()
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, 5*time.Second)
	require.Regexp(t, "wait recvLoop timeout, timeout:5s, wait_send_duration:.*, wait_recv_duration:.*: context deadline exceeded", err.Error())
	server.Stop()
}

// chanClient sends received requests to the channel.
type chanClient struct {
	wg *sync.WaitGroup
	ch chan<- *tikvrpc.Request
}

func (c *chanClient) Close() error {
	return nil
}

func (c *chanClient) CloseAddr(addr string) error {
	return nil
}

func (c *chanClient) SetEventListener(listener ClientEventListener) {}

func (c *chanClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.wg.Wait()
	c.ch <- req
	return nil, nil
}

func TestCollapseResolveLock(t *testing.T) {
	buildResolveLockReq := func(regionID uint64, startTS uint64, commitTS uint64, keys [][]byte) *tikvrpc.Request {
		region := &metapb.Region{Id: regionID}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{
			StartVersion:  startTS,
			CommitVersion: commitTS,
			Keys:          keys,
		})
		tikvrpc.SetContext(req, region, nil)
		return req
	}
	buildBatchResolveLockReq := func(regionID uint64, txnInfos []*kvrpcpb.TxnInfo) *tikvrpc.Request {
		region := &metapb.Region{Id: regionID}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{
			TxnInfos: txnInfos,
		})
		tikvrpc.SetContext(req, region, nil)
		return req
	}

	var wg sync.WaitGroup
	reqCh := make(chan *tikvrpc.Request)
	client := reqCollapse{&chanClient{wg: &wg, ch: reqCh}}
	ctx := context.Background()

	// Collapse ResolveLock.
	resolveLockReq := buildResolveLockReq(1, 10, 20, nil)
	wg.Add(1)
	go client.SendRequest(ctx, "", resolveLockReq, time.Second)
	go client.SendRequest(ctx, "", resolveLockReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	req := <-reqCh
	assert.Equal(t, *req, *resolveLockReq)
	select {
	case <-reqCh:
		assert.Fail(t, "fail to collapse ResolveLock")
	default:
	}

	// Don't collapse ResolveLockLite.
	resolveLockLiteReq := buildResolveLockReq(1, 10, 20, [][]byte{[]byte("foo")})
	wg.Add(1)
	go client.SendRequest(ctx, "", resolveLockLiteReq, time.Second)
	go client.SendRequest(ctx, "", resolveLockLiteReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	for i := 0; i < 2; i++ {
		req := <-reqCh
		assert.Equal(t, *req, *resolveLockLiteReq)
	}

	// Don't collapse BatchResolveLock.
	batchResolveLockReq := buildBatchResolveLockReq(1, []*kvrpcpb.TxnInfo{
		{Txn: 10, Status: 20},
	})
	wg.Add(1)
	go client.SendRequest(ctx, "", batchResolveLockReq, time.Second)
	go client.SendRequest(ctx, "", batchResolveLockReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	for i := 0; i < 2; i++ {
		req := <-reqCh
		assert.Equal(t, *req, *batchResolveLockReq)
	}

	// Mixed
	wg.Add(1)
	go client.SendRequest(ctx, "", resolveLockReq, time.Second)
	go client.SendRequest(ctx, "", resolveLockLiteReq, time.Second)
	go client.SendRequest(ctx, "", batchResolveLockReq, time.Second)
	time.Sleep(300 * time.Millisecond)
	wg.Done()
	for i := 0; i < 3; i++ {
		<-reqCh
	}
	select {
	case <-reqCh:
		assert.Fail(t, "unexpected request")
	default:
	}
}

func TestForwardMetadataByUnaryCall(t *testing.T) {
	server, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	defer server.Stop()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Disable batch.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient()
	defer rpcClient.Close()

	var checkCnt uint64
	// Check no corresponding metadata if ForwardedHost is empty.
	server.SetMetaChecker(func(ctx context.Context) error {
		atomic.AddUint64(&checkCnt, 1)
		// gRPC may set some metadata by default, e.g. "context-type".
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			vals := md.Get(forwardMetadataKey)
			assert.Equal(t, len(vals), 0)
		}
		return nil
	})

	// Prewrite represents unary-unary call.
	prewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	for i := 0; i < 3; i++ {
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		assert.Nil(t, err)
	}
	assert.Equal(t, atomic.LoadUint64(&checkCnt), uint64(3))

	// CopStream represents unary-stream call.
	copStreamReq := tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{})
	_, err := rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	assert.Nil(t, err)
	assert.Equal(t, atomic.LoadUint64(&checkCnt), uint64(4))

	checkCnt = 0
	forwardedHost := "127.0.0.1:6666"
	// Check the metadata exists.
	server.SetMetaChecker(func(ctx context.Context) error {
		atomic.AddUint64(&checkCnt, 1)
		// gRPC may set some metadata by default, e.g. "context-type".
		md, ok := metadata.FromIncomingContext(ctx)
		assert.True(t, ok)
		vals := md.Get(forwardMetadataKey)
		assert.Equal(t, vals, []string{forwardedHost})
		return nil
	})

	prewriteReq.ForwardedHost = forwardedHost
	for i := 0; i < 3; i++ {
		_, err = rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		assert.Nil(t, err)
	}
	// checkCnt should be 3 because we don't use BatchCommands for redirection for now.
	assert.Equal(t, atomic.LoadUint64(&checkCnt), uint64(3))

	copStreamReq.ForwardedHost = forwardedHost
	_, err = rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	assert.Nil(t, err)
	assert.Equal(t, atomic.LoadUint64(&checkCnt), uint64(4))
}

func TestForwardMetadataByBatchCommands(t *testing.T) {
	server, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	defer server.Stop()
	addr := server.Addr()

	// Enable batch and limit the connection count to 1 so that
	// there is only one BatchCommands stream for each host or forwarded host.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 128
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient()
	defer rpcClient.Close()

	var checkCnt uint64
	setCheckHandler := func(forwardedHost string) {
		server.SetMetaChecker(func(ctx context.Context) error {
			atomic.AddUint64(&checkCnt, 1)
			md, ok := metadata.FromIncomingContext(ctx)
			if forwardedHost == "" {
				if ok {
					vals := md.Get(forwardMetadataKey)
					assert.Equal(t, len(vals), 0)
				}
			} else {
				assert.True(t, ok)
				vals := md.Get(forwardMetadataKey)
				assert.Equal(t, vals, []string{forwardedHost})

			}
			return nil
		})
	}

	prewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	forwardedHosts := []string{"", "127.0.0.1:6666", "127.0.0.1:7777", "127.0.0.1:8888"}
	for i, forwardedHost := range forwardedHosts {
		setCheckHandler(forwardedHost)
		prewriteReq.ForwardedHost = forwardedHost
		for i := 0; i < 3; i++ {
			_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
			assert.Nil(t, err)
		}
		// checkCnt should be i because there is a stream for each forwardedHost.
		assert.Equal(t, atomic.LoadUint64(&checkCnt), 1+uint64(i))
	}

	checkCnt = 0
	// CopStream is a unary-stream call which doesn't support batch.
	copStreamReq := tikvrpc.NewRequest(tikvrpc.CmdCopStream, &coprocessor.Request{})
	// Check no corresponding metadata if forwardedHost is empty.
	setCheckHandler("")
	_, err := rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	assert.Nil(t, err)
	assert.Equal(t, atomic.LoadUint64(&checkCnt), uint64(1))

	copStreamReq.ForwardedHost = "127.0.0.1:6666"
	// Check the metadata exists.
	setCheckHandler(copStreamReq.ForwardedHost)
	_, err = rpcClient.SendRequest(context.Background(), addr, copStreamReq, 10*time.Second)
	assert.Nil(t, err)
	assert.Equal(t, atomic.LoadUint64(&checkCnt), uint64(2))
}

func TestBatchCommandsBuilder(t *testing.T) {
	builder := newBatchCommandsBuilder(128)

	// Test no forwarding requests.
	builder.reset()
	req := new(tikvpb.BatchCommandsRequest_Request)
	for i := 0; i < 10; i++ {
		builder.push(&batchCommandsEntry{req: req})
		assert.Equal(t, builder.len(), i+1)
	}
	entryMap := make(map[uint64]*batchCommandsEntry)
	batchedReq, forwardingReqs := builder.buildWithLimit(math.MaxInt64, func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	assert.Equal(t, len(batchedReq.GetRequests()), 10)
	assert.Equal(t, len(batchedReq.GetRequestIds()), 10)
	assert.Equal(t, len(entryMap), 10)
	for i, id := range batchedReq.GetRequestIds() {
		assert.Equal(t, id, uint64(i))
		assert.Equal(t, entryMap[id].req, batchedReq.GetRequests()[i])
	}
	assert.Equal(t, len(forwardingReqs), 0)
	assert.Equal(t, builder.idAlloc, uint64(10))

	// Test collecting forwarding requests.
	builder.reset()
	forwardedHosts := []string{"", "127.0.0.1:6666", "127.0.0.1:7777", "127.0.0.1:8888"}
	for i := range forwardedHosts {
		for j, host := range forwardedHosts {
			// Each forwarded host has incremental count of requests
			// and interleaves with each other.
			if i <= j {
				builder.push(&batchCommandsEntry{req: req, forwardedHost: host})
			}
		}
	}
	entryMap = make(map[uint64]*batchCommandsEntry)
	batchedReq, forwardingReqs = builder.buildWithLimit(math.MaxInt64, func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	assert.Equal(t, len(batchedReq.GetRequests()), 1)
	assert.Equal(t, len(batchedReq.GetRequestIds()), 1)
	assert.Equal(t, len(forwardingReqs), 3)
	for i, host := range forwardedHosts[1:] {
		assert.Equal(t, len(forwardingReqs[host].GetRequests()), i+2)
		assert.Equal(t, len(forwardingReqs[host].GetRequestIds()), i+2)
	}
	assert.Equal(t, int(builder.idAlloc), 20)
	assert.Equal(t, len(entryMap), 10)
	for host, forwardingReq := range forwardingReqs {
		for i, id := range forwardingReq.GetRequestIds() {
			assert.Equal(t, entryMap[id].req, forwardingReq.GetRequests()[i])
			assert.Equal(t, entryMap[id].forwardedHost, host)
		}
	}

	// Test not collecting canceled requests
	builder.reset()
	entries := []*batchCommandsEntry{
		{canceled: 1, req: req},
		{canceled: 0, req: req},
		{canceled: 1, req: req},
		{canceled: 1, req: req},
		{canceled: 0, req: req},
	}
	for _, entry := range entries {
		builder.push(entry)
	}
	entryMap = make(map[uint64]*batchCommandsEntry)
	batchedReq, forwardingReqs = builder.buildWithLimit(math.MaxInt64, func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	assert.Equal(t, len(batchedReq.GetRequests()), 2)
	assert.Equal(t, len(batchedReq.GetRequestIds()), 2)
	assert.Equal(t, len(forwardingReqs), 0)
	assert.Equal(t, len(entryMap), 2)
	for i, id := range batchedReq.GetRequestIds() {
		assert.Equal(t, entryMap[id].req, batchedReq.GetRequests()[i])
		assert.False(t, entryMap[id].isCanceled())
	}

	// Test canceling all requests
	builder.reset()
	entries = entries[:0]
	for i := 0; i < 3; i++ {
		entry := &batchCommandsEntry{req: req, res: make(chan *tikvpb.BatchCommandsResponse_Response, 1)}
		entries = append(entries, entry)
		builder.push(entry)
	}
	err := errors.New("error")
	builder.cancel(err)
	for _, entry := range entries {
		_, ok := <-entry.res
		assert.False(t, ok)
		assert.Equal(t, entry.err, err)
	}

	// Test reset
	builder.reset()
	assert.Equal(t, builder.len(), 0)
	assert.Equal(t, len(builder.requests), 0)
	assert.Equal(t, len(builder.requestIDs), 0)
	assert.Equal(t, len(builder.forwardingReqs), 0)
	assert.NotEqual(t, builder.idAlloc, 0)
}

func TestTraceExecDetails(t *testing.T) {
	assert.Nil(t, buildSpanInfoFromResp(nil))
	assert.Nil(t, buildSpanInfoFromResp(&tikvrpc.Response{}))
	assert.Nil(t, buildSpanInfoFromResp(&tikvrpc.Response{Resp: &kvrpcpb.GetResponse{}}))
	assert.Nil(t, buildSpanInfoFromResp(&tikvrpc.Response{Resp: &kvrpcpb.GetResponse{ExecDetailsV2: &kvrpcpb.ExecDetailsV2{}}}))

	buildSpanInfo := func(details *kvrpcpb.ExecDetailsV2) *spanInfo {
		return buildSpanInfoFromResp(&tikvrpc.Response{Resp: &kvrpcpb.GetResponse{ExecDetailsV2: details}})
	}
	fmtMockTracer := func(tracer *mocktracer.MockTracer) string {
		buf := new(strings.Builder)
		for i, span := range tracer.FinishedSpans() {
			if i > 0 {
				buf.WriteString("\n")
			}
			buf.WriteString("[")
			buf.WriteString(span.StartTime.Format("05.000"))
			buf.WriteString(",")
			buf.WriteString(span.FinishTime.Format("05.000"))
			buf.WriteString("] ")
			buf.WriteString(span.OperationName)
			if span.Tag("async") != nil {
				buf.WriteString("'")
			}
		}
		return buf.String()
	}
	baseTime := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, baseTime, (&spanInfo{}).addTo(nil, baseTime))

	for i, tt := range []struct {
		details  *kvrpcpb.ExecDetailsV2
		infoOut  string
		traceOut string
	}{
		{
			&kvrpcpb.ExecDetailsV2{
				TimeDetailV2: &kvrpcpb.TimeDetailV2{TotalRpcWallTimeNs: uint64(time.Second)},
			},
			"tikv.RPC[1s]{ tikv.Wait tikv.Process tikv.Suspend }",
			"[00.000,01.000] tikv.RPC",
		},
		{
			&kvrpcpb.ExecDetailsV2{
				TimeDetailV2: &kvrpcpb.TimeDetailV2{
					TotalRpcWallTimeNs:       uint64(time.Second),
					WaitWallTimeNs:           100000000,
					ProcessWallTimeNs:        500000000,
					ProcessSuspendWallTimeNs: 50000000,
				},
			},
			"tikv.RPC[1s]{ tikv.Wait[100ms] tikv.Process[500ms] tikv.Suspend[50ms] }",
			strings.Join([]string{
				"[00.000,00.100] tikv.Wait",
				"[00.100,00.600] tikv.Process",
				"[00.600,00.650] tikv.Suspend",
				"[00.000,01.000] tikv.RPC",
			}, "\n"),
		},
		{
			&kvrpcpb.ExecDetailsV2{
				TimeDetailV2: &kvrpcpb.TimeDetailV2{
					TotalRpcWallTimeNs:       uint64(time.Second),
					WaitWallTimeNs:           100000000,
					ProcessWallTimeNs:        500000000,
					ProcessSuspendWallTimeNs: 50000000,
				},
				ScanDetailV2: &kvrpcpb.ScanDetailV2{
					GetSnapshotNanos:      uint64(80 * time.Millisecond),
					RocksdbBlockReadNanos: uint64(200 * time.Millisecond),
				},
			},
			"tikv.RPC[1s]{ tikv.Wait[100ms]{ tikv.GetSnapshot[80ms] } tikv.Process[500ms]{ tikv.RocksDBBlockRead[200ms] } tikv.Suspend[50ms] }",
			strings.Join([]string{
				"[00.000,00.080] tikv.GetSnapshot",
				"[00.000,00.100] tikv.Wait",
				"[00.100,00.300] tikv.RocksDBBlockRead",
				"[00.100,00.600] tikv.Process",
				"[00.600,00.650] tikv.Suspend",
				"[00.000,01.000] tikv.RPC",
			}, "\n"),
		},
		{
			// WriteDetail hides RocksDBBlockRead
			&kvrpcpb.ExecDetailsV2{
				TimeDetailV2: &kvrpcpb.TimeDetailV2{
					TotalRpcWallTimeNs:       uint64(time.Second),
					WaitWallTimeNs:           100000000,
					ProcessWallTimeNs:        500000000,
					ProcessSuspendWallTimeNs: 50000000,
				},
				ScanDetailV2: &kvrpcpb.ScanDetailV2{
					GetSnapshotNanos:      uint64(80 * time.Millisecond),
					RocksdbBlockReadNanos: uint64(200 * time.Millisecond),
				},
				WriteDetail: &kvrpcpb.WriteDetail{},
			},
			"tikv.RPC[1s]{ tikv.Wait[100ms]{ tikv.GetSnapshot[80ms] } tikv.Process[500ms] tikv.Suspend[50ms] tikv.AsyncWrite{ tikv.StoreBatchWait tikv.ProposeSendWait tikv.PersistLog'{ tikv.RaftDBWriteWait tikv.RaftDBWriteWAL tikv.RaftDBWriteMemtable } tikv.CommitLog tikv.ApplyBatchWait tikv.ApplyLog{ tikv.ApplyMutexLock tikv.ApplyWriteLeaderWait tikv.ApplyWriteWAL tikv.ApplyWriteMemtable } } }",
			strings.Join([]string{
				"[00.000,00.080] tikv.GetSnapshot",
				"[00.000,00.100] tikv.Wait",
				"[00.100,00.600] tikv.Process",
				"[00.600,00.650] tikv.Suspend",
				"[00.000,01.000] tikv.RPC",
			}, "\n"),
		},
		{
			&kvrpcpb.ExecDetailsV2{
				TimeDetailV2: &kvrpcpb.TimeDetailV2{
					TotalRpcWallTimeNs: uint64(time.Second),
				},
				ScanDetailV2: &kvrpcpb.ScanDetailV2{
					GetSnapshotNanos: uint64(80 * time.Millisecond),
				},
				WriteDetail: &kvrpcpb.WriteDetail{
					StoreBatchWaitNanos:        uint64(10 * time.Millisecond),
					ProposeSendWaitNanos:       uint64(10 * time.Millisecond),
					PersistLogNanos:            uint64(100 * time.Millisecond),
					RaftDbWriteLeaderWaitNanos: uint64(20 * time.Millisecond),
					RaftDbSyncLogNanos:         uint64(30 * time.Millisecond),
					RaftDbWriteMemtableNanos:   uint64(30 * time.Millisecond),
					CommitLogNanos:             uint64(200 * time.Millisecond),
					ApplyBatchWaitNanos:        uint64(20 * time.Millisecond),
					ApplyLogNanos:              uint64(300 * time.Millisecond),
					ApplyMutexLockNanos:        uint64(10 * time.Millisecond),
					ApplyWriteLeaderWaitNanos:  uint64(10 * time.Millisecond),
					ApplyWriteWalNanos:         uint64(80 * time.Millisecond),
					ApplyWriteMemtableNanos:    uint64(50 * time.Millisecond),
				},
			},
			"tikv.RPC[1s]{ tikv.Wait{ tikv.GetSnapshot[80ms] } tikv.Process tikv.Suspend tikv.AsyncWrite{ tikv.StoreBatchWait[10ms] tikv.ProposeSendWait[10ms] tikv.PersistLog'[100ms]{ tikv.RaftDBWriteWait[20ms] tikv.RaftDBWriteWAL[30ms] tikv.RaftDBWriteMemtable[30ms] } tikv.CommitLog[200ms] tikv.ApplyBatchWait[20ms] tikv.ApplyLog[300ms]{ tikv.ApplyMutexLock[10ms] tikv.ApplyWriteLeaderWait[10ms] tikv.ApplyWriteWAL[80ms] tikv.ApplyWriteMemtable[50ms] } } }",
			strings.Join([]string{
				"[00.000,00.080] tikv.GetSnapshot",
				"[00.000,00.080] tikv.Wait",
				"[00.080,00.090] tikv.StoreBatchWait",
				"[00.090,00.100] tikv.ProposeSendWait",
				"[00.100,00.120] tikv.RaftDBWriteWait",
				"[00.120,00.150] tikv.RaftDBWriteWAL",
				"[00.150,00.180] tikv.RaftDBWriteMemtable",
				"[00.100,00.200] tikv.PersistLog'",
				"[00.100,00.300] tikv.CommitLog",
				"[00.300,00.320] tikv.ApplyBatchWait",
				"[00.320,00.330] tikv.ApplyMutexLock",
				"[00.330,00.340] tikv.ApplyWriteLeaderWait",
				"[00.340,00.420] tikv.ApplyWriteWAL",
				"[00.420,00.470] tikv.ApplyWriteMemtable",
				"[00.320,00.620] tikv.ApplyLog",
				"[00.080,00.620] tikv.AsyncWrite",
				"[00.000,01.000] tikv.RPC",
			}, "\n"),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			info := buildSpanInfo(tt.details)
			assert.Equal(t, tt.infoOut, info.String())
			tracer := mocktracer.New()
			info.addTo(tracer.StartSpan("root"), baseTime)
			assert.Equal(t, tt.traceOut, fmtMockTracer(tracer))
		})
	}
}

func TestBatchClientRecoverAfterServerRestart(t *testing.T) {
	server, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, server.IsRunning())
	addr := server.Addr()
	client := NewRPCClient()
	defer func() {
		err := client.Close()
		require.NoError(t, err)
		server.Stop()
	}()

	req := &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: &coprocessor.Request{}}}
	conn, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	// send some request, it should be success.
	for i := 0; i < 100; i++ {
		_, err = sendBatchRequest(context.Background(), addr, "", conn.batchConn, req, time.Second*20, 0)
		require.NoError(t, err)
	}

	logutil.BgLogger().Info("stop mock tikv server")
	server.Stop()
	require.False(t, server.IsRunning())

	// send some request, it should be failed since server is down.
	for i := 0; i < 10; i++ {
		_, err = sendBatchRequest(context.Background(), addr, "", conn.batchConn, req, time.Millisecond*100, 0)
		require.Error(t, err)
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(300)))
		grpcConn := conn.Get()
		require.NotNil(t, grpcConn)
		logutil.BgLogger().Info("conn state",
			zap.String("state", grpcConn.GetState().String()),
			zap.Int("idx", i),
			zap.Int("goroutine-count", runtime.NumGoroutine()))
	}

	logutil.BgLogger().Info("restart mock tikv server")
	server.Start(addr)
	require.True(t, server.IsRunning())
	require.Equal(t, addr, server.Addr())

	// Wait batch client to auto reconnect.
	start := time.Now()
	for {
		grpcConn := conn.Get()
		require.NotNil(t, grpcConn)
		var cli *batchCommandsClient
		for i := range conn.batchConn.batchCommandsClients {
			if conn.batchConn.batchCommandsClients[i].tryLockForSend() {
				cli = conn.batchConn.batchCommandsClients[i]
				break
			}
		}
		// Wait for the connection to be ready,
		if cli != nil {
			cli.unlockForSend()
			break
		}
		if time.Since(start) > time.Second*10 {
			// It shouldn't take too long for batch_client to reconnect.
			require.Fail(t, "wait batch client reconnect timeout")
		}
		logutil.BgLogger().Info("goroutine count", zap.Int("count", runtime.NumGoroutine()))
		time.Sleep(time.Millisecond * 100)
	}

	// send some request, it should be success again.
	for i := 0; i < 100; i++ {
		_, err = sendBatchRequest(context.Background(), addr, "", conn.batchConn, req, time.Second*20, 0)
		require.NoError(t, err)
	}
}

func TestLimitConcurrency(t *testing.T) {
	re := require.New(t)
	batch := newBatchConn(1, 128, nil)
	{
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}})
		reqs, _ := batch.reqBuilder.buildWithLimit(1, func(_ uint64, _ *batchCommandsEntry) {})
		re.Len(reqs.RequestIds, 1)
		re.Equal(0, batch.reqBuilder.len())
		batch.reqBuilder.reset()
	}

	// highest priority task will be sent immediately, not limited by concurrency
	{
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}, pri: highTaskPriority})
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}, pri: highTaskPriority - 1})
		reqs, _ := batch.reqBuilder.buildWithLimit(0, func(_ uint64, _ *batchCommandsEntry) {})
		re.Len(reqs.RequestIds, 1)
		batch.reqBuilder.reset()
		re.Equal(1, batch.reqBuilder.len())
	}

	// medium priority tasks are limited by concurrency
	{
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}})
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}})
		reqs, _ := batch.reqBuilder.buildWithLimit(2, func(_ uint64, _ *batchCommandsEntry) {})
		re.Len(reqs.RequestIds, 2)
		re.Equal(1, batch.reqBuilder.len())
		batch.reqBuilder.reset()
	}

	// the expired tasks should be removed from the queue.
	{
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}, canceled: 1})
		batch.reqBuilder.push(&batchCommandsEntry{req: &tikvpb.BatchCommandsRequest_Request{}, canceled: 1})
		batch.reqBuilder.reset()
		re.Equal(1, batch.reqBuilder.len())
	}

}

func TestPrioritySentLimit(t *testing.T) {
	re := require.New(t)
	restoreFn := config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxConcurrencyRequestLimit = 2
		conf.TiKVClient.GrpcConnectionCount = 1
	})
	defer restoreFn()

	server, port := mockserver.StartMockTikvService()
	re.Greater(port, 0)
	rpcClient := NewRPCClient()
	defer rpcClient.Close()
	addr := server.Addr()
	wait := sync.WaitGroup{}
	bench := 10
	wait.Add(bench * 2)
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFn()
	sendFn := func(pri uint64, dur *atomic.Int64, qps *atomic.Int64) {
		for i := 0; i < bench; i++ {
			go func() {
				for {
					req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
					req.ResourceControlContext = &kvrpcpb.ResourceControlContext{OverridePriority: pri}
					now := time.Now()
					rpcClient.SendRequest(context.Background(), addr, req, 100*time.Millisecond)
					dur.Add(time.Since(now).Microseconds())
					qps.Add(1)
					select {
					case <-ctx.Done():
						wait.Done()
						return
					default:
					}
				}
			}()
		}
	}

	highDur, mediumDur := atomic.Int64{}, atomic.Int64{}
	highQps, mediumQps := atomic.Int64{}, atomic.Int64{}
	go sendFn(16, &highDur, &highQps)
	go sendFn(8, &mediumDur, &mediumQps)
	wait.Wait()
	re.Less(highDur.Load()/highQps.Load()*2, mediumDur.Load()/mediumQps.Load())
	server.Stop()
}

type testClientEventListener struct {
	healthFeedbackCh chan *kvrpcpb.HealthFeedback
}

func newTestClientEventListener() *testClientEventListener {
	return &testClientEventListener{
		healthFeedbackCh: make(chan *kvrpcpb.HealthFeedback, 100),
	}
}

func (l *testClientEventListener) OnHealthFeedback(feedback *kvrpcpb.HealthFeedback) {
	l.healthFeedbackCh <- feedback
}

func TestBatchClientReceiveHealthFeedback(t *testing.T) {
	server, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, server.IsRunning())
	defer server.Stop()
	addr := server.Addr()

	client := NewRPCClient()
	defer client.Close()

	conn, err := client.getConnArray(addr, true)
	assert.NoError(t, err)
	tikvClient := tikvpb.NewTikvClient(conn.Get())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := tikvClient.BatchCommands(ctx)
	assert.NoError(t, err)

	for reqID := uint64(1); reqID <= 3; reqID++ {
		assert.NoError(t, stream.Send(&tikvpb.BatchCommandsRequest{
			Requests: []*tikvpb.BatchCommandsRequest_Request{{
				Cmd: &tikvpb.BatchCommandsRequest_Request_Get{Get: &kvrpcpb.GetRequest{
					Context: &kvrpcpb.Context{},
					Key:     []byte("k"),
					Version: 1,
				}},
			}},
			RequestIds: []uint64{reqID},
		}))
		resp, err := stream.Recv()
		assert.NoError(t, err)
		assert.Equal(t, []uint64{reqID}, resp.GetRequestIds())
		assert.Len(t, resp.GetResponses(), 1)
		assert.Equal(t, uint64(1), resp.GetHealthFeedback().GetStoreId())
		assert.Equal(t, reqID, resp.GetHealthFeedback().GetFeedbackSeqNo())
		assert.Equal(t, int32(1), resp.GetHealthFeedback().GetSlowScore())
	}
	cancel()

	eventListener := newTestClientEventListener()
	client.SetEventListener(eventListener)
	ctx = context.Background()
	resp, err := client.SendRequest(ctx, addr, tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{}), time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, resp.Resp)

	select {
	case feedback := <-eventListener.healthFeedbackCh:
		assert.Equal(t, uint64(1), feedback.GetStoreId())
		assert.Equal(t, int32(1), feedback.GetSlowScore())
	default:
		assert.Fail(t, "health feedback not received")
	}
}

func TestRandomRestartStoreAndForwarding(t *testing.T) {
	store1, port1 := mockserver.StartMockTikvService()
	require.True(t, port1 > 0)
	require.True(t, store1.IsRunning())
	client1 := NewRPCClient()
	store2, port2 := mockserver.StartMockTikvService()
	require.True(t, port2 > 0)
	require.True(t, store2.IsRunning())
	defer func() {
		store1.Stop()
		store2.Stop()
		err := client1.Close()
		require.NoError(t, err)
	}()

	wg := sync.WaitGroup{}
	done := int64(0)
	concurrency := 500
	addr1 := store1.Addr()
	addr2 := store2.Addr()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// intermittent stop and start store1 or store2.
			var store *mockserver.MockServer
			if rand.Intn(10) < 9 {
				store = store1
			} else {
				store = store2
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(200)))
			addr := store.Addr()
			store.Stop()
			require.False(t, store.IsRunning())
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(200)))
			store.Start(addr)
			if atomic.LoadInt64(&done) >= int64(concurrency) {
				return
			}
		}
	}()

	conn, err := client1.getConnArray(addr1, true)
	assert.Nil(t, err)
	for j := 0; j < concurrency; j++ {
		wg.Add(1)
		go func() {
			defer func() {
				atomic.AddInt64(&done, 1)
				wg.Done()
			}()
			for i := 0; i < 5000; i++ {
				req := &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: &coprocessor.Request{}}}
				forwardedHost := ""
				if i%2 != 0 {
					forwardedHost = addr2
				}
				_, err := sendBatchRequest(context.Background(), addr1, forwardedHost, conn.batchConn, req, time.Millisecond*50, 0)
				if err == nil ||
					err.Error() == "EOF" ||
					err.Error() == "rpc error: code = Unavailable desc = error reading from server: EOF" ||
					strings.Contains(err.Error(), "context deadline exceeded") ||
					strings.Contains(err.Error(), "connect: connection refused") ||
					strings.Contains(err.Error(), "no available connections") ||
					strings.Contains(err.Error(), "rpc error: code = Unavailable desc = error reading from server") {
					continue
				}
				require.Fail(t, err.Error(), "unexpected error")
			}
		}()
	}
	wg.Wait()

	for _, cli := range conn.batchConn.batchCommandsClients {
		require.Equal(t, int64(9223372036854775807), cli.maxConcurrencyRequestLimit.Load())
		require.True(t, cli.available() > 0, fmt.Sprintf("sent: %d", cli.sent.Load()))
		require.True(t, cli.sent.Load() >= 0, fmt.Sprintf("sent: %d", cli.sent.Load()))
	}
}

func TestFastFailRequest(t *testing.T) {
	client := NewRPCClient()
	defer func() {
		err := client.Close()
		require.NoError(t, err)
	}()
	start := time.Now()
	unknownAddr := "127.0.0.1:52027"
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("key")})
	_, err := client.sendRequest(context.Background(), unknownAddr, req, time.Second*20)
	require.Equal(t, "context deadline exceeded", errors.Cause(err).Error())
	require.True(t, time.Since(start) < time.Second*6) // fast fail when dial target failed.
}

func TestErrConn(t *testing.T) {
	e := errors.New("conn error")
	err1 := &ErrConn{Err: e, Addr: "127.0.0.1", Ver: 10}
	err2 := &ErrConn{Err: e, Addr: "127.0.0.1", Ver: 10}

	e3 := errors.New("conn error 3")
	err3 := &ErrConn{Err: e3}

	err4 := errors.New("not ErrConn")

	assert.True(t, errors.Is(err1, err1))
	assert.True(t, errors.Is(fmt.Errorf("%w", err1), err1))
	assert.False(t, errors.Is(fmt.Errorf("%w", err2), err1)) // err2 != err1
	assert.False(t, errors.Is(fmt.Errorf("%w", err4), err1))

	var errConn *ErrConn
	assert.True(t, errors.As(err1, &errConn))
	assert.Equal(t, "127.0.0.1", errConn.Addr)
	assert.EqualValues(t, 10, errConn.Ver)
	assert.EqualError(t, errConn.Err, "conn error")

	assert.True(t, errors.As(err3, &errConn))
	assert.EqualError(t, e3, "conn error 3")

	assert.False(t, errors.As(err4, &errConn))

	errMsg := errors.New("unknown")
	assert.True(t, errors.As(err1, &errMsg))
	assert.EqualError(t, err1, errMsg.Error())
}

func TestFastFailWhenNoAvailableConn(t *testing.T) {
	server, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, server.IsRunning())
	addr := server.Addr()
	client := NewRPCClient()
	defer func() {
		err := client.Close()
		require.NoError(t, err)
		server.Stop()
	}()

	req := &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: &coprocessor.Request{}}}
	conn, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	_, err = sendBatchRequest(context.Background(), addr, "", conn.batchConn, req, time.Second, 0)
	require.NoError(t, err)

	for _, c := range conn.batchConn.batchCommandsClients {
		// mock all client a in recreate.
		c.lockForRecreate()
	}
	start := time.Now()
	timeout := time.Second
	_, err = sendBatchRequest(context.Background(), addr, "", conn.batchConn, req, timeout, 0)
	require.Error(t, err)
	require.Equal(t, "no available connections", err.Error())
	require.Less(t, time.Since(start), timeout)
}

func TestConcurrentCloseConnPanic(t *testing.T) {
	client := NewRPCClient()
	addr := "127.0.0.1:6379"
	_, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := client.Close()
		assert.Nil(t, err)
	}()
	go func() {
		defer wg.Done()
		err := client.CloseAddr(addr)
		assert.Nil(t, err)
	}()
	wg.Wait()
}
