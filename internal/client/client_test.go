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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

func TestConn(t *testing.T) {
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
	})()

	client := NewRPCClient()

	addr := "127.0.0.1:6379"
	conn1, err := client.getConnArray(addr, true)
	assert.Nil(t, err)

	conn2, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	assert.False(t, conn2.Get() == conn1.Get())

	assert.Nil(t, client.CloseAddr(addr))
	_, ok := client.conns[addr]
	assert.False(t, ok)
	conn3, err := client.getConnArray(addr, true)
	assert.Nil(t, err)
	assert.NotNil(t, conn3)

	client.Close()
	conn4, err := client.getConnArray(addr, true)
	assert.NotNil(t, err)
	assert.Nil(t, conn4)
}

func TestGetConnAfterClose(t *testing.T) {
	client := NewRPCClient()

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
	_, err := sendBatchRequest(ctx, "", "", a, req, 2*time.Second)
	assert.Equal(t, errors.Cause(err), context.Canceled)

	_, err = sendBatchRequest(context.Background(), "", "", a, req, 0)
	assert.Equal(t, errors.Cause(err), context.DeadlineExceeded)
}

func TestSendWhenReconnect(t *testing.T) {
	server, port := startMockTikvService()
	require.True(t, port > 0)

	rpcClient := NewRPCClient()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	conn, err := rpcClient.getConnArray(addr, true)
	assert.Nil(t, err)

	// Suppose all connections are re-establishing.
	for _, client := range conn.batchConn.batchCommandsClients {
		client.lockForRecreate()
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, 100*time.Second)
	assert.True(t, err.Error() == "no available connections")
	conn.Close()
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
	server, port := startMockTikvService()
	require.True(t, port > 0)
	defer server.Stop()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Disable batch.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 0
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient()
	defer rpcClient.closeConns()

	var checkCnt uint64
	// Check no corresponding metadata if ForwardedHost is empty.
	server.setMetaChecker(func(ctx context.Context) error {
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
	server.setMetaChecker(func(ctx context.Context) error {
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
	server, port := startMockTikvService()
	require.True(t, port > 0)
	defer server.Stop()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Enable batch and limit the connection count to 1 so that
	// there is only one BatchCommands stream for each host or forwarded host.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 128
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient()
	defer rpcClient.closeConns()

	var checkCnt uint64
	setCheckHandler := func(forwardedHost string) {
		server.setMetaChecker(func(ctx context.Context) error {
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
	batchedReq, forwardingReqs := builder.build(func(id uint64, e *batchCommandsEntry) {
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
	batchedReq, forwardingReqs = builder.build(func(id uint64, e *batchCommandsEntry) {
		entryMap[id] = e
	})
	assert.Equal(t, len(batchedReq.GetRequests()), 1)
	assert.Equal(t, len(batchedReq.GetRequestIds()), 1)
	assert.Equal(t, len(forwardingReqs), 3)
	for i, host := range forwardedHosts[1:] {
		assert.Equal(t, len(forwardingReqs[host].GetRequests()), i+2)
		assert.Equal(t, len(forwardingReqs[host].GetRequestIds()), i+2)
	}
	assert.Equal(t, builder.idAlloc, uint64(10+builder.len()))
	assert.Equal(t, len(entryMap), builder.len())
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
	batchedReq, forwardingReqs = builder.build(func(id uint64, e *batchCommandsEntry) {
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
	assert.Equal(t, len(builder.entries), 0)
	assert.Equal(t, len(builder.requests), 0)
	assert.Equal(t, len(builder.requestIDs), 0)
	assert.Equal(t, len(builder.forwardingReqs), 0)
	assert.NotEqual(t, builder.idAlloc, 0)
}
