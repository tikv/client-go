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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

func TestSendRequestAsyncBasic(t *testing.T) {
	ctx := context.Background()
	srv, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, srv.IsRunning())
	addr := srv.Addr()

	cli := NewRPCClient()
	defer func() {
		cli.Close()
		srv.Stop()
	}()

	t.Run("BatchDisabled", func(t *testing.T) {
		defer config.UpdateGlobal(func(conf *config.Config) { conf.TiKVClient.MaxBatchSize = 0 })()
		called := false
		cb := async.NewCallback(nil, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorContains(t, err, "batch client is disabled")
		})
		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		cli.SendRequestAsync(ctx, addr, req, cb)
		require.True(t, called)
	})

	t.Run("UnsupportedStoreType", func(t *testing.T) {
		called := false
		cb := async.NewCallback(nil, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorContains(t, err, "unsupported store type")
		})
		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		req.StoreTp = tikvrpc.TiFlash
		cli.SendRequestAsync(ctx, addr, req, cb)
		require.True(t, called)
	})

	t.Run("UnsupportedRequestType", func(t *testing.T) {
		called := false
		cb := async.NewCallback(nil, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorContains(t, err, "unsupported request type")
		})
		req := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{})
		cli.SendRequestAsync(ctx, addr, req, cb)
		require.True(t, called)
	})

	t.Run("OK", func(t *testing.T) {
		rl := async.NewRunLoop()
		ok := false
		cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
			require.NoError(t, err)
			ok = true
		})
		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		cli.SendRequestAsync(ctx, addr, req, cb)

		rl.Exec(ctx)
		require.True(t, ok)
	})
}

func TestSendRequestAsyncTimeout(t *testing.T) {
	ctx := context.Background()
	srv, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, srv.IsRunning())
	addr := srv.Addr()

	cli := NewRPCClient()
	defer func() {
		cli.Close()
		srv.Stop()
	}()

	makeBatchResponse := func(req *tikvpb.BatchCommandsRequest) *tikvpb.BatchCommandsResponse {
		resp := &tikvpb.BatchCommandsResponse{RequestIds: req.GetRequestIds()}
		for range req.GetRequestIds() {
			resp.Responses = append(resp.Responses, &tikvpb.BatchCommandsResponse_Response{
				Cmd: &tikvpb.BatchCommandsResponse_Response_Empty{},
			})
		}
		return resp
	}

	t.Run("TimeoutOnHandle", func(t *testing.T) {
		sendCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		var received atomic.Bool
		handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
			received.Store(true)
			<-sendCtx.Done()
			return makeBatchResponse(req), nil
		}
		srv.OnBatchCommandsRequest.Store(&handle)
		defer srv.OnBatchCommandsRequest.Store(nil)

		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		called := false
		rl := async.NewRunLoop()
		cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
		cli.SendRequestAsync(sendCtx, addr, req, cb)
		rl.Exec(ctx)
		require.True(t, received.Load())
		require.True(t, called)
	})

	t.Run("CanceledOnHandle", func(t *testing.T) {
		sendCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)

		var received atomic.Bool
		handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
			received.Store(true)
			<-sendCtx.Done()
			return makeBatchResponse(req), nil
		}
		srv.OnBatchCommandsRequest.Store(&handle)
		defer srv.OnBatchCommandsRequest.Store(nil)

		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		called := false
		rl := async.NewRunLoop()
		cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorIs(t, err, context.Canceled)
		})
		cli.SendRequestAsync(sendCtx, addr, req, cb)
		time.AfterFunc(time.Millisecond, cancel)
		rl.Exec(ctx)
		require.True(t, received.Load())
		require.True(t, called)
	})

	t.Run("TimeoutBeforeSend", func(t *testing.T) {
		sendCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		failpoint.Enable("tikvclient/mockBatchCommandsChannelFullOnAsyncSend", `1*return(100)`)
		defer failpoint.Disable("tikvclient/mockBatchCommandsChannelFullOnAsyncSend")

		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		called := false
		rl := async.NewRunLoop()
		cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
		cli.SendRequestAsync(sendCtx, addr, req, cb)
		rl.Exec(ctx)
		require.True(t, called)
	})

	t.Run("CanceledBeforeSend", func(t *testing.T) {
		sendCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		failpoint.Enable("tikvclient/mockBatchCommandsChannelFullOnAsyncSend", `1*return(100)`)
		defer failpoint.Disable("tikvclient/mockBatchCommandsChannelFullOnAsyncSend")

		req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
		called := false
		rl := async.NewRunLoop()
		cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
			called = true
			require.Nil(t, resp)
			require.ErrorIs(t, err, context.Canceled)
		})
		cancel()
		cli.SendRequestAsync(sendCtx, addr, req, cb)
		rl.Exec(ctx)
		require.True(t, called)
	})
}

func TestSendRequestAsyncAndCloseClientOnHandle(t *testing.T) {
	ctx := context.Background()
	srv, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, srv.IsRunning())
	defer srv.Stop()
	addr := srv.Addr()
	cli := NewRPCClient()

	var received atomic.Bool
	handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
		received.Store(true)
		time.Sleep(100 * time.Millisecond)
		return nil, errors.New("mock server error")
	}
	srv.OnBatchCommandsRequest.Store(&handle)

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	rl, called := async.NewRunLoop(), false
	cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		called = true
		require.Nil(t, resp)
		require.ErrorContains(t, err, "batch client closed")
	})
	cli.SendRequestAsync(ctx, addr, req, cb)
	time.AfterFunc(10*time.Millisecond, func() { cli.Close() })
	rl.Exec(ctx)
	require.True(t, received.Load())
	require.True(t, called)
}

func TestSendRequestAsyncAndCloseClientBeforeSend(t *testing.T) {
	ctx := context.Background()
	srv, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, srv.IsRunning())
	defer srv.Stop()
	addr := srv.Addr()
	cli := NewRPCClient()

	failpoint.Enable("tikvclient/mockBatchCommandsChannelFullOnAsyncSend", `1*return(100)`)
	defer failpoint.Disable("tikvclient/mockBatchCommandsChannelFullOnAsyncSend")

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	called := false
	cb := async.NewCallback(nil, func(resp *tikvrpc.Response, err error) {
		called = true
		require.Nil(t, resp)
		require.ErrorContains(t, err, "batchConn closed")
	})
	time.AfterFunc(10*time.Millisecond, func() { cli.Close() })
	cli.SendRequestAsync(ctx, addr, req, cb)
	require.True(t, called)
}
