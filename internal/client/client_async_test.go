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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/internal/txnprotocol"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
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

func TestSendRequestAsyncAttachContext(t *testing.T) {
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

	handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
		ids := req.GetRequestIds()
		require.Len(t, ids, 1)
		getReq := req.GetRequests()[0].GetGet()
		var getResp *kvrpcpb.GetResponse
		if getReq.GetContext().GetRegionId() == 0 {
			getResp = &kvrpcpb.GetResponse{RegionError: &errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}}
		} else {
			getResp = &kvrpcpb.GetResponse{Value: getReq.Key}
		}
		return &tikvpb.BatchCommandsResponse{RequestIds: ids, Responses: []*tikvpb.BatchCommandsResponse_Response{{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: getResp}}}}, nil
	}
	srv.OnBatchCommandsRequest.Store(&handle)

	called := false
	rl := async.NewRunLoop()
	cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		called = true
		require.NoError(t, err)
		getResp := resp.Resp.(*kvrpcpb.GetResponse)
		require.Nil(t, getResp.GetRegionError())
		require.Equal(t, []byte("foo"), getResp.Value)
	})
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("foo"), Version: math.MaxUint64})

	require.Zero(t, req.RegionId)
	tikvrpc.AttachContext(req, req.Context)
	tikvrpc.SetContextNoAttach(req, &metapb.Region{Id: 1}, &metapb.Peer{})

	cli.SendRequestAsync(ctx, addr, req, cb)
	rl.Exec(ctx)
	require.True(t, called)
}

func useDefaultTxnProtocolVersion(t *testing.T, version kvrpcpb.TxnProtocolVersion) {
	t.Helper()
	previous := tikvrpc.GetDefaultTxnProtocolVersion()
	require.NoError(t, tikvrpc.SetDefaultTxnProtocolVersion(version))
	t.Cleanup(func() { require.NoError(t, tikvrpc.SetDefaultTxnProtocolVersion(previous)) })
}

func contextWithSelectedDeclaration(ctx context.Context, selected kvrpcpb.TxnProtocolVersion) context.Context {
	return txnprotocol.WithDeclaration(ctx, txnprotocol.Declaration{Version: uint32(selected)})
}

func sendBatchRequestAsyncForTest(t *testing.T, client *RPCClient, ctx context.Context, addr string, req *tikvrpc.Request) {
	t.Helper()
	called := false
	runLoop := async.NewRunLoop()
	client.SendRequestAsync(ctx, addr, req, async.NewCallback(runLoop, func(_ *tikvrpc.Response, err error) {
		called = true
		require.NoError(t, err)
	}))
	runLoop.Exec(ctx)
	require.True(t, called)
}

func sendBatchRequestSyncForTest(t *testing.T, client *RPCClient, ctx context.Context, addr string, req *tikvrpc.Request) {
	t.Helper()
	resp, err := client.SendRequest(ctx, addr, req, time.Second)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// TestSendRequestBatchDeclaration covers synchronous and asynchronous batch
// conversion, including the API v2 encode/clone path. Every case starts with a
// caller-supplied declaration that the controlled selection must override, and
// verifies the child received by the server carries the selected version.
func TestSendRequestBatchDeclaration(t *testing.T) {
	defer config.UpdateGlobal(func(conf *config.Config) { conf.TiKVClient.MaxBatchSize = 128 })()
	// The process ceiling is 2 while the execution Store caps the declaration at
	// 1, so the selected version is distinguishable from both the ceiling and the
	// caller-supplied value.
	const selected = kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING
	newCopRequest := func() *tikvrpc.Request { return tikvrpc.NewRequest(tikvrpc.CmdCop, &coprocessor.Request{}) }
	copResponse := func() *tikvpb.BatchCommandsResponse_Response {
		return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Coprocessor{Coprocessor: &coprocessor.Response{}}}
	}
	copWireContext := func(req *tikvpb.BatchCommandsRequest_Request) *kvrpcpb.Context {
		return req.GetCoprocessor().GetContext()
	}

	tests := []struct {
		name        string
		newCodec    func(*testing.T) apicodec.Codec
		newRequest  func() *tikvrpc.Request
		response    func() *tikvpb.BatchCommandsResponse_Response
		wireContext func(*tikvpb.BatchCommandsRequest_Request) *kvrpcpb.Context
		send        func(*testing.T, *RPCClient, context.Context, string, *tikvrpc.Request)
		noHint      bool
	}{
		{
			name:        "async_coprocessor",
			newRequest:  newCopRequest,
			response:    copResponse,
			wireContext: copWireContext,
			send:        sendBatchRequestAsyncForTest,
		},
		{
			name:        "sync_coprocessor",
			newRequest:  newCopRequest,
			response:    copResponse,
			wireContext: copWireContext,
			send:        sendBatchRequestSyncForTest,
		},
		{
			name: "sync_without_hint", newRequest: newCopRequest, response: copResponse,
			wireContext: copWireContext, send: sendBatchRequestSyncForTest, noHint: true,
		},
		{
			name: "async_without_hint", newRequest: newCopRequest, response: copResponse,
			wireContext: copWireContext, send: sendBatchRequestAsyncForTest, noHint: true,
		},
		{
			name: "async_api_v2_get",
			newCodec: func(t *testing.T) apicodec.Codec {
				codec, err := apicodec.NewCodecV2(apicodec.ModeTxn, &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 1}})
				require.NoError(t, err)
				require.Equal(t, kvrpcpb.APIVersion_V2, codec.GetAPIVersion())
				return codec
			},
			newRequest: func() *tikvrpc.Request {
				return tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: []byte("foo")})
			},
			response: func() *tikvpb.BatchCommandsResponse_Response {
				return &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: &kvrpcpb.GetResponse{}}}
			},
			wireContext: func(req *tikvpb.BatchCommandsRequest_Request) *kvrpcpb.Context {
				return req.GetGet().GetContext()
			},
			send: sendBatchRequestAsyncForTest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			useDefaultTxnProtocolVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
			server, port := mockserver.StartMockTikvService()
			require.Positive(t, port)

			var client *RPCClient
			if test.newCodec != nil {
				client = NewRPCClient(WithCodec(test.newCodec(t)))
			} else {
				client = NewRPCClient()
			}
			t.Cleanup(func() {
				client.Close()
				server.Stop()
			})

			var seen atomic.Pointer[tikvpb.BatchCommandsRequest]
			handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
				seen.Store(req)
				responses := make([]*tikvpb.BatchCommandsResponse_Response, len(req.GetRequests()))
				for i := range responses {
					responses[i] = test.response()
				}
				return &tikvpb.BatchCommandsResponse{RequestIds: req.GetRequestIds(), Responses: responses}, nil
			}
			server.OnBatchCommandsRequest.Store(&handle)

			req := test.newRequest()
			tikvrpc.SetContextNoAttach(req, &metapb.Region{Id: 1}, &metapb.Peer{})
			req.TxnProtocolVersion = uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
			ctx := context.Background()
			wantVersion := uint32(0)
			if !test.noHint {
				ctx = contextWithSelectedDeclaration(ctx, selected)
				wantVersion = uint32(selected)
			}
			test.send(t, client, ctx, server.Addr(), req)

			captured := seen.Load()
			require.NotNil(t, captured)
			require.Len(t, captured.GetRequests(), 1)
			require.Equal(t, wantVersion, test.wireContext(captured.GetRequests()[0]).GetTxnProtocolVersion())
		})
	}
}

// TestSendRequestFailsClosedForUnpreparedProtectedRequest covers the transport
// fallback: a protected request that never went through the region request sender
// has no trusted Store range, so a payload that needs a newer transaction
// protocol version must fail locally instead of being sent.
func TestSendRequestFailsClosedForUnpreparedProtectedRequest(t *testing.T) {
	useDefaultTxnProtocolVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)
	client := NewRPCClient()
	t.Cleanup(func() { _ = client.Close() })

	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_SharedLock, Key: []byte("k")}},
	})
	tikvrpc.SetContextNoAttach(req, &metapb.Region{Id: 1}, &metapb.Peer{})

	// The connection is never used: the request is rejected before it is sent.
	_, err := client.SendRequest(context.Background(), "127.0.0.1:1", req, time.Second)
	require.Error(t, err)
	var incompatible *tikverr.ErrIncompatibleRequest
	require.ErrorAs(t, err, &incompatible)
	require.Equal(t, errorpb.IncompatibleRequestReason_IncompatibleRequestReasonUnknown, incompatible.GetReason())
}

func TestSendRequestAsyncUpdateTiKVRUV2(t *testing.T) {
	ctx := context.Background()
	original := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(original)
	})

	cfg := config.DefaultConfig()
	cfg.TiKVClient.RUV2 = config.DefaultRUV2TiKVConfig()
	config.StoreGlobalConfig(&cfg)
	weights := cfg.TiKVClient.RUV2

	srv, port := mockserver.StartMockTikvService()
	require.True(t, port > 0)
	require.True(t, srv.IsRunning())
	addr := srv.Addr()

	cli := NewRPCClient()
	defer func() {
		cli.Close()
		srv.Stop()
	}()

	handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
		ids := req.GetRequestIds()
		require.Len(t, ids, 1)
		prewriteResp := &kvrpcpb.PrewriteResponse{
			ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
				RuV2: &kvrpcpb.RUV2{
					KvEngineCacheMiss: 1,
				},
			},
		}
		return &tikvpb.BatchCommandsResponse{
			RequestIds: ids,
			Responses: []*tikvpb.BatchCommandsResponse_Response{{
				Cmd: &tikvpb.BatchCommandsResponse_Response_Prewrite{Prewrite: prewriteResp},
			}},
		}, nil
	}
	srv.OnBatchCommandsRequest.Store(&handle)

	ruDetails := util.NewRUDetails()
	sendCtx := context.WithValue(ctx, util.RUDetailsCtxKey, ruDetails)
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})

	rl := async.NewRunLoop()
	called := false
	cb := async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		called = true
		require.NoError(t, err)
		require.IsType(t, &kvrpcpb.PrewriteResponse{}, resp.Resp)
		require.Equal(t, uint64(1), resp.GetExecDetailsV2().GetRuV2().GetWriteRpcCount())
	})

	cli.SendRequestAsync(sendCtx, addr, req, cb)
	rl.Exec(ctx)
	require.True(t, called)

	expected := (weights.ResourceManagerWriteCntTiKV + weights.TiKVKVEngineCacheMiss) * weights.RUScale
	require.InDelta(t, expected, ruDetails.TiKVRUV2(), 1e-9)
	drained := ruDetails.DrainRUV2()
	require.NotNil(t, drained)
	require.Equal(t, uint64(1), drained.GetWriteRpcCount())
	require.Equal(t, uint64(1), drained.GetKvEngineCacheMiss())
	require.Nil(t, ruDetails.DrainRUV2())

	bypassDetails := util.NewRUDetails()
	bypassCtx := context.WithValue(ctx, util.RUDetailsCtxKey, bypassDetails)
	bypassReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	bypassReq.RequestSource = "xxx_internal_others"

	rl = async.NewRunLoop()
	called = false
	cb = async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		called = true
		require.NoError(t, err)
		require.IsType(t, &kvrpcpb.PrewriteResponse{}, resp.Resp)
		require.Zero(t, resp.GetExecDetailsV2().GetRuV2().GetWriteRpcCount())
	})

	cli.SendRequestAsync(bypassCtx, addr, bypassReq, cb)
	rl.Exec(ctx)
	require.True(t, called)
	require.Zero(t, bypassDetails.TiKVRUV2())
	require.Nil(t, bypassDetails.DrainRUV2())
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
			cancel()
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
	defer cli.Close()

	handleCtx, releaseHandle := context.WithCancel(context.Background())
	defer releaseHandle()

	var received atomic.Bool
	handleEntered := make(chan struct{}, 1)
	handle := func(req *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
		received.Store(true)
		select {
		case handleEntered <- struct{}{}:
		default:
		}
		<-handleCtx.Done()
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

	select {
	case <-handleEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("request is not sent to mock server")
	}

	cli.Close()
	releaseHandle()

	execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := rl.Exec(execCtx)
	require.NoError(t, err)
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
