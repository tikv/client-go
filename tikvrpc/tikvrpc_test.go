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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tikvrpc/tikvrpc_test.go
//

// Copyright 2020 PingCAP, Inc.
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

package tikvrpc

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/util"
	"google.golang.org/grpc/metadata"
)

func TestBatchResponse(t *testing.T) {
	resp := &tikvpb.BatchCommandsResponse_Response{}
	batchResp, err := FromBatchCommandsResponse(resp)
	assert.Nil(t, batchResp)
	assert.NotNil(t, err)
}

func TestDefaultRequestOrigin(t *testing.T) {
	previousOrigin := GetDefaultRequestOrigin()
	SetDefaultRequestOrigin(kvrpcpb.RequestOrigin_RequestOriginTiDB)
	t.Cleanup(func() {
		SetDefaultRequestOrigin(previousOrigin)
	})

	req := NewRequest(CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{})
	require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginTiDB, req.GetRequestOrigin())

	req = NewRequest(CmdGet, &kvrpcpb.GetRequest{})
	require.True(t, AttachContext(req, kvrpcpb.Context{}))
	require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginTiDB, req.GetRequestOrigin())
	require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginTiDB, req.Get().GetContext().GetRequestOrigin())

	for _, tc := range []struct {
		name   string
		req    *Request
		origin func(*Request) kvrpcpb.RequestOrigin
	}{
		{
			name: "scan_lock",
			req:  NewRequest(CmdScanLock, &kvrpcpb.ScanLockRequest{}),
			origin: func(req *Request) kvrpcpb.RequestOrigin {
				return req.ScanLock().GetContext().GetRequestOrigin()
			},
		},
		{
			name: "cleanup",
			req:  NewRequest(CmdCleanup, &kvrpcpb.CleanupRequest{}),
			origin: func(req *Request) kvrpcpb.RequestOrigin {
				return req.Cleanup().GetContext().GetRequestOrigin()
			},
		},
		{
			name: "check_txn_status",
			req:  NewRequest(CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{}),
			origin: func(req *Request) kvrpcpb.RequestOrigin {
				return req.CheckTxnStatus().GetContext().GetRequestOrigin()
			},
		},
		{
			name: "check_secondary_locks",
			req:  NewRequest(CmdCheckSecondaryLocks, &kvrpcpb.CheckSecondaryLocksRequest{}),
			origin: func(req *Request) kvrpcpb.RequestOrigin {
				return req.CheckSecondaryLocks().GetContext().GetRequestOrigin()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, AttachContext(tc.req, kvrpcpb.Context{}))
			require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginTiDB, tc.origin(tc.req))
		})
	}

	SetDefaultRequestOrigin(kvrpcpb.RequestOrigin_RequestOriginUnknown)
	req = NewRequest(CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{
		RequestOrigin: kvrpcpb.RequestOrigin_RequestOriginTiDB,
	})
	require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginTiDB, req.GetRequestOrigin())
}

// useDefaultTxnProtocolVersion installs a process-wide transaction protocol
// version for the duration of the test and restores the previous one afterwards.
// Tests that call it must not run in parallel, because the setting is global.
func useDefaultTxnProtocolVersion(t *testing.T, version kvrpcpb.TxnProtocolVersion) {
	t.Helper()
	previousVersion := GetDefaultTxnProtocolVersion()
	require.NoError(t, SetDefaultTxnProtocolVersion(version))
	t.Cleanup(func() {
		require.NoError(t, SetDefaultTxnProtocolVersion(previousVersion))
	})
}

func TestDefaultTxnProtocolVersion(t *testing.T) {
	require.Equal(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, GetDefaultTxnProtocolVersion())
	previous := GetDefaultTxnProtocolVersion()
	t.Cleanup(func() { require.NoError(t, SetDefaultTxnProtocolVersion(previous)) })

	for _, version := range []kvrpcpb.TxnProtocolVersion{
		kvrpcpb.TxnProtocolVersion_TXN_VER_LEGACY,
		kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING,
		kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK,
	} {
		require.NoError(t, SetDefaultTxnProtocolVersion(version))
		require.Equal(t, version, GetDefaultTxnProtocolVersion())
	}
}

func TestSetDefaultTxnProtocolVersionRejectsOutOfRangeValues(t *testing.T) {
	useDefaultTxnProtocolVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING)

	for _, version := range []kvrpcpb.TxnProtocolVersion{
		-1,
		clientMaxSupportedTxnProtocolVersion + 1,
	} {
		t.Run(version.String(), func(t *testing.T) {
			require.Error(t, SetDefaultTxnProtocolVersion(version))
			require.Equal(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING, GetDefaultTxnProtocolVersion())
		})
	}
}

// allCmdTypes lists every concrete CmdType this package knows about. Tests that
// classify commands must cover this list exactly, so adding a new CmdType forces
// an explicit protected/required decision instead of silently inheriting the
// unprotected default.
func allCmdTypes() []CmdType {
	return []CmdType{
		CmdGet, CmdScan, CmdPrewrite, CmdCommit, CmdCleanup, CmdBatchGet, CmdBatchRollback,
		CmdScanLock, CmdResolveLock, CmdGC, CmdDeleteRange, CmdPessimisticLock,
		CmdPessimisticRollback, CmdTxnHeartBeat, CmdCheckTxnStatus, CmdCheckSecondaryLocks,
		CmdFlashbackToVersion, CmdPrepareFlashbackToVersion, CmdFlush, CmdBufferBatchGet,
		CmdRawGet, CmdRawBatchGet, CmdRawPut, CmdRawBatchPut, CmdRawDelete, CmdRawBatchDelete,
		CmdRawDeleteRange, CmdRawScan, CmdRawGetKeyTTL, CmdRawCompareAndSwap, CmdRawChecksum,
		CmdUnsafeDestroyRange, CmdRegisterLockObserver, CmdCheckLockObserver, CmdRemoveLockObserver,
		CmdPhysicalScanLock, CmdStoreSafeTS, CmdLockWaitInfo, CmdGetHealthFeedback,
		CmdBroadcastTxnStatus, CmdCop, CmdCopStream, CmdBatchCop, CmdMPPTask, CmdMPPConn,
		CmdMPPCancel, CmdMPPAlive, CmdMvccGetByKey, CmdMvccGetByStartTs, CmdSplitRegion,
		CmdDebugGetRegionProperties, CmdCompact, CmdGetTiFlashSystemTable, CmdEmpty,
	}
}

func TestNewRequestDoesNotInjectTxnProtocolVersion(t *testing.T) {
	useDefaultTxnProtocolVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	req := NewRequest(CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{RegionId: 1})
	require.Zero(t, req.GetTxnProtocolVersion())
	require.Equal(t, uint64(1), req.GetRegionId())

	// A plain AttachContext without a prepared attempt uses the unknown [0, 0]
	// range for protected commands, not the process ceiling.
	req = NewRequest(CmdGet, &kvrpcpb.GetRequest{})
	require.True(t, AttachContext(req, kvrpcpb.Context{RegionId: 2}))
	require.Equal(t, txnProtocolVersionLegacy, req.GetTxnProtocolVersion())
	require.Equal(t, txnProtocolVersionLegacy, req.Get().GetContext().GetTxnProtocolVersion())
}

// TestCallerSuppliedVersionNeverReachesWire pins that a version written by the
// caller, on the wrapper context or on the concrete request context, is never a
// trusted declaration and is cleared by the transport, for protected and
// unprotected commands alike.
func TestCallerSuppliedVersionNeverReachesWire(t *testing.T) {
	useDefaultTxnProtocolVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK)

	prewrite := &kvrpcpb.PrewriteRequest{Context: &kvrpcpb.Context{
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK),
	}}
	req := NewRequest(CmdPrewrite, prewrite, kvrpcpb.Context{
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK),
	})
	// The wrapper context is cleared at construction time.
	require.Zero(t, req.GetTxnProtocolVersion())
	require.True(t, AttachContext(req, kvrpcpb.Context{RegionId: 1}))
	require.Zero(t, req.GetTxnProtocolVersion())
	require.Zero(t, req.Prewrite().GetContext().GetTxnProtocolVersion())

	rawPut := &kvrpcpb.RawPutRequest{Context: &kvrpcpb.Context{
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK),
	}}
	rawReq := NewRequest(CmdRawPut, rawPut, kvrpcpb.Context{
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK),
	})
	require.True(t, AttachContext(rawReq, kvrpcpb.Context{RegionId: 1}))
	require.Zero(t, rawReq.GetTxnProtocolVersion())
	require.Zero(t, rawReq.RawPut().GetContext().GetTxnProtocolVersion())
}

// TestAttachContextPatchesProvidedVersion verifies that tikvrpc only patches
// the context selected by its caller. Standard transports determine the version
// through their internal policy before calling this helper.
func TestAttachContextPatchesProvidedVersion(t *testing.T) {

	for _, tc := range []struct {
		name string
		req  *Request
		// ctx reads the concrete request context of the command.
		ctx func(*Request) *kvrpcpb.Context
	}{
		{
			name: "get",
			req:  NewRequest(CmdGet, &kvrpcpb.GetRequest{}),
			ctx:  func(req *Request) *kvrpcpb.Context { return req.Get().GetContext() },
		},
		{
			name: "scan_lock",
			req:  NewRequest(CmdScanLock, &kvrpcpb.ScanLockRequest{}),
			ctx:  func(req *Request) *kvrpcpb.Context { return req.ScanLock().GetContext() },
		},
		{
			name: "check_secondary_locks",
			req:  NewRequest(CmdCheckSecondaryLocks, &kvrpcpb.CheckSecondaryLocksRequest{}),
			ctx:  func(req *Request) *kvrpcpb.Context { return req.CheckSecondaryLocks().GetContext() },
		},
		{
			name: "cop",
			req:  NewRequest(CmdCop, &coprocessor.Request{}),
			ctx:  func(req *Request) *kvrpcpb.Context { return req.Cop().GetContext() },
		},
		{
			name: "cop_stream",
			req:  NewRequest(CmdCopStream, &coprocessor.Request{}),
			ctx:  func(req *Request) *kvrpcpb.Context { return req.Cop().GetContext() },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, AttachContext(tc.req, kvrpcpb.Context{RegionId: 1}))
			require.Equal(t, txnProtocolVersionLegacy, tc.ctx(tc.req).GetTxnProtocolVersion())
		})
	}

	// A caller-selected version is patched unchanged. This low-level helper is
	// not the transaction-protocol selection boundary.
	req := NewRequest(CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{})
	require.True(t, AttachContext(req, kvrpcpb.Context{
		RegionId:           1,
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING),
	}))
	require.Equal(t, uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING), req.UnsafeDestroyRange().GetContext().GetTxnProtocolVersion())
}

func TestOriginAndTxnProtocolVersionAreInjectedIndependently(t *testing.T) {
	previousOrigin := GetDefaultRequestOrigin()
	SetDefaultRequestOrigin(kvrpcpb.RequestOrigin_RequestOriginTiCDC)
	t.Cleanup(func() {
		SetDefaultRequestOrigin(previousOrigin)
	})
	useDefaultTxnProtocolVersion(t, kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_INCOMPATIBLE_ERROR_HANDLING)

	// Origin still follows the Unknown-only rule, independently of anything the
	// caller wrote into the version field.
	req := NewRequest(CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_LEGACY),
	})
	require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginTiCDC, req.GetRequestOrigin())
	require.Zero(t, req.GetTxnProtocolVersion())

	req = NewRequest(CmdGet, &kvrpcpb.GetRequest{}, kvrpcpb.Context{
		RequestOrigin:      kvrpcpb.RequestOrigin_RequestOriginBR,
		TxnProtocolVersion: uint32(kvrpcpb.TxnProtocolVersion_TXN_VER_SUPPORT_SHARED_LOCK),
	})
	require.Equal(t, kvrpcpb.RequestOrigin_RequestOriginBR, req.GetRequestOrigin())
	// The caller-supplied version is still cleared: only the controlled send path
	// may declare one.
	require.Zero(t, req.GetTxnProtocolVersion())
}

func TestAttachContextSetsRequestContext(t *testing.T) {
	rpcCtx := kvrpcpb.Context{
		RegionId:     123,
		ApiVersion:   kvrpcpb.APIVersion_V2,
		Keyspace:     &kvrpcpb.Context_KeyspaceId{KeyspaceId: 456},
		KeyspaceName: "test-keyspace",
	}
	nextRPCtx := kvrpcpb.Context{
		RegionId:     789,
		ApiVersion:   kvrpcpb.APIVersion_V2,
		Keyspace:     &kvrpcpb.Context_KeyspaceId{KeyspaceId: 101112},
		KeyspaceName: "next-test-keyspace",
	}

	for _, tc := range []struct {
		name string
		req  *Request
		ctx  func(*Request) *kvrpcpb.Context
	}{
		{
			name: "get",
			req:  NewRequest(CmdGet, &kvrpcpb.GetRequest{}),
			ctx: func(req *Request) *kvrpcpb.Context {
				return req.Get().GetContext()
			},
		},
		{
			name: "lock_wait_info",
			req:  NewRequest(CmdLockWaitInfo, &kvrpcpb.GetLockWaitInfoRequest{}),
			ctx: func(req *Request) *kvrpcpb.Context {
				return req.LockWaitInfo().GetContext()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, AttachContext(tc.req, rpcCtx))
			require.Equal(t, rpcCtx.GetRegionId(), tc.ctx(tc.req).GetRegionId())
			require.Equal(t, rpcCtx.GetApiVersion(), tc.ctx(tc.req).GetApiVersion())
			require.Equal(t, rpcCtx.GetKeyspaceId(), tc.ctx(tc.req).GetKeyspaceId())
			require.Equal(t, rpcCtx.GetKeyspaceName(), tc.ctx(tc.req).GetKeyspaceName())

			oldReq := tc.req.Req
			require.True(t, AttachContext(tc.req, nextRPCtx))
			require.NotSame(t, oldReq, tc.req.Req)
			require.Equal(t, nextRPCtx.GetRegionId(), tc.ctx(tc.req).GetRegionId())
			require.Equal(t, nextRPCtx.GetApiVersion(), tc.ctx(tc.req).GetApiVersion())
			require.Equal(t, nextRPCtx.GetKeyspaceId(), tc.ctx(tc.req).GetKeyspaceId())
			require.Equal(t, nextRPCtx.GetKeyspaceName(), tc.ctx(tc.req).GetKeyspaceName())
		})
	}
}

// https://github.com/pingcap/tidb/issues/51921
func TestTiDB51921(t *testing.T) {
	for _, r := range []*Request{
		NewRequest(CmdGet, &kvrpcpb.GetRequest{}),
		NewRequest(CmdScan, &kvrpcpb.ScanRequest{}),
		NewRequest(CmdPrewrite, &kvrpcpb.PrewriteRequest{}),
		NewRequest(CmdPessimisticLock, &kvrpcpb.PessimisticLockRequest{}),
		NewRequest(CmdPessimisticRollback, &kvrpcpb.PessimisticRollbackRequest{}),
		NewRequest(CmdCommit, &kvrpcpb.CommitRequest{}),
		NewRequest(CmdCleanup, &kvrpcpb.CleanupRequest{}),
		NewRequest(CmdBatchGet, &kvrpcpb.BatchGetRequest{}),
		NewRequest(CmdBatchRollback, &kvrpcpb.BatchRollbackRequest{}),
		NewRequest(CmdScanLock, &kvrpcpb.ScanLockRequest{}),
		NewRequest(CmdResolveLock, &kvrpcpb.ResolveLockRequest{}),
		NewRequest(CmdGC, &kvrpcpb.GCRequest{}),
		NewRequest(CmdDeleteRange, &kvrpcpb.DeleteRangeRequest{}),
		NewRequest(CmdRawGet, &kvrpcpb.RawGetRequest{}),
		NewRequest(CmdRawBatchGet, &kvrpcpb.RawBatchGetRequest{}),
		NewRequest(CmdRawPut, &kvrpcpb.RawPutRequest{}),
		NewRequest(CmdRawBatchPut, &kvrpcpb.RawBatchPutRequest{}),
		NewRequest(CmdRawDelete, &kvrpcpb.RawDeleteRequest{}),
		NewRequest(CmdRawBatchDelete, &kvrpcpb.RawBatchDeleteRequest{}),
		NewRequest(CmdRawDeleteRange, &kvrpcpb.RawDeleteRangeRequest{}),
		NewRequest(CmdRawScan, &kvrpcpb.RawScanRequest{}),
		NewRequest(CmdRawGetKeyTTL, &kvrpcpb.RawGetKeyTTLRequest{}),
		NewRequest(CmdRawCompareAndSwap, &kvrpcpb.RawCASRequest{}),
		NewRequest(CmdRawChecksum, &kvrpcpb.RawChecksumRequest{}),
		NewRequest(CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{}),
		NewRequest(CmdRegisterLockObserver, &kvrpcpb.RegisterLockObserverRequest{}),
		NewRequest(CmdCheckLockObserver, &kvrpcpb.CheckLockObserverRequest{}),
		NewRequest(CmdRemoveLockObserver, &kvrpcpb.RemoveLockObserverRequest{}),
		NewRequest(CmdPhysicalScanLock, &kvrpcpb.PhysicalScanLockRequest{}),
		NewRequest(CmdCop, &coprocessor.Request{}),
		NewRequest(CmdCopStream, &coprocessor.Request{}),
		NewRequest(CmdBatchCop, &coprocessor.BatchRequest{}),
		NewRequest(CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{}),
		NewRequest(CmdMvccGetByStartTs, &kvrpcpb.MvccGetByStartTsRequest{}),
		NewRequest(CmdSplitRegion, &kvrpcpb.SplitRegionRequest{}),
		NewRequest(CmdTxnHeartBeat, &kvrpcpb.TxnHeartBeatRequest{}),
		NewRequest(CmdCheckTxnStatus, &kvrpcpb.CheckTxnStatusRequest{}),
		NewRequest(CmdCheckSecondaryLocks, &kvrpcpb.CheckSecondaryLocksRequest{}),
		NewRequest(CmdFlashbackToVersion, &kvrpcpb.FlashbackToVersionRequest{}),
		NewRequest(CmdPrepareFlashbackToVersion, &kvrpcpb.PrepareFlashbackToVersionRequest{}),
		NewRequest(CmdFlush, &kvrpcpb.FlushRequest{}),
		NewRequest(CmdBufferBatchGet, &kvrpcpb.BufferBatchGetRequest{}),
	} {
		req := r
		t.Run(fmt.Sprintf("%s#%d", req.Type.String(), req.Type), func(t *testing.T) {
			if req.ToBatchCommandsRequest() == nil {
				t.Skipf("%s doesn't support batch commands", req.Type.String())
			}
			done := make(chan struct{})
			cmds := make(chan *tikvpb.BatchCommandsRequest_Request, 8)
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						close(cmds)
						return
					default:
						// mock relocate and retry
						AttachContext(req, kvrpcpb.Context{RegionId: rand.Uint64()})
						cmds <- req.ToBatchCommandsRequest()
					}
				}
			}()
			go func() {
				defer wg.Done()
				for cmd := range cmds {
					// mock send and marshal in batch-send-loop
					cmd.Marshal()
				}
			}()

			time.Sleep(time.Second / 4)
			close(done)
			wg.Wait()
		})
	}
}

type mockCoprocessorStreamClient struct {
	resp *coprocessor.Response
	err  error
}

func (*mockCoprocessorStreamClient) Header() (metadata.MD, error) { return nil, nil }
func (*mockCoprocessorStreamClient) Trailer() metadata.MD         { return nil }
func (*mockCoprocessorStreamClient) CloseSend() error             { return nil }
func (*mockCoprocessorStreamClient) Context() context.Context     { return context.Background() }
func (*mockCoprocessorStreamClient) SendMsg(any) error            { return nil }
func (*mockCoprocessorStreamClient) RecvMsg(any) error            { return nil }
func (c *mockCoprocessorStreamClient) Recv() (*coprocessor.Response, error) {
	return c.resp, c.err
}

func TestCopStreamResponseRecvBypass(t *testing.T) {
	original := config.GetGlobalConfig()
	t.Cleanup(func() {
		config.StoreGlobalConfig(original)
	})

	cfg := config.DefaultConfig()
	cfg.TiKVClient.RUV2 = config.DefaultRUV2TiKVConfig()
	config.StoreGlobalConfig(&cfg)

	makeResponse := func() *coprocessor.Response {
		return &coprocessor.Response{
			ExecDetailsV2: &kvrpcpb.ExecDetailsV2{
				RuV2: &kvrpcpb.RUV2{
					KvEngineCacheMiss:            1,
					StorageProcessedKeysGet:      2,
					StorageProcessedKeysBatchGet: 3,
				},
			},
		}
	}

	t.Run("charged stream updates tikv ruv2", func(t *testing.T) {
		ruDetails := util.NewRUDetails()
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)
		resp := &CopStreamResponse{
			Tikv_CoprocessorStreamClient: &mockCoprocessorStreamClient{resp: makeResponse()},
			Ctx:                          ctx,
			CountRPC:                     true,
		}
		_, err := resp.Recv()
		require.NoError(t, err)
		require.Greater(t, ruDetails.TiKVRUV2(), 0.0)
		require.Equal(t, uint64(1), resp.Response.GetExecDetailsV2().GetRuV2().GetReadRpcCount())
		require.Equal(t, uint64(2), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysGet())
		require.Equal(t, uint64(3), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysBatchGet())
		drained := ruDetails.DrainRUV2()
		require.NotNil(t, drained)
		require.Equal(t, uint64(1), drained.GetReadRpcCount())
		require.Equal(t, uint64(2), drained.GetStorageProcessedKeysGet())
		require.Equal(t, uint64(3), drained.GetStorageProcessedKeysBatchGet())
		require.Nil(t, ruDetails.DrainRUV2())
	})

	t.Run("bypass stream skips tikv ruv2", func(t *testing.T) {
		ruDetails := util.NewRUDetails()
		ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)
		resp := &CopStreamResponse{
			Tikv_CoprocessorStreamClient: &mockCoprocessorStreamClient{resp: makeResponse()},
			Ctx:                          ctx,
			Bypass:                       true,
			CountRPC:                     true,
		}
		_, err := resp.Recv()
		require.NoError(t, err)
		require.Zero(t, ruDetails.TiKVRUV2())
		require.Zero(t, resp.Response.GetExecDetailsV2().GetRuV2().GetReadRpcCount())
		require.Equal(t, uint64(2), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysGet())
		require.Equal(t, uint64(3), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysBatchGet())
		require.Nil(t, ruDetails.DrainRUV2())
	})
}
