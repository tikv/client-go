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
		raw := ruDetails.RUV2()
		require.NotNil(t, raw)
		require.Equal(t, uint64(1), raw.GetKvEngineCacheMiss())
		require.Equal(t, uint64(1), raw.GetReadRpcCount())
		require.Equal(t, uint64(2), raw.GetStorageProcessedKeysGet())
		require.Equal(t, uint64(3), raw.GetStorageProcessedKeysBatchGet())
		require.Equal(t, uint64(1), resp.Response.GetExecDetailsV2().GetRuV2().GetReadRpcCount())
		require.Equal(t, uint64(2), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysGet())
		require.Equal(t, uint64(3), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysBatchGet())
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
		require.Nil(t, ruDetails.RUV2())
		require.Zero(t, resp.Response.GetExecDetailsV2().GetRuV2().GetReadRpcCount())
		require.Equal(t, uint64(2), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysGet())
		require.Equal(t, uint64(3), resp.Response.GetExecDetailsV2().GetRuV2().GetStorageProcessedKeysBatchGet())
	})
}
