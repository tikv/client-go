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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/assert"
)

func TestBatchResponse(t *testing.T) {
	resp := &tikvpb.BatchCommandsResponse_Response{}
	batchResp, err := FromBatchCommandsResponse(resp)
	assert.Nil(t, batchResp)
	assert.NotNil(t, err)
}

func TestKeepOnlyActiveWithHardDeadline(t *testing.T) {
	var canceled int32
	activeLease := &Lease{Cancel: func() {}}
	activeLease.SetHardTimeout(time.Hour)
	expiredLease := &Lease{Cancel: func() { atomic.AddInt32(&canceled, 1) }}
	atomic.StoreInt64(&expiredLease.hardDeadline, time.Now().Add(-time.Second).UnixNano())

	leases := []*Lease{activeLease, expiredLease}
	active := keepOnlyActive(leases, time.Now().UnixNano())

	assert.Equal(t, []*Lease{activeLease}, active)
	assert.Nil(t, leases[1])
	assert.Equal(t, int32(1), atomic.LoadInt32(&canceled))
}

func TestKeepOnlyActiveHardDeadlineOverridesRecvDeadline(t *testing.T) {
	var canceled int32
	now := time.Now().UnixNano()
	lease := &Lease{Cancel: func() { atomic.AddInt32(&canceled, 1) }}
	atomic.StoreInt64(&lease.deadline, now+int64(time.Hour))
	atomic.StoreInt64(&lease.hardDeadline, now-1)

	leases := []*Lease{lease}
	active := keepOnlyActive(leases, now)

	assert.Empty(t, active)
	assert.Nil(t, leases[0])
	assert.Equal(t, int32(1), atomic.LoadInt32(&canceled))
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
