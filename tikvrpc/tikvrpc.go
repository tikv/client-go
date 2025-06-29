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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tikvrpc/tikvrpc.go
//

// Copyright 2017 PingCAP, Inc.
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
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdCleanup
	CmdBatchGet
	CmdBatchRollback
	CmdScanLock
	CmdResolveLock
	CmdGC
	CmdDeleteRange
	CmdPessimisticLock
	CmdPessimisticRollback
	CmdTxnHeartBeat
	CmdCheckTxnStatus
	CmdCheckSecondaryLocks
	CmdFlashbackToVersion
	CmdPrepareFlashbackToVersion
	CmdFlush
	CmdBufferBatchGet

	CmdRawGet CmdType = 256 + iota
	CmdRawBatchGet
	CmdRawPut
	CmdRawBatchPut
	CmdRawDelete
	CmdRawBatchDelete
	CmdRawDeleteRange
	CmdRawScan
	CmdRawGetKeyTTL
	CmdRawCompareAndSwap
	CmdRawChecksum

	CmdUnsafeDestroyRange

	CmdRegisterLockObserver
	CmdCheckLockObserver
	CmdRemoveLockObserver
	CmdPhysicalScanLock

	CmdStoreSafeTS
	CmdLockWaitInfo

	CmdGetHealthFeedback
	CmdBroadcastTxnStatus

	CmdCop CmdType = 512 + iota
	CmdCopStream
	CmdBatchCop
	CmdMPPTask   // TODO: These non TiKV RPCs should be moved out of TiKV client
	CmdMPPConn   // TODO: These non TiKV RPCs should be moved out of TiKV client
	CmdMPPCancel // TODO: These non TiKV RPCs should be moved out of TiKV client
	CmdMPPAlive  // TODO: These non TiKV RPCs should be moved out of TiKV client

	CmdMvccGetByKey CmdType = 1024 + iota
	CmdMvccGetByStartTs
	CmdSplitRegion

	CmdDebugGetRegionProperties CmdType = 2048 + iota
	CmdCompact                          // TODO: These non TiKV RPCs should be moved out of TiKV client
	CmdGetTiFlashSystemTable            // TODO: These non TiKV RPCs should be moved out of TiKV client

	CmdEmpty CmdType = 3072 + iota
)

// CmdType aliases.
const (
	CmdGetKeyTTL = CmdRawGetKeyTTL
)

func (t CmdType) String() string {
	switch t {
	case CmdGet:
		return "Get"
	case CmdScan:
		return "Scan"
	case CmdPrewrite:
		return "Prewrite"
	case CmdPessimisticLock:
		return "PessimisticLock"
	case CmdPessimisticRollback:
		return "PessimisticRollback"
	case CmdCommit:
		return "Commit"
	case CmdCleanup:
		return "Cleanup"
	case CmdBatchGet:
		return "BatchGet"
	case CmdBatchRollback:
		return "BatchRollback"
	case CmdScanLock:
		return "ScanLock"
	case CmdResolveLock:
		return "ResolveLock"
	case CmdGC:
		return "GC"
	case CmdDeleteRange:
		return "DeleteRange"
	case CmdRawGet:
		return "RawGet"
	case CmdRawBatchGet:
		return "RawBatchGet"
	case CmdRawPut:
		return "RawPut"
	case CmdRawBatchPut:
		return "RawBatchPut"
	case CmdRawDelete:
		return "RawDelete"
	case CmdRawBatchDelete:
		return "RawBatchDelete"
	case CmdRawDeleteRange:
		return "RawDeleteRange"
	case CmdRawScan:
		return "RawScan"
	case CmdRawChecksum:
		return "RawChecksum"
	case CmdRawGetKeyTTL:
		return "RawGetKeyTTL"
	case CmdRawCompareAndSwap:
		return "RawCompareAndSwap"
	case CmdUnsafeDestroyRange:
		return "UnsafeDestroyRange"
	case CmdRegisterLockObserver:
		return "RegisterLockObserver"
	case CmdCheckLockObserver:
		return "CheckLockObserver"
	case CmdRemoveLockObserver:
		return "RemoveLockObserver"
	case CmdPhysicalScanLock:
		return "PhysicalScanLock"
	case CmdCop:
		return "Cop"
	case CmdCopStream:
		return "CopStream"
	case CmdBatchCop:
		return "BatchCop"
	case CmdMPPTask:
		return "DispatchMPPTask"
	case CmdMPPConn:
		return "EstablishMPPConnection"
	case CmdMPPCancel:
		return "CancelMPPTask"
	case CmdMPPAlive:
		return "MPPAlive"
	case CmdMvccGetByKey:
		return "MvccGetByKey"
	case CmdMvccGetByStartTs:
		return "MvccGetByStartTS"
	case CmdSplitRegion:
		return "SplitRegion"
	case CmdCheckTxnStatus:
		return "CheckTxnStatus"
	case CmdCheckSecondaryLocks:
		return "CheckSecondaryLocks"
	case CmdDebugGetRegionProperties:
		return "DebugGetRegionProperties"
	case CmdCompact:
		return "Compact"
	case CmdTxnHeartBeat:
		return "TxnHeartBeat"
	case CmdStoreSafeTS:
		return "StoreSafeTS"
	case CmdLockWaitInfo:
		return "LockWaitInfo"
	case CmdGetHealthFeedback:
		return "GetHealthFeedback"
	case CmdBroadcastTxnStatus:
		return "BroadcastTxnStatus"
	case CmdFlashbackToVersion:
		return "FlashbackToVersion"
	case CmdPrepareFlashbackToVersion:
		return "PrepareFlashbackToVersion"
	case CmdGetTiFlashSystemTable:
		return "GetTiFlashSystemTable"
	case CmdFlush:
		return "Flush"
	case CmdBufferBatchGet:
		return "BufferBatchGet"
	}
	return "Unknown"
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	Type CmdType
	// Req is one of the request type defined in kvrpcpb.
	//
	// WARN: It may be read concurrently in batch-send-loop, so you should ONLY modify it via `AttachContext`,
	// otherwise there could be a risk of data race.
	Req interface{}
	kvrpcpb.Context
	ReadReplicaScope string
	// remove txnScope after tidb removed txnScope
	TxnScope        string
	ReplicaReadType kv.ReplicaReadType // different from `kvrpcpb.Context.ReplicaRead`
	ReplicaReadSeed *uint32            // pointer to follower read seed in snapshot/coprocessor
	StoreTp         EndpointType
	// ForwardedHost is the address of a store which will handle the request. It's different from
	// the address the request sent to.
	// If it's not empty, the store which receive the request will forward it to
	// the forwarded host. It's useful when network partition occurs.
	ForwardedHost string
	// ReplicaNumber is the number of current replicas, which is used to calculate the RU cost.
	ReplicaNumber int64
	// The initial read type, note this will be assigned in the first try, no need to set it outside the client.
	ReadType string
	// InputRequestSource is the input source of the request, if it's not empty, the final RequestSource sent to store will be attached with the retry info.
	InputRequestSource string
	// AccessLocationAttr indicates the request is sent to a different zone.
	AccessLocation kv.AccessLocationType
	// rev represents the revision of the request, it's increased when `Req.Context` gets patched.
	rev uint32
}

// NewRequest returns new kv rpc request.
func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request {
	if len(ctxs) > 0 {
		return &Request{
			Type:    typ,
			Req:     pointer,
			Context: ctxs[0],
		}
	}
	return &Request{
		Type: typ,
		Req:  pointer,
	}
}

// NewReplicaReadRequest returns new kv rpc request with replica read.
func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType kv.ReplicaReadType, replicaReadSeed *uint32, ctxs ...kvrpcpb.Context) *Request {
	req := NewRequest(typ, pointer, ctxs...)
	req.ReplicaRead = replicaReadType.IsFollowerRead()
	req.ReplicaReadType = replicaReadType
	req.ReplicaReadSeed = replicaReadSeed
	return req
}

func (req *Request) SetReplicaReadType(replicaReadType kv.ReplicaReadType) {
	if req == nil {
		return
	}
	req.ReplicaRead = replicaReadType.IsFollowerRead()
	req.ReplicaReadType = replicaReadType
}

// GetReplicaReadSeed returns ReplicaReadSeed pointer.
func (req *Request) GetReplicaReadSeed() *uint32 {
	if req != nil {
		return req.ReplicaReadSeed
	}
	return nil
}

// EnableStaleWithMixedReplicaRead enables stale read and set replica read type to mixed.
func (req *Request) EnableStaleWithMixedReplicaRead() {
	req.StaleRead = true
	req.ReplicaReadType = kv.ReplicaReadMixed
	req.ReplicaRead = false
}

// DisableStaleReadMeetLock is called when stale-read fallbacks to leader read after meeting key-is-locked error.
func (req *Request) DisableStaleReadMeetLock() {
	req.StaleRead = false
	req.ReplicaReadType = kv.ReplicaReadLeader
}

// IsGlobalStaleRead checks if the request is a global stale read request.
func (req *Request) IsGlobalStaleRead() bool {
	return req.ReadReplicaScope == oracle.GlobalTxnScope &&
		// remove txnScope after tidb remove it
		req.TxnScope == oracle.GlobalTxnScope &&
		req.GetStaleRead()
}

// IsDebugReq check whether the req is debug req.
func (req *Request) IsDebugReq() bool {
	switch req.Type {
	case CmdDebugGetRegionProperties:
		return true
	}
	return false
}

// IsInterruptible checks if the request can be interrupted when the query is killed.
func (req *Request) IsInterruptible() bool {
	switch req.Type {
	case CmdPessimisticRollback, CmdBatchRollback, CmdCommit:
		return false
	default:
		return true
	}
}

// Get returns GetRequest in request.
func (req *Request) Get() *kvrpcpb.GetRequest {
	return req.Req.(*kvrpcpb.GetRequest)
}

// Scan returns ScanRequest in request.
func (req *Request) Scan() *kvrpcpb.ScanRequest {
	return req.Req.(*kvrpcpb.ScanRequest)
}

// Prewrite returns PrewriteRequest in request.
func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest {
	return req.Req.(*kvrpcpb.PrewriteRequest)
}

// Commit returns CommitRequest in request.
func (req *Request) Commit() *kvrpcpb.CommitRequest {
	return req.Req.(*kvrpcpb.CommitRequest)
}

// Cleanup returns CleanupRequest in request.
func (req *Request) Cleanup() *kvrpcpb.CleanupRequest {
	return req.Req.(*kvrpcpb.CleanupRequest)
}

// BatchGet returns BatchGetRequest in request.
func (req *Request) BatchGet() *kvrpcpb.BatchGetRequest {
	return req.Req.(*kvrpcpb.BatchGetRequest)
}

// BatchRollback returns BatchRollbackRequest in request.
func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest {
	return req.Req.(*kvrpcpb.BatchRollbackRequest)
}

// ScanLock returns ScanLockRequest in request.
func (req *Request) ScanLock() *kvrpcpb.ScanLockRequest {
	return req.Req.(*kvrpcpb.ScanLockRequest)
}

// ResolveLock returns ResolveLockRequest in request.
func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest {
	return req.Req.(*kvrpcpb.ResolveLockRequest)
}

// GC returns GCRequest in request.
func (req *Request) GC() *kvrpcpb.GCRequest {
	return req.Req.(*kvrpcpb.GCRequest)
}

// DeleteRange returns DeleteRangeRequest in request.
func (req *Request) DeleteRange() *kvrpcpb.DeleteRangeRequest {
	return req.Req.(*kvrpcpb.DeleteRangeRequest)
}

// RawGet returns RawGetRequest in request.
func (req *Request) RawGet() *kvrpcpb.RawGetRequest {
	return req.Req.(*kvrpcpb.RawGetRequest)
}

// RawBatchGet returns RawBatchGetRequest in request.
func (req *Request) RawBatchGet() *kvrpcpb.RawBatchGetRequest {
	return req.Req.(*kvrpcpb.RawBatchGetRequest)
}

// RawPut returns RawPutRequest in request.
func (req *Request) RawPut() *kvrpcpb.RawPutRequest {
	return req.Req.(*kvrpcpb.RawPutRequest)
}

// RawBatchPut returns RawBatchPutRequest in request.
func (req *Request) RawBatchPut() *kvrpcpb.RawBatchPutRequest {
	return req.Req.(*kvrpcpb.RawBatchPutRequest)
}

// RawDelete returns PrewriteRequest in request.
func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest {
	return req.Req.(*kvrpcpb.RawDeleteRequest)
}

// RawBatchDelete returns RawBatchDeleteRequest in request.
func (req *Request) RawBatchDelete() *kvrpcpb.RawBatchDeleteRequest {
	return req.Req.(*kvrpcpb.RawBatchDeleteRequest)
}

// RawDeleteRange returns RawDeleteRangeRequest in request.
func (req *Request) RawDeleteRange() *kvrpcpb.RawDeleteRangeRequest {
	return req.Req.(*kvrpcpb.RawDeleteRangeRequest)
}

// RawScan returns RawScanRequest in request.
func (req *Request) RawScan() *kvrpcpb.RawScanRequest {
	return req.Req.(*kvrpcpb.RawScanRequest)
}

// UnsafeDestroyRange returns UnsafeDestroyRangeRequest in request.
func (req *Request) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeRequest {
	return req.Req.(*kvrpcpb.UnsafeDestroyRangeRequest)
}

// RawGetKeyTTL returns RawGetKeyTTLRequest in request.
func (req *Request) RawGetKeyTTL() *kvrpcpb.RawGetKeyTTLRequest {
	return req.Req.(*kvrpcpb.RawGetKeyTTLRequest)
}

// RawCompareAndSwap returns RawCASRequest in request.
func (req *Request) RawCompareAndSwap() *kvrpcpb.RawCASRequest {
	return req.Req.(*kvrpcpb.RawCASRequest)
}

// RawChecksum returns RawChecksumRequest in request.
func (req *Request) RawChecksum() *kvrpcpb.RawChecksumRequest {
	return req.Req.(*kvrpcpb.RawChecksumRequest)
}

// RegisterLockObserver returns RegisterLockObserverRequest in request.
func (req *Request) RegisterLockObserver() *kvrpcpb.RegisterLockObserverRequest {
	return req.Req.(*kvrpcpb.RegisterLockObserverRequest)
}

// CheckLockObserver returns CheckLockObserverRequest in request.
func (req *Request) CheckLockObserver() *kvrpcpb.CheckLockObserverRequest {
	return req.Req.(*kvrpcpb.CheckLockObserverRequest)
}

// RemoveLockObserver returns RemoveLockObserverRequest in request.
func (req *Request) RemoveLockObserver() *kvrpcpb.RemoveLockObserverRequest {
	return req.Req.(*kvrpcpb.RemoveLockObserverRequest)
}

// PhysicalScanLock returns PhysicalScanLockRequest in request.
func (req *Request) PhysicalScanLock() *kvrpcpb.PhysicalScanLockRequest {
	return req.Req.(*kvrpcpb.PhysicalScanLockRequest)
}

// Cop returns coprocessor request in request.
func (req *Request) Cop() *coprocessor.Request {
	return req.Req.(*coprocessor.Request)
}

// BatchCop returns BatchCop request in request.
func (req *Request) BatchCop() *coprocessor.BatchRequest {
	return req.Req.(*coprocessor.BatchRequest)
}

// DispatchMPPTask returns dispatch task request in request.
func (req *Request) DispatchMPPTask() *mpp.DispatchTaskRequest {
	return req.Req.(*mpp.DispatchTaskRequest)
}

// IsMPPAlive returns IsAlive request in request.
func (req *Request) IsMPPAlive() *mpp.IsAliveRequest {
	return req.Req.(*mpp.IsAliveRequest)
}

// EstablishMPPConn returns EstablishMPPConnectionRequest in request.
func (req *Request) EstablishMPPConn() *mpp.EstablishMPPConnectionRequest {
	return req.Req.(*mpp.EstablishMPPConnectionRequest)
}

// CancelMPPTask returns canceling task in request
func (req *Request) CancelMPPTask() *mpp.CancelTaskRequest {
	return req.Req.(*mpp.CancelTaskRequest)
}

// MvccGetByKey returns MvccGetByKeyRequest in request.
func (req *Request) MvccGetByKey() *kvrpcpb.MvccGetByKeyRequest {
	return req.Req.(*kvrpcpb.MvccGetByKeyRequest)
}

// MvccGetByStartTs returns MvccGetByStartTsRequest in request.
func (req *Request) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsRequest {
	return req.Req.(*kvrpcpb.MvccGetByStartTsRequest)
}

// SplitRegion returns SplitRegionRequest in request.
func (req *Request) SplitRegion() *kvrpcpb.SplitRegionRequest {
	return req.Req.(*kvrpcpb.SplitRegionRequest)
}

// PessimisticLock returns PessimisticLockRequest in request.
func (req *Request) PessimisticLock() *kvrpcpb.PessimisticLockRequest {
	return req.Req.(*kvrpcpb.PessimisticLockRequest)
}

// PessimisticRollback returns PessimisticRollbackRequest in request.
func (req *Request) PessimisticRollback() *kvrpcpb.PessimisticRollbackRequest {
	return req.Req.(*kvrpcpb.PessimisticRollbackRequest)
}

// DebugGetRegionProperties returns GetRegionPropertiesRequest in request.
func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest {
	return req.Req.(*debugpb.GetRegionPropertiesRequest)
}

// Compact returns CompactRequest in request.
func (req *Request) Compact() *kvrpcpb.CompactRequest {
	return req.Req.(*kvrpcpb.CompactRequest)
}

// GetTiFlashSystemTable returns TiFlashSystemTableRequest in request.
func (req *Request) GetTiFlashSystemTable() *kvrpcpb.TiFlashSystemTableRequest {
	return req.Req.(*kvrpcpb.TiFlashSystemTableRequest)
}

// Empty returns BatchCommandsEmptyRequest in request.
func (req *Request) Empty() *tikvpb.BatchCommandsEmptyRequest {
	return req.Req.(*tikvpb.BatchCommandsEmptyRequest)
}

// CheckTxnStatus returns CheckTxnStatusRequest in request.
func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest {
	return req.Req.(*kvrpcpb.CheckTxnStatusRequest)
}

// CheckSecondaryLocks returns CheckSecondaryLocksRequest in request.
func (req *Request) CheckSecondaryLocks() *kvrpcpb.CheckSecondaryLocksRequest {
	return req.Req.(*kvrpcpb.CheckSecondaryLocksRequest)
}

// TxnHeartBeat returns TxnHeartBeatRequest in request.
func (req *Request) TxnHeartBeat() *kvrpcpb.TxnHeartBeatRequest {
	return req.Req.(*kvrpcpb.TxnHeartBeatRequest)
}

// StoreSafeTS returns StoreSafeTSRequest in request.
func (req *Request) StoreSafeTS() *kvrpcpb.StoreSafeTSRequest {
	return req.Req.(*kvrpcpb.StoreSafeTSRequest)
}

// LockWaitInfo returns GetLockWaitInfoRequest in request.
func (req *Request) LockWaitInfo() *kvrpcpb.GetLockWaitInfoRequest {
	return req.Req.(*kvrpcpb.GetLockWaitInfoRequest)
}

// GetHealthFeedback returns GetHealthFeedbackRequest in request.
func (req *Request) GetHealthFeedback() *kvrpcpb.GetHealthFeedbackRequest {
	return req.Req.(*kvrpcpb.GetHealthFeedbackRequest)
}

func (req *Request) BroadcastTxnStatus() *kvrpcpb.BroadcastTxnStatusRequest {
	return req.Req.(*kvrpcpb.BroadcastTxnStatusRequest)
}

// FlashbackToVersion returns FlashbackToVersionRequest in request.
func (req *Request) FlashbackToVersion() *kvrpcpb.FlashbackToVersionRequest {
	return req.Req.(*kvrpcpb.FlashbackToVersionRequest)
}

// PrepareFlashbackToVersion returns PrepareFlashbackToVersionRequest in request.
func (req *Request) PrepareFlashbackToVersion() *kvrpcpb.PrepareFlashbackToVersionRequest {
	return req.Req.(*kvrpcpb.PrepareFlashbackToVersionRequest)
}

// Flush returns FlushRequest in request.
func (req *Request) Flush() *kvrpcpb.FlushRequest {
	return req.Req.(*kvrpcpb.FlushRequest)
}

// BufferBatchGet returns BufferBatchGetRequest in request.
func (req *Request) BufferBatchGet() *kvrpcpb.BufferBatchGetRequest {
	return req.Req.(*kvrpcpb.BufferBatchGetRequest)
}

// ToBatchCommandsRequest converts the request to an entry in BatchCommands request.
func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request {
	switch req.Type {
	case CmdGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Get{Get: req.Get()}}
	case CmdScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Scan{Scan: req.Scan()}}
	case CmdPrewrite:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Prewrite{Prewrite: req.Prewrite()}}
	case CmdCommit:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Commit{Commit: req.Commit()}}
	case CmdCleanup:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Cleanup{Cleanup: req.Cleanup()}}
	case CmdBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchGet{BatchGet: req.BatchGet()}}
	case CmdBatchRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchRollback{BatchRollback: req.BatchRollback()}}
	case CmdScanLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ScanLock{ScanLock: req.ScanLock()}}
	case CmdResolveLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ResolveLock{ResolveLock: req.ResolveLock()}}
	case CmdGC:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_GC{GC: req.GC()}}
	case CmdDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_DeleteRange{DeleteRange: req.DeleteRange()}}
	case CmdRawGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawGet{RawGet: req.RawGet()}}
	case CmdRawBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchGet{RawBatchGet: req.RawBatchGet()}}
	case CmdRawPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawPut{RawPut: req.RawPut()}}
	case CmdRawBatchPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchPut{RawBatchPut: req.RawBatchPut()}}
	case CmdRawDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDelete{RawDelete: req.RawDelete()}}
	case CmdRawBatchDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchDelete{RawBatchDelete: req.RawBatchDelete()}}
	case CmdRawDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDeleteRange{RawDeleteRange: req.RawDeleteRange()}}
	case CmdRawScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawScan{RawScan: req.RawScan()}}
	case CmdCop:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: req.Cop()}}
	case CmdPessimisticLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticLock{PessimisticLock: req.PessimisticLock()}}
	case CmdPessimisticRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticRollback{PessimisticRollback: req.PessimisticRollback()}}
	case CmdEmpty:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{Empty: req.Empty()}}
	case CmdCheckTxnStatus:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_CheckTxnStatus{CheckTxnStatus: req.CheckTxnStatus()}}
	case CmdCheckSecondaryLocks:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_CheckSecondaryLocks{CheckSecondaryLocks: req.CheckSecondaryLocks()}}
	case CmdTxnHeartBeat:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_TxnHeartBeat{TxnHeartBeat: req.TxnHeartBeat()}}
	case CmdFlashbackToVersion:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_FlashbackToVersion{FlashbackToVersion: req.FlashbackToVersion()}}
	case CmdPrepareFlashbackToVersion:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PrepareFlashbackToVersion{PrepareFlashbackToVersion: req.PrepareFlashbackToVersion()}}
	case CmdFlush:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Flush{Flush: req.Flush()}}
	case CmdBufferBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BufferBatchGet{BufferBatchGet: req.BufferBatchGet()}}
	case CmdGetHealthFeedback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_GetHealthFeedback{GetHealthFeedback: req.GetHealthFeedback()}}
	case CmdBroadcastTxnStatus:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BroadcastTxnStatus{BroadcastTxnStatus: req.BroadcastTxnStatus()}}
	}
	return nil
}

// GetSize return the data size of the request.
func (req *Request) GetSize() int {
	size := 0
	switch req.Type {
	case CmdGet:
		size = req.Get().Size()
	case CmdBatchGet:
		size = req.BatchGet().Size()
	case CmdScan:
		size = req.Scan().Size()
	case CmdCop:
		size = req.Cop().Size()
	case CmdPrewrite:
		size = req.Prewrite().Size()
	case CmdCommit:
		size = req.Commit().Size()
	case CmdPessimisticLock:
		size = req.PessimisticLock().Size()
	case CmdPessimisticRollback:
		size = req.PessimisticRollback().Size()
	case CmdBatchRollback:
		size = req.BatchRollback().Size()
	case CmdCheckSecondaryLocks:
		size = req.CheckSecondaryLocks().Size()
	case CmdScanLock:
		size = req.ScanLock().Size()
	case CmdResolveLock:
		size = req.ResolveLock().Size()
	case CmdFlush:
		size = req.Flush().Size()
	case CmdCheckTxnStatus:
		size = req.CheckTxnStatus().Size()
	case CmdMPPTask:
		size = req.DispatchMPPTask().Size()
	default:
		// ignore others
	}
	return size
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Resp interface{}
}

// ResponseExt likes Response but contains extra information.
type ResponseExt struct {
	Response
	// The address of the target store. When forwarding is enabled, it points to the target node handling the request
	// rather than the node forwarding the message.
	Addr string
}

// FromBatchCommandsResponse converts a BatchCommands response to Response.
func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) (*Response, error) {
	if res.GetCmd() == nil {
		return nil, errors.New("Unknown command response")
	}
	switch res := res.GetCmd().(type) {
	case *tikvpb.BatchCommandsResponse_Response_Get:
		return &Response{Resp: res.Get}, nil
	case *tikvpb.BatchCommandsResponse_Response_Scan:
		return &Response{Resp: res.Scan}, nil
	case *tikvpb.BatchCommandsResponse_Response_Prewrite:
		return &Response{Resp: res.Prewrite}, nil
	case *tikvpb.BatchCommandsResponse_Response_Commit:
		return &Response{Resp: res.Commit}, nil
	case *tikvpb.BatchCommandsResponse_Response_Cleanup:
		return &Response{Resp: res.Cleanup}, nil
	case *tikvpb.BatchCommandsResponse_Response_BatchGet:
		return &Response{Resp: res.BatchGet}, nil
	case *tikvpb.BatchCommandsResponse_Response_BatchRollback:
		return &Response{Resp: res.BatchRollback}, nil
	case *tikvpb.BatchCommandsResponse_Response_ScanLock:
		return &Response{Resp: res.ScanLock}, nil
	case *tikvpb.BatchCommandsResponse_Response_ResolveLock:
		return &Response{Resp: res.ResolveLock}, nil
	case *tikvpb.BatchCommandsResponse_Response_GC:
		return &Response{Resp: res.GC}, nil
	case *tikvpb.BatchCommandsResponse_Response_DeleteRange:
		return &Response{Resp: res.DeleteRange}, nil
	case *tikvpb.BatchCommandsResponse_Response_FlashbackToVersion:
		return &Response{Resp: res.FlashbackToVersion}, nil
	case *tikvpb.BatchCommandsResponse_Response_PrepareFlashbackToVersion:
		return &Response{Resp: res.PrepareFlashbackToVersion}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawGet:
		return &Response{Resp: res.RawGet}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawBatchGet:
		return &Response{Resp: res.RawBatchGet}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawPut:
		return &Response{Resp: res.RawPut}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawBatchPut:
		return &Response{Resp: res.RawBatchPut}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawDelete:
		return &Response{Resp: res.RawDelete}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawBatchDelete:
		return &Response{Resp: res.RawBatchDelete}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawDeleteRange:
		return &Response{Resp: res.RawDeleteRange}, nil
	case *tikvpb.BatchCommandsResponse_Response_RawScan:
		return &Response{Resp: res.RawScan}, nil
	case *tikvpb.BatchCommandsResponse_Response_Coprocessor:
		return &Response{Resp: res.Coprocessor}, nil
	case *tikvpb.BatchCommandsResponse_Response_PessimisticLock:
		return &Response{Resp: res.PessimisticLock}, nil
	case *tikvpb.BatchCommandsResponse_Response_PessimisticRollback:
		return &Response{Resp: res.PessimisticRollback}, nil
	case *tikvpb.BatchCommandsResponse_Response_Empty:
		return &Response{Resp: res.Empty}, nil
	case *tikvpb.BatchCommandsResponse_Response_TxnHeartBeat:
		return &Response{Resp: res.TxnHeartBeat}, nil
	case *tikvpb.BatchCommandsResponse_Response_CheckTxnStatus:
		return &Response{Resp: res.CheckTxnStatus}, nil
	case *tikvpb.BatchCommandsResponse_Response_CheckSecondaryLocks:
		return &Response{Resp: res.CheckSecondaryLocks}, nil
	case *tikvpb.BatchCommandsResponse_Response_Flush:
		return &Response{Resp: res.Flush}, nil
	case *tikvpb.BatchCommandsResponse_Response_BufferBatchGet:
		return &Response{Resp: res.BufferBatchGet}, nil
	case *tikvpb.BatchCommandsResponse_Response_GetHealthFeedback:
		return &Response{Resp: res.GetHealthFeedback}, nil
	case *tikvpb.BatchCommandsResponse_Response_BroadcastTxnStatus:
		return &Response{Resp: res.BroadcastTxnStatus}, nil
	}
	panic("unreachable")
}

// CopStreamResponse combines tikvpb.Tikv_CoprocessorStreamClient and the first Recv() result together.
// In streaming API, get grpc stream client may not involve any network packet, then region error have
// to be handled in Recv() function. This struct facilitates the error handling.
type CopStreamResponse struct {
	tikvpb.Tikv_CoprocessorStreamClient
	*coprocessor.Response // The first result of Recv()
	Timeout               time.Duration
	Lease                 // Shared by this object and a background goroutine.
}

// BatchCopStreamResponse comprises the BatchCoprocessorClient , the first result and timeout detector.
type BatchCopStreamResponse struct {
	tikvpb.Tikv_BatchCoprocessorClient
	*coprocessor.BatchResponse
	Timeout time.Duration
	Lease   // Shared by this object and a background goroutine.
}

// MPPStreamResponse is indeed a wrapped client that can receive data packet from tiflash mpp server.
type MPPStreamResponse struct {
	tikvpb.Tikv_EstablishMPPConnectionClient
	*mpp.MPPDataPacket
	Timeout time.Duration
	Lease
}

//go:generate bash gen.sh

// AttachContext sets the request context to the request,
// return false if encounter unknown request type.
// Parameter `rpcCtx` use `kvrpcpb.Context` instead of `*kvrpcpb.Context` to avoid concurrent modification by shallow copy.
func AttachContext(req *Request, rpcCtx kvrpcpb.Context) bool {
	ctx := &rpcCtx
	cmd := req.Type
	// CmdCopStream and CmdCop share the same request type.
	if cmd == CmdCopStream {
		cmd = CmdCop
	}
	if patchCmdCtx(req, cmd, ctx) {
		return true
	}
	switch req.Type {
	// Dispatching MPP tasks don't need a region context, because it's a request for store but not region.
	case CmdMPPTask:
	case CmdMPPConn:
	case CmdMPPCancel:
	case CmdMPPAlive:

	// Empty command doesn't need a region context.
	case CmdEmpty:

	default:
		return false
	}
	return true
}

// SetContext set the Context field for the given req to the specified ctx.
//
// Deprecated: use SetContextNoAttach instead, RPCClient will call AttachContext(req, req.Context).
func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	if region != nil {
		req.Context.RegionId = region.Id
		req.Context.RegionEpoch = region.RegionEpoch
	}
	req.Context.Peer = peer

	// Shallow copy the context to avoid concurrent modification.
	if !AttachContext(req, req.Context) {
		return errors.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// SetContextNoAttach likes SetContext, but it doesn't attach the context to the underlying request.
func SetContextNoAttach(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	if !isValidReqType(req.Type) {
		return errors.Errorf("invalid request type %v", req.Type)
	}
	if region != nil {
		req.Context.RegionId = region.Id
		req.Context.RegionEpoch = region.RegionEpoch
	}
	req.Context.Peer = peer
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	var p interface{}
	resp := &Response{}
	switch req.Type {
	case CmdGet:
		p = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		p = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		p = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdPessimisticLock:
		p = &kvrpcpb.PessimisticLockResponse{
			RegionError: e,
		}
	case CmdPessimisticRollback:
		p = &kvrpcpb.PessimisticRollbackResponse{
			RegionError: e,
		}
	case CmdCommit:
		p = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		p = &kvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		p = &kvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		p = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		p = &kvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		p = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		p = &kvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdDeleteRange:
		p = &kvrpcpb.DeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawGet:
		p = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawBatchGet:
		p = &kvrpcpb.RawBatchGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		p = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawBatchPut:
		p = &kvrpcpb.RawBatchPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		p = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawBatchDelete:
		p = &kvrpcpb.RawBatchDeleteResponse{
			RegionError: e,
		}
	case CmdRawDeleteRange:
		p = &kvrpcpb.RawDeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawScan:
		p = &kvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdUnsafeDestroyRange:
		p = &kvrpcpb.UnsafeDestroyRangeResponse{
			RegionError: e,
		}
	case CmdGetKeyTTL:
		p = &kvrpcpb.RawGetKeyTTLResponse{
			RegionError: e,
		}
	case CmdRawCompareAndSwap:
		p = &kvrpcpb.RawCASResponse{
			RegionError: e,
		}
	case CmdRawChecksum:
		p = &kvrpcpb.RawChecksumResponse{
			RegionError: e,
		}
	case CmdCop:
		p = &coprocessor.Response{
			RegionError: e,
		}
	case CmdCopStream:
		p = &CopStreamResponse{
			Response: &coprocessor.Response{
				RegionError: e,
			},
		}
	case CmdMvccGetByKey:
		p = &kvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		}
	case CmdMvccGetByStartTs:
		p = &kvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		}
	case CmdSplitRegion:
		p = &kvrpcpb.SplitRegionResponse{
			RegionError: e,
		}
	case CmdEmpty:
	case CmdTxnHeartBeat:
		p = &kvrpcpb.TxnHeartBeatResponse{
			RegionError: e,
		}
	case CmdCheckTxnStatus:
		p = &kvrpcpb.CheckTxnStatusResponse{
			RegionError: e,
		}
	case CmdCheckSecondaryLocks:
		p = &kvrpcpb.CheckSecondaryLocksResponse{
			RegionError: e,
		}
	case CmdFlashbackToVersion:
		p = &kvrpcpb.FlashbackToVersionResponse{
			RegionError: e,
		}
	case CmdPrepareFlashbackToVersion:
		p = &kvrpcpb.PrepareFlashbackToVersionResponse{
			RegionError: e,
		}
	case CmdFlush:
		p = &kvrpcpb.FlushResponse{
			RegionError: e,
		}
	case CmdBufferBatchGet:
		p = &kvrpcpb.BufferBatchGetResponse{
			RegionError: e,
		}
	case CmdGetHealthFeedback:
		p = &kvrpcpb.GetHealthFeedbackResponse{
			RegionError: e,
		}
	default:
		return nil, errors.Errorf("invalid request type %v", req.Type)
	}
	resp.Resp = p
	return resp, nil
}

type getRegionError interface {
	GetRegionError() *errorpb.Error
}

func isResponseOKToNotImplGetRegionError(resp interface{}) bool {
	switch resp.(type) {
	case *MPPStreamResponse, *mpp.CancelTaskResponse, *mpp.IsAliveResponse, *mpp.ReportTaskStatusResponse,
		*mpp.DispatchTaskResponse, *BatchCopStreamResponse, *tikvpb.BatchCommandsEmptyResponse:
		return true
	default:
		return false
	}
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	if resp.Resp == nil {
		return nil, nil
	}
	err, ok := resp.Resp.(getRegionError)
	if !ok {
		if isResponseOKToNotImplGetRegionError(resp.Resp) {
			return nil, nil
		}
		return nil, errors.Errorf("invalid response type %v", resp)
	}
	return err.GetRegionError(), nil
}

type getExecDetailsV2 interface {
	GetExecDetailsV2() *kvrpcpb.ExecDetailsV2
}

// GetExecDetailsV2 returns the ExecDetailsV2 of the underlying concrete response.
func (resp *Response) GetExecDetailsV2() *kvrpcpb.ExecDetailsV2 {
	if resp == nil || resp.Resp == nil {
		return nil
	}
	details, ok := resp.Resp.(getExecDetailsV2)
	if !ok {
		return nil
	}
	return details.GetExecDetailsV2()
}

func (resp *Response) GetSize() int {
	size := 0
	switch r := resp.Resp.(type) {
	case *kvrpcpb.GetResponse:
		size = r.Size()
	case *kvrpcpb.BatchGetResponse:
		size = r.Size()
	case *kvrpcpb.ScanResponse:
		size = r.Size()
	case *coprocessor.Response:
		size = r.Size()
	case *kvrpcpb.PrewriteResponse:
		size = r.Size()
	case *kvrpcpb.CommitResponse:
		size = r.Size()
	case *kvrpcpb.PessimisticLockResponse:
		size = r.Size()
	case *kvrpcpb.PessimisticRollbackResponse:
		size = r.Size()
	case *kvrpcpb.BatchRollbackResponse:
		size = r.Size()
	case *kvrpcpb.CheckSecondaryLocksResponse:
		size = r.Size()
	case *kvrpcpb.ScanLockResponse:
		size = r.Size()
	case *kvrpcpb.ResolveLockResponse:
		size = r.Size()
	case *kvrpcpb.FlushResponse:
		size = r.Size()
	case *kvrpcpb.CheckTxnStatusResponse:
		size = r.Size()
	case *mpp.MPPDataPacket:
		size = r.Size()
	case *mpp.DispatchTaskResponse:
		size = r.Size()
	default:
		// ignore others
	}
	return size
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for coprocessor streaming, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdGet:
		resp.Resp, err = client.KvGet(ctx, req.Get())
	case CmdScan:
		resp.Resp, err = client.KvScan(ctx, req.Scan())
	case CmdPrewrite:
		resp.Resp, err = client.KvPrewrite(ctx, req.Prewrite())
	case CmdPessimisticLock:
		resp.Resp, err = client.KvPessimisticLock(ctx, req.PessimisticLock())
	case CmdPessimisticRollback:
		resp.Resp, err = client.KVPessimisticRollback(ctx, req.PessimisticRollback())
	case CmdCommit:
		resp.Resp, err = client.KvCommit(ctx, req.Commit())
	case CmdCleanup:
		resp.Resp, err = client.KvCleanup(ctx, req.Cleanup())
	case CmdBatchGet:
		resp.Resp, err = client.KvBatchGet(ctx, req.BatchGet())
	case CmdBatchRollback:
		resp.Resp, err = client.KvBatchRollback(ctx, req.BatchRollback())
	case CmdScanLock:
		resp.Resp, err = client.KvScanLock(ctx, req.ScanLock())
	case CmdResolveLock:
		resp.Resp, err = client.KvResolveLock(ctx, req.ResolveLock())
	case CmdGC:
		resp.Resp, err = client.KvGC(ctx, req.GC())
	case CmdDeleteRange:
		resp.Resp, err = client.KvDeleteRange(ctx, req.DeleteRange())
	case CmdRawGet:
		resp.Resp, err = client.RawGet(ctx, req.RawGet())
	case CmdRawBatchGet:
		resp.Resp, err = client.RawBatchGet(ctx, req.RawBatchGet())
	case CmdRawPut:
		resp.Resp, err = client.RawPut(ctx, req.RawPut())
	case CmdRawBatchPut:
		resp.Resp, err = client.RawBatchPut(ctx, req.RawBatchPut())
	case CmdRawDelete:
		resp.Resp, err = client.RawDelete(ctx, req.RawDelete())
	case CmdRawBatchDelete:
		resp.Resp, err = client.RawBatchDelete(ctx, req.RawBatchDelete())
	case CmdRawDeleteRange:
		resp.Resp, err = client.RawDeleteRange(ctx, req.RawDeleteRange())
	case CmdRawScan:
		resp.Resp, err = client.RawScan(ctx, req.RawScan())
	case CmdUnsafeDestroyRange:
		resp.Resp, err = client.UnsafeDestroyRange(ctx, req.UnsafeDestroyRange())
	case CmdGetKeyTTL:
		resp.Resp, err = client.RawGetKeyTTL(ctx, req.RawGetKeyTTL())
	case CmdRawCompareAndSwap:
		resp.Resp, err = client.RawCompareAndSwap(ctx, req.RawCompareAndSwap())
	case CmdRawChecksum:
		resp.Resp, err = client.RawChecksum(ctx, req.RawChecksum())
	case CmdRegisterLockObserver:
		resp.Resp, err = client.RegisterLockObserver(ctx, req.RegisterLockObserver())
	case CmdCheckLockObserver:
		resp.Resp, err = client.CheckLockObserver(ctx, req.CheckLockObserver())
	case CmdRemoveLockObserver:
		resp.Resp, err = client.RemoveLockObserver(ctx, req.RemoveLockObserver())
	case CmdPhysicalScanLock:
		resp.Resp, err = client.PhysicalScanLock(ctx, req.PhysicalScanLock())
	case CmdCop:
		resp.Resp, err = client.Coprocessor(ctx, req.Cop())
	case CmdMPPTask:
		resp.Resp, err = client.DispatchMPPTask(ctx, req.DispatchMPPTask())
	case CmdMPPAlive:
		resp.Resp, err = client.IsAlive(ctx, req.IsMPPAlive())
	case CmdMPPConn:
		var streamClient tikvpb.Tikv_EstablishMPPConnectionClient
		streamClient, err = client.EstablishMPPConnection(ctx, req.EstablishMPPConn())
		resp.Resp = &MPPStreamResponse{
			Tikv_EstablishMPPConnectionClient: streamClient,
		}
	case CmdMPPCancel:
		// it cannot use the ctx with cancel(), otherwise this cmd will fail.
		resp.Resp, err = client.CancelMPPTask(ctx, req.CancelMPPTask())
	case CmdCopStream:
		var streamClient tikvpb.Tikv_CoprocessorStreamClient
		streamClient, err = client.CoprocessorStream(ctx, req.Cop())
		resp.Resp = &CopStreamResponse{
			Tikv_CoprocessorStreamClient: streamClient,
		}
	case CmdBatchCop:
		var streamClient tikvpb.Tikv_BatchCoprocessorClient
		streamClient, err = client.BatchCoprocessor(ctx, req.BatchCop())
		resp.Resp = &BatchCopStreamResponse{
			Tikv_BatchCoprocessorClient: streamClient,
		}
	case CmdMvccGetByKey:
		resp.Resp, err = client.MvccGetByKey(ctx, req.MvccGetByKey())
	case CmdMvccGetByStartTs:
		resp.Resp, err = client.MvccGetByStartTs(ctx, req.MvccGetByStartTs())
	case CmdSplitRegion:
		resp.Resp, err = client.SplitRegion(ctx, req.SplitRegion())
	case CmdEmpty:
		resp.Resp, err = &tikvpb.BatchCommandsEmptyResponse{}, nil
	case CmdCheckTxnStatus:
		resp.Resp, err = client.KvCheckTxnStatus(ctx, req.CheckTxnStatus())
	case CmdCheckSecondaryLocks:
		resp.Resp, err = client.KvCheckSecondaryLocks(ctx, req.CheckSecondaryLocks())
	case CmdTxnHeartBeat:
		resp.Resp, err = client.KvTxnHeartBeat(ctx, req.TxnHeartBeat())
	case CmdStoreSafeTS:
		resp.Resp, err = client.GetStoreSafeTS(ctx, req.StoreSafeTS())
	case CmdLockWaitInfo:
		resp.Resp, err = client.GetLockWaitInfo(ctx, req.LockWaitInfo())
	case CmdCompact:
		resp.Resp, err = client.Compact(ctx, req.Compact())
	case CmdFlashbackToVersion:
		resp.Resp, err = client.KvFlashbackToVersion(ctx, req.FlashbackToVersion())
	case CmdPrepareFlashbackToVersion:
		resp.Resp, err = client.KvPrepareFlashbackToVersion(ctx, req.PrepareFlashbackToVersion())
	case CmdGetTiFlashSystemTable:
		resp.Resp, err = client.GetTiFlashSystemTable(ctx, req.GetTiFlashSystemTable())
	case CmdFlush:
		resp.Resp, err = client.KvFlush(ctx, req.Flush())
	case CmdBufferBatchGet:
		resp.Resp, err = client.KvBufferBatchGet(ctx, req.BufferBatchGet())
	case CmdGetHealthFeedback:
		resp.Resp, err = client.GetHealthFeedback(ctx, req.GetHealthFeedback())
	case CmdBroadcastTxnStatus:
		resp.Resp, err = client.BroadcastTxnStatus(ctx, req.BroadcastTxnStatus())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return resp, nil
}

// CallDebugRPC launches a debug rpc call.
func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdDebugGetRegionProperties:
		resp.Resp, err = client.GetRegionProperties(ctx, req.DebugGetRegionProperties())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	return resp, err
}

// Lease is used to implement grpc stream timeout.
type Lease struct {
	Cancel   context.CancelFunc
	deadline int64 // A time.UnixNano value, if time.Now().UnixNano() > deadline, cancel() would be called.
}

// Recv overrides the stream client Recv() function.
func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_CoprocessorStreamClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.WithStack(err)
}

// Close closes the CopStreamResponse object.
func (resp *CopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// Recv overrides the stream client Recv() function.
func (resp *BatchCopStreamResponse) Recv() (*coprocessor.BatchResponse, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_BatchCoprocessorClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.WithStack(err)
}

// Close closes the BatchCopStreamResponse object.
func (resp *BatchCopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// Recv overrides the stream client Recv() function.
func (resp *MPPStreamResponse) Recv() (*mpp.MPPDataPacket, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_EstablishMPPConnectionClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.WithStack(err)
}

// Close closes the MPPStreamResponse object.
func (resp *MPPStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// CheckStreamTimeoutLoop runs periodically to check is there any stream request timed out.
// Lease is an object to track stream requests, call this function with "go CheckStreamTimeoutLoop()"
// It is not guaranteed to call every Lease.Cancel() putting into channel when exits.
// If grpc-go supports SetDeadline(https://github.com/grpc/grpc-go/issues/2917), we can stop using this method.
func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{}) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	array := make([]*Lease, 0, 1024)

	for {
		select {
		case <-done:
		drainLoop:
			// Try my best cleaning the channel to make SendRequest which is blocking by it continues.
			for {
				select {
				case <-ch:
				default:
					break drainLoop
				}
			}
			return
		case item := <-ch:
			array = append(array, item)
		case now := <-ticker.C:
			array = keepOnlyActive(array, now.UnixNano())
		}
	}
}

// keepOnlyActive removes completed items, call cancel function for timeout items.
func keepOnlyActive(array []*Lease, now int64) []*Lease {
	idx := 0
	for i := 0; i < len(array); i++ {
		item := array[i]
		deadline := atomic.LoadInt64(&item.deadline)
		if deadline == 0 || deadline > now {
			array[idx] = array[i]
			idx++
		} else {
			item.Cancel()
		}
	}
	return array[:idx]
}

// IsGreenGCRequest checks if the request is used by Green GC's protocol. This is used for failpoints to inject errors
// to specified RPC requests.
func (req *Request) IsGreenGCRequest() bool {
	if req.Type == CmdCheckLockObserver ||
		req.Type == CmdRegisterLockObserver ||
		req.Type == CmdRemoveLockObserver ||
		req.Type == CmdPhysicalScanLock {
		return true
	}
	return false
}

// IsTxnWriteRequest checks if the request is a transactional write request. This is used for failpoints to inject
// errors to specified RPC requests.
func (req *Request) IsTxnWriteRequest() bool {
	if req.Type == CmdPessimisticLock ||
		req.Type == CmdPrewrite ||
		req.Type == CmdCommit ||
		req.Type == CmdBatchRollback ||
		req.Type == CmdPessimisticRollback ||
		req.Type == CmdCheckTxnStatus ||
		req.Type == CmdCheckSecondaryLocks ||
		req.Type == CmdCleanup ||
		req.Type == CmdTxnHeartBeat ||
		req.Type == CmdResolveLock ||
		req.Type == CmdFlashbackToVersion ||
		req.Type == CmdPrepareFlashbackToVersion ||
		req.Type == CmdFlush {
		return true
	}
	return false
}

// IsRawWriteRequest checks if the request is a raw write request.
func (req *Request) IsRawWriteRequest() bool {
	if req.Type == CmdRawPut ||
		req.Type == CmdRawBatchPut ||
		req.Type == CmdRawDelete {
		return true
	}
	return false
}

// ResourceGroupTagger is used to fill the ResourceGroupTag in the kvrpcpb.Context.
type ResourceGroupTagger func(req *Request)

// GetStartTS returns the `start_ts` of the request.
func (req *Request) GetStartTS() uint64 {
	switch req.Type {
	case CmdGet:
		return req.Get().GetVersion()
	case CmdScan:
		return req.Scan().GetVersion()
	case CmdPrewrite:
		return req.Prewrite().GetStartVersion()
	case CmdCommit:
		return req.Commit().GetStartVersion()
	case CmdCleanup:
		return req.Cleanup().GetStartVersion()
	case CmdBatchGet:
		return req.BatchGet().GetVersion()
	case CmdBatchRollback:
		return req.BatchRollback().GetStartVersion()
	case CmdScanLock:
		return req.ScanLock().GetMaxVersion()
	case CmdResolveLock:
		return req.ResolveLock().GetStartVersion()
	case CmdPessimisticLock:
		return req.PessimisticLock().GetStartVersion()
	case CmdPessimisticRollback:
		return req.PessimisticRollback().GetStartVersion()
	case CmdTxnHeartBeat:
		return req.TxnHeartBeat().GetStartVersion()
	case CmdCheckTxnStatus:
		return req.CheckTxnStatus().GetLockTs()
	case CmdCheckSecondaryLocks:
		return req.CheckSecondaryLocks().GetStartVersion()
	case CmdFlashbackToVersion:
		return req.FlashbackToVersion().GetStartTs()
	case CmdPrepareFlashbackToVersion:
		return req.PrepareFlashbackToVersion().GetStartTs()
	case CmdFlush:
		return req.Flush().GetStartTs()
	case CmdBufferBatchGet:
		return req.BufferBatchGet().GetVersion()
	case CmdCop:
		return req.Cop().GetStartTs()
	case CmdCopStream:
		return req.Cop().GetStartTs()
	case CmdBatchCop:
		return req.BatchCop().GetStartTs()
	case CmdMvccGetByStartTs:
		return req.MvccGetByStartTs().GetStartTs()
	default:
	}
	return 0
}
