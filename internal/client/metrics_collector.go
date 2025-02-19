// Copyright 2024 TiKV Authors
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
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

type staleReadMetricsCollector struct {
}

func (s *staleReadMetricsCollector) onReq(size float64, isCrossZoneTraffic bool) {
	if isCrossZoneTraffic {
		metrics.StaleReadRemoteOutBytes.Add(float64(size))
		metrics.StaleReadReqCrossZoneCounter.Add(1)

	} else {
		metrics.StaleReadLocalOutBytes.Add(float64(size))
		metrics.StaleReadReqLocalCounter.Add(1)
	}
}

func (s *staleReadMetricsCollector) onResp(size float64, isCrossZoneTraffic bool) {
	if isCrossZoneTraffic {
		metrics.StaleReadRemoteInBytes.Add(float64(size))
	} else {
		metrics.StaleReadLocalInBytes.Add(float64(size))
	}
}

type networkCollector struct {
	staleReadMetricsCollector
}

func (s *networkCollector) onReq(req *tikvrpc.Request, details *util.ExecDetails) {
	if req == nil {
		return
	}
	size := 0
	switch req.Type {
	case tikvrpc.CmdGet:
		size = req.Get().Size()
	case tikvrpc.CmdBatchGet:
		size = req.BatchGet().Size()
	case tikvrpc.CmdScan:
		size = req.Scan().Size()
	case tikvrpc.CmdCop:
		size = req.Cop().Size()
	case tikvrpc.CmdPrewrite:
		size = req.Prewrite().Size()
	case tikvrpc.CmdCommit:
		size = req.Commit().Size()
	case tikvrpc.CmdPessimisticLock:
		size = req.PessimisticLock().Size()
	case tikvrpc.CmdPessimisticRollback:
		size = req.PessimisticRollback().Size()
	case tikvrpc.CmdBatchRollback:
		size = req.BatchRollback().Size()
	case tikvrpc.CmdCheckSecondaryLocks:
		size = req.CheckSecondaryLocks().Size()
	case tikvrpc.CmdScanLock:
		size = req.ScanLock().Size()
	case tikvrpc.CmdResolveLock:
		size = req.ResolveLock().Size()
	case tikvrpc.CmdFlush:
		size = req.Flush().Size()
	case tikvrpc.CmdCheckTxnStatus:
		size = req.CheckTxnStatus().Size()
	case tikvrpc.CmdMPPTask:
		size = req.DispatchMPPTask().Size()
	default:
		// ignore others
		return
	}
	isTiflashTarget := req.StoreTp == tikvrpc.TiFlash
	var total, crossZone *int64
	if isTiflashTarget {
		total = &details.UnpackedBytesSentMPPTotal
		crossZone = &details.UnpackedBytesSentMPPCrossZone
	} else {
		total = &details.UnpackedBytesSentKVTotal
		crossZone = &details.UnpackedBytesSentKVCrossZone
	}

	atomic.AddInt64(total, int64(size))
	isCrossZoneTraffic := req.AccessLocation == kv.AccessCrossZone
	if isCrossZoneTraffic {
		atomic.AddInt64(crossZone, int64(size))
	}
	// stale read metrics
	if req.StaleRead {
		s.staleReadMetricsCollector.onReq(float64(size), isCrossZoneTraffic)
	}
}

func (s *networkCollector) onResp(req *tikvrpc.Request, resp *tikvrpc.Response, details *util.ExecDetails) {
	if resp == nil {
		return
	}
	size := 0
	switch req.Type {
	case tikvrpc.CmdGet:
		size += resp.Resp.(*kvrpcpb.GetResponse).Size()
	case tikvrpc.CmdBatchGet:
		size += resp.Resp.(*kvrpcpb.BatchGetResponse).Size()
	case tikvrpc.CmdScan:
		size += resp.Resp.(*kvrpcpb.ScanResponse).Size()
	case tikvrpc.CmdCop:
		size += resp.Resp.(*coprocessor.Response).Size()
	case tikvrpc.CmdPrewrite:
		size += resp.Resp.(*kvrpcpb.PrewriteResponse).Size()
	case tikvrpc.CmdCommit:
		size += resp.Resp.(*kvrpcpb.CommitResponse).Size()
	case tikvrpc.CmdPessimisticLock:
		size += resp.Resp.(*kvrpcpb.PessimisticLockResponse).Size()
	case tikvrpc.CmdPessimisticRollback:
		size += resp.Resp.(*kvrpcpb.PessimisticRollbackResponse).Size()
	case tikvrpc.CmdBatchRollback:
		size += resp.Resp.(*kvrpcpb.BatchRollbackResponse).Size()
	case tikvrpc.CmdCheckSecondaryLocks:
		size += resp.Resp.(*kvrpcpb.CheckSecondaryLocksResponse).Size()
	case tikvrpc.CmdScanLock:
		size += resp.Resp.(*kvrpcpb.ScanLockResponse).Size()
	case tikvrpc.CmdResolveLock:
		size += resp.Resp.(*kvrpcpb.ResolveLockResponse).Size()
	case tikvrpc.CmdFlush:
		size += resp.Resp.(*kvrpcpb.FlushResponse).Size()
	case tikvrpc.CmdCheckTxnStatus:
		size += resp.Resp.(*kvrpcpb.CheckTxnStatusResponse).Size()
	case tikvrpc.CmdMPPTask:
		// if is MPPDataPacket
		if resp1, ok := resp.Resp.(*mpp.MPPDataPacket); ok && resp1 != nil {
			size += resp1.Size()
		}
		// if is DispatchTaskResponse
		if resp1, ok := resp.Resp.(*mpp.DispatchTaskResponse); ok && resp1 != nil {
			size += resp1.Size()
		}
	default:
		// ignore others
		return
	}
	var total, crossZone *int64
	isTiflashTarget := req.StoreTp == tikvrpc.TiFlash
	if isTiflashTarget {
		total = &details.UnpackedBytesReceivedMPPTotal
		crossZone = &details.UnpackedBytesReceivedMPPCrossZone
	} else {
		total = &details.UnpackedBytesReceivedKVTotal
		crossZone = &details.UnpackedBytesReceivedKVCrossZone
	}

	atomic.AddInt64(total, int64(size))
	isCrossZoneTraffic := req.AccessLocation == kv.AccessCrossZone
	if isCrossZoneTraffic {
		atomic.AddInt64(crossZone, int64(size))
	}
	// stale read metrics
	if req.StaleRead {
		s.staleReadMetricsCollector.onResp(float64(size), isCrossZoneTraffic)
	}
}
