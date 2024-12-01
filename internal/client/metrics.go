package client

import (
	"reflect"
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

func (s *staleReadMetricsCollector) onReq(req *tikvrpc.Request) {
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
	default:
		// ignore non-read requests
		return
	}
	isLocalTraffic := req.AccessLocation == kv.AccessLocalZone
	size += req.Context.Size()
	if isLocalTraffic {
		metrics.StaleReadLocalOutBytes.Add(float64(size))
		metrics.StaleReadReqLocalCounter.Add(1)
	} else {
		metrics.StaleReadRemoteOutBytes.Add(float64(size))
		metrics.StaleReadReqCrossZoneCounter.Add(1)
	}
}

func (s *staleReadMetricsCollector) onResp(req *tikvrpc.Request, resp *tikvrpc.Response) {
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
	default:
		// ignore non-read requests
		return
	}
	isLocalTraffic := req.AccessLocation == kv.AccessLocalZone
	if isLocalTraffic {
		metrics.StaleReadLocalInBytes.Add(float64(size))
	} else {
		metrics.StaleReadRemoteInBytes.Add(float64(size))
	}
}

type networkCollector struct {
	staleReadMetricsCollector
}

func (s *networkCollector) onReq(req *tikvrpc.Request, details *util.ExecDetails) {
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
	case tikvrpc.CmdMPPTask:
		size = req.DispatchMPPTask().Size()
	default:
		// ignore others
		return
	}
	size += req.Context.Size()
	isTiflashTarget := req.StoreTp == tikvrpc.TiFlash
	var total, crossZone *int64
	if isTiflashTarget {
		total = &details.BytesSendMPPTotal
		crossZone = &details.BytesSendMPPCrossZone
	} else {
		total = &details.BytesSendKVTotal
		crossZone = &details.BytesSendKVCrossZone
	}

	atomic.AddInt64(total, int64(size))
	if req.AccessLocation == kv.AccessCrossZone {
		atomic.AddInt64(crossZone, int64(size))
	}
	// stale read metrics
	if req.StaleRead {
		s.staleReadMetricsCollector.onReq(req)
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
		resp1 := resp.Resp.(*kvrpcpb.BatchGetResponse)
		if resp1 == nil {
			panic(reflect.TypeOf(resp.Resp))
		} else {
			size += resp.Resp.(*kvrpcpb.BatchGetResponse).Size()
		}
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
		total = &details.BytesReceivedMPPTotal
		crossZone = &details.BytesReceivedMPPCrossZone
	} else {
		total = &details.BytesReceivedKVTotal
		crossZone = &details.BytesReceivedKVCrossZone
	}

	atomic.AddInt64(total, int64(size))
	if req.AccessLocation == kv.AccessCrossZone {
		atomic.AddInt64(crossZone, int64(size))
	}
	// stale read metrics
	if req.StaleRead {
		s.staleReadMetricsCollector.onResp(req, resp)
	}
}
