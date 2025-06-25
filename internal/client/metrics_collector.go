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
	"github.com/prometheus/client_golang/prometheus"
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
	reqSize int
}

func (s *networkCollector) onReq(req *tikvrpc.Request, details *util.ExecDetails) {
	if req == nil {
		return
	}
	size := req.GetSize()
	if size == 0 {
		return
	}
	size += req.Context.Size()
	s.reqSize = size

	isCrossZoneTraffic := req.AccessLocation == kv.AccessCrossZone
	// stale read metrics
	if req.StaleRead {
		s.staleReadMetricsCollector.onReq(float64(size), isCrossZoneTraffic)
	}
}

func (s *networkCollector) onResp(req *tikvrpc.Request, resp *tikvrpc.Response, details *util.ExecDetails) {
	if resp == nil {
		return
	}

	size := resp.GetSize()
	if size == 0 {
		return
	}
	isCrossZoneTraffic := req.AccessLocation == kv.AccessCrossZone
	// stale read metrics
	if req.StaleRead {
		s.staleReadMetricsCollector.onResp(float64(size), isCrossZoneTraffic)
	}
	// replica read metrics
	if isReadReq(req.Type) {
		var observer prometheus.Observer
		switch req.AccessLocation {
		case kv.AccessLocalZone:
			if req.ReplicaRead {
				observer = metrics.ReadRequestFollowerLocalBytes
			} else {
				observer = metrics.ReadRequestLeaderLocalBytes
			}
		case kv.AccessCrossZone:
			if req.ReplicaRead {
				observer = metrics.ReadRequestFollowerRemoteBytes
			} else {
				observer = metrics.ReadRequestLeaderRemoteBytes
			}
		case kv.AccessUnknown:
		}
		if observer != nil {
			observer.Observe(float64(size + s.reqSize))
		}
	}
}

func isReadReq(tp tikvrpc.CmdType) bool {
	switch tp {
	case tikvrpc.CmdGet, tikvrpc.CmdBatchGet, tikvrpc.CmdScan,
		tikvrpc.CmdCop, tikvrpc.CmdBatchCop, tikvrpc.CmdCopStream:
		return true
	default:
		return false
	}
}
