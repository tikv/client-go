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

package locate

import (
	"sync/atomic"

	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

type networkCollector struct {
	staleRead       bool
	replicaReadType kv.ReplicaReadType
}

func (s *networkCollector) onReq(req *tikvrpc.Request, details *util.ExecDetails) {
	if req == nil {
		return
	}
	size := req.GetSize()
	if size == 0 {
		return
	}
	isCrossZoneTraffic := req.AccessLocation == kv.AccessCrossZone
	if details != nil {
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
		if isCrossZoneTraffic {
			atomic.AddInt64(crossZone, int64(size))
		}
	}

	// stale read metrics
	if s.staleRead {
		s.onReqStaleRead(float64(size), isCrossZoneTraffic)
	}

	// replica read metrics
	switch req.AccessLocation {
	case kv.AccessLocalZone:
		if s.replicaReadType == kv.ReplicaReadFollower || s.replicaReadType == kv.ReplicaReadMixed {
			metrics.QueryBytesFollowerLocalOutBytes.Observe(float64(size))
		}
	case kv.AccessCrossZone:
		if s.replicaReadType == kv.ReplicaReadLeader {
			metrics.QueryBytesLeaderRemoteOutBytes.Observe(float64(size))
		}
	case kv.AccessUnknown:
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
	// exec details
	if details != nil {
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
		if isCrossZoneTraffic {
			atomic.AddInt64(crossZone, int64(size))
		}
	}

	// stale read metrics
	if s.staleRead {
		s.onRespStaleRead(float64(size), isCrossZoneTraffic)
	}

	// replica read metrics
	switch req.AccessLocation {
	case kv.AccessLocalZone:
		if s.replicaReadType == kv.ReplicaReadFollower || s.replicaReadType == kv.ReplicaReadMixed {
			metrics.QueryBytesFollowerLocalInBytes.Observe(float64(size))
		}
	case kv.AccessCrossZone:
		if s.replicaReadType == kv.ReplicaReadLeader {
			metrics.QueryBytesLeaderRemoteInBytes.Observe(float64(size))
		}
	case kv.AccessUnknown:
	}
}

func (s *networkCollector) onReqStaleRead(size float64, isCrossZoneTraffic bool) {
	if isCrossZoneTraffic {
		metrics.StaleReadRemoteOutBytes.Add(size)
		metrics.StaleReadReqCrossZoneCounter.Add(1)
	} else {
		metrics.StaleReadLocalOutBytes.Add(size)
		metrics.StaleReadReqLocalCounter.Add(1)
	}
}

func (s *networkCollector) onRespStaleRead(size float64, isCrossZoneTraffic bool) {
	if isCrossZoneTraffic {
		metrics.StaleReadRemoteInBytes.Add(size)
	} else {
		metrics.StaleReadLocalInBytes.Add(size)
	}
}
