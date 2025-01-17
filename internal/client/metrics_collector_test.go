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
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

func TestNetworkCollectorOnReq(t *testing.T) {
	// Initialize the collector and dependencies
	collector := &networkCollector{}

	// Create a mock request
	// Construct requests
	reqs := []*tikvrpc.Request{
		tikvrpc.NewRequest(
			tikvrpc.CmdGet,
			&kvrpcpb.GetRequest{Context: &kvrpcpb.Context{BusyThresholdMs: 50}, Key: []byte("key")},
		),
		tikvrpc.NewReplicaReadRequest(
			tikvrpc.CmdGet,
			&kvrpcpb.GetRequest{Context: &kvrpcpb.Context{StaleRead: true}, Key: []byte("key")},
			kv.ReplicaReadFollower,
			nil,
		),
	}

	testCases := []struct {
		expectUnpackedBytesSentKV          int64
		expectUnpackedBytesSentKVCrossZone int64
		req                                *tikvrpc.Request
	}{
		{
			expectUnpackedBytesSentKV:          10,
			expectUnpackedBytesSentKVCrossZone: 0,
			req:                                reqs[0],
		},
		{
			expectUnpackedBytesSentKV:          20,
			expectUnpackedBytesSentKVCrossZone: 0,
			req:                                reqs[1],
		},
	}

	details := &util.ExecDetails{}

	for _, cas := range testCases {
		// Call the method
		cas.req.AccessLocation = kv.AccessLocalZone
		collector.onReq(cas.req, details)

		// Verify metrics
		assert.Equal(t, cas.expectUnpackedBytesSentKV, atomic.LoadInt64(&details.UnpackedBytesSentKVTotal), "Total bytes mismatch")
		assert.Equal(t, cas.expectUnpackedBytesSentKVCrossZone, atomic.LoadInt64(&details.UnpackedBytesSentKVCrossZone), "Cross-zone bytes mismatch")
		beforeMetric := dto.Metric{}
		// Verify stale-read metrics
		if cas.req.StaleRead {
			assert.NoError(t, metrics.StaleReadLocalOutBytes.Write(&beforeMetric))
			assert.Equal(t, float64(10), beforeMetric.GetCounter().GetValue(), "Stale-read local bytes mismatch")
			assert.NoError(t, metrics.StaleReadReqLocalCounter.Write(&beforeMetric))
			assert.Equal(t, float64(1), beforeMetric.GetCounter().GetValue(), "Stale-read local counter mismatch")
		}
	}
}

func TestNetworkCollectorOnResp(t *testing.T) {
	// Initialize the collector and dependencies
	collector := &networkCollector{}

	// Construct requests and responses
	reqs := []*tikvrpc.Request{
		tikvrpc.NewRequest(
			tikvrpc.CmdGet,
			&kvrpcpb.GetRequest{Key: []byte("key")},
			kvrpcpb.Context{},
		),
		tikvrpc.NewReplicaReadRequest(
			tikvrpc.CmdGet,
			&kvrpcpb.GetRequest{Key: []byte("key")},
			kv.ReplicaReadFollower,
			nil,
			kvrpcpb.Context{
				StaleRead: true,
			},
		),
	}

	resps := []*tikvrpc.Response{
		{
			Resp: &kvrpcpb.GetResponse{Value: []byte("value")},
		},
		{
			Resp: &kvrpcpb.GetResponse{Value: []byte("stale-value")},
		},
	}

	testCases := []struct {
		expectUnpackedBytesReceivedKV          int64
		expectUnpackedBytesReceivedKVCrossZone int64
		req                                    *tikvrpc.Request
		resp                                   *tikvrpc.Response
	}{
		{
			expectUnpackedBytesReceivedKV:          7,
			expectUnpackedBytesReceivedKVCrossZone: 0,
			req:                                    reqs[0],
			resp:                                   resps[0],
		},
		{
			expectUnpackedBytesReceivedKV:          20,
			expectUnpackedBytesReceivedKVCrossZone: 0,
			req:                                    reqs[1],
			resp:                                   resps[1],
		},
	}

	details := &util.ExecDetails{}

	for _, cas := range testCases {
		// Call the method
		cas.req.AccessLocation = kv.AccessLocalZone
		collector.onResp(cas.req, cas.resp, details)

		// Verify metrics
		assert.Equal(t, cas.expectUnpackedBytesReceivedKV, atomic.LoadInt64(&details.UnpackedBytesReceivedKVTotal), "Total bytes mismatch")
		assert.Equal(t, cas.expectUnpackedBytesReceivedKVCrossZone, atomic.LoadInt64(&details.UnpackedBytesReceivedKVCrossZone), "Cross-zone bytes mismatch")

		// Verify stale-read metrics if applicable
		if cas.req.StaleRead {
			metric := dto.Metric{}
			assert.NoError(t, metrics.StaleReadLocalInBytes.Write(&metric))
			assert.Equal(t, float64(13), metric.GetCounter().GetValue(), "Stale-read local bytes mismatch") // Stale value size
		}
	}
}
