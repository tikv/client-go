// Copyright 2026 TiKV Authors
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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
)

func TestEncodedBatchCmd_SizeAndMarshalTo(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	cmd := &encodedBatchCmd{data: data}

	// Verify Size returns correct length
	require.Equal(t, 5, cmd.Size())

	// Verify MarshalTo copies data correctly
	buf := make([]byte, 5)
	n, err := cmd.MarshalTo(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, data, buf)
}

func TestEncodeRequestCmd_Basic(t *testing.T) {
	cmd := &tikvpb.BatchCommandsRequest_Request_Get{
		Get: &kvrpcpb.GetRequest{
			Key: []byte("test-key"),
		},
	}
	batch1 := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{{Cmd: cmd}}}
	batch2 := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{{Cmd: cmd}}}

	err := encodeRequestCmd(batch1.Requests[0])
	require.NoError(t, err)

	// Verify req.Cmd is now a preparedBatchCmd
	_, ok := batch1.Requests[0].Cmd.(*encodedBatchCmd)
	require.True(t, ok, "req.Cmd should be converted to *preparedBatchCmd")

	// Verify two batches have the same size after encoding
	require.Equal(t, batch1.Size(), batch2.Size())

	// Verify marshaled data matches
	data1 := make([]byte, batch1.Size())
	n1, err := batch1.MarshalTo(data1)
	require.NoError(t, err)
	require.Equal(t, batch1.Size(), n1)

	data2 := make([]byte, batch2.Size())
	n2, err := batch2.MarshalTo(data2)
	require.NoError(t, err)
	require.Equal(t, batch2.Size(), n2)

	require.Equal(t, data1, data2)
}

func TestEncodeRequestCmd_PoolReuse(t *testing.T) {
	// Encode cmd of an Empty request
	req1 := &tikvpb.BatchCommandsRequest_Request{
		Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
			Empty: &tikvpb.BatchCommandsEmptyRequest{},
		},
	}
	err := encodeRequestCmd(req1)
	require.NoError(t, err)

	// Simulate send/reuse cycle
	batchReq := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{req1}}
	reuseRequestData(batchReq)

	// Encode cmd of a Get request
	req2 := &tikvpb.BatchCommandsRequest_Request{
		Cmd: &tikvpb.BatchCommandsRequest_Request_Get{
			Get: &kvrpcpb.GetRequest{
				Key: []byte("test-key"),
			},
		},
	}
	err = encodeRequestCmd(req2)
	require.NoError(t, err)
}

func TestEncodeRequestCmd_ConcurrentSafety(t *testing.T) {
	var wg sync.WaitGroup
	errors := make([]error, 50)
	successCount := int32(0)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := &tikvpb.BatchCommandsRequest_Request{
				Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
					Empty: &tikvpb.BatchCommandsEmptyRequest{},
				},
			}
			err := encodeRequestCmd(req)
			errors[idx] = err

			if err == nil {
				atomic.AddInt32(&successCount, 1)
				batch := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{req}}
				reuseRequestData(batch)
			}
		}(i)
	}

	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, int32(50), successCount)
}

func TestReuseRequestData_Basic(t *testing.T) {

	// Normal request
	normalReq := &tikvpb.BatchCommandsRequest_Request{
		Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
			Empty: &tikvpb.BatchCommandsEmptyRequest{},
		},
	}

	// Encoded request
	encodedReq := &tikvpb.BatchCommandsRequest_Request{
		Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
			Empty: &tikvpb.BatchCommandsEmptyRequest{},
		},
	}
	err := encodeRequestCmd(encodedReq)
	require.NoError(t, err)

	// Batch with mixed requests
	batch := &tikvpb.BatchCommandsRequest{
		Requests: []*tikvpb.BatchCommandsRequest_Request{normalReq, encodedReq},
	}

	// reuseRequestData should only affect the encoded request
	count := reuseRequestData(batch)
	require.Equal(t, 1, count)
	_, ok := normalReq.Cmd.(*encodedBatchCmd)
	require.False(t, ok, "Normal request should not be converted")
	_, ok = encodedReq.Cmd.(*encodedBatchCmd)
	require.True(t, ok, "Encoded request should remain encoded")
}
