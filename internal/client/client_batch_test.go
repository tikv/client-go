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
	cmd := &encodedBatchCmd{data: &data}

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

	// Verify req.Cmd is now a encodedBatchCmd
	_, ok := batch1.Requests[0].Cmd.(*encodedBatchCmd)
	require.True(t, ok, "req.Cmd should be converted to *encodedBatchCmd")

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

func TestEncodeRequestCmd_AfterPoolReturn(t *testing.T) {
	req := &tikvpb.BatchCommandsRequest_Request{
		Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
			Empty: &tikvpb.BatchCommandsEmptyRequest{},
		},
	}

	// First encode - should succeed
	err := encodeRequestCmd(req)
	require.NoError(t, err)

	// Simulate send: marshal and return to pool
	batch := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{req}}
	_, err = batch.Marshal()
	require.NoError(t, err)
	reuseRequestData(batch)

	// Second encode on same request - should fail
	err = encodeRequestCmd(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already been encoded and sent")
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
	_, err = encodedReq.Marshal()
	require.Error(t, err, "Marshaling released data should fail")
}

func TestReuseRequestData_DoubleReturn(t *testing.T) {
	req := &tikvpb.BatchCommandsRequest_Request{
		Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
			Empty: &tikvpb.BatchCommandsEmptyRequest{},
		},
	}

	// Encode the request
	err := encodeRequestCmd(req)
	require.NoError(t, err)

	// First reuse - should succeed and return 1
	batch := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{req}}
	count := reuseRequestData(batch)
	require.Equal(t, 1, count)

	// Verify data is now nil
	encodedCmd := req.Cmd.(*encodedBatchCmd)
	require.Nil(t, encodedCmd.data)

	// Second reuse - should return 0 (nil data is not returned to pool)
	count = reuseRequestData(batch)
	require.Equal(t, 0, count)
}

func TestEncodedMsgDataPool_ConcurrentSafety(t *testing.T) {
	var wg sync.WaitGroup
	errors := make([]error, 50)
	successCount := int32(0)

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				req := &tikvpb.BatchCommandsRequest_Request{
					Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{
						Empty: &tikvpb.BatchCommandsEmptyRequest{},
					},
				}
				err := encodeRequestCmd(req)
				if err != nil {
					errors[idx] = err
					return
				}

				atomic.AddInt32(&successCount, 1)
				batch := &tikvpb.BatchCommandsRequest{Requests: []*tikvpb.BatchCommandsRequest_Request{req}}
				_, err = batch.Marshal()
				if err != nil {
					errors[idx] = err
					return
				}
				reuseRequestData(batch)
			}
		}(i)
	}

	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, int32(5000), successCount)
}
