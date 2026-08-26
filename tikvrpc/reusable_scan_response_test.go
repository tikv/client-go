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

package tikvrpc

import (
	"testing"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
)

type reusableScanStatsCollector struct {
	stats ReusableScanStats
}

func (c *reusableScanStatsCollector) ObserveReusableScanStats(delta ReusableScanStats) {
	c.stats.ScannerNextCalls += delta.ScannerNextCalls
	c.stats.ResponseCount += delta.ResponseCount
	c.stats.ResponseBytes += delta.ResponseBytes
	c.stats.DecodeBufferAllocatedBytes += delta.DecodeBufferAllocatedBytes
	c.stats.PairSliceAllocatedBytes += delta.PairSliceAllocatedBytes
	c.stats.PairObjectAllocatedBytes += delta.PairObjectAllocatedBytes
	c.stats.PairKeyValueAllocatedBytes += delta.PairKeyValueAllocatedBytes
	c.stats.PairCount += delta.PairCount
	c.stats.KeyBytes += delta.KeyBytes
	c.stats.ValueBytes += delta.ValueBytes
}

func TestReusableScanResponseReusesDecodedBuffers(t *testing.T) {
	encoded := marshalScanResponse(t, 256, 4<<10)
	input := mem.BufferSlice{mem.SliceBuffer(encoded)}
	response := NewReusableScanResponse(16 << 20)

	require.NoError(t, response.UnmarshalBufferSlice(input))
	require.Len(t, response.Response().GetPairs(), 256)
	firstPair := response.Response().GetPairs()[0]
	firstKey := unsafe.SliceData(firstPair.Key)
	firstValue := unsafe.SliceData(firstPair.Value)
	firstDecodeBuffer := unsafe.SliceData(response.decodeBuffer)

	allocs := testing.AllocsPerRun(20, func() {
		require.NoError(t, response.UnmarshalBufferSlice(input))
	})
	require.Zero(t, allocs)
	require.Same(t, firstPair, response.Response().GetPairs()[0])
	require.Equal(t, firstKey, unsafe.SliceData(response.Response().GetPairs()[0].Key))
	require.Equal(t, firstValue, unsafe.SliceData(response.Response().GetPairs()[0].Value))
	require.Equal(t, firstDecodeBuffer, unsafe.SliceData(response.decodeBuffer))
}

func TestReusableScanResponseClearsStaleData(t *testing.T) {
	response := NewReusableScanResponse(16 << 20)
	first := &kvrpcpb.ScanResponse{
		Pairs: []*kvrpcpb.KvPair{{
			Error:    &kvrpcpb.KeyError{Abort: "stale"},
			Key:      []byte("old-key"),
			Value:    []byte("old-value"),
			CommitTs: 42,
		}},
		Error: &kvrpcpb.KeyError{Abort: "stale-response-error"},
	}
	firstBytes, err := first.Marshal()
	require.NoError(t, err)
	require.NoError(t, response.UnmarshalBufferSlice(mem.BufferSlice{mem.SliceBuffer(firstBytes)}))

	second := &kvrpcpb.ScanResponse{
		Pairs: []*kvrpcpb.KvPair{{
			Key:   []byte("new-key"),
			Value: []byte("new-value"),
		}},
	}
	secondBytes, err := second.Marshal()
	require.NoError(t, err)
	require.NoError(t, response.UnmarshalBufferSlice(mem.BufferSlice{mem.SliceBuffer(secondBytes)}))

	require.Nil(t, response.Response().GetError())
	require.Len(t, response.Response().GetPairs(), 1)
	pair := response.Response().GetPairs()[0]
	require.Nil(t, pair.GetError())
	require.Equal(t, []byte("new-key"), pair.GetKey())
	require.Equal(t, []byte("new-value"), pair.GetValue())
	require.Zero(t, pair.GetCommitTs())
}

func TestReusableScanResponseDropsOversizedRetention(t *testing.T) {
	const retainedLimit = 1 << 20
	response := NewReusableScanResponse(retainedLimit)
	large := marshalScanResponse(t, 4, retainedLimit)
	require.NoError(t, response.UnmarshalBufferSlice(mem.BufferSlice{mem.SliceBuffer(large)}))
	require.Greater(t, response.RetainedMemory(), int64(retainedLimit))

	small := marshalScanResponse(t, 1, 32)
	require.NoError(t, response.UnmarshalBufferSlice(mem.BufferSlice{mem.SliceBuffer(small)}))
	require.LessOrEqual(t, response.RetainedMemory(), int64(retainedLimit))
}

func TestReusableScanResponseReportsStats(t *testing.T) {
	collector := &reusableScanStatsCollector{}
	encoded := marshalScanResponse(t, 4, 128)
	input := mem.BufferSlice{mem.SliceBuffer(encoded)}
	response := NewReusableScanResponseWithObserver(16<<20, collector)

	response.ObserveScannerNext()
	require.NoError(t, response.UnmarshalBufferSlice(input))
	firstAllocationBytes := collector.stats.DecodeBufferAllocatedBytes +
		collector.stats.PairSliceAllocatedBytes +
		collector.stats.PairObjectAllocatedBytes +
		collector.stats.PairKeyValueAllocatedBytes
	require.Positive(t, firstAllocationBytes)
	require.Equal(t, uint64(1), collector.stats.ScannerNextCalls)
	require.Equal(t, uint64(1), collector.stats.ResponseCount)
	require.Equal(t, uint64(len(encoded)), collector.stats.ResponseBytes)
	require.Equal(t, uint64(4), collector.stats.PairCount)
	require.Equal(t, uint64(8), collector.stats.KeyBytes)
	require.Equal(t, uint64(4*128), collector.stats.ValueBytes)

	require.NoError(t, response.UnmarshalBufferSlice(input))
	require.Equal(t, uint64(2), collector.stats.ResponseCount)
	require.Equal(t, uint64(2*len(encoded)), collector.stats.ResponseBytes)
	require.Equal(t, uint64(8), collector.stats.PairCount)
	require.Equal(t, firstAllocationBytes,
		collector.stats.DecodeBufferAllocatedBytes+
			collector.stats.PairSliceAllocatedBytes+
			collector.stats.PairObjectAllocatedBytes+
			collector.stats.PairKeyValueAllocatedBytes)
}

func marshalScanResponse(t *testing.T, pairCount, valueSize int) []byte {
	t.Helper()
	response := &kvrpcpb.ScanResponse{Pairs: make([]*kvrpcpb.KvPair, pairCount)}
	for i := range response.Pairs {
		response.Pairs[i] = &kvrpcpb.KvPair{
			Key:      []byte{byte(i), byte(i >> 8)},
			Value:    make([]byte, valueSize),
			CommitTs: uint64(i + 1),
		}
		response.Pairs[i].Value[0] = byte(i)
	}
	encoded, err := response.Marshal()
	require.NoError(t, err)
	return encoded
}
