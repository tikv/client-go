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
	"context"
	"fmt"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

const scanResponsePairSize = int64(unsafe.Sizeof(kvrpcpb.KvPair{}))

// ReusableScanResponse owns the buffers used to decode sequential scan RPC
// responses. The result returned by Response remains valid only until the next
// call to UnmarshalBufferSlice.
type ReusableScanResponse struct {
	response         kvrpcpb.ScanResponse
	decodeBuffer     []byte
	pairs            []*kvrpcpb.KvPair
	activePairs      int
	maxRetainedBytes int64
}

// NewReusableScanResponse creates a response that drops retained capacity when
// it grows beyond maxRetainedBytes. One oversized response can transiently
// exceed the limit, but its buffers are discarded before decoding the next one.
func NewReusableScanResponse(maxRetainedBytes int64) *ReusableScanResponse {
	if maxRetainedBytes < 0 {
		maxRetainedBytes = 0
	}
	return &ReusableScanResponse{maxRetainedBytes: maxRetainedBytes}
}

// Response returns the most recently decoded scan response.
func (r *ReusableScanResponse) Response() *kvrpcpb.ScanResponse {
	return &r.response
}

// RetainedMemory returns the reusable byte and object capacity owned by the
// response. It excludes uncommon region and key error graphs.
func (r *ReusableScanResponse) RetainedMemory() int64 {
	usage := int64(cap(r.decodeBuffer)) + int64(cap(r.pairs))*int64(unsafe.Sizeof((*kvrpcpb.KvPair)(nil)))
	for _, pair := range r.pairs {
		if pair != nil {
			usage += scanResponsePairSize + int64(cap(pair.Key)+cap(pair.Value))
		}
	}
	return usage
}

// UnmarshalBufferSlice copies a gRPC response into reusable storage and
// decodes its KvPair objects without allocating after capacities stabilize.
func (r *ReusableScanResponse) UnmarshalBufferSlice(data mem.BufferSlice) error {
	r.prepareForDecode(data.Len())
	data.CopyTo(r.decodeBuffer)
	return r.unmarshal(r.decodeBuffer)
}

func (r *ReusableScanResponse) prepareForDecode(size int) {
	if r.RetainedMemory() > r.maxRetainedBytes {
		clear(r.pairs)
		r.pairs = nil
		r.decodeBuffer = nil
	}

	r.response.RegionError = nil
	r.response.Error = nil
	r.response.Pairs = nil
	r.activePairs = 0
	if cap(r.decodeBuffer) < size {
		r.decodeBuffer = make([]byte, size)
	} else {
		r.decodeBuffer = r.decodeBuffer[:size]
	}
}

func (r *ReusableScanResponse) nextPair() *kvrpcpb.KvPair {
	if r.activePairs == len(r.pairs) {
		r.pairs = append(r.pairs, &kvrpcpb.KvPair{})
	}
	pair := r.pairs[r.activePairs]
	r.activePairs++
	pair.Error = nil
	pair.Key = pair.Key[:0]
	pair.Value = pair.Value[:0]
	pair.CommitTs = 0
	return pair
}

func (r *ReusableScanResponse) unmarshal(data []byte) error {
	for len(data) > 0 {
		fieldNumber, wireType, tagBytes := protowire.ConsumeTag(data)
		if tagBytes < 0 {
			return protowire.ParseError(tagBytes)
		}
		data = data[tagBytes:]

		switch fieldNumber {
		case 1:
			message, consumed, err := consumeScanResponseMessage(fieldNumber, wireType, data)
			if err != nil {
				return err
			}
			regionError := &errorpb.Error{}
			if err := regionError.Unmarshal(message); err != nil {
				return err
			}
			r.response.RegionError = regionError
			data = data[consumed:]
		case 2:
			message, consumed, err := consumeScanResponseMessage(fieldNumber, wireType, data)
			if err != nil {
				return err
			}
			pair := r.nextPair()
			if err := pair.Unmarshal(message); err != nil {
				return err
			}
			data = data[consumed:]
		case 3:
			message, consumed, err := consumeScanResponseMessage(fieldNumber, wireType, data)
			if err != nil {
				return err
			}
			keyError := &kvrpcpb.KeyError{}
			if err := keyError.Unmarshal(message); err != nil {
				return err
			}
			r.response.Error = keyError
			data = data[consumed:]
		default:
			consumed := protowire.ConsumeFieldValue(fieldNumber, wireType, data)
			if consumed < 0 {
				return protowire.ParseError(consumed)
			}
			data = data[consumed:]
		}
	}
	r.response.Pairs = r.pairs[:r.activePairs]
	return nil
}

func consumeScanResponseMessage(fieldNumber protowire.Number, wireType protowire.Type, data []byte) ([]byte, int, error) {
	if wireType != protowire.BytesType {
		return nil, 0, fmt.Errorf("protobuf field %d has wire type %d, want bytes", fieldNumber, wireType)
	}
	message, consumed := protowire.ConsumeBytes(data)
	if consumed < 0 {
		return nil, 0, protowire.ParseError(consumed)
	}
	return message, consumed, nil
}

type reusableScanCodec struct{}

func (reusableScanCodec) Marshal(value any) (mem.BufferSlice, error) {
	message := reusableScanMessageV2Of(value)
	if message == nil {
		return nil, fmt.Errorf("failed to marshal %T as protobuf", value)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return mem.BufferSlice{mem.SliceBuffer(data)}, nil
}

func (reusableScanCodec) Unmarshal(data mem.BufferSlice, value any) error {
	response, ok := value.(*ReusableScanResponse)
	if !ok {
		return fmt.Errorf("failed to unmarshal reusable scan response into %T", value)
	}
	return response.UnmarshalBufferSlice(data)
}

func (reusableScanCodec) Name() string {
	return "proto"
}

func reusableScanMessageV2Of(value any) proto.Message {
	switch message := value.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(message)
	case protoadapt.MessageV2:
		return message
	default:
		return nil
	}
}

var _ encoding.CodecV2 = reusableScanCodec{}

// CallReusableScanRPC sends one unary scan request and decodes the response
// into the caller-owned buffers attached to req.
func CallReusableScanRPC(ctx context.Context, conn grpc.ClientConnInterface, req *Request) (*Response, error) {
	response := &Response{}
	if req == nil || req.Type != CmdScan || req.ReusableScanResponse == nil {
		return response, fmt.Errorf("reusable scan RPC requires a scan request and response storage")
	}
	err := conn.Invoke(
		ctx,
		"/tikvpb.Tikv/KvScan",
		req.Scan(),
		req.ReusableScanResponse,
		grpc.ForceCodecV2(reusableScanCodec{}),
	)
	if err == nil {
		response.Resp = req.ReusableScanResponse.Response()
	}
	return response, err
}
