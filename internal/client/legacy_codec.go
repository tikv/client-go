// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// legacyCodec is a legacy Codec implementation used by gRPC < 1.66, copied from
// https://github.com/grpc/grpc-go/blob/v1.65.x/encoding/proto/proto.go
// In tipb and kvproto, we reuse the buffer from grpc-go by "overriding" the
// `Unmarshal` method. But in grpc-go >= 1.66, the Codec interface is changed
// to CodecV2, which uses mem.BufferSlice. And this mem.BufferSlice will be freed
// after `Unmarshal` returns. So we need to manually switch to use the old Codec
// interface.
type legacyCodec struct{}

func (legacyCodec) Marshal(v any) ([]byte, error) {
	vv := messageV2Of(v)
	if vv == nil {
		return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
	}

	return proto.Marshal(vv)
}

func (legacyCodec) Unmarshal(data []byte, v any) error {
	vv := messageV2Of(v)
	if vv == nil {
		return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
	}

	return proto.Unmarshal(data, vv)
}

func messageV2Of(v any) proto.Message {
	switch v := v.(type) {
	case protoadapt.MessageV1:
		return protoadapt.MessageV2Of(v)
	case protoadapt.MessageV2:
		return v
	}

	return nil
}

func (legacyCodec) Name() string {
	return ""
}
