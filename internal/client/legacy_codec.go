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

// legacyCodec copies the legacy proto Codec used in grpc-go < 1.66, see
// https://github.com/grpc/grpc-go/blob/v1.65.x/encoding/proto/proto.go
// tipb/kvproto rely on grpc-go passing a []byte into `Unmarshal` that can be
// owned by the callee, so we override the `Marshal` to reduce the copy overhead.
// But after grpc-go >= 1.66, it switches to CodecV2. And the buffer passed into
// `Unmarshal` is now from a shared pool, which will be returned to the pool right
// after `Unmarshal` returns, which breaks the assumption of tipb/kvproto. So we
// force the old Codec behavior until sharedBytes is gone.
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
	return "proto"
}
