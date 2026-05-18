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

// legacyCodec copies the legacy proto codec used in grpc-go < v1.66.
//
// grpc-go v1.66 switched the default proto codec to CodecV2. The new codec can
// pass pooled buffers to Unmarshal and return those buffers immediately after
// Unmarshal finishes. tipb/kvproto still use sharedBytes in some generated
// messages and assume the unmarshaled []byte can be owned by the callee. Force
// the old codec behavior for the release-8.5 grpc upgrade so the dependency
// bump does not change protobuf buffer ownership semantics.
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
