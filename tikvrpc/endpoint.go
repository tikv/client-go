// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tikvrpc/endpoint.go
//

// Copyright 2021 PingCAP, Inc.
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

import "github.com/pingcap/kvproto/pkg/metapb"

// EndpointType represents the type of a remote endpoint..
type EndpointType uint8

// EndpointType type enums.
const (
	TiKV EndpointType = iota
	TiFlash
	TiDB
	TiFlashMPP
)

// Name returns the name of endpoint type.
func (t EndpointType) Name() string {
	switch t {
	case TiKV:
		return "tikv"
	case TiFlash:
		return "tiflash"
	case TiDB:
		return "tidb"
	case TiFlashMPP:
		return "tiflash_mpp"
	}
	return "unspecified"
}

// IsTiFlashRelatedType return true if it's tiflash or tiflash_mpp.
func (t EndpointType) IsTiFlashRelatedType() bool {
	return t == TiFlash || t == TiFlashMPP
}

// Constants to determine engine type.
// They should be synced with PD.
const (
	EngineLabelKey        = "engine"
	EngineLabelTiFlash    = "tiflash"
	EngineLabelTiFlashMPP = "tiflash_mpp"
)

// GetStoreTypeByMeta gets store type by store meta pb.
func GetStoreTypeByMeta(store *metapb.Store) EndpointType {
	for _, label := range store.Labels {
		if label.Key == EngineLabelKey && label.Value == EngineLabelTiFlash {
			return TiFlash
		}
		if label.Key == EngineLabelKey && label.Value == EngineLabelTiFlashMPP {
			return TiFlashMPP
		}
	}
	return TiKV
}
