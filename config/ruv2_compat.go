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

package config

import (
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/util"
)

// RUV2TiKVConfig contains the removed TiKV RU v2 accounting weights.
//
// Deprecated: the fields are retained only so existing configuration and source
// code continue to parse and compile. They have no effect on RU accounting.
type RUV2TiKVConfig struct {
	RUScale                           float64 `toml:"ru-scale" json:"ru-scale"`
	TiKVKVEngineCacheMiss             float64 `toml:"tikv-kv-engine-cache-miss" json:"tikv-kv-engine-cache-miss"`
	ResourceManagerWriteCntTiKV       float64 `toml:"resource-manager-write-cnt-tikv" json:"resource-manager-write-cnt-tikv"`
	ExecutorInputs                    float64 `toml:"executor-inputs" json:"executor-inputs"`
	TiKVCoprocessorExecutorIterations float64 `toml:"tikv-coprocessor-executor-iterations" json:"tikv-coprocessor-executor-iterations"`
	TiKVCoprocessorResponseBytes      float64 `toml:"tikv-coprocessor-response-bytes" json:"tikv-coprocessor-response-bytes"`
	TiKVRaftstoreStoreWriteTriggerWB  float64 `toml:"tikv-raftstore-store-write-trigger-wb-bytes" json:"tikv-raftstore-store-write-trigger-wb-bytes"`
	TiKVStorageProcessedKeysBatchGet  float64 `toml:"tikv-storage-processed-keys-batch-get" json:"tikv-storage-processed-keys-batch-get"`
	TiKVStorageProcessedKeysGet       float64 `toml:"tikv-storage-processed-keys-get" json:"tikv-storage-processed-keys-get"`
}

// DefaultRUV2TiKVConfig returns the former default TiKV RU v2 weights.
//
// Deprecated: the returned weights are retained for configuration compatibility
// and are no longer used for RU accounting.
func DefaultRUV2TiKVConfig() RUV2TiKVConfig {
	return RUV2TiKVConfig{
		RUScale:                           2.10,
		TiKVKVEngineCacheMiss:             0.45975389,
		ResourceManagerWriteCntTiKV:       0.09642181,
		ExecutorInputs:                    0.00003150,
		TiKVCoprocessorExecutorIterations: 0.05775369,
		TiKVCoprocessorResponseBytes:      0.00000087,
		TiKVRaftstoreStoreWriteTriggerWB:  0.00006100,
		TiKVStorageProcessedKeysBatchGet:  0.00266791,
		TiKVStorageProcessedKeysGet:       0.01416829,
	}
}

// UpdateTiKVRUV2FromExecDetailsV2 records the remaining RU v2 response-byte metric.
//
// Deprecated: callers should add ExecDetailsV2.RuV2 directly to util.RUDetails.
// The RPC counts are ignored because client-side TiKV RU v2 calculation has been removed.
func UpdateTiKVRUV2FromExecDetailsV2(ctx context.Context, details *kvrpcpb.ExecDetailsV2, _, _ int64) {
	if ctx == nil || details == nil || details.RuV2 == nil {
		return
	}
	ruDetails, _ := ctx.Value(util.RUDetailsCtxKey).(*util.RUDetails)
	if ruDetails != nil {
		ruDetails.AddRUV2(details.RuV2)
	}
}
