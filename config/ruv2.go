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

// UpdateTiKVRUV2FromExecDetailsV2 calculates and accumulates TiKV RU v2 from ExecDetailsV2.RuV2 into RUDetails in ctx.
// writeRPCCount is the number of completed write RPCs to TiKV.
func UpdateTiKVRUV2FromExecDetailsV2(ctx context.Context, details *kvrpcpb.ExecDetailsV2, writeRPCCount float64) {
	if ctx == nil || (details == nil && writeRPCCount == 0) {
		return
	}
	ruDetails, _ := ctx.Value(util.RUDetailsCtxKey).(*util.RUDetails)
	if ruDetails == nil {
		return
	}

	weights := DefaultRUV2TiKVConfig()
	if cfg := GetGlobalConfig(); cfg != nil {
		weights = cfg.TiKVClient.RUV2
	}
	if weights.RUScale == 0 {
		weights.RUScale = DefaultRUV2TiKVConfig().RUScale
	}

	deltaFloat := writeRPCCount * weights.ResourceManagerWriteCntTiKV
	if details != nil && details.RuV2 != nil {
		ru := details.RuV2
		var execInputs uint64
		if inputs := ru.ExecutorInputs; inputs != nil {
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchTableScan
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchSelection
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchTopN
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchLimit
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchSimpleAggr
			execInputs += inputs.TikvCoprocessorExecutorWorkTotalBatchFastHashAggr
		}
		deltaFloat += float64(ru.KvEngineCacheMiss)*weights.TiKVKVEngineCacheMiss +
			float64(execInputs)*weights.ExecutorInputs +
			float64(ru.CoprocessorExecutorIterations)*weights.TiKVCoprocessorExecutorIterations +
			float64(ru.CoprocessorResponseBytes)*weights.TiKVCoprocessorResponseBytes +
			float64(ru.RaftstoreStoreWriteTriggerWbBytes)*weights.TiKVRaftstoreStoreWriteTriggerWB +
			float64(ru.StorageProcessedKeysBatchGet)*weights.TiKVStorageProcessedKeysBatchGet +
			float64(ru.StorageProcessedKeysGet)*weights.TiKVStorageProcessedKeysGet
	}
	if deltaFloat == 0 {
		return
	}
	ruDetails.AddTiKVRUV2(deltaFloat * weights.RUScale)
}
