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

// UpdateTiKVRUV2FromExecDetailsV2 updates TiKV-side raw RPC counters in ExecDetailsV2.RuV2 and accumulates TiKV RU v2 into RUDetails in ctx.
func UpdateTiKVRUV2FromExecDetailsV2(ctx context.Context, details *kvrpcpb.ExecDetailsV2, readRPCCount, writeRPCCount int64) {
	if details == nil {
		details = &kvrpcpb.ExecDetailsV2{}
	}
	if details.RuV2 == nil {
		details.RuV2 = &kvrpcpb.RUV2{}
	}

	ru := details.RuV2
	if readRPCCount != 0 {
		ru.ReadRpcCount += uint64(readRPCCount)
	}
	if writeRPCCount != 0 {
		ru.WriteRpcCount += uint64(writeRPCCount)
	}
	UpdateTiKVRUV2FromRUV2(ctx, ru)
}

// UpdateTiKVRUV2FromRUV2 accumulates raw TiKV RU v2 counters into RUDetails in ctx.
func UpdateTiKVRUV2FromRUV2(ctx context.Context, ru *kvrpcpb.RUV2) {
	if ctx == nil || ru == nil {
		return
	}
	ruDetails, _ := ctx.Value(util.RUDetailsCtxKey).(*util.RUDetails)
	if ruDetails == nil {
		return
	}
	ruDetails.AddRUV2(ru)
	if deltaFloat := calculateTiKVRUV2(ru); deltaFloat != 0 {
		ruDetails.AddTiKVRUV2(deltaFloat)
	}
}

func calculateTiKVRUV2(ru *kvrpcpb.RUV2) float64 {
	if ru == nil {
		return 0
	}
	weights := GetGlobalConfig().TiKVClient.RUV2
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
	deltaFloat := float64(ru.KvEngineCacheMiss)*weights.TiKVKVEngineCacheMiss +
		float64(execInputs)*weights.ExecutorInputs +
		float64(ru.CoprocessorExecutorIterations)*weights.TiKVCoprocessorExecutorIterations +
		float64(ru.CoprocessorResponseBytes)*weights.TiKVCoprocessorResponseBytes +
		float64(ru.RaftstoreStoreWriteTriggerWbBytes)*weights.TiKVRaftstoreStoreWriteTriggerWB +
		float64(ru.StorageProcessedKeysBatchGet)*weights.TiKVStorageProcessedKeysBatchGet +
		float64(ru.StorageProcessedKeysGet)*weights.TiKVStorageProcessedKeysGet +
		float64(ru.WriteRpcCount)*weights.ResourceManagerWriteCntTiKV
	return deltaFloat * weights.RUScale
}
