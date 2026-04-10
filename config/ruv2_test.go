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
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestUpdateTiKVRUV2FromExecDetailsV2(t *testing.T) {
	original := GetGlobalConfig()
	t.Cleanup(func() {
		StoreGlobalConfig(original)
	})

	cfg := DefaultConfig()
	cfg.TiKVClient.RUV2 = DefaultRUV2TiKVConfig()
	StoreGlobalConfig(&cfg)
	weights := cfg.TiKVClient.RUV2

	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)

	UpdateTiKVRUV2FromExecDetailsV2(ctx, &kvrpcpb.ExecDetailsV2{
		RuV2: &kvrpcpb.RUV2{
			KvEngineCacheMiss: 43,
			ExecutorInputs: &kvrpcpb.ExecutorInputs{
				TikvCoprocessorExecutorWorkTotalBatchSelection: 53,
				TikvCoprocessorExecutorWorkTotalBatchTopN:      59,
			},
			CoprocessorExecutorIterations:     61,
			CoprocessorResponseBytes:          67,
			RaftstoreStoreWriteTriggerWbBytes: 71,
			StorageProcessedKeysBatchGet:      73,
			StorageProcessedKeysGet:           79,
			WriteRpcCount:                     31,
		},
	}, 2, 3)

	expected := (43*weights.TiKVKVEngineCacheMiss +
		float64(53+59)*weights.ExecutorInputs +
		61*weights.TiKVCoprocessorExecutorIterations +
		67*weights.TiKVCoprocessorResponseBytes +
		71*weights.TiKVRaftstoreStoreWriteTriggerWB +
		73*weights.TiKVStorageProcessedKeysBatchGet +
		79*weights.TiKVStorageProcessedKeysGet +
		34*weights.ResourceManagerWriteCntTiKV) * weights.RUScale
	require.InDelta(t, expected, ruDetails.TiKVRUV2(), 1e-9)
	raw := ruDetails.RUV2()
	require.NotNil(t, raw)
	require.Equal(t, uint64(43), raw.GetKvEngineCacheMiss())
	require.Equal(t, uint64(2), raw.GetReadRpcCount())
	require.Equal(t, uint64(34), raw.GetWriteRpcCount())
	require.Equal(t, uint64(73), raw.GetStorageProcessedKeysBatchGet())
	require.Equal(t, uint64(79), raw.GetStorageProcessedKeysGet())
}

func TestUpdateTiKVRUV2FromExecDetailsV2WithoutResponseRUV2(t *testing.T) {
	original := GetGlobalConfig()
	t.Cleanup(func() {
		StoreGlobalConfig(original)
	})

	cfg := DefaultConfig()
	cfg.TiKVClient.RUV2 = DefaultRUV2TiKVConfig()
	StoreGlobalConfig(&cfg)
	weights := cfg.TiKVClient.RUV2

	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)

	UpdateTiKVRUV2FromExecDetailsV2(ctx, nil, 2, 3)

	require.InDelta(t, 3*weights.ResourceManagerWriteCntTiKV*weights.RUScale, ruDetails.TiKVRUV2(), 1e-9)
	raw := ruDetails.RUV2()
	require.NotNil(t, raw)
	require.Equal(t, uint64(2), raw.GetReadRpcCount())
	require.Equal(t, uint64(3), raw.GetWriteRpcCount())
}

func TestUpdateTiKVRUV2FromRUV2(t *testing.T) {
	original := GetGlobalConfig()
	t.Cleanup(func() {
		StoreGlobalConfig(original)
	})

	cfg := DefaultConfig()
	cfg.TiKVClient.RUV2 = DefaultRUV2TiKVConfig()
	StoreGlobalConfig(&cfg)
	weights := cfg.TiKVClient.RUV2

	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)
	ru := &kvrpcpb.RUV2{
		KvEngineCacheMiss:            11,
		StorageProcessedKeysBatchGet: 13,
		StorageProcessedKeysGet:      17,
		WriteRpcCount:                19,
	}

	UpdateTiKVRUV2FromRUV2(ctx, ru)

	expected := (11*weights.TiKVKVEngineCacheMiss +
		13*weights.TiKVStorageProcessedKeysBatchGet +
		17*weights.TiKVStorageProcessedKeysGet +
		19*weights.ResourceManagerWriteCntTiKV) * weights.RUScale
	require.InDelta(t, expected, ruDetails.TiKVRUV2(), 1e-9)
	raw := ruDetails.RUV2()
	require.NotNil(t, raw)
	require.Equal(t, uint64(11), raw.GetKvEngineCacheMiss())
	require.Equal(t, uint64(13), raw.GetStorageProcessedKeysBatchGet())
	require.Equal(t, uint64(17), raw.GetStorageProcessedKeysGet())
	require.Equal(t, uint64(19), raw.GetWriteRpcCount())
}
