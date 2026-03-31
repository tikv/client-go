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
	}, 0, 0)

	require.InDelta(t, 38.64481334, ruDetails.TiKVRUV2(), 1e-9)
}
