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
	"encoding/json"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestRUV2TiKVConfigCompatibility(t *testing.T) {
	defaultConfig := DefaultTiKVClient()
	require.Equal(t, DefaultRUV2TiKVConfig(), defaultConfig.RUV2)

	var clientConfig TiKVClient
	require.NoError(t, json.Unmarshal([]byte(`{
		"ru-v2": {
			"ru-scale": 3.5,
			"tikv-coprocessor-response-bytes": 0.25
		}
	}`), &clientConfig))
	require.Equal(t, 3.5, clientConfig.RUV2.RUScale)
	require.Equal(t, 0.25, clientConfig.RUV2.TiKVCoprocessorResponseBytes)
}

func TestUpdateTiKVRUV2FromExecDetailsV2Compatibility(t *testing.T) {
	ruDetails := util.NewRUDetails()
	ctx := context.WithValue(context.Background(), util.RUDetailsCtxKey, ruDetails)
	details := &kvrpcpb.ExecDetailsV2{RuV2: &kvrpcpb.RUV2{
		ReadRpcCount:             7,
		WriteRpcCount:            11,
		KvEngineCacheMiss:        13,
		CoprocessorResponseBytes: 17,
	}}

	UpdateTiKVRUV2FromExecDetailsV2(ctx, details, 19, 23)

	// The compatibility wrapper does not patch RPC counts or calculate TiKV RU.
	require.Equal(t, uint64(7), details.RuV2.ReadRpcCount)
	require.Equal(t, uint64(11), details.RuV2.WriteRpcCount)
	require.Zero(t, ruDetails.TiKVRUV2())
	drained := ruDetails.DrainRUV2()
	require.Equal(t, uint64(17), drained.CoprocessorResponseBytes)
	require.Zero(t, drained.ReadRpcCount)
	require.Zero(t, drained.WriteRpcCount)
	require.Zero(t, drained.KvEngineCacheMiss)
}
