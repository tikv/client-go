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

package util

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/require"
)

func TestRUDetailsRUV2AndTiFlash(t *testing.T) {
	rd := NewRUDetails()
	rd.AddTiKVRUV2(3.5)
	rd.UpdateTiFlash(&rmpb.Consumption{RRU: 2, WRU: 5})

	require.InDelta(t, 3.5, rd.TiKVRUV2(), 1e-9)
	require.InDelta(t, 7.0, rd.TiflashRU(), 1e-9)
	require.InDelta(t, 2.0, rd.RRU(), 1e-9)
	require.InDelta(t, 5.0, rd.WRU(), 1e-9)

	rd.AddRUV2(&kvrpcpb.RUV2{
		KvEngineCacheMiss: 1,
		ExecutorInputs: &kvrpcpb.ExecutorInputs{
			TikvCoprocessorExecutorWorkTotalBatchIndexScan: 2,
		},
	})
	rd.AddRUV2(&kvrpcpb.RUV2{
		KvEngineCacheMiss: 3,
		ExecutorInputs: &kvrpcpb.ExecutorInputs{
			TikvCoprocessorExecutorWorkTotalBatchIndexScan: 4,
		},
	})

	drained := rd.DrainRUV2()
	require.NotNil(t, drained)
	require.Equal(t, uint64(4), drained.KvEngineCacheMiss)
	require.NotNil(t, drained.ExecutorInputs)
	require.Equal(t, uint64(6), drained.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan)
	require.Nil(t, rd.DrainRUV2())
}
