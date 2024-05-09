// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestGetHealthFeedback(t *testing.T) {
	if *withTiKV {
		return
	}

	tikvCluster := NewTestStore(t)

	// Find any TiKV node
	store := tikvCluster.GetRegionCache().GetAllStores()[0]
	for _, s := range tikvCluster.GetRegionCache().GetAllStores() {
		if s.StoreType() == tikvrpc.TiKV {
			store = s
		}
	}
	require.NotNil(t, store)

	client := tikvCluster.GetTiKVClient()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		// In normal cases TiKV's slow score should be stable with value 1. Set it to any unstable value and check again
		// to ensure the value is indeed received from TiKV.
		store.GetHealthStatus().ResetTiKVServerSideSlowScoreForTest(50)

		resp, err := client.SendRequest(ctx, store.GetAddr(), tikvrpc.NewRequest(tikvrpc.CmdGetHealthFeedback, &kvrpcpb.GetHealthFeedbackRequest{}), time.Second)
		require.NoError(t, err)
		getHealthFeedbackResp := resp.Resp.(*kvrpcpb.GetHealthFeedbackResponse)
		require.NotNil(t, getHealthFeedbackResp)
		require.NotEqual(t, 0, getHealthFeedbackResp.GetHealthFeedback().GetFeedbackSeqNo())
		require.Equal(t, 1, getHealthFeedbackResp.GetHealthFeedback().GetSlowScore())
		require.Equal(t, store.StoreID(), getHealthFeedbackResp.GetHealthFeedback().GetStoreId())
		// Updated in batch RPC stream.
		require.Equal(t, int64(1), store.GetHealthStatus().GetHealthStatusDetail().TiKVSideSlowScore)
	}
}
