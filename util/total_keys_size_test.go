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

package util_test

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/util"
)

func TestCloudTotalKeysSize(t *testing.T) {
	for _, size := range []uint64{0, 80} {
		pb := &kvrpcpb.ScanDetailV2{TotalVersions: 5, ProcessedVersions: 2, ProcessedVersionsSize: 20, TotalVersionsSize: size}
		want := int64(0)
		if config.NextGen {
			want = int64(size)
		}
		var scan util.ScanDetail
		scan.MergeFromScanDetailV2(nil)
		scan.MergeFromScanDetailV2(pb)
		require.Equal(t, want, scan.TotalKeysSize)
		require.Equal(t, int64(20), scan.ProcessedKeysSize)
		scan.MergeFromScanDetailV2(pb)
		require.Equal(t, 2*want, scan.TotalKeysSize)
		var merged util.ScanDetail
		merged.Merge(&scan)
		merged.Merge(&scan)
		require.Equal(t, 4*want, merged.TotalKeysSize)

		var point util.PointResponseStats
		point.RecordResponse(pb, 3)
		require.Equal(t, want, point.ScanDetail.TotalKeysSize)
		point.RecordResponse(pb, 3)
		require.Equal(t, 2*want, point.ScanDetail.TotalKeysSize)
		var combined util.PointResponseStats
		combined.Merge(point)
		combined.Merge(point)
		require.Equal(t, 4*want, combined.ScanDetail.TotalKeysSize)
		require.True(t, combined.ScanDetailComplete())
		combined.RecordResponse(nil, 0)
		require.Equal(t, 4*want, combined.ScanDetail.TotalKeysSize)
		require.False(t, combined.ScanDetailComplete())
	}
}
