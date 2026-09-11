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

package locate

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func TestNoisyGroupsReplaceAndLookup(t *testing.T) {
	var n noisyGroups

	// Nothing reported yet, which must not read as a clean bill of health for
	// anyone in particular -- it simply says nothing.
	require.False(t, n.contains("uds_006"))

	n.replace([]string{"uds_006", "uds_007"})
	require.True(t, n.contains("uds_006"))
	require.True(t, n.contains("uds_007"))
	require.False(t, n.contains("uds_008"))
	require.False(t, n.contains(""))

	// A report replaces rather than accumulates, so a group the store no longer
	// blames stops being pinned on the next report.
	n.replace([]string{"uds_008"})
	require.False(t, n.contains("uds_006"))
	require.True(t, n.contains("uds_008"))

	n.replace(nil)
	require.False(t, n.contains("uds_008"))
}

func TestRecordHealthFeedbackNoisyGroups(t *testing.T) {
	store := &Store{healthStatus: newStoreHealthStatus(1)}

	// A store that does not report the set leaves what is known untouched,
	// rather than being taken to have cleared it.
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{StoreId: 1, SlowScore: 1})
	require.False(t, store.noisyGroups.contains("uds_006"))
	require.False(t, store.healthStatus.IsOverloaded())

	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:     1,
		SlowScore:   1,
		NoisyGroups: &kvrpcpb.NoisyGroups{Names: []string{"uds_006"}},
	})
	require.True(t, store.noisyGroups.contains("uds_006"))
	// Naming anyone marks the whole store, which is what routing keys on.
	require.True(t, store.healthStatus.IsOverloaded())

	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{StoreId: 1, SlowScore: 1})
	require.True(t, store.noisyGroups.contains("uds_006"))
	require.True(t, store.healthStatus.IsOverloaded())

	// Only a store that does report the set may clear it, by reporting empty.
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:     1,
		SlowScore:   1,
		NoisyGroups: &kvrpcpb.NoisyGroups{},
	})
	require.False(t, store.noisyGroups.contains("uds_006"))
	require.False(t, store.healthStatus.IsOverloaded())

	// A store too old to report the set can only ever signal by ServerIsBusy,
	// which nothing clears, so that mark has to lapse on its own.
	store.healthStatus.markOverloaded(true)
	require.True(t, store.healthStatus.IsOverloaded())
	store.healthStatus.overloadedUntil.Store(time.Now().Add(-time.Second).UnixNano())
	require.False(t, store.healthStatus.IsOverloaded())
}
