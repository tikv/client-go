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
	now := time.Now()

	// Nothing reported yet, which must not read as a clean bill of health for
	// anyone in particular -- it simply says nothing.
	require.False(t, n.contains("uds_006", now))

	n.replace([]string{"uds_006", "uds_007"}, now)
	require.True(t, n.contains("uds_006", now))
	require.True(t, n.contains("uds_007", now))
	require.False(t, n.contains("uds_008", now))
	require.False(t, n.contains("", now))

	// A report replaces rather than accumulates, so a group the store no longer
	// blames stops being pinned on the next report.
	n.replace([]string{"uds_008"}, now)
	require.False(t, n.contains("uds_006", now))
	require.True(t, n.contains("uds_008", now))

	n.replace(nil, now)
	require.False(t, n.contains("uds_008", now))
}

func TestNoisyGroupsFreshness(t *testing.T) {
	var n noisyGroups
	now := time.Now()

	n.replace([]string{"uds_006"}, now)
	require.True(t, n.contains("uds_006", now))
	require.True(t, n.contains("uds_006", now.Add(noisyGroupsFreshDuration-time.Millisecond)))

	// Feedback that stops arriving stops meaning anything: the store's diagnosis
	// is not renewed by this client's own timeouts, however many it sees.
	require.False(t, n.contains("uds_006", now.Add(noisyGroupsFreshDuration)))
	require.False(t, n.contains("uds_006", now.Add(time.Minute)))
}

func TestAcceptHealthFeedbackOrder(t *testing.T) {
	store := &Store{healthStatus: newStoreHealthStatus(1)}
	now := time.Now()

	// The first feedback sets the baseline; a higher sequence advances it.
	require.True(t, store.acceptHealthFeedback(100, now))
	require.True(t, store.acceptHealthFeedback(102, now.Add(time.Second)))

	// A duplicate and one overtaken on another connection are both dropped.
	require.False(t, store.acceptHealthFeedback(102, now.Add(time.Second+time.Millisecond)))
	require.False(t, store.acceptHealthFeedback(101, now.Add(time.Second+time.Millisecond)))
	require.Equal(t, uint64(102), store.lastFeedback.Load().seq)

	// An unset sequence is applied without moving the baseline.
	require.True(t, store.acceptHealthFeedback(0, now.Add(2*time.Second)))
	require.Equal(t, uint64(102), store.lastFeedback.Load().seq)
	require.False(t, store.acceptHealthFeedback(101, now.Add(2*time.Second)))

	// Past the freshness bound a lower sequence is taken (TiKV restarted).
	later := now.Add(time.Second + healthFeedbackFreshDuration)
	require.True(t, store.acceptHealthFeedback(7, later))
	require.Equal(t, uint64(7), store.lastFeedback.Load().seq)
	require.False(t, store.acceptHealthFeedback(6, later))
}

func TestRecordHealthFeedbackDropsStaleFeedback(t *testing.T) {
	store := &Store{healthStatus: newStoreHealthStatus(1)}
	now := time.Now()

	// Report 103 carries no group set but is still the newest word from the
	// store, so the overtaken report 102 is dropped whole: its empty set must
	// not clear the blame that 101 set and 103 left standing.
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:       1,
		FeedbackSeqNo: 101,
		NoisyGroups:   &kvrpcpb.NoisyGroups{Names: []string{"uds_006"}},
	})
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{StoreId: 1, FeedbackSeqNo: 103})
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:       1,
		FeedbackSeqNo: 102,
		NoisyGroups:   &kvrpcpb.NoisyGroups{},
	})
	require.True(t, store.noisyGroups.contains("uds_006", now))
	require.True(t, store.healthStatus.IsOverloaded())
}

func TestRecordHealthFeedbackNoisyGroups(t *testing.T) {
	store := &Store{healthStatus: newStoreHealthStatus(1)}

	// A store that does not report the set leaves what is known untouched,
	// rather than being taken to have cleared it.
	now := time.Now()
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{StoreId: 1, SlowScore: 1})
	require.False(t, store.noisyGroups.contains("uds_006", now))
	require.False(t, store.healthStatus.IsOverloaded())

	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:       1,
		FeedbackSeqNo: 100,
		SlowScore:     1,
		NoisyGroups:   &kvrpcpb.NoisyGroups{Names: []string{"uds_006"}},
	})
	require.True(t, store.noisyGroups.contains("uds_006", now))
	// Naming anyone marks the whole store, which is what routing keys on.
	require.True(t, store.healthStatus.IsOverloaded())

	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{StoreId: 1, FeedbackSeqNo: 101, SlowScore: 1})
	require.True(t, store.noisyGroups.contains("uds_006", now))
	require.True(t, store.healthStatus.IsOverloaded())

	// Only a store that does report the set may clear it, by reporting empty.
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:       1,
		FeedbackSeqNo: 102,
		SlowScore:     1,
		NoisyGroups:   &kvrpcpb.NoisyGroups{},
	})
	require.False(t, store.noisyGroups.contains("uds_006", now))
	require.False(t, store.healthStatus.IsOverloaded())

	// A report that was overtaken on another connection arrives late. It is
	// dropped whole: neither the set nor the overload mark it would have set.
	store.recordHealthFeedback(&kvrpcpb.HealthFeedback{
		StoreId:       1,
		FeedbackSeqNo: 101,
		SlowScore:     1,
		NoisyGroups:   &kvrpcpb.NoisyGroups{Names: []string{"uds_006"}},
	})
	require.False(t, store.noisyGroups.contains("uds_006", now))
	require.False(t, store.healthStatus.IsOverloaded())

	// A store too old to report the set can only ever signal by ServerIsBusy,
	// which nothing clears, so that mark has to lapse on its own.
	store.healthStatus.markOverloaded(true)
	require.True(t, store.healthStatus.IsOverloaded())
	store.healthStatus.overloadedUntil.Store(time.Now().Add(-time.Second).UnixNano())
	require.False(t, store.healthStatus.IsOverloaded())
}
