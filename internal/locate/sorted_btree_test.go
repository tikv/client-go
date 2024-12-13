// Copyright 2022 TiKV Authors
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
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
)

// Helper function to create test regions
func createTestRegion(id uint64, startKey, endKey []byte) *Region {
	return &Region{
		meta: &metapb.Region{
			Id:          id,
			StartKey:    startKey,
			EndKey:      endKey,
			RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		},
		ttl: time.Now().Unix() + 600, // 10 minutes TTL
	}
}

func TestSortedRegionsComparison(t *testing.T) {
	btree := NewSortedRegions(32)
	btreeV2 := NewSortedRegionsV2(32)

	t.Run("ReplaceOrInsert", func(t *testing.T) {
		regions := []*Region{
			createTestRegion(1, []byte("a"), []byte("b")),
			createTestRegion(2, []byte("b"), []byte("c")),
			createTestRegion(3, []byte("c"), []byte("d")),
		}

		for _, r := range regions {
			old1 := btree.ReplaceOrInsert(r)
			old2 := btreeV2.ReplaceOrInsert(r)
			require.Equal(t, old1, old2, "ReplaceOrInsert should return same result")
		}

		// Test replacing existing region
		updatedRegion := createTestRegion(1, []byte("a"), []byte("b"))
		old1 := btree.ReplaceOrInsert(updatedRegion)
		old2 := btreeV2.ReplaceOrInsert(updatedRegion)
		require.Equal(t, old1, old2, "ReplaceOrInsert should return same result for updates")
	})

	t.Run("SearchByKey", func(t *testing.T) {
		testCases := []struct {
			key      []byte
			isEndKey bool
		}{
			{[]byte("a"), false},
			{[]byte("b"), false},
			{[]byte("b"), true},
			{[]byte("c"), false},
			{[]byte("d"), false},
			{[]byte("e"), false}, // non-existent key
		}

		for _, tc := range testCases {
			r1 := btree.SearchByKey(tc.key, tc.isEndKey)
			r2 := btreeV2.SearchByKey(tc.key, tc.isEndKey)

			if r1 == nil {
				require.Nil(t, r2, "Both implementations should return nil for key %s", tc.key)
			} else {
				require.NotNil(t, r2, "Both implementations should return non-nil for key %s", tc.key)
				require.True(t, bytes.Equal(r1.StartKey(), r2.StartKey()),
					"Start keys should match for key %s", tc.key)
				require.True(t, bytes.Equal(r1.EndKey(), r2.EndKey()),
					"End keys should match for key %s", tc.key)
			}
		}
	})

	t.Run("AscendGreaterOrEqual", func(t *testing.T) {
		testCases := []struct {
			startKey []byte
			endKey   []byte
			limit    int
		}{
			{[]byte("a"), []byte("c"), 2},
			{[]byte("b"), []byte("d"), 2},
			{[]byte("a"), []byte("d"), 3},
			{[]byte("d"), []byte("e"), 1}, // beyond existing regions
		}

		for _, tc := range testCases {
			regions1 := btree.AscendGreaterOrEqual(tc.startKey, tc.endKey, tc.limit)
			regions2 := btreeV2.AscendGreaterOrEqual(tc.startKey, tc.endKey, tc.limit)

			require.Equal(t, len(regions1), len(regions2),
				"Should return same number of regions for range [%s, %s]", tc.startKey, tc.endKey)

			for i := range regions1 {
				require.True(t, bytes.Equal(regions1[i].StartKey(), regions2[i].StartKey()),
					"Start keys should match for range [%s, %s]", tc.startKey, tc.endKey)
				require.True(t, bytes.Equal(regions1[i].EndKey(), regions2[i].EndKey()),
					"End keys should match for range [%s, %s]", tc.startKey, tc.endKey)
			}
		}
	})

	t.Run("RemoveIntersecting", func(t *testing.T) {
		region := createTestRegion(4, []byte("b"), []byte("c"))
		verID := RegionVerID{id: 4, ver: 1, confVer: 1}

		deleted1, stale1 := btree.removeIntersecting(region, verID)
		deleted2, stale2 := btreeV2.removeIntersecting(region, verID)

		require.Equal(t, stale1, stale2, "Stale flag should match")
		require.Equal(t, len(deleted1), len(deleted2), "Should delete same number of items")
	})

	t.Run("ValidRegionsInBtree", func(t *testing.T) {
		now := time.Now().Unix()
		count1 := btree.ValidRegionsInBtree(now)
		count2 := btreeV2.ValidRegionsInBtree(now)
		require.Equal(t, count1, count2, "Should have same number of valid regions")
	})

	t.Run("Clear", func(t *testing.T) {
		btree.Clear()
		btreeV2.Clear()

		now := time.Now().Unix()
		require.Equal(t, 0, btree.ValidRegionsInBtree(now), "Btree should be empty after clear")
		require.Equal(t, 0, btreeV2.ValidRegionsInBtree(now), "BtreeV2 should be empty after clear")
	})
}

func BenchmarkSortedRegions_SearchByKey(b *testing.B) {
	btree := NewSortedRegions(32)
	btreeV2 := NewSortedRegionsV2(32)

	// Create 10000 regions with sequential keys
	for i := 0; i < 10000; i++ {
		start := []byte(fmt.Sprintf("key%09d", i)) // key000000001, key000000002, etc.
		end := []byte(fmt.Sprintf("key%09d", i+1))
		region := createTestRegion(uint64(i+1), start, end)
		btree.ReplaceOrInsert(region)
		btreeV2.ReplaceOrInsert(region)
	}

	// Create some test keys for searching
	testKey := []byte("key000005000") // Middle of the range

	b.Run("SearchByKey/Original", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			btree.SearchByKey(testKey, false)
		}
	})

	b.Run("SearchByKey/V2", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			btreeV2.SearchByKey(testKey, false)
		}
	})
}

func BenchmarkSortedRegions_ReplaceOrInsert(b *testing.B) {
	btree := NewSortedRegions(32)
	btreeV2 := NewSortedRegionsV2(32)

	// Create 10000 regions with random keys
	regions := make([]*Region, 10000)
	for i := 0; i < 10000; i++ {
		// Generate random key with prefix to ensure good distribution
		start := []byte(fmt.Sprintf("key%09d", rand.Intn(1000000)))
		end := []byte(fmt.Sprintf("key%09d", rand.Intn(1000000)))
		if bytes.Compare(start, end) > 0 {
			// Ensure start key is less than end key
			start, end = end, start
		}
		regions[i] = createTestRegion(uint64(i+1), start, end)
	}

	b.Run("ReplaceOrInsert/Original", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			btree.ReplaceOrInsert(regions[i%10000])
		}
	})

	b.Run("ReplaceOrInsert/V2", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			btreeV2.ReplaceOrInsert(regions[i%10000])
		}
	})
}

func BenchmarkSortedRegions_AscendGreaterOrEqual(b *testing.B) {
	btree := NewSortedRegions(32)
	btreeV2 := NewSortedRegionsV2(32)

	// Create 10000 regions with sequential keys
	regions := make([]*Region, 10000)
	for i := 0; i < 10000; i++ {
		start := []byte(fmt.Sprintf("key%09d", i))
		end := []byte(fmt.Sprintf("key%09d", i+1))
		regions[i] = createTestRegion(uint64(i+1), start, end)
	}

	// Insert all regions into both trees
	for i := 0; i < 10000; i++ {
		btree.ReplaceOrInsert(regions[i])
		btreeV2.ReplaceOrInsert(regions[i])
	}

	// Pick a random start key for benchmarking
	testKey := []byte(fmt.Sprintf("key%09d", 5000))
	endKey := []byte(fmt.Sprintf("key%09d", 6000))

	b.Run("AscendGreaterOrEqual/Original", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			btree.AscendGreaterOrEqual(testKey, endKey, 100)
		}
	})

	b.Run("AscendGreaterOrEqual/V2", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			btreeV2.AscendGreaterOrEqual(testKey, endKey, 100)
		}
	})
}
