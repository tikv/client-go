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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_request.go
//

// Copyright 2016 PingCAP, Inc.
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

	"github.com/google/btree"
)

// SortedRegions is a sorted btree.
type SortedRegions struct {
	b *btree.BTreeG[*btreeItem]
}

// NewSortedRegions returns a new SortedRegions.
func NewSortedRegions(btreeDegree int) *SortedRegions {
	return &SortedRegions{
		b: btree.NewG(btreeDegree, func(a, b *btreeItem) bool { return a.Less(b) }),
	}
}

// ReplaceOrInsert inserts a new item into the btree.
func (s *SortedRegions) ReplaceOrInsert(cachedRegion *Region) *Region {
	old, _ := s.b.ReplaceOrInsert(newBtreeItem(cachedRegion))
	if old != nil {
		return old.cachedRegion
	}
	return nil
}

// DescendLessOrEqual returns all items that are less than or equal to the key.
func (s *SortedRegions) DescendLessOrEqual(key []byte, isEndKey bool, ts int64) (r *Region) {
	s.b.DescendLessOrEqual(newBtreeSearchItem(key), func(item *btreeItem) bool {
		r = item.cachedRegion
		if isEndKey && bytes.Equal(r.StartKey(), key) {
			r = nil     // clear result
			return true // iterate next item
		}
		if !r.checkRegionCacheTTL(ts) {
			r = nil
			return true
		}
		return false
	})
	return r
}

// AscendGreaterOrEqual returns all items that are greater than or equal to the key.
func (s *SortedRegions) AscendGreaterOrEqual(startKey, endKey []byte, limit int) (regions []*Region) {
	s.b.AscendGreaterOrEqual(newBtreeSearchItem(startKey), func(item *btreeItem) bool {
		region := item.cachedRegion
		if len(endKey) > 0 && bytes.Compare(region.StartKey(), endKey) >= 0 {
			return false
		}
		regions = append(regions, region)
		return len(regions) < limit
	})
	return regions
}

// removeIntersecting removes all items that have intersection with the key range of given region.
// If the region itself is in the cache, it's not removed.
func (s *SortedRegions) removeIntersecting(r *Region) []*btreeItem {
	var deleted []*btreeItem
	s.b.AscendGreaterOrEqual(newBtreeSearchItem(r.StartKey()), func(item *btreeItem) bool {
		// Skip the item that is equal to the given region.
		if item.cachedRegion.VerID() == r.VerID() {
			return true
		}
		if len(r.EndKey()) > 0 && bytes.Compare(item.cachedRegion.StartKey(), r.EndKey()) >= 0 {
			return false
		}
		deleted = append(deleted, item)
		return true
	})
	for _, item := range deleted {
		s.b.Delete(item)
	}
	return deleted
}

// Clear removes all items from the btree.
func (s *SortedRegions) Clear() {
	s.b.Clear(false)
}

// ValidRegionsInBtree returns the number of valid regions in the btree.
func (s *SortedRegions) ValidRegionsInBtree(ts int64) (len int) {
	s.b.Descend(func(item *btreeItem) bool {
		r := item.cachedRegion
		if !r.checkRegionCacheTTL(ts) {
			return true
		}
		len++
		return true
	})
	return
}
