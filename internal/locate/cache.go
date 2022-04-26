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

//go:build !go1.18
// +build !go1.18

package locate

import (
	"bytes"
	"sync"

	"github.com/google/btree"
)

const btreeDegree = 32

type CacheMu struct {
	sync.RWMutex                           // mutex protect cached region
	regions        map[RegionVerID]*Region // cached regions are organized as regionVerID to region ref mapping
	latestVersions map[uint64]RegionVerID  // cache the map from regionID to its latest RegionVerID
	sorted         btree.BTree             // cache regions are organized as sorted key to region ref mapping
}

func (item *btreeItem) Less(other btree.Item) bool {
	return bytes.Compare(item.key, other.(*btreeItem).key) < 0
}

func NewCache() CacheMu {
	mu := CacheMu{}
	mu.regions = make(map[RegionVerID]*Region)
	mu.latestVersions = make(map[uint64]RegionVerID)
	mu.sorted = *btree.New(btreeDegree)
	return mu
}

func (mu *CacheMu) searchCachedRegion(key []byte, isEndKey bool, ts int64) (r *Region) {
	mu.RLock()
	defer mu.RUnlock()
	mu.sorted.DescendLessOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		r = item.(*btreeItem).cachedRegion
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
	return
}

func (mu *CacheMu) scanRegionsFromCache(startKey, endKey []byte, limit int) (regions []*Region) {
	if limit == 0 {
		return nil
	}
	mu.RLock()
	defer mu.RUnlock()
	mu.sorted.AscendGreaterOrEqual(newBtreeSearchItem(startKey), func(item btree.Item) bool {
		region := item.(*btreeItem).cachedRegion
		if len(endKey) > 0 && bytes.Compare(region.StartKey(), endKey) >= 0 {
			return false
		}
		regions = append(regions, region)
		return len(regions) < limit
	})
	return
}

func (mu *CacheMu) ReplaceOrInsert(item *btreeItem) *btreeItem {
	old := mu.sorted.ReplaceOrInsert(item)
	if vara, ok := old.(*btreeItem); ok {
		return vara
	}
	return nil
}
func (mu *CacheMu) Clear() {
	mu.Lock()
	mu.regions = make(map[RegionVerID]*Region)
	mu.latestVersions = make(map[uint64]RegionVerID)
	mu.sorted = *btree.New(btreeDegree)
	mu.Unlock()
}

func validRegionsInBtree(t *CacheMu, ts int64) (len int) {
	t.sorted.Descend(func(item btree.Item) bool {
		r := item.(*btreeItem).cachedRegion
		if !r.checkRegionCacheTTL(ts) {
			return true
		}
		len++
		return true
	})
	return
}
