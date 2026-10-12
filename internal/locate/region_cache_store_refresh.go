// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package locate

import (
	"bytes"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// WorkStoreMatch is a cached region whose working TiKV is a given store.
type WorkStoreMatch struct {
	Region   RegionVerID
	StartKey []byte
	EndKey   []byte
	Peer     *metapb.Peer
	Addr     string
	Epoch    *metapb.RegionEpoch
}

// CountWorkStoreMatches returns how many unexpired cache entries currently work on storeID.
// It does not call isValid()/checkRegionCacheTTL, so it will not renew TTL.
// Unlike CollectWorkStoreMatches it does not copy keys or peers.
func (c *RegionCache) CountWorkStoreMatches(storeID uint64) int {
	if storeID == 0 {
		return 0
	}
	now := time.Now().Unix()
	c.mu.RLock()
	defer c.mu.RUnlock()
	n := 0
	for _, r := range c.mu.regions {
		if r == nil || r.meta == nil || r.isCacheTTLExpired(now) {
			continue
		}
		rs := r.getStore()
		if rs == nil || int(rs.workTiKVIdx) >= rs.accessStoreNum(tiKVOnly) {
			continue
		}
		store, peer, _, _ := r.WorkStorePeer(rs)
		if store != nil && peer != nil && store.StoreID() == storeID {
			n++
		}
	}
	return n
}

// CollectWorkStoreMatches snapshots unexpired cache entries whose working TiKV is storeID.
func (c *RegionCache) CollectWorkStoreMatches(storeID uint64) []WorkStoreMatch {
	if storeID == 0 {
		return nil
	}
	now := time.Now().Unix()
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]WorkStoreMatch, 0)
	for _, r := range c.mu.regions {
		if r == nil || r.meta == nil || r.isCacheTTLExpired(now) {
			continue
		}
		rs := r.getStore()
		if rs == nil || int(rs.workTiKVIdx) >= rs.accessStoreNum(tiKVOnly) {
			continue
		}
		store, peer, _, _ := r.WorkStorePeer(rs)
		if store == nil || peer == nil || store.StoreID() != storeID {
			continue
		}
		start := append([]byte(nil), r.StartKey()...)
		end := append([]byte(nil), r.EndKey()...)
		out = append(out, WorkStoreMatch{
			Region:   r.VerID(),
			StartKey: start,
			EndKey:   end,
			Peer:     protoClonePeer(peer),
			Addr:     store.GetAddr(),
			Epoch:    protoCloneEpoch(r.meta.GetRegionEpoch()),
		})
	}
	return out
}

func protoClonePeer(p *metapb.Peer) *metapb.Peer {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}

func protoCloneEpoch(e *metapb.RegionEpoch) *metapb.RegionEpoch {
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// WorkStoreMatchState is how a cached region currently relates to storeID.
type WorkStoreMatchState int

const (
	// WorkStoreOnStore means the region is still cached and works on storeID.
	WorkStoreOnStore WorkStoreMatchState = iota
	// WorkStoreMoved means the region is still cached but no longer works on storeID.
	WorkStoreMoved
	// WorkStoreGone means the region is missing or its cache TTL has expired.
	WorkStoreGone
)

// WorkStoreMatchOnStore returns the cached match if id still works on storeID.
func (c *RegionCache) WorkStoreMatchOnStore(id RegionVerID, storeID uint64) (WorkStoreMatch, bool) {
	r := c.GetCachedRegionWithRLock(id)
	if r == nil || r.meta == nil || r.isCacheTTLExpired(time.Now().Unix()) {
		return WorkStoreMatch{}, false
	}
	rs := r.getStore()
	if rs == nil || int(rs.workTiKVIdx) >= rs.accessStoreNum(tiKVOnly) {
		return WorkStoreMatch{}, false
	}
	store, peer, _, _ := r.WorkStorePeer(rs)
	if store == nil || peer == nil || store.StoreID() != storeID {
		return WorkStoreMatch{}, false
	}
	return WorkStoreMatch{
		Region:   r.VerID(),
		StartKey: append([]byte(nil), r.StartKey()...),
		EndKey:   append([]byte(nil), r.EndKey()...),
		Peer:     protoClonePeer(peer),
		Addr:     store.GetAddr(),
		Epoch:    protoCloneEpoch(r.meta.GetRegionEpoch()),
	}, true
}

// ClassifyWorkStore reports whether a cached region still works on storeID.
func (c *RegionCache) ClassifyWorkStore(id RegionVerID, storeID uint64) WorkStoreMatchState {
	r := c.GetCachedRegionWithRLock(id)
	if r == nil || r.meta == nil || r.isCacheTTLExpired(time.Now().Unix()) {
		return WorkStoreGone
	}
	rs := r.getStore()
	if rs == nil || int(rs.workTiKVIdx) >= rs.accessStoreNum(tiKVOnly) {
		return WorkStoreGone
	}
	store, peer, _, _ := r.WorkStorePeer(rs)
	if store == nil || peer == nil {
		return WorkStoreGone
	}
	if store.StoreID() != storeID {
		return WorkStoreMoved
	}
	return WorkStoreOnStore
}

// WorkStoreRangeStatus checks contiguous, unexpired coverage of [startKey, endKey).
// coveredTo is the end of the cached prefix and may exceed endKey on completion.
// An empty coveredTo with complete=true means coverage reaches infinity.
// complete reports full coverage; resolved additionally requires every covering
// region to work on a different store. Cache TTL is not renewed.
// It uses the existing region B-tree in O(log R + K) time, where R is the
// number of cached regions and K is the number of regions visited in the range.
// Only the returned boundary is copied; no full-cache snapshot or sort is needed.
func (c *RegionCache) WorkStoreRangeStatus(startKey, endKey []byte, storeID uint64) (coveredTo []byte, complete, resolved bool) {
	now := time.Now().Unix()
	c.mu.RLock()
	defer c.mu.RUnlock()
	coveredTo = startKey
	if c.mu.sorted == nil || len(endKey) > 0 && bytes.Compare(startKey, endKey) >= 0 {
		return append([]byte(nil), coveredTo...), false, false
	}
	first := c.mu.sorted.SearchByKey(startKey, false)
	if first == nil {
		return append([]byte(nil), coveredTo...), false, false
	}
	allMoved := true
	c.mu.sorted.b.AscendGreaterOrEqual(newBtreeSearchItem(first.StartKey()), func(item *btreeItem) bool {
		r := item.cachedRegion
		if r == nil || r.meta == nil || r.isCacheTTLExpired(now) ||
			!r.Contains(coveredTo) {
			return false
		}
		rs := r.getStore()
		if rs != nil && int(rs.workTiKVIdx) < rs.accessStoreNum(tiKVOnly) {
			store, peer, _, _ := r.WorkStorePeer(rs)
			if store != nil && peer != nil && store.StoreID() == storeID {
				allMoved = false
			}
		}
		coveredTo = r.EndKey()
		if len(coveredTo) == 0 || len(endKey) > 0 && bytes.Compare(coveredTo, endKey) >= 0 {
			complete = true
			return false
		}
		return true
	})
	return append([]byte(nil), coveredTo...), complete, complete && allMoved
}

// IsWorkStoreRangeResolved reports whether the entire range is cached and no
// covering region still works on storeID. Missing or expired coverage is unresolved.
func (c *RegionCache) IsWorkStoreRangeResolved(startKey, endKey []byte, storeID uint64) bool {
	_, _, resolved := c.WorkStoreRangeStatus(startKey, endKey, storeID)
	return resolved
}

// ApplyLeaderIfOnStore CAS-updates the working TiKV to leader only while the
// current working store is still oldStoreID. applied means this call changed
// the leader. moved means the cache already left oldStoreID.
func (c *RegionCache) ApplyLeaderIfOnStore(id RegionVerID, leader *metapb.Peer, oldStoreID uint64) (applied, moved bool) {
	if leader == nil {
		return false, false
	}
	r := c.GetCachedRegionWithRLock(id)
	if r == nil {
		return false, false
	}
	if r.isCacheTTLExpired(time.Now().Unix()) {
		return false, false
	}
	return r.switchWorkLeaderToPeerIfOnStore(leader, oldStoreID)
}
