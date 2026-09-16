// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package locate

import (
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// WorkStoreMatch is a cached region whose working TiKV is a given store.
type WorkStoreMatch struct {
	Region    RegionVerID
	StartKey  []byte
	Peer      *metapb.Peer
	AccessIdx AccessIndex
	Addr      string
	Epoch     *metapb.RegionEpoch
}

// CountWorkStoreMatches returns how many unexpired cache entries currently work on storeID.
// It does not call isValid()/checkRegionCacheTTL, so it will not renew TTL.
func (c *RegionCache) CountWorkStoreMatches(storeID uint64) int {
	return len(c.CollectWorkStoreMatches(storeID))
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
		store, peer, accessIdx, _ := r.WorkStorePeer(rs)
		if store == nil || peer == nil || store.StoreID() != storeID {
			continue
		}
		start := append([]byte(nil), r.StartKey()...)
		out = append(out, WorkStoreMatch{
			Region:    r.VerID(),
			StartKey:  start,
			Peer:      protoClonePeer(peer),
			AccessIdx: accessIdx,
			Addr:      store.GetAddr(),
			Epoch:     protoCloneEpoch(r.meta.GetRegionEpoch()),
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

// ClassifyWorkStore reports whether a cached region still works on storeID.
func (c *RegionCache) ClassifyWorkStore(id RegionVerID, storeID uint64) WorkStoreMatchState {
	r := c.GetCachedRegionWithRLock(id)
	if r == nil || r.isCacheTTLExpired(time.Now().Unix()) {
		return WorkStoreGone
	}
	if r.GetLeaderStoreID() != storeID {
		return WorkStoreMoved
	}
	return WorkStoreOnStore
}

// StillWorksOnStore reports whether the cached region still uses storeID as working TiKV.
func (c *RegionCache) StillWorksOnStore(id RegionVerID, storeID uint64) bool {
	return c.ClassifyWorkStore(id, storeID) == WorkStoreOnStore
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
