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

// StillWorksOnStore reports whether the cached region still uses storeID as working TiKV.
func (c *RegionCache) StillWorksOnStore(id RegionVerID, storeID uint64) bool {
	r := c.GetCachedRegionWithRLock(id)
	if r == nil {
		return false
	}
	return r.GetLeaderStoreID() == storeID && !r.isCacheTTLExpired(time.Now().Unix())
}

// CanSwitchLeader reports whether the cached region contains the given leader peer.
func (c *RegionCache) CanSwitchLeader(id RegionVerID, leader *metapb.Peer) bool {
	if leader == nil {
		return false
	}
	r := c.GetCachedRegionWithRLock(id)
	if r == nil {
		return false
	}
	_, found := r.getPeerStoreIndex(leader)
	return found
}

// UpdateLeaderIfStillOnStore updates the working leader only if the region still works on oldStoreID.
// Returns true if the region no longer works on oldStoreID afterwards (updated or already moved).
func (c *RegionCache) UpdateLeaderIfStillOnStore(id RegionVerID, leader *metapb.Peer, accessIdx AccessIndex, oldStoreID uint64) bool {
	if leader == nil {
		return false
	}
	r := c.GetCachedRegionWithRLock(id)
	if r == nil {
		return true
	}
	if r.GetLeaderStoreID() != oldStoreID {
		return true
	}
	if _, found := r.getPeerStoreIndex(leader); !found {
		return false
	}
	c.UpdateLeader(id, leader, accessIdx)
	return !c.StillWorksOnStore(id, oldStoreID)
}
