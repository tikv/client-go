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
	"sort"
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

func keyRangesOverlap(aStart, aEnd, bStart, bEnd []byte) bool {
	if len(aEnd) > 0 && bytes.Compare(bStart, aEnd) >= 0 {
		return false
	}
	if len(bEnd) > 0 && bytes.Compare(aStart, bEnd) >= 0 {
		return false
	}
	return true
}

type workStoreSpan struct {
	start   []byte
	end     []byte
	onStore bool
}

// WorkSpanIndex is a one-shot snapshot of unexpired cache ranges for storeID.
type WorkSpanIndex struct {
	spans []workStoreSpan
}

func rangeCoveredTo(coveredTo, endKey []byte, toInf bool) bool {
	if toInf {
		return true
	}
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(coveredTo, endKey) >= 0
}

// NewWorkSpanIndex copies unexpired region ranges and whether they still work
// on storeID. It does not renew region TTL.
func (c *RegionCache) NewWorkSpanIndex(storeID uint64) *WorkSpanIndex {
	now := time.Now().Unix()
	c.mu.RLock()
	spans := make([]workStoreSpan, 0, len(c.mu.regions))
	for _, r := range c.mu.regions {
		if r == nil || r.meta == nil || r.isCacheTTLExpired(now) {
			continue
		}
		onStore := false
		rs := r.getStore()
		if rs != nil && int(rs.workTiKVIdx) < rs.accessStoreNum(tiKVOnly) {
			store, peer, _, _ := r.WorkStorePeer(rs)
			onStore = store != nil && peer != nil && store.StoreID() == storeID
		}
		spans = append(spans, workStoreSpan{
			start:   append([]byte(nil), r.StartKey()...),
			end:     append([]byte(nil), r.EndKey()...),
			onStore: onStore,
		})
	}
	c.mu.RUnlock()
	sort.Slice(spans, func(i, j int) bool {
		return bytes.Compare(spans[i].start, spans[j].start) < 0
	})
	return &WorkSpanIndex{spans: spans}
}

// RangeResolved reports whether [startKey, endKey) is fully covered by snapshot
// ranges that no longer work on the target store. Uncached holes are unresolved.
func (idx *WorkSpanIndex) RangeResolved(startKey, endKey []byte) bool {
	if idx == nil || len(idx.spans) == 0 {
		return false
	}
	spans := idx.spans
	i := sort.Search(len(spans), func(j int) bool {
		end := spans[j].end
		return len(end) == 0 || bytes.Compare(end, startKey) > 0
	})
	coveredTo := startKey
	toInf := false
	started := false
	for ; i < len(spans); i++ {
		sp := spans[i]
		if len(endKey) > 0 && bytes.Compare(sp.start, endKey) >= 0 {
			break
		}
		if !keyRangesOverlap(startKey, endKey, sp.start, sp.end) {
			continue
		}
		if sp.onStore {
			return false
		}
		need := startKey
		if started {
			need = coveredTo
		}
		if bytes.Compare(sp.start, need) > 0 {
			return false
		}
		started = true
		if len(sp.end) == 0 {
			toInf = true
			break
		}
		if bytes.Compare(coveredTo, sp.end) < 0 {
			coveredTo = sp.end
		}
		if rangeCoveredTo(coveredTo, endKey, toInf) {
			return true
		}
	}
	return started && rangeCoveredTo(coveredTo, endKey, toInf)
}

// CoveredTo returns how far [startKey, endKey) is covered by contiguous cached
// spans, ignoring whether they still work on the target store. A hole or the
// first span that starts after the covered prefix stops the walk. complete
// means the original range is fully in cache; the caller still needs
// RangeResolved before dropping a failure.
func (idx *WorkSpanIndex) CoveredTo(startKey, endKey []byte) (coveredTo []byte, complete bool) {
	coveredTo = startKey
	if idx == nil || len(idx.spans) == 0 {
		return coveredTo, false
	}
	spans := idx.spans
	i := sort.Search(len(spans), func(j int) bool {
		end := spans[j].end
		return len(end) == 0 || bytes.Compare(end, startKey) > 0
	})
	toInf := false
	started := false
	for ; i < len(spans); i++ {
		sp := spans[i]
		if len(endKey) > 0 && bytes.Compare(sp.start, endKey) >= 0 {
			break
		}
		if !keyRangesOverlap(startKey, endKey, sp.start, sp.end) {
			continue
		}
		need := startKey
		if started {
			need = coveredTo
		}
		if bytes.Compare(sp.start, need) > 0 {
			return coveredTo, false
		}
		started = true
		if len(sp.end) == 0 {
			return sp.end, true
		}
		if bytes.Compare(coveredTo, sp.end) < 0 {
			coveredTo = sp.end
		}
		if rangeCoveredTo(coveredTo, endKey, toInf) {
			return coveredTo, true
		}
	}
	return coveredTo, started && rangeCoveredTo(coveredTo, endKey, toInf)
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
