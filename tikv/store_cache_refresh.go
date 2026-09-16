// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const (
	storeCacheRefreshWorkers  = 8
	storeCacheRefreshTimeout  = 3 * time.Second
	storeCacheRefreshAttempts = 3
	storeCacheRefreshBackoff  = 50 * time.Millisecond
)

// StoreCacheStatus is a snapshot of cache entries still working on a store.
type StoreCacheStatus struct {
	StoreID    uint64 `json:"store_id"`
	Matched    int    `json:"matched"`
	Ready      bool   `json:"ready"`
	ObservedAt int64  `json:"observed_at"`
}

// StoreCacheRefreshResult is the outcome of a store-cache refresh against one store.
type StoreCacheRefreshResult struct {
	StoreID    uint64   `json:"store_id"`
	Scanned    int      `json:"scanned"`
	Matched    int      `json:"matched"`
	Updated    int      `json:"updated"`
	Remaining  int      `json:"remaining"`
	Ready      bool     `json:"ready"`
	Errors     []string `json:"errors,omitempty"`
	ObservedAt int64    `json:"observed_at"`
}

// GetStoreCacheStatus counts unexpired cache entries whose working TiKV is storeID.
func (s *KVStore) GetStoreCacheStatus(storeID uint64) StoreCacheStatus {
	n := s.regionCache.CountWorkStoreMatches(storeID)
	return StoreCacheStatus{
		StoreID:    storeID,
		Matched:    n,
		Ready:      n == 0,
		ObservedAt: time.Now().Unix(),
	}
}

// RefreshStoreCache probes the old store with a one-shot Get and updates cache from NotLeader.
// It does not use RegionRequestSender.SendReq, so it will not retry on the new leader.
func (s *KVStore) RefreshStoreCache(ctx context.Context, storeID uint64) StoreCacheRefreshResult {
	matches := s.regionCache.CollectWorkStoreMatches(storeID)
	res := StoreCacheRefreshResult{
		StoreID:    storeID,
		Scanned:    len(matches),
		Matched:    len(matches),
		ObservedAt: time.Now().Unix(),
	}
	if storeID == 0 || len(matches) == 0 {
		res.Ready = true
		return res
	}

	workers := storeCacheRefreshWorkers
	if workers > len(matches) {
		workers = len(matches)
	}
	var updated atomic.Int64
	errCh := make(chan string, len(matches))
	jobs := make(chan locate.WorkStoreMatch, len(matches))
	for _, m := range matches {
		jobs <- m
	}
	close(jobs)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range jobs {
				if ctx.Err() != nil {
					errCh <- ctx.Err().Error()
					continue
				}
				ok, errMsg := s.probeWorkStoreMatch(ctx, storeID, m)
				if ok {
					updated.Add(1)
				} else if errMsg != "" {
					errCh <- errMsg
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if len(res.Errors) < 8 {
			res.Errors = append(res.Errors, e)
		}
	}
	res.Updated = int(updated.Load())
	res.Remaining = s.regionCache.CountWorkStoreMatches(storeID)
	// Expired entries drop out of the match set. Do not report ready if this
	// round failed to update some of the original matches.
	failed := res.Matched - res.Updated
	res.Ready = res.Remaining == 0 && failed == 0
	if res.Remaining == 0 && failed > 0 {
		res.Remaining = failed
	}
	res.ObservedAt = time.Now().Unix()
	return res
}

func (s *KVStore) probeWorkStoreMatch(ctx context.Context, storeID uint64, m locate.WorkStoreMatch) (bool, string) {
	if m.Addr == "" || m.Peer == nil {
		return false, "missing store address or peer"
	}
	var last string
	for attempt := 0; attempt < storeCacheRefreshAttempts; attempt++ {
		if !s.regionCache.StillWorksOnStore(m.Region, storeID) {
			return true, ""
		}
		leader, errMsg := s.sendOneShotGet(ctx, m)
		if errMsg != "" {
			last = errMsg
			select {
			case <-ctx.Done():
				return false, ctx.Err().Error()
			case <-time.After(storeCacheRefreshBackoff):
			}
			continue
		}
		if leader == nil {
			last = "still leader or empty NotLeader"
			select {
			case <-ctx.Done():
				return false, ctx.Err().Error()
			case <-time.After(storeCacheRefreshBackoff):
			}
			continue
		}
		if s.regionCache.UpdateLeaderIfStillOnStore(m.Region, leader, m.AccessIdx, storeID) {
			return true, ""
		}
		return false, "new leader peer not in cache or update lost"
	}
	return false, last
}

func (s *KVStore) sendOneShotGet(ctx context.Context, m locate.WorkStoreMatch) (*metapb.Peer, string) {
	key := m.StartKey
	rpcCtx := kvrpcpb.Context{
		RegionId:    m.Region.GetID(),
		RegionEpoch: m.Epoch,
		Peer:        m.Peer,
		StaleRead:   false,
		ReplicaRead: false,
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: key, Version: 0}, rpcCtx)
	req.StoreTp = tikvrpc.TiKV
	req.ReplicaReadType = kv.ReplicaReadLeader
	req.ForwardedHost = ""

	cli := s.GetTiKVClient()
	if cli == nil {
		return nil, "nil tikv client"
	}
	cctx, cancel := context.WithTimeout(ctx, storeCacheRefreshTimeout)
	defer cancel()
	resp, err := cli.SendRequest(cctx, m.Addr, req, storeCacheRefreshTimeout)
	if err != nil {
		return nil, err.Error()
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, err.Error()
	}
	if regionErr == nil {
		return nil, ""
	}
	nl := regionErr.GetNotLeader()
	if nl == nil {
		return nil, regionErr.GetMessage()
	}
	return nl.GetLeader(), ""
}
