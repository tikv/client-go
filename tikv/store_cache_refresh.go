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
	storeCacheRefreshWorkers    = 8
	storeCacheRefreshRPCTimeout = 3 * time.Second
	storeCacheRefreshAttempts   = 3
	storeCacheRefreshBackoff    = 50 * time.Millisecond
	storeCacheRefreshJobTimeout = 2 * time.Minute
)

// StoreCacheStatus is a snapshot of cache entries still working on a store.
type StoreCacheStatus struct {
	StoreID    uint64 `json:"store_id"`
	Matched    int    `json:"matched"`
	Failed     int    `json:"failed,omitempty"`
	Ready      bool   `json:"ready"`
	InProgress bool   `json:"in_progress,omitempty"`
	ObservedAt int64  `json:"observed_at"`
}

// StoreCacheRefreshResult is the outcome of a store-cache refresh against one store.
type StoreCacheRefreshResult struct {
	StoreID    uint64   `json:"store_id"`
	Scanned    int      `json:"scanned"`
	Matched    int      `json:"matched"`
	Updated    int      `json:"updated"`
	Failed     int      `json:"failed"`
	Remaining  int      `json:"remaining"`
	Ready      bool     `json:"ready"`
	Errors     []string `json:"errors,omitempty"`
	ObservedAt int64    `json:"observed_at"`
}

type storeCacheRefreshTask struct {
	mu      sync.Mutex
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
	last    StoreCacheRefreshResult
	failed  int
}

func (s *KVStore) getRefreshTask(storeID uint64) *storeCacheRefreshTask {
	v, _ := s.refreshTasks.LoadOrStore(storeID, &storeCacheRefreshTask{})
	return v.(*storeCacheRefreshTask)
}

// GetStoreCacheStatus counts unexpired cache entries whose working TiKV is storeID,
// plus unresolved failures from the last refresh of this store.
func (s *KVStore) GetStoreCacheStatus(storeID uint64) StoreCacheStatus {
	n := s.regionCache.CountWorkStoreMatches(storeID)
	task := s.getRefreshTask(storeID)
	task.mu.Lock()
	failed := task.failed
	inProgress := task.running
	task.mu.Unlock()
	return StoreCacheStatus{
		StoreID:    storeID,
		Matched:    n,
		Failed:     failed,
		Ready:      n == 0 && failed == 0 && !inProgress,
		InProgress: inProgress,
		ObservedAt: time.Now().Unix(),
	}
}

// RefreshStoreCache probes the old store with a one-shot Get and updates cache from NotLeader.
// It does not use RegionRequestSender.SendReq, so it will not retry on the new leader.
// Concurrent calls for the same store wait for the in-flight job instead of stacking workers.
func (s *KVStore) RefreshStoreCache(ctx context.Context, storeID uint64) StoreCacheRefreshResult {
	if storeID == 0 {
		return StoreCacheRefreshResult{Ready: true, ObservedAt: time.Now().Unix()}
	}
	task := s.getRefreshTask(storeID)
	task.mu.Lock()
	if task.running {
		done := task.done
		task.mu.Unlock()
		select {
		case <-ctx.Done():
			return StoreCacheRefreshResult{StoreID: storeID, Errors: []string{ctx.Err().Error()}, ObservedAt: time.Now().Unix()}
		case <-done:
			task.mu.Lock()
			last := task.last
			task.mu.Unlock()
			return last
		}
	}
	task.running = true
	task.done = make(chan struct{})
	prevFailed := task.failed
	jobCtx, cancel := context.WithTimeout(ctx, storeCacheRefreshJobTimeout)
	task.cancel = cancel
	task.mu.Unlock()

	res := s.runRefresh(jobCtx, storeID)
	if res.Matched == 0 && prevFailed > 0 {
		res.Failed = prevFailed
		res.Remaining = prevFailed
		res.Ready = false
	}

	task.mu.Lock()
	task.last = res
	task.failed = res.Failed
	task.running = false
	task.cancel = nil
	close(task.done)
	task.mu.Unlock()
	cancel()
	return res
}

func (s *KVStore) runRefresh(ctx context.Context, storeID uint64) StoreCacheRefreshResult {
	matches := s.regionCache.CollectWorkStoreMatches(storeID)
	res := StoreCacheRefreshResult{
		StoreID:    storeID,
		Scanned:    len(matches),
		Matched:    len(matches),
		ObservedAt: time.Now().Unix(),
	}
	if len(matches) == 0 {
		res.Ready = true
		return res
	}

	workers := storeCacheRefreshWorkers
	if workers > len(matches) {
		workers = len(matches)
	}
	var updated, failed atomic.Int64
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
					failed.Add(1)
					errCh <- ctx.Err().Error()
					continue
				}
				outcome, errMsg := s.probeWorkStoreMatch(ctx, storeID, m)
				switch outcome {
				case probeApplied:
					updated.Add(1)
				case probeFailed:
					failed.Add(1)
					if errMsg != "" {
						errCh <- errMsg
					}
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
	res.Failed = int(failed.Load())
	res.Remaining = s.regionCache.CountWorkStoreMatches(storeID)
	res.Ready = res.Failed == 0 && res.Remaining == 0
	res.ObservedAt = time.Now().Unix()
	return res
}

type probeOutcome int

const (
	probeApplied probeOutcome = iota
	probeMoved
	probeFailed
)

func (s *KVStore) probeWorkStoreMatch(ctx context.Context, storeID uint64, m locate.WorkStoreMatch) (probeOutcome, string) {
	if m.Addr == "" || m.Peer == nil {
		return probeFailed, "missing store address or peer"
	}
	var last string
	for attempt := 0; attempt < storeCacheRefreshAttempts; attempt++ {
		if ctx.Err() != nil {
			return probeFailed, ctx.Err().Error()
		}
		switch s.regionCache.ClassifyWorkStore(m.Region, storeID) {
		case locate.WorkStoreMoved:
			return probeMoved, ""
		case locate.WorkStoreGone:
			return probeFailed, "cache entry expired or removed"
		}
		leader, errMsg := s.sendOneShotGet(ctx, m)
		if errMsg != "" {
			last = errMsg
			if attempt+1 < storeCacheRefreshAttempts && !sleepCtx(ctx, storeCacheRefreshBackoff) {
				return probeFailed, ctx.Err().Error()
			}
			continue
		}
		if leader == nil {
			last = "still leader or empty NotLeader"
			if attempt+1 < storeCacheRefreshAttempts && !sleepCtx(ctx, storeCacheRefreshBackoff) {
				return probeFailed, ctx.Err().Error()
			}
			continue
		}
		applied, moved := s.regionCache.ApplyLeaderIfOnStore(m.Region, leader, storeID)
		if applied {
			return probeApplied, ""
		}
		if moved {
			return probeMoved, ""
		}
		return probeFailed, "new leader peer not in cache"
	}
	return probeFailed, last
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (s *KVStore) sendOneShotGet(ctx context.Context, m locate.WorkStoreMatch) (*metapb.Peer, string) {
	rpcCtx := kvrpcpb.Context{
		RegionId:    m.Region.GetID(),
		RegionEpoch: m.Epoch,
		Peer:        m.Peer,
		StaleRead:   false,
		ReplicaRead: false,
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: m.StartKey, Version: 0}, rpcCtx)
	req.StoreTp = tikvrpc.TiKV
	req.ReplicaReadType = kv.ReplicaReadLeader
	req.ForwardedHost = ""

	cli := s.GetTiKVClient()
	if cli == nil {
		return nil, "nil tikv client"
	}
	cctx, cancel := context.WithTimeout(ctx, storeCacheRefreshRPCTimeout)
	defer cancel()
	resp, err := cli.SendRequest(cctx, m.Addr, req, storeCacheRefreshRPCTimeout)
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
