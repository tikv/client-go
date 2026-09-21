// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package tikv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

const (
	storeCacheRefreshWorkers         = 8
	storeCacheRefreshRPCTimeout      = 3 * time.Second
	storeCacheRefreshAttempts        = 3
	storeCacheRefreshBackoff         = 50 * time.Millisecond
	storeCacheRefreshJobTimeout      = 2 * time.Minute
	storeCacheRefreshRecoverBackoff  = 5000
	storeCacheRefreshRecoverMaxSpans = 128
)

// ErrStoreCacheRefreshBusy means reset found an in-flight refresh for this store.
// The running job is left alone; the caller should retry after it finishes.
var ErrStoreCacheRefreshBusy = errors.New("store cache refresh is in progress")

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

type unresolvedFail struct {
	startKey []byte
	endKey   []byte
	err      string
}

type refreshRound struct {
	done chan struct{}
	res  StoreCacheRefreshResult
}

type storeCacheRefreshTask struct {
	mu         sync.Mutex
	running    bool
	cancel     context.CancelFunc
	round      *refreshRound
	unresolved map[locate.RegionVerID]unresolvedFail
}

func (s *KVStore) getRefreshTask(storeID uint64) *storeCacheRefreshTask {
	v, _ := s.refreshTasks.LoadOrStore(storeID, &storeCacheRefreshTask{
		unresolved: make(map[locate.RegionVerID]unresolvedFail),
	})
	return v.(*storeCacheRefreshTask)
}

func (s *KVStore) getRefreshTaskIfAny(storeID uint64) *storeCacheRefreshTask {
	v, ok := s.refreshTasks.Load(storeID)
	if !ok {
		return nil
	}
	return v.(*storeCacheRefreshTask)
}

func (s *KVStore) taskStillLive(storeID uint64, task *storeCacheRefreshTask) bool {
	cur, ok := s.refreshTasks.Load(storeID)
	return ok && cur == task
}

func (s *KVStore) recycleIfIdleReadyLocked(storeID uint64, task *storeCacheRefreshTask, remaining, failed int) {
	if remaining == 0 && failed == 0 && !task.running {
		s.refreshTasks.CompareAndDelete(storeID, task)
	}
}

// dropMovedUnresolvedLocked removes failures only when the original key range is
// fully covered by unexpired cached regions that no longer work on storeID.
// Cache TTL expiry is not success: Operator fallback is a separate wait, and
// leftover failures are dropped only by ResetStoreCacheRefresh.
func (s *KVStore) dropMovedUnresolvedLocked(task *storeCacheRefreshTask, storeID uint64) {
	var idx *locate.WorkSpanIndex
	for id, u := range task.unresolved {
		switch s.regionCache.ClassifyWorkStore(id, storeID) {
		case locate.WorkStoreMoved:
			delete(task.unresolved, id)
		case locate.WorkStoreGone:
			if idx == nil {
				idx = s.regionCache.NewWorkSpanIndex(storeID)
			}
			if idx.RangeResolved(u.startKey, u.endKey) {
				delete(task.unresolved, id)
			}
		}
	}
}

func (s *KVStore) stopRefreshTasks() {
	var waits []chan struct{}
	s.refreshTasks.Range(func(_, v any) bool {
		task := v.(*storeCacheRefreshTask)
		task.mu.Lock()
		if task.running {
			if task.cancel != nil {
				task.cancel()
			}
			if task.round != nil {
				waits = append(waits, task.round.done)
			}
		}
		task.mu.Unlock()
		return true
	})
	for _, ch := range waits {
		<-ch
	}
	s.refreshTasks.Range(func(k, v any) bool {
		s.refreshTasks.CompareAndDelete(k, v)
		return true
	})
}

// ResetStoreCacheRefresh drops unresolved failures and the idle task for storeID.
// It does not cancel or wait for an in-flight refresh: that returns
// ErrStoreCacheRefreshBusy so the caller can retry. It does not mark a refresh
// successful. Operator fallback is: stop waiting for ready, wait the region-cache
// TTL, then restart; pass reset=1 on the next rolling's POST so leftover failures
// from that fallback do not stick.
func (s *KVStore) ResetStoreCacheRefresh(storeID uint64) error {
	if storeID == 0 {
		return nil
	}
	for {
		task := s.getRefreshTaskIfAny(storeID)
		if task == nil {
			return nil
		}
		task.mu.Lock()
		if !s.taskStillLive(storeID, task) {
			task.mu.Unlock()
			continue
		}
		if task.running {
			task.mu.Unlock()
			return ErrStoreCacheRefreshBusy
		}
		task.unresolved = make(map[locate.RegionVerID]unresolvedFail)
		s.refreshTasks.CompareAndDelete(storeID, task)
		task.mu.Unlock()
		return nil
	}
}

func closedStoreCacheResult(storeID uint64) StoreCacheRefreshResult {
	return StoreCacheRefreshResult{
		StoreID:    storeID,
		Errors:     []string{"store is closed"},
		ObservedAt: time.Now().Unix(),
	}
}

// GetStoreCacheStatus counts unexpired cache entries whose working TiKV is storeID,
// plus unresolved failures from previous refreshes of this store.
func (s *KVStore) GetStoreCacheStatus(storeID uint64) StoreCacheStatus {
	n := s.regionCache.CountWorkStoreMatches(storeID)
	task := s.getRefreshTaskIfAny(storeID)
	if task == nil {
		return StoreCacheStatus{
			StoreID:    storeID,
			Matched:    n,
			Ready:      n == 0,
			ObservedAt: time.Now().Unix(),
		}
	}
	task.mu.Lock()
	s.dropMovedUnresolvedLocked(task, storeID)
	failed := len(task.unresolved)
	inProgress := task.running
	s.recycleIfIdleReadyLocked(storeID, task, n, failed)
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
	return s.refreshStoreCache(ctx, storeID, nil)
}

// afterLoad runs after the task is loaded and before it is locked. Tests use it
// to interleave recycle with a stale pointer; production passes nil.
func (s *KVStore) refreshStoreCache(ctx context.Context, storeID uint64, afterLoad func()) StoreCacheRefreshResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if storeID == 0 {
		return StoreCacheRefreshResult{Ready: true, ObservedAt: time.Now().Unix()}
	}
	if s.IsClose() {
		return closedStoreCacheResult(storeID)
	}
	var (
		task      *storeCacheRefreshTask
		round     *refreshRound
		jobCtx    context.Context
		cancel    context.CancelFunc
		stopAfter func() bool
	)
	for {
		if s.IsClose() {
			return closedStoreCacheResult(storeID)
		}
		task = s.getRefreshTask(storeID)
		if afterLoad != nil {
			afterLoad()
			afterLoad = nil
		}
		task.mu.Lock()
		if s.IsClose() || !s.taskStillLive(storeID, task) {
			task.mu.Unlock()
			continue
		}
		if task.running {
			waitRound := task.round
			task.mu.Unlock()
			if waitRound == nil {
				continue
			}
			select {
			case <-ctx.Done():
				return StoreCacheRefreshResult{StoreID: storeID, Errors: []string{ctx.Err().Error()}, ObservedAt: time.Now().Unix()}
			case <-waitRound.done:
				return waitRound.res
			}
		}
		round = &refreshRound{done: make(chan struct{})}
		task.running = true
		task.round = round
		parent := s.ctx
		if parent == nil {
			parent = context.Background()
		}
		jobCtx, cancel = context.WithTimeout(parent, storeCacheRefreshJobTimeout)
		jobCtx = util.WithInternalSourceType(jobCtx, util.InternalTxnStoreCacheRefresh)
		if ctx != nil {
			stopAfter = context.AfterFunc(ctx, cancel)
		}
		task.cancel = cancel
		task.mu.Unlock()
		break
	}

	res, events := s.runRefresh(jobCtx, storeID)
	if stopAfter != nil {
		stopAfter()
	}

	task.mu.Lock()
	if task.unresolved == nil {
		task.unresolved = make(map[locate.RegionVerID]unresolvedFail)
	}
	for _, ev := range events {
		switch ev.outcome {
		case probeFailed:
			task.unresolved[ev.id] = unresolvedFail{
				startKey: append([]byte(nil), ev.startKey...),
				endKey:   append([]byte(nil), ev.endKey...),
				err:      ev.err,
			}
		}
	}
	s.dropMovedUnresolvedLocked(task, storeID)
	res.Failed = len(task.unresolved)
	res.Ready = res.Remaining == 0 && res.Failed == 0
	for _, u := range task.unresolved {
		if u.err != "" && len(res.Errors) < 8 {
			res.Errors = append(res.Errors, u.err)
		}
	}
	round.res = res
	task.running = false
	task.cancel = nil
	close(round.done)
	s.recycleIfIdleReadyLocked(storeID, task, res.Remaining, res.Failed)
	task.mu.Unlock()
	cancel()
	return res
}

type probeEvent struct {
	id       locate.RegionVerID
	startKey []byte
	endKey   []byte
	outcome  probeOutcome
	err      string
}

func (s *KVStore) runRefresh(ctx context.Context, storeID uint64) (StoreCacheRefreshResult, []probeEvent) {
	matches := s.regionCache.CollectWorkStoreMatches(storeID)
	have := make(map[locate.RegionVerID]struct{}, len(matches))
	for _, m := range matches {
		have[m.Region] = struct{}{}
	}
	retryMatches, early := s.recoverGoneUnresolved(ctx, storeID, have)
	matches = append(matches, retryMatches...)
	res := StoreCacheRefreshResult{
		StoreID:    storeID,
		Scanned:    len(matches),
		Matched:    len(matches),
		ObservedAt: time.Now().Unix(),
	}
	if len(matches) == 0 {
		res.Remaining = s.regionCache.CountWorkStoreMatches(storeID)
		res.ObservedAt = time.Now().Unix()
		return res, early
	}

	workers := storeCacheRefreshWorkers
	if workers > len(matches) {
		workers = len(matches)
	}
	var updated atomic.Int64
	events := make(chan probeEvent, len(matches))
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
					events <- probeEvent{id: m.Region, startKey: m.StartKey, endKey: m.EndKey, outcome: probeFailed, err: ctx.Err().Error()}
					continue
				}
				outcome, errMsg := s.probeWorkStoreMatch(ctx, storeID, m)
				if outcome == probeApplied {
					updated.Add(1)
				}
				events <- probeEvent{id: m.Region, startKey: m.StartKey, endKey: m.EndKey, outcome: outcome, err: errMsg}
			}
		}()
	}
	wg.Wait()
	close(events)
	evs := append([]probeEvent(nil), early...)
	for ev := range events {
		evs = append(evs, ev)
		if ev.outcome == probeFailed && ev.err != "" && len(res.Errors) < 8 {
			res.Errors = append(res.Errors, ev.err)
		}
	}
	res.Updated = int(updated.Load())
	res.Remaining = s.regionCache.CountWorkStoreMatches(storeID)
	res.ObservedAt = time.Now().Unix()
	return res, evs
}

// recoverGoneUnresolved re-locates key ranges of failures whose cache is gone
// and not covered by moved cache. It does not drop failures here: probeMoved
// events let dropMovedUnresolvedLocked / event apply delete them. Range lookup
// may hit PD; that is only for stuck leftovers, not the hot B2 path.
func (s *KVStore) recoverGoneUnresolved(ctx context.Context, storeID uint64, have map[locate.RegionVerID]struct{}) ([]locate.WorkStoreMatch, []probeEvent) {
	task := s.getRefreshTaskIfAny(storeID)
	if task == nil {
		return nil, nil
	}
	type gone struct {
		id locate.RegionVerID
		u  unresolvedFail
	}
	var items []gone
	task.mu.Lock()
	for id, u := range task.unresolved {
		if _, ok := have[id]; ok {
			continue
		}
		if s.regionCache.ClassifyWorkStore(id, storeID) != locate.WorkStoreGone {
			continue
		}
		items = append(items, gone{id: id, u: u})
	}
	task.mu.Unlock()
	if len(items) == 0 {
		return nil, nil
	}
	idx := s.regionCache.NewWorkSpanIndex(storeID)
	type locateCandidate struct {
		item     gone
		startKey []byte
	}
	var candidates []locateCandidate
	var evs []probeEvent
	located := false
	for _, it := range items {
		if ctx.Err() != nil {
			break
		}
		if idx.RangeResolved(it.u.startKey, it.u.endKey) {
			evs = append(evs, probeEvent{id: it.id, startKey: it.u.startKey, endKey: it.u.endKey, outcome: probeMoved})
			continue
		}
		cur, complete := idx.CoveredTo(it.u.startKey, it.u.endKey)
		if complete {
			// Fully cached but not resolved: remaining work is on-store
			// probes, not another prefix walk from startKey.
			continue
		}
		candidates = append(candidates, locateCandidate{item: it, startKey: cur})
	}
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			break
		}
		if s.locateFailedRange(ctx, candidate.startKey, candidate.item.u.endKey) {
			located = true
		}
	}
	if located {
		// LocateKey updates the cache. Rebuild once after the batch of walks so
		// split/merge replacements are checked against a consistent snapshot.
		idx = s.regionCache.NewWorkSpanIndex(storeID)
	}
	for _, candidate := range candidates {
		it := candidate.item
		if idx.RangeResolved(it.u.startKey, it.u.endKey) {
			evs = append(evs, probeEvent{id: it.id, startKey: it.u.startKey, endKey: it.u.endKey, outcome: probeMoved})
		}
	}
	if !located {
		return nil, evs
	}
	var extra []locate.WorkStoreMatch
	for _, m := range s.regionCache.CollectWorkStoreMatches(storeID) {
		if _, ok := have[m.Region]; ok {
			continue
		}
		have[m.Region] = struct{}{}
		extra = append(extra, m)
	}
	return extra, evs
}

// locateFailedRange walks [startKey, endKey) so a split sibling after startKey
// is loaded. The caller should pass the first uncached hole, not the original
// start, so a later round is not spent re-walking an already cached prefix.
// Stops on cancel, backoff budget, max spans, or a region that does not advance.
func (s *KVStore) locateFailedRange(ctx context.Context, startKey, endKey []byte) bool {
	if ctx.Err() != nil {
		return false
	}
	bo := NewBackofferWithVars(ctx, storeCacheRefreshRecoverBackoff, nil)
	cur := startKey
	progressed := false
	for i := 0; i < storeCacheRefreshRecoverMaxSpans; i++ {
		if ctx.Err() != nil {
			return progressed
		}
		loc, err := s.regionCache.LocateKey(bo, cur)
		if err != nil || loc == nil {
			return progressed
		}
		progressed = true
		if len(loc.EndKey) == 0 {
			return true
		}
		if len(endKey) > 0 && bytes.Compare(loc.EndKey, endKey) >= 0 {
			return true
		}
		if bytes.Compare(loc.EndKey, cur) <= 0 {
			return progressed
		}
		cur = loc.EndKey
		if len(endKey) > 0 && bytes.Compare(cur, endKey) >= 0 {
			return true
		}
	}
	return progressed
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
	reloaded := false
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
		leader, reload, errMsg := s.sendOneShotGet(ctx, m)
		if errMsg != "" {
			last = errMsg
			if reload && !reloaded {
				reloaded = true
				next, moved, ok := s.reloadOriginalMatch(ctx, m, storeID)
				if moved {
					return probeMoved, ""
				}
				if !ok {
					return probeFailed, last
				}
				m = next
				continue
			}
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
		last = s.peerMissDetail(m, storeID, leader, "new_leader_peer_not_in_cache")
		if reloaded {
			return probeFailed, s.peerMissDetail(m, storeID, leader, "new_leader_peer_not_in_cache_after_reload")
		}
		reloaded = true
		next, moved, ok := s.reloadOriginalMatch(ctx, m, storeID)
		if moved {
			return probeMoved, ""
		}
		if !ok {
			return probeFailed, last
		}
		// Drop the stale NotLeader peer. Re-probe only from the new snapshot.
		m = next
		continue
	}
	return probeFailed, last
}

func (s *KVStore) reloadOriginalMatch(ctx context.Context, orig locate.WorkStoreMatch, storeID uint64) (locate.WorkStoreMatch, bool, bool) {
	s.regionCache.InvalidateCachedRegion(orig.Region)
	bo := NewBackofferWithVars(ctx, storeCacheRefreshRecoverBackoff, nil)
	loc, err := s.regionCache.LocateKey(bo, orig.StartKey)
	if err != nil || loc == nil {
		return locate.WorkStoreMatch{}, false, false
	}
	// A replacement at the original start key is not enough to resolve the
	// failed range: a split sibling may still be uncached or may still work on
	// storeID. Walk the original span once when needed, then check the whole
	// range before reporting it moved.
	resolved := s.regionCache.NewWorkSpanIndex(storeID).RangeResolved(orig.StartKey, orig.EndKey)
	switch s.regionCache.ClassifyWorkStore(loc.Region, storeID) {
	case locate.WorkStoreMoved:
		if !resolved {
			s.locateFailedRange(ctx, orig.StartKey, orig.EndKey)
			resolved = s.regionCache.NewWorkSpanIndex(storeID).RangeResolved(orig.StartKey, orig.EndKey)
		}
		if resolved {
			return locate.WorkStoreMatch{}, true, true
		}
		return locate.WorkStoreMatch{}, false, false
	case locate.WorkStoreGone:
		return locate.WorkStoreMatch{}, false, false
	}
	m, ok := s.regionCache.WorkStoreMatchOnStore(loc.Region, storeID)
	if !ok {
		return locate.WorkStoreMatch{}, false, false
	}
	return m, false, true
}

func (s *KVStore) peerMissDetail(m locate.WorkStoreMatch, storeID uint64, leader *metapb.Peer, class string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "class=%s region=%d store=%d", class, m.Region.GetID(), storeID)
	if r := s.regionCache.GetCachedRegionWithRLock(m.Region); r != nil && r.GetMeta() != nil {
		b.WriteString(" peers=")
		for i, p := range r.GetMeta().GetPeers() {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%d", p.GetId())
		}
	}
	if leader != nil {
		fmt.Fprintf(&b, " not_leader_peer=%d", leader.GetId())
	}
	return b.String()
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

func (s *KVStore) sendOneShotGet(ctx context.Context, m locate.WorkStoreMatch) (*metapb.Peer, bool, string) {
	rpcCtx := kvrpcpb.Context{
		RegionId:      m.Region.GetID(),
		RegionEpoch:   m.Epoch,
		Peer:          m.Peer,
		StaleRead:     false,
		ReplicaRead:   false,
		RequestSource: util.BuildRequestSource(true, util.InternalTxnStoreCacheRefresh, ""),
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: m.StartKey, Version: math.MaxUint64}, rpcCtx)
	req.StoreTp = tikvrpc.TiKV
	req.ReplicaReadType = kv.ReplicaReadLeader
	req.ForwardedHost = ""

	cli := s.GetTiKVClient()
	if cli == nil {
		return nil, false, "nil tikv client"
	}
	cctx, cancel := context.WithTimeout(ctx, storeCacheRefreshRPCTimeout)
	defer cancel()
	resp, err := cli.SendRequest(cctx, m.Addr, req, storeCacheRefreshRPCTimeout)
	if err != nil {
		return nil, false, err.Error()
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, false, err.Error()
	}
	if regionErr == nil {
		return nil, false, ""
	}
	nl := regionErr.GetNotLeader()
	if nl == nil {
		message := regionErr.GetMessage()
		if message == "" {
			message = regionErr.String()
		}
		return nil, needsRegionReload(regionErr), message
	}
	return nl.GetLeader(), false, ""
}

func needsRegionReload(regionErr *errorpb.Error) bool {
	return regionErr != nil && (regionErr.GetEpochNotMatch() != nil ||
		regionErr.GetRegionNotFound() != nil || regionErr.GetKeyNotInRegion() != nil)
}
