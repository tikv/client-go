// Copyright 2021 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/region_cache.go
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
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	btreeDegree            = 32
	expiredTTL             = -1
	defaultRegionsPerBatch = 128
)

// LabelFilter returns false means label doesn't match, and will ignore this store.
type LabelFilter = func(labels []*metapb.StoreLabel) bool

// LabelFilterOnlyTiFlashWriteNode will only select stores whose label contains: <engine, tiflash> and <engine_role, write>.
// Only used for tiflash_compute node.
var LabelFilterOnlyTiFlashWriteNode = func(labels []*metapb.StoreLabel) bool {
	return isStoreContainLabel(labels, tikvrpc.EngineLabelKey, tikvrpc.EngineLabelTiFlash) &&
		isStoreContainLabel(labels, tikvrpc.EngineRoleLabelKey, tikvrpc.EngineRoleWrite)
}

// LabelFilterNoTiFlashWriteNode will only select stores whose label contains: <engine, tiflash>, but not contains <engine_role, write>.
// Normally tidb use this filter.
var LabelFilterNoTiFlashWriteNode = func(labels []*metapb.StoreLabel) bool {
	return isStoreContainLabel(labels, tikvrpc.EngineLabelKey, tikvrpc.EngineLabelTiFlash) &&
		!isStoreContainLabel(labels, tikvrpc.EngineRoleLabelKey, tikvrpc.EngineRoleWrite)
}

// LabelFilterAllTiFlashNode will select all tiflash stores.
var LabelFilterAllTiFlashNode = func(labels []*metapb.StoreLabel) bool {
	return isStoreContainLabel(labels, tikvrpc.EngineLabelKey, tikvrpc.EngineLabelTiFlash)
}

// LabelFilterAllNode will select all stores.
var LabelFilterAllNode = func(_ []*metapb.StoreLabel) bool {
	return true
}

// regionCacheTTLSec is the max idle time for regions in the region cache.
var regionCacheTTLSec int64 = 600

// SetRegionCacheTTLSec sets regionCacheTTLSec to t.
func SetRegionCacheTTLSec(t int64) {
	regionCacheTTLSec = t
}

// regionCacheTTLJitterSec is the max jitter time for region cache TTL.
var regionCacheTTLJitterSec int64 = 60

// SetRegionCacheTTLWithJitter sets region cache TTL with jitter. The real TTL is in range of [base, base+jitter).
func SetRegionCacheTTLWithJitter(base int64, jitter int64) {
	regionCacheTTLSec = base
	regionCacheTTLJitterSec = jitter
}

// nextTTL returns a random TTL in range [ts+base, ts+base+jitter). The input ts should be an epoch timestamp in seconds.
func nextTTL(ts int64) int64 {
	jitter := int64(0)
	if regionCacheTTLJitterSec > 0 {
		jitter = rand.Int63n(regionCacheTTLJitterSec)
	}
	return ts + regionCacheTTLSec + jitter
}

// nextTTLWithoutJitter is used for test.
func nextTTLWithoutJitter(ts int64) int64 {
	return ts + regionCacheTTLSec
}

const (
	needReloadOnAccess       int32 = 1 << iota // indicates the region will be reloaded on next access
	needExpireAfterTTL                         // indicates the region will expire after RegionCacheTTL (even when it's accessed continuously)
	needDelayedReloadPending                   // indicates the region will be reloaded later after it's scanned by GC
	needDelayedReloadReady                     // indicates the region has been scanned by GC and can be reloaded by id on next access
)

// InvalidReason is the reason why a cached region is invalidated.
// The region cache may take different strategies to handle different reasons.
// For example, when a cached region is invalidated due to no leader, region cache
// will always access to a different peer.
type InvalidReason int32

const (
	// Ok indicates the cached region is valid
	Ok InvalidReason = iota
	// NoLeader indicates it's invalidated due to no leader
	NoLeader
	// RegionNotFound indicates it's invalidated due to region not found in the store
	RegionNotFound
	// EpochNotMatch indicates it's invalidated due to epoch not match
	EpochNotMatch
	// StoreNotFound indicates it's invalidated due to store not found in PD
	StoreNotFound
	// Other indicates it's invalidated due to other reasons, e.g., the store
	// is removed from the cluster, fail to send requests to the store.
	Other
)

func (r InvalidReason) String() string {
	switch r {
	case Ok:
		return "Ok"
	case Other:
		return "Other"
	case EpochNotMatch:
		return "EpochNotMatch"
	case RegionNotFound:
		return "RegionNotFound"
	case StoreNotFound:
		return "StoreNotFound"
	case NoLeader:
		return "NoLeader"
	default:
		return "Unknown"
	}
}

// Region presents kv region
type Region struct {
	meta          *metapb.Region // raw region meta from PD, immutable after init
	store         unsafe.Pointer // point to region store info, see RegionStore
	ttl           int64          // region TTL in epoch seconds, see checkRegionCacheTTL
	syncFlags     int32          // region need be sync later, see needReloadOnAccess, needExpireAfterTTL
	invalidReason InvalidReason  // the reason why the region is invalidated
}

// AccessIndex represent the index for accessIndex array
type AccessIndex int

// regionStore represents region stores info
// it will be store as unsafe.Pointer and be load at once
type regionStore struct {
	// corresponding stores(in the same order) of Region.meta.Peers in this region.
	stores []*Store
	// snapshots of store's epoch, need reload when `storeEpochs[curr] != stores[cur].fail`
	storeEpochs []uint32
	// A region can consist of stores with different type(TiKV and TiFlash). It maintains AccessMode => idx in stores,
	// e.g., stores[accessIndex[tiKVOnly][workTiKVIdx]] is the current working TiKV.
	accessIndex [numAccessMode][]int
	// accessIndex[tiKVOnly][workTiKVIdx] is the index of the current working TiKV in stores.
	workTiKVIdx AccessIndex
	// accessIndex[tiKVOnly][proxyTiKVIdx] is the index of TiKV that can forward requests to the leader in stores, -1 means not using proxy.
	proxyTiKVIdx AccessIndex
	// accessIndex[tiFlashOnly][workTiFlashIdx] is the index of the current working TiFlash in stores.
	workTiFlashIdx atomic.Int32
	// buckets is not accurate and it can change even if the region is not changed.
	// It can be stale and buckets keys can be out of the region range.
	buckets *metapb.Buckets
	// pendingPeers refers to pdRegion.PendingPeers. It's immutable and can be used to reconstruct pdRegions.
	pendingPeers []*metapb.Peer
	// downPeers refers to pdRegion.DownPeers. It's immutable and can be used to reconstruct pdRegions.
	downPeers []*metapb.Peer
}

func (r *regionStore) accessStore(mode accessMode, idx AccessIndex) (int, *Store) {
	sidx := r.accessIndex[mode][idx]
	return sidx, r.stores[sidx]
}

func (r *regionStore) getAccessIndex(mode accessMode, store *Store) AccessIndex {
	for index, sidx := range r.accessIndex[mode] {
		if r.stores[sidx].storeID == store.storeID {
			return AccessIndex(index)
		}
	}
	return -1
}

func (r *regionStore) accessStoreNum(mode accessMode) int {
	return len(r.accessIndex[mode])
}

// clone clones region store struct.
func (r *regionStore) clone() *regionStore {
	storeEpochs := make([]uint32, len(r.stores))
	copy(storeEpochs, r.storeEpochs)
	rs := &regionStore{
		proxyTiKVIdx: r.proxyTiKVIdx,
		workTiKVIdx:  r.workTiKVIdx,
		stores:       r.stores,
		storeEpochs:  storeEpochs,
		buckets:      r.buckets,
	}
	rs.workTiFlashIdx.Store(r.workTiFlashIdx.Load())
	for i := 0; i < int(numAccessMode); i++ {
		rs.accessIndex[i] = make([]int, len(r.accessIndex[i]))
		copy(rs.accessIndex[i], r.accessIndex[i])
	}
	return rs
}

// return next follower store's index
func (r *regionStore) follower(seed uint32, op *storeSelectorOp) AccessIndex {
	l := uint32(r.accessStoreNum(tiKVOnly))
	if l <= 1 {
		return r.workTiKVIdx
	}

	for retry := l - 1; retry > 0; retry-- {
		followerIdx := AccessIndex(seed % (l - 1))
		if followerIdx >= r.workTiKVIdx {
			followerIdx++
		}
		storeIdx, s := r.accessStore(tiKVOnly, followerIdx)
		if r.storeEpochs[storeIdx] == atomic.LoadUint32(&s.epoch) && r.filterStoreCandidate(followerIdx, op) {
			return followerIdx
		}
		seed++
	}
	return r.workTiKVIdx
}

// return next leader or follower store's index
func (r *regionStore) kvPeer(seed uint32, op *storeSelectorOp) AccessIndex {
	if op.leaderOnly {
		return r.workTiKVIdx
	}
	candidates := make([]AccessIndex, 0, r.accessStoreNum(tiKVOnly))
	for i := 0; i < r.accessStoreNum(tiKVOnly); i++ {
		accessIdx := AccessIndex(i)
		storeIdx, s := r.accessStore(tiKVOnly, accessIdx)
		if r.storeEpochs[storeIdx] != atomic.LoadUint32(&s.epoch) || !r.filterStoreCandidate(accessIdx, op) {
			continue
		}
		candidates = append(candidates, accessIdx)
	}
	// If there is no candidates, send to current workTiKVIdx which generally is the leader.
	if len(candidates) == 0 {
		return r.workTiKVIdx
	}
	return candidates[seed%uint32(len(candidates))]
}

func (r *regionStore) filterStoreCandidate(aidx AccessIndex, op *storeSelectorOp) bool {
	_, s := r.accessStore(tiKVOnly, aidx)
	// filter label unmatched store and slow stores when ReplicaReadMode == PreferLeader
	return s.IsLabelsMatch(op.labels) && (!op.preferLeader || (aidx == r.workTiKVIdx && !s.healthStatus.IsSlow()))
}

func newRegion(bo *retry.Backoffer, c *RegionCache, pdRegion *pd.Region) (*Region, error) {
	r := &Region{meta: pdRegion.Meta}
	// regionStore pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &regionStore{
		workTiKVIdx:  0,
		proxyTiKVIdx: -1,
		stores:       make([]*Store, 0, len(r.meta.Peers)),
		storeEpochs:  make([]uint32, 0, len(r.meta.Peers)),
		buckets:      pdRegion.Buckets,
		pendingPeers: pdRegion.PendingPeers,
		downPeers:    pdRegion.DownPeers,
	}

	leader := pdRegion.Leader
	var leaderAccessIdx AccessIndex
	availablePeers := r.meta.GetPeers()[:0]
	for _, p := range r.meta.Peers {
		store, exists := c.stores.get(p.StoreId)
		if !exists {
			store = c.stores.getOrInsertDefault(p.StoreId)
		}
		addr, err := store.initResolve(bo, c.stores)
		if err != nil {
			return nil, err
		}
		// Filter out the peer on a tombstone or down store.
		if addr == "" || slices.ContainsFunc(pdRegion.DownPeers, func(dp *metapb.Peer) bool { return isSamePeer(dp, p) }) {
			continue
		}

		// Since the witness is read-write prohibited, it does not make sense to send requests to it
		// unless it is the leader. When it is the leader and transfer leader encounters a problem,
		// the backoff timeout will be triggered, and the client can give a more accurate error message.
		if p.IsWitness && !isSamePeer(p, leader) {
			continue
		}

		if isSamePeer(p, leader) {
			leaderAccessIdx = AccessIndex(len(rs.accessIndex[tiKVOnly]))
		}
		availablePeers = append(availablePeers, p)
		switch store.storeType {
		case tikvrpc.TiKV:
			rs.accessIndex[tiKVOnly] = append(rs.accessIndex[tiKVOnly], len(rs.stores))
		case tikvrpc.TiFlash:
			rs.accessIndex[tiFlashOnly] = append(rs.accessIndex[tiFlashOnly], len(rs.stores))
		}
		rs.stores = append(rs.stores, store)
		rs.storeEpochs = append(rs.storeEpochs, atomic.LoadUint32(&store.epoch))
	}
	// TODO(youjiali1995): It's possible the region info in PD is stale for now but it can recover.
	// Maybe we need backoff here.
	if len(availablePeers) == 0 {
		return nil, errors.Errorf("no available peers, region: {%v}", r.meta)
	}

	rs.workTiKVIdx = leaderAccessIdx
	r.setStore(rs)
	r.meta.Peers = availablePeers
	// if the region has down peers, let it expire after TTL.
	if len(pdRegion.DownPeers) > 0 {
		r.syncFlags |= needExpireAfterTTL
	}

	// mark region has been init accessed.
	r.ttl = nextTTL(time.Now().Unix())
	return r, nil
}

func (r *Region) getStore() (store *regionStore) {
	store = (*regionStore)(atomic.LoadPointer(&r.store))
	return
}

func (r *Region) setStore(store *regionStore) {
	atomic.StorePointer(&r.store, unsafe.Pointer(store))
}

func (r *Region) compareAndSwapStore(oldStore, newStore *regionStore) bool {
	return atomic.CompareAndSwapPointer(&r.store, unsafe.Pointer(oldStore), unsafe.Pointer(newStore))
}

func (r *Region) isCacheTTLExpired(ts int64) bool {
	return ts > atomic.LoadInt64(&r.ttl)
}

// checkRegionCacheTTL returns false means the region cache is expired.
func (r *Region) checkRegionCacheTTL(ts int64) bool {
	// Only consider use percentage on this failpoint, for example, "2%return"
	if _, err := util.EvalFailpoint("invalidateRegionCache"); err == nil {
		r.invalidate(Other)
	}
	newTTL := int64(0)
	for {
		ttl := atomic.LoadInt64(&r.ttl)
		if ts > ttl {
			return false
		}
		// skip updating TTL when:
		// 1. the region has been marked as `needExpireAfterTTL`
		// 2. the TTL is far away from ts (still within jitter time)
		if r.checkSyncFlags(needExpireAfterTTL) || ttl > ts+regionCacheTTLSec {
			return true
		}
		if newTTL == 0 {
			newTTL = nextTTL(ts)
		}
		// now we have ts <= ttl <= ts+regionCacheTTLSec <= newTTL = ts+regionCacheTTLSec+randomJitter
		if atomic.CompareAndSwapInt64(&r.ttl, ttl, newTTL) {
			return true
		}
	}
}

// invalidate invalidates a region, next time it will got null result.
func (r *Region) invalidate(reason InvalidReason, nocount ...bool) {
	if atomic.CompareAndSwapInt32((*int32)(&r.invalidReason), int32(Ok), int32(reason)) {
		if len(nocount) == 0 || !nocount[0] {
			metrics.RegionCacheCounterWithInvalidateRegionFromCacheOK.Inc()
		}
		atomic.StoreInt64(&r.ttl, expiredTTL)
	}
}

func (r *Region) getSyncFlags() int32 {
	return atomic.LoadInt32(&r.syncFlags)
}

// checkSyncFlags returns true if sync_flags contains any of flags.
func (r *Region) checkSyncFlags(flags int32) bool {
	return atomic.LoadInt32(&r.syncFlags)&flags > 0
}

// setSyncFlags sets the sync_flags bits to sync_flags|flags.
func (r *Region) setSyncFlags(flags int32) {
	for {
		oldFlags := atomic.LoadInt32(&r.syncFlags)
		if oldFlags&flags == flags {
			return
		}
		if atomic.CompareAndSwapInt32(&r.syncFlags, oldFlags, oldFlags|flags) {
			return
		}
	}
}

// resetSyncFlags reverts flags from sync_flags (that is sync_flags&^flags), returns the flags that are reset (0 means no flags are reverted).
func (r *Region) resetSyncFlags(flags int32) int32 {
	for {
		oldFlags := atomic.LoadInt32(&r.syncFlags)
		if oldFlags&flags == 0 {
			return 0
		}
		if atomic.CompareAndSwapInt32(&r.syncFlags, oldFlags, oldFlags&^flags) {
			return oldFlags & flags
		}
	}
}

func (r *Region) isValid() bool {
	return r != nil && !r.checkSyncFlags(needReloadOnAccess) && r.checkRegionCacheTTL(time.Now().Unix())
}

type regionIndexMu struct {
	sync.RWMutex
	regions        map[RegionVerID]*Region // cached regions are organized as regionVerID to region ref mapping
	latestVersions map[uint64]RegionVerID  // cache the map from regionID to its latest RegionVerID
	sorted         *SortedRegions          // cache regions are organized as sorted key to region ref mapping
}

func newRegionIndexMu(rs []*Region) *regionIndexMu {
	r := &regionIndexMu{}
	r.regions = make(map[RegionVerID]*Region)
	r.latestVersions = make(map[uint64]RegionVerID)
	r.sorted = NewSortedRegions(btreeDegree)
	for _, region := range rs {
		r.insertRegionToCache(region, true, false)
	}
	return r
}

func (mu *regionIndexMu) refresh(r []*Region) {
	newMu := newRegionIndexMu(r)
	mu.Lock()
	defer mu.Unlock()
	mu.regions = newMu.regions
	mu.latestVersions = newMu.latestVersions
	mu.sorted = newMu.sorted
}

// repeat wraps a `func()` as a schedulable fuction for `bgRunner`.
func repeat(f func()) func(context.Context, time.Time) bool {
	return func(_ context.Context, _ time.Time) bool {
		f()
		return false
	}
}

// until wraps a `func() bool` as a schedulable fuction for `bgRunner`.
func until(f func() bool) func(context.Context, time.Time) bool {
	return func(_ context.Context, _ time.Time) bool {
		return f()
	}
}

type bgRunner struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newBackgroundRunner(ctx context.Context) *bgRunner {
	ctx, cancel := context.WithCancel(ctx)
	return &bgRunner{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (r *bgRunner) closed() bool {
	select {
	case <-r.ctx.Done():
		return true
	default:
		return false
	}
}

func (r *bgRunner) shutdown(wait bool) {
	r.cancel()
	if wait {
		r.wg.Wait()
	}
}

// run calls `f` once in background.
func (r *bgRunner) run(f func(context.Context)) {
	if r.closed() {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f(r.ctx)
	}()
}

// schedule calls `f` every `interval`.
func (r *bgRunner) schedule(f func(context.Context, time.Time) bool, interval time.Duration) {
	if r.closed() || interval <= 0 {
		return
	}
	r.wg.Add(1)
	go func() {
		ticker := time.NewTicker(interval)
		defer func() {
			r.wg.Done()
			ticker.Stop()
		}()
		for {
			select {
			case <-r.ctx.Done():
				return
			case t := <-ticker.C:
				if f(r.ctx, t) {
					return
				}
			}
		}
	}()
}

// scheduleWithTrigger likes schedule, but also call `f` when `<-trigger`, in which case the time arg of `f` is zero.
func (r *bgRunner) scheduleWithTrigger(f func(context.Context, time.Time) bool, interval time.Duration, trigger <-chan struct{}) {
	if r.closed() || interval <= 0 {
		return
	}
	r.wg.Add(1)
	go func() {
		ticker := time.NewTicker(interval)
		defer func() {
			r.wg.Done()
			ticker.Stop()
		}()
		triggerEnabled := trigger != nil
		for triggerEnabled {
			select {
			case <-r.ctx.Done():
				return
			case t := <-ticker.C:
				if f(r.ctx, t) {
					return
				}
			case _, ok := <-trigger:
				if !ok {
					triggerEnabled = false
				} else if f(r.ctx, time.Time{}) {
					return
				}
			}
		}
		for {
			select {
			case <-r.ctx.Done():
				return
			case t := <-ticker.C:
				if f(r.ctx, t) {
					return
				}
			}
		}
	}()
}

// RegionCache caches Regions loaded from PD.
// All public methods of this struct should be thread-safe, unless explicitly pointed out or the method is for testing
// purposes only.
type RegionCache struct {
	pdClient         pd.Client
	codec            apicodec.Codec
	enableForwarding bool

	requestHealthFeedbackCallback func(ctx context.Context, addr string) error

	mu regionIndexMu

	stores storeCache

	// runner for background jobs
	bg *bgRunner

	clusterID uint64
}

type regionCacheOptions struct {
	noHealthTick                  bool
	requestHealthFeedbackCallback func(ctx context.Context, addr string) error
}

type RegionCacheOpt func(*regionCacheOptions)

func RegionCacheNoHealthTick(o *regionCacheOptions) {
	o.noHealthTick = true
}

func WithRequestHealthFeedbackCallback(callback func(ctx context.Context, addr string) error) RegionCacheOpt {
	return func(options *regionCacheOptions) {
		options.requestHealthFeedbackCallback = callback
	}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client, opt ...RegionCacheOpt) *RegionCache {
	var options regionCacheOptions
	for _, o := range opt {
		o(&options)
	}

	c := &RegionCache{
		pdClient:                      pdClient,
		requestHealthFeedbackCallback: options.requestHealthFeedbackCallback,
	}

	c.codec = apicodec.NewCodecV1(apicodec.ModeRaw)
	if codecPDClient, ok := pdClient.(*CodecPDClient); ok {
		c.codec = codecPDClient.GetCodec()
	}

	c.stores = newStoreCache(pdClient)
	c.bg = newBackgroundRunner(context.Background())
	c.enableForwarding = config.GetGlobalConfig().EnableForwarding
	if c.pdClient != nil {
		c.clusterID = c.pdClient.GetClusterID(context.Background())
	}
	if c.clusterID == 0 {
		logutil.BgLogger().Error("cluster id is not set properly")
	}

	if config.GetGlobalConfig().EnablePreload {
		logutil.BgLogger().Info("preload region index start")
		if err := c.refreshRegionIndex(retry.NewBackofferWithVars(c.bg.ctx, 20000, nil)); err != nil {
			logutil.BgLogger().Error("refresh region index failed", zap.Error(err))
		}
		logutil.BgLogger().Info("preload region index finish")
	} else {
		c.mu = *newRegionIndexMu(nil)
	}

	var (
		refreshStoreInterval = config.GetGlobalConfig().StoresRefreshInterval
		needCheckStores      []*Store
	)
	c.bg.scheduleWithTrigger(func(ctx context.Context, t time.Time) bool {
		// check and resolve normal stores periodically by default.
		filter := func(state resolveState) bool {
			return state != unresolved && state != tombstone && state != deleted
		}
		if t.IsZero() {
			// check and resolve needCheck stores because it's triggered by a CheckStoreEvent this time.
			filter = func(state resolveState) bool { return state == needCheck }
		}
		needCheckStores = c.checkAndResolve(needCheckStores[:0], func(s *Store) bool { return filter(s.getResolveState()) })
		return false
	}, time.Duration(refreshStoreInterval/4)*time.Second, c.stores.getCheckStoreEvents())
	if !options.noHealthTick {
		c.bg.schedule(c.checkAndUpdateStoreHealthStatus, time.Duration(refreshStoreInterval/4)*time.Second)
	}
	c.bg.schedule(repeat(c.reportStoreReplicaFlows), time.Duration(refreshStoreInterval/2)*time.Second)
	if refreshCacheInterval := config.GetGlobalConfig().RegionsRefreshInterval; refreshCacheInterval > 0 {
		c.bg.schedule(func(ctx context.Context, _ time.Time) bool {
			if err := c.refreshRegionIndex(retry.NewBackofferWithVars(ctx, int(refreshCacheInterval)*1000, nil)); err != nil {
				logutil.BgLogger().Error("refresh region cache failed", zap.Error(err))
			}
			return false
		}, time.Duration(refreshCacheInterval)*time.Second)
	} else {
		// cache GC is incompatible with cache refresh
		c.bg.schedule(c.gcRoundFunc(cleanRegionNumPerRound), cleanCacheInterval)
	}
	c.bg.schedule(
		func(ctx context.Context, _ time.Time) bool {
			refreshFullStoreList(ctx, c.stores)
			return false
		}, refreshStoreListInterval,
	)
	return c
}

// Try to refresh full store list. Errors are ignored.
func refreshFullStoreList(ctx context.Context, stores storeCache) {
	storeList, err := stores.fetchAllStores(ctx)
	if err != nil {
		logutil.Logger(ctx).Info("refresh full store list failed", zap.Error(err))
		return
	}
	for _, store := range storeList {
		_, exist := stores.get(store.GetId())
		if exist {
			continue
		}
		// GetAllStores is supposed to return only Up and Offline stores.
		// This check is being defensive and to make it consistent with store resolve code.
		if store == nil || store.GetState() == metapb.StoreState_Tombstone {
			continue
		}
		addr := store.GetAddress()
		if addr == "" {
			continue
		}
		s := stores.getOrInsertDefault(store.GetId())
		// TODO: maybe refactor this, together with other places initializing Store
		s.addr = addr
		s.peerAddr = store.GetPeerAddress()
		s.saddr = store.GetStatusAddress()
		s.storeType = tikvrpc.GetStoreTypeByMeta(store)
		s.labels = store.GetLabels()
		s.changeResolveStateTo(unresolved, resolved)
	}
}

// only used fot test.
func newTestRegionCache() *RegionCache {
	c := &RegionCache{}
	c.bg = newBackgroundRunner(context.Background())
	c.mu = *newRegionIndexMu(nil)
	return c
}

// clear clears all cached data in the RegionCache. It's only used in tests.
func (c *RegionCache) clear() {
	c.mu.refresh(nil)
	c.stores.clear()
}

// thread unsafe, should use with lock
func (c *RegionCache) insertRegionToCache(cachedRegion *Region, invalidateOldRegion bool, shouldCount bool) bool {
	return c.mu.insertRegionToCache(cachedRegion, invalidateOldRegion, shouldCount)
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	c.bg.shutdown(true)
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores []*Store, needCheck func(*Store) bool) []*Store {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("panic in the checkAndResolve goroutine",
				zap.Any("r", r),
				zap.Stack("stack trace"))
		}
	}()

	needCheckStores = c.stores.filter(needCheckStores, needCheck)
	for _, store := range needCheckStores {
		_, err := store.reResolve(c.stores, c.bg)
		tikverr.Log(err)
	}
	return needCheckStores
}

// SetRegionCacheStore is used to set a store in region cache, for testing only
func (c *RegionCache) SetRegionCacheStore(id uint64, addr string, peerAddr string, storeType tikvrpc.EndpointType, state uint64, labels []*metapb.StoreLabel) {
	c.stores.put(newStore(id, addr, peerAddr, "", storeType, resolveState(state), labels))
}

// SetPDClient replaces pd client,for testing only
func (c *RegionCache) SetPDClient(client pd.Client) {
	c.pdClient = client
	c.stores = newStoreCache(client)
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
	ClusterID     uint64
	Region        RegionVerID
	Meta          *metapb.Region
	Peer          *metapb.Peer
	AccessIdx     AccessIndex
	Store         *Store
	Addr          string
	AccessMode    accessMode
	ProxyStore    *Store // nil means proxy is not used
	ProxyAddr     string // valid when ProxyStore is not nil
	TiKVNum       int    // Number of TiKV nodes among the region's peers. Assuming non-TiKV peers are all TiFlash peers.
	BucketVersion uint64
}

func (c *RPCContext) String() string {
	var runStoreType string
	if c.Store != nil {
		runStoreType = c.Store.storeType.Name()
	}
	res := fmt.Sprintf("region ID: %d, meta: %s, peer: %s, addr: %s, idx: %d, reqStoreType: %s, runStoreType: %s",
		c.Region.GetID(), c.Meta, c.Peer, c.Addr, c.AccessIdx, c.AccessMode, runStoreType)
	if c.ProxyStore != nil {
		res += fmt.Sprintf(", proxy store id: %d, proxy addr: %s", c.ProxyStore.storeID, c.ProxyStore.addr)
	}
	return res
}

type storeSelectorOp struct {
	leaderOnly   bool
	preferLeader bool
	labels       []*metapb.StoreLabel
	stores       []uint64
}

// StoreSelectorOption configures storeSelectorOp.
type StoreSelectorOption func(*storeSelectorOp)

// WithMatchLabels indicates selecting stores with matched labels.
func WithMatchLabels(labels []*metapb.StoreLabel) StoreSelectorOption {
	return func(op *storeSelectorOp) {
		op.labels = append(op.labels, labels...)
	}
}

// WithLeaderOnly indicates selecting stores with leader only.
func WithLeaderOnly() StoreSelectorOption {
	return func(op *storeSelectorOp) {
		op.leaderOnly = true
	}
}

// WithPerferLeader indicates selecting stores with leader as priority until leader unaccessible.
func WithPerferLeader() StoreSelectorOption {
	return func(op *storeSelectorOp) {
		op.preferLeader = true
	}
}

// WithMatchStores indicates selecting stores with matched store ids.
func WithMatchStores(stores []uint64) StoreSelectorOption {
	return func(op *storeSelectorOp) {
		op.stores = stores
	}
}

// GetTiKVRPCContext returns RPCContext for a region. If it returns nil, the region
// must be out of date and already dropped from cache.
func (c *RegionCache) GetTiKVRPCContext(bo *retry.Backoffer, id RegionVerID, replicaRead kv.ReplicaReadType, followerStoreSeed uint32, opts ...StoreSelectorOption) (*RPCContext, error) {
	cachedRegion := c.GetCachedRegionWithRLock(id)
	if !cachedRegion.isValid() {
		return nil, nil
	}

	regionStore := cachedRegion.getStore()
	var (
		store     *Store
		peer      *metapb.Peer
		storeIdx  int
		accessIdx AccessIndex
	)
	options := &storeSelectorOp{}
	for _, op := range opts {
		op(options)
	}
	isLeaderReq := false
	switch replicaRead {
	case kv.ReplicaReadFollower:
		store, peer, accessIdx, storeIdx = cachedRegion.FollowerStorePeer(regionStore, followerStoreSeed, options)
	case kv.ReplicaReadMixed:
		store, peer, accessIdx, storeIdx = cachedRegion.AnyStorePeer(regionStore, followerStoreSeed, options)
	case kv.ReplicaReadPreferLeader:
		options.preferLeader = true
		store, peer, accessIdx, storeIdx = cachedRegion.AnyStorePeer(regionStore, followerStoreSeed, options)
	default:
		isLeaderReq = true
		store, peer, accessIdx, storeIdx = cachedRegion.WorkStorePeer(regionStore)
	}
	addr, err := c.getStoreAddr(bo, cachedRegion, store)
	if err != nil {
		return nil, err
	}
	// enable by `curl -XPUT -d '1*return("[some-addr]")->return("")' http://host:port/tikvclient/injectWrongStoreAddr`
	if val, err := util.EvalFailpoint("injectWrongStoreAddr"); err == nil {
		if a, ok := val.(string); ok && len(a) > 0 {
			addr = a
		}
	}
	if store == nil || len(addr) == 0 {
		// Store not found, region must be out of date.
		cachedRegion.invalidate(StoreNotFound)
		return nil, nil
	}

	storeFailEpoch := atomic.LoadUint32(&store.epoch)
	if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
		cachedRegion.invalidate(Other)
		logutil.Logger(bo.GetCtx()).Info("invalidate current region, because others failed on same store",
			zap.Uint64("region", id.GetID()),
			zap.String("store", store.addr))
		return nil, nil
	}

	var (
		proxyStore *Store
		proxyAddr  string
	)
	if c.enableForwarding && isLeaderReq {
		if store.getLivenessState() == reachable {
			regionStore.unsetProxyStoreIfNeeded(cachedRegion)
		} else {
			proxyStore, _, _ = c.getProxyStore(cachedRegion, store, regionStore, accessIdx)
			if proxyStore != nil {
				proxyAddr, err = c.getStoreAddr(bo, cachedRegion, proxyStore)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return &RPCContext{
		ClusterID:  c.clusterID,
		Region:     id,
		Meta:       cachedRegion.meta,
		Peer:       peer,
		AccessIdx:  accessIdx,
		Store:      store,
		Addr:       addr,
		AccessMode: tiKVOnly,
		ProxyStore: proxyStore,
		ProxyAddr:  proxyAddr,
		TiKVNum:    regionStore.accessStoreNum(tiKVOnly),
	}, nil
}

// GetAllValidTiFlashStores returns the store ids of all valid TiFlash stores, the store id of currentStore is always the first one
// Caller may use `nonPendingStores` first, this can avoid task need to wait tiflash replica syncing from tikv.
// But if all tiflash peers are pending(len(nonPendingStores) == 0), use `allStores` is also ok.
func (c *RegionCache) GetAllValidTiFlashStores(id RegionVerID, currentStore *Store, labelFilter LabelFilter) ([]uint64, []uint64) {
	// set the cap to 2 because usually, TiFlash table will have 2 replicas
	allStores := make([]uint64, 0, 2)
	nonPendingStores := make([]uint64, 0, 2)
	// make sure currentStore id is always the first in allStores
	allStores = append(allStores, currentStore.storeID)
	ts := time.Now().Unix()
	cachedRegion := c.GetCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return allStores, nonPendingStores
	}
	if !cachedRegion.checkRegionCacheTTL(ts) {
		return allStores, nonPendingStores
	}
	regionStore := cachedRegion.getStore()
	currentIndex := regionStore.getAccessIndex(tiFlashOnly, currentStore)
	if currentIndex == -1 {
		return allStores, nonPendingStores
	}
	for startOffset := 1; startOffset < regionStore.accessStoreNum(tiFlashOnly); startOffset++ {
		accessIdx := AccessIndex((int(currentIndex) + startOffset) % regionStore.accessStoreNum(tiFlashOnly))
		storeIdx, store := regionStore.accessStore(tiFlashOnly, accessIdx)
		if store.getResolveState() == needCheck {
			continue
		}
		storeFailEpoch := atomic.LoadUint32(&store.epoch)
		if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
			continue
		}
		if !labelFilter(store.labels) {
			continue
		}
		allStores = append(allStores, store.storeID)
	}
	for _, storeID := range allStores {
		if !slices.ContainsFunc(regionStore.pendingPeers, func(p *metapb.Peer) bool { return p.StoreId == storeID }) {
			nonPendingStores = append(nonPendingStores, storeID)
		}
	}
	return allStores, nonPendingStores
}

// GetTiFlashRPCContext returns RPCContext for a region must access flash store. If it returns nil, the region
// must be out of date and already dropped from cache or not flash store found.
// `loadBalance` is an option. For batch cop, it is pointless and might cause try the failed store repeatly.
func (c *RegionCache) GetTiFlashRPCContext(bo *retry.Backoffer, id RegionVerID, loadBalance bool, labelFilter LabelFilter) (*RPCContext, error) {

	cachedRegion := c.GetCachedRegionWithRLock(id)
	if !cachedRegion.isValid() {
		return nil, nil
	}

	regionStore := cachedRegion.getStore()

	// sIdx is for load balance of TiFlash store.
	var sIdx int
	if loadBalance {
		sIdx = int(regionStore.workTiFlashIdx.Add(1))
	} else {
		sIdx = int(regionStore.workTiFlashIdx.Load())
	}
	for i := 0; i < regionStore.accessStoreNum(tiFlashOnly); i++ {
		accessIdx := AccessIndex((sIdx + i) % regionStore.accessStoreNum(tiFlashOnly))
		storeIdx, store := regionStore.accessStore(tiFlashOnly, accessIdx)
		if !labelFilter(store.labels) {
			continue
		}
		addr, err := c.getStoreAddr(bo, cachedRegion, store)
		if err != nil {
			return nil, err
		}
		if len(addr) == 0 {
			cachedRegion.invalidate(StoreNotFound)
			return nil, nil
		}
		if store.getResolveState() == needCheck {
			_, err := store.reResolve(c.stores, c.bg)
			tikverr.Log(err)
		}
		regionStore.workTiFlashIdx.Store(int32(accessIdx))
		peer := cachedRegion.meta.Peers[storeIdx]
		storeFailEpoch := atomic.LoadUint32(&store.epoch)
		if storeFailEpoch != regionStore.storeEpochs[storeIdx] {
			cachedRegion.invalidate(Other)
			logutil.Logger(bo.GetCtx()).Info("invalidate current region, because others failed on same store",
				zap.Uint64("region", id.GetID()),
				zap.String("store", store.addr))
			// TiFlash will always try to find out a valid peer, avoiding to retry too many times.
			continue
		}
		return &RPCContext{
			ClusterID:  c.clusterID,
			Region:     id,
			Meta:       cachedRegion.meta,
			Peer:       peer,
			AccessIdx:  accessIdx,
			Store:      store,
			Addr:       addr,
			AccessMode: tiFlashOnly,
			TiKVNum:    regionStore.accessStoreNum(tiKVOnly),
		}, nil
	}

	cachedRegion.invalidate(Other)
	return nil, nil
}

// KeyLocation is the region and range that a key is located.
type KeyLocation struct {
	Region   RegionVerID
	StartKey []byte
	EndKey   []byte
	Buckets  *metapb.Buckets
}

// Contains checks if key is in [StartKey, EndKey).
func (l *KeyLocation) Contains(key []byte) bool {
	return bytes.Compare(l.StartKey, key) <= 0 &&
		(bytes.Compare(key, l.EndKey) < 0 || len(l.EndKey) == 0)
}

// String implements fmt.Stringer interface.
func (l *KeyLocation) String() string {
	return fmt.Sprintf("region %s,startKey:%s,endKey:%s", l.Region.String(), kv.StrKey(l.StartKey), kv.StrKey(l.EndKey))
}

// GetBucketVersion gets the bucket version of the region.
// If the region doesn't contain buckets, returns 0.
func (l *KeyLocation) GetBucketVersion() uint64 {
	if l.Buckets == nil {
		return 0
	}
	return l.Buckets.GetVersion()
}

// LocateBucket handles with a type of edge case of locateBucket that returns nil.
// There are two cases where locateBucket returns nil:
// Case one is that the key neither does not belong to any bucket nor does not belong to the region.
// Case two is that the key belongs to the region but not any bucket.
// LocateBucket will not return nil in the case two.
// Specifically, when the key is in [KeyLocation.StartKey, first Bucket key), the result returned by locateBucket will be nil
// as there's no bucket containing this key. LocateBucket will return Bucket{KeyLocation.StartKey, first Bucket key}
// as it's reasonable to assume that Bucket{KeyLocation.StartKey, first Bucket key} is a bucket belonging to the region.
// Key in [last Bucket key, KeyLocation.EndKey) is handled similarly.
func (l *KeyLocation) LocateBucket(key []byte) *Bucket {
	bucket := l.locateBucket(key)
	// Return the bucket when locateBucket can locate the key
	if bucket != nil {
		return bucket
	}
	// Case one returns nil too.
	if !l.Contains(key) {
		return nil
	}
	counts := len(l.Buckets.Keys)
	if counts == 0 {
		return &Bucket{
			l.StartKey,
			l.EndKey,
		}
	}
	// Handle case two
	firstBucketKey := l.Buckets.Keys[0]
	if bytes.Compare(key, firstBucketKey) < 0 {
		return &Bucket{
			l.StartKey,
			firstBucketKey,
		}
	}
	lastBucketKey := l.Buckets.Keys[counts-1]
	if bytes.Compare(lastBucketKey, key) <= 0 {
		return &Bucket{
			lastBucketKey,
			l.EndKey,
		}
	}
	// unreachable
	logutil.Logger(context.Background()).Info(
		"Unreachable place", zap.String("KeyLocation", l.String()), zap.String("Key", hex.EncodeToString(key)))
	panic("Unreachable")
}

// locateBucket returns the bucket the key is located. It returns nil if the key is outside the bucket.
func (l *KeyLocation) locateBucket(key []byte) *Bucket {
	keys := l.Buckets.GetKeys()
	searchLen := len(keys) - 1
	i := sort.Search(searchLen, func(i int) bool {
		return bytes.Compare(key, keys[i]) < 0
	})

	// buckets contains region's start/end key, so i==0 means it can't find a suitable bucket
	// which can happen if the bucket information is stale.
	if i == 0 ||
		// the key isn't located in the last range.
		(i == searchLen && len(keys[searchLen]) != 0 && bytes.Compare(key, keys[searchLen]) >= 0) {
		return nil
	}
	return &Bucket{keys[i-1], keys[i]}
}

// Bucket is a single bucket of a region.
type Bucket struct {
	StartKey []byte
	EndKey   []byte
}

// Contains checks whether the key is in the Bucket.
func (b *Bucket) Contains(key []byte) bool {
	return contains(b.StartKey, b.EndKey, key)
}

// LocateKeyRange lists region and range that key in [start_key,end_key).
// Regions without leader won't be returned.
func (c *RegionCache) LocateKeyRange(bo *retry.Backoffer, startKey, endKey []byte) ([]*KeyLocation, error) {
	var res []*KeyLocation
	for {
		// 1. find regions from cache
		for {
			r := c.tryFindRegionByKey(startKey, false)
			if r == nil {
				break
			}
			res = append(res, &KeyLocation{
				Region:   r.VerID(),
				StartKey: r.StartKey(),
				EndKey:   r.EndKey(),
				Buckets:  r.getStore().buckets,
			})
			if r.ContainsByEnd(endKey) {
				return res, nil
			}
			startKey = r.EndKey()
		}
		// 2. load remaining regions from pd client
		batchRegions, err := c.BatchLoadRegionsWithKeyRanges(bo, []pd.KeyRange{{StartKey: startKey, EndKey: endKey}}, defaultRegionsPerBatch, WithNeedRegionHasLeaderPeer())
		if err != nil {
			return nil, err
		}
		if len(batchRegions) == 0 {
			// should never happen
			err := errors.Errorf("BatchLoadRegionsWithKeyRange return empty batchRegions without err")
			return nil, err
		}
		for _, r := range batchRegions {
			res = append(res, &KeyLocation{
				Region:   r.VerID(),
				StartKey: r.StartKey(),
				EndKey:   r.EndKey(),
				Buckets:  r.getStore().buckets,
			})
		}
		endRegion := batchRegions[len(batchRegions)-1]
		if endRegion.ContainsByEnd(endKey) {
			break
		}
		startKey = endRegion.EndKey()
	}
	return res, nil
}

type batchLocateKeyRangesOption struct {
	// whether load leader only, if it's set to true, regions without leader will be skipped.
	// Note there is leader even the leader is invalid or outdated.
	needRegionHasLeaderPeer bool
	// whether load buckets, if it's set to true, more traffic will be consumed.
	needBuckets bool
}

type BatchLocateKeyRangesOpt func(*batchLocateKeyRangesOption)

func WithNeedBuckets() BatchLocateKeyRangesOpt {
	return func(opt *batchLocateKeyRangesOption) {
		opt.needBuckets = true
	}
}

func WithNeedRegionHasLeaderPeer() BatchLocateKeyRangesOpt {
	return func(opt *batchLocateKeyRangesOption) {
		opt.needRegionHasLeaderPeer = true
	}
}

// BatchLocateKeyRanges lists regions in the given ranges.
func (c *RegionCache) BatchLocateKeyRanges(bo *retry.Backoffer, keyRanges []kv.KeyRange, opts ...BatchLocateKeyRangesOpt) ([]*KeyLocation, error) {
	uncachedRanges := make([]pd.KeyRange, 0, len(keyRanges))
	cachedRegions := make([]*Region, 0, len(keyRanges))
	// 1. find regions from cache
	var lastRegion *Region
	for _, keyRange := range keyRanges {
		if lastRegion != nil {
			if lastRegion.ContainsByEnd(keyRange.EndKey) {
				continue
			} else if lastRegion.Contains(keyRange.StartKey) {
				keyRange.StartKey = lastRegion.EndKey()
			}
		}
		// TODO: find all the cached regions in the range.
		// now we only check if the region is cached from the lower bound, if there is a uncached hole in the middle,
		// we will load the rest regions even they are cached.
		r := c.tryFindRegionByKey(keyRange.StartKey, false)
		lastRegion = r
		if r == nil {
			// region cache miss, add the cut range to uncachedRanges, load from PD later.
			uncachedRanges = append(uncachedRanges, pd.KeyRange{StartKey: keyRange.StartKey, EndKey: keyRange.EndKey})
			continue
		}
		// region cache hit, add the region to cachedRegions.
		cachedRegions = append(cachedRegions, r)
		if r.ContainsByEnd(keyRange.EndKey) {
			// the range is fully hit in the region cache.
			continue
		}
		keyRange.StartKey = r.EndKey()
		// Batch load rest regions from Cache.
		containsAll := false
	outer:
		for {
			batchRegionInCache, err := c.scanRegionsFromCache(bo, keyRange.StartKey, keyRange.EndKey, defaultRegionsPerBatch)
			if err != nil {
				return nil, err
			}
			for _, r = range batchRegionInCache {
				if !r.Contains(keyRange.StartKey) { // uncached hole, load the rest regions
					break outer
				}
				cachedRegions = append(cachedRegions, r)
				lastRegion = r
				if r.ContainsByEnd(keyRange.EndKey) {
					// the range is fully hit in the region cache.
					containsAll = true
					break outer
				}
				keyRange.StartKey = r.EndKey()
			}
			if len(batchRegionInCache) < defaultRegionsPerBatch { // region cache miss, load the rest regions
				break
			}
		}
		if !containsAll {
			uncachedRanges = append(uncachedRanges, pd.KeyRange{StartKey: keyRange.StartKey, EndKey: keyRange.EndKey})
		}
	}

	merger := newBatchLocateRegionMerger(cachedRegions, len(cachedRegions)+len(uncachedRanges))

	// 2. load remaining regions from pd client
	for len(uncachedRanges) > 0 {
		regions, err := c.BatchLoadRegionsWithKeyRanges(bo, uncachedRanges, defaultRegionsPerBatch, opts...)
		if err != nil {
			return nil, err
		}
		if len(regions) == 0 {
			return nil, errors.Errorf("BatchLoadRegionsWithKeyRanges return empty batchedRegions without err")
		}

		for _, r := range regions {
			merger.appendRegion(r)
		}
		// if the regions are not loaded completely, split uncachedRanges and load the rest.
		uncachedRanges = rangesAfterKey(uncachedRanges, regions[len(regions)-1].EndKey())
	}
	return merger.build(), nil
}

type batchLocateRangesMerger struct {
	lastEndKey      *[]byte
	cachedIdx       int
	cachedRegions   []*Region
	mergedLocations []*KeyLocation
}

func newBatchLocateRegionMerger(cachedRegions []*Region, sizeHint int) *batchLocateRangesMerger {
	return &batchLocateRangesMerger{
		lastEndKey:      nil,
		cachedRegions:   cachedRegions,
		mergedLocations: make([]*KeyLocation, 0, sizeHint),
	}
}

func (m *batchLocateRangesMerger) appendKeyLocation(r *Region) {
	m.mergedLocations = append(m.mergedLocations, &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		Buckets:  r.getStore().buckets,
	})
}

func (m *batchLocateRangesMerger) appendRegion(uncachedRegion *Region) {
	defer func() {
		endKey := uncachedRegion.EndKey()
		if len(endKey) == 0 {
			// len(end_key) == 0 means region end to +inf, it should be the last region.
			// discard the rest cached regions by moving the cachedIdx to the end of cachedRegions.
			m.cachedIdx = len(m.cachedRegions)
		} else {
			lastEndKey := uncachedRegion.EndKey()
			m.lastEndKey = &lastEndKey
		}
	}()
	if len(uncachedRegion.StartKey()) == 0 {
		// len(start_key) == 0 means region start from -inf, it should be the first region.
		m.appendKeyLocation(uncachedRegion)
		return
	}
	if m.lastEndKey != nil && bytes.Compare(*m.lastEndKey, uncachedRegion.StartKey()) >= 0 {
		// the uncached regions are continued, do not consider cached region by now.
		m.appendKeyLocation(uncachedRegion)
		return
	}
	for ; m.cachedIdx < len(m.cachedRegions); m.cachedIdx++ {
		if m.lastEndKey != nil && bytes.Compare(*m.lastEndKey, m.cachedRegions[m.cachedIdx].EndKey()) >= 0 {
			// skip the cached region that is covered by the uncached region.
			continue
		}
		if bytes.Compare(m.cachedRegions[m.cachedIdx].StartKey(), uncachedRegion.StartKey()) >= 0 {
			break
		}
		// append the cached regions that are before the uncached region.
		m.appendKeyLocation(m.cachedRegions[m.cachedIdx])
	}
	m.appendKeyLocation(uncachedRegion)
}

func (m *batchLocateRangesMerger) build() []*KeyLocation {
	// append the rest cache hit regions
	for ; m.cachedIdx < len(m.cachedRegions); m.cachedIdx++ {
		if m.lastEndKey != nil && bytes.Compare(*m.lastEndKey, m.cachedRegions[m.cachedIdx].EndKey()) >= 0 {
			// skip the cached region that is covered by the uncached region.
			continue
		}
		m.appendKeyLocation(m.cachedRegions[m.cachedIdx])
	}
	return m.mergedLocations
}

// rangesAfterKey split the key ranges and return the rest ranges after splitKey.
// the returned ranges are referenced to the input keyRanges, and the key range may be changed in place,
// the input keyRanges should not be used after calling this function.
func rangesAfterKey(keyRanges []pd.KeyRange, splitKey []byte) []pd.KeyRange {
	if len(keyRanges) == 0 {
		return nil
	}
	if len(splitKey) == 0 || len(keyRanges[len(keyRanges)-1].EndKey) > 0 && bytes.Compare(splitKey, keyRanges[len(keyRanges)-1].EndKey) >= 0 {
		// fast check, if all ranges are loaded from PD, quit the loop.
		return nil
	}

	n := sort.Search(len(keyRanges), func(i int) bool {
		return len(keyRanges[i].EndKey) == 0 || bytes.Compare(keyRanges[i].EndKey, splitKey) > 0
	})

	keyRanges = keyRanges[n:]
	if bytes.Compare(splitKey, keyRanges[0].StartKey) > 0 {
		keyRanges[0].StartKey = splitKey
	}
	return keyRanges
}

// LocateKey searches for the region and range that the key is located.
func (c *RegionCache) LocateKey(bo *retry.Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, false)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		Buckets:  r.getStore().buckets,
	}, nil
}

// TryLocateKey searches for the region and range that the key is located, but return nil when region miss or invalid.
func (c *RegionCache) TryLocateKey(key []byte) *KeyLocation {
	r := c.tryFindRegionByKey(key, false)
	if r == nil {
		return nil
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		Buckets:  r.getStore().buckets,
	}
}

// LocateEndKey searches for the region and range that the key is located.
// Unlike LocateKey, start key of a region is exclusive and end key is inclusive.
func (c *RegionCache) LocateEndKey(bo *retry.Backoffer, key []byte) (*KeyLocation, error) {
	r, err := c.findRegionByKey(bo, key, true)
	if err != nil {
		return nil, err
	}
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		Buckets:  r.getStore().buckets,
	}, nil
}

func (c *RegionCache) findRegionByKey(bo *retry.Backoffer, key []byte, isEndKey bool) (r *Region, err error) {
	var expired bool
	r, expired = c.searchCachedRegionByKey(key, isEndKey)
	tag := "ByKey"
	if isEndKey {
		tag = "ByEndKey"
	}
	if r == nil || expired {
		// load region when it is not exists or expired.
		observeLoadRegion(tag, r, expired, 0)
		lr, err := c.loadRegion(bo, key, isEndKey, pd.WithAllowFollowerHandle())
		if err != nil {
			// no region data, return error if failure.
			return nil, err
		}
		logutil.Eventf(bo.GetCtx(), "load region %d from pd, due to cache-miss", lr.GetID())
		r = lr
		c.mu.Lock()
		stale := !c.insertRegionToCache(r, true, true)
		c.mu.Unlock()
		// just retry once, it won't bring much overhead.
		if stale {
			observeLoadRegion(tag+":Retry", r, expired, 0)
			lr, err = c.loadRegion(bo, key, isEndKey)
			if err != nil {
				// no region data, return error if failure.
				return nil, err
			}
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r, true, true)
			c.mu.Unlock()
		}
	} else if flags := r.resetSyncFlags(needReloadOnAccess | needDelayedReloadReady); flags > 0 {
		// load region when it be marked as need reload.
		observeLoadRegion(tag, r, expired, flags)
		// NOTE: we can NOT use c.loadRegionByID(bo, r.GetID()) here because the new region (loaded by id) is not
		// guaranteed to contain the key. (ref: https://github.com/tikv/client-go/pull/1299)
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// ignore error and use old region info.
			logutil.Logger(bo.GetCtx()).Error("load region failure",
				zap.String("key", util.HexRegionKeyStr(key)), zap.Error(err),
				zap.String("encode-key", util.HexRegionKeyStr(c.codec.EncodeRegionKey(key))))
		} else {
			logutil.Eventf(bo.GetCtx(), "load region %d from pd, due to need-reload", lr.GetID())
			reloadOnAccess := flags&needReloadOnAccess > 0
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r, reloadOnAccess, reloadOnAccess)
			c.mu.Unlock()
		}
	}
	return r, nil
}

func (c *RegionCache) tryFindRegionByKey(key []byte, isEndKey bool) (r *Region) {
	var expired bool
	r, expired = c.searchCachedRegionByKey(key, isEndKey)
	if r == nil || expired || r.checkSyncFlags(needReloadOnAccess) {
		return nil
	}
	return r
}

// OnSendFailForTiFlash handles send request fail logic for tiflash.
func (c *RegionCache) OnSendFailForTiFlash(bo *retry.Backoffer, store *Store, region RegionVerID, prev *metapb.Region, scheduleReload bool, err error, skipSwitchPeerLog bool) {
	r := c.GetCachedRegionWithRLock(region)
	if r == nil {
		return
	}

	rs := r.getStore()
	peersNum := len(r.GetMeta().Peers)
	if len(prev.Peers) != peersNum {
		logutil.Logger(bo.GetCtx()).Info("retry and refresh current region after send request fail and up/down stores length changed",
			zap.Stringer("region", &region),
			zap.Bool("needReload", scheduleReload),
			zap.Reflect("oldPeers", prev.Peers),
			zap.Reflect("newPeers", r.GetMeta().Peers),
			zap.Error(err))
		return
	}

	accessMode := tiFlashOnly
	accessIdx := rs.getAccessIndex(accessMode, store)
	if accessIdx == -1 {
		logutil.Logger(bo.GetCtx()).Warn("can not get access index for region " + region.String())
		return
	}
	if err != nil {
		storeIdx, s := rs.accessStore(accessMode, accessIdx)
		c.markRegionNeedBeRefill(s, storeIdx, rs)
	}

	// try next peer
	rs.switchNextFlashPeer(r, accessIdx)
	// In most scenarios, TiFlash will batch all the regions in one TiFlash store into one request, so when meet send failure,
	// this function is called repeatedly for all the regions, since one TiFlash store might contain thousands of regions, we
	// need a way to avoid generating too much useless log
	if !skipSwitchPeerLog {
		logutil.Logger(bo.GetCtx()).Info("switch region tiflash peer to next due to send request fail",
			zap.Stringer("region", &region),
			zap.Bool("needReload", scheduleReload),
			zap.Error(err))
	}

	// force reload region when retry all known peers in region.
	if scheduleReload {
		r.setSyncFlags(needReloadOnAccess)
	}
}

func (c *RegionCache) markRegionNeedBeRefill(s *Store, storeIdx int, rs *regionStore) int {
	incEpochStoreIdx := -1
	// invalidate regions in store.
	epoch := rs.storeEpochs[storeIdx]
	if atomic.CompareAndSwapUint32(&s.epoch, epoch, epoch+1) {
		logutil.BgLogger().Info("mark store's regions need be refill", zap.String("store", s.addr))
		incEpochStoreIdx = storeIdx
		metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
	}
	// schedule a store addr resolve.
	c.stores.markStoreNeedCheck(s)
	return incEpochStoreIdx
}

// OnSendFail handles send request fail logic.
func (c *RegionCache) OnSendFail(bo *retry.Backoffer, ctx *RPCContext, scheduleReload bool, err error) {
	metrics.RegionCacheCounterWithSendFail.Inc()
	r := c.GetCachedRegionWithRLock(ctx.Region)
	if r == nil {
		return
	}
	peersNum := len(r.meta.Peers)
	if len(ctx.Meta.Peers) != peersNum {
		logutil.Logger(bo.GetCtx()).Info("retry and refresh current ctx after send request fail and up/down stores length changed",
			zap.Stringer("current", ctx),
			zap.Bool("needReload", scheduleReload),
			zap.Reflect("oldPeers", ctx.Meta.Peers),
			zap.Reflect("newPeers", r.meta.Peers),
			zap.Error(err))
		return
	}

	rs := r.getStore()

	if err != nil {
		storeIdx, s := rs.accessStore(ctx.AccessMode, ctx.AccessIdx)

		// invalidate regions in store.
		c.markRegionNeedBeRefill(s, storeIdx, rs)
	}

	// try next peer to found new leader.
	if ctx.AccessMode == tiKVOnly {
		rs.switchNextTiKVPeer(r, ctx.AccessIdx)
		logutil.Logger(bo.GetCtx()).Info("switch region peer to next due to send request fail",
			zap.Stringer("current", ctx),
			zap.Bool("needReload", scheduleReload),
			zap.Error(err))
	} else {
		rs.switchNextFlashPeer(r, ctx.AccessIdx)
		logutil.Logger(bo.GetCtx()).Info("switch region tiflash peer to next due to send request fail",
			zap.Stringer("current", ctx),
			zap.Bool("needReload", scheduleReload),
			zap.Error(err))
	}

	// force reload region when retry all known peers in region.
	if scheduleReload {
		r.setSyncFlags(needReloadOnAccess)
	}

}

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *retry.Backoffer, regionID uint64) (*KeyLocation, error) {
	r, expired := c.searchCachedRegionByID(regionID)
	if r != nil && !expired {
		if flags := r.resetSyncFlags(needReloadOnAccess | needDelayedReloadReady); flags > 0 {
			reloadOnAccess := flags&needReloadOnAccess > 0
			observeLoadRegion("ByID", r, expired, flags)
			lr, err := c.loadRegionByID(bo, regionID)
			if err != nil {
				// ignore error and use old region info.
				logutil.Logger(bo.GetCtx()).Error("load region failure",
					zap.Uint64("regionID", regionID), zap.Error(err))
			} else {
				r = lr
				c.mu.Lock()
				c.insertRegionToCache(r, reloadOnAccess, reloadOnAccess)
				c.mu.Unlock()
			}
		}
		loc := &KeyLocation{
			Region:   r.VerID(),
			StartKey: r.StartKey(),
			EndKey:   r.EndKey(),
			Buckets:  r.getStore().buckets,
		}
		return loc, nil
	}

	observeLoadRegion("ByID", r, expired, 0)
	r, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.insertRegionToCache(r, true, true)
	c.mu.Unlock()
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		Buckets:  r.getStore().buckets,
	}, nil
}

// GroupKeysByRegion separates keys into groups by their belonging Regions.
// Specially it also returns the first key's region which may be used as the
// 'PrimaryLockKey' and should be committed ahead of others.
// filter is used to filter some unwanted keys.
func (c *RegionCache) GroupKeysByRegion(bo *retry.Backoffer, keys [][]byte, filter func(key, regionStartKey []byte) bool) (map[RegionVerID][][]byte, RegionVerID, error) {
	groups := make(map[RegionVerID][][]byte)
	var first RegionVerID
	var lastLoc *KeyLocation
	for i, k := range keys {
		if lastLoc == nil || !lastLoc.Contains(k) {
			var err error
			lastLoc, err = c.LocateKey(bo, k)
			if err != nil {
				return nil, first, err
			}
			if filter != nil && filter(k, lastLoc.StartKey) {
				continue
			}
		}
		id := lastLoc.Region
		if i == 0 {
			first = id
		}
		groups[id] = append(groups[id], k)
	}
	return groups, first, nil
}

// ListRegionIDsInKeyRange lists ids of regions in [start_key,end_key].
func (c *RegionCache) ListRegionIDsInKeyRange(bo *retry.Backoffer, startKey, endKey []byte) (regionIDs []uint64, err error) {
	for {
		curRegion, err := c.LocateKey(bo, startKey)
		if err != nil {
			return nil, err
		}
		regionIDs = append(regionIDs, curRegion.Region.id)
		if curRegion.Contains(endKey) {
			break
		}
		startKey = curRegion.EndKey
	}
	return regionIDs, nil
}

// LoadRegionsInKeyRange lists regions in [start_key,end_key].
func (c *RegionCache) LoadRegionsInKeyRange(bo *retry.Backoffer, startKey, endKey []byte) (regions []*Region, err error) {
	var batchRegions []*Region
	for {
		batchRegions, err = c.BatchLoadRegionsWithKeyRange(bo, startKey, endKey, defaultRegionsPerBatch)
		if err != nil {
			return nil, err
		}
		if len(batchRegions) == 0 {
			// should never happen
			break
		}
		regions = append(regions, batchRegions...)
		endRegion := batchRegions[len(batchRegions)-1]
		if endRegion.ContainsByEnd(endKey) {
			break
		}
		startKey = endRegion.EndKey()
	}
	return
}

// BatchLoadRegionsWithKeyRange loads at most given numbers of regions to the RegionCache,
// within the given key range from the startKey to endKey. Returns the loaded regions.
func (c *RegionCache) BatchLoadRegionsWithKeyRange(bo *retry.Backoffer, startKey []byte, endKey []byte, count int) (regions []*Region, err error) {
	regions, err = c.scanRegions(bo, startKey, endKey, count)
	if err != nil {
		return
	}
	if len(regions) == 0 {
		err = errors.Errorf("PD returned no region, start_key: %q, end_key: %q, encode_start_key: %q, encode_end_key: %q",
			util.HexRegionKeyStr(startKey), util.HexRegionKeyStr(endKey),
			util.HexRegionKeyStr(c.codec.EncodeRegionKey(startKey)), util.HexRegionKeyStr(c.codec.EncodeRegionKey(endKey)))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO(youjiali1995): scanRegions always fetch regions from PD and these regions don't contain buckets information
	// for less traffic, so newly inserted regions in region cache don't have buckets information. We should improve it.
	for _, region := range regions {
		c.insertRegionToCache(region, true, false)
	}

	return
}

// BatchLoadRegionsWithKeyRanges loads at most given numbers of regions to the RegionCache,
// within the given key range from the key ranges. Returns the loaded regions.
func (c *RegionCache) BatchLoadRegionsWithKeyRanges(bo *retry.Backoffer, keyRanges []pd.KeyRange, count int, opts ...BatchLocateKeyRangesOpt) (regions []*Region, err error) {
	if len(keyRanges) == 0 {
		return nil, nil
	}
	regions, err = c.batchScanRegions(bo, keyRanges, count, opts...)
	if err != nil {
		return
	}
	if len(regions) == 0 {
		err = errors.Errorf("PD returned no region, range num: %d, count: %d", len(keyRanges), count)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, region := range regions {
		c.insertRegionToCache(region, true, false)
	}
	return
}

// BatchLoadRegionsFromKey loads at most given numbers of regions to the RegionCache, from the given startKey. Returns
// the endKey of the last loaded region. If some of the regions has no leader, their entries in RegionCache will not be
// updated.
func (c *RegionCache) BatchLoadRegionsFromKey(bo *retry.Backoffer, startKey []byte, count int) ([]byte, error) {
	regions, err := c.BatchLoadRegionsWithKeyRange(bo, startKey, nil, count)
	if err != nil {
		return nil, err
	}
	return regions[len(regions)-1].EndKey(), nil
}

// InvalidateCachedRegion removes a cached Region.
func (c *RegionCache) InvalidateCachedRegion(id RegionVerID) {
	c.InvalidateCachedRegionWithReason(id, Other)
}

// InvalidateCachedRegionWithReason removes a cached Region with the reason why it's invalidated.
func (c *RegionCache) InvalidateCachedRegionWithReason(id RegionVerID, reason InvalidReason) {
	cachedRegion := c.GetCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return
	}
	cachedRegion.invalidate(reason)
}

// UpdateLeader update some region cache with newer leader info.
func (c *RegionCache) UpdateLeader(regionID RegionVerID, leader *metapb.Peer, currentPeerIdx AccessIndex) {
	r := c.GetCachedRegionWithRLock(regionID)
	if r == nil {
		logutil.BgLogger().Debug("regionCache: cannot find region when updating leader",
			zap.Uint64("regionID", regionID.GetID()))
		return
	}

	if leader == nil {
		rs := r.getStore()
		rs.switchNextTiKVPeer(r, currentPeerIdx)
		logutil.BgLogger().Info("switch region peer to next due to NotLeader with NULL leader",
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("regionID", regionID.GetID()))
		return
	}

	if !r.switchWorkLeaderToPeer(leader) {
		logutil.BgLogger().Info("invalidate region cache due to cannot find peer when updating leader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("leaderStoreID", leader.GetStoreId()))
		r.invalidate(StoreNotFound)
	} else {
		logutil.BgLogger().Info("switch region leader to specific leader due to kv return NotLeader",
			zap.Uint64("regionID", regionID.GetID()),
			zap.Int("currIdx", int(currentPeerIdx)),
			zap.Uint64("leaderStoreID", leader.GetStoreId()))
	}
}

// removeVersionFromCache removes a RegionVerID from cache, tries to cleanup
// both c.mu.regions and c.mu.versions. Note this function is not thread-safe.
func (mu *regionIndexMu) removeVersionFromCache(oldVer RegionVerID, regionID uint64) {
	delete(mu.regions, oldVer)
	if ver, ok := mu.latestVersions[regionID]; ok && ver.Equals(oldVer) {
		delete(mu.latestVersions, regionID)
	}
}

// insertRegionToCache tries to insert the Region to cache.
// It should be protected by c.mu.l.Lock().
// if `invalidateOldRegion` is false, the old region cache should be still valid,
// and it may still be used by some kv requests.
// Moreover, it will return false if the region is stale.
func (mu *regionIndexMu) insertRegionToCache(cachedRegion *Region, invalidateOldRegion bool, shouldCount bool) bool {
	newVer := cachedRegion.VerID()
	oldVer, ok := mu.latestVersions[newVer.id]
	// There are two or more situations in which the region we got is stale.
	// The first case is that the process of getting a region is concurrent.
	// The stale region may be returned later due to network reasons.
	// The second case is that the region may be obtained from the PD follower,
	// and there is the synchronization time between the pd follower and the leader.
	// So we should check the epoch.
	if ok && (oldVer.GetVer() > newVer.GetVer() || oldVer.GetConfVer() > newVer.GetConfVer()) {
		logutil.BgLogger().Debug("get stale region",
			zap.Uint64("region", newVer.GetID()), zap.Uint64("new-ver", newVer.GetVer()), zap.Uint64("new-conf", newVer.GetConfVer()),
			zap.Uint64("old-ver", oldVer.GetVer()), zap.Uint64("old-conf", oldVer.GetConfVer()))
		return false
	}
	// Also check and remove the intersecting regions including the old region.
	intersectedRegions, stale := mu.sorted.removeIntersecting(cachedRegion, newVer)
	if stale {
		return false
	}
	// Insert the region (won't replace because of above deletion).
	mu.sorted.ReplaceOrInsert(cachedRegion)
	// Inherit the workTiKVIdx, workTiFlashIdx and buckets from the first intersected region.
	if len(intersectedRegions) > 0 {
		oldRegion := intersectedRegions[0].cachedRegion
		store := cachedRegion.getStore()
		oldRegionStore := oldRegion.getStore()
		// TODO(youjiali1995): remove this because the new retry logic can handle this issue.
		//
		// Joint consensus is enabled in v5.0, which is possible to make a leader step down as a learner during a conf change.
		// And if hibernate region is enabled, after the leader step down, there can be a long time that there is no leader
		// in the region and the leader info in PD is stale until requests are sent to followers or hibernate timeout.
		// To solve it, one solution is always to try a different peer if the invalid reason of the old cached region is no-leader.
		// There is a small probability that the current peer who reports no-leader becomes a leader and TiDB has to retry once in this case.
		if InvalidReason(atomic.LoadInt32((*int32)(&oldRegion.invalidReason))) == NoLeader {
			store.workTiKVIdx = (oldRegionStore.workTiKVIdx + 1) % AccessIndex(store.accessStoreNum(tiKVOnly))
		}
		// Don't refresh TiFlash work idx for region. Otherwise, it will always goto a invalid store which
		// is under transferring regions.
		store.workTiFlashIdx.Store(oldRegionStore.workTiFlashIdx.Load())

		// Keep the buckets information if needed.
		if store.buckets == nil || (oldRegionStore.buckets != nil && store.buckets.GetVersion() < oldRegionStore.buckets.GetVersion()) {
			store.buckets = oldRegionStore.buckets
		}
	}
	// The intersecting regions in the cache are probably stale, clear them.
	for _, region := range intersectedRegions {
		mu.removeVersionFromCache(region.cachedRegion.VerID(), region.cachedRegion.GetID())
		// If the old region is still valid, do not invalidate it to avoid unnecessary backoff.
		if invalidateOldRegion {
			// Invalidate the old region in case it's not invalidated and some requests try with the stale region information.
			region.cachedRegion.invalidate(Other, !shouldCount)
		}
	}
	// update related vars.
	mu.regions[newVer] = cachedRegion
	mu.latestVersions[newVer.id] = newVer
	return true
}

// searchCachedRegionByKey finds the region from cache by key.
func (c *RegionCache) searchCachedRegionByKey(key []byte, isEndKey bool) (*Region, bool) {
	c.mu.RLock()
	region := c.mu.sorted.SearchByKey(key, isEndKey)
	c.mu.RUnlock()
	if region == nil {
		return nil, false
	}
	return region, !region.checkRegionCacheTTL(time.Now().Unix())
}

// searchCachedRegionByID finds the region from cache by id.
func (c *RegionCache) searchCachedRegionByID(regionID uint64) (*Region, bool) {
	c.mu.RLock()
	ver, ok := c.mu.latestVersions[regionID]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	region, ok := c.mu.regions[ver]
	c.mu.RUnlock()
	if !ok {
		// should not happen
		logutil.BgLogger().Warn("region not found", zap.Uint64("id", regionID), zap.Stringer("ver", &ver))
		return nil, false
	}
	return region, !region.checkRegionCacheTTL(time.Now().Unix())
}

// GetStoresByType gets stores by type `typ`
func (c *RegionCache) GetStoresByType(typ tikvrpc.EndpointType) []*Store {
	return c.stores.filter(nil, func(s *Store) bool {
		return s.getResolveState() == resolved && s.storeType == typ
	})
}

// GetAllStores gets TiKV and TiFlash stores.
func (c *RegionCache) GetAllStores() []*Store {
	return c.stores.filter(nil, func(s *Store) bool {
		return s.getResolveState() == resolved && (s.storeType == tikvrpc.TiKV || s.storeType == tikvrpc.TiFlash)
	})
}

var loadRegionCounters sync.Map

const (
	loadRegionReasonMissing        = "Missing"
	loadRegionReasonExpiredNormal  = "Expired:Normal"
	loadRegionReasonExpiredFrozen  = "Expired:Frozen"
	loadRegionReasonExpiredInvalid = "Expired:Invalid:"
	loadRegionReasonReloadOnAccess = "Reload:OnAccess"
	loadRegionReasonReloadDelayed  = "Reload:Delayed"
	loadRegionReasonUpdateBuckets  = "UpdateBuckets"
	loadRegionReasonUnknown        = "Unknown"
)

func observeLoadRegion(tag string, region *Region, expired bool, reloadFlags int32, explicitReason ...string) {
	reason := loadRegionReasonUnknown
	if len(explicitReason) > 0 {
		reason = strings.Join(explicitReason, ":")
	} else if region == nil {
		reason = loadRegionReasonMissing
	} else if expired {
		invalidReason := InvalidReason(atomic.LoadInt32((*int32)(&region.invalidReason)))
		if invalidReason != Ok {
			reason = loadRegionReasonExpiredInvalid + invalidReason.String()
		} else if region.checkSyncFlags(needExpireAfterTTL) {
			reason = loadRegionReasonExpiredFrozen
		} else {
			reason = loadRegionReasonExpiredNormal
		}
	} else if reloadFlags > 0 {
		if reloadFlags&needReloadOnAccess > 0 {
			reason = loadRegionReasonReloadOnAccess
		} else if reloadFlags&needDelayedReloadReady > 0 {
			reason = loadRegionReasonReloadDelayed
		}
	}
	type key struct {
		t string
		r string
	}
	counter, ok := loadRegionCounters.Load(key{tag, reason})
	if !ok {
		counter = metrics.TiKVLoadRegionCounter.WithLabelValues(tag, reason)
		loadRegionCounters.Store(key{tag, reason}, counter)
	}
	counter.(prometheus.Counter).Inc()
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
func (c *RegionCache) loadRegion(bo *retry.Backoffer, key []byte, isEndKey bool, opts ...pd.GetRegionOption) (*Region, error) {
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("loadRegion", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var backoffErr error
	searchPrev := false
	opts = append(opts, pd.WithBuckets())
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		start := time.Now()
		var reg *pd.Region
		var err error
		if searchPrev {
			reg, err = c.pdClient.GetPrevRegion(ctx, key, opts...)
		} else {
			reg, err = c.pdClient.GetRegion(ctx, key, opts...)
		}
		metrics.LoadRegionCacheHistogramWhenCacheMiss.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.RegionCacheCounterWithGetCacheMissError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetCacheMissOK.Inc()
		}
		if err != nil {
			if apicodec.IsDecodeError(err) {
				return nil, errors.Errorf("failed to decode region range key, key: %q, err: %v, encode_key: %q",
					util.HexRegionKeyStr(key), err, util.HexRegionKey(c.codec.EncodeRegionKey(key)))
			}
			backoffErr = errors.Errorf("loadRegion from PD failed, key: %q, err: %v", util.HexRegionKeyStr(key), err)
			continue
		}
		if reg == nil || reg.Meta == nil {
			backoffErr = errors.Errorf("region not found for key %q, encode_key: %q", util.HexRegionKeyStr(key), util.HexRegionKey(c.codec.EncodeRegionKey(key)))
			continue
		}
		if len(reg.Meta.Peers) == 0 {
			return nil, errors.New("receive Region with no available peer")
		}
		if isEndKey && !searchPrev && bytes.Equal(reg.Meta.StartKey, key) && len(reg.Meta.StartKey) != 0 {
			searchPrev = true
			continue
		}
		return newRegion(bo, c, reg)
	}
}

// loadRegionByID loads region from pd client, and picks the first peer as leader.
func (c *RegionCache) loadRegionByID(bo *retry.Backoffer, regionID uint64) (*Region, error) {
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("loadRegionByID", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		start := time.Now()
		reg, err := c.pdClient.GetRegionByID(ctx, regionID, pd.WithBuckets())
		metrics.LoadRegionCacheHistogramWithRegionByID.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.RegionCacheCounterWithGetRegionByIDError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetRegionByIDOK.Inc()
		}
		if err != nil {
			if apicodec.IsDecodeError(err) {
				return nil, errors.Errorf("failed to decode region range key, regionID: %q, err: %v", regionID, err)
			}
			backoffErr = errors.Errorf("loadRegion from PD failed, regionID: %v, err: %v", regionID, err)
			continue
		}
		if reg == nil || reg.Meta == nil {
			return nil, errors.Errorf("region not found for regionID %d", regionID)
		}
		if len(reg.Meta.Peers) == 0 {
			return nil, errors.New("receive Region with no available peer")
		}
		return newRegion(bo, c, reg)
	}
}

// For optimizing BatchLocateKeyRanges, scanRegionsFromCache scans at most `limit` regions from cache.
// It is the caller's responsibility to make sure that startKey is a node in the B-tree, otherwise, the startKey will not be included in the return regions.
func (c *RegionCache) scanRegionsFromCache(bo *retry.Backoffer, startKey, endKey []byte, limit int) ([]*Region, error) {
	if limit == 0 {
		return nil, nil
	}

	var regions []*Region
	c.mu.RLock()
	defer c.mu.RUnlock()
	regions = c.mu.sorted.AscendGreaterOrEqual(startKey, endKey, limit)

	return regions, nil
}

func (c *RegionCache) refreshRegionIndex(bo *retry.Backoffer) error {
	totalRegions := make([]*Region, 0)
	startKey := []byte{}
	for {
		regions, err := c.scanRegions(bo, startKey, nil, 10000)
		if err != nil {
			return err
		}
		totalRegions = append(totalRegions, regions...)
		if len(regions) == 0 || len(regions[len(regions)-1].meta.EndKey) == 0 {
			break
		}
		startKey = regions[len(regions)-1].meta.EndKey
	}
	c.mu.refresh(totalRegions)
	return nil
}

// scanRegions scans at most `limit` regions from PD, starts from the region containing `startKey` and in key order.
// Regions with no leader will not be returned.
func (c *RegionCache) scanRegions(bo *retry.Backoffer, startKey, endKey []byte, limit int) ([]*Region, error) {
	if limit == 0 {
		return nil, nil
	}
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("scanRegions", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		start := time.Now()
		//nolint:staticcheck
		regionsInfo, err := c.pdClient.ScanRegions(ctx, startKey, endKey, limit, pd.WithAllowFollowerHandle())
		metrics.LoadRegionCacheHistogramWithRegions.Observe(time.Since(start).Seconds())
		if err != nil {
			if apicodec.IsDecodeError(err) {
				return nil, errors.Errorf("failed to decode region range key, limit: %d, err: %v",
					limit, err)
			}
			metrics.RegionCacheCounterWithScanRegionsError.Inc()
			backoffErr = errors.Errorf(
				"scanRegion from PD failed, limit: %d, err: %v",
				limit,
				err)
			continue
		}

		metrics.RegionCacheCounterWithScanRegionsOK.Inc()

		if len(regionsInfo) == 0 {
			backoffErr = errors.Errorf("PD returned no region, limit: %d", limit)
			continue
		}
		if regionsHaveGapInRanges([]pd.KeyRange{{StartKey: startKey, EndKey: endKey}}, regionsInfo, limit) {
			backoffErr = errors.Errorf("PD returned regions have gaps, limit: %d", limit)
			continue
		}
		validRegions, err := c.handleRegionInfos(bo, regionsInfo, true)
		if err != nil {
			return nil, err
		}
		// If the region information is loaded from the local disk and the current leader has not
		// yet reported a heartbeat to PD, the region information scanned at this time will not include the leader.
		// Retry if there is no valid regions with leaders.
		if len(validRegions) == 0 {
			backoffErr = errors.Errorf("All returned regions have no leaders, limit: %d", limit)
			continue
		}
		return validRegions, nil
	}
}

// batchScanRegions scans at most `limit` regions from PD, starts from the region containing `startKey` and in key order.
func (c *RegionCache) batchScanRegions(bo *retry.Backoffer, keyRanges []pd.KeyRange, limit int, opts ...BatchLocateKeyRangesOpt) ([]*Region, error) {
	if limit == 0 || len(keyRanges) == 0 {
		return nil, nil
	}
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("batchScanRegions", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	var opt batchLocateKeyRangesOption
	for _, op := range opts {
		op(&opt)
	}
	// TODO: return start key and end key after redact is introduced.
	var backoffErr error
	for {
		if backoffErr != nil {
			err := bo.Backoff(retry.BoPDRPC, backoffErr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		start := time.Now()
		pdOpts := []pd.GetRegionOption{pd.WithAllowFollowerHandle()}
		if opt.needBuckets {
			pdOpts = append(pdOpts, pd.WithBuckets())
		}
		regionsInfo, err := c.pdClient.BatchScanRegions(ctx, keyRanges, limit, pdOpts...)
		metrics.LoadRegionCacheHistogramWithBatchScanRegions.Observe(time.Since(start).Seconds())
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.Unimplemented {
				return c.batchScanRegionsFallback(bo, keyRanges, limit, opts...)
			}
			if apicodec.IsDecodeError(err) {
				return nil, errors.Errorf("failed to decode region range key, range num: %d, limit: %d, err: %v",
					len(keyRanges), limit, err)
			}
			metrics.RegionCacheCounterWithBatchScanRegionsError.Inc()
			backoffErr = errors.Errorf(
				"batchScanRegion from PD failed, range num: %d, limit: %d, err: %v",
				len(keyRanges),
				limit,
				err)
			continue
		}

		metrics.RegionCacheCounterWithBatchScanRegionsOK.Inc()
		if len(regionsInfo) == 0 {
			backoffErr = errors.Errorf(
				"PD returned no region, range num: %d, limit: %d",
				len(keyRanges), limit,
			)
			continue
		}
		if regionsHaveGapInRanges(keyRanges, regionsInfo, limit) {
			backoffErr = errors.Errorf(
				"PD returned regions have gaps, range num: %d, limit: %d",
				len(keyRanges), limit,
			)
			continue
		}
		validRegions, err := c.handleRegionInfos(bo, regionsInfo, opt.needRegionHasLeaderPeer)
		if err != nil {
			return nil, err
		}
		// If the region information is loaded from the local disk and the current leader has not
		// yet reported a heartbeat to PD, the region information scanned at this time will not include the leader.
		// Retry if there is no valid regions with leaders.
		if len(validRegions) == 0 {
			backoffErr = errors.Errorf("All returned regions have no leaders, limit: %d", limit)
			continue
		}
		return validRegions, nil
	}
}

// regionsHaveGapInRanges checks if the loaded regions can fully cover the key ranges.
// If there are any gaps between the regions, it returns true, then the requests might be retried.
// TODO: remove this function after PD client supports gap detection and handling it.
func regionsHaveGapInRanges(ranges []pd.KeyRange, regionsInfo []*pd.Region, limit int) bool {
	if len(ranges) == 0 {
		return false
	}
	if len(regionsInfo) == 0 {
		return true
	}
	checkIdx := 0                  // checked index of ranges
	checkKey := ranges[0].StartKey // checked key of ranges
	for _, r := range regionsInfo {
		if r.Meta == nil {
			return true
		}
		if bytes.Compare(r.Meta.StartKey, checkKey) > 0 {
			// there is a gap between returned region's start_key and current check key
			return true
		}
		if len(r.Meta.EndKey) == 0 {
			// the current region contains all the rest ranges.
			return false
		}
		checkKey = r.Meta.EndKey
		for len(ranges[checkIdx].EndKey) > 0 && bytes.Compare(checkKey, ranges[checkIdx].EndKey) >= 0 {
			// the end_key of returned region can cover multi ranges.
			checkIdx++
			if checkIdx == len(ranges) {
				// all ranges are covered.
				return false
			}
		}
		if bytes.Compare(checkKey, ranges[checkIdx].StartKey) < 0 {
			// if check_key < start_key, move it forward to start_key.
			checkKey = ranges[checkIdx].StartKey
		}
	}
	if limit > 0 && len(regionsInfo) == limit {
		// the regionsInfo is limited by the limit, so there may be some ranges not covered.
		// But the previous regions are continuous, so we just need to check the rest ranges.
		return false
	}
	if checkIdx < len(ranges)-1 {
		// there are still some ranges not covered.
		return true
	}
	if len(checkKey) == 0 {
		return false
	} else if len(ranges[checkIdx].EndKey) == 0 {
		return true
	}
	return bytes.Compare(checkKey, ranges[checkIdx].EndKey) < 0
}

func (c *RegionCache) batchScanRegionsFallback(bo *retry.Backoffer, keyRanges []pd.KeyRange, limit int, opts ...BatchLocateKeyRangesOpt) ([]*Region, error) {
	logutil.BgLogger().Warn("batch scan regions fallback to scan regions", zap.Int("range-num", len(keyRanges)))
	res := make([]*Region, 0, len(keyRanges))
	var lastRegion *Region
	for _, keyRange := range keyRanges {
		if lastRegion != nil {
			endKey := lastRegion.EndKey()
			if len(endKey) == 0 {
				// end_key is empty means the last region is the last region of the store, which certainly contains all the rest ranges.
				break
			}
			if bytes.Compare(endKey, keyRange.EndKey) >= 0 {
				continue
			}
			if bytes.Compare(endKey, keyRange.StartKey) > 0 {
				keyRange.StartKey = endKey
			}
		}
		regions, err := c.scanRegions(bo, keyRange.StartKey, keyRange.EndKey, limit)
		if err != nil {
			return nil, err
		}
		if len(regions) > 0 {
			lastRegion = regions[len(regions)-1]
		}
		res = append(res, regions...)
		if len(regions) >= limit {
			return res, nil
		}
		limit -= len(regions)
	}
	return res, nil
}

func (c *RegionCache) handleRegionInfos(bo *retry.Backoffer, regionsInfo []*pd.Region, needLeader bool) ([]*Region, error) {
	regions := make([]*Region, 0, len(regionsInfo))
	for _, r := range regionsInfo {
		// Leader id = 0 indicates no leader.
		if needLeader && (r.Leader == nil || r.Leader.GetId() == 0) {
			continue
		}
		region, err := newRegion(bo, c, r)
		if err != nil {
			return nil, err
		}
		regions = append(regions, region)
	}
	if len(regions) == 0 {
		return nil, nil
	}
	if len(regions) < len(regionsInfo) {
		logutil.Logger(context.Background()).Debug(
			"regionCache: scanRegion finished but some regions has no leader.")
	}
	return regions, nil
}

// GetCachedRegionWithRLock returns region with lock.
func (c *RegionCache) GetCachedRegionWithRLock(regionID RegionVerID) (r *Region) {
	c.mu.RLock()
	r = c.mu.regions[regionID]
	c.mu.RUnlock()
	return
}

func (c *RegionCache) getStoreAddr(bo *retry.Backoffer, region *Region, store *Store) (addr string, err error) {
	state := store.getResolveState()
	switch state {
	case resolved, needCheck:
		addr = store.addr
		return
	case unresolved:
		addr, err = store.initResolve(bo, c.stores)
		return
	case deleted:
		addr = c.changeToActiveStore(region, store.storeID)
		return
	case tombstone:
		return "", nil
	default:
		panic("unsupported resolve state")
	}
}

func (c *RegionCache) getProxyStore(region *Region, store *Store, rs *regionStore, workStoreIdx AccessIndex) (proxyStore *Store, proxyAccessIdx AccessIndex, proxyStoreIdx int) {
	if !c.enableForwarding || store.storeType != tikvrpc.TiKV || store.getLivenessState() == reachable {
		return
	}

	if rs.proxyTiKVIdx >= 0 {
		storeIdx, proxyStore := rs.accessStore(tiKVOnly, rs.proxyTiKVIdx)
		return proxyStore, rs.proxyTiKVIdx, storeIdx
	}

	tikvNum := rs.accessStoreNum(tiKVOnly)
	if tikvNum <= 1 {
		return
	}

	// Randomly select an non-leader peer
	first := rand.Intn(tikvNum - 1)
	if first >= int(workStoreIdx) {
		first = (first + 1) % tikvNum
	}

	// If the current selected peer is not reachable, switch to the next one, until a reachable peer is found or all
	// peers are checked.
	for i := 0; i < tikvNum; i++ {
		index := (i + first) % tikvNum
		// Skip work store which is the actual store to be accessed
		if index == int(workStoreIdx) {
			continue
		}
		storeIdx, store := rs.accessStore(tiKVOnly, AccessIndex(index))
		// Skip unreachable stores.
		if store.getLivenessState() == unreachable {
			continue
		}

		rs.setProxyStoreIdx(region, AccessIndex(index))
		return store, AccessIndex(index), storeIdx
	}

	return nil, 0, 0
}

// changeToActiveStore replace the deleted store in the region by an up-to-date store in the stores map.
// The order is guaranteed by reResolve() which adds the new store before marking old store deleted.
func (c *RegionCache) changeToActiveStore(region *Region, storeID uint64) (addr string) {
	store, _ := c.stores.get(storeID)
	for {
		oldRegionStore := region.getStore()
		newRegionStore := oldRegionStore.clone()
		newRegionStore.stores = make([]*Store, 0, len(oldRegionStore.stores))
		for _, s := range oldRegionStore.stores {
			if s.storeID == store.storeID {
				newRegionStore.stores = append(newRegionStore.stores, store)
			} else {
				newRegionStore.stores = append(newRegionStore.stores, s)
			}
		}
		if region.compareAndSwapStore(oldRegionStore, newRegionStore) {
			break
		}
	}
	addr = store.addr
	return
}

// OnBucketVersionNotMatch removes the old buckets meta if the version is stale.
func (c *RegionCache) OnBucketVersionNotMatch(ctx *RPCContext, version uint64, keys [][]byte) {
	r := c.GetCachedRegionWithRLock(ctx.Region)
	if r == nil {
		return
	}

	buckets := r.getStore().buckets
	if buckets == nil || buckets.GetVersion() < version {
		oldStore := r.getStore()
		store := oldStore.clone()
		store.buckets = &metapb.Buckets{
			Version:  version,
			Keys:     keys,
			RegionId: r.meta.GetId(),
		}
		r.compareAndSwapStore(oldStore, store)
	}
}

// OnRegionEpochNotMatch removes the old region and inserts new regions into the cache.
// It returns whether retries the request because it's possible the region epoch is ahead of TiKV's due to slow appling.
func (c *RegionCache) OnRegionEpochNotMatch(bo *retry.Backoffer, ctx *RPCContext, currentRegions []*metapb.Region) (bool, error) {
	if len(currentRegions) == 0 {
		c.InvalidateCachedRegionWithReason(ctx.Region, EpochNotMatch)
		return false, nil
	}

	// Find whether the region epoch in `ctx` is ahead of TiKV's. If so, backoff.
	for _, meta := range currentRegions {
		if meta.GetId() == ctx.Region.id &&
			(meta.GetRegionEpoch().GetConfVer() < ctx.Region.confVer ||
				meta.GetRegionEpoch().GetVersion() < ctx.Region.ver) {
			err := errors.Errorf("region epoch is ahead of tikv. rpc ctx: %+v, currentRegions: %+v", ctx, currentRegions)
			logutil.Logger(bo.GetCtx()).Info("region epoch is ahead of tikv", zap.Error(err))
			return true, bo.Backoff(retry.BoRegionMiss, err)
		}
	}

	var buckets *metapb.Buckets
	c.mu.Lock()
	cachedRegion, ok := c.mu.regions[ctx.Region]
	if ok {
		buckets = cachedRegion.getStore().buckets
	}
	c.mu.Unlock()

	needInvalidateOld := true
	newRegions := make([]*Region, 0, len(currentRegions))
	// If the region epoch is not ahead of TiKV's, replace region meta in region cache.
	for _, meta := range currentRegions {
		// TODO(youjiali1995): new regions inherit old region's buckets now. Maybe we should make EpochNotMatch error
		// carry buckets information. Can it bring much overhead?
		region, err := newRegion(bo, c, &pd.Region{Meta: meta, Buckets: buckets})
		if err != nil {
			return false, err
		}
		var initLeaderStoreID uint64
		if ctx.Store.storeType == tikvrpc.TiFlash {
			initLeaderStoreID = region.findElectableStoreID()
		} else {
			initLeaderStoreID = ctx.Store.storeID
		}
		region.switchWorkLeaderToPeer(region.getPeerOnStore(initLeaderStoreID))
		newRegions = append(newRegions, region)
		if ctx.Region == region.VerID() {
			needInvalidateOld = false
		}
	}
	if needInvalidateOld && cachedRegion != nil {
		cachedRegion.invalidate(EpochNotMatch)
	}

	c.mu.Lock()
	for _, region := range newRegions {
		c.insertRegionToCache(region, true, true)
	}
	c.mu.Unlock()

	return false, nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RegionCache) PDClient() pd.Client {
	return c.pdClient
}

// GetTiFlashStores returns the information of all tiflash nodes. Like `GetAllStores`, the method only returns resolved
// stores so that users won't be bothered by tombstones. (related issue: https://github.com/pingcap/tidb/issues/46602)
func (c *RegionCache) GetTiFlashStores(labelFilter LabelFilter) []*Store {
	return c.stores.filter(nil, func(s *Store) bool {
		return s.storeType == tikvrpc.TiFlash && labelFilter(s.labels) && s.getResolveState() == resolved
	})
}

// GetTiFlashComputeStores returns all stores with lable <engine, tiflash_compute>.
func (c *RegionCache) GetTiFlashComputeStores(bo *retry.Backoffer) (res []*Store, err error) {
	stores, needReload := c.stores.listTiflashComputeStores()

	if needReload {
		stores, err = reloadTiFlashComputeStores(bo.GetCtx(), c.stores)
		if err == nil {
			c.stores.setTiflashComputeStores(stores)
		}
		return stores, err
	}
	return stores, nil
}

func reloadTiFlashComputeStores(ctx context.Context, registry storeRegistry) (res []*Store, _ error) {
	stores, err := registry.fetchAllStores(ctx)
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		if s.GetState() == metapb.StoreState_Up && isStoreContainLabel(s.GetLabels(), tikvrpc.EngineLabelKey, tikvrpc.EngineLabelTiFlashCompute) {
			res = append(res, newStore(
				s.GetId(),
				s.GetAddress(),
				s.GetPeerAddress(),
				s.GetStatusAddress(),
				tikvrpc.GetStoreTypeByMeta(s),
				resolved,
				s.GetLabels(),
			))
		}
	}
	return res, nil
}

// InvalidateTiFlashComputeStoresIfGRPCError will invalid cache if is GRPC error.
// For now, only consider GRPC unavailable error.
func (c *RegionCache) InvalidateTiFlashComputeStoresIfGRPCError(err error) bool {
	var invalidate bool
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable:
			invalidate = true
		default:
		}
	}
	if !invalidate {
		return false
	}

	c.InvalidateTiFlashComputeStores()
	return true
}

// InvalidateTiFlashComputeStores set needReload be true,
// and will refresh tiflash_compute store cache next time.
func (c *RegionCache) InvalidateTiFlashComputeStores() {
	c.stores.markTiflashComputeStoresNeedReload()
}

// UpdateBucketsIfNeeded queries PD to update the buckets of the region in the cache if
// the latestBucketsVer is newer than the cached one.
func (c *RegionCache) UpdateBucketsIfNeeded(regionID RegionVerID, latestBucketsVer uint64) {
	r := c.GetCachedRegionWithRLock(regionID)
	if r == nil {
		return
	}

	buckets := r.getStore().buckets
	var bucketsVer uint64
	if buckets != nil {
		bucketsVer = buckets.GetVersion()
	}
	if bucketsVer < latestBucketsVer {
		// TODO(youjiali1995): use singleflight.
		go func() {
			bo := retry.NewBackoffer(context.Background(), 20000)
			observeLoadRegion("ByID", r, false, 0, loadRegionReasonUpdateBuckets)
			new, err := c.loadRegionByID(bo, regionID.id)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("failed to update buckets",
					zap.String("region", regionID.String()), zap.Uint64("bucketsVer", bucketsVer),
					zap.Uint64("latestBucketsVer", latestBucketsVer), zap.Error(err))
				return
			}
			c.mu.Lock()
			c.insertRegionToCache(new, true, true)
			c.mu.Unlock()
		}()
	}
}

const cleanCacheInterval = time.Second
const cleanRegionNumPerRound = 50
const refreshStoreListInterval = 10 * time.Second

// gcScanItemHook is only used for testing
var gcScanItemHook = new(atomic.Pointer[func(*btreeItem)])

// The returned function is expected to run in a background goroutine.
// It keeps iterating over the whole region cache, searching for stale region
// info. It runs at cleanCacheInterval and checks only cleanRegionNumPerRound
// regions. In this way, the impact of this background goroutine should be
// negligible.
func (c *RegionCache) gcRoundFunc(limit int) func(context.Context, time.Time) bool {
	if limit < 1 {
		limit = 1
	}
	beginning := newBtreeSearchItem([]byte(""))
	cursor := beginning
	expiredItems := make([]*btreeItem, limit)
	needCheckRegions := make([]*Region, limit)

	return func(_ context.Context, t time.Time) bool {
		expiredItems = expiredItems[:0]
		needCheckRegions = needCheckRegions[:0]
		hasMore, count, ts := false, 0, t.Unix()
		onScanItem := gcScanItemHook.Load()

		// Only RLock when checking TTL to avoid blocking other readers
		c.mu.RLock()
		c.mu.sorted.b.Ascend(cursor, func(item *btreeItem) bool {
			count++
			if count > limit {
				cursor = item
				hasMore = true
				return false
			}
			if onScanItem != nil {
				(*onScanItem)(item)
			}
			if item.cachedRegion.isCacheTTLExpired(ts) {
				expiredItems = append(expiredItems, item)
			} else {
				needCheckRegions = append(needCheckRegions, item.cachedRegion)
			}
			return true
		})
		c.mu.RUnlock()

		// Reach the end of the region cache, start from the beginning
		if !hasMore {
			cursor = beginning
		}

		// Clean expired regions
		if len(expiredItems) > 0 {
			c.mu.Lock()
			for _, item := range expiredItems {
				c.mu.sorted.b.Delete(item)
				c.mu.removeVersionFromCache(item.cachedRegion.VerID(), item.cachedRegion.GetID())
			}
			c.mu.Unlock()
		}

		// Check remaining regions and update sync flags
		for _, region := range needCheckRegions {
			syncFlags := region.getSyncFlags()
			if syncFlags&needDelayedReloadReady > 0 {
				// the region will be reload soon on access
				continue
			}
			if syncFlags&needDelayedReloadPending > 0 {
				region.setSyncFlags(needDelayedReloadReady)
				// the region will be reload soon on access, no need to check if it needs to be expired
				continue
			}
			if syncFlags&needExpireAfterTTL == 0 {
				regionStore := region.getStore()
				for i, store := range regionStore.stores {
					// if the region has a stale or unreachable store, let it expire after TTL.
					if atomic.LoadUint32(&store.epoch) != regionStore.storeEpochs[i] || store.getLivenessState() != reachable {
						region.setSyncFlags(needExpireAfterTTL)
						break
					}
				}
			}
		}
		return false
	}
}

// btreeItem is BTree's Item that uses []byte to compare.
type btreeItem struct {
	key          []byte
	cachedRegion *Region
}

func newBtreeItem(cr *Region) *btreeItem {
	return &btreeItem{
		key:          cr.StartKey(),
		cachedRegion: cr,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{
		key: key,
	}
}

// GetID returns id.
func (r *Region) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta returns region meta.
func (r *Region) GetMeta() *metapb.Region {
	return proto.Clone(r.meta).(*metapb.Region)
}

// GetLeaderPeerID returns leader peer ID.
func (r *Region) GetLeaderPeerID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= store.accessStoreNum(tiKVOnly) {
		return 0
	}
	storeIdx, _ := store.accessStore(tiKVOnly, store.workTiKVIdx)
	return r.meta.Peers[storeIdx].Id
}

// GetLeaderStoreID returns the store ID of the leader region.
func (r *Region) GetLeaderStoreID() uint64 {
	store := r.getStore()
	if int(store.workTiKVIdx) >= store.accessStoreNum(tiKVOnly) {
		return 0
	}
	storeIdx, _ := store.accessStore(tiKVOnly, store.workTiKVIdx)
	return r.meta.Peers[storeIdx].StoreId
}

func (r *Region) getKvStorePeer(rs *regionStore, aidx AccessIndex) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	storeIdx, store = rs.accessStore(tiKVOnly, aidx)
	peer = r.meta.Peers[storeIdx]
	accessIdx = aidx
	return
}

// WorkStorePeer returns current work store with work peer.
func (r *Region) WorkStorePeer(rs *regionStore) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.workTiKVIdx)
}

// FollowerStorePeer returns a follower store with follower peer.
func (r *Region) FollowerStorePeer(rs *regionStore, followerStoreSeed uint32, op *storeSelectorOp) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.follower(followerStoreSeed, op))
}

// AnyStorePeer returns a leader or follower store with the associated peer.
func (r *Region) AnyStorePeer(rs *regionStore, followerStoreSeed uint32, op *storeSelectorOp) (store *Store, peer *metapb.Peer, accessIdx AccessIndex, storeIdx int) {
	return r.getKvStorePeer(rs, rs.kvPeer(followerStoreSeed, op))
}

// RegionVerID is a unique ID that can identify a Region at a specific version.
type RegionVerID struct {
	id      uint64
	confVer uint64
	ver     uint64
}

// NewRegionVerID creates a region ver id, which used for invalidating regions.
func NewRegionVerID(id, confVer, ver uint64) RegionVerID {
	return RegionVerID{id, confVer, ver}
}

// GetID returns the id of the region
func (r *RegionVerID) GetID() uint64 {
	return r.id
}

// GetVer returns the version of the region's epoch
func (r *RegionVerID) GetVer() uint64 {
	return r.ver
}

// GetConfVer returns the conf ver of the region's epoch
func (r *RegionVerID) GetConfVer() uint64 {
	return r.confVer
}

// String formats the RegionVerID to string
func (r *RegionVerID) String() string {
	return fmt.Sprintf("{ region id: %v, ver: %v, confVer: %v }", r.id, r.ver, r.confVer)
}

// Equals checks whether the RegionVerID equals to another one
func (r *RegionVerID) Equals(another RegionVerID) bool {
	return r.id == another.id && r.confVer == another.confVer && r.ver == another.ver
}

// VerID returns the Region's RegionVerID.
func (r *Region) VerID() RegionVerID {
	return RegionVerID{
		id:      r.meta.GetId(),
		confVer: r.meta.GetRegionEpoch().GetConfVer(),
		ver:     r.meta.GetRegionEpoch().GetVersion(),
	}
}

// StartKey returns StartKey.
func (r *Region) StartKey() []byte {
	return r.meta.StartKey
}

// EndKey returns EndKey.
func (r *Region) EndKey() []byte {
	return r.meta.EndKey
}

func (r *Region) getPeerStoreIndex(peer *metapb.Peer) (idx int, found bool) {
	if len(r.meta.Peers) == 0 || peer == nil {
		return
	}
	for i, p := range r.meta.Peers {
		if isSamePeer(p, peer) {
			idx = i
			found = true
			return
		}
	}
	return
}

// switchWorkLeaderToPeer switches current store to the one on specific store. It returns
// false if no peer matches the peer.
func (r *Region) switchWorkLeaderToPeer(peer *metapb.Peer) (found bool) {
	globalStoreIdx, found := r.getPeerStoreIndex(peer)
	if !found {
		return
	}
retry:
	// switch to new leader.
	oldRegionStore := r.getStore()
	var leaderIdx AccessIndex
	for i, gIdx := range oldRegionStore.accessIndex[tiKVOnly] {
		if gIdx == globalStoreIdx {
			leaderIdx = AccessIndex(i)
		}
	}
	if oldRegionStore.workTiKVIdx == leaderIdx {
		return
	}
	newRegionStore := oldRegionStore.clone()
	newRegionStore.workTiKVIdx = leaderIdx
	newRegionStore.storeEpochs[leaderIdx] = atomic.LoadUint32(&newRegionStore.stores[leaderIdx].epoch)
	if !r.compareAndSwapStore(oldRegionStore, newRegionStore) {
		goto retry
	}
	return
}

func (r *regionStore) switchNextFlashPeer(rr *Region, currentPeerIdx AccessIndex) {
	nextIdx := (currentPeerIdx + 1) % AccessIndex(r.accessStoreNum(tiFlashOnly))
	newRegionStore := r.clone()
	newRegionStore.workTiFlashIdx.Store(int32(nextIdx))
	rr.compareAndSwapStore(r, newRegionStore)
}

func (r *regionStore) switchNextTiKVPeer(rr *Region, currentPeerIdx AccessIndex) {
	if r.workTiKVIdx != currentPeerIdx {
		return
	}
	nextIdx := (currentPeerIdx + 1) % AccessIndex(r.accessStoreNum(tiKVOnly))
	newRegionStore := r.clone()
	newRegionStore.workTiKVIdx = nextIdx
	rr.compareAndSwapStore(r, newRegionStore)
}

func (r *regionStore) setProxyStoreIdx(rr *Region, idx AccessIndex) {
	if r.proxyTiKVIdx == idx {
		return
	}

	newRegionStore := r.clone()
	newRegionStore.proxyTiKVIdx = idx
	success := rr.compareAndSwapStore(r, newRegionStore)
	logutil.BgLogger().Debug("try set proxy store index",
		zap.Uint64("region", rr.GetID()),
		zap.Int("index", int(idx)),
		zap.Bool("success", success))
}

func (r *regionStore) unsetProxyStoreIfNeeded(rr *Region) {
	r.setProxyStoreIdx(rr, -1)
}

func (r *Region) findElectableStoreID() uint64 {
	if len(r.meta.Peers) == 0 {
		return 0
	}
	for _, p := range r.meta.Peers {
		if p.Role != metapb.PeerRole_Learner {
			return p.StoreId
		}
	}
	return 0
}

func (r *Region) getPeerOnStore(storeID uint64) *metapb.Peer {
	for _, p := range r.meta.Peers {
		if p.StoreId == storeID {
			return p
		}
	}
	return nil
}

// Contains checks whether the key is in the region, for the maximum region endKey is empty.
// startKey <= key < endKey.
func (r *Region) Contains(key []byte) bool {
	return contains(r.meta.GetStartKey(), r.meta.GetEndKey(), key)
}

// ContainsByEnd check the region contains the greatest key that is less than key.
// for the maximum region endKey is empty.
// startKey < key <= endKey.
func (r *Region) ContainsByEnd(key []byte) bool {
	// Only a region's right bound expands to inf contains the point at inf.
	if len(key) == 0 {
		return len(r.EndKey()) == 0
	}
	return bytes.Compare(r.meta.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) <= 0 || len(r.meta.GetEndKey()) == 0)
}

// checkAndUpdateStoreHealthStatus checks and updates health stats on each store.
func (c *RegionCache) checkAndUpdateStoreHealthStatus(ctx context.Context, now time.Time) bool {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("panic in the checkAndUpdateStoreHealthStatus goroutine",
				zap.Any("r", r),
				zap.Stack("stack trace"))
			if _, err := util.EvalFailpoint("doNotRecoverStoreHealthCheckPanic"); err == nil {
				panic(r)
			}
		}
	}()
	var stores []*Store
	c.stores.forEach(func(store *Store) {
		stores = append(stores, store)
	})
	for _, store := range stores {
		store.healthStatus.tick(ctx, now, store, c.requestHealthFeedbackCallback)
		healthDetails := store.healthStatus.GetHealthStatusDetail()
		metrics.TiKVStoreSlowScoreGauge.WithLabelValues(strconv.FormatUint(store.storeID, 10)).Set(float64(healthDetails.ClientSideSlowScore))
		metrics.TiKVFeedbackSlowScoreGauge.WithLabelValues(strconv.FormatUint(store.storeID, 10)).Set(float64(healthDetails.TiKVSideSlowScore))
	}

	return false
}

// reportStoreReplicaFlows reports the statistics on the related replicaFlowsType.
func (c *RegionCache) reportStoreReplicaFlows() {
	c.stores.forEach(func(store *Store) {
		for destType := toLeader; destType < numReplicaFlowsType; destType++ {
			metrics.TiKVPreferLeaderFlowsGauge.WithLabelValues(destType.String(), store.addr).Set(float64(store.getReplicaFlowsStats(destType)))
			store.resetReplicaFlowsStats(destType)
		}
	})
}

func isSamePeer(lhs *metapb.Peer, rhs *metapb.Peer) bool {
	return lhs == rhs || (lhs.GetId() == rhs.GetId() && lhs.GetStoreId() == rhs.GetStoreId())
}

// contains returns true if startKey <= key < endKey. Empty endKey is the maximum key.
func contains(startKey, endKey, key []byte) bool {
	return bytes.Compare(startKey, key) <= 0 &&
		(bytes.Compare(key, endKey) < 0 || len(endKey) == 0)
}

func (c *RegionCache) onHealthFeedback(feedback *kvrpcpb.HealthFeedback) {
	store, ok := c.stores.get(feedback.GetStoreId())
	if !ok {
		logutil.BgLogger().Info("dropped health feedback info due to unknown store id", zap.Uint64("storeID", feedback.GetStoreId()))
		return
	}
	store.recordHealthFeedback(feedback)
}

// GetClientEventListener returns the listener to observe the RPC client's events and let the region cache respond to
// them. When creating the `KVStore` using `tikv.NewKVStore` function, the listener will be setup immediately.
func (c *RegionCache) GetClientEventListener() client.ClientEventListener {
	return &regionCacheClientEventListener{c: c}
}

// regionCacheClientEventListener is the listener to let RegionCache respond to events in the RPC client.
type regionCacheClientEventListener struct {
	c *RegionCache
}

// OnHealthFeedback implements the `client.ClientEventListener` interface.
func (l *regionCacheClientEventListener) OnHealthFeedback(feedback *kvrpcpb.HealthFeedback) {
	l.c.onHealthFeedback(feedback)
}
