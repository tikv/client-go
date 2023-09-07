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
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	btreeDegree               = 32
	invalidatedLastAccessTime = -1
	defaultRegionsPerBatch    = 128
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

const (
	updated  int32 = iota // region is updated and no need to reload.
	needSync              // need sync new region info.
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

// Region presents kv region
type Region struct {
	meta          *metapb.Region // raw region meta from PD, immutable after init
	store         unsafe.Pointer // point to region store info, see RegionStore
	syncFlag      int32          // region need be sync in next turn
	lastAccess    int64          // last region access time, see checkRegionCacheTTL
	invalidReason InvalidReason  // the reason why the region is invalidated
	asyncReload   atomic.Bool    // the region need to be reloaded in async mode
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
	workTiFlashIdx atomic2.Int32
	// buckets is not accurate and it can change even if the region is not changed.
	// It can be stale and buckets keys can be out of the region range.
	buckets *metapb.Buckets
	// record all storeIDs on which pending peers reside.
	// key is storeID, val is peerID.
	pendingTiFlashPeerStores map[uint64]uint64
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
	return s.IsLabelsMatch(op.labels) && (!op.preferLeader || (aidx == r.workTiKVIdx && !s.isSlow()))
}

func newRegion(bo *retry.Backoffer, c *RegionCache, pdRegion *pd.Region) (*Region, error) {
	r := &Region{meta: pdRegion.Meta}
	// regionStore pull used store from global store map
	// to avoid acquire storeMu in later access.
	rs := &regionStore{
		workTiKVIdx:              0,
		proxyTiKVIdx:             -1,
		stores:                   make([]*Store, 0, len(r.meta.Peers)),
		pendingTiFlashPeerStores: map[uint64]uint64{},
		storeEpochs:              make([]uint32, 0, len(r.meta.Peers)),
		buckets:                  pdRegion.Buckets,
	}

	leader := pdRegion.Leader
	var leaderAccessIdx AccessIndex
	availablePeers := r.meta.GetPeers()[:0]
	for _, p := range r.meta.Peers {
		c.storeMu.RLock()
		store, exists := c.storeMu.stores[p.StoreId]
		c.storeMu.RUnlock()
		if !exists {
			store = c.getStoreByStoreID(p.StoreId)
		}
		addr, err := store.initResolve(bo, c)
		if err != nil {
			return nil, err
		}
		// Filter the peer on a tombstone store.
		if addr == "" {
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
		for _, pendingPeer := range pdRegion.PendingPeers {
			if pendingPeer.Id == p.Id {
				rs.pendingTiFlashPeerStores[store.storeID] = p.Id
			}
		}
	}
	// TODO(youjiali1995): It's possible the region info in PD is stale for now but it can recover.
	// Maybe we need backoff here.
	if len(availablePeers) == 0 {
		return nil, errors.Errorf("no available peers, region: {%v}", r.meta)
	}
	rs.workTiKVIdx = leaderAccessIdx
	r.meta.Peers = availablePeers

	r.setStore(rs)

	// mark region has been init accessed.
	r.lastAccess = time.Now().Unix()
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
	lastAccess := atomic.LoadInt64(&r.lastAccess)
	return ts-lastAccess > regionCacheTTLSec
}

func (r *Region) checkRegionCacheTTL(ts int64) bool {
	// Only consider use percentage on this failpoint, for example, "2%return"
	if _, err := util.EvalFailpoint("invalidateRegionCache"); err == nil {
		r.invalidate(Other)
	}
	for {
		lastAccess := atomic.LoadInt64(&r.lastAccess)
		if ts-lastAccess > regionCacheTTLSec {
			return false
		}
		if atomic.CompareAndSwapInt64(&r.lastAccess, lastAccess, ts) {
			return true
		}
	}
}

// invalidate invalidates a region, next time it will got null result.
func (r *Region) invalidate(reason InvalidReason) {
	metrics.RegionCacheCounterWithInvalidateRegionFromCacheOK.Inc()
	atomic.StoreInt32((*int32)(&r.invalidReason), int32(reason))
	atomic.StoreInt64(&r.lastAccess, invalidatedLastAccessTime)
}

// scheduleReload schedules reload region request in next LocateKey.
func (r *Region) scheduleReload() {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue != updated {
		return
	}
	atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, needSync)
}

// checkNeedReloadAndMarkUpdated returns whether the region need reload and marks the region to be updated.
func (r *Region) checkNeedReloadAndMarkUpdated() bool {
	oldValue := atomic.LoadInt32(&r.syncFlag)
	if oldValue == updated {
		return false
	}
	return atomic.CompareAndSwapInt32(&r.syncFlag, oldValue, updated)
}

func (r *Region) checkNeedReload() bool {
	v := atomic.LoadInt32(&r.syncFlag)
	return v != updated
}

func (r *Region) isValid() bool {
	return r != nil && !r.checkNeedReload() && r.checkRegionCacheTTL(time.Now().Unix())
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
		r.insertRegionToCache(region, true)
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

type livenessFunc func(s *Store, bo *retry.Backoffer) livenessState

// RegionCache caches Regions loaded from PD.
// All public methods of this struct should be thread-safe, unless explicitly pointed out or the method is for testing
// purposes only.
type RegionCache struct {
	pdClient         pd.Client
	codec            apicodec.Codec
	enableForwarding bool

	mu regionIndexMu

	storeMu struct {
		sync.RWMutex
		stores map[uint64]*Store
	}
	tiflashComputeStoreMu struct {
		sync.RWMutex
		needReload bool
		stores     []*Store
	}
	notifyCheckCh chan struct{}

	// Context for background jobs
	ctx        context.Context
	cancelFunc context.CancelFunc

	testingKnobs struct {
		// Replace the requestLiveness function for test purpose. Note that in unit tests, if this is not set,
		// requestLiveness always returns unreachable.
		mockRequestLiveness atomic.Pointer[livenessFunc]
	}

	regionsNeedReload struct {
		sync.Mutex
		regions []uint64
	}
}

// NewRegionCache creates a RegionCache.
func NewRegionCache(pdClient pd.Client) *RegionCache {
	c := &RegionCache{
		pdClient: pdClient,
	}

	c.codec = apicodec.NewCodecV1(apicodec.ModeRaw)
	if codecPDClient, ok := pdClient.(*CodecPDClient); ok {
		c.codec = codecPDClient.GetCodec()
	}

	c.storeMu.stores = make(map[uint64]*Store)
	c.tiflashComputeStoreMu.needReload = true
	c.tiflashComputeStoreMu.stores = make([]*Store, 0)
	c.notifyCheckCh = make(chan struct{}, 1)
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	interval := config.GetGlobalConfig().StoresRefreshInterval

	if config.GetGlobalConfig().EnablePreload {
		logutil.BgLogger().Info("preload region index start")
		if err := c.refreshRegionIndex(retry.NewBackofferWithVars(c.ctx, 20000, nil)); err != nil {
			logutil.BgLogger().Error("refresh region index failed", zap.Error(err))
		}
		logutil.BgLogger().Info("preload region index finish")
	} else {
		c.mu = *newRegionIndexMu(nil)
	}

	go c.asyncCheckAndResolveLoop(time.Duration(interval) * time.Second)
	c.enableForwarding = config.GetGlobalConfig().EnableForwarding
	// Default use 15s as the update inerval.
	go c.asyncUpdateStoreSlowScore(time.Duration(interval/4) * time.Second)
	if config.GetGlobalConfig().RegionsRefreshInterval > 0 {
		c.timelyRefreshCache(config.GetGlobalConfig().RegionsRefreshInterval)
	} else {
		// cacheGC is not compatible with timelyRefreshCache
		go c.cacheGC()
	}
	go c.asyncReportStoreReplicaFlows(time.Duration(interval/2) * time.Second)
	return c
}

// clear clears all cached data in the RegionCache. It's only used in tests.
func (c *RegionCache) clear() {
	c.mu = *newRegionIndexMu(nil)
	c.storeMu.Lock()
	c.storeMu.stores = make(map[uint64]*Store)
	c.storeMu.Unlock()
}

// thread unsafe, should use with lock
func (c *RegionCache) insertRegionToCache(cachedRegion *Region, invalidateOldRegion bool) {
	c.mu.insertRegionToCache(cachedRegion, invalidateOldRegion)
}

// Close releases region cache's resource.
func (c *RegionCache) Close() {
	c.cancelFunc()
}

// asyncCheckAndResolveLoop with
func (c *RegionCache) asyncCheckAndResolveLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	reloadRegionTicker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		reloadRegionTicker.Stop()
	}()
	var needCheckStores []*Store
	reloadNextLoop := make(map[uint64]struct{})
	for {
		needCheckStores = needCheckStores[:0]
		select {
		case <-c.ctx.Done():
			return
		case <-c.notifyCheckCh:
			c.checkAndResolve(needCheckStores, func(s *Store) bool {
				return s.getResolveState() == needCheck
			})
		case <-ticker.C:
			// refresh store to update labels.
			c.checkAndResolve(needCheckStores, func(s *Store) bool {
				state := s.getResolveState()
				// Only valid stores should be reResolved. In fact, it's impossible
				// there's a deleted store in the stores map which guaranteed by reReslve().
				return state != unresolved && state != tombstone && state != deleted
			})
		case <-reloadRegionTicker.C:
			for regionID := range reloadNextLoop {
				c.reloadRegion(regionID)
				delete(reloadNextLoop, regionID)
			}
			c.regionsNeedReload.Lock()
			for _, regionID := range c.regionsNeedReload.regions {
				// will reload in next tick, wait a while for two reasons:
				// 1. there may an unavailable duration while recreating the connection.
				// 2. the store may just be started, and wait safe ts synced to avoid the
				// possible dataIsNotReady error.
				reloadNextLoop[regionID] = struct{}{}
			}
			c.regionsNeedReload.regions = c.regionsNeedReload.regions[:0]
			c.regionsNeedReload.Unlock()
		}
	}
}

// checkAndResolve checks and resolve addr of failed stores.
// this method isn't thread-safe and only be used by one goroutine.
func (c *RegionCache) checkAndResolve(needCheckStores []*Store, needCheck func(*Store) bool) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("panic in the checkAndResolve goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()

	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		if needCheck(store) {
			needCheckStores = append(needCheckStores, store)
		}
	}
	c.storeMu.RUnlock()

	for _, store := range needCheckStores {
		_, err := store.reResolve(c)
		tikverr.Log(err)
	}
}

// SetRegionCacheStore is used to set a store in region cache, for testing only
func (c *RegionCache) SetRegionCacheStore(id uint64, addr string, peerAddr string, storeType tikvrpc.EndpointType, state uint64, labels []*metapb.StoreLabel) {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	c.storeMu.stores[id] = &Store{
		storeID:   id,
		storeType: storeType,
		state:     state,
		labels:    labels,
		addr:      addr,
		peerAddr:  peerAddr,
	}
}

// SetPDClient replaces pd client,for testing only
func (c *RegionCache) SetPDClient(client pd.Client) {
	c.pdClient = client
}

// RPCContext contains data that is needed to send RPC to a region.
type RPCContext struct {
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

	contextPatcher contextPatcher // kvrpcpb.Context fields that need to be overridden
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

type contextPatcher struct {
	replicaRead   *bool
	busyThreshold *time.Duration
	staleRead     *bool
}

func (patcher *contextPatcher) applyTo(pbCtx *kvrpcpb.Context) {
	if patcher.replicaRead != nil {
		pbCtx.ReplicaRead = *patcher.replicaRead
	}
	if patcher.staleRead != nil {
		pbCtx.StaleRead = *patcher.staleRead
	}
	if patcher.busyThreshold != nil {
		millis := patcher.busyThreshold.Milliseconds()
		if millis > 0 && millis <= math.MaxUint32 {
			pbCtx.BusyThresholdMs = uint32(millis)
		} else {
			pbCtx.BusyThresholdMs = 0
		}
	}
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
	ts := time.Now().Unix()

	cachedRegion := c.GetCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}

	if cachedRegion.checkNeedReload() {
		return nil, nil
	}

	if !cachedRegion.checkRegionCacheTTL(ts) {
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
		if _, ok := regionStore.pendingTiFlashPeerStores[storeID]; !ok {
			nonPendingStores = append(nonPendingStores, storeID)
		}
	}
	return allStores, nonPendingStores
}

// GetTiFlashRPCContext returns RPCContext for a region must access flash store. If it returns nil, the region
// must be out of date and already dropped from cache or not flash store found.
// `loadBalance` is an option. For batch cop, it is pointless and might cause try the failed store repeatly.
func (c *RegionCache) GetTiFlashRPCContext(bo *retry.Backoffer, id RegionVerID, loadBalance bool, labelFilter LabelFilter) (*RPCContext, error) {
	ts := time.Now().Unix()

	cachedRegion := c.GetCachedRegionWithRLock(id)
	if cachedRegion == nil {
		return nil, nil
	}
	if !cachedRegion.checkRegionCacheTTL(ts) {
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
			_, err := store.reResolve(c)
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
	r = c.searchCachedRegion(key, isEndKey)
	if r == nil {
		// load region when it is not exists or expired.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// no region data, return error if failure.
			return nil, err
		}
		logutil.Eventf(bo.GetCtx(), "load region %d from pd, due to cache-miss", lr.GetID())
		r = lr
		c.mu.Lock()
		c.insertRegionToCache(r, true)
		c.mu.Unlock()
	} else if r.checkNeedReloadAndMarkUpdated() {
		// load region when it be marked as need reload.
		lr, err := c.loadRegion(bo, key, isEndKey)
		if err != nil {
			// ignore error and use old region info.
			logutil.Logger(bo.GetCtx()).Error("load region failure",
				zap.String("key", util.HexRegionKeyStr(key)), zap.Error(err))
		} else {
			logutil.Eventf(bo.GetCtx(), "load region %d from pd, due to need-reload", lr.GetID())
			r = lr
			c.mu.Lock()
			c.insertRegionToCache(r, true)
			c.mu.Unlock()
		}
	}
	return r, nil
}

func (c *RegionCache) tryFindRegionByKey(key []byte, isEndKey bool) (r *Region) {
	r = c.searchCachedRegion(key, isEndKey)
	if r == nil || r.checkNeedReloadAndMarkUpdated() {
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
		r.scheduleReload()
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
	s.markNeedCheck(c.notifyCheckCh)
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
		r.scheduleReload()
	}

}

// LocateRegionByID searches for the region with ID.
func (c *RegionCache) LocateRegionByID(bo *retry.Backoffer, regionID uint64) (*KeyLocation, error) {
	c.mu.RLock()
	r := c.getRegionByIDFromCache(regionID)
	c.mu.RUnlock()
	if r != nil {
		if r.checkNeedReloadAndMarkUpdated() {
			lr, err := c.loadRegionByID(bo, regionID)
			if err != nil {
				// ignore error and use old region info.
				logutil.Logger(bo.GetCtx()).Error("load region failure",
					zap.Uint64("regionID", regionID), zap.Error(err))
			} else {
				r = lr
				c.mu.Lock()
				c.insertRegionToCache(r, true)
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

	r, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.insertRegionToCache(r, true)
	c.mu.Unlock()
	return &KeyLocation{
		Region:   r.VerID(),
		StartKey: r.StartKey(),
		EndKey:   r.EndKey(),
		Buckets:  r.getStore().buckets,
	}, nil
}

func (c *RegionCache) scheduleReloadRegion(region *Region) {
	if region == nil || !region.asyncReload.CompareAndSwap(false, true) {
		// async reload scheduled by other thread.
		return
	}
	regionID := region.GetID()
	if regionID > 0 {
		c.regionsNeedReload.Lock()
		c.regionsNeedReload.regions = append(c.regionsNeedReload.regions, regionID)
		c.regionsNeedReload.Unlock()
	}
}

func (c *RegionCache) reloadRegion(regionID uint64) {
	bo := retry.NewNoopBackoff(context.Background())
	lr, err := c.loadRegionByID(bo, regionID)
	if err != nil {
		// ignore error and use old region info.
		logutil.Logger(bo.GetCtx()).Error("load region failure",
			zap.Uint64("regionID", regionID), zap.Error(err))
		c.mu.RLock()
		if oldRegion := c.getRegionByIDFromCache(regionID); oldRegion != nil {
			oldRegion.asyncReload.Store(false)
		}
		c.mu.RUnlock()
		return
	}
	c.mu.Lock()
	c.insertRegionToCache(lr, false)
	c.mu.Unlock()
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
		err = errors.Errorf("PD returned no region, startKey: %q, endKey: %q", util.HexRegionKeyStr(startKey), util.HexRegionKeyStr(endKey))
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO(youjiali1995): scanRegions always fetch regions from PD and these regions don't contain buckets information
	// for less traffic, so newly inserted regions in region cache don't have buckets information. We should improve it.
	for _, region := range regions {
		c.insertRegionToCache(region, true)
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
func (mu *regionIndexMu) insertRegionToCache(cachedRegion *Region, invalidateOldRegion bool) {
	oldRegion := mu.sorted.ReplaceOrInsert(cachedRegion)
	if oldRegion != nil {
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
		// If the old region is still valid, do not invalidate it to avoid unnecessary backoff.
		if invalidateOldRegion {
			// Invalidate the old region in case it's not invalidated and some requests try with the stale region information.
			oldRegion.invalidate(Other)
		}
		// Don't refresh TiFlash work idx for region. Otherwise, it will always goto a invalid store which
		// is under transferring regions.
		store.workTiFlashIdx.Store(oldRegionStore.workTiFlashIdx.Load())

		// Keep the buckets information if needed.
		if store.buckets == nil || (oldRegionStore.buckets != nil && store.buckets.GetVersion() < oldRegionStore.buckets.GetVersion()) {
			store.buckets = oldRegionStore.buckets
		}
		mu.removeVersionFromCache(oldRegion.VerID(), cachedRegion.VerID().id)
	}
	mu.regions[cachedRegion.VerID()] = cachedRegion
	newVer := cachedRegion.VerID()
	latest, ok := mu.latestVersions[cachedRegion.VerID().id]
	if !ok || latest.GetVer() < newVer.GetVer() || latest.GetConfVer() < newVer.GetConfVer() {
		mu.latestVersions[cachedRegion.VerID().id] = newVer
	}
	// The intersecting regions in the cache are probably stale, clear them.
	deleted := mu.sorted.removeIntersecting(cachedRegion)
	for _, region := range deleted {
		mu.removeVersionFromCache(region.cachedRegion.VerID(), region.cachedRegion.GetID())
	}

} // searchCachedRegion finds a region from cache by key. Like `getCachedRegion`,
// it should be called with c.mu.RLock(), and the returned Region should not be
// used after c.mu is RUnlock().
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
func (c *RegionCache) searchCachedRegion(key []byte, isEndKey bool) *Region {
	ts := time.Now().Unix()
	var r *Region
	c.mu.RLock()
	r = c.mu.sorted.DescendLessOrEqual(key, isEndKey, ts)
	c.mu.RUnlock()
	if r != nil && (!isEndKey && r.Contains(key) || isEndKey && r.ContainsByEnd(key)) {
		return r
	}
	return nil
}

// getRegionByIDFromCache tries to get region by regionID from cache. Like
// `getCachedRegion`, it should be called with c.mu.RLock(), and the returned
// Region should not be used after c.mu is RUnlock().
func (c *RegionCache) getRegionByIDFromCache(regionID uint64) *Region {
	ts := time.Now().Unix()
	ver, ok := c.mu.latestVersions[regionID]
	if !ok {
		return nil
	}
	latestRegion, ok := c.mu.regions[ver]
	if !ok {
		// should not happen
		logutil.BgLogger().Warn("region version not found",
			zap.Uint64("regionID", regionID), zap.Stringer("version", &ver))
		return nil
	}
	lastAccess := atomic.LoadInt64(&latestRegion.lastAccess)
	if ts-lastAccess > regionCacheTTLSec {
		return nil
	}
	if latestRegion != nil {
		atomic.CompareAndSwapInt64(&latestRegion.lastAccess, atomic.LoadInt64(&latestRegion.lastAccess), ts)
	}
	return latestRegion
}

// GetStoresByType gets stores by type `typ`
// TODO: revise it by get store by closure.
func (c *RegionCache) GetStoresByType(typ tikvrpc.EndpointType) []*Store {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()
	stores := make([]*Store, 0)
	for _, store := range c.storeMu.stores {
		if store.getResolveState() != resolved {
			continue
		}
		if store.storeType == typ {
			//TODO: revise it with store.clone()
			storeLabel := make([]*metapb.StoreLabel, 0)
			for _, label := range store.labels {
				storeLabel = append(storeLabel, &metapb.StoreLabel{
					Key:   label.Key,
					Value: label.Value,
				})
			}
			stores = append(stores, &Store{
				addr:      store.addr,
				peerAddr:  store.peerAddr,
				storeID:   store.storeID,
				labels:    storeLabel,
				storeType: typ,
			})
		}
	}
	return stores
}

// GetAllStores gets TiKV and TiFlash stores.
func (c *RegionCache) GetAllStores() []*Store {
	stores := c.GetStoresByType(tikvrpc.TiKV)
	tiflashStores := c.GetStoresByType(tikvrpc.TiFlash)
	return append(stores, tiflashStores...)
}

func filterUnavailablePeers(region *pd.Region) {
	if len(region.DownPeers) == 0 {
		return
	}
	new := region.Meta.Peers[:0]
	for _, p := range region.Meta.Peers {
		available := true
		for _, downPeer := range region.DownPeers {
			if p.Id == downPeer.Id && p.StoreId == downPeer.StoreId {
				available = false
				break
			}
		}
		if available {
			new = append(new, p)
		}
	}
	region.Meta.Peers = new
}

// loadRegion loads region from pd client, and picks the first peer as leader.
// If the given key is the end key of the region that you want, you may set the second argument to true. This is useful
// when processing in reverse order.
func (c *RegionCache) loadRegion(bo *retry.Backoffer, key []byte, isEndKey bool) (*Region, error) {
	ctx := bo.GetCtx()
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("loadRegion", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	var backoffErr error
	searchPrev := false
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
			reg, err = c.pdClient.GetPrevRegion(ctx, key, pd.WithBuckets())
		} else {
			reg, err = c.pdClient.GetRegion(ctx, key, pd.WithBuckets())
		}
		metrics.LoadRegionCacheHistogramWhenCacheMiss.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.RegionCacheCounterWithGetCacheMissError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetCacheMissOK.Inc()
		}
		if err != nil {
			if apicodec.IsDecodeError(err) {
				return nil, errors.Errorf("failed to decode region range key, key: %q, err: %v", util.HexRegionKeyStr(key), err)
			}
			backoffErr = errors.Errorf("loadRegion from PD failed, key: %q, err: %v", util.HexRegionKeyStr(key), err)
			continue
		}
		if reg == nil || reg.Meta == nil {
			backoffErr = errors.Errorf("region not found for key %q", util.HexRegionKeyStr(key))
			continue
		}
		filterUnavailablePeers(reg)
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
		filterUnavailablePeers(reg)
		if len(reg.Meta.Peers) == 0 {
			return nil, errors.New("receive Region with no available peer")
		}
		return newRegion(bo, c, reg)
	}
}

// TODO(youjiali1995): for optimizing BatchLoadRegionsWithKeyRange, not used now.
func (c *RegionCache) scanRegionsFromCache(bo *retry.Backoffer, startKey, endKey []byte, limit int) ([]*Region, error) {
	if limit == 0 {
		return nil, nil
	}

	var regions []*Region
	c.mu.RLock()
	defer c.mu.RUnlock()
	regions = c.mu.sorted.AscendGreaterOrEqual(startKey, endKey, limit)

	if len(regions) == 0 {
		return nil, errors.New("no regions in the cache")
	}
	return regions, nil
}

func (c *RegionCache) timelyRefreshCache(intervalS uint64) {
	if intervalS <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(intervalS) * time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				intervalMs := int(1000 * intervalS)
				if err := c.refreshRegionIndex(retry.NewBackofferWithVars(c.ctx, intervalMs, nil)); err != nil {
					logutil.BgLogger().Error("refresh region cache failed", zap.Error(err))
				}
			}
		}
	}()
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
		regionsInfo, err := c.pdClient.ScanRegions(ctx, startKey, endKey, limit)
		metrics.LoadRegionCacheHistogramWithRegions.Observe(time.Since(start).Seconds())
		if err != nil {
			if apicodec.IsDecodeError(err) {
				return nil, errors.Errorf("failed to decode region range key, startKey: %q, limit: %d, err: %v", util.HexRegionKeyStr(startKey), limit, err)
			}
			metrics.RegionCacheCounterWithScanRegionsError.Inc()
			backoffErr = errors.Errorf(
				"scanRegion from PD failed, startKey: %q, limit: %d, err: %v",
				util.HexRegionKeyStr(startKey),
				limit,
				err)
			continue
		}

		metrics.RegionCacheCounterWithScanRegionsOK.Inc()

		if len(regionsInfo) == 0 {
			return nil, errors.Errorf(
				"PD returned no region, startKey: %q, endKey: %q, limit: %d",
				util.HexRegionKeyStr(startKey), util.HexRegionKeyStr(endKey), limit,
			)
		}
		regions := make([]*Region, 0, len(regionsInfo))
		for _, r := range regionsInfo {
			// Leader id = 0 indicates no leader.
			if r.Leader == nil || r.Leader.GetId() == 0 {
				continue
			}
			region, err := newRegion(bo, c, r)
			if err != nil {
				return nil, err
			}
			regions = append(regions, region)
		}
		if len(regions) == 0 {
			return nil, errors.New("receive Regions with no peer")
		}
		if len(regions) < len(regionsInfo) {
			logutil.Logger(context.Background()).Debug(
				"regionCache: scanRegion finished but some regions has no leader.")
		}
		return regions, nil
	}
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
		addr, err = store.initResolve(bo, c)
		return
	case deleted:
		addr = c.changeToActiveStore(region, store)
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
func (c *RegionCache) changeToActiveStore(region *Region, store *Store) (addr string) {
	c.storeMu.RLock()
	store = c.storeMu.stores[store.storeID]
	c.storeMu.RUnlock()
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

func (c *RegionCache) getStoreByStoreID(storeID uint64) (store *Store) {
	var ok bool
	c.storeMu.Lock()
	store, ok = c.storeMu.stores[storeID]
	if ok {
		c.storeMu.Unlock()
		return
	}
	store = &Store{storeID: storeID}
	c.storeMu.stores[storeID] = store
	c.storeMu.Unlock()
	return
}

func (c *RegionCache) getStoresByLabels(labels []*metapb.StoreLabel) []*Store {
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	s := make([]*Store, 0)
	for _, store := range c.storeMu.stores {
		if store.IsLabelsMatch(labels) {
			s = append(s, store)
		}
	}
	return s
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
		c.insertRegionToCache(region, true)
	}
	c.mu.Unlock()

	return false, nil
}

// PDClient returns the pd.Client in RegionCache.
func (c *RegionCache) PDClient() pd.Client {
	return c.pdClient
}

// GetTiFlashStores returns the information of all tiflash nodes.
func (c *RegionCache) GetTiFlashStores(labelFilter LabelFilter) []*Store {
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	var stores []*Store
	for _, s := range c.storeMu.stores {
		if s.storeType == tikvrpc.TiFlash {
			if !labelFilter(s.labels) {
				continue
			}
			stores = append(stores, s)
		}
	}
	return stores
}

// GetTiFlashComputeStores returns all stores with lable <engine, tiflash_compute>.
func (c *RegionCache) GetTiFlashComputeStores(bo *retry.Backoffer) (res []*Store, err error) {
	c.tiflashComputeStoreMu.RLock()
	needReload := c.tiflashComputeStoreMu.needReload
	stores := c.tiflashComputeStoreMu.stores
	c.tiflashComputeStoreMu.RUnlock()

	if needReload {
		return c.reloadTiFlashComputeStores(bo)
	}
	return stores, nil
}

func (c *RegionCache) reloadTiFlashComputeStores(bo *retry.Backoffer) (res []*Store, _ error) {
	stores, err := c.pdClient.GetAllStores(bo.GetCtx())
	if err != nil {
		return nil, err
	}
	for _, s := range stores {
		if s.GetState() == metapb.StoreState_Up && isStoreContainLabel(s.GetLabels(), tikvrpc.EngineLabelKey, tikvrpc.EngineLabelTiFlashCompute) {
			res = append(res, &Store{
				storeID:   s.GetId(),
				addr:      s.GetAddress(),
				peerAddr:  s.GetPeerAddress(),
				saddr:     s.GetStatusAddress(),
				storeType: tikvrpc.GetStoreTypeByMeta(s),
				labels:    s.GetLabels(),
				state:     uint64(resolved),
			})
		}
	}

	c.tiflashComputeStoreMu.Lock()
	c.tiflashComputeStoreMu.stores = res
	c.tiflashComputeStoreMu.Unlock()
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
	c.tiflashComputeStoreMu.Lock()
	defer c.tiflashComputeStoreMu.Unlock()
	c.tiflashComputeStoreMu.needReload = true
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
			new, err := c.loadRegionByID(bo, regionID.id)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("failed to update buckets",
					zap.String("region", regionID.String()), zap.Uint64("bucketsVer", bucketsVer),
					zap.Uint64("latestBucketsVer", latestBucketsVer), zap.Error(err))
				return
			}
			c.mu.Lock()
			c.insertRegionToCache(new, true)
			c.mu.Unlock()
		}()
	}
}

const cleanCacheInterval = time.Second
const cleanRegionNumPerRound = 50

// This function is expected to run in a background goroutine.
// It keeps iterating over the whole region cache, searching for stale region
// info. It runs at cleanCacheInterval and checks only cleanRegionNumPerRound
// regions. In this way, the impact of this background goroutine should be
// negligible.
func (c *RegionCache) cacheGC() {
	ticker := time.NewTicker(cleanCacheInterval)
	defer ticker.Stop()

	beginning := newBtreeSearchItem([]byte(""))
	iterItem := beginning
	expired := make([]*btreeItem, cleanRegionNumPerRound)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			count := 0
			expired = expired[:0]

			// Only RLock when checking TTL to avoid blocking other readers
			c.mu.RLock()
			ts := time.Now().Unix()
			c.mu.sorted.b.AscendGreaterOrEqual(iterItem, func(item *btreeItem) bool {
				if count > cleanRegionNumPerRound {
					iterItem = item
					return false
				}
				count++
				if item.cachedRegion.isCacheTTLExpired(ts) {
					expired = append(expired, item)
				}
				return true
			})
			c.mu.RUnlock()

			// Reach the end of the region cache, start from the beginning
			if count <= cleanRegionNumPerRound {
				iterItem = beginning
			}

			if len(expired) > 0 {
				c.mu.Lock()
				for _, item := range expired {
					c.mu.sorted.b.Delete(item)
					c.mu.removeVersionFromCache(item.cachedRegion.VerID(), item.cachedRegion.GetID())
				}
				c.mu.Unlock()
			}
		}
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

func (item *btreeItem) Less(other btree.Item) bool {
	return bytes.Compare(item.key, other.(*btreeItem).key) < 0
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
	return bytes.Compare(r.meta.GetStartKey(), key) < 0 &&
		(bytes.Compare(key, r.meta.GetEndKey()) <= 0 || len(r.meta.GetEndKey()) == 0)
}

// Store contains a kv process's address.
type Store struct {
	addr         string               // loaded store address
	peerAddr     string               // TiFlash Proxy use peerAddr
	saddr        string               // loaded store status address
	storeID      uint64               // store's id
	state        uint64               // unsafe store storeState
	labels       []*metapb.StoreLabel // stored store labels
	resolveMutex sync.Mutex           // protect pd from concurrent init requests
	epoch        uint32               // store fail epoch, see RegionStore.storeEpochs
	storeType    tikvrpc.EndpointType // type of the store
	tokenCount   atomic2.Int64        // used store token count

	loadStats atomic2.Pointer[storeLoadStats]

	// whether the store is unreachable due to some reason, therefore requests to the store needs to be
	// forwarded by other stores. this is also the flag that a checkUntilHealth goroutine is running for this store.
	// this mechanism is currently only applicable for TiKV stores.
	livenessState    uint32
	unreachableSince time.Time

	// A statistic for counting the request latency to this store
	slowScore SlowScoreStat
	// A statistic for counting the flows of different replicas on this store
	replicaFlowsStats [numReplicaFlowsType]uint64
}

type resolveState uint64

type storeLoadStats struct {
	estimatedWait     time.Duration
	waitTimeUpdatedAt time.Time
}

const (
	// The store is just created and normally is being resolved.
	// Store in this state will only be resolved by initResolve().
	unresolved resolveState = iota
	// The store is resolved and its address is valid.
	resolved
	// Request failed on this store and it will be re-resolved by asyncCheckAndResolveLoop().
	needCheck
	// The store's address or label is changed and marked deleted.
	// There is a new store struct replaced it in the RegionCache and should
	// call changeToActiveStore() to get the new struct.
	deleted
	// The store is a tombstone. Should invalidate the region if tries to access it.
	tombstone
)

// String implements fmt.Stringer interface.
func (s resolveState) String() string {
	switch s {
	case unresolved:
		return "unresolved"
	case resolved:
		return "resolved"
	case needCheck:
		return "needCheck"
	case deleted:
		return "deleted"
	case tombstone:
		return "tombstone"
	default:
		return fmt.Sprintf("unknown-%v", uint64(s))
	}
}

// IsTiFlash returns true if the storeType is TiFlash
func (s *Store) IsTiFlash() bool {
	return s.storeType == tikvrpc.TiFlash
}

// StoreID returns storeID.
func (s *Store) StoreID() uint64 {
	return s.storeID
}

// initResolve resolves the address of the store that never resolved and returns an
// empty string if it's a tombstone.
func (s *Store) initResolve(bo *retry.Backoffer, c *RegionCache) (addr string, err error) {
	s.resolveMutex.Lock()
	state := s.getResolveState()
	defer s.resolveMutex.Unlock()
	if state != unresolved {
		if state != tombstone {
			addr = s.addr
		}
		return
	}
	var store *metapb.Store
	for {
		start := time.Now()
		store, err = c.pdClient.GetStore(bo.GetCtx(), s.storeID)
		metrics.LoadRegionCacheHistogramWithGetStore.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.RegionCacheCounterWithGetStoreError.Inc()
		} else {
			metrics.RegionCacheCounterWithGetStoreOK.Inc()
		}
		if err := bo.GetCtx().Err(); err != nil && errors.Cause(err) == context.Canceled {
			return "", errors.WithStack(err)
		}
		if err != nil && !isStoreNotFoundError(err) {
			// TODO: more refine PD error status handle.
			err = errors.Errorf("loadStore from PD failed, id: %d, err: %v", s.storeID, err)
			if err = bo.Backoff(retry.BoPDRPC, err); err != nil {
				return
			}
			continue
		}
		// The store is a tombstone.
		if store == nil || (store != nil && store.GetState() == metapb.StoreState_Tombstone) {
			s.setResolveState(tombstone)
			return "", nil
		}
		addr = store.GetAddress()
		if addr == "" {
			return "", errors.Errorf("empty store(%d) address", s.storeID)
		}
		s.addr = addr
		s.peerAddr = store.GetPeerAddress()
		s.saddr = store.GetStatusAddress()
		s.storeType = tikvrpc.GetStoreTypeByMeta(store)
		s.labels = store.GetLabels()
		// Shouldn't have other one changing its state concurrently, but we still use changeResolveStateTo for safety.
		s.changeResolveStateTo(unresolved, resolved)
		return s.addr, nil
	}
}

// A quick and dirty solution to find out whether an err is caused by StoreNotFound.
// todo: A better solution, maybe some err-code based error handling?
func isStoreNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "invalid store ID") && strings.Contains(err.Error(), "not found")
}

// reResolve try to resolve addr for store that need check. Returns false if the region is in tombstone state or is
// deleted.
func (s *Store) reResolve(c *RegionCache) (bool, error) {
	var addr string
	store, err := c.pdClient.GetStore(context.Background(), s.storeID)
	if err != nil {
		metrics.RegionCacheCounterWithGetStoreError.Inc()
	} else {
		metrics.RegionCacheCounterWithGetStoreOK.Inc()
	}
	// `err` here can mean either "load Store from PD failed" or "store not found"
	// If load Store from PD is successful but PD didn't find the store
	// the err should be handled by next `if` instead of here
	if err != nil && !isStoreNotFoundError(err) {
		logutil.BgLogger().Error("loadStore from PD failed", zap.Uint64("id", s.storeID), zap.Error(err))
		// we cannot do backoff in reResolve loop but try check other store and wait tick.
		return false, err
	}
	if store == nil || (store != nil && store.GetState() == metapb.StoreState_Tombstone) {
		// store has be removed in PD, we should invalidate all regions using those store.
		logutil.BgLogger().Info("invalidate regions in removed store",
			zap.Uint64("store", s.storeID), zap.String("add", s.addr))
		atomic.AddUint32(&s.epoch, 1)
		s.setResolveState(tombstone)
		metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
		return false, nil
	}

	storeType := tikvrpc.GetStoreTypeByMeta(store)
	addr = store.GetAddress()
	if s.addr != addr || !s.IsSameLabels(store.GetLabels()) {
		newStore := &Store{storeID: s.storeID, addr: addr, peerAddr: store.GetPeerAddress(), saddr: store.GetStatusAddress(), storeType: storeType, labels: store.GetLabels(), state: uint64(resolved)}
		c.storeMu.Lock()
		if s.addr == addr {
			newStore.slowScore = s.slowScore
		}
		c.storeMu.stores[newStore.storeID] = newStore
		c.storeMu.Unlock()
		s.setResolveState(deleted)
		return false, nil
	}
	s.changeResolveStateTo(needCheck, resolved)
	return true, nil
}

func (s *Store) getResolveState() resolveState {
	if s == nil {
		var state resolveState
		return state
	}
	return resolveState(atomic.LoadUint64(&s.state))
}

func (s *Store) setResolveState(state resolveState) {
	atomic.StoreUint64(&s.state, uint64(state))
}

// changeResolveStateTo changes the store resolveState from the old state to the new state.
// Returns true if it changes the state successfully, and false if the store's state
// is changed by another one.
func (s *Store) changeResolveStateTo(from, to resolveState) bool {
	for {
		state := s.getResolveState()
		if state == to {
			return true
		}
		if state != from {
			return false
		}
		if atomic.CompareAndSwapUint64(&s.state, uint64(from), uint64(to)) {
			logutil.BgLogger().Info("change store resolve state",
				zap.Uint64("store", s.storeID),
				zap.String("addr", s.addr),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
				zap.String("liveness-state", s.getLivenessState().String()))
			return true
		}
	}
}

// markNeedCheck marks resolved store to be async resolve to check store addr change.
func (s *Store) markNeedCheck(notifyCheckCh chan struct{}) {
	if s.changeResolveStateTo(resolved, needCheck) {
		select {
		case notifyCheckCh <- struct{}{}:
		default:
		}
	}
}

// IsSameLabels returns whether the store have the same labels with target labels
func (s *Store) IsSameLabels(labels []*metapb.StoreLabel) bool {
	if len(s.labels) != len(labels) {
		return false
	}
	return s.IsLabelsMatch(labels)
}

// IsLabelsMatch return whether the store's labels match the target labels
func (s *Store) IsLabelsMatch(labels []*metapb.StoreLabel) bool {
	if len(labels) < 1 {
		return true
	}
	for _, targetLabel := range labels {
		if !isStoreContainLabel(s.labels, targetLabel.Key, targetLabel.Value) {
			return false
		}
	}
	return true
}

// IsStoreMatch returns whether the store's id match the target ids.
func (s *Store) IsStoreMatch(stores []uint64) bool {
	if len(stores) < 1 {
		return true
	}
	for _, storeID := range stores {
		if s.storeID == storeID {
			return true
		}
	}
	return false
}

func isStoreContainLabel(labels []*metapb.StoreLabel, key string, val string) (res bool) {
	for _, label := range labels {
		if label.GetKey() == key && label.GetValue() == val {
			res = true
			break
		}
	}
	return res
}

// GetLabelValue returns the value of the label
func (s *Store) GetLabelValue(key string) (string, bool) {
	for _, label := range s.labels {
		if label.Key == key {
			return label.Value, true
		}
	}
	return "", false
}

// getLivenessState gets the cached liveness state of the store.
// When it's not reachable, a goroutine will update the state in background.
// To get the accurate liveness state, use checkLiveness instead.
func (s *Store) getLivenessState() livenessState {
	return livenessState(atomic.LoadUint32(&s.livenessState))
}

type livenessState uint32

var (
	livenessSf singleflight.Group
	// storeLivenessTimeout is the max duration of resolving liveness of a TiKV instance.
	storeLivenessTimeout = time.Second
)

// SetStoreLivenessTimeout sets storeLivenessTimeout to t.
func SetStoreLivenessTimeout(t time.Duration) {
	storeLivenessTimeout = t
}

// GetStoreLivenessTimeout returns storeLivenessTimeout.
func GetStoreLivenessTimeout() time.Duration {
	return storeLivenessTimeout
}

const (
	reachable livenessState = iota
	unreachable
	unknown
)

// String implements fmt.Stringer interface.
func (s livenessState) String() string {
	switch s {
	case unreachable:
		return "unreachable"
	case reachable:
		return "reachable"
	case unknown:
		return "unknown"
	default:
		return fmt.Sprintf("unknown-%v", uint32(s))
	}
}

func (s *Store) startHealthCheckLoopIfNeeded(c *RegionCache, liveness livenessState) {
	// This mechanism doesn't support non-TiKV stores currently.
	if s.storeType != tikvrpc.TiKV {
		logutil.BgLogger().Info("[health check] skip running health check loop for non-tikv store",
			zap.Uint64("storeID", s.storeID), zap.String("addr", s.addr))
		return
	}

	// It may be already started by another thread.
	if atomic.CompareAndSwapUint32(&s.livenessState, uint32(reachable), uint32(liveness)) {
		s.unreachableSince = time.Now()
		go s.checkUntilHealth(c)
	}
}

func (s *Store) checkUntilHealth(c *RegionCache) {
	defer atomic.StoreUint32(&s.livenessState, uint32(reachable))

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastCheckPDTime := time.Now()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if time.Since(lastCheckPDTime) > time.Second*30 {
				lastCheckPDTime = time.Now()

				valid, err := s.reResolve(c)
				if err != nil {
					logutil.BgLogger().Warn("[health check] failed to re-resolve unhealthy store", zap.Error(err))
				} else if !valid {
					logutil.BgLogger().Info("[health check] store meta deleted, stop checking", zap.Uint64("storeID", s.storeID), zap.String("addr", s.addr))
					return
				}
			}

			bo := retry.NewNoopBackoff(c.ctx)
			l := s.requestLiveness(bo, c)
			if l == reachable {
				logutil.BgLogger().Info("[health check] store became reachable", zap.Uint64("storeID", s.storeID))

				return
			}
			atomic.StoreUint32(&s.livenessState, uint32(l))
		}
	}
}

func (s *Store) requestLiveness(bo *retry.Backoffer, c *RegionCache) (l livenessState) {
	// It's not convenient to mock liveness in integration tests. Use failpoint to achieve that instead.
	if val, err := util.EvalFailpoint("injectLiveness"); err == nil {
		switch val.(string) {
		case "unreachable":
			return unreachable
		case "reachable":
			return reachable
		case "unknown":
			return unknown
		}
	}

	if c != nil {
		livenessFunc := c.testingKnobs.mockRequestLiveness.Load()
		if livenessFunc != nil {
			return (*livenessFunc)(s, bo)
		}
	}

	if storeLivenessTimeout == 0 {
		return unreachable
	}

	if s.getResolveState() != resolved {
		l = unknown
		return
	}
	addr := s.addr
	rsCh := livenessSf.DoChan(addr, func() (interface{}, error) {
		return invokeKVStatusAPI(addr, storeLivenessTimeout), nil
	})
	var ctx context.Context
	if bo != nil {
		ctx = bo.GetCtx()
	} else {
		ctx = context.Background()
	}
	select {
	case rs := <-rsCh:
		l = rs.Val.(livenessState)
	case <-ctx.Done():
		l = unknown
		return
	}
	return
}

// GetAddr returns the address of the store
func (s *Store) GetAddr() string {
	return s.addr
}

// GetPeerAddr returns the peer address of the store
func (s *Store) GetPeerAddr() string {
	return s.peerAddr
}

// EstimatedWaitTime returns an optimistic estimation of how long a request will wait in the store.
// It's calculated by subtracting the time since the last update from the wait time returned from TiKV.
func (s *Store) EstimatedWaitTime() time.Duration {
	loadStats := s.loadStats.Load()
	if loadStats == nil {
		return 0
	}
	timeSinceUpdated := time.Since(loadStats.waitTimeUpdatedAt)
	if loadStats.estimatedWait < timeSinceUpdated {
		return 0
	}
	return loadStats.estimatedWait - timeSinceUpdated
}

func invokeKVStatusAPI(addr string, timeout time.Duration) (l livenessState) {
	start := time.Now()
	defer func() {
		if l == reachable {
			metrics.StatusCountWithOK.Inc()
		} else {
			metrics.StatusCountWithError.Inc()
		}
		metrics.TiKVStatusDuration.WithLabelValues(addr).Observe(time.Since(start).Seconds())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, cli, err := createKVHealthClient(ctx, addr)
	if err != nil {
		logutil.BgLogger().Info("[health check] create grpc connection failed", zap.String("store", addr), zap.Error(err))
		l = unreachable
		return
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			logutil.BgLogger().Info("[health check] failed to close the grpc connection for health check", zap.String("store", addr), zap.Error(err))
		}
	}()

	req := &healthpb.HealthCheckRequest{}
	resp, err := cli.Check(ctx, req)
	if err != nil {
		logutil.BgLogger().Info("[health check] check health error", zap.String("store", addr), zap.Error(err))
		l = unreachable
		return
	}

	status := resp.GetStatus()
	if status == healthpb.HealthCheckResponse_UNKNOWN || status == healthpb.HealthCheckResponse_SERVICE_UNKNOWN {
		logutil.BgLogger().Info("[health check] check health returns unknown", zap.String("store", addr))
		l = unknown
		return
	}

	if status != healthpb.HealthCheckResponse_SERVING {
		logutil.BgLogger().Info("[health check] service not serving", zap.Stringer("status", status))
		l = unreachable
		return
	}

	l = reachable
	return
}

// getSlowScore returns the slow score of store.
func (s *Store) getSlowScore() uint64 {
	return s.slowScore.getSlowScore()
}

// isSlow returns whether current Store is slow or not.
func (s *Store) isSlow() bool {
	return s.slowScore.isSlow()
}

// updateSlowScore updates the slow score of this store according to the timecost of current request.
func (s *Store) updateSlowScoreStat() {
	s.slowScore.updateSlowScore()
}

// recordSlowScoreStat records timecost of each request.
func (s *Store) recordSlowScoreStat(timecost time.Duration) {
	s.slowScore.recordSlowScoreStat(timecost)
}

// markAlreadySlow marks the related store already slow.
func (s *Store) markAlreadySlow() {
	s.slowScore.markAlreadySlow()
}

// asyncUpdateStoreSlowScore updates the slow score of each store periodically.
func (c *RegionCache) asyncUpdateStoreSlowScore(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// update store slowScores
			c.checkAndUpdateStoreSlowScores()
		}
	}
}

// checkAndUpdateStoreSlowScores checks and updates slowScore on each store.
func (c *RegionCache) checkAndUpdateStoreSlowScores() {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Error("panic in the checkAndUpdateStoreSlowScores goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
	}()
	slowScoreMetrics := make(map[string]float64)
	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		store.updateSlowScoreStat()
		slowScoreMetrics[store.addr] = float64(store.getSlowScore())
	}
	c.storeMu.RUnlock()
	for store, score := range slowScoreMetrics {
		metrics.TiKVStoreSlowScoreGauge.WithLabelValues(store).Set(score)
	}
}

// getReplicaFlowsStats returns the statistics on the related replicaFlowsType.
func (s *Store) getReplicaFlowsStats(destType replicaFlowsType) uint64 {
	return atomic.LoadUint64(&s.replicaFlowsStats[destType])
}

// resetReplicaFlowsStats resets the statistics on the related replicaFlowsType.
func (s *Store) resetReplicaFlowsStats(destType replicaFlowsType) {
	atomic.StoreUint64(&s.replicaFlowsStats[destType], 0)
}

// recordReplicaFlowsStats records the statistics on the related replicaFlowsType.
func (s *Store) recordReplicaFlowsStats(destType replicaFlowsType) {
	atomic.AddUint64(&s.replicaFlowsStats[destType], 1)
}

// asyncReportStoreReplicaFlows reports the statistics on the related replicaFlowsType.
func (c *RegionCache) asyncReportStoreReplicaFlows(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.storeMu.RLock()
			for _, store := range c.storeMu.stores {
				for destType := toLeader; destType < numReplicaFlowsType; destType++ {
					metrics.TiKVPreferLeaderFlowsGauge.WithLabelValues(destType.String(), store.addr).Set(float64(store.getReplicaFlowsStats(destType)))
					store.resetReplicaFlowsStats(destType)
				}
			}
			c.storeMu.RUnlock()
		}
	}
}

func createKVHealthClient(ctx context.Context, addr string) (*grpc.ClientConn, healthpb.HealthClient, error) {
	// Temporarily directly load the config from the global config, however it's not a good idea to let RegionCache to
	// access it.
	// TODO: Pass the config in a better way, or use the connArray inner the client directly rather than creating new
	// connection.

	cfg := config.GetGlobalConfig()

	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if len(cfg.Security.ClusterSSLCA) != 0 {
		tlsConfig, err := cfg.Security.ToTLSConfig()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}
	keepAlive := cfg.TiKVClient.GrpcKeepAliveTime
	keepAliveTimeout := cfg.TiKVClient.GrpcKeepAliveTimeout
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithInitialWindowSize(client.GrpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(client.GrpcInitialConnWindowSize),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond, // Default was 1s.
				Multiplier: 1.6,                    // Default
				Jitter:     0.2,                    // Default
				MaxDelay:   3 * time.Second,        // Default was 120s.
			},
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(keepAlive) * time.Second,
			Timeout: time.Duration(keepAliveTimeout) * time.Second,
		}),
	)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	cli := healthpb.NewHealthClient(conn)
	return conn, cli, nil
}

func isSamePeer(lhs *metapb.Peer, rhs *metapb.Peer) bool {
	return lhs == rhs || (lhs.GetId() == rhs.GetId() && lhs.GetStoreId() == rhs.GetStoreId())
}

// contains returns true if startKey <= key < endKey. Empty endKey is the maximum key.
func contains(startKey, endKey, key []byte) bool {
	return bytes.Compare(startKey, key) <= 0 &&
		(bytes.Compare(key, endKey) < 0 || len(endKey) == 0)
}
