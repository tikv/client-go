// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locate

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

type testingKnobs interface {
	getMockRequestLiveness() livenessFunc
	setMockRequestLiveness(f livenessFunc)
}

type storeRegistry interface {
	fetchStore(ctx context.Context, id uint64) (*metapb.Store, error)
	fetchAllStores(ctx context.Context) ([]*metapb.Store, error)
}

type storeCache interface {
	testingKnobs
	storeRegistry
	get(id uint64) (store *Store, exists bool)
	getOrInsertDefault(id uint64) *Store
	put(store *Store)
	clear()
	forEach(f func(*Store))
	filter(dst []*Store, predicate func(*Store) bool) []*Store
	listTiflashComputeStores() (stores []*Store, needReload bool)
	setTiflashComputeStores(stores []*Store)
	markTiflashComputeStoresNeedReload()
	markStoreNeedCheck(store *Store)
	getCheckStoreEvents() <-chan struct{}
}

func newStoreCache(pdClient pd.Client) *storeCacheImpl {
	c := &storeCacheImpl{pdClient: pdClient}
	c.notifyCheckCh = make(chan struct{}, 1)
	c.storeMu.stores = make(map[uint64]*Store)
	c.tiflashComputeStoreMu.needReload = true
	c.tiflashComputeStoreMu.stores = make([]*Store, 0)
	return c
}

type storeCacheImpl struct {
	pdClient pd.Client

	testingKnobs struct {
		// Replace the requestLiveness function for test purpose. Note that in unit tests, if this is not set,
		// requestLiveness always returns unreachable.
		mockRequestLiveness atomic.Pointer[livenessFunc]
	}

	notifyCheckCh chan struct{}
	storeMu       struct {
		sync.RWMutex
		stores map[uint64]*Store
	}

	tiflashComputeStoreMu struct {
		sync.RWMutex
		needReload bool
		stores     []*Store
	}
}

func (c *storeCacheImpl) getMockRequestLiveness() livenessFunc {
	f := c.testingKnobs.mockRequestLiveness.Load()
	if f == nil {
		return nil
	}
	return *f
}

func (c *storeCacheImpl) setMockRequestLiveness(f livenessFunc) {
	c.testingKnobs.mockRequestLiveness.Store(&f)
}

func (c *storeCacheImpl) fetchStore(ctx context.Context, id uint64) (*metapb.Store, error) {
	return c.pdClient.GetStore(ctx, id)
}

func (c *storeCacheImpl) fetchAllStores(ctx context.Context) ([]*metapb.Store, error) {
	return c.pdClient.GetAllStores(ctx)
}

func (c *storeCacheImpl) get(id uint64) (store *Store, exists bool) {
	c.storeMu.RLock()
	store, exists = c.storeMu.stores[id]
	c.storeMu.RUnlock()
	return
}

func (c *storeCacheImpl) getOrInsertDefault(id uint64) *Store {
	c.storeMu.Lock()
	store, exists := c.storeMu.stores[id]
	if !exists {
		store = newUninitializedStore(id)
		c.storeMu.stores[id] = store
	}
	c.storeMu.Unlock()
	return store
}

func (c *storeCacheImpl) put(store *Store) {
	c.storeMu.Lock()
	c.storeMu.stores[store.storeID] = store
	c.storeMu.Unlock()
}

func (c *storeCacheImpl) clear() {
	c.storeMu.Lock()
	c.storeMu.stores = make(map[uint64]*Store)
	c.storeMu.Unlock()
}

func (c *storeCacheImpl) forEach(f func(*Store)) {
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	for _, s := range c.storeMu.stores {
		f(s)
	}
}

func (c *storeCacheImpl) filter(dst []*Store, predicate func(*Store) bool) []*Store {
	c.storeMu.RLock()
	for _, store := range c.storeMu.stores {
		if predicate == nil || predicate(store) {
			dst = append(dst, store)
		}
	}
	c.storeMu.RUnlock()
	return dst
}

func (c *storeCacheImpl) listTiflashComputeStores() (stores []*Store, needReload bool) {
	c.tiflashComputeStoreMu.RLock()
	needReload = c.tiflashComputeStoreMu.needReload
	stores = c.tiflashComputeStoreMu.stores
	c.tiflashComputeStoreMu.RUnlock()
	return
}

func (c *storeCacheImpl) setTiflashComputeStores(stores []*Store) {
	c.tiflashComputeStoreMu.Lock()
	c.tiflashComputeStoreMu.stores = stores
	c.tiflashComputeStoreMu.needReload = false
	c.tiflashComputeStoreMu.Unlock()
}

func (c *storeCacheImpl) markTiflashComputeStoresNeedReload() {
	c.tiflashComputeStoreMu.Lock()
	c.tiflashComputeStoreMu.needReload = true
	c.tiflashComputeStoreMu.Unlock()
}

func (c *storeCacheImpl) markStoreNeedCheck(store *Store) {
	if store.changeResolveStateTo(resolved, needCheck) {
		select {
		case c.notifyCheckCh <- struct{}{}:
		default:
		}
	}
}

func (c *storeCacheImpl) getCheckStoreEvents() <-chan struct{} {
	return c.notifyCheckCh
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
	tokenCount   atomic.Int64         // used store token count

	loadStats atomic.Pointer[storeLoadStats]

	// whether the store is unreachable due to some reason, therefore requests to the store needs to be
	// forwarded by other stores. this is also the flag that a health check loop is running for this store.
	// this mechanism is currently only applicable for TiKV stores.
	livenessState    uint32
	unreachableSince time.Time

	healthStatus *StoreHealthStatus
	// A statistic for counting the flows of different replicas on this store
	replicaFlowsStats [numReplicaFlowsType]uint64
}

func newStore(
	id uint64,
	addr string,
	peerAddr string,
	statusAddr string,
	storeType tikvrpc.EndpointType,
	state resolveState,
	labels []*metapb.StoreLabel,
) *Store {
	return &Store{
		storeID:   id,
		storeType: storeType,
		state:     uint64(state),
		labels:    labels,
		addr:      addr,
		peerAddr:  peerAddr,
		saddr:     statusAddr,
		// Make sure healthStatus field is never null.
		healthStatus: newStoreHealthStatus(id),
	}
}

// newUninitializedStore creates a `Store` instance with only storeID initialized.
func newUninitializedStore(id uint64) *Store {
	return &Store{
		storeID: id,
		// Make sure healthStatus field is never null.
		healthStatus: newStoreHealthStatus(id),
	}
}

// StoreType returns the type of the store.
func (s *Store) StoreType() tikvrpc.EndpointType {
	return s.storeType
}

// IsTiFlash returns true if the storeType is TiFlash
func (s *Store) IsTiFlash() bool {
	return s.storeType == tikvrpc.TiFlash
}

// StoreID returns storeID.
func (s *Store) StoreID() uint64 {
	return s.storeID
}

// GetAddr returns the address of the store
func (s *Store) GetAddr() string {
	return s.addr
}

// GetPeerAddr returns the peer address of the store
func (s *Store) GetPeerAddr() string {
	return s.peerAddr
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

// GetLabelValue returns the value of the label
func (s *Store) GetLabelValue(key string) (string, bool) {
	for _, label := range s.labels {
		if label.Key == key {
			return label.Value, true
		}
	}
	return "", false
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

// GetHealthStatus returns the health status of the store. This is exported for test purpose.
func (s *Store) GetHealthStatus() *StoreHealthStatus {
	return s.healthStatus
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

type resolveState uint64

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

// initResolve resolves the address of the store that never resolved and returns an
// empty string if it's a tombstone.
func (s *Store) initResolve(bo *retry.Backoffer, c storeCache) (addr string, err error) {
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
		store, err = c.fetchStore(bo.GetCtx(), s.storeID)
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
		if store == nil || store.GetState() == metapb.StoreState_Tombstone {
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

// reResolve try to resolve addr for store that need check. Returns false if the region is in tombstone state or is
// deleted.
func (s *Store) reResolve(c storeCache) (bool, error) {
	var addr string
	store, err := c.fetchStore(context.Background(), s.storeID)
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
	if store == nil || store.GetState() == metapb.StoreState_Tombstone {
		// store has be removed in PD, we should invalidate all regions using those store.
		logutil.BgLogger().Info("invalidate regions in removed store",
			zap.Uint64("store", s.storeID), zap.String("addr", s.addr))
		atomic.AddUint32(&s.epoch, 1)
		s.setResolveState(tombstone)
		metrics.RegionCacheCounterWithInvalidateStoreRegionsOK.Inc()
		return false, nil
	}

	storeType := tikvrpc.GetStoreTypeByMeta(store)
	addr = store.GetAddress()
	if s.addr != addr || !s.IsSameLabels(store.GetLabels()) {
		newStore := newStore(
			s.storeID,
			addr,
			store.GetPeerAddress(),
			store.GetStatusAddress(),
			storeType,
			resolved,
			store.GetLabels(),
		)
		newStore.livenessState = atomic.LoadUint32(&s.livenessState)
		newStore.unreachableSince = s.unreachableSince
		if s.addr == addr {
			newStore.healthStatus = s.healthStatus
		}
		c.put(newStore)
		s.setResolveState(deleted)
		return false, nil
	}
	s.changeResolveStateTo(needCheck, resolved)
	return true, nil
}

// A quick and dirty solution to find out whether an err is caused by StoreNotFound.
// todo: A better solution, maybe some err-code based error handling?
func isStoreNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "invalid store ID") && strings.Contains(err.Error(), "not found")
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

type livenessFunc func(ctx context.Context, s *Store) livenessState

type livenessState uint32

func (l livenessState) injectConstantLiveness(tk testingKnobs) {
	tk.setMockRequestLiveness(func(ctx context.Context, s *Store) livenessState { return l })
}

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

// getLivenessState gets the cached liveness state of the store.
// When it's not reachable, a goroutine will update the state in background.
// To get the accurate liveness state, use checkLiveness instead.
func (s *Store) getLivenessState() livenessState {
	return livenessState(atomic.LoadUint32(&s.livenessState))
}

func (s *Store) requestLivenessAndStartHealthCheckLoopIfNeeded(bo *retry.Backoffer, scheduler *bgRunner, c storeCache) (liveness livenessState) {
	liveness = requestLiveness(bo.GetCtx(), s, c)
	if liveness == reachable {
		return
	}
	// This mechanism doesn't support non-TiKV stores currently.
	if s.storeType != tikvrpc.TiKV {
		logutil.BgLogger().Info("[health check] skip running health check loop for non-tikv store",
			zap.Uint64("storeID", s.storeID), zap.String("addr", s.addr))
		return
	}

	// It may be already started by another thread.
	if atomic.CompareAndSwapUint32(&s.livenessState, uint32(reachable), uint32(liveness)) {
		s.unreachableSince = time.Now()
		reResolveInterval := 30 * time.Second
		if val, err := util.EvalFailpoint("injectReResolveInterval"); err == nil {
			if dur, err := time.ParseDuration(val.(string)); err == nil {
				reResolveInterval = dur
			}
		}
		if _, err := util.EvalFailpoint("skipStoreCheckUntilHealth"); err == nil {
			return
		}
		startHealthCheckLoop(scheduler, c, s, liveness, reResolveInterval)
	}
	return
}

func startHealthCheckLoop(scheduler *bgRunner, c storeCache, s *Store, liveness livenessState, reResolveInterval time.Duration) {
	lastCheckPDTime := time.Now()

	scheduler.schedule(func(ctx context.Context, t time.Time) bool {
		if t.Sub(lastCheckPDTime) > reResolveInterval {
			lastCheckPDTime = t

			valid, err := s.reResolve(c)
			if err != nil {
				logutil.BgLogger().Warn("[health check] failed to re-resolve unhealthy store", zap.Error(err))
			} else if !valid {
				if s.getResolveState() != deleted {
					logutil.BgLogger().Warn("[health check] store was still unhealthy at the end of health check loop",
						zap.Uint64("storeID", s.storeID),
						zap.String("state", s.getResolveState().String()),
						zap.String("liveness", s.getLivenessState().String()))
					return true
				}
				// if the store is deleted, a new store with same id must be inserted (guaranteed by reResolve).
				newStore, _ := c.get(s.storeID)
				logutil.BgLogger().Info("[health check] store meta changed",
					zap.Uint64("storeID", s.storeID),
					zap.String("oldAddr", s.addr),
					zap.String("oldLabels", fmt.Sprintf("%v", s.labels)),
					zap.String("newAddr", newStore.addr),
					zap.String("newLabels", fmt.Sprintf("%v", newStore.labels)))
				s = newStore
			}
		}

		liveness = requestLiveness(ctx, s, c)
		atomic.StoreUint32(&s.livenessState, uint32(liveness))
		if liveness == reachable {
			logutil.BgLogger().Info("[health check] store became reachable", zap.Uint64("storeID", s.storeID))
			return true
		}
		return false
	}, time.Second)
}

func requestLiveness(ctx context.Context, s *Store, tk testingKnobs) (l livenessState) {
	// It's not convenient to mock liveness in integration tests. Use failpoint to achieve that instead.
	if val, err := util.EvalFailpoint("injectLiveness"); err == nil {
		liveness := val.(string)
		if strings.Contains(liveness, " ") {
			for _, item := range strings.Split(liveness, " ") {
				kv := strings.Split(item, ":")
				if len(kv) != 2 {
					continue
				}
				if kv[0] == s.addr {
					liveness = kv[1]
					break
				}
			}
		}
		switch liveness {
		case "unreachable":
			return unreachable
		case "reachable":
			return reachable
		case "unknown":
			return unknown
		}
	}

	if tk != nil {
		livenessFunc := tk.getMockRequestLiveness()
		if livenessFunc != nil {
			return livenessFunc(ctx, s)
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
	select {
	case rs := <-rsCh:
		l = rs.Val.(livenessState)
	case <-ctx.Done():
		l = unknown
		return
	}
	return
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
		grpc.WithInitialWindowSize(cfg.TiKVClient.GrpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(cfg.TiKVClient.GrpcInitialConnWindowSize),
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

type storeLoadStats struct {
	estimatedWait     time.Duration
	waitTimeUpdatedAt time.Time
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

func (s *Store) updateServerLoadStats(estimatedWaitMs uint32) {
	estimatedWait := time.Duration(estimatedWaitMs) * time.Millisecond
	// Update the estimated wait time of the store.
	loadStats := &storeLoadStats{
		estimatedWait:     estimatedWait,
		waitTimeUpdatedAt: time.Now(),
	}
	s.loadStats.Store(loadStats)
}

const (
	tikvSlowScoreDecayRate     float64 = 20.0 / 60.0 // s^(-1), linear decaying
	tikvSlowScoreSlowThreshold int64   = 80

	tikvSlowScoreUpdateInterval       = time.Millisecond * 100
	tikvSlowScoreActiveUpdateInterval = time.Second * 15
)

type StoreHealthStatus struct {
	// Used for logging.
	storeID uint64

	isSlow atomic.Bool

	// A statistic for counting the request latency to this store
	clientSideSlowScore SlowScoreStat

	tikvSideSlowScore struct {
		sync.Mutex

		// The following atomic fields is designed to be able to be read by atomic options directly, but only written
		// while holding the mutex.

		hasTiKVFeedback atomic.Bool
		score           atomic.Int64
		lastUpdateTime  atomic.Pointer[time.Time]
	}
}

type HealthStatusDetail struct {
	ClientSideSlowScore int64
	TiKVSideSlowScore   int64
}

func (d HealthStatusDetail) IsSlow() bool {
	return clientSideSlowScoreIsSlow(uint64(d.ClientSideSlowScore)) || d.TiKVSideSlowScore >= tikvSlowScoreSlowThreshold
}

func (d HealthStatusDetail) String() string {
	return fmt.Sprintf("{ ClientSideSlowScore: %d, TiKVSideSlowScore: %d }", d.ClientSideSlowScore, d.TiKVSideSlowScore)
}

func newStoreHealthStatus(storeID uint64) *StoreHealthStatus {
	return &StoreHealthStatus{
		storeID: storeID,
	}
}

// IsSlow returns whether current Store is slow.
func (s *StoreHealthStatus) IsSlow() bool {
	return s.isSlow.Load()
}

// GetHealthStatusDetail gets the current detailed information about the store's health status.
func (s *StoreHealthStatus) GetHealthStatusDetail() HealthStatusDetail {
	return HealthStatusDetail{
		ClientSideSlowScore: int64(s.clientSideSlowScore.getSlowScore()),
		TiKVSideSlowScore:   s.tikvSideSlowScore.score.Load(),
	}
}

// tick updates the health status that changes over time, such as slow score's decaying, etc. This function is expected
// to be called periodically.
func (s *StoreHealthStatus) tick(ctx context.Context, now time.Time, store *Store, requestHealthFeedbackCallback func(ctx context.Context, addr string) error) {
	metrics.TiKVHealthFeedbackOpsCounter.WithLabelValues(strconv.FormatUint(store.StoreID(), 10), "tick").Inc()
	s.clientSideSlowScore.updateSlowScore()
	s.updateTiKVServerSideSlowScoreOnTick(ctx, now, store, requestHealthFeedbackCallback)
	s.updateSlowFlag()
}

// recordClientSideSlowScoreStat records timecost of each request to update the client side slow score.
func (s *StoreHealthStatus) recordClientSideSlowScoreStat(timecost time.Duration) {
	s.clientSideSlowScore.recordSlowScoreStat(timecost)
	s.updateSlowFlag()
}

// markAlreadySlow marks the related store already slow.
func (s *StoreHealthStatus) markAlreadySlow() {
	s.clientSideSlowScore.markAlreadySlow()
	s.updateSlowFlag()
}

// updateTiKVServerSideSlowScoreOnTick updates the slow score actively, which is expected to be a periodic job.
// It skips updating if the last update time didn't elapse long enough, or it's being updated concurrently.
func (s *StoreHealthStatus) updateTiKVServerSideSlowScoreOnTick(ctx context.Context, now time.Time, store *Store, requestHealthFeedbackCallback func(ctx context.Context, addr string) error) {
	if !s.tikvSideSlowScore.hasTiKVFeedback.Load() {
		// Do nothing if no feedback has been received from this store yet.
		return
	}

	// Skip tick if the store's slow score is 1, as it's likely to be a normal case that a health store is not being
	// accessed.
	if s.tikvSideSlowScore.score.Load() <= 1 {
		return
	}

	needRefreshing := func() bool {
		lastUpdateTime := s.tikvSideSlowScore.lastUpdateTime.Load()
		if lastUpdateTime == nil {
			// If the first hasn't been received yet, assume the store doesn't support feeding back and skip the tick.
			return false
		}

		return now.Sub(*lastUpdateTime) >= tikvSlowScoreActiveUpdateInterval
	}

	if !needRefreshing() {
		return
	}

	// If not updated for too long, try to explicitly fetch it from TiKV.
	// Note that this can't be done while holding the mutex, because the updating is done by the client when receiving
	// the response (in the same way as handling the feedback information pushed from TiKV), which needs acquiring the
	// mutex.
	if requestHealthFeedbackCallback != nil && store.getLivenessState() == reachable {
		addr := store.GetAddr()
		if len(addr) == 0 {
			logutil.Logger(ctx).Warn("skip actively request health feedback info from store due to unknown addr", zap.Uint64("storeID", store.StoreID()))
		} else {
			metrics.TiKVHealthFeedbackOpsCounter.WithLabelValues(strconv.FormatUint(store.StoreID(), 10), "active_update").Inc()
			err := requestHealthFeedbackCallback(ctx, store.GetAddr())
			if err != nil {
				metrics.TiKVHealthFeedbackOpsCounter.WithLabelValues(strconv.FormatUint(store.StoreID(), 10), "active_update_err").Inc()
				logutil.Logger(ctx).Warn("actively request health feedback info from store got error", zap.Uint64("storeID", store.StoreID()), zap.Error(err))
			}
		}

		// Continue if active updating is unsuccessful.
		if !needRefreshing() {
			return
		}
	}

	if !s.tikvSideSlowScore.TryLock() {
		// It must be being updated concurrently.
		return
	}
	defer s.tikvSideSlowScore.Unlock()

	// Reload update time as it might be updated concurrently before acquiring mutex
	lastUpdateTime := s.tikvSideSlowScore.lastUpdateTime.Load()
	elapsed := now.Sub(*lastUpdateTime)
	if elapsed < tikvSlowScoreActiveUpdateInterval {
		return
	}

	// If requesting from TiKV is not successful: decay the slow score.
	score := s.tikvSideSlowScore.score.Load()
	if score < 1 {
		return
	}
	// Linear decay by time
	score = max(int64(math.Round(float64(score)-tikvSlowScoreDecayRate*elapsed.Seconds())), 1)
	s.tikvSideSlowScore.score.Store(score)

	newUpdateTime := new(time.Time)
	*newUpdateTime = now
	s.tikvSideSlowScore.lastUpdateTime.Store(newUpdateTime)
}

// updateTiKVServerSideSlowScore updates the tikv side slow score with the given value.
// Ignores if the last update time didn't elapse long enough, or it's being updated concurrently.
func (s *StoreHealthStatus) updateTiKVServerSideSlowScore(score int64, currTime time.Time) {
	defer s.updateSlowFlag()

	lastScore := s.tikvSideSlowScore.score.Load()

	if lastScore == score {
		// It's still needed to update the lastUpdateTime to tell whether the slow score is not being updated for too
		// long (so that it's needed to explicitly get the slow score).
		// from TiKV.
		// But it can be safely skipped if the score is 1 (as explicit getting slow score won't be performed in this
		// case). And note that it should be updated within mutex.
		if score > 1 {
			// Skip if not locked as it's being updated concurrently.
			if s.tikvSideSlowScore.TryLock() {
				newUpdateTime := new(time.Time)
				*newUpdateTime = currTime
				s.tikvSideSlowScore.lastUpdateTime.Store(newUpdateTime)
				s.tikvSideSlowScore.Unlock()
			}
		}
		return
	}

	lastUpdateTime := s.tikvSideSlowScore.lastUpdateTime.Load()

	if lastUpdateTime != nil && currTime.Sub(*lastUpdateTime) < tikvSlowScoreUpdateInterval {
		return
	}

	if !s.tikvSideSlowScore.TryLock() {
		// It must be being updated concurrently. Skip.
		return
	}
	defer s.tikvSideSlowScore.Unlock()

	s.tikvSideSlowScore.hasTiKVFeedback.Store(true)
	// Reload update time as it might be updated concurrently before acquiring mutex
	lastUpdateTime = s.tikvSideSlowScore.lastUpdateTime.Load()
	if lastUpdateTime != nil && currTime.Sub(*lastUpdateTime) < tikvSlowScoreUpdateInterval {
		return
	}

	newScore := score

	newUpdateTime := new(time.Time)
	*newUpdateTime = currTime

	s.tikvSideSlowScore.score.Store(newScore)
	s.tikvSideSlowScore.lastUpdateTime.Store(newUpdateTime)
}

// ResetTiKVServerSideSlowScoreForTest resets the TiKV-side slow score information and make it expired so that the
// next update can be effective. A new score should be passed to the function. For a store that's running normally
// without any sign of being slow, the value should be 1.
func (s *StoreHealthStatus) ResetTiKVServerSideSlowScoreForTest(score int64) {
	s.setTiKVSlowScoreLastUpdateTimeForTest(time.Now().Add(-time.Hour * 2))
	s.updateTiKVServerSideSlowScore(score, time.Now().Add(-time.Hour))
}

func (s *StoreHealthStatus) updateSlowFlag() {
	healthDetail := s.GetHealthStatusDetail()
	isSlow := healthDetail.IsSlow()
	old := s.isSlow.Swap(isSlow)
	if old != isSlow {
		logutil.BgLogger().Info("store health status changed", zap.Uint64("storeID", s.storeID), zap.Bool("isSlow", isSlow), zap.Stringer("healthDetail", healthDetail))
	}
}

// setTiKVSlowScoreLastUpdateTimeForTest force sets last update time of TiKV server side slow score to specified value.
// For test purpose only.
func (s *StoreHealthStatus) setTiKVSlowScoreLastUpdateTimeForTest(lastUpdateTime time.Time) {
	s.tikvSideSlowScore.Lock()
	defer s.tikvSideSlowScore.Unlock()
	s.tikvSideSlowScore.lastUpdateTime.Store(&lastUpdateTime)
}

func (s *Store) recordHealthFeedback(feedback *kvrpcpb.HealthFeedback) {
	// Note that the `FeedbackSeqNo` field of `HealthFeedback` is not used yet. It's a monotonic value that can help
	// to drop out-of-order feedback messages. But it's not checked for now since it's not very necessary to receive
	// only a slow score. It's prepared for possible use in the future.
	s.healthStatus.updateTiKVServerSideSlowScore(int64(feedback.GetSlowScore()), time.Now())
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
