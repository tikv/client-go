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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv.go
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

package tikv

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/latch"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// DCLabelKey indicates the key of label which represents the dc for Store.
	DCLabelKey           = "zone"
	safeTSUpdateInterval = time.Second * 2
)

func createEtcdKV(addrs []string, tlsConfig *tls.Config) (*clientv3.Client, error) {
	cfg := config.GetGlobalConfig()
	cli, err := clientv3.New(
		clientv3.Config{
			Endpoints:            addrs,
			AutoSyncInterval:     30 * time.Second,
			DialTimeout:          5 * time.Second,
			TLS:                  tlsConfig,
			DialKeepAliveTime:    time.Second * time.Duration(cfg.TiKVClient.GrpcKeepAliveTime),
			DialKeepAliveTimeout: time.Second * time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout),
		},
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return cli, nil
}

// Update oracle's lastTS every 2000ms by default. Can override at startup with an option to NewKVStore
// or at runtime by calling SetLowResolutionTimestampUpdateInterval on the oracle
var defaultOracleUpdateInterval = 2 * time.Second

// KVStore contains methods to interact with a TiKV cluster.
type KVStore struct {
	clusterID uint64
	uuid      string
	oracle    oracle.Oracle
	clientMu  struct {
		sync.RWMutex
		client Client
	}
	pdClient     pd.Client
	pdHttpClient pdhttp.Client
	regionCache  *locate.RegionCache
	lockResolver *txnlock.LockResolver
	txnLatches   *latch.LatchesScheduler

	mock bool

	kv        SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex // this is used to update safePoint and spTime

	// storeID -> safeTS, stored as map[uint64]uint64
	// safeTS here will be used during the Stale Read process,
	// it indicates the safe timestamp point that can be used to read consistent but may not the latest data.
	safeTSMap sync.Map

	// MinSafeTs stores the minimum ts value for each txnScope
	minSafeTS sync.Map

	replicaReadSeed uint32 // this is used to load balance followers / learners when replica read is enabled

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	close  atomicutil.Bool
	gP     Pool
}

var _ Storage = (*KVStore)(nil)

// Go run the function in a separate goroutine.
func (s *KVStore) Go(f func()) error {
	return s.gP.Run(f)
}

// UpdateSPCache updates cached safepoint.
func (s *KVStore) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

// CheckVisibility checks if it is safe to read using given ts.
func (s *KVStore) CheckVisibility(startTime uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return tikverr.NewErrPDServerTimeout("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return &tikverr.ErrGCTooEarly{
			TxnStartTS:  t1,
			GCSafePoint: t2,
		}
	}

	return nil
}

// Option is the option for pool.
type Option func(*KVStore)

// WithPool set the pool
func WithPool(gp Pool) Option {
	return func(o *KVStore) {
		o.gP = gp
	}
}

// WithUpdateInterval sets the frequency with which to refresh read timestamps
// from the PD client. Smaller updateInterval will lead to more HTTP calls to
// PD and less staleness on reads, and vice versa.
func WithUpdateInterval(updateInterval time.Duration) Option {
	return func(o *KVStore) {
		err := o.oracle.SetLowResolutionTimestampUpdateInterval(updateInterval)
		if err != nil {
			panic(err)
		}
	}
}

// WithPDHTTPClient sets the PD HTTP client with the given PD addresses and options.
// Source is to mark where the HTTP client is created, which is used for metrics and logs.
func WithPDHTTPClient(
	source string,
	pdAddrs []string,
	opts ...pdhttp.ClientOption,
) Option {
	return func(o *KVStore) {
		if cli := o.GetPDClient(); cli != nil {
			o.pdHttpClient = pdhttp.NewClientWithServiceDiscovery(
				source,
				o.pdClient.GetServiceDiscovery(),
				opts...,
			)
		} else {
			o.pdHttpClient = pdhttp.NewClient(
				source,
				pdAddrs,
				opts...,
			)
		}
	}
}

// loadOption load KVStore option into KVStore.
func loadOption(store *KVStore, opt ...Option) {
	for _, f := range opt {
		f(store)
	}
}

const getHealthFeedbackTimeout = time.Second * 2

func requestHealthFeedbackFromKVClient(ctx context.Context, addr string, tikvClient Client) error {
	// When batch RPC is enabled (`MaxBatchSize` > 0), a `GetHealthFeedback` RPC call will cause TiKV also sending the
	// health feedback information in via the `BatchCommandsResponse`, which will be handled by the batch client.
	// Therefore the same information carried in the response don't need to be handled in this case. And as we're
	// currently not supporting health feedback mechanism without enabling batch RPC, we do not use the information
	// carried in the `resp` here.
	resp, err := tikvClient.SendRequest(ctx, addr, tikvrpc.NewRequest(tikvrpc.CmdGetHealthFeedback, &kvrpcpb.GetHealthFeedbackRequest{}), getHealthFeedbackTimeout)
	if err != nil {
		return err
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	if regionErr != nil {
		return errors.Errorf("requested health feedback from store but received region error: %s", regionErr.String())
	}
	return nil
}

// NewKVStore creates a new TiKV store instance.
func NewKVStore(uuid string, pdClient pd.Client, spkv SafePointKV, tikvclient Client, opt ...Option) (*KVStore, error) {
	o, err := oracles.NewPdOracle(pdClient, defaultOracleUpdateInterval)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	regionCache := locate.NewRegionCache(pdClient, locate.WithRequestHealthFeedbackCallback(func(ctx context.Context, addr string) error {
		return requestHealthFeedbackFromKVClient(ctx, addr, tikvclient)
	}))
	store := &KVStore{
		clusterID:       pdClient.GetClusterID(context.TODO()),
		uuid:            uuid,
		oracle:          o,
		pdClient:        pdClient,
		regionCache:     regionCache,
		kv:              spkv,
		safePoint:       0,
		spTime:          time.Now(),
		replicaReadSeed: rand.Uint32(),
		ctx:             ctx,
		cancel:          cancel,
		gP:              NewSpool(128, 10*time.Second),
	}
	store.clientMu.client = client.NewReqCollapse(client.NewInterceptedClient(tikvclient))
	store.clientMu.client.SetEventListener(regionCache.GetClientEventListener())

	store.lockResolver = txnlock.NewLockResolver(store)
	loadOption(store, opt...)

	store.wg.Add(2)
	go store.runSafePointChecker()
	go store.safeTSUpdater()

	return store, nil
}

// NewPDClient returns an unwrapped pd client.
func NewPDClient(pdAddrs []string) (pd.Client, error) {
	cfg := config.GetGlobalConfig()
	// init pd-client
	pdCli, err := pd.NewClient(
		pdAddrs, pd.SecurityOption{
			CAPath:   cfg.Security.ClusterSSLCA,
			CertPath: cfg.Security.ClusterSSLCert,
			KeyPath:  cfg.Security.ClusterSSLKey,
		},
		pd.WithGRPCDialOptions(
			grpc.WithKeepaliveParams(
				keepalive.ClientParameters{
					Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
					Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
				},
			),
		),
		pd.WithCustomTimeoutOption(time.Duration(cfg.PDClient.PDServerTimeout)*time.Second),
		pd.WithForwardingOption(config.GetGlobalConfig().EnableForwarding),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return pdCli, nil
}

// EnableTxnLocalLatches enables txn latch. It should be called before using
// the store to serve any requests.
func (s *KVStore) EnableTxnLocalLatches(size uint) {
	s.txnLatches = latch.NewScheduler(size)
}

// IsLatchEnabled is used by mockstore.TestConfig.
func (s *KVStore) IsLatchEnabled() bool {
	return s.txnLatches != nil
}

func (s *KVStore) runSafePointChecker() {
	defer s.wg.Done()
	d := gcSafePointUpdateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV())
			if err == nil {
				metrics.TiKVLoadSafepointCounter.WithLabelValues("ok").Inc()
				s.UpdateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUpdateInterval
			} else {
				metrics.TiKVLoadSafepointCounter.WithLabelValues("fail").Inc()
				logutil.BgLogger().Error("fail to load safepoint from pd", zap.Error(err))
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// Begin a global transaction.
func (s *KVStore) Begin(opts ...TxnOption) (txn *transaction.KVTxn, err error) {
	options := &transaction.TxnOptions{}
	// Inject the options
	for _, opt := range opts {
		opt(options)
	}

	if options.TxnScope == "" {
		options.TxnScope = oracle.GlobalTxnScope
	}
	var (
		startTS uint64
	)
	if options.StartTS != nil {
		startTS = *options.StartTS
	} else {
		bo := retry.NewBackofferWithVars(context.Background(), transaction.TsoMaxBackoff, nil)
		startTS, err = s.getTimestampWithRetry(bo, options.TxnScope)
		if err != nil {
			return nil, err
		}
	}

	snapshot := txnsnapshot.NewTiKVSnapshot(s, startTS, s.nextReplicaReadSeed())
	return transaction.NewTiKVTxn(s, snapshot, startTS, options)
}

// DeleteRange delete all versions of all keys in the range[startKey,endKey) immediately.
// Be careful while using this API. This API doesn't keep recent MVCC versions, but will delete all versions of all keys
// in the range immediately. Also notice that frequent invocation to this API may cause performance problems to TiKV.
func (s *KVStore) DeleteRange(
	ctx context.Context, startKey []byte, endKey []byte, concurrency int,
) (completedRegions int, err error) {
	task := rangetask.NewDeleteRangeTask(s, startKey, endKey, concurrency)
	err = task.Execute(ctx)
	if err == nil {
		completedRegions = task.CompletedRegions()
	}
	return completedRegions, err
}

// GetSnapshot gets a snapshot that is able to read any data which data is <= the given ts.
// If the given ts is greater than the current TSO timestamp, the snapshot is not guaranteed
// to be consistent.
// Specially, it is useful to set ts to math.MaxUint64 to point get the latest committed data.
func (s *KVStore) GetSnapshot(ts uint64) *txnsnapshot.KVSnapshot {
	snapshot := txnsnapshot.NewTiKVSnapshot(s, ts, s.nextReplicaReadSeed())
	return snapshot
}

// Close store
func (s *KVStore) Close() error {
	defer s.gP.Close()
	s.close.Store(true)
	s.cancel()
	s.wg.Wait()

	s.oracle.Close()
	s.pdClient.Close()
	if s.pdHttpClient != nil {
		s.pdHttpClient.Close()
	}
	s.lockResolver.Close()

	if err := s.GetTiKVClient().Close(); err != nil {
		return err
	}

	if s.txnLatches != nil {
		s.txnLatches.Close()
	}
	s.regionCache.Close()

	if err := s.kv.Close(); err != nil {
		return err
	}
	return nil
}

// UUID return a unique ID which represents a Storage.
func (s *KVStore) UUID() string {
	return s.uuid
}

// CurrentTimestamp returns current timestamp with the given txnScope (local or global).
func (s *KVStore) CurrentTimestamp(txnScope string) (uint64, error) {
	bo := retry.NewBackofferWithVars(context.Background(), transaction.TsoMaxBackoff, nil)
	startTS, err := s.getTimestampWithRetry(bo, txnScope)
	if err != nil {
		return 0, err
	}
	return startTS, nil
}

// CurrentAllTSOKeyspaceGroupMinTs returns a minimum timestamp from all TSO keyspace groups.
func (s *KVStore) CurrentAllTSOKeyspaceGroupMinTs() (uint64, error) {
	bo := retry.NewBackofferWithVars(context.Background(), transaction.TsoMaxBackoff, nil)
	startTS, err := s.getAllTSOKeyspaceGroupMinTSWithRetry(bo)
	if err != nil {
		return 0, err
	}
	return startTS, nil
}

// GetTimestampWithRetry returns latest timestamp.
func (s *KVStore) GetTimestampWithRetry(bo *Backoffer, scope string) (uint64, error) {
	return s.getTimestampWithRetry(bo, scope)
}

func (s *KVStore) getTimestampWithRetry(bo *Backoffer, txnScope string) (uint64, error) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("TiKVStore.getTimestampWithRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	for {
		startTS, err := s.oracle.GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: txnScope})
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: PD server timeout
		// This may cause duplicate data to be written.
		if val, e := util.EvalFailpoint("mockGetTSErrorInRetry"); e == nil && val.(bool) {
			if _, e := util.EvalFailpoint("mockCommitErrorOpt"); e != nil {
				err = tikverr.NewErrPDServerTimeout("mock PD timeout")
			}
		}

		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(retry.BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, err
		}
	}
}

func (s *KVStore) getAllTSOKeyspaceGroupMinTSWithRetry(bo *Backoffer) (uint64, error) {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("TiKVStore.getAllTSOKeyspaceGroupMinTSWithRetry", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	for {
		minTS, err := s.oracle.GetAllTSOKeyspaceGroupMinTS(bo.GetCtx())
		if err == nil {
			return minTS, nil
		}
		// no need to back off
		if strings.Contains(err.Error(), "Unimplemented") {
			return 0, err
		}
		err = bo.Backoff(retry.BoPDRPC, errors.Errorf("get minimum timestamp failed: %v", err))
		if err != nil {
			return 0, err
		}
	}
}

func (s *KVStore) nextReplicaReadSeed() uint32 {
	return atomic.AddUint32(&s.replicaReadSeed, 1)
}

// GetOracle gets a timestamp oracle client.
func (s *KVStore) GetOracle() oracle.Oracle {
	return s.oracle
}

// GetPDClient returns the PD client.
func (s *KVStore) GetPDClient() pd.Client {
	return s.pdClient
}

// GetPDHTTPClient returns the PD HTTP client.
func (s *KVStore) GetPDHTTPClient() pdhttp.Client {
	return s.pdHttpClient
}

// SupportDeleteRange gets the storage support delete range or not.
func (s *KVStore) SupportDeleteRange() (supported bool) {
	return !s.mock
}

// SendReq sends a request to locate.
func (s *KVStore) SendReq(
	bo *Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration,
) (*tikvrpc.Response, error) {
	sender := locate.NewRegionRequestSender(s.regionCache, s.GetTiKVClient())
	resp, _, err := sender.SendReq(bo, req, regionID, timeout)
	return resp, err
}

// GetRegionCache returns the region cache instance.
func (s *KVStore) GetRegionCache() *locate.RegionCache {
	return s.regionCache
}

// GetLockResolver returns the lock resolver instance.
func (s *KVStore) GetLockResolver() *txnlock.LockResolver {
	return s.lockResolver
}

// Closed returns a channel that indicates if the store is closed.
func (s *KVStore) Closed() <-chan struct{} {
	return s.ctx.Done()
}

// GetSafePointKV returns the kv store that used for safepoint.
func (s *KVStore) GetSafePointKV() SafePointKV {
	return s.kv
}

// SetOracle resets the oracle instance.
func (s *KVStore) SetOracle(oracle oracle.Oracle) {
	s.oracle = oracle
}

// SetTiKVClient resets the client instance.
func (s *KVStore) SetTiKVClient(client Client) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	s.clientMu.client = client
}

// GetTiKVClient gets the client instance.
func (s *KVStore) GetTiKVClient() (client Client) {
	s.clientMu.RLock()
	defer s.clientMu.RUnlock()
	return s.clientMu.client
}

// GetMinSafeTS return the minimal safeTS of the storage with given txnScope.
func (s *KVStore) GetMinSafeTS(txnScope string) uint64 {
	if val, ok := s.minSafeTS.Load(txnScope); ok {
		return val.(uint64)
	}
	return 0
}

func (s *KVStore) setMinSafeTS(txnScope string, safeTS uint64) {
	// ensure safeTS is not set to max uint64
	if safeTS == math.MaxUint64 {
		logutil.AssertWarn(logutil.BgLogger(), "skip setting min-safe-ts to max uint64", zap.String("txnScope", txnScope), zap.Stack("stack"))
		return
	}
	s.minSafeTS.Store(txnScope, safeTS)
}

// Ctx returns ctx.
func (s *KVStore) Ctx() context.Context {
	return s.ctx
}

// IsClose checks whether the store is closed.
func (s *KVStore) IsClose() bool {
	return s.close.Load()
}

// WaitGroup returns wg
func (s *KVStore) WaitGroup() *sync.WaitGroup {
	return &s.wg
}

// TxnLatches returns txnLatches.
func (s *KVStore) TxnLatches() *latch.LatchesScheduler {
	return s.txnLatches
}

// GetClusterID returns store's cluster id.
func (s *KVStore) GetClusterID() uint64 {
	return s.clusterID
}

func (s *KVStore) getSafeTS(storeID uint64) (bool, uint64) {
	safeTS, ok := s.safeTSMap.Load(storeID)
	if !ok {
		return false, 0
	}
	return true, safeTS.(uint64)
}

// setSafeTS sets safeTs for store storeID, export for testing
func (s *KVStore) setSafeTS(storeID, safeTS uint64) {
	// ensure safeTS is not set to max uint64
	if safeTS == math.MaxUint64 {
		logutil.AssertWarn(logutil.BgLogger(), "skip setting safe-ts to max uint64", zap.Uint64("storeID", storeID), zap.Stack("stack"))
		return
	}
	s.safeTSMap.Store(storeID, safeTS)
}

func (s *KVStore) updateMinSafeTS(txnScope string, storeIDs []uint64) {
	minSafeTS := uint64(math.MaxUint64)
	// when there is no store, return 0 in order to let minStartTS become startTS directly
	// actually storeIDs won't be empty since updateMinSafeTS is only called by updateSafeTS and updateSafeTS builds
	// txnScopeMap with non-empty values. here we check it to make the logic more robust.
	if len(storeIDs) < 1 {
		s.setMinSafeTS(txnScope, 0)
		return
	}
	for _, store := range storeIDs {
		ok, safeTS := s.getSafeTS(store)
		if ok {
			// safeTS is guaranteed to be less than math.MaxUint64 (by setSafeTS and its callers)
			if safeTS != 0 && safeTS < minSafeTS {
				minSafeTS = safeTS
			}
		} else {
			minSafeTS = 0
		}
	}
	// if minSafeTS is still math.MaxUint64, that means all store safe ts are 0, then we set minSafeTS to 0.
	if minSafeTS == math.MaxUint64 {
		minSafeTS = 0
	}
	s.setMinSafeTS(txnScope, minSafeTS)
}

func (s *KVStore) safeTSUpdater() {
	defer s.wg.Done()
	t := time.NewTicker(safeTSUpdateInterval)
	if _, e := util.EvalFailpoint("mockFastSafeTSUpdater"); e == nil {
		t.Reset(time.Millisecond * 100)
	}
	defer t.Stop()
	ctx, cancel := context.WithCancel(s.ctx)
	ctx = util.WithInternalSourceType(ctx, util.InternalTxnGC)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.updateSafeTS(ctx)
		}
	}
}

func (s *KVStore) updateSafeTS(ctx context.Context) {
	// Try to get the cluster-level minimum resolved timestamp from PD first.
	if s.updateGlobalTxnScopeTSFromPD(ctx) {
		return
	}

	// When txn scope is not global, we need to get the minimum resolved timestamp of each store.
	stores := s.regionCache.GetAllStores()
	tikvClient := s.GetTiKVClient()
	wg := &sync.WaitGroup{}
	wg.Add(len(stores))
	// Try to get the minimum resolved timestamp of the store from PD.
	var (
		err                 error
		storeMinResolvedTSs map[uint64]uint64
	)
	storeIDs := make([]uint64, len(stores))
	if s.pdHttpClient != nil {
		for i, store := range stores {
			storeIDs[i] = store.StoreID()
		}
		_, storeMinResolvedTSs, err = s.getMinResolvedTSByStoresIDs(ctx, storeIDs)
		if err != nil {
			// If getting the minimum resolved timestamp from PD failed, log the error and need to get it from TiKV.
			logutil.BgLogger().Debug("get resolved TS from PD failed", zap.Error(err), zap.Any("stores", storeIDs))
		}
	}

	for _, store := range stores {
		storeID := store.StoreID()
		storeAddr := store.GetAddr()
		if store.IsTiFlash() {
			storeAddr = store.GetPeerAddr()
		}
		go func(ctx context.Context, wg *sync.WaitGroup, storeID uint64, storeAddr string) {
			defer wg.Done()

			var (
				safeTS     uint64
				storeIDStr = strconv.FormatUint(storeID, 10)
			)
			// If getting the minimum resolved timestamp from PD failed or returned 0/MaxUint64, try to get it from TiKV.
			if storeMinResolvedTSs == nil || !isValidSafeTS(storeMinResolvedTSs[storeID]) || err != nil {
				resp, err := tikvClient.SendRequest(
					ctx, storeAddr, tikvrpc.NewRequest(
						tikvrpc.CmdStoreSafeTS, &kvrpcpb.StoreSafeTSRequest{
							KeyRange: &kvrpcpb.KeyRange{
								StartKey: []byte(""),
								EndKey:   []byte(""),
							},
						}, kvrpcpb.Context{
							RequestSource: util.RequestSourceFromCtx(ctx),
						},
					), client.ReadTimeoutShort,
				)
				if err != nil {
					metrics.TiKVSafeTSUpdateCounter.WithLabelValues("fail", storeIDStr).Inc()
					logutil.BgLogger().Debug("update safeTS failed", zap.Error(err), zap.Uint64("store-id", storeID))
					return
				}
				safeTS = resp.Resp.(*kvrpcpb.StoreSafeTSResponse).GetSafeTs()
			} else {
				safeTS = storeMinResolvedTSs[storeID]
			}

			_, preSafeTS := s.getSafeTS(storeID)
			if preSafeTS > safeTS {
				metrics.TiKVSafeTSUpdateCounter.WithLabelValues("skip", storeIDStr).Inc()
				preSafeTSTime := oracle.GetTimeFromTS(preSafeTS)
				metrics.TiKVMinSafeTSGapSeconds.WithLabelValues(storeIDStr).Set(time.Since(preSafeTSTime).Seconds())
				return
			}
			s.setSafeTS(storeID, safeTS)
			metrics.TiKVSafeTSUpdateCounter.WithLabelValues("success", storeIDStr).Inc()
			safeTSTime := oracle.GetTimeFromTS(safeTS)
			metrics.TiKVMinSafeTSGapSeconds.WithLabelValues(storeIDStr).Set(time.Since(safeTSTime).Seconds())
		}(ctx, wg, storeID, storeAddr)
	}

	txnScopeMap := make(map[string][]uint64)
	for _, store := range stores {
		txnScopeMap[oracle.GlobalTxnScope] = append(txnScopeMap[oracle.GlobalTxnScope], store.StoreID())

		if label, ok := store.GetLabelValue(DCLabelKey); ok {
			txnScopeMap[label] = append(txnScopeMap[label], store.StoreID())
		}
	}
	for txnScope, storeIDs := range txnScopeMap {
		s.updateMinSafeTS(txnScope, storeIDs)
	}
	wg.Wait()
}

func (s *KVStore) getMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []uint64) (uint64, map[uint64]uint64, error) {
	var (
		minResolvedTS       uint64
		storeMinResolvedTSs map[uint64]uint64
		err                 error
	)
	minResolvedTS, storeMinResolvedTSs, err = s.pdHttpClient.GetMinResolvedTSByStoresIDs(ctx, storeIDs)
	if err != nil {
		return 0, nil, err
	}
	if val, e := util.EvalFailpoint("InjectPDMinResolvedTS"); e == nil {
		injectedTS, ok := val.(int)
		if !ok {
			return minResolvedTS, storeMinResolvedTSs, err
		}
		minResolvedTS = uint64(injectedTS)
		logutil.BgLogger().Info("inject min resolved ts", zap.Uint64("ts", uint64(injectedTS)))
		// Currently we only have a store 1 in the test, so it's OK to inject the same min resolved TS for all stores here.
		for storeID, v := range storeMinResolvedTSs {
			if v != 0 && v != math.MaxUint64 {
				storeMinResolvedTSs[storeID] = uint64(injectedTS)
				logutil.BgLogger().Info("inject store min resolved ts", zap.Uint64("storeID", storeID), zap.Uint64("ts", uint64(injectedTS)))
			}
		}
	}
	return minResolvedTS, storeMinResolvedTSs, err
}

var (
	skipSafeTSUpdateCounter    = metrics.TiKVSafeTSUpdateCounter.WithLabelValues("skip", "cluster")
	successSafeTSUpdateCounter = metrics.TiKVSafeTSUpdateCounter.WithLabelValues("success", "cluster")
	clusterMinSafeTSGap        = metrics.TiKVMinSafeTSGapSeconds.WithLabelValues("cluster")
)

// updateGlobalTxnScopeTSFromPD check whether it is needed to get cluster-level's min resolved ts from PD
// to update min safe ts for global txn scope.
func (s *KVStore) updateGlobalTxnScopeTSFromPD(ctx context.Context) bool {
	isGlobal := config.GetTxnScopeFromConfig() == oracle.GlobalTxnScope
	// Try to get the minimum resolved timestamp of the cluster from PD.
	if s.pdHttpClient != nil && isGlobal {
		clusterMinSafeTS, _, err := s.getMinResolvedTSByStoresIDs(ctx, nil)
		if err != nil {
			logutil.BgLogger().Debug("get resolved TS from PD failed", zap.Error(err))
		} else if isValidSafeTS(clusterMinSafeTS) {
			// Update ts and metrics.
			preClusterMinSafeTS := s.GetMinSafeTS(oracle.GlobalTxnScope)
			// preClusterMinSafeTS is guaranteed to be less than math.MaxUint64 (by this method and setMinSafeTS)
			// related to https://github.com/tikv/client-go/issues/991
			if preClusterMinSafeTS > clusterMinSafeTS {
				skipSafeTSUpdateCounter.Inc()
				preSafeTSTime := oracle.GetTimeFromTS(preClusterMinSafeTS)
				clusterMinSafeTSGap.Set(time.Since(preSafeTSTime).Seconds())
			} else {
				s.setMinSafeTS(oracle.GlobalTxnScope, clusterMinSafeTS)
				successSafeTSUpdateCounter.Inc()
				safeTSTime := oracle.GetTimeFromTS(clusterMinSafeTS)
				clusterMinSafeTSGap.Set(time.Since(safeTSTime).Seconds())
			}
			return true
		}
	}

	return false
}

func isValidSafeTS(ts uint64) bool {
	return ts != 0 && ts != math.MaxUint64
}

// EnableResourceControl enables the resource control.
func EnableResourceControl() {
	client.ResourceControlSwitch.Store(true)
}

// DisableResourceControl disables the resource control.
func DisableResourceControl() {
	client.ResourceControlSwitch.Store(false)
}

// SetResourceControlInterceptor sets the interceptor for resource control.
func SetResourceControlInterceptor(interceptor resourceControlClient.ResourceGroupKVInterceptor) {
	client.ResourceControlInterceptor.Store(&interceptor)
}

// UnsetResourceControlInterceptor un-sets the interceptor for resource control.
func UnsetResourceControlInterceptor() {
	client.ResourceControlInterceptor.Store(nil)
}

// Variables defines the variables used by TiKV storage.
type Variables = kv.Variables

// NewLockResolver is exported for other pkg to use, suppress unused warning.
var _ = NewLockResolver

// NewLockResolver creates a LockResolver.
// It is exported for other pkg to use. For instance, binlog service needs
// to determine a transaction's commit state.
// TODO(iosmanthus): support api v2
func NewLockResolver(etcdAddrs []string, security config.Security, opts ...pd.ClientOption) (
	*txnlock.LockResolver, error,
) {
	pdCli, err := pd.NewClient(
		etcdAddrs, pd.SecurityOption{
			CAPath:   security.ClusterSSLCA,
			CertPath: security.ClusterSSLCert,
			KeyPath:  security.ClusterSSLKey,
		}, opts...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	pdCli = util.InterceptedPDClient{Client: pdCli}
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))

	tlsConfig, err := security.ToTLSConfig()
	if err != nil {
		return nil, err
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs, tlsConfig)
	if err != nil {
		return nil, err
	}

	s, err := NewKVStore(
		uuid,
		locate.NewCodecPDClient(ModeTxn, pdCli),
		spkv,
		client.NewRPCClient(WithSecurity(security)),
	)
	if err != nil {
		return nil, err
	}
	return s.lockResolver, nil
}

// TxnOption configures Transaction
type TxnOption func(*transaction.TxnOptions)

// WithTxnScope sets the TxnScope to txnScope
func WithTxnScope(txnScope string) TxnOption {
	return func(st *transaction.TxnOptions) {
		st.TxnScope = txnScope
	}
}

// WithStartTS sets the StartTS to startTS
func WithStartTS(startTS uint64) TxnOption {
	return func(st *transaction.TxnOptions) {
		st.StartTS = &startTS
	}
}

// WithPipelinedMemDB creates transaction with pipelined memdb.
func WithPipelinedMemDB() TxnOption {
	return func(st *transaction.TxnOptions) {
		st.PipelinedMemDB = true
	}
}

// TODO: remove once tidb and br are ready

// KVTxn contains methods to interact with a TiKV transaction.
type KVTxn = transaction.KVTxn

// BinlogWriteResult defines the result of prewrite binlog.
type BinlogWriteResult = transaction.BinlogWriteResult

// KVFilter is a filter that filters out unnecessary KV pairs.
type KVFilter = transaction.KVFilter

// SchemaLeaseChecker is used to validate schema version is not changed during transaction execution.
type SchemaLeaseChecker = transaction.SchemaLeaseChecker

// SchemaVer is the infoSchema which will return the schema version.
type SchemaVer = transaction.SchemaVer

// MaxTxnTimeUse is the max time a Txn may use (in ms) from its begin to commit.
// We use it to abort the transaction to guarantee GC worker will not influence it.
const MaxTxnTimeUse = transaction.MaxTxnTimeUse
