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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/mocktikv/pd.go
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

package mocktikv

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/clients/tso"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"github.com/tikv/pd/client/pkg/circuitbreaker"
	sd "github.com/tikv/pd/client/servicediscovery"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Use global variables to prevent pdClients from creating duplicate timestamps.
var tsMu = struct {
	sync.Mutex
	physicalTS int64
	logicalTS  int64
}{}

const defaultResourceGroupName = "default"

var _ pd.Client = (*pdClient)(nil)

type MockPDOption func(*pdClient)

func WithDelay(delay *atomic.Bool) MockPDOption {
	return func(pc *pdClient) {
		pc.delay = delay
	}
}

type pdClient struct {
	cluster *Cluster

	// GC safe point set by `UpdateGCSafePoint`(deprecated) and `AdvanceGCSafePoint`. Not to be confused with SafePointKV.
	gcSafePoint uint64
	// txn safe point set by `AdvanceTxnSafePoint`.
	txnSafePoint uint64
	// Represents the GC barriers for blocking GC from advancing.
	gcBarriers map[string]uint64
	// Represents the global GC barriers that block GC across all keyspaces.
	globalGCBarriers map[string]uint64
	// As there are still usages of SafePointKV, the txn safe point will still be put in the SavePointKV.
	gcStatesMu sync.Mutex

	externalTimestamp atomic.Uint64

	groups map[string]*rmpb.ResourceGroup

	delay *atomic.Bool
}

// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
// from a Cluster.
func NewPDClient(cluster *Cluster, ops ...MockPDOption) *pdClient {
	mockCli := &pdClient{
		cluster:          cluster,
		gcBarriers:       make(map[string]uint64),
		globalGCBarriers: make(map[string]uint64),
		groups:           make(map[string]*rmpb.ResourceGroup),
	}

	mockCli.groups[defaultResourceGroupName] = &rmpb.ResourceGroup{
		Name: defaultResourceGroupName,
		Mode: rmpb.GroupMode_RUMode,
		RUSettings: &rmpb.GroupRequestUnitSettings{
			RU: &rmpb.TokenBucket{
				Settings: &rmpb.TokenLimitSettings{
					FillRate:   math.MaxInt32,
					BurstLimit: -1,
				},
			},
		},
		Priority: 8,
	}
	for _, op := range ops {
		op(mockCli)
	}
	return mockCli
}

func (c *pdClient) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]pd.GlobalConfigItem, int64, error) {
	return nil, 0, nil
}

func (c *pdClient) StoreGlobalConfig(ctx context.Context, configPath string, items []pd.GlobalConfigItem) error {
	return nil
}

func (c *pdClient) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []pd.GlobalConfigItem, error) {
	return nil, nil
}

func (c *pdClient) GetClusterID(ctx context.Context) uint64 {
	return 1
}

func (c *pdClient) GetTS(context.Context) (int64, int64, error) {
	tsMu.Lock()
	defer tsMu.Unlock()

	ts := time.Now().UnixNano() / int64(time.Millisecond)
	if tsMu.physicalTS >= ts {
		tsMu.logicalTS++
	} else {
		tsMu.physicalTS = ts
		tsMu.logicalTS = 0
	}
	return tsMu.physicalTS, tsMu.logicalTS, nil
}

// GetMinTS returns the minimal ts.
func (c *pdClient) GetMinTS(ctx context.Context) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	panic("unimplemented")
}

func (c *pdClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *pdClient) GetTSAsync(ctx context.Context) tso.TSFuture {
	return &mockTSFuture{c, ctx, false}
}

func (c *pdClient) GetLocalTSAsync(ctx context.Context, dcLocation string) tso.TSFuture {
	return c.GetTSAsync(ctx)
}

func (c *pdClient) SetExternalTimestamp(ctx context.Context, newTimestamp uint64) error {
	p, l, err := c.GetTS(ctx)
	if err != nil {
		return err
	}

	currentTSO := oracle.ComposeTS(p, l)
	if newTimestamp > currentTSO {
		return errors.New("external timestamp is greater than global tso")
	}
	for {
		externalTimestamp := c.externalTimestamp.Load()
		if externalTimestamp > newTimestamp {
			return errors.New("cannot decrease the external timestamp")
		} else if externalTimestamp == newTimestamp {
			return nil
		}

		if c.externalTimestamp.CompareAndSwap(externalTimestamp, newTimestamp) {
			return nil
		}
	}
}

func (c *pdClient) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	return c.externalTimestamp.Load(), nil
}

type mockTSFuture struct {
	pdc  *pdClient
	ctx  context.Context
	used bool
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	if m.used {
		return 0, 0, errors.New("cannot wait tso twice")
	}
	m.used = true
	return m.pdc.GetTS(m.ctx)
}

func (c *pdClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	enforceCircuitBreakerFor("GetRegion", ctx)
	region, peer, buckets, downPeers := c.cluster.GetRegionByKey(key)
	if len(opts) == 0 {
		buckets = nil
	}
	if c.delay != nil && c.delay.Load() {
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	return &router.Region{Meta: region, Leader: peer, Buckets: buckets, DownPeers: downPeers}, nil
}

func (c *pdClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...opt.GetRegionOption) (*router.Region, error) {
	return &router.Region{}, nil
}

func (c *pdClient) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	enforceCircuitBreakerFor("GetPrevRegion", ctx)
	region, peer, buckets, downPeers := c.cluster.GetPrevRegionByKey(key)
	if len(opts) == 0 {
		buckets = nil
	}
	return &router.Region{Meta: region, Leader: peer, Buckets: buckets, DownPeers: downPeers}, nil
}

func (c *pdClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*router.Region, error) {
	enforceCircuitBreakerFor("GetRegionByID", ctx)
	region, peer, buckets, downPeers := c.cluster.GetRegionByID(regionID)
	return &router.Region{Meta: region, Leader: peer, Buckets: buckets, DownPeers: downPeers}, nil
}

func (c *pdClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error) {
	enforceCircuitBreakerFor("ScanRegions", ctx)
	regions := c.cluster.ScanRegions(startKey, endKey, limit, opts...)
	return regions, nil
}

func (c *pdClient) BatchScanRegions(ctx context.Context, keyRanges []router.KeyRange, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error) {
	enforceCircuitBreakerFor("BatchScanRegions", ctx)
	if _, err := util.EvalFailpoint("mockBatchScanRegionsUnimplemented"); err == nil {
		return nil, status.Errorf(codes.Unimplemented, "mock BatchScanRegions is not implemented")
	}
	regions := make([]*router.Region, 0, len(keyRanges))
	var lastRegion *router.Region
	for _, keyRange := range keyRanges {
		if lastRegion != nil && lastRegion.Meta != nil {
			if lastRegion.Meta.EndKey == nil || bytes.Compare(lastRegion.Meta.EndKey, keyRange.EndKey) >= 0 {
				continue
			}
			if bytes.Compare(lastRegion.Meta.EndKey, keyRange.StartKey) > 0 {
				keyRange.StartKey = lastRegion.Meta.EndKey
			}
		}
		rangeRegions := c.cluster.ScanRegions(keyRange.StartKey, keyRange.EndKey, limit, opts...)
		if len(rangeRegions) > 0 {
			lastRegion = rangeRegions[len(rangeRegions)-1]
		}
		regions = append(regions, rangeRegions...)
		limit -= len(regions)
		if limit <= 0 {
			break
		}
	}
	return regions, nil
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	store := c.cluster.GetStore(storeID)
	// It's same as PD's implementation.
	if store == nil {
		return nil, fmt.Errorf("invalid store ID %d, not found", storeID)
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return store, nil
}

func (c *pdClient) GetAllStores(ctx context.Context, opts ...opt.GetStoreOption) ([]*metapb.Store, error) {
	return c.cluster.GetAllStores(), nil
}

func (c *pdClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	c.gcStatesMu.Lock()
	defer c.gcStatesMu.Unlock()

	if safePoint > c.gcSafePoint {
		c.gcSafePoint = safePoint
	}
	return c.gcSafePoint, nil
}

func (c *pdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	c.gcStatesMu.Lock()
	defer c.gcStatesMu.Unlock()

	if ttl == 0 {
		delete(c.gcBarriers, serviceID)
	} else {
		var minSafePoint uint64 = math.MaxUint64
		for _, ssp := range c.gcBarriers {
			if ssp < minSafePoint {
				minSafePoint = ssp
			}
		}

		if len(c.gcBarriers) == 0 || minSafePoint <= safePoint {
			c.gcBarriers[serviceID] = safePoint
		}
	}

	// The minSafePoint may have changed. Reload it.
	var minSafePoint uint64 = math.MaxUint64
	for _, ssp := range c.gcBarriers {
		if ssp < minSafePoint {
			minSafePoint = ssp
		}
	}
	return minSafePoint, nil
}

func (c *pdClient) GetAllKeyspaces(ctx context.Context, startID uint32, limit uint32) ([]*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) Close() {
}

func (c *pdClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *pdClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...opt.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, nil
}

func (c *pdClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	return nil, nil
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *pdClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	return nil, nil
}

func (c *pdClient) GetAllMembers(ctx context.Context) (*pdpb.GetMembersResponse, error) {
	return nil, nil
}

func (c *pdClient) GetLeaderURL() string { return "mockpd" }

func (c *pdClient) UpdateOption(option opt.DynamicOption, value interface{}) error {
	return nil
}

func (c *pdClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *pdClient) ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (c *pdClient) GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	group, ok := c.groups[resourceGroupName]
	if !ok {
		return nil, fmt.Errorf("the group %s does not exist", resourceGroupName)
	}
	return group, nil
}

func (c *pdClient) AddResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (c *pdClient) ModifyResourceGroup(ctx context.Context, metaGroup *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (c *pdClient) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	return "", nil
}

func (c *pdClient) WatchResourceGroup(ctx context.Context, revision int64) (chan []*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (c *pdClient) AcquireTokenBuckets(ctx context.Context, request *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (c *pdClient) GetTSWithinKeyspace(ctx context.Context, keyspaceID uint32) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) GetTSWithinKeyspaceAsync(ctx context.Context, keyspaceID uint32) tso.TSFuture {
	return nil
}

func (c *pdClient) GetLocalTSWithinKeyspace(ctx context.Context, dcLocation string, keyspaceID uint32) (int64, int64, error) {
	return 0, 0, nil
}

func (c *pdClient) GetLocalTSWithinKeyspaceAsync(ctx context.Context, dcLocation string, keyspaceID uint32) tso.TSFuture {
	return nil
}

func (c *pdClient) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan []*meta_storagepb.Event, error) {
	return nil, nil
}

func (c *pdClient) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	return nil, nil
}

func (c *pdClient) Put(ctx context.Context, key []byte, value []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return nil, nil
}

func (c *pdClient) LoadResourceGroups(ctx context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

func (c *pdClient) GetServiceDiscovery() sd.ServiceDiscovery { return nil }

func (c *pdClient) WithCallerComponent(caller.Component) pd.Client { return c }

func enforceCircuitBreakerFor(name string, ctx context.Context) {
	if circuitbreaker.FromContext(ctx) == nil {
		panic(fmt.Errorf("CircuitBreaker must be configured for %s", name))
	}
}

func (c *pdClient) GetGCInternalController(keyspaceID uint32) pdgc.InternalController {
	return &gcInternalController{
		inner:      c,
		keyspaceID: keyspaceID,
	}
}

func (c *pdClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &gcStatesClient{
		inner:      c,
		keyspaceID: keyspaceID,
	}
}

type gcInternalController struct {
	inner      *pdClient
	keyspaceID uint32
}

func (c gcInternalController) AdvanceTxnSafePoint(ctx context.Context, target uint64) (pdgc.AdvanceTxnSafePointResult, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	if target < c.inner.txnSafePoint {
		return pdgc.AdvanceTxnSafePointResult{},
			errors.Errorf("trying to update txn safe point to a smaller value, current value: %v, given: %v",
				c.inner.txnSafePoint, target)
	}

	res := pdgc.AdvanceTxnSafePointResult{
		OldTxnSafePoint:    c.inner.txnSafePoint,
		Target:             target,
		NewTxnSafePoint:    target,
		BlockerDescription: "",
	}

	minGCBarrierName := ""
	var minGCBarrierTS uint64 = 0
	isGlobalBarrier := false
	for name, ts := range c.inner.gcBarriers {
		if ts == 0 {
			panic("found 0 in barrier ts of GC barriers")
		}
		if ts < minGCBarrierTS || minGCBarrierTS == 0 {
			minGCBarrierName = name
			minGCBarrierTS = ts
			isGlobalBarrier = false
		}
	}
	for name, ts := range c.inner.globalGCBarriers {
		if ts == 0 {
			panic("found 0 in barrier ts of global GC barriers")
		}
		if ts < minGCBarrierTS || minGCBarrierTS == 0 {
			minGCBarrierName = name
			minGCBarrierTS = ts
			isGlobalBarrier = true
		}
	}

	if minGCBarrierTS != 0 && minGCBarrierTS < res.NewTxnSafePoint {
		res.NewTxnSafePoint = minGCBarrierTS
		barrierType := "GCBarrier"
		if isGlobalBarrier {
			barrierType = "GlobalGCBarrier"
		}
		res.BlockerDescription = fmt.Sprintf("%s { BarrierID: %+q, BarrierTS: %d, ExpirationTime: <nil> }", barrierType, minGCBarrierName, res.NewTxnSafePoint)
		logutil.Logger(ctx).Info("txn safe point blocked",
			zap.Uint64("oldTxnSafePoint", res.OldTxnSafePoint), zap.Uint64("newTxnSafePoint", res.NewTxnSafePoint),
			zap.String("blocker", res.BlockerDescription))
	}

	if res.NewTxnSafePoint < res.OldTxnSafePoint {
		res.NewTxnSafePoint = res.OldTxnSafePoint
		logutil.Logger(ctx).Info("txn safe point unable to be blocked",
			zap.Uint64("oldTxnSafePoint", res.OldTxnSafePoint), zap.Uint64("newTxnSafePoint", res.NewTxnSafePoint),
			zap.String("blocker", res.BlockerDescription))
	}

	c.inner.txnSafePoint = res.NewTxnSafePoint

	return res, nil
}

func (c gcInternalController) AdvanceGCSafePoint(ctx context.Context, target uint64) (pdgc.AdvanceGCSafePointResult, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	if target < c.inner.gcSafePoint {
		return pdgc.AdvanceGCSafePointResult{},
			errors.Errorf("trying to update gc safe point to a smaller value, current value: %v, given: %v",
				c.inner.gcSafePoint, target)
	}

	if target > c.inner.txnSafePoint {
		return pdgc.AdvanceGCSafePointResult{},
			errors.Errorf("trying to update GC safe point to a too large value that exceeds the txn safe point, current value: %v, given: %v, current txn safe point: %v",
				c.inner.gcSafePoint, target, c.inner.txnSafePoint)
	}

	res := pdgc.AdvanceGCSafePointResult{
		OldGCSafePoint: c.inner.gcSafePoint,
		Target:         target,
		NewGCSafePoint: target,
	}

	c.inner.gcSafePoint = res.NewGCSafePoint

	return res, nil
}

type gcStatesClient struct {
	inner      *pdClient
	keyspaceID uint32
}

func (c gcStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	startTime := time.Now()

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	if barrierTS == 0 || barrierID == "" || ttl <= 0 {
		return nil, errors.New("invalid arguments")
	}

	// TTL is unimplemented here.

	if barrierTS < c.inner.txnSafePoint {
		return nil, errors.Errorf("trying to set a GC barrier on ts %d which is already behind the txn safe point %d", barrierTS, c.inner.txnSafePoint)
	}

	res := pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime)
	c.inner.gcBarriers[barrierID] = barrierTS
	return res, nil
}

func (c gcStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	startTime := time.Now()

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	barrierTS, exists := c.inner.gcBarriers[barrierID]

	if !exists {
		return nil, nil
	}

	delete(c.inner.gcBarriers, barrierID)
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime), nil
}

func (c gcStatesClient) SetGlobalGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GlobalGCBarrierInfo, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	startTime := time.Now()

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	if barrierTS == 0 || barrierID == "" || ttl <= 0 {
		return nil, errors.New("invalid arguments")
	}

	if barrierTS < c.inner.txnSafePoint {
		return nil, errors.Errorf("trying to set a global GC barrier on ts %d which is already behind the txn safe point %d", barrierTS, c.inner.txnSafePoint)
	}

	ttlToUse := ttl
	if ttlToUse == 0 || ttlToUse == pdgc.TTLNeverExpire {
		ttlToUse = pdgc.TTLNeverExpire
	}

	c.inner.globalGCBarriers[barrierID] = barrierTS
	return pdgc.NewGlobalGCBarrierInfo(barrierID, barrierTS, ttlToUse, startTime), nil
}

func (c gcStatesClient) DeleteGlobalGCBarrier(ctx context.Context, barrierID string) (*pdgc.GlobalGCBarrierInfo, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	startTime := time.Now()

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	barrierTS, exists := c.inner.globalGCBarriers[barrierID]
	if !exists {
		return nil, nil
	}

	delete(c.inner.globalGCBarriers, barrierID)
	return pdgc.NewGlobalGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime), nil
}

func (c gcStatesClient) GetAllKeyspacesGCStates(ctx context.Context) (pdgc.ClusterGCStates, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	startTime := time.Now()

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	gcStates := make(map[uint32]pdgc.GCState, 1)

	localBarriers := make([]*pdgc.GCBarrierInfo, 0, len(c.inner.gcBarriers))
	for barrierID, barrierTS := range c.inner.gcBarriers {
		localBarriers = append(localBarriers, pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime))
	}

	gcStates[constants.NullKeyspaceID] = pdgc.GCState{
		KeyspaceID:   constants.NullKeyspaceID,
		TxnSafePoint: c.inner.txnSafePoint,
		GCSafePoint:  c.inner.gcSafePoint,
		GCBarriers:   localBarriers,
	}

	globalBarriers := make([]*pdgc.GlobalGCBarrierInfo, 0, len(c.inner.globalGCBarriers))
	for barrierID, barrierTS := range c.inner.globalGCBarriers {
		globalBarriers = append(globalBarriers, pdgc.NewGlobalGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime))
	}

	return pdgc.ClusterGCStates{
		GCStates:         gcStates,
		GlobalGCBarriers: globalBarriers,
	}, nil
}

func (c gcStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	if c.keyspaceID != constants.NullKeyspaceID {
		panic("unimplemented")
	}

	startTime := time.Now()

	c.inner.gcStatesMu.Lock()
	defer c.inner.gcStatesMu.Unlock()

	res := pdgc.GCState{
		KeyspaceID:   c.keyspaceID,
		TxnSafePoint: c.inner.txnSafePoint,
		GCSafePoint:  c.inner.gcSafePoint,
	}

	gcBarriers := make([]*pdgc.GCBarrierInfo, 0, len(c.inner.gcBarriers))
	for barrierID, barrierTS := range c.inner.gcBarriers {
		gcBarriers = append(gcBarriers, pdgc.NewGCBarrierInfo(barrierID, barrierTS, pdgc.TTLNeverExpire, startTime))
	}
	res.GCBarriers = gcBarriers

	return res, nil
}
