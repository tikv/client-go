// Copyright 2025 TiKV Authors
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

package transaction

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/latch"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
)

var _ oracle.Oracle = (*unimplementedOracle)(nil)

type unimplementedOracle struct{}

// Close implements oracle.Oracle.
func (u *unimplementedOracle) Close() {
	panic("unimplemented")
}

// GetAllTSOKeyspaceGroupMinTS implements oracle.Oracle.
func (u *unimplementedOracle) GetAllTSOKeyspaceGroupMinTS(ctx context.Context) (uint64, error) {
	panic("unimplemented")
}

// GetExternalTimestamp implements oracle.Oracle.
func (u *unimplementedOracle) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	panic("unimplemented")
}

// GetLowResolutionTimestamp implements oracle.Oracle.
func (u *unimplementedOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	panic("unimplemented")
}

// GetLowResolutionTimestampAsync implements oracle.Oracle.
func (u *unimplementedOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	panic("unimplemented")
}

// GetStaleTimestamp implements oracle.Oracle.
func (u *unimplementedOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (uint64, error) {
	panic("unimplemented")
}

// GetTimestamp implements oracle.Oracle.
func (u *unimplementedOracle) GetTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	panic("unimplemented")
}

// GetTimestampAsync implements oracle.Oracle.
func (u *unimplementedOracle) GetTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	panic("unimplemented")
}

// IsExpired implements oracle.Oracle.
func (u *unimplementedOracle) IsExpired(lockTimestamp uint64, TTL uint64, opt *oracle.Option) bool {
	panic("unimplemented")
}

// SetExternalTimestamp implements oracle.Oracle.
func (u *unimplementedOracle) SetExternalTimestamp(ctx context.Context, ts uint64) error {
	panic("unimplemented")
}

// SetLowResolutionTimestampUpdateInterval implements oracle.Oracle.
func (u *unimplementedOracle) SetLowResolutionTimestampUpdateInterval(time.Duration) error {
	panic("unimplemented")
}

// UntilExpired implements oracle.Oracle.
func (u *unimplementedOracle) UntilExpired(lockTimeStamp uint64, TTL uint64, opt *oracle.Option) int64 {
	panic("unimplemented")
}

// ValidateReadTS implements oracle.Oracle.
func (u *unimplementedOracle) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *oracle.Option) error {
	panic("unimplemented")
}

var _ pd.Client = (*unimplementedPDClient)(nil)

type unimplementedPDClient struct{}

// AcquireTokenBuckets implements pd.Client.
func (u *unimplementedPDClient) AcquireTokenBuckets(ctx context.Context, request *resource_manager.TokenBucketsRequest) ([]*resource_manager.TokenBucketResponse, error) {
	panic("unimplemented")
}

// AddResourceGroup implements pd.Client.
func (u *unimplementedPDClient) AddResourceGroup(ctx context.Context, metaGroup *resource_manager.ResourceGroup) (string, error) {
	panic("unimplemented")
}

// BatchScanRegions implements pd.Client.
func (u *unimplementedPDClient) BatchScanRegions(ctx context.Context, keyRanges []pd.KeyRange, limit int, opts ...pd.GetRegionOption) ([]*pd.Region, error) {
	panic("unimplemented")
}

// Close implements pd.Client.
func (u *unimplementedPDClient) Close() {
	panic("unimplemented")
}

// DeleteResourceGroup implements pd.Client.
func (u *unimplementedPDClient) DeleteResourceGroup(ctx context.Context, resourceGroupName string) (string, error) {
	panic("unimplemented")
}

// Get implements pd.Client.
func (u *unimplementedPDClient) Get(ctx context.Context, key []byte, opts ...pd.OpOption) (*meta_storagepb.GetResponse, error) {
	panic("unimplemented")
}

// GetAllKeyspaces implements pd.Client.
func (u *unimplementedPDClient) GetAllKeyspaces(ctx context.Context, startID uint32, limit uint32) ([]*keyspacepb.KeyspaceMeta, error) {
	panic("unimplemented")
}

// GetAllMembers implements pd.Client.
func (u *unimplementedPDClient) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	panic("unimplemented")
}

// GetAllStores implements pd.Client.
func (u *unimplementedPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	panic("unimplemented")
}

// GetClusterID implements pd.Client.
func (u *unimplementedPDClient) GetClusterID(ctx context.Context) uint64 {
	panic("unimplemented")
}

// GetExternalTimestamp implements pd.Client.
func (u *unimplementedPDClient) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	panic("unimplemented")
}

// GetLeaderURL implements pd.Client.
func (u *unimplementedPDClient) GetLeaderURL() string {
	panic("unimplemented")
}

// GetLocalTS implements pd.Client.
func (u *unimplementedPDClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	panic("unimplemented")
}

// GetLocalTSAsync implements pd.Client.
func (u *unimplementedPDClient) GetLocalTSAsync(ctx context.Context, dcLocation string) pd.TSFuture {
	panic("unimplemented")
}

// GetMinTS implements pd.Client.
func (u *unimplementedPDClient) GetMinTS(ctx context.Context) (int64, int64, error) {
	panic("unimplemented")
}

// GetOperator implements pd.Client.
func (u *unimplementedPDClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	panic("unimplemented")
}

// GetPrevRegion implements pd.Client.
func (u *unimplementedPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	panic("unimplemented")
}

// GetRegion implements pd.Client.
func (u *unimplementedPDClient) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	panic("unimplemented")
}

// GetRegionByID implements pd.Client.
func (u *unimplementedPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	panic("unimplemented")
}

// GetRegionFromMember implements pd.Client.
func (u *unimplementedPDClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string, opts ...pd.GetRegionOption) (*pd.Region, error) {
	panic("unimplemented")
}

// GetResourceGroup implements pd.Client.
func (u *unimplementedPDClient) GetResourceGroup(ctx context.Context, resourceGroupName string, opts ...pd.GetResourceGroupOption) (*resource_manager.ResourceGroup, error) {
	panic("unimplemented")
}

// GetServiceDiscovery implements pd.Client.
func (u *unimplementedPDClient) GetServiceDiscovery() pd.ServiceDiscovery {
	panic("unimplemented")
}

// GetStore implements pd.Client.
func (u *unimplementedPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	panic("unimplemented")
}

// GetTS implements pd.Client.
func (u *unimplementedPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	panic("unimplemented")
}

// GetTSAsync implements pd.Client.
func (u *unimplementedPDClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	panic("unimplemented")
}

// ListResourceGroups implements pd.Client.
func (u *unimplementedPDClient) ListResourceGroups(ctx context.Context, opts ...pd.GetResourceGroupOption) ([]*resource_manager.ResourceGroup, error) {
	panic("unimplemented")
}

// LoadGlobalConfig implements pd.Client.
func (u *unimplementedPDClient) LoadGlobalConfig(ctx context.Context, names []string, configPath string) ([]pd.GlobalConfigItem, int64, error) {
	panic("unimplemented")
}

// LoadKeyspace implements pd.Client.
func (u *unimplementedPDClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	panic("unimplemented")
}

// LoadResourceGroups implements pd.Client.
func (u *unimplementedPDClient) LoadResourceGroups(ctx context.Context) ([]*resource_manager.ResourceGroup, int64, error) {
	panic("unimplemented")
}

// ModifyResourceGroup implements pd.Client.
func (u *unimplementedPDClient) ModifyResourceGroup(ctx context.Context, metaGroup *resource_manager.ResourceGroup) (string, error) {
	panic("unimplemented")
}

// Put implements pd.Client.
func (u *unimplementedPDClient) Put(ctx context.Context, key []byte, value []byte, opts ...pd.OpOption) (*meta_storagepb.PutResponse, error) {
	panic("unimplemented")
}

// ScanRegions implements pd.Client.
func (u *unimplementedPDClient) ScanRegions(ctx context.Context, key []byte, endKey []byte, limit int, opts ...pd.GetRegionOption) ([]*pd.Region, error) {
	panic("unimplemented")
}

// ScatterRegion implements pd.Client.
func (u *unimplementedPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	panic("unimplemented")
}

// ScatterRegions implements pd.Client.
func (u *unimplementedPDClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	panic("unimplemented")
}

// SetExternalTimestamp implements pd.Client.
func (u *unimplementedPDClient) SetExternalTimestamp(ctx context.Context, timestamp uint64) error {
	panic("unimplemented")
}

// SplitAndScatterRegions implements pd.Client.
func (u *unimplementedPDClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	panic("unimplemented")
}

// SplitRegions implements pd.Client.
func (u *unimplementedPDClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	panic("unimplemented")
}

// StoreGlobalConfig implements pd.Client.
func (u *unimplementedPDClient) StoreGlobalConfig(ctx context.Context, configPath string, items []pd.GlobalConfigItem) error {
	panic("unimplemented")
}

// UpdateGCSafePoint implements pd.Client.
func (u *unimplementedPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

// UpdateGCSafePointV2 implements pd.Client.
func (u *unimplementedPDClient) UpdateGCSafePointV2(ctx context.Context, keyspaceID uint32, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

// UpdateKeyspaceState implements pd.Client.
func (u *unimplementedPDClient) UpdateKeyspaceState(ctx context.Context, id uint32, state keyspacepb.KeyspaceState) (*keyspacepb.KeyspaceMeta, error) {
	panic("unimplemented")
}

// UpdateOption implements pd.Client.
func (u *unimplementedPDClient) UpdateOption(option pd.DynamicOption, value any) error {
	panic("unimplemented")
}

// UpdateServiceGCSafePoint implements pd.Client.
func (u *unimplementedPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

// UpdateServiceSafePointV2 implements pd.Client.
func (u *unimplementedPDClient) UpdateServiceSafePointV2(ctx context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

// Watch implements pd.Client.
func (u *unimplementedPDClient) Watch(ctx context.Context, key []byte, opts ...pd.OpOption) (chan []*meta_storagepb.Event, error) {
	panic("unimplemented")
}

// WatchGCSafePointV2 implements pd.Client.
func (u *unimplementedPDClient) WatchGCSafePointV2(ctx context.Context, revision int64) (chan []*pdpb.SafePointEvent, error) {
	panic("unimplemented")
}

// WatchGlobalConfig implements pd.Client.
func (u *unimplementedPDClient) WatchGlobalConfig(ctx context.Context, configPath string, revision int64) (chan []pd.GlobalConfigItem, error) {
	panic("unimplemented")
}

// WatchKeyspaces implements pd.Client.
func (u *unimplementedPDClient) WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error) {
	panic("unimplemented")
}

var _ kvstore = (*unimplementedKVStore)(nil)

type unimplementedKVStore struct{}

// Ctx implements kvstore.
func (u *unimplementedKVStore) Ctx() context.Context {
	panic("unimplemented")
}

// CurrentTimestamp implements kvstore.
func (u *unimplementedKVStore) CurrentTimestamp(txnScope string) (uint64, error) {
	panic("unimplemented")
}

// GetClusterID implements kvstore.
func (u *unimplementedKVStore) GetClusterID() uint64 {
	panic("unimplemented")
}

// GetLockResolver implements kvstore.
func (u *unimplementedKVStore) GetLockResolver() *txnlock.LockResolver {
	panic("unimplemented")
}

// GetOracle implements kvstore.
func (u *unimplementedKVStore) GetOracle() oracle.Oracle {
	panic("unimplemented")
}

// GetRegionCache implements kvstore.
func (u *unimplementedKVStore) GetRegionCache() *locate.RegionCache {
	panic("unimplemented")
}

// GetTiKVClient implements kvstore.
func (u *unimplementedKVStore) GetTiKVClient() (client client.Client) {
	panic("unimplemented")
}

// GetTimestampWithRetry implements kvstore.
func (u *unimplementedKVStore) GetTimestampWithRetry(bo *retry.Backoffer, scope string) (uint64, error) {
	panic("unimplemented")
}

// Go implements kvstore.
func (u *unimplementedKVStore) Go(f func()) error {
	panic("unimplemented")
}

// IsClose implements kvstore.
func (u *unimplementedKVStore) IsClose() bool {
	panic("unimplemented")
}

// SendReq implements kvstore.
func (u *unimplementedKVStore) SendReq(bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	panic("unimplemented")
}

// SplitRegions implements kvstore.
func (u *unimplementedKVStore) SplitRegions(ctx context.Context, splitKeys [][]byte, scatter bool, tableID *int64) (regionIDs []uint64, err error) {
	panic("unimplemented")
}

// TxnLatches implements kvstore.
func (u *unimplementedKVStore) TxnLatches() *latch.LatchesScheduler {
	panic("unimplemented")
}

// WaitGroup implements kvstore.
func (u *unimplementedKVStore) WaitGroup() *sync.WaitGroup {
	panic("unimplemented")
}

// WaitScatterRegionFinish implements kvstore.
func (u *unimplementedKVStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error {
	panic("unimplemented")
}

// CheckVisibility implements txnsnapshot.kvstore.
func (m *unimplementedKVStore) CheckVisibility(startTime uint64) error {
	panic("unimplemented")
}

var _ client.Client = (*unimplementedKVClient)(nil)

type unimplementedKVClient struct{}

// Close implements client.Client.
func (u *unimplementedKVClient) Close() error {
	panic("unimplemented")
}

// CloseAddr implements client.Client.
func (u *unimplementedKVClient) CloseAddr(addr string) error {
	panic("unimplemented")
}

// SendRequest implements client.Client.
func (u *unimplementedKVClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	panic("unimplemented")
}

// SetEventListener implements client.Client.
func (u *unimplementedKVClient) SetEventListener(listener client.ClientEventListener) {
	panic("unimplemented")
}
