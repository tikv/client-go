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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/split_test.go
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

package tikv_test

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
)

func TestSplit(t *testing.T) {
	suite.Run(t, new(testSplitSuite))
}

type testSplitSuite struct {
	suite.Suite
	cluster testutils.Cluster
	store   tikv.StoreProbe
	bo      *tikv.Backoffer
}

func (s *testSplitSuite) SetupTest() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	s.Require().Nil(err)
	testutils.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	s.Require().Nil(err)

	s.store = tikv.StoreProbe{KVStore: store}
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testSplitSuite) TearDownTest() {
	s.store.Close()
}

func (s *testSplitSuite) begin() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func (s *testSplitSuite) split(regionID uint64, key []byte) {
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(regionID, newRegionID, key, []uint64{peerID}, peerID)
}

func (s *testSplitSuite) TestSplitBatchGet() {
	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	s.Require().Nil(err)

	txn := s.begin()

	keys := [][]byte{{'a'}, {'b'}, {'c'}}
	_, region, err := s.store.GetRegionCache().GroupKeysByRegion(s.bo, keys, nil)
	s.Nil(err)

	s.split(loc.Region.GetID(), []byte("b"))
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// mocktikv will panic if it meets a not-in-region key.
	err = txn.BatchGetSingleRegion(s.bo, region, keys, func([]byte, []byte) {})
	s.Nil(err)
}

func (s *testSplitSuite) TestStaleEpoch() {
	mockPDClient := &mockPDClient{client: s.store.GetRegionCache().PDClient()}
	s.store.SetRegionCachePDClient(mockPDClient)

	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	s.Require().Nil(err)

	txn := s.begin()
	err = txn.Set([]byte("a"), []byte("a"))
	s.Nil(err)
	err = txn.Set([]byte("c"), []byte("c"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)

	// Initiate a split and disable the PD client. If it still works, the
	// new region is updated from kvrpc.
	s.split(loc.Region.GetID(), []byte("b"))
	mockPDClient.disable()

	txn = s.begin()
	_, err = txn.Get(context.TODO(), []byte("a"))
	s.Nil(err)
	_, err = txn.Get(context.TODO(), []byte("c"))
	s.Nil(err)
}

var errStopped = errors.New("stopped")

type mockPDClient struct {
	sync.RWMutex
	client pd.Client
	stop   bool
}

func (c *mockPDClient) LoadGlobalConfig(ctx context.Context, names []string) ([]pd.GlobalConfigItem, error) {
	return nil, nil
}

func (c *mockPDClient) StoreGlobalConfig(ctx context.Context, items []pd.GlobalConfigItem) error {
	return nil
}

func (c *mockPDClient) WatchGlobalConfig(ctx context.Context) (chan []pd.GlobalConfigItem, error) {
	return nil, nil
}

func (c *mockPDClient) disable() {
	c.Lock()
	defer c.Unlock()
	c.stop = true
}

func (c *mockPDClient) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	return nil, nil
}

func (c *mockPDClient) GetClusterID(context.Context) uint64 {
	return 1
}

func (c *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return 0, 0, errors.WithStack(errStopped)
	}
	return c.client.GetTS(ctx)
}

func (c *mockPDClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *mockPDClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return nil
}

func (c *mockPDClient) GetLocalTSAsync(ctx context.Context, dcLocation string) pd.TSFuture {
	return nil
}

func (c *mockPDClient) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.WithStack(errStopped)
	}
	return c.client.GetRegion(ctx, key, opts...)
}

func (c *mockPDClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*pd.Region, error) {
	return nil, nil
}

func (c *mockPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.WithStack(errStopped)
	}
	return c.client.GetPrevRegion(ctx, key, opts...)
}

func (c *mockPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.WithStack(errStopped)
	}
	return c.client.GetRegionByID(ctx, regionID, opts...)
}

func (c *mockPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.WithStack(errStopped)
	}
	return c.client.ScanRegions(ctx, startKey, endKey, limit)
}

func (c *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.WithStack(errStopped)
	}
	return c.client.GetStore(ctx, storeID)
}

func (c *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	c.RLock()
	defer c.Unlock()

	if c.stop {
		return nil, errors.WithStack(errStopped)
	}
	return c.client.GetAllStores(ctx)
}

func (c *mockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockPDClient) Close() {}

func (c *mockPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *mockPDClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, nil
}

func (c *mockPDClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	return nil, nil
}

func (c *mockPDClient) SplitAndScatterRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitAndScatterRegionsResponse, error) {
	return nil, nil
}

func (c *mockPDClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *mockPDClient) GetLeaderAddr() string { return "mockpd" }

func (c *mockPDClient) UpdateOption(option pd.DynamicOption, value interface{}) error {
	return nil
}

func (c *mockPDClient) LoadKeyspace(ctx context.Context, name string) (*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *mockPDClient) WatchKeyspaces(ctx context.Context) (chan []*keyspacepb.KeyspaceMeta, error) {
	return nil, nil
}

func (c *mockPDClient) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	panic("unimplemented")
}

func (c *mockPDClient) SetExternalTimestamp(ctx context.Context, tso uint64) error {
	panic("unimplemented")
}
