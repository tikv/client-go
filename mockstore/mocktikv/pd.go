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
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pkg/errors"
	pd "github.com/tikv/pd/client"
)

// Use global variables to prevent pdClients from creating duplicate timestamps.
var tsMu = struct {
	sync.Mutex
	physicalTS int64
	logicalTS  int64
}{}

type pdClient struct {
	cluster *Cluster
}

// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
// from a Cluster.
func NewPDClient(cluster *Cluster) pd.Client {
	return &pdClient{
		cluster: cluster,
	}
}

func (c *pdClient) GetClusterID(ctx context.Context) uint64 {
	return 1
}

func (c *pdClient) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	panic("unimplemented")
}

func (c *pdClient) GetLeaderAddr() string {
	panic("unimplemented")
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

func (c *pdClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return &mockTSFuture{c, ctx}
}

func (c *pdClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *pdClient) GetLocalTSAsync(ctx context.Context, dcLocation string) pd.TSFuture {
	return c.GetTSAsync(ctx)
}

func (c *pdClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	panic("unimplemented")
}

type mockTSFuture struct {
	pdc *pdClient
	ctx context.Context
}

func (m *mockTSFuture) Wait() (int64, int64, error) {
	return m.pdc.GetTS(m.ctx)
}

func (c *pdClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	region, peer := c.cluster.GetRegionByKey(key)
	return &pd.Region{Meta: region, Leader: peer}, nil
}

func (c *pdClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*pd.Region, error) {
	panic("unimplemented")
}

func (c *pdClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	region, peer := c.cluster.GetPrevRegionByKey(key)
	return &pd.Region{Meta: region, Leader: peer}, nil
}

func (c *pdClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	region, peer := c.cluster.GetRegionByID(regionID)
	return &pd.Region{Meta: region, Leader: peer}, nil
}

func (c *pdClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	panic("unimplemented")
}

func (c *pdClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	store := c.cluster.GetStore(storeID)
	return store, nil
}

func (c *pdClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	panic(errors.New("unimplemented"))
}

func (c *pdClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *pdClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	panic("unimplemented")
}

func (c *pdClient) ScatterRegionWithOption(ctx context.Context, regionID uint64, opts ...pd.RegionsOption) error {
	panic("unimplemented")
}

func (c *pdClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	panic("unimplemented")
}

func (c *pdClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	panic("unimplemented")
}

func (c *pdClient) Close() {
}
