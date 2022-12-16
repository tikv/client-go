// Copyright 2022 TiKV Authors
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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestKV(t *testing.T) {
	suite.Run(t, new(testKVSuite))
}

type testKVSuite struct {
	suite.Suite
	store              *KVStore
	cluster            *mocktikv.Cluster
	tikvStoreId        uint64
	tiflashStoreId     uint64
	tiflashPeerStoreId uint64
}

func (s *testKVSuite) SetupTest() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	s.Require().Nil(err)
	testutils.BootstrapWithSingleStore(cluster)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	s.Require().Nil(err)

	s.store = store
	s.cluster = cluster

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.tikvStoreId = storeIDs[0]
	s.tiflashStoreId = storeIDs[1]
	tiflashPeerAddrId := cluster.AllocIDs(1)
	s.tiflashPeerStoreId = tiflashPeerAddrId[0]

	s.cluster.UpdateStorePeerAddr(s.tiflashStoreId, s.storeAddr(s.tiflashPeerStoreId), &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.store.regionCache.SetRegionCacheStore(s.tikvStoreId, s.storeAddr(s.tikvStoreId), s.storeAddr(s.tikvStoreId), tikvrpc.TiKV, 1, nil)
	var labels []*metapb.StoreLabel
	labels = append(labels, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.store.regionCache.SetRegionCacheStore(s.tiflashStoreId, s.storeAddr(s.tiflashStoreId), s.storeAddr(s.tiflashPeerStoreId), tikvrpc.TiFlash, 1, labels)

}

func (s *testKVSuite) TearDownTest() {
	s.Require().Nil(s.store.Close())
}

func (s *testKVSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

type storeSafeTsMockClient struct {
	Client
	requestCount int
	testSuite    *testKVSuite
}

func (c *storeSafeTsMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdStoreSafeTS {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	c.requestCount++
	resp := &tikvrpc.Response{}
	if addr == c.testSuite.storeAddr(c.testSuite.tiflashPeerStoreId) {
		resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: 80}
	} else {
		resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: 100}
	}
	return resp, nil
}

func (c *storeSafeTsMockClient) Close() error {
	return c.Client.Close()
}

func (c *storeSafeTsMockClient) CloseAddr(addr string) error {
	return c.Client.CloseAddr(addr)
}

func (s *testKVSuite) TestMinSafeTs() {
	mockClient := storeSafeTsMockClient{
		Client:    s.store.GetTiKVClient(),
		testSuite: s,
	}
	s.store.SetTiKVClient(&mockClient)
	// wait for updateSafeTS
	time.Sleep(4 * time.Second)
	s.Require().Equal(2, mockClient.requestCount)
	s.Require().Equal(uint64(80), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}
