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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestKV(t *testing.T) {
	util.EnableFailpoints()
	suite.Run(t, new(testKVSuite))
}

type testKVSuite struct {
	suite.Suite
	store          *KVStore
	cluster        *mocktikv.Cluster
	tikvStoreID    uint64
	tiflashStoreID uint64

	mockGetMinResolvedTSByStoresIDs atomic.Pointer[func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)]
}

func (s *testKVSuite) SetupTest() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	s.Require().Nil(err)
	testutils.BootstrapWithSingleStore(cluster)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		return 0, nil, nil
	})
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0, Option(func(store *KVStore) {
		store.pdHttpClient = &mockPDHTTPClient{
			Client:                          pdhttp.NewClientWithServiceDiscovery("test", nil),
			mockGetMinResolvedTSByStoresIDs: &s.mockGetMinResolvedTSByStoresIDs,
		}
	}))
	s.Require().Nil(err)

	s.store = store
	s.cluster = cluster

	storeIDs, _, _, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.tikvStoreID = storeIDs[0]
	s.tiflashStoreID = storeIDs[1]

	var labels []*metapb.StoreLabel
	labels = append(cluster.GetStore(s.tikvStoreID).Labels,
		&metapb.StoreLabel{Key: DCLabelKey, Value: "z1"})
	s.cluster.UpdateStorePeerAddr(s.tikvStoreID, s.storeAddr(s.tikvStoreID), labels...)
	s.store.regionCache.SetRegionCacheStore(s.tikvStoreID, s.storeAddr(s.tikvStoreID), s.storeAddr(s.tikvStoreID), tikvrpc.TiKV, 1, labels)

	labels = append(cluster.GetStore(s.tiflashStoreID).Labels,
		&metapb.StoreLabel{Key: DCLabelKey, Value: "z2"},
		&metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.cluster.UpdateStorePeerAddr(s.tiflashStoreID, s.storeAddr(s.tiflashStoreID), labels...)
	s.store.regionCache.SetRegionCacheStore(s.tiflashStoreID, s.storeAddr(s.tiflashStoreID), s.storeAddr(s.tiflashStoreID), tikvrpc.TiFlash, 1, labels)

}

func (s *testKVSuite) TearDownTest() {
	s.Require().Nil(s.store.Close())
}

func (s *testKVSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testKVSuite) setGetMinResolvedTSByStoresIDs(f func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)) {
	s.mockGetMinResolvedTSByStoresIDs.Store(&f)
}

type storeSafeTsMockClient struct {
	Client
	requestCount int32
	testSuite    *testKVSuite

	tikvSafeTs    uint64
	tiflashSafeTs uint64
}

func newStoreSafeTsMockClient(s *testKVSuite) *storeSafeTsMockClient {
	return &storeSafeTsMockClient{
		Client:        s.store.GetTiKVClient(),
		testSuite:     s,
		tikvSafeTs:    100,
		tiflashSafeTs: 80,
	}
}

func (c *storeSafeTsMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdStoreSafeTS {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	atomic.AddInt32(&c.requestCount, 1)
	resp := &tikvrpc.Response{}
	if addr == c.testSuite.storeAddr(c.testSuite.tiflashStoreID) {
		resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: c.tiflashSafeTs}
	} else {
		resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: c.tikvSafeTs}
	}
	return resp, nil
}

func (c *storeSafeTsMockClient) Close() error {
	return c.Client.Close()
}

func (c *storeSafeTsMockClient) CloseAddr(addr string) error {
	return c.Client.CloseAddr(addr)
}

type mockPDHTTPClient struct {
	pdhttp.Client
	mockGetMinResolvedTSByStoresIDs *atomic.Pointer[func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error)]
}

func (c *mockPDHTTPClient) GetMinResolvedTSByStoresIDs(ctx context.Context, storeIDs []uint64) (uint64, map[uint64]uint64, error) {
	if f := c.mockGetMinResolvedTSByStoresIDs.Load(); f != nil {
		return (*f)(ctx, storeIDs)
	}
	return c.Client.GetMinResolvedTSByStoresIDs(ctx, storeIDs)
}

func (s *testKVSuite) TestMinSafeTsFromStores() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)

	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS(oracle.GlobalTxnScope)
		s.Require().False(math.MaxUint64 == ts)
		return ts == mockClient.tiflashSafeTs
	}, 15*time.Second, time.Second)
	s.Require().GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(2))
	s.Require().Equal(mockClient.tiflashSafeTs, s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	ok, ts := s.store.getSafeTS(s.tikvStoreID)
	s.Require().True(ok)
	s.Require().Equal(mockClient.tikvSafeTs, ts)
}

func (s *testKVSuite) TestMinSafeTsFromStoresWithZeroSafeTs() {
	// ref https://github.com/tikv/client-go/issues/1276
	mockClient := newStoreSafeTsMockClient(s)
	mockClient.tikvSafeTs = 0
	mockClient.tiflashSafeTs = 0
	s.store.SetTiKVClient(mockClient)

	s.Eventually(func() bool {
		return atomic.LoadInt32(&mockClient.requestCount) >= 4
	}, 15*time.Second, time.Second)

	s.Require().Equal(uint64(0), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromPD() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		return 90, nil, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS(oracle.GlobalTxnScope)
		s.Require().False(math.MaxUint64 == ts)
		return ts == 90
	}, 15*time.Second, time.Second)
	s.Require().Equal(atomic.LoadInt32(&mockClient.requestCount), int32(0))
	s.Require().Equal(uint64(90), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromPDByStores() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		m := make(map[uint64]uint64)
		for _, id := range ids {
			m[id] = uint64(100) + id
		}
		return math.MaxUint64, m, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS(oracle.GlobalTxnScope)
		s.Require().False(math.MaxUint64 == ts)
		return ts == uint64(100)+s.tikvStoreID
	}, 15*time.Second, time.Second)
	s.Require().Equal(atomic.LoadInt32(&mockClient.requestCount), int32(0))
	s.Require().Equal(uint64(100)+s.tikvStoreID, s.store.GetMinSafeTS(oracle.GlobalTxnScope))
}

func (s *testKVSuite) TestMinSafeTsFromMixed1() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		m := make(map[uint64]uint64)
		for _, id := range ids {
			if id == s.tiflashStoreID {
				m[id] = 0
			} else {
				m[id] = uint64(10)
			}
		}
		return math.MaxUint64, m, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS("z1")
		s.Require().False(math.MaxUint64 == ts)
		return ts == uint64(10)
	}, 15*time.Second, time.Second)
	s.Require().GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(1))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS("z1"))
	s.Require().Equal(mockClient.tiflashSafeTs, s.store.GetMinSafeTS("z2"))
}

func (s *testKVSuite) TestMinSafeTsFromMixed2() {
	mockClient := newStoreSafeTsMockClient(s)
	s.store.SetTiKVClient(mockClient)
	s.setGetMinResolvedTSByStoresIDs(func(ctx context.Context, ids []uint64) (uint64, map[uint64]uint64, error) {
		m := make(map[uint64]uint64)
		for _, id := range ids {
			if id == s.tiflashStoreID {
				m[id] = uint64(10)
			} else {
				m[id] = math.MaxUint64
			}
		}
		return math.MaxUint64, m, nil
	})
	s.Eventually(func() bool {
		ts := s.store.GetMinSafeTS("z2")
		s.Require().False(math.MaxUint64 == ts)
		return ts == uint64(10)
	}, 15*time.Second, time.Second)
	s.Require().GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(1))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	s.Require().Equal(mockClient.tikvSafeTs, s.store.GetMinSafeTS("z1"))
	s.Require().Equal(uint64(10), s.store.GetMinSafeTS("z2"))
}
