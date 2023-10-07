// Copyright 2023 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/prewrite_test.go
//

// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
)

func TestPDAPI(t *testing.T) {
	if !*withTiKV {
		t.Skip("skipping TestPDAPI because with-tikv is not enabled")
	}
	suite.Run(t, new(apiTestSuite))
}

type apiTestSuite struct {
	suite.Suite
	store *tikv.KVStore
}

func (s *apiTestSuite) SetupTest() {
	addrs := strings.Split(*pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	s.Require().NoError(err)
	rpcClient := tikv.NewRPCClient()
	// Set PD HTTP client.
	store, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0, tikv.WithPDHTTPClient(nil, addrs))
	s.store = store
	storeID := uint64(1)
	s.store.GetRegionCache().SetRegionCacheStore(storeID, s.storeAddr(storeID), s.storeAddr(storeID), tikvrpc.TiKV, 1, nil)
}

func (s *apiTestSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

type storeSafeTsMockClient struct {
	tikv.Client
	requestCount int32
}

func (c *storeSafeTsMockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type != tikvrpc.CmdStoreSafeTS {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	atomic.AddInt32(&c.requestCount, 1)
	resp := &tikvrpc.Response{}
	resp.Resp = &kvrpcpb.StoreSafeTSResponse{SafeTs: 150}
	return resp, nil
}

func (c *storeSafeTsMockClient) Close() error {
	return c.Client.Close()
}

func (c *storeSafeTsMockClient) CloseAddr(addr string) error {
	return c.Client.CloseAddr(addr)
}

func (s *apiTestSuite) TestGetStoresMinResolvedTS() {
	util.EnableFailpoints()
	require := s.Require()
	require.NoError(failpoint.Enable("tikvclient/mockFastSafeTSUpdater", `return()`))
	defer func() {
		require.NoError(failpoint.Disable("tikvclient/mockFastSafeTSUpdater"))
	}()

	// Set DC label for store 1.
	// Mock Cluster-level min resolved ts failed.
	dcLabel := "testDC"
	restore := config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnScope = dcLabel
	})
	defer restore()

	labels := []*metapb.StoreLabel{
		{
			Key:   tikv.DCLabelKey,
			Value: dcLabel,
		},
	}
	storeID := uint64(1)
	s.store.GetRegionCache().SetRegionCacheStore(storeID, s.storeAddr(storeID), s.storeAddr(storeID), tikvrpc.TiKV, 1, labels)
	// Try to get the minimum resolved timestamp of the stores from PD.
	require.NoError(failpoint.Enable("tikvclient/InjectMinResolvedTS", `return(100)`))
	mockClient := storeSafeTsMockClient{
		Client: s.store.GetTiKVClient(),
	}
	s.store.SetTiKVClient(&mockClient)
	var retryCount int
	for s.store.GetMinSafeTS(dcLabel) != 100 {
		time.Sleep(100 * time.Millisecond)
		if retryCount > 5 {
			break
		}
		retryCount++
	}
	require.Equal(int32(0), atomic.LoadInt32(&mockClient.requestCount))
	require.Equal(uint64(100), s.store.GetMinSafeTS(dcLabel))
	require.Nil(failpoint.Disable("tikvclient/InjectMinResolvedTS"))
}

func (s *apiTestSuite) TestDCLabelClusterMinResolvedTS() {
	util.EnableFailpoints()
	// Try to get the minimum resolved timestamp of the cluster from PD.
	require := s.Require()
	require.NoError(failpoint.Enable("tikvclient/mockFastSafeTSUpdater", `return()`))
	defer func() {
		require.NoError(failpoint.Disable("tikvclient/mockFastSafeTSUpdater"))
	}()
	require.NoError(failpoint.Enable("tikvclient/InjectMinResolvedTS", `return(100)`))
	mockClient := storeSafeTsMockClient{
		Client: s.store.GetTiKVClient(),
	}
	s.store.SetTiKVClient(&mockClient)
	var retryCount int
	for s.store.GetMinSafeTS(oracle.GlobalTxnScope) != 100 {
		time.Sleep(100 * time.Millisecond)
		if retryCount > 5 {
			break
		}
		retryCount++
	}
	require.Equal(atomic.LoadInt32(&mockClient.requestCount), int32(0))
	require.Equal(uint64(100), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	require.NoError(failpoint.Disable("tikvclient/InjectMinResolvedTS"))

	// Set DC label for store 1.
	// Mock PD server not support get min resolved ts by stores.
	require.NoError(failpoint.Enable("tikvclient/InjectMinResolvedTS", `return(0)`))
	defer func() {
		require.NoError(failpoint.Disable("tikvclient/InjectMinResolvedTS"))
	}()
	dcLabel := "testDC"
	restore := config.UpdateGlobal(func(conf *config.Config) {
		conf.TxnScope = dcLabel
	})
	defer restore()

	labels := []*metapb.StoreLabel{
		{
			Key:   tikv.DCLabelKey,
			Value: dcLabel,
		},
	}
	storeID := uint64(1)
	s.store.GetRegionCache().SetRegionCacheStore(storeID, s.storeAddr(storeID), s.storeAddr(storeID), tikvrpc.TiKV, 1, labels)

	// Try to get the minimum resolved timestamp of the store from TiKV.
	retryCount = 0
	for s.store.GetMinSafeTS(dcLabel) != 150 {
		time.Sleep(100 * time.Millisecond)
		if retryCount > 5 {
			break
		}
		retryCount++
	}

	require.GreaterOrEqual(atomic.LoadInt32(&mockClient.requestCount), int32(1))
	require.Equal(uint64(150), s.store.GetMinSafeTS(dcLabel))
}

func (s *apiTestSuite) TestInitClusterMinResolvedTSZero() {
	util.EnableFailpoints()
	require := s.Require()
	require.NoError(failpoint.Enable("tikvclient/mockFastSafeTSUpdater", `return()`))
	defer func() {
		require.NoError(failpoint.Disable("tikvclient/mockFastSafeTSUpdater"))
	}()
	// Try to get the minimum resolved timestamp of the cluster from PD.
	require.NoError(failpoint.Enable("tikvclient/InjectMinResolvedTS", `return(100)`))
	mockClient := storeSafeTsMockClient{
		Client: s.store.GetTiKVClient(),
	}
	s.store.SetTiKVClient(&mockClient)
	// Mock safeTS is not initialized.
	s.store.SetStoreSafeTS(uint64(1), 0)
	s.store.SetMinSafeTS(oracle.GlobalTxnScope, 0)
	require.Equal(uint64(0), s.store.GetMinSafeTS(oracle.GlobalTxnScope))

	var retryCount int
	for s.store.GetMinSafeTS(oracle.GlobalTxnScope) != 100 {
		require.NotEqual(uint64(math.MaxUint64), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
		time.Sleep(100 * time.Millisecond)
		if retryCount > 5 {
			break
		}
		retryCount++
	}
	require.Equal(uint64(100), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	require.NoError(failpoint.Disable("tikvclient/InjectMinResolvedTS"))

	// Try to get the minimum resolved timestamp of the cluster from TiKV.
	require.NoError(failpoint.Enable("tikvclient/InjectMinResolvedTS", `return(0)`))
	// Mock safeTS is not initialized.
	s.store.SetStoreSafeTS(uint64(1), 0)
	s.store.SetMinSafeTS(oracle.GlobalTxnScope, 0)
	require.Equal(uint64(0), s.store.GetMinSafeTS(oracle.GlobalTxnScope))

	for s.store.GetMinSafeTS(oracle.GlobalTxnScope) != 150 {
		require.NotEqual(uint64(math.MaxUint64), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
		time.Sleep(100 * time.Millisecond)
		if retryCount > 5 {
			break
		}
		retryCount++
	}
	require.Equal(uint64(150), s.store.GetMinSafeTS(oracle.GlobalTxnScope))
	require.NoError(failpoint.Disable("tikvclient/InjectMinResolvedTS"))
}

func (s *apiTestSuite) TearDownTest() {
	if s.store != nil {
		s.Require().Nil(s.store.Close())
	}
}
