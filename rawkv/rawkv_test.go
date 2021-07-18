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
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/rawkv_test.go
//

// Copyright 2021 PingCAP, Inc.
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

package rawkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
)

func TestRawKV(t *testing.T) {
	suite.Run(t, new(testRawkvSuite))
}

type testRawkvSuite struct {
	suite.Suite
	mvccStore mocktikv.MVCCStore
	cluster   *mocktikv.Cluster
	store1    uint64 // store1 is leader
	store2    uint64 // store2 is follower
	peer1     uint64 // peer1 is leader
	peer2     uint64 // peer2 is follower
	region1   uint64
	bo        *retry.Backoffer
}

func (s *testRawkvSuite) SetupTest() {
	s.mvccStore = mocktikv.MustNewMVCCStore()
	s.cluster = mocktikv.NewCluster(s.mvccStore)
	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.peer1 = peerIDs[0]
	s.peer2 = peerIDs[1]
	s.bo = retry.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRawkvSuite) TearDownTest() {
	s.mvccStore.Close()
}

func (s *testRawkvSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRawkvSuite) TestReplaceAddrWithNewStore() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(context.Background(), testKey, testValue)
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	getVal, err := client.Get(context.Background(), testKey)

	s.Nil(err)
	s.Equal(getVal, testValue)
}

func (s *testRawkvSuite) TestUpdateStoreAddr() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(context.Background(), testKey, testValue)
	s.Nil(err)
	// tikv-server reports `StoreNotMatch` And retry
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)

	getVal, err := client.Get(context.Background(), testKey)

	s.Nil(err)
	s.Equal(getVal, testValue)
}

func (s *testRawkvSuite) TestReplaceNewAddrAndOldOfflineImmediately() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(context.Background(), testKey, testValue)
	s.Nil(err)

	// pre-load store2's address into cache via follower-read.
	loc, err := client.regionCache.LocateKey(s.bo, testKey)
	s.Nil(err)
	fctx, err := client.regionCache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	s.Nil(err)
	s.Equal(fctx.Store.StoreID(), s.store2)
	s.Equal(fctx.Addr, "store2")

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	getVal, err := client.Get(context.Background(), testKey)
	s.Nil(err)
	s.Equal(getVal, testValue)
}

func (s *testRawkvSuite) TestReplaceStore() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(context.Background(), testKey, testValue)
	s.Nil(err)

	s.cluster.MarkTombstone(s.store1)
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(s.store1))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.RemovePeer(s.region1, s.peer1)
	s.cluster.ChangeLeader(s.region1, peer3)

	err = client.Put(context.Background(), testKey, testValue)
	s.Nil(err)
}
