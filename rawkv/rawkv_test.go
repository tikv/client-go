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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rawkv

import (
	"bytes"
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

type key = []byte
type value = []byte

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

func (s *testRawkvSuite) TestColumnFamilyForClient() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()

	testKeyCf1, testValueCf1, cf1 := []byte("test_key_cf1"), []byte("test_value_cf1"), "cf1"
	testKeyCf2, testValueCf2, cf2 := []byte("test_key_cf2"), []byte("test_value_cf2"), "cf2"

	// test put
	client.SetColumnFamily(cf1)
	err := client.Put(context.Background(), testKeyCf1, testValueCf1)
	s.Nil(err)
	client.SetColumnFamily(cf2)
	err = client.Put(context.Background(), testKeyCf2, testValueCf2)
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	// test get
	client.SetColumnFamily(cf1)
	getVal, err := client.Get(context.Background(), testKeyCf1)
	s.Nil(err)
	s.Equal(getVal, testValueCf1)
	getVal, err = client.Get(context.Background(), testKeyCf2)
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	client.SetColumnFamily(cf2)
	getVal, err = client.Get(context.Background(), testKeyCf2)
	s.Nil(err)
	s.Equal(getVal, testValueCf2)
	getVal, err = client.Get(context.Background(), testKeyCf1)
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	client.SetColumnFamily("")
	getVal, err = client.Get(context.Background(), testKeyCf1)
	s.Nil(err)
	s.Equal(getVal, []byte(nil))
	getVal, err = client.Get(context.Background(), testKeyCf2)
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	// test delete
	client.SetColumnFamily(cf1)
	err = client.Delete(context.Background(), testKeyCf1)
	s.Nil(err)
	getVal, err = client.Get(context.Background(), testKeyCf1)
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	client.SetColumnFamily(cf2)
	err = client.Delete(context.Background(), testKeyCf2)
	s.Nil(err)
	getVal, err = client.Get(context.Background(), testKeyCf2)
	s.Nil(err)
	s.Equal(getVal, []byte(nil))
}

func (s *testRawkvSuite) TestColumnFamilyForOptions() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()

	keyInCf1, valueInCf1, cf1 := []byte("db"), []byte("TiDB"), "cf1"
	keyInCf2, valueInCf2, cf2 := []byte("kv"), []byte("TiKV"), "cf2"

	// test put
	err := client.Put(context.Background(), keyInCf1, valueInCf1, SetColumnFamily(cf1))
	s.Nil(err)
	err = client.Put(context.Background(), keyInCf2, valueInCf2, SetColumnFamily(cf2))
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	// test get
	getVal, err := client.Get(context.Background(), keyInCf1, SetColumnFamily(cf1))
	s.Nil(err)
	s.Equal(getVal, valueInCf1)
	getVal, err = client.Get(context.Background(), keyInCf2, SetColumnFamily(cf1))
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	getVal, err = client.Get(context.Background(), keyInCf2, SetColumnFamily(cf2))
	s.Nil(err)
	s.Equal(getVal, valueInCf2)
	getVal, err = client.Get(context.Background(), keyInCf1, SetColumnFamily(cf2))
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	// test delete
	err = client.Delete(context.Background(), keyInCf1, SetColumnFamily(cf1))
	s.Nil(err)
	getVal, err = client.Get(context.Background(), keyInCf1, SetColumnFamily(cf1))
	s.Nil(err)
	s.Equal(getVal, []byte(nil))

	err = client.Delete(context.Background(), keyInCf2, SetColumnFamily(cf2))
	s.Nil(err)
	getVal, err = client.Get(context.Background(), keyInCf2, SetColumnFamily(cf2))
	s.Nil(err)
	s.Equal(getVal, []byte(nil))
}

func (s *testRawkvSuite) TestBatch() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()

	cf := "test_cf"
	paris := map[string]string{
		"db":   "TiDB",
		"key2": "value2",
		"key1": "value1",
		"key3": "value3",
		"kv":   "TiKV",
	}
	keys := make([]key, 0)
	values := make([]value, 0)
	for k, v := range paris {
		keys = append(keys, []byte(k))
		values = append(values, []byte(v))
	}

	// test BatchPut
	err := client.BatchPut(context.Background(), keys, values, SetColumnFamily(cf))
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	// test BatchGet
	returnValues, err := client.BatchGet(context.Background(), keys, SetColumnFamily(cf))
	s.Nil(err)
	s.Equal(len(returnValues), len(paris))
	for i, v := range returnValues {
		s.True(bytes.Equal(v, []byte(paris[string(keys[i])])))
	}

	// test BatchDelete
	err = client.BatchDelete(context.Background(), keys, SetColumnFamily(cf))
	s.Nil(err)

	returnValue, err := client.Get(context.Background(), keys[0], SetColumnFamily(cf))
	s.Nil(err)
	s.Equal(returnValue, []byte(nil))
}

func (s *testRawkvSuite) TestScan() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()

	cf := "test_cf"
	paris := map[string]string{
		"db":   "TiDB",
		"key2": "value2",
		"key1": "value1",
		"key4": "value4",
		"key3": "value3",
		"kv":   "TiKV",
	}
	keys := make([]key, 0)
	values := make([]value, 0)
	for k, v := range paris {
		keys = append(keys, []byte(k))
		values = append(values, []byte(v))
	}

	// BatchPut
	err := client.BatchPut(context.Background(), keys, values, SetColumnFamily(cf))
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	// test scan
	startKey, endKey := []byte("key1"), []byte("keyz")
	limit := 3
	returnKeys, returnValues, err := client.Scan(context.Background(), startKey, endKey, limit, SetColumnFamily(cf))
	s.Nil(err)
	s.Equal(len(returnKeys), limit)
	s.Equal(len(returnValues), limit)

	s.True(bytes.Equal(returnKeys[0], []byte("key1")))
	s.True(bytes.Equal(returnKeys[1], []byte("key2")))
	s.True(bytes.Equal(returnKeys[2], []byte("key3")))
	for i, k := range returnKeys {
		s.True(bytes.Equal(returnValues[i], []byte(paris[string(k)])))
	}

	// test ReverseScan with onlyKey
	startKey, endKey = []byte("key3"), nil
	limit = 10
	returnKeys, _, err = client.ReverseScan(
		context.Background(),
		startKey,
		endKey,
		limit,
		SetColumnFamily(cf),
		ScanKeyOnly(),
	)
	s.Nil(err)
	s.Equal(len(returnKeys), 3)
	s.True(bytes.Equal(returnKeys[0], []byte("key2")))
	s.True(bytes.Equal(returnKeys[1], []byte("key1")))
	s.True(bytes.Equal(returnKeys[2], []byte("db")))
}

func (s *testRawkvSuite) TestDeleteRange() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()

	cf := "test_cf"
	paris := map[string]string{
		"db":   "TiDB",
		"key2": "value2",
		"key1": "value1",
		"key4": "value4",
		"key3": "value3",
		"kv":   "TiKV",
	}
	keys := make([]key, 0)
	values := make([]value, 0)
	for k, v := range paris {
		keys = append(keys, []byte(k))
		values = append(values, []byte(v))
	}

	// BatchPut
	err := client.BatchPut(context.Background(), keys, values, SetColumnFamily(cf))
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	// test DeleteRange
	startKey, endKey := []byte("key3"), []byte(nil)
	err = client.DeleteRange(context.Background(), startKey, endKey, SetColumnFamily(cf))
	s.Nil(err)

	ks, vs, err := client.Scan(context.Background(), startKey, endKey, 10, SetColumnFamily(cf))
	s.Nil(err)
	s.Equal(0, len(ks))
	s.Equal(0, len(vs))
}

func (s *testRawkvSuite) TestCompareAndSwap() {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &Client{
		clusterID:   0,
		regionCache: locate.NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()

	cf := "my_cf"
	key, value, newValue := []byte("kv"), []byte("TiDB"), []byte("TiKV")

	// put
	err := client.Put(context.Background(), key, value, SetColumnFamily(cf))
	s.Nil(err)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	// test CompareAndSwap for false atomic
	_, _, err = client.CompareAndSwap(
		context.Background(),
		key,
		value,
		newValue,
		SetColumnFamily(cf))
	s.Error(err)

	// test CompareAndSwap for swap successfully
	client.SetAtomicForCAS(true)
	returnValue, swapped, err := client.CompareAndSwap(
		context.Background(),
		key,
		newValue,
		newValue,
		SetColumnFamily(cf))
	s.Nil(err)
	s.False(swapped)
	s.True(bytes.Equal(value, returnValue))

	// test CompareAndSwap for swap successfully
	client.SetAtomicForCAS(true)
	returnValue, swapped, err = client.CompareAndSwap(
		context.Background(),
		key,
		value,
		newValue,
		SetColumnFamily(cf))
	s.Nil(err)
	s.True(swapped)
	s.True(bytes.Equal(value, returnValue))

	v, err := client.Get(context.Background(), key, SetColumnFamily(cf))
	s.Nil(err)
	s.Equal(string(v), string(newValue))
}
