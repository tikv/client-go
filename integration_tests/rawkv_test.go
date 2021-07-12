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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/rawkv_test.go
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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRawKV(t *testing.T) {
	suite.Run(t, new(testRawKVSuite))
}

type testRawKVSuite struct {
	suite.Suite
	cluster testutils.Cluster
	client  rawkv.ClientProbe
	bo      *tikv.Backoffer
}

func (s *testRawKVSuite) SetupTest() {
	client, pdClient, cluster, err := unistore.New("")
	s.Require().Nil(err)
	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	s.client = rawkv.ClientProbe{Client: &rawkv.Client{}}
	s.client.SetPDClient(pdClient)
	s.client.SetRegionCache(tikv.NewRegionCache(pdClient))
	s.client.SetRPCClient(client)
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRawKVSuite) TearDownTest() {
	s.client.Close()
}

func (s *testRawKVSuite) mustNotExist(key []byte) {
	v, err := s.client.Get(context.Background(), key)
	s.Nil(err)
	s.Nil(v)
}

func (s *testRawKVSuite) mustBatchNotExist(keys [][]byte) {
	values, err := s.client.BatchGet(context.Background(), keys)
	s.Nil(err)
	s.NotNil(values)
	s.Equal(len(keys), len(values))
	for _, value := range values {
		s.Len(value, 0)
	}
}

func (s *testRawKVSuite) mustGet(key, value []byte) {
	v, err := s.client.Get(context.Background(), key)
	s.Nil(err)
	s.NotNil(v)
	s.Equal(v, value)
}

func (s *testRawKVSuite) mustBatchGet(keys, values [][]byte) {
	checkValues, err := s.client.BatchGet(context.Background(), keys)
	s.Nil(err)
	s.NotNil(checkValues)
	s.Equal(len(keys), len(checkValues))
	for i := range keys {
		s.Equal(values[i], checkValues[i])
	}
}

func (s *testRawKVSuite) mustPut(key, value []byte) {
	err := s.client.Put(context.Background(), key, value)
	s.Nil(err)
}

func (s *testRawKVSuite) mustBatchPut(keys, values [][]byte) {
	err := s.client.BatchPut(context.Background(), keys, values)
	s.Nil(err)
}

func (s *testRawKVSuite) mustDelete(key []byte) {
	err := s.client.Delete(context.Background(), key)
	s.Nil(err)
}

func (s *testRawKVSuite) mustBatchDelete(keys [][]byte) {
	err := s.client.BatchDelete(context.Background(), keys)
	s.Nil(err)
}

func (s *testRawKVSuite) mustScan(startKey string, limit int, expect ...string) {
	keys, values, err := s.client.Scan(context.Background(), []byte(startKey), nil, limit)
	s.Nil(err)
	s.Equal(len(keys)*2, len(expect))
	for i := range keys {
		s.Equal(string(keys[i]), expect[i*2])
		s.Equal(string(values[i]), expect[i*2+1])
	}
}

func (s *testRawKVSuite) mustScanRange(startKey string, endKey string, limit int, expect ...string) {
	keys, values, err := s.client.Scan(context.Background(), []byte(startKey), []byte(endKey), limit)
	s.Nil(err)
	s.Equal(len(keys)*2, len(expect))
	for i := range keys {
		s.Equal(string(keys[i]), expect[i*2])
		s.Equal(string(values[i]), expect[i*2+1])
	}
}

func (s *testRawKVSuite) mustReverseScan(startKey []byte, limit int, expect ...string) {
	keys, values, err := s.client.ReverseScan(context.Background(), startKey, nil, limit)
	s.Nil(err)
	s.Equal(len(keys)*2, len(expect))
	for i := range keys {
		s.Equal(string(keys[i]), expect[i*2])
		s.Equal(string(values[i]), expect[i*2+1])
	}
}

func (s *testRawKVSuite) mustReverseScanRange(startKey, endKey []byte, limit int, expect ...string) {
	keys, values, err := s.client.ReverseScan(context.Background(), startKey, endKey, limit)
	s.Nil(err)
	s.Equal(len(keys)*2, len(expect))
	for i := range keys {
		s.Equal(string(keys[i]), expect[i*2])
		s.Equal(string(values[i]), expect[i*2+1])
	}
}

func (s *testRawKVSuite) mustDeleteRange(startKey, endKey []byte, expected map[string]string) {
	err := s.client.DeleteRange(context.Background(), startKey, endKey)
	s.Nil(err)

	for keyStr := range expected {
		key := []byte(keyStr)
		if bytes.Compare(startKey, key) <= 0 && bytes.Compare(key, endKey) < 0 {
			delete(expected, keyStr)
		}
	}

	s.checkData(expected)
}

func (s *testRawKVSuite) checkData(expected map[string]string) {
	keys, values, err := s.client.Scan(context.Background(), []byte(""), nil, len(expected)+1)
	s.Nil(err)

	s.Equal(len(expected), len(keys))
	for i, key := range keys {
		s.Equal(expected[string(key)], string(values[i]))
	}
}

func (s *testRawKVSuite) split(regionKey, splitKey string) error {
	loc, err := s.client.GetRegionCache().LocateKey(s.bo, []byte(regionKey))
	if err != nil {
		return err
	}

	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.SplitRaw(loc.Region.GetID(), newRegionID, []byte(splitKey), []uint64{peerID}, peerID)
	return nil
}

func (s *testRawKVSuite) TestSimple() {
	s.mustNotExist([]byte("key"))
	s.mustPut([]byte("key"), []byte("value"))
	s.mustGet([]byte("key"), []byte("value"))
	s.mustDelete([]byte("key"))
	s.mustNotExist([]byte("key"))
	err := s.client.Put(context.Background(), []byte("key"), []byte(""))
	s.NotNil(err)
}

func (s *testRawKVSuite) TestRawBatch() {
	testNum := 0
	size := 0
	var testKeys [][]byte
	var testValues [][]byte
	for i := 0; size/(rawkv.ConfigProbe{}.GetRawBatchPutSize()) < 4; i++ {
		key := fmt.Sprint("key", i)
		size += len(key)
		testKeys = append(testKeys, []byte(key))
		value := fmt.Sprint("value", i)
		size += len(value)
		testValues = append(testValues, []byte(value))
		s.mustNotExist([]byte(key))
		testNum = i
	}
	err := s.split("", fmt.Sprint("key", testNum/2))
	s.Nil(err)
	s.mustBatchPut(testKeys, testValues)
	s.mustBatchGet(testKeys, testValues)
	s.mustBatchDelete(testKeys)
	s.mustBatchNotExist(testKeys)
}

func (s *testRawKVSuite) TestSplit() {
	s.mustPut([]byte("k1"), []byte("v1"))
	s.mustPut([]byte("k3"), []byte("v3"))

	err := s.split("k", "k2")
	s.Nil(err)

	s.mustGet([]byte("k1"), []byte("v1"))
	s.mustGet([]byte("k3"), []byte("v3"))
}

func (s *testRawKVSuite) TestScan() {
	s.mustPut([]byte("k1"), []byte("v1"))
	s.mustPut([]byte("k3"), []byte("v3"))
	s.mustPut([]byte("k5"), []byte("v5"))
	s.mustPut([]byte("k7"), []byte("v7"))

	check := func() {
		s.mustScan("", 1, "k1", "v1")
		s.mustScan("k1", 2, "k1", "v1", "k3", "v3")
		s.mustScan("", 10, "k1", "v1", "k3", "v3", "k5", "v5", "k7", "v7")
		s.mustScan("k2", 2, "k3", "v3", "k5", "v5")
		s.mustScan("k2", 3, "k3", "v3", "k5", "v5", "k7", "v7")
		s.mustScanRange("", "k1", 1)
		s.mustScanRange("k1", "k3", 2, "k1", "v1")
		s.mustScanRange("k1", "k5", 10, "k1", "v1", "k3", "v3")
		s.mustScanRange("k1", "k5\x00", 10, "k1", "v1", "k3", "v3", "k5", "v5")
		s.mustScanRange("k5\x00", "k5\x00\x00", 10)
	}

	check()

	err := s.split("k", "k2")
	s.Nil(err)
	check()

	err = s.split("k2", "k5")
	s.Nil(err)
	check()
}

func (s *testRawKVSuite) TestReverseScan() {
	s.mustPut([]byte("k1"), []byte("v1"))
	s.mustPut([]byte("k3"), []byte("v3"))
	s.mustPut([]byte("k5"), []byte("v5"))
	s.mustPut([]byte("k7"), []byte("v7"))

	s.checkReverseScan()

	err := s.split("k", "k2")
	s.Nil(err)
	s.checkReverseScan()

	err = s.split("k2", "k5")
	s.Nil(err)
	s.checkReverseScan()
}

func (s *testRawKVSuite) checkReverseScan() {
	s.mustReverseScan([]byte(""), 10)
	s.mustReverseScan([]byte("z"), 1, "k7", "v7")
	s.mustReverseScan([]byte("z"), 2, "k7", "v7", "k5", "v5")
	s.mustReverseScan([]byte("z"), 10, "k7", "v7", "k5", "v5", "k3", "v3", "k1", "v1")
	s.mustReverseScan([]byte("k2"), 10, "k1", "v1")
	s.mustReverseScan([]byte("k6"), 2, "k5", "v5", "k3", "v3")
	s.mustReverseScan([]byte("k5"), 1, "k3", "v3")
	s.mustReverseScan(append([]byte("k5"), 0), 1, "k5", "v5")
	s.mustReverseScan([]byte("k6"), 3, "k5", "v5", "k3", "v3", "k1", "v1")

	s.mustReverseScanRange([]byte("z"), []byte("k3"), 10, "k7", "v7", "k5", "v5", "k3", "v3")
	s.mustReverseScanRange([]byte("k7"), append([]byte("k3"), 0), 10, "k5", "v5")
}

func (s *testRawKVSuite) TestDeleteRange() {
	// Init data
	testData := map[string]string{}
	for _, i := range []byte("abcd") {
		for j := byte('0'); j <= byte('9'); j++ {
			key := []byte{i, j}
			value := []byte{'v', i, j}
			s.mustPut(key, value)

			testData[string(key)] = string(value)
		}
	}

	err := s.split("b", "b")
	s.Nil(err)
	err = s.split("c", "c")
	s.Nil(err)
	err = s.split("d", "d")
	s.Nil(err)

	s.checkData(testData)
	s.mustDeleteRange([]byte("b"), []byte("c0"), testData)
	s.mustDeleteRange([]byte("c11"), []byte("c12"), testData)
	s.mustDeleteRange([]byte("d0"), []byte("d0"), testData)
	s.mustDeleteRange([]byte("c5"), []byte("d5"), testData)
	s.mustDeleteRange([]byte("a"), []byte("z"), testData)
}
