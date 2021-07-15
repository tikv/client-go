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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/delete_range_test.go
//

// Copyright 2018 PingCAP, Inc.
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
	"math/rand"
	"sort"
	"testing"

	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestDeleteRange(t *testing.T) {
	suite.Run(t, new(testDeleteRangeSuite))
}

type testDeleteRangeSuite struct {
	suite.Suite
	cluster testutils.Cluster
	store   *tikv.KVStore
}

func (s *testDeleteRangeSuite) SetupTest() {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	s.Require().Nil(err)
	testutils.BootstrapWithMultiRegions(cluster, []byte("b"), []byte("c"), []byte("d"))
	s.cluster = cluster
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	s.Require().Nil(err)

	s.store = store
}

func (s *testDeleteRangeSuite) TearDownTest() {
	err := s.store.Close()
	s.Require().Nil(err)
}

func (s *testDeleteRangeSuite) checkData(expectedData map[string]string) {
	txn, err := s.store.Begin()
	s.Nil(err)
	it, err := txn.Iter([]byte("a"), nil)
	s.Nil(err)

	// Scan all data and save into a map
	data := map[string]string{}
	for it.Valid() {
		data[string(it.Key())] = string(it.Value())
		err = it.Next()
		s.Nil(err)
	}
	err = txn.Commit(context.Background())
	s.Nil(err)

	// Print log
	actualKeys := make([]string, 0, len(data))
	expectedKeys := make([]string, 0, len(expectedData))
	for key := range data {
		actualKeys = append(actualKeys, key)
	}
	for key := range expectedData {
		expectedKeys = append(expectedKeys, key)
	}
	sort.Strings(actualKeys)
	sort.Strings(expectedKeys)
	s.T().Log("Actual:   ", actualKeys)
	s.T().Log("Expected: ", expectedKeys)

	// Assert data in the store is the same as expected
	s.Equal(data, expectedData)
}

func (s *testDeleteRangeSuite) deleteRange(startKey []byte, endKey []byte) int {
	completedRegions, err := s.store.DeleteRange(context.Background(), startKey, endKey, 1)
	s.Nil(err)

	return completedRegions
}

// deleteRangeFromMap deletes all keys in a given range from a map
func deleteRangeFromMap(m map[string]string, startKey []byte, endKey []byte) {
	for keyStr := range m {
		key := []byte(keyStr)
		if bytes.Compare(startKey, key) <= 0 && bytes.Compare(key, endKey) < 0 {
			delete(m, keyStr)
		}
	}
}

// mustDeleteRange does delete range on both the map and the storage, and assert they are equal after deleting
func (s *testDeleteRangeSuite) mustDeleteRange(startKey []byte, endKey []byte, expected map[string]string, regions int) {
	completedRegions := s.deleteRange(startKey, endKey)
	deleteRangeFromMap(expected, startKey, endKey)
	s.checkData(expected)
	s.Equal(completedRegions, regions)
}

func (s *testDeleteRangeSuite) TestDeleteRange() {
	// Write some key-value pairs
	txn, err := s.store.Begin()
	s.Nil(err)

	testData := map[string]string{}

	// Generate a sequence of keys and random values
	for _, i := range []byte("abcd") {
		for j := byte('0'); j <= byte('9'); j++ {
			key := []byte{i, j}
			value := []byte{byte(rand.Intn(256)), byte(rand.Intn(256))}
			testData[string(key)] = string(value)
			err := txn.Set(key, value)
			s.Nil(err)
		}
	}

	err = txn.Commit(context.Background())
	s.Nil(err)

	s.checkData(testData)

	s.mustDeleteRange([]byte("b"), []byte("c0"), testData, 2)
	s.mustDeleteRange([]byte("d0"), []byte("d0"), testData, 0)
	s.mustDeleteRange([]byte("d0\x00"), []byte("d1\x00"), testData, 1)
	s.mustDeleteRange([]byte("c5"), []byte("d5"), testData, 2)
	s.mustDeleteRange([]byte("a"), []byte("z"), testData, 4)
}
