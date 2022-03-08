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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/ticlient_test.go
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

package tikv_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ninedraft/israce"
	"github.com/stretchr/testify/suite"
	kverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
)

func TestTiclient(t *testing.T) {
	suite.Run(t, new(testTiclientSuite))
}

type testTiclientSuite struct {
	suite.Suite
	store *tikv.KVStore
	// prefix is prefix of each key in this test. It is used for table isolation,
	// or it may pollute other data.
	prefix string
}

func (s *testTiclientSuite) SetupSuite() {
	s.store = NewTestStore(s.T())
	s.prefix = fmt.Sprintf("ticlient_%d", time.Now().Unix())
}

func (s *testTiclientSuite) TearDownSuite() {
	// Clean all data, or it may pollute other data.
	txn := s.beginTxn()
	scanner, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	s.Require().Nil(err)
	s.Require().NotNil(scanner)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		s.Require().Nil(err)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	s.Require().Nil(err)
	err = s.store.Close()
	s.Require().Nil(err)
}

func (s *testTiclientSuite) beginTxn() *tikv.KVTxn {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func (s *testTiclientSuite) TestSingleKey() {
	txn := s.beginTxn()
	err := txn.Set(encodeKey(s.prefix, "key"), []byte("value"))
	s.Nil(err)
	err = txn.LockKeys(context.Background(), new(tikvstore.LockCtx), encodeKey(s.prefix, "key"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)

	txn = s.beginTxn()
	val, err := txn.Get(context.TODO(), encodeKey(s.prefix, "key"))
	s.Nil(err)
	s.Equal(val, []byte("value"))

	txn = s.beginTxn()
	err = txn.Delete(encodeKey(s.prefix, "key"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testTiclientSuite) TestMultiKeys() {
	const keyNum = 100

	txn := s.beginTxn()
	for i := 0; i < keyNum; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		s.Nil(err)
	}
	err := txn.Commit(context.Background())
	s.Nil(err)

	txn = s.beginTxn()
	for i := 0; i < keyNum; i++ {
		val, err1 := txn.Get(context.TODO(), encodeKey(s.prefix, s08d("key", i)))
		s.Nil(err1)
		s.Equal(val, valueBytes(i))
	}

	txn = s.beginTxn()
	for i := 0; i < keyNum; i++ {
		err = txn.Delete(encodeKey(s.prefix, s08d("key", i)))
		s.Nil(err)
	}
	err = txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testTiclientSuite) TestNotExist() {
	txn := s.beginTxn()
	_, err := txn.Get(context.TODO(), encodeKey(s.prefix, "noSuchKey"))
	s.NotNil(err)
}

func (s *testTiclientSuite) TestLargeRequest() {
	largeValue := make([]byte, 9*1024*1024) // 9M value.
	txn := s.beginTxn()
	txn.GetUnionStore().SetEntrySizeLimit(1024*1024, 100*1024*1024)
	err := txn.Set([]byte("key"), largeValue)
	s.NotNil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)
	var retryableErr *kverr.ErrRetryable
	s.False(errors.As(err, &retryableErr))
}

func (s *testTiclientSuite) TestSplitRegionIn2PC() {
	if *withTiKV {
		s.T().Skip("scatter will timeout with single node TiKV")
	}
	if israce.Race {
		s.T().Skip("skip slow test when race is enabled")
	}

	config := tikv.ConfigProbe{}
	const preSplitThresholdInTest = 500
	old := config.LoadPreSplitDetectThreshold()
	defer config.StorePreSplitDetectThreshold(old)
	config.StorePreSplitDetectThreshold(preSplitThresholdInTest)

	old = config.LoadPreSplitSizeThreshold()
	defer config.StorePreSplitSizeThreshold(old)
	config.StorePreSplitSizeThreshold(5000)

	bo := tikv.NewBackofferWithVars(context.Background(), 1, nil)
	checkKeyRegion := func(bo *tikv.Backoffer, start, end []byte, eq bool) {
		// Check regions after split.
		loc1, err := s.store.GetRegionCache().LocateKey(bo, start)
		s.Nil(err)
		loc2, err := s.store.GetRegionCache().LocateKey(bo, end)
		s.Nil(err)
		s.Equal(loc1.Region.GetID() == loc2.Region.GetID(), eq)
	}
	mode := []string{"optimistic", "pessimistic"}
	var (
		startKey []byte
		endKey   []byte
	)
	ctx := context.Background()
	for _, m := range mode {
		if m == "optimistic" {
			startKey = encodeKey(s.prefix, s08d("key", 0))
			endKey = encodeKey(s.prefix, s08d("key", preSplitThresholdInTest))
		} else {
			startKey = encodeKey(s.prefix, s08d("pkey", 0))
			endKey = encodeKey(s.prefix, s08d("pkey", preSplitThresholdInTest))
		}
		// Check before test.
		checkKeyRegion(bo, startKey, endKey, true)
		txn := s.beginTxn()
		if m == "pessimistic" {
			txn.SetPessimistic(true)
			lockCtx := &tikvstore.LockCtx{}
			lockCtx.ForUpdateTS = txn.StartTS()
			keys := make([][]byte, 0, preSplitThresholdInTest)
			for i := 0; i < preSplitThresholdInTest; i++ {
				keys = append(keys, encodeKey(s.prefix, s08d("pkey", i)))
			}
			err := txn.LockKeys(ctx, lockCtx, keys...)
			s.Nil(err)
			checkKeyRegion(bo, startKey, endKey, false)
		}
		var err error
		for i := 0; i < preSplitThresholdInTest; i++ {
			if m == "optimistic" {
				err = txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
			} else {
				err = txn.Set(encodeKey(s.prefix, s08d("pkey", i)), valueBytes(i))
			}
			s.Nil(err)
		}
		err = txn.Commit(context.Background())
		s.Nil(err)
		// Check region split after test.
		checkKeyRegion(bo, startKey, endKey, false)
	}
}
