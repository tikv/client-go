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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/safepoint_test.go
//

// Copyright 2017 PingCAP, Inc.
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
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestSafepoint(t *testing.T) {
	suite.Run(t, new(testSafePointSuite))
}

type testSafePointSuite struct {
	suite.Suite
	store  tikv.StoreProbe
	prefix string
}

func (s *testSafePointSuite) SetupSuite() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
}

func (s *testSafePointSuite) TearDownSuite() {
	err := s.store.Close()
	s.Require().Nil(err)
}

func (s *testSafePointSuite) beginTxn() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func mymakeKeys(rowNum int, prefix string) [][]byte {
	keys := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSafePointSuite) waitUntilErrorPlugIn(t uint64) {
	for {
		s.store.SaveSafePoint(t + 10)
		cachedTime := time.Now()
		newSafePoint, err := s.store.LoadSafePoint()
		if err == nil {
			s.store.UpdateSPCache(newSafePoint, cachedTime)
			break
		}
		time.Sleep(time.Second)
	}
}

func (s *testSafePointSuite) TestSafePoint() {
	txn := s.beginTxn()
	for i := 0; i < 10; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		s.Nil(err)
	}
	err := txn.Commit(context.Background())
	s.Nil(err)

	// for txn get
	txn2 := s.beginTxn()
	_, err = txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	s.Nil(err)

	s.waitUntilErrorPlugIn(txn2.StartTS())

	_, geterr2 := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	s.NotNil(geterr2)

	_, isFallBehind := errors.Cause(geterr2).(*error.ErrGCTooEarly)
	isMayFallBehind := strings.Contains(geterr2.Error(), "start timestamp may fall behind safe point")
	isBehind := isFallBehind || isMayFallBehind
	s.True(isBehind)

	// for txn seek
	txn3 := s.beginTxn()

	s.waitUntilErrorPlugIn(txn3.StartTS())

	_, seekerr := txn3.Iter(encodeKey(s.prefix, ""), nil)
	s.NotNil(seekerr)
	_, isFallBehind = errors.Cause(geterr2).(*error.ErrGCTooEarly)
	isMayFallBehind = strings.Contains(geterr2.Error(), "start timestamp may fall behind safe point")
	isBehind = isFallBehind || isMayFallBehind
	s.True(isBehind)

	// for snapshot batchGet
	keys := mymakeKeys(10, s.prefix)
	txn4 := s.beginTxn()

	s.waitUntilErrorPlugIn(txn4.StartTS())

	_, batchgeterr := toTiDBTxn(&txn4).BatchGet(context.Background(), toTiDBKeys(keys))
	s.NotNil(batchgeterr)
	_, isFallBehind = errors.Cause(geterr2).(*error.ErrGCTooEarly)
	isMayFallBehind = strings.Contains(geterr2.Error(), "start timestamp may fall behind safe point")
	isBehind = isFallBehind || isMayFallBehind
	s.True(isBehind)
}
