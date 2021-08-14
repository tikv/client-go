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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/scan_mock_test.go
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/tikv"
)

func TestScanMock(t *testing.T) {
	suite.Run(t, new(testScanMockSuite))
}

type testScanMockSuite struct {
	suite.Suite
}

func (s *testScanMockSuite) TestScanMultipleRegions() {
	store := tikv.StoreProbe{KVStore: NewTestStore(s.T())}
	defer store.Close()

	txn, err := store.Begin()
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch})
		s.Nil(err)
	}
	err = txn.Commit(context.Background())
	s.Nil(err)

	txn, err = store.Begin()
	s.Nil(err)
	scanner, err := txn.NewScanner([]byte("a"), nil, 10, false)
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.Equal([]byte{ch}, scanner.Key())
		s.Nil(scanner.Next())
	}
	s.False(scanner.Valid())

	scanner, err = txn.NewScanner([]byte("a"), []byte("i"), 10, false)
	s.Nil(err)
	for ch := byte('a'); ch <= byte('h'); ch++ {
		s.Equal([]byte{ch}, scanner.Key())
		s.Nil(scanner.Next())
	}
	s.False(scanner.Valid())
}

func (s *testScanMockSuite) TestReverseScan() {
	store := tikv.StoreProbe{KVStore: NewTestStore(s.T())}
	defer store.Close()

	txn, err := store.Begin()
	s.Nil(err)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch})
		s.Nil(err)
	}
	err = txn.Commit(context.Background())
	s.Nil(err)

	txn, err = store.Begin()
	s.Nil(err)
	scanner, err := txn.NewScanner(nil, []byte("z"), 10, true)
	s.Nil(err)
	for ch := byte('y'); ch >= byte('a'); ch-- {
		s.Equal(string([]byte{ch}), string(scanner.Key()))
		s.Nil(scanner.Next())
	}
	s.False(scanner.Valid())

	scanner, err = txn.NewScanner([]byte("a"), []byte("i"), 10, true)
	s.Nil(err)
	for ch := byte('h'); ch >= byte('a'); ch-- {
		s.Equal(string([]byte{ch}), string(scanner.Key()))
		s.Nil(scanner.Next())
	}
	s.False(scanner.Valid())
}
