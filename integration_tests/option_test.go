// Copyright 2025 TiKV Authors
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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/lock_test.go
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
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

type testOptionSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testOptionSuite) SetupSuite() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
}

func (s *testOptionSuite) TearDownSuite() {
	s.store.Close()
}

func TestOption(t *testing.T) {
	suite.Run(t, new(testOptionSuite))
}

func (s *testOptionSuite) TestSetCommitWaitUntilTSO() {
	getTS1 := func(startTS uint64) uint64 { return 100 }
	getTS2 := func(startTS uint64) uint64 {
		return startTS + 200
	}
	getTS3 := func(startTS uint64) uint64 {
		now := oracle.GetTimeFromTS(startTS)
		return oracle.GoTimeToTS(now.Add(10 * time.Second))
	}
	type testCase struct {
		getTS   func(startTS uint64) uint64
		noError bool
	}

	for _, tc := range []testCase{
		{getTS1, true},
		{getTS2, true},
		{getTS3, false},
	} {
		txn, err := s.store.Begin()
		s.NoError(err)
		commitUntil := tc.getTS(txn.StartTS())
		txn.KVTxn.SetCommitWaitUntilTSO(commitUntil)
		s.NoError(txn.Set([]byte("somekey"), []byte("somevalue")))
		err = txn.Commit(context.Background())
		s.Equal(tc.noError, err == nil)

		if tc.noError {
			s.Greater(txn.CommitTS(), uint64(100))
		}
	}
}

func (s *testOptionSuite) TestSetCommitWaitUntilTSOTimeout() {
	txn, err := s.store.Begin()
	s.NoError(err)
	defer txn.Rollback()
	s.Equal(time.Second, txn.GetMaxClockDriftInActiveActiveReplication())
	txn.KVTxn.SetCommitWaitUntilTSOTimeout(2 * time.Second)
	s.Equal(2*time.Second, txn.GetMaxClockDriftInActiveActiveReplication())
}
