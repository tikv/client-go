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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/prewrite_test.go
//

// Copyright 2020 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/tikv/client-go/v2/tikv"
)

type testPrewriteSuite struct {
	store *tikv.KVStore
}

var _ = Suite(&testPrewriteSuite{})

func (s *testPrewriteSuite) SetUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testPrewriteSuite) TestSetMinCommitTSInAsyncCommit(c *C) {
	t, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn := tikv.TxnProbe{KVTxn: t}
	err = txn.Set([]byte("k"), []byte("v"))
	c.Assert(err, IsNil)
	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	committer.SetUseAsyncCommit()

	buildRequest := func() *kvrpcpb.PrewriteRequest {
		req := committer.BuildPrewriteRequest(1, 1, 1, committer.GetMutations(), 1)
		return req.Req.(*kvrpcpb.PrewriteRequest)
	}

	// no forUpdateTS
	req := buildRequest()
	c.Assert(req.MinCommitTs, Equals, txn.StartTS()+1)

	// forUpdateTS is set
	committer.SetForUpdateTS(txn.StartTS() + (5 << 18))
	req = buildRequest()
	c.Assert(req.MinCommitTs, Equals, committer.GetForUpdateTS()+1)

	// minCommitTS is set
	committer.SetMinCommitTS(txn.StartTS() + (10 << 18))
	req = buildRequest()
	c.Assert(req.MinCommitTs, Equals, committer.GetMinCommitTS())

}
