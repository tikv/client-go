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
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestSetMinCommitTSInAsyncCommit(t *testing.T) {
	require, assert := require.New(t), assert.New(t)

	client, pdClient, cluster, err := unistore.New("")
	require.Nil(err)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(err)

	tx, err := store.Begin()
	require.Nil(err)
	txn := tikv.TxnProbe{KVTxn: tx}
	err = txn.Set([]byte("k"), []byte("v"))
	assert.Nil(err)
	committer, err := txn.NewCommitter(1)
	assert.Nil(err)
	committer.SetUseAsyncCommit()

	buildRequest := func() *kvrpcpb.PrewriteRequest {
		req := committer.BuildPrewriteRequest(1, 1, 1, committer.GetMutations(), 1)
		return req.Req.(*kvrpcpb.PrewriteRequest)
	}

	// no forUpdateTS
	req := buildRequest()
	assert.Equal(req.MinCommitTs, txn.StartTS()+1)

	// forUpdateTS is set
	committer.SetForUpdateTS(txn.StartTS() + (5 << 18))
	req = buildRequest()
	assert.Equal(req.MinCommitTs, committer.GetForUpdateTS()+1)

	// minCommitTS is set
	committer.SetMinCommitTS(txn.StartTS() + (10 << 18))
	req = buildRequest()
	assert.Equal(req.MinCommitTs, committer.GetMinCommitTS())

}
