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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestSetMinCommitTSInAsyncCommit(t *testing.T) {
	require, assert := require.New(t), assert.New(t)

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.Nil(err)
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(err)
	defer store.Close()

	tx, err := store.Begin()
	require.Nil(err)
	txn := transaction.TxnProbe{KVTxn: tx}
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

// TestIsRetryRequestFlagWithRegionError tests that the is_retry_request flag is true for all retrying prewrite requests.
func TestIsRetryRequestFlagWithRegionError(t *testing.T) {
	require := require.New(t)

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.Nil(err)
	_, peerID, regionID := testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(err)
	defer store.Close()

	failpoint.Enable("tikvclient/mockRetrySendReqToRegion", "1*return(true)->return(false)")
	defer failpoint.Disable("tikvclient/mockRetrySendReqToRegion")
	failpoint.Enable("tikvclient/invalidCacheAndRetry", "1*off->pause")

	// the history of this field
	isRetryRequest := make([]bool, 0, 0)
	var mu sync.Mutex
	hook := func(req *tikvrpc.Request) {
		if req.Type != tikvrpc.CmdPrewrite {
			return
		}
		if req != nil {
			mu.Lock()
			isRetryRequest = append(isRetryRequest, req.Context.IsRetryRequest)
			mu.Unlock()
		}
	}
	failpoint.Enable("tikvclient/beforeSendReqToRegion", "return")
	defer failpoint.Disable("tikvclient/beforeSendReqToRegion")
	ctx := context.WithValue(context.TODO(), "sendReqToRegionHook", hook)

	tx, err := store.Begin()
	require.Nil(err)
	txn := transaction.TxnProbe{KVTxn: tx}
	err = txn.Set([]byte("a"), []byte("v"))
	require.Nil(err)
	err = txn.Set([]byte("z"), []byte("v"))
	require.Nil(err)
	committer, err := txn.NewCommitter(1)
	require.Nil(err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		committer.PrewriteAllMutations(ctx)
		wg.Done()
	}()
	time.Sleep(time.Second * 3)
	cluster.Split(regionID, cluster.AllocID(), []byte("h"), []uint64{peerID}, peerID)
	failpoint.Disable("tikvclient/invalidCacheAndRetry")
	wg.Wait()

	// The event history should be:
	// 1. The first prewrite succeeds in TiKV, but due to some reason, client-go doesn't get the response. We inject a retry to simulate it.
	// 2. The second prewrite request returns a region error, which is caused by the region split. And it retries.
	// 3. The third and fourth prewrite requests (for 'a' and 'z' respectively, so there are 2 of them) succeed.
	//
	// The last three requests are retry requests, we assert the is_retry_request flags are all true.
	require.Equal([]bool{false, true, true, true}, isRetryRequest)
}
