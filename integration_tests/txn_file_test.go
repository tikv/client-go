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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestTxnFilePrewriteTxnSize(t *testing.T) {
	require := require.New(t)
	const maxChunkSize = 1024

	var chunkIDCounter atomic.Uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := chunkIDCounter.Add(1)
		resp, _ := json.Marshal(map[string]uint64{"chunk_id": id})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVClient.TxnChunkWriterAddr = srv.Listener.Addr().String()
	newCfg.TiKVClient.TxnChunkMaxSize = maxChunkSize
	newCfg.TiKVClient.TxnFileMinMutationSize = 1
	config.StoreGlobalConfig(&newCfg)
	defer config.StoreGlobalConfig(origCfg)

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.Nil(err)
	_, _, regionID := testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(err)
	defer store.Close()

	type capturedPrewrite struct {
		txnFileChunks []uint64
		txnSize       uint64
	}
	var mu sync.Mutex
	var captured []capturedPrewrite

	hook := func(req *tikvrpc.Request) {
		if req.Type != tikvrpc.CmdPrewrite {
			return
		}
		inner := req.Req.(*kvrpcpb.PrewriteRequest)
		if len(inner.TxnFileChunks) == 0 {
			return
		}
		chunks := make([]uint64, len(inner.TxnFileChunks))
		copy(chunks, inner.TxnFileChunks)
		mu.Lock()
		captured = append(captured, capturedPrewrite{
			txnFileChunks: chunks,
			txnSize:       inner.TxnSize,
		})
		mu.Unlock()
	}

	require.Nil(failpoint.Enable("tikvclient/beforeSendReqToRegion", "return"))
	defer failpoint.Disable("tikvclient/beforeSendReqToRegion")
	ctx := context.WithValue(context.Background(), "sendReqToRegionHook", hook)

	commitTxn := func(keys [][]byte) {
		tx, err := store.Begin()
		require.Nil(err)
		txn := transaction.TxnProbe{KVTxn: tx}

		vars := *kv.DefaultVars
		vars.TxnFileMinMutationSize = 1
		txn.SetVars(&vars)

		for _, key := range keys {
			val := make([]byte, 64)
			require.Nil(txn.Set(key, val))
		}

		// The mock environment is only used to inspect outgoing txn-file prewrite requests.
		// Commit may fail later because the mock stack does not fully model txn-file follow-up behavior.
		_ = txn.Commit(ctx)
	}

	assertCaptured := func(expectedRequests int, expectedTxnSize uint64) {
		mu.Lock()
		defer mu.Unlock()
		require.GreaterOrEqual(len(captured), expectedRequests)
		for _, c := range captured {
			require.NotEmpty(c.txnFileChunks)
			require.Equal(expectedTxnSize, c.txnSize)
		}
	}

	// Single-region case: exact txn size should match the mutation count.
	commitTxn([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")})
	assertCaptured(1, 5)

	// Split the single region into two. With the default large chunk size, one chunk spans both
	// regions, so each region batch should conservatively reuse the full chunk entry count.
	newRegionID := cluster.AllocID()
	newPeerID := cluster.AllocID()
	cluster.Split(regionID, newRegionID, []byte("m"), []uint64{newPeerID}, newPeerID)

	mu.Lock()
	captured = nil
	mu.Unlock()

	commitTxn([][]byte{[]byte("a"), []byte("b"), []byte("x"), []byte("y"), []byte("z")})
	assertCaptured(2, 5)
}

func TestTxnFilePrewriteTxnSizeAfterRegionRegroup(t *testing.T) {
	require := require.New(t)
	const maxChunkSize = 1024

	var chunkIDCounter atomic.Uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := chunkIDCounter.Add(1)
		resp, _ := json.Marshal(map[string]uint64{"chunk_id": id})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVClient.TxnChunkWriterAddr = srv.Listener.Addr().String()
	newCfg.TiKVClient.TxnChunkMaxSize = maxChunkSize
	newCfg.TiKVClient.TxnFileMinMutationSize = 1
	config.StoreGlobalConfig(&newCfg)
	defer config.StoreGlobalConfig(origCfg)

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.Nil(err)
	_, peerID, regionID := testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(err)
	defer store.Close()

	type capturedPrewrite struct {
		txnFileChunks []uint64
		txnSize       uint64
		isRetry       bool
		regionErr     bool
	}
	var mu sync.Mutex
	var captured []capturedPrewrite

	hook := func(req *tikvrpc.Request, resp *tikvrpc.Response, sendErr error) {
		if req.Type != tikvrpc.CmdPrewrite {
			return
		}
		inner, ok := req.Req.(*kvrpcpb.PrewriteRequest)
		if !ok || len(inner.TxnFileChunks) == 0 {
			return
		}
		if sendErr != nil {
			return
		}
		chunks := make([]uint64, len(inner.TxnFileChunks))
		copy(chunks, inner.TxnFileChunks)
		var regionErr bool
		if resp != nil {
			if respRegionErr, err := resp.GetRegionError(); err == nil && respRegionErr != nil {
				regionErr = true
			}
		}
		mu.Lock()
		captured = append(captured, capturedPrewrite{
			txnFileChunks: chunks,
			txnSize:       inner.TxnSize,
			isRetry:       req.Context.IsRetryRequest,
			regionErr:     regionErr,
		})
		mu.Unlock()
	}

	require.Nil(failpoint.Enable("tikvclient/mockRetrySendReqToRegion", "1*return(true)->return(false)"))
	defer failpoint.Disable("tikvclient/mockRetrySendReqToRegion")
	require.Nil(failpoint.Enable("tikvclient/invalidCacheAndRetry", "1*off->pause"))
	defer failpoint.Disable("tikvclient/invalidCacheAndRetry")
	require.Nil(failpoint.Enable("tikvclient/afterSendReqToRegion", "return"))
	defer failpoint.Disable("tikvclient/afterSendReqToRegion")
	ctx := context.WithValue(context.Background(), "sendReqToRegionFinishHook", hook)

	tx, err := store.Begin()
	require.Nil(err)
	txn := transaction.TxnProbe{KVTxn: tx}

	vars := *kv.DefaultVars
	vars.TxnFileMinMutationSize = 1
	txn.SetVars(&vars)

	for _, key := range [][]byte{[]byte("a"), []byte("z")} {
		val := make([]byte, 64)
		require.Nil(txn.Set(key, val))
	}

	done := make(chan struct{})
	go func() {
		_ = txn.Commit(ctx)
		close(done)
	}()

	time.Sleep(3 * time.Second)
	cluster.Split(regionID, cluster.AllocID(), []byte("h"), []uint64{peerID}, peerID)
	require.Nil(failpoint.Disable("tikvclient/invalidCacheAndRetry"))
	<-done

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(len(captured), 4, "expected initial send, stale-region retry, and regrouped region requests")
	regionErrRetries := 0
	successfulRetryPrewrites := 0
	for _, c := range captured {
		require.NotEmpty(c.txnFileChunks)
		require.Equal(uint64(2), c.txnSize)
		if c.isRetry && c.regionErr {
			regionErrRetries++
		}
		if c.isRetry && !c.regionErr {
			successfulRetryPrewrites++
		}
	}
	require.GreaterOrEqual(regionErrRetries, 1, "expected a retry-marked txn-file prewrite to hit a region error after the split")
	require.GreaterOrEqual(successfulRetryPrewrites, 2, "expected regrouped retry prewrites to reach both post-split regions")
}
