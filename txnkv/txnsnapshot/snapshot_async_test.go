// Copyright 2026 TiKV Authors
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

package txnsnapshot_test

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
)

type pauseBatchGetRetryClient struct {
	tikv.Client

	injectedLock atomic.Bool
	retryStarted chan struct{}
	releaseRetry chan struct{}
}

func (c *pauseBatchGetRetryClient) SendRequest(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	// The initial requests use SendRequestAsync. A synchronous BatchGet here is
	// therefore the retry worker spawned after the injected lock response.
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if req.Type != tikvrpc.CmdBatchGet {
		return resp, err
	}

	close(c.retryStarted)
	<-c.releaseRetry
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return resp, err
}

func (c *pauseBatchGetRetryClient) SendRequestAsync(
	ctx context.Context,
	addr string,
	req *tikvrpc.Request,
	cb async.Callback[*tikvrpc.Response],
) {
	go func() {
		resp, err := c.Client.SendRequest(ctx, addr, req, 0)
		// Force one initial Region response through the lock-retry path without
		// creating a real lock. The expired synthetic lock is resolved by the
		// mock store before the retry reaches SendRequest above.
		if err == nil && req.Type == tikvrpc.CmdBatchGet && c.injectedLock.CompareAndSwap(false, true) {
			key := req.BatchGet().Keys[0]
			resp.Resp.(*kvrpcpb.BatchGetResponse).Error = &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
				PrimaryLock: key,
				LockVersion: 1,
				Key:         key,
				LockTtl:     0,
				LockType:    kvrpcpb.Op_Put,
			}}
		}
		cb.Schedule(resp, err)
	}()
}

func TestAsyncBatchGetCancellationWaitsForRetryWorker(t *testing.T) {
	restoreConfig := config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableAsyncBatchGet = true
	})
	defer restoreConfig()

	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithMultiRegions(cluster, []byte("m"))

	var pausedClient *pauseBatchGetRetryClient
	store, err := tikv.NewTestTiKVStore(client, pdClient, func(client tikv.Client) tikv.Client {
		pausedClient = &pauseBatchGetRetryClient{
			Client:       client,
			retryStarted: make(chan struct{}),
			releaseRetry: make(chan struct{}),
		}
		return pausedClient
	}, nil, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	var releaseRetry sync.Once
	releaseWorker := func() {
		releaseRetry.Do(func() { close(pausedClient.releaseRetry) })
	}
	defer releaseWorker()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := store.GetSnapshot(math.MaxUint64).BatchGet(ctx, [][]byte{[]byte("a"), []byte("z")})
		result <- err
	}()

	select {
	case <-pausedClient.retryStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("async BatchGet did not start a retry worker")
	}

	cancel()
	var batchGetErr error
	returnedBeforeWorkerExit := false
	// A correct cancellation path must remain blocked on the paused retry
	// worker. The affected implementation returns immediately instead.
	select {
	case batchGetErr = <-result:
		returnedBeforeWorkerExit = true
	case <-time.After(100 * time.Millisecond):
	}

	releaseWorker()
	if !returnedBeforeWorkerExit {
		select {
		case batchGetErr = <-result:
		case <-time.After(5 * time.Second):
			t.Fatal("async BatchGet did not return after its retry worker exited")
		}
	}

	require.ErrorIs(t, batchGetErr, context.Canceled)
	require.False(t, returnedBeforeWorkerExit, "async BatchGet returned while its retry worker was still running")
}
