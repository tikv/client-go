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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/client_fail_test.go
//

// Copyright 2019 PingCAP, Inc.
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

package client

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestPanicInRecvLoop(t *testing.T) {
	require.Nil(t, failpoint.Enable("tikvclient/panicInFailPendingRequests", `panic`))
	require.Nil(t, failpoint.Enable("tikvclient/gotErrorInRecvLoop", `return("0")`))

	server, port := startMockTikvService()
	require.True(t, port > 0)
	defer server.Stop()

	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)
	rpcClient := NewRPCClient()
	rpcClient.dialTimeout = time.Second / 3

	// Start batchRecvLoop, and it should panic in `failPendingRequests`.
	_, err := rpcClient.getConnArray(addr, true, func(cfg *config.TiKVClient) { cfg.GrpcConnectionCount = 1 })
	assert.Nil(t, err, "cannot establish local connection due to env problems(e.g. heavy load in test machine), please retry again")

	req := tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second/2)
	assert.NotNil(t, err)

	require.Nil(t, failpoint.Disable("tikvclient/gotErrorInRecvLoop"))
	require.Nil(t, failpoint.Disable("tikvclient/panicInFailPendingRequests"))
	time.Sleep(time.Second * 2)

	req = tikvrpc.NewRequest(tikvrpc.CmdEmpty, &tikvpb.BatchCommandsEmptyRequest{})
	_, err = rpcClient.SendRequest(context.Background(), addr, req, time.Second*4)
	assert.Nil(t, err)

	rpcClient.closeConns()
}

func TestRecvErrorInMultipleRecvLoops(t *testing.T) {
	server, port := startMockTikvService()
	require.True(t, port > 0)
	defer server.Stop()
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port)

	// Enable batch and limit the connection count to 1 so that
	// there is only one BatchCommands stream for each host or forwarded host.
	defer config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.MaxBatchSize = 128
		conf.TiKVClient.GrpcConnectionCount = 1
	})()
	rpcClient := NewRPCClient()
	defer rpcClient.closeConns()

	// Create 4 BatchCommands streams.
	prewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{})
	forwardedHosts := []string{"", "127.0.0.1:6666", "127.0.0.1:7777", "127.0.0.1:8888"}
	for _, forwardedHost := range forwardedHosts {
		prewriteReq.ForwardedHost = forwardedHost
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		assert.Nil(t, err)
	}
	connArray, err := rpcClient.getConnArray(addr, true)
	assert.NotNil(t, connArray)
	assert.Nil(t, err)
	batchConn := connArray.batchConn
	assert.NotNil(t, batchConn)
	assert.Equal(t, len(batchConn.batchCommandsClients), 1)
	batchClient := batchConn.batchCommandsClients[0]
	assert.NotNil(t, batchClient.client)
	assert.Equal(t, batchClient.client.forwardedHost, "")
	assert.Equal(t, len(batchClient.forwardedClients), 3)
	for _, forwardedHosts := range forwardedHosts[1:] {
		assert.Equal(t, batchClient.forwardedClients[forwardedHosts].forwardedHost, forwardedHosts)
	}

	// Save all streams
	clientSave := batchClient.client.Tikv_BatchCommandsClient
	forwardedClientsSave := make(map[string]tikvpb.Tikv_BatchCommandsClient)
	for host, client := range batchClient.forwardedClients {
		forwardedClientsSave[host] = client.Tikv_BatchCommandsClient
	}
	epoch := atomic.LoadUint64(&batchClient.epoch)

	fp := "tikvclient/gotErrorInRecvLoop"
	// Send a request to each stream to trigger reconnection.
	for _, forwardedHost := range forwardedHosts {
		require.Nil(t, failpoint.Enable(fp, `1*return("0")`))
		prewriteReq.ForwardedHost = forwardedHost
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		assert.Nil(t, err)
		time.Sleep(100 * time.Millisecond)
		assert.Nil(t, failpoint.Disable(fp))
	}

	// Wait for finishing reconnection.
	for {
		batchClient.lockForRecreate()
		if atomic.LoadUint64(&batchClient.epoch) != epoch {
			batchClient.unlockForRecreate()
			break
		}
		batchClient.unlockForRecreate()
		time.Sleep(time.Millisecond * 100)
	}

	// send request after reconnection.
	for _, forwardedHost := range forwardedHosts {
		prewriteReq.ForwardedHost = forwardedHost
		_, err := rpcClient.SendRequest(context.Background(), addr, prewriteReq, 10*time.Second)
		assert.Nil(t, err)
	}
	// Should only reconnect once.
	assert.Equal(t, atomic.LoadUint64(&batchClient.epoch), epoch+1)
	// All streams are refreshed.
	assert.NotEqual(t, batchClient.client.Tikv_BatchCommandsClient, clientSave)
	assert.Equal(t, len(batchClient.forwardedClients), len(forwardedClientsSave))
	for host, clientSave := range forwardedClientsSave {
		assert.NotEqual(t, batchClient.forwardedClients[host].Tikv_BatchCommandsClient, clientSave)
	}
}
