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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/config/config_test.go
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

package config

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
)

func TestParsePath(t *testing.T) {
	etcdAddrs, disableGC, keyspaceName, err := ParsePath("tikv://node1:2379,node2:2379")

	assert.Nil(t, err)
	assert.Equal(t, []string{"node1:2379", "node2:2379"}, etcdAddrs)
	assert.False(t, disableGC)
	assert.Empty(t, keyspaceName)

	_, _, _, err = ParsePath("tikv://node1:2379")
	assert.Nil(t, err)

	_, disableGC, keyspaceName, err = ParsePath("tikv://node1:2379?disableGC=true&keyspaceName=DEFAULT")
	assert.Nil(t, err)
	assert.True(t, disableGC)
	assert.Equal(t, "DEFAULT", keyspaceName)
}

func TestTxnScopeValue(t *testing.T) {
	var err error

	err = failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`)
	assert.Nil(t, err)
	assert.Equal(t, "bj", GetTxnScopeFromConfig())

	err = failpoint.Enable("tikvclient/injectTxnScope", `return("")`)
	assert.Nil(t, err)
	assert.Equal(t, "global", GetTxnScopeFromConfig())

	err = failpoint.Enable("tikvclient/injectTxnScope", `return("global")`)
	assert.Nil(t, err)
	assert.Equal(t, "global", GetTxnScopeFromConfig())

	err = failpoint.Disable("tikvclient/injectTxnScope")
	assert.Nil(t, err)
}

func TestValidateGRPCKeepAliveTimeout(t *testing.T) {
	cfg := DefaultTiKVClient()
	assert.Nil(t, cfg.Valid())
	assert.Equal(t, time.Second*3, cfg.GetGrpcKeepAliveTimeout())
	cfg.GrpcKeepAliveTimeout = 0.05
	assert.Nil(t, cfg.Valid())
	assert.Equal(t, time.Millisecond*50, cfg.GetGrpcKeepAliveTimeout())
	cfg.GrpcKeepAliveTimeout = 0.04
	assert.NotNil(t, cfg.Valid())
	assert.Equal(t, "grpc-keepalive-timeout should be at least 0.05, but got 0.040000", cfg.Valid().Error())
}

func TestValidateTxnFileConfig(t *testing.T) {
	maxInt := uint64(math.MaxInt)
	tests := []struct {
		name      string
		configure func(*TiKVClient)
		err       string
	}{
		{
			name: "default",
		},
		{
			name: "zero chunk size",
			configure: func(cfg *TiKVClient) {
				cfg.TxnChunkMaxSize = 0
			},
			err: "txn-chunk-max-size should be greater than 0",
		},
		{
			name: "maximum chunk size",
			configure: func(cfg *TiKVClient) {
				cfg.TxnChunkMaxSize = maxInt
			},
		},
		{
			name: "chunk size exceeds int",
			configure: func(cfg *TiKVClient) {
				cfg.TxnChunkMaxSize = maxInt + 1
			},
			err: fmt.Sprintf("txn-chunk-max-size should not exceed %d, but got %d", maxInt, maxInt+1),
		},
		{
			name: "zero writer concurrency",
			configure: func(cfg *TiKVClient) {
				cfg.TxnChunkWriterConcurrency = 0
			},
			err: "txn-chunk-writer-concurrency should be greater than 0",
		},
		{
			name: "maximum writer concurrency",
			configure: func(cfg *TiKVClient) {
				cfg.TxnChunkWriterConcurrency = uint(maxInt)
			},
		},
		{
			name: "writer concurrency exceeds int",
			configure: func(cfg *TiKVClient) {
				cfg.TxnChunkWriterConcurrency = uint(maxInt) + 1
			},
			err: fmt.Sprintf("txn-chunk-writer-concurrency should not exceed %d, but got %d", maxInt, uint(maxInt)+1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := DefaultTiKVClient()
			if test.configure != nil {
				test.configure(&cfg)
			}
			if test.err == "" {
				assert.NoError(t, cfg.Valid())
				return
			}
			assert.EqualError(t, cfg.Valid(), test.err)
		})
	}
}
