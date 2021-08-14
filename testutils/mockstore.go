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

package testutils

import (
	"github.com/tikv/client-go/v2/internal/mockstore/cluster"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	pd "github.com/tikv/pd/client"
)

// Cluster simulates a TiKV cluster.
type Cluster = cluster.Cluster

// CoprRPCHandler is the interface to handle coprocessor RPC commands.
type CoprRPCHandler = mocktikv.CoprRPCHandler

// MVCCStore is a mvcc key-value storage.
type MVCCStore = mocktikv.MVCCStore

// MVCCPair is a KV pair read from MvccStore or an error if any occurs.
type MVCCPair = mocktikv.Pair

// MockCluster simulates a TiKV cluster.
type MockCluster = mocktikv.Cluster

// MockClient sends kv RPC calls to mock cluster.
type MockClient = mocktikv.RPCClient

// RPCSession stores session scope rpc data.
type RPCSession = mocktikv.Session

// NewMockTiKV creates a TiKV client and PD client from options.
func NewMockTiKV(path string, coprHandler CoprRPCHandler) (*MockClient, *MockCluster, pd.Client, error) {
	return mocktikv.NewTiKVAndPDClient(path, coprHandler)
}

// BootstrapWithSingleStore initializes a Cluster with 1 Region and 1 Store.
var BootstrapWithSingleStore = mocktikv.BootstrapWithSingleStore

// BootstrapWithMultiStores initializes a Cluster with 1 Region and n Stores.
var BootstrapWithMultiStores = mocktikv.BootstrapWithMultiStores

// BootstrapWithMultiRegions initializes a Cluster with multiple Regions and 1
// Store. The number of Regions will be len(splitKeys) + 1.
var BootstrapWithMultiRegions = mocktikv.BootstrapWithMultiRegions

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked = mocktikv.ErrLocked
