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

package mocktikv

import "github.com/tikv/client-go/v2/internal/mockstore/mocktikv"

// For backward campatibility.
// TODO: remove it.

// CoprRPCHandler is the interface to handle coprocessor RPC commands.
type CoprRPCHandler = mocktikv.CoprRPCHandler

// MVCCStore is a mvcc key-value storage.
type MVCCStore = mocktikv.MVCCStore

// MustNewMVCCStore creates a MVCCStore for testing.
var MustNewMVCCStore = mocktikv.MustNewMVCCStore

// Pair is a KV pair read from MvccStore or an error if any occurs.
type Pair = mocktikv.Pair

// Cluster simulates a TiKV cluster.
type Cluster = mocktikv.Cluster

// RPCClient sends kv RPC calls to mock cluster.
type RPCClient = mocktikv.RPCClient

// Session stores session scope rpc data.
type Session = mocktikv.Session

// NewTiKVAndPDClient creates a TiKV client and PD client from options.
var NewTiKVAndPDClient = mocktikv.NewTiKVAndPDClient

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
