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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/util_test.go
//

// Copyright 2021 PingCAP, Inc.
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
	"flag"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/kv"
	txndriver "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/util/codec"
	pd "github.com/tikv/pd/client"
)

var (
	withTiKV = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
	pdAddrs  = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
)

// NewTestStore creates a KVStore for testing purpose.
func NewTestStore(t *testing.T) *tikv.KVStore {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *withTiKV {
		return newTiKVStore(t)
	}
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.Nil(t, err)
	return store
}

// NewTestUniStore creates a KVStore (using tidb/unistore) for testing purpose.
// TODO: switch to use mockstore and remove it.
func NewTestUniStore(t *testing.T) *tikv.KVStore {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *withTiKV {
		return newTiKVStore(t)
	}
	client, pdClient, cluster, err := unistore.New("")
	require.Nil(t, err)
	unistore.BootstrapWithSingleStore(cluster)
	store, err := tikv.NewTestTiKVStore(&unistoreClientWrapper{client}, pdClient, nil, nil, 0)
	require.Nil(t, err)
	return store
}

func newTiKVStore(t *testing.T) *tikv.KVStore {
	addrs := strings.Split(*pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	require.Nil(t, err)
	var securityConfig config.Security
	tlsConfig, err := securityConfig.ToTLSConfig()
	require.Nil(t, err)
	spKV, err := tikv.NewEtcdSafePointKV(addrs, tlsConfig)
	require.Nil(t, err)
	store, err := tikv.NewKVStore("test-store", &tikv.CodecPDClient{Client: pdClient}, spKV, tikv.NewRPCClient())
	require.Nil(t, err)
	err = clearStorage(store)
	require.Nil(t, err)
	return store
}

func clearStorage(store *tikv.KVStore) error {
	txn, err := store.Begin()
	if err != nil {
		return err
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return err
	}
	for iter.Valid() {
		txn.Delete(iter.Key())
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return txn.Commit(context.Background())
}

func encodeKey(prefix, s string) []byte {
	return codec.EncodeBytes(nil, []byte(fmt.Sprintf("%s_%s", prefix, s)))
}

func valueBytes(n int) []byte {
	return []byte(fmt.Sprintf("value%d", n))
}

// s08d is for returning format string "%s%08d" to keep string sorted.
// e.g.: "0002" < "0011", otherwise "2" > "11"
func s08d(prefix string, n int) string {
	return fmt.Sprintf("%s%08d", prefix, n)
}

func toTiDBTxn(txn *transaction.TxnProbe) kv.Transaction {
	return txndriver.NewTiKVTxn(txn.KVTxn)
}

func toTiDBKeys(keys [][]byte) []kv.Key {
	kvKeys := *(*[]kv.Key)(unsafe.Pointer(&keys))
	return kvKeys
}
