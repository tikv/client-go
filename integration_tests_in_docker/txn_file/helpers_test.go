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

package txn_file_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/util/codec"
)

const (
	regionRetryTimeout  = 30 * time.Second
	regionRetryInterval = 100 * time.Millisecond
	regionLocateBackoff = 1000
	directGetAttempts   = 5
)

type testEnvironment struct {
	pdAddr             string
	keyspaceName       string
	txnChunkWriterAddr string
}

type keyValue struct {
	key   []byte
	value []byte
}

// orderedKeyValues preserves the key order used for txn-file mutation planning.
type orderedKeyValues []keyValue

type directGetResult struct {
	value    []byte
	locked   *kvrpcpb.LockInfo
	notFound bool
}

func testEnv(t *testing.T) testEnvironment {
	t.Helper()
	return testEnvironment{
		pdAddr:             requiredEnv(t, "PD_ADDR"),
		keyspaceName:       requiredEnv(t, "KEYSPACE_NAME"),
		txnChunkWriterAddr: requiredEnv(t, "TXN_CHUNK_WRITER_ADDR"),
	}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value, ok := os.LookupEnv(name)
	if !ok || value == "" || value != strings.TrimSpace(value) {
		t.Fatalf("%s must be set to a non-empty value without surrounding whitespace", name)
	}
	return value
}

func newTestClient(t *testing.T) (*txnkv.Client, testEnvironment) {
	t.Helper()
	env := testEnv(t)
	restore := config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.TxnChunkWriterAddr = env.txnChunkWriterAddr
		conf.TiKVClient.TxnFileMinMutationSize = 1
		conf.TiKVClient.TxnChunkMaxSize = 256
		conf.TiKVClient.TxnChunkWriterConcurrency = 2
	})
	t.Cleanup(restore)

	require.NoError(t, config.GetGlobalConfig().TiKVClient.Valid())
	client, err := txnkv.NewClient(
		[]string{env.pdAddr},
		txnkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		txnkv.WithKeyspace(env.keyspaceName),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})
	return client, env
}

func testPrefix(t *testing.T) string {
	t.Helper()
	return t.Name() + "_" + uuid.NewString()
}

func encodeKey(prefix, suffix string) []byte {
	return codec.EncodeBytes(nil, []byte(prefix+"_"+suffix))
}

func seed(t *testing.T, client *txnkv.Client, values orderedKeyValues) {
	t.Helper()
	txn, err := client.Begin()
	require.NoError(t, err)
	txn.DisableTxnFile()
	for _, pair := range values {
		require.NoError(t, txn.Set(pair.key, pair.value))
	}
	require.NoError(t, txn.Commit(context.Background()))
}

func deleteKeys(t *testing.T, client *txnkv.Client, values orderedKeyValues) {
	t.Helper()
	txn, err := client.Begin()
	if err != nil {
		t.Errorf("begin cleanup transaction: %v", err)
		return
	}
	txn.DisableTxnFile()
	for _, pair := range values {
		if err := txn.Delete(pair.key); err != nil {
			t.Errorf("delete cleanup key %q: %v", pair.key, err)
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				t.Errorf("rollback cleanup transaction: %v", rollbackErr)
			}
			return
		}
	}
	if err := txn.Commit(context.Background()); err != nil {
		t.Errorf("commit cleanup transaction: %v", err)
	}
}

func splitAndVerifyRegionGroups(
	t *testing.T,
	store *tikv.KVStore,
	splitKeys [][]byte,
	expectedGroups [][][]byte,
) {
	t.Helper()
	require.NotEmpty(t, splitKeys)
	requireOrderedGroups(t, expectedGroups)

	old := locateKey(t, store, splitKeys[0])
	_, err := store.SplitRegions(context.Background(), splitKeys, false, nil)
	require.NoError(t, err)
	store.GetRegionCache().InvalidateCachedRegion(old.Region)

	require.Eventually(t, func() bool {
		return regionGroupsMatch(store, expectedGroups)
	}, regionRetryTimeout, regionRetryInterval)
	requireRegionGroups(t, store, expectedGroups)
}

func requireOrderedGroups(t *testing.T, groups [][][]byte) {
	t.Helper()
	require.NotEmpty(t, groups)
	var previous []byte
	for _, group := range groups {
		require.NotEmpty(t, group)
		for _, key := range group {
			if previous != nil {
				require.Less(t, bytes.Compare(previous, key), 0, "expected ordered region groups")
			}
			previous = key
		}
	}
}

func regionGroupsMatch(store *tikv.KVStore, groups [][][]byte) bool {
	seen := make(map[uint64]struct{}, len(groups))
	for _, group := range groups {
		location, err := locateKeyOnce(store, group[0])
		if err != nil {
			return false
		}
		regionID := location.Region.GetID()
		if _, ok := seen[regionID]; ok {
			return false
		}
		seen[regionID] = struct{}{}
		for _, key := range group[1:] {
			location, err := locateKeyOnce(store, key)
			if err != nil || location.Region.GetID() != regionID {
				return false
			}
		}
	}
	return true
}

func requireRegionGroups(t *testing.T, store *tikv.KVStore, groups [][][]byte) {
	t.Helper()
	seen := make(map[uint64]struct{}, len(groups))
	for _, group := range groups {
		regionID := locateKey(t, store, group[0]).Region.GetID()
		_, duplicate := seen[regionID]
		require.False(t, duplicate, "expected distinct regions for ordered groups")
		seen[regionID] = struct{}{}
		for _, key := range group[1:] {
			require.Equal(t, regionID, locateKey(t, store, key).Region.GetID())
		}
	}
}

func locateKey(t *testing.T, store *tikv.KVStore, key []byte) *tikv.KeyLocation {
	t.Helper()
	location, err := locateKeyOnce(store, key)
	require.NoError(t, err)
	return location
}

func locateKeyOnce(store *tikv.KVStore, key []byte) (*tikv.KeyLocation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(regionLocateBackoff)*time.Millisecond)
	defer cancel()
	bo := tikv.NewBackofferWithVars(ctx, regionLocateBackoff, nil)
	return store.GetRegionCache().LocateKey(bo, key)
}

func directGet(ctx context.Context, store *tikv.KVStore, key []byte, version uint64) (directGetResult, error) {
	for range directGetAttempts {
		bo := tikv.NewBackofferWithVars(ctx, regionLocateBackoff, nil)
		location, err := store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return directGetResult{}, fmt.Errorf("locate direct get key: %w", err)
		}
		response, err := store.SendReq(bo, tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
			Key:     key,
			Version: version,
		}), location.Region, tikv.ReadTimeoutShort)
		if err != nil {
			return directGetResult{}, fmt.Errorf("send direct get: %w", err)
		}
		if response == nil || response.Resp == nil {
			return directGetResult{}, fmt.Errorf("direct get returned an empty response")
		}
		regionErr, err := response.GetRegionError()
		if err != nil {
			return directGetResult{}, fmt.Errorf("decode direct get region error: %w", err)
		}
		if regionErr != nil {
			store.GetRegionCache().InvalidateCachedRegion(location.Region)
			continue
		}

		getResponse, ok := response.Resp.(*kvrpcpb.GetResponse)
		if !ok {
			return directGetResult{}, fmt.Errorf("unexpected direct get response type %T", response.Resp)
		}
		if keyErr := getResponse.GetError(); keyErr != nil {
			if lock := keyErr.GetLocked(); lock != nil {
				return directGetResult{locked: lock}, nil
			}
			return directGetResult{}, fmt.Errorf("direct get key error: %s", keyErr)
		}
		return directGetResult{value: getResponse.GetValue(), notFound: getResponse.GetNotFound()}, nil
	}

	return directGetResult{}, fmt.Errorf("direct get exhausted %d region retries", directGetAttempts)
}

func txnFileRequestsOK() float64 {
	return testutil.ToFloat64(metrics.TxnFileRequestsOk)
}

func txnFileRequestsError() float64 {
	return testutil.ToFloat64(metrics.TxnFileRequestsError)
}
