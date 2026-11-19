// Copyright 2026 TiKV Authors.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func TestTxnFileResolvesExpiredSharedHolderBeforeActiveHolder(t *testing.T) {
	client, _ := newTestClient(t)
	const managedLockTTL = 500
	originalManagedLockTTL := atomic.LoadUint64(&transaction.ManagedLockTTL)
	atomic.StoreUint64(&transaction.ManagedLockTTL, managedLockTTL)
	t.Cleanup(func() {
		atomic.StoreUint64(&transaction.ManagedLockTTL, originalManagedLockTTL)
	})

	prefix := testPrefix(t)
	sharedKey := encodeKey(prefix, "00000010_shared")
	primaryKeys := [][]byte{
		encodeKey(prefix, "00000000_expired_primary"),
		encodeKey(prefix, "00000001_active_primary"),
	}

	beginSharedHolder := func(primaryKey []byte) transaction.TxnProbe {
		rawTxn, err := client.Begin()
		require.NoError(t, err)
		rawTxn.DisableTxnFile()
		txn := transaction.TxnProbe{KVTxn: rawTxn}
		txn.SetPessimistic(true)
		forUpdateTS, err := client.CurrentTimestamp(oracle.GlobalTxnScope)
		require.NoError(t, err)
		require.NoError(t, txn.LockKeys(context.Background(), kv.NewLockCtx(forUpdateTS, 1000, time.Now()), primaryKey))
		require.False(t, txn.GetCommitter().IsNil(), "exclusive primary lock must initialize the committer")
		require.Equal(t, primaryKey, txn.GetCommitter().GetPrimaryKey())

		lockTS, err := client.CurrentTimestamp(oracle.GlobalTxnScope)
		require.NoError(t, err)
		lockCtx := kv.NewLockCtx(lockTS, 1000, time.Now())
		lockCtx.InShareMode = true
		require.NoError(t, txn.LockKeys(context.Background(), lockCtx, sharedKey))
		return txn
	}

	expiredTxn := beginSharedHolder(primaryKeys[0])
	activeTxn := beginSharedHolder(primaryKeys[1])
	t.Cleanup(func() {
		_ = activeTxn.Rollback()
		_ = expiredTxn.Rollback()
	})
	require.True(t, activeTxn.GetCommitter().IsTTLRunning())

	sharedKeyEnd := append(bytes.Clone(sharedKey), 0)
	store := tikv.StoreProbe{KVStore: client.KVStore}
	var lastScanErr error
	scanSharedLocks := func() []*txnlock.Lock {
		maxTS, err := client.CurrentTimestamp(oracle.GlobalTxnScope)
		if err != nil {
			lastScanErr = err
			return nil
		}
		locks, err := store.ScanLocks(context.Background(), sharedKey, sharedKeyEnd, maxTS)
		lastScanErr = err
		return locks
	}
	require.Eventually(t, func() bool {
		locks := scanSharedLocks()
		if len(locks) != 2 {
			return false
		}
		versions := map[uint64]struct{}{
			locks[0].TxnID: {},
			locks[1].TxnID: {},
		}
		_, hasExpired := versions[expiredTxn.StartTS()]
		_, hasActive := versions[activeTxn.StartTS()]
		return hasExpired && hasActive
	}, regionRetryTimeout, regionRetryInterval, "expected two shared lock holders")
	require.NoError(t, lastScanErr)

	expiredTxn.GetCommitter().CloseTTLManager()
	require.Eventually(t, func() bool {
		return client.KVStore.GetOracle().UntilExpired(
			expiredTxn.StartTS(),
			managedLockTTL+200,
			&oracle.Option{TxnScope: oracle.GlobalTxnScope},
		) <= 0
	}, regionRetryTimeout, regionRetryInterval, "expected the expired holder to age while the active holder stays alive")
	require.True(t, activeTxn.GetCommitter().IsTTLRunning())

	values := orderedKeyValues{
		{key: sharedKey, value: bytes.Repeat([]byte("a"), 128)},
		{key: encodeKey(prefix, "00000020"), value: bytes.Repeat([]byte("b"), 128)},
		{key: encodeKey(prefix, "00000030"), value: bytes.Repeat([]byte("c"), 128)},
	}
	contender, err := client.Begin()
	require.NoError(t, err)
	for _, pair := range values {
		require.NoError(t, contender.Set(pair.key, pair.value))
	}

	okBefore := txnFileRequestsOK()
	commitCtx, cancel := context.WithTimeout(context.Background(), regionRetryTimeout)
	defer cancel()
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- contender.Commit(commitCtx)
	}()

	require.Eventually(t, func() bool {
		locks := scanSharedLocks()
		return len(locks) == 1 && locks[0].TxnID == activeTxn.StartTS()
	}, regionRetryTimeout, regionRetryInterval, "expected only the active shared holder to remain")
	require.NoError(t, lastScanErr)
	select {
	case err := <-commitDone:
		require.FailNow(t, "txn-file prewrite returned while the active shared holder remained", err)
	default:
	}

	require.NoError(t, activeTxn.Rollback())
	require.NoError(t, <-commitDone)
	require.Equal(t, okBefore+1, txnFileRequestsOK())

	readTS, err := client.CurrentTimestamp(oracle.GlobalTxnScope)
	require.NoError(t, err)
	result, err := directGet(context.Background(), client.KVStore, sharedKey, readTS)
	require.NoError(t, err)
	require.Nil(t, result.locked)
	require.Equal(t, values[0].value, result.value)

	deleteKeys(t, client, values)
}
