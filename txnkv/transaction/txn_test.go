// Copyright 2025 TiKV Authors
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

package transaction

import (
	"context"
	stderrs "errors"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

type mockPDClient struct {
	unimplementedPDClient
}

func (c *mockPDClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	peer := &metapb.Peer{Id: 1, StoreId: 1}
	return &router.Region{
		Meta: &metapb.Region{
			Id:       1,
			StartKey: []byte{},
			EndKey:   []byte{},
			Peers:    []*metapb.Peer{peer},
		},
		Leader: peer,
	}, nil
}

func (c *mockPDClient) GetStore(ctx context.Context, storeID uint64, opts ...opt.GetStoreOption) (*metapb.Store, error) {
	return &metapb.Store{
		Id:      storeID,
		Address: "mock-store",
	}, nil
}

func (c *mockPDClient) WithCallerComponent(caller.Component) pd.Client {
	return c
}

type fnClient struct {
	unimplementedKVClient

	onSend func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

func (c *fnClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	return c.onSend(ctx, addr, req, timeout)
}

type mockStore struct {
	unimplementedKVStore

	cache  *locate.RegionCache
	client fnClient
}

func (m *mockStore) GetRegionCache() *locate.RegionCache {
	return m.cache
}

func (m *mockStore) GetTiKVClient() client.Client {
	return &m.client
}

func (m *mockStore) GetOracle() oracle.Oracle {
	return nil
}

type testTxn struct {
	*KVTxn

	store    *mockStore
	snapshot *txnsnapshot.KVSnapshot
}

func newTestTxn(t *testing.T, startTS uint64) *testTxn {
	return newTestTxnWithOptions(t, startTS, &TxnOptions{StartTS: &startTS})
}

func newTestTxnWithOptions(t *testing.T, startTS uint64, options *TxnOptions) *testTxn {
	pd := &mockPDClient{}
	cache := locate.NewTestRegionCache()
	cache.SetPDClient(pd)
	store := &mockStore{cache: cache}
	snapshot := txnsnapshot.NewTiKVSnapshot(store, startTS, 0)
	txn, err := NewTiKVTxn(store, snapshot, startTS, options)
	require.NoError(t, err)
	return &testTxn{
		KVTxn:    txn,
		store:    store,
		snapshot: snapshot,
	}
}

func TestLockKeys(t *testing.T) {

	requireNoRequest := func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		require.FailNow(t, "locking keys in optimistic mode do not invoke SendRequest")
		return nil, errors.New("mock error")
	}

	t.Run("OptimisticExclusive", func(t *testing.T) {
		key := []byte("k")
		txn := newTestTxn(t, 1)
		txn.store.client.onSend = requireNoRequest

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key))
		flags, err := txn.GetMemBuffer().GetFlags(key)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.False(t, flags.HasLockedInShareMode())
	})

	t.Run("OptimisticShared", func(t *testing.T) {
		key := []byte("k")
		txn := newTestTxn(t, 1)
		txn.store.client.onSend = requireNoRequest

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.InShareMode = true
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key))
		flags, err := txn.GetMemBuffer().GetFlags(key)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.False(t, flags.HasLockedInShareMode()) // shared lock is not supported in optimistic txn for now
	})

	var expectedLockType kvrpcpb.Op
	handlePessimisticLock := func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		if req.Type != tikvrpc.CmdPessimisticLock {
			require.FailNow(t, "locking keys only send PessimisticLock request")
			return nil, errors.New("mock error")
		}
		lockReq := req.PessimisticLock()
		require.Len(t, lockReq.Mutations, 1)
		require.Equal(t, expectedLockType, lockReq.Mutations[0].Op)
		return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
	}

	t.Run("PessimisticExclusive", func(t *testing.T) {
		key := []byte("k")
		txn := newTestTxn(t, 1)
		txn.SetPessimistic(true)
		txn.store.client.onSend = handlePessimisticLock

		expectedLockType = kvrpcpb.Op_PessimisticLock
		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key))
		flags, err := txn.GetMemBuffer().GetFlags(key)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.False(t, flags.HasLockedInShareMode())
	})

	t.Run("PessimisticShared", func(t *testing.T) {
		key1 := []byte("k1")
		key2 := []byte("k2")
		txn := newTestTxn(t, 1)
		txn.SetPessimistic(true)
		txn.store.client.onSend = handlePessimisticLock

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())

		// shared-locked key cannot be primary key thus `lockKeys` fails.
		lockCtx.InShareMode = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, key1)
		require.ErrorContains(t, err, "pessimistic lock in share mode requires primary key to be selected")

		// `lockKeys` in exclusive mode to ensure primary key is selected.
		lockCtx.InShareMode = false
		expectedLockType = kvrpcpb.Op_PessimisticLock
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key2))

		// `lockKeys` in share mode again.
		lockCtx.InShareMode = true
		expectedLockType = kvrpcpb.Op_SharedPessimisticLock
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key1))
		flags, err := txn.GetMemBuffer().GetFlags(key1)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.True(t, flags.HasLockedInShareMode())
	})

	t.Run("PessimisticSharedRejectsAggressiveLocking", func(t *testing.T) {
		key1 := []byte("k1")
		key2 := []byte("k2")
		txn := newTestTxn(t, 1)
		txn.SetPessimistic(true)
		txn.store.client.onSend = handlePessimisticLock

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		expectedLockType = kvrpcpb.Op_PessimisticLock
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key2))

		txn.StartAggressiveLocking()
		lockCtx.InShareMode = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, key1)
		require.ErrorContains(t, err, "shared lock is not supported in aggressive/fair locking mode")
		require.True(t, txn.IsInAggressiveLockingMode())
		txn.CancelAggressiveLocking(context.Background())
	})

	t.Run("PessimisticSharedRejectsPipelinedTxn", func(t *testing.T) {
		startTS := uint64(1)
		key := []byte("k")
		txn := newTestTxnWithOptions(t, startTS, &TxnOptions{
			StartTS: &startTS,
			PipelinedTxn: PipelinedTxnOptions{
				Enable:                 true,
				FlushConcurrency:       1,
				ResolveLockConcurrency: 1,
			},
		})
		txn.store.client.onSend = requireNoRequest
		defer txn.pipelinedCancel()

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.InShareMode = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, key)
		require.ErrorContains(t, err, "shared lock is not supported in pipelined transaction")
	})

	t.Run("UpgradeSharedToExclusive", func(t *testing.T) {
		key1 := []byte("k1")
		key2 := []byte("k2")
		txn := newTestTxn(t, 1)
		txn.SetPessimistic(true)
		txn.store.client.onSend = handlePessimisticLock

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())

		// `lockKeys` in exclusive mode on k2 to ensure primary key is selected.
		lockCtx.InShareMode = false
		expectedLockType = kvrpcpb.Op_PessimisticLock
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key2))

		// `lockKeys` in share mode on k1.
		lockCtx.InShareMode = true
		expectedLockType = kvrpcpb.Op_SharedPessimisticLock
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key1))
		flags, err := txn.GetMemBuffer().GetFlags(key1)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.True(t, flags.HasLockedInShareMode())

		// `lockKeys` in exclusive mode again on k1 to upgrade the lock.
		lockCtx.InShareMode = false
		expectedLockType = kvrpcpb.Op_PessimisticLock
		lockCtx.AllowSharedLockUpgrade = true
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, key1))
		flags, err = txn.GetMemBuffer().GetFlags(key1)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.False(t, flags.HasLockedInShareMode())

		done := make(chan struct{})
		go func() {
			txn.us.GetMemBuffer().Delete(key1)
			defer close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			require.FailNow(t, "lockKeys holds rlock after returning error")
		}
	})
}

func TestSharedLockUpgrade(t *testing.T) {
	type requestSummary struct {
		keys [][]byte
		op   kvrpcpb.Op
	}

	newRecorderTxn := func(t *testing.T, onLock func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error)) (*testTxn, *[]requestSummary) {
		txn := newTestTxn(t, 1)
		txn.SetPessimistic(true)
		requests := make([]requestSummary, 0, 8)
		txn.store.client.onSend = func(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
			require.Equal(t, tikvrpc.CmdPessimisticLock, req.Type)
			lockReq := req.PessimisticLock()
			keys := make([][]byte, len(lockReq.Mutations))
			for i, mutation := range lockReq.Mutations {
				keys[i] = append([]byte(nil), mutation.Key...)
				if i == 0 {
					requests = append(requests, requestSummary{
						keys: keys[:0],
						op:   mutation.Op,
					})
				} else {
					require.Equal(t, requests[len(requests)-1].op, mutation.Op)
				}
				requests[len(requests)-1].keys = append(requests[len(requests)-1].keys, keys[i])
			}
			return onLock(len(requests)-1, lockReq)
		}
		return txn, &requests
	}

	lockSharedKey := func(t *testing.T, txn *testTxn, primaryKey, upgradeKey []byte) {
		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, primaryKey))
		lockCtx.InShareMode = true
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, upgradeKey))
		flags, err := txn.GetMemBuffer().GetFlags(upgradeKey)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.True(t, flags.HasLockedInShareMode())
	}

	keysAsStrings := func(keys [][]byte) []string {
		ret := make([]string, len(keys))
		for i, key := range keys {
			ret[i] = string(key)
		}
		return ret
	}

	t.Run("GateOffRejectsLocally", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		txn, requests := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)

		requestCount := len(*requests)
		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		err := txn.lockKeys(context.TODO(), lockCtx, nil, upgradeKey)
		require.ErrorContains(t, err, "upgrading a shared lock to an exclusive lock is not supported")
		require.Len(t, *requests, requestCount)

		flags, err := txn.GetMemBuffer().GetFlags(upgradeKey)
		require.NoError(t, err)
		require.True(t, flags.HasLockedInShareMode())
	})

	t.Run("GateOnSendsUpgradeSeparatelyAndPromotesLocalFlags", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		normalKey1 := []byte("normal-key-1")
		normalKey2 := []byte("normal-key-2")
		txn, requests := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)
		*requests = (*requests)[:0]

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.AllowSharedLockUpgrade = true
		require.NoError(t, txn.lockKeys(context.TODO(), lockCtx, nil, normalKey1, upgradeKey, normalKey2))

		require.Len(t, *requests, 2)
		require.Equal(t, kvrpcpb.Op_PessimisticLock, (*requests)[0].op)
		require.ElementsMatch(t, []string{string(normalKey1), string(normalKey2)}, keysAsStrings((*requests)[0].keys))
		require.Equal(t, kvrpcpb.Op_PessimisticLock, (*requests)[1].op)
		require.Equal(t, []string{string(upgradeKey)}, keysAsStrings((*requests)[1].keys))

		flags, err := txn.GetMemBuffer().GetFlags(upgradeKey)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.False(t, flags.HasLockedInShareMode())

		committer, err := TxnProbe{KVTxn: txn.KVTxn}.NewCommitter(1)
		require.NoError(t, err)
		mutations := committer.MutationsOfKeys([][]byte{upgradeKey})
		prewriteReq := committer.BuildPrewriteRequest(1, 1, 1, mutations, 1).Req.(*kvrpcpb.PrewriteRequest)
		require.Len(t, prewriteReq.Mutations, 1)
		require.Equal(t, kvrpcpb.Op_Lock, prewriteReq.Mutations[0].Op)
	})

	t.Run("RejectUpgradeInAggressiveLockingMode", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		txn, requests := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)

		requestCount := len(*requests)
		txn.StartAggressiveLocking()
		defer txn.CancelAggressiveLocking(context.Background())

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.AllowSharedLockUpgrade = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, upgradeKey)
		require.ErrorContains(t, err, "shared lock upgrade is not supported in aggressive/fair locking mode")
		require.Len(t, *requests, requestCount)
		require.True(t, txn.IsInAggressiveLockingMode())
	})

	t.Run("UpgradeLockOnlyIfExistsRequiresReturnValues", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		txn, _ := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.AllowSharedLockUpgrade = true
		lockCtx.LockOnlyIfExists = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, upgradeKey)
		var noReturnValueErr *tikverr.ErrLockOnlyIfExistsNoReturnValue
		require.ErrorAs(t, err, &noReturnValueErr)
		require.Equal(t, upgradeKey, noReturnValueErr.LockKey)
	})

	t.Run("ExplicitUpgradeFailureKeepsExistingSharedHolderAndEarlierExclusiveLocks", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		normalKey := []byte("normal-key")
		txn, requests := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			if len(req.Mutations) == 1 &&
				req.Mutations[0].Op == kvrpcpb.Op_PessimisticLock &&
				string(req.Mutations[0].Key) == string(upgradeKey) {
				return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{
					Errors: []*kvrpcpb.KeyError{{
						Conflict: &kvrpcpb.WriteConflict{
							StartTs:          1,
							ConflictTs:       2,
							ConflictCommitTs: 3,
							Key:              upgradeKey,
							Reason:           kvrpcpb.WriteConflict_PessimisticRetry,
						},
					}},
				}}, nil
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)
		*requests = (*requests)[:0]

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.AllowSharedLockUpgrade = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, normalKey, upgradeKey)
		require.True(t, tikverr.IsErrWriteConflict(err))
		require.Len(t, *requests, 2)
		require.Equal(t, []string{string(normalKey)}, keysAsStrings((*requests)[0].keys))
		require.Equal(t, []string{string(upgradeKey)}, keysAsStrings((*requests)[1].keys))

		upgradeFlags, err := txn.GetMemBuffer().GetFlags(upgradeKey)
		require.NoError(t, err)
		require.True(t, upgradeFlags.HasLocked())
		require.True(t, upgradeFlags.HasLockedInShareMode())

		normalFlags, err := txn.GetMemBuffer().GetFlags(normalKey)
		require.NoError(t, err)
		require.True(t, normalFlags.HasLocked())
		require.False(t, normalFlags.HasLockedInShareMode())

		require.ElementsMatch(t,
			[]string{string(primaryKey), string(upgradeKey), string(normalKey)},
			keysAsStrings(TxnProbe{KVTxn: txn.KVTxn}.CollectLockedKeys()))
	})

	t.Run("LockUpgradeConflictReturnsTypedErrorWithoutRetrySemantics", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		txn, requests := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			if len(req.Mutations) == 1 &&
				req.Mutations[0].Op == kvrpcpb.Op_PessimisticLock &&
				string(req.Mutations[0].Key) == string(upgradeKey) {
				return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{
					Errors: []*kvrpcpb.KeyError{{
						LockUpgradeConflict: &kvrpcpb.LockUpgradeConflict{
							Key:          upgradeKey,
							StartTs:      1,
							OwnerStartTs: 2,
							Reason:       kvrpcpb.LockUpgradeConflict_SecondUpgrader,
						},
					}},
				}}, nil
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)
		*requests = (*requests)[:0]

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.AllowSharedLockUpgrade = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, upgradeKey)
		require.Error(t, err)
		require.Len(t, *requests, 1)
		require.Equal(t, []string{string(upgradeKey)}, keysAsStrings((*requests)[0].keys))
		require.False(t, tikverr.IsErrWriteConflict(err))
		require.False(t, tikverr.IsErrorUndetermined(err))

		var retryable *tikverr.ErrRetryable
		require.False(t, stderrs.As(err, &retryable))
		var deadlock *tikverr.ErrDeadlock
		require.False(t, stderrs.As(err, &deadlock))

		var conflict *tikverr.ErrLockUpgradeConflict
		require.ErrorAs(t, err, &conflict)
		require.Equal(t, []byte("upgrade-key"), conflict.Key)
		require.Equal(t, uint64(1), conflict.StartTs)
		require.Equal(t, uint64(2), conflict.OwnerStartTs)
		require.Equal(t, kvrpcpb.LockUpgradeConflict_SecondUpgrader, conflict.Reason)

		flags, getErr := txn.GetMemBuffer().GetFlags(upgradeKey)
		require.NoError(t, getErr)
		require.True(t, flags.HasLocked())
		require.True(t, flags.HasLockedInShareMode())
	})

	t.Run("OutcomeUnknownUpgradeFailureIsTransactionFatal", func(t *testing.T) {
		primaryKey := []byte("primary-key")
		upgradeKey := []byte("upgrade-key")
		txn, requests := newRecorderTxn(t, func(callIndex int, req *kvrpcpb.PessimisticLockRequest) (*tikvrpc.Response, error) {
			if len(req.Mutations) == 1 &&
				req.Mutations[0].Op == kvrpcpb.Op_PessimisticLock &&
				string(req.Mutations[0].Key) == string(upgradeKey) {
				return &tikvrpc.Response{}, nil
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		})
		lockSharedKey(t, txn, primaryKey, upgradeKey)
		*requests = (*requests)[:0]

		lockCtx := kv.NewLockCtx(2, kv.LockNoWait, time.Now())
		lockCtx.AllowSharedLockUpgrade = true
		err := txn.lockKeys(context.TODO(), lockCtx, nil, upgradeKey)
		require.Error(t, err)
		require.True(t, tikverr.IsErrorUndetermined(err))
		require.Len(t, *requests, 1)
		require.Equal(t, []string{string(upgradeKey)}, keysAsStrings((*requests)[0].keys))

		flags, err := txn.GetMemBuffer().GetFlags(upgradeKey)
		require.NoError(t, err)
		require.True(t, flags.HasLocked())
		require.True(t, flags.HasLockedInShareMode())

		err = txn.LockKeys(context.TODO(), kv.NewLockCtx(2, kv.LockNoWait, time.Now()), []byte("later-key"))
		require.Error(t, err)
		require.True(t, tikverr.IsErrorUndetermined(err))

		err = txn.Commit(context.TODO())
		require.Error(t, err)
		require.True(t, tikverr.IsErrorUndetermined(err))
	})
}

func TestSharedLockCommitterIncompatibilities(t *testing.T) {
	t.Run("RejectSharedLockPrimaryKey", func(t *testing.T) {
		key := []byte("shared-key")
		txn := newTestTxn(t, 1)
		txn.SetPessimistic(true)
		txn.GetMemBuffer().UpdateFlags(key, kv.SetKeyLocked, kv.SetKeyLockedInShareMode)

		committer, err := newTwoPhaseCommitter(txn.KVTxn, 1)
		require.NoError(t, err)
		committer.primaryKey = key
		err = committer.initKeysAndMutations(context.Background())
		require.ErrorContains(t, err, "shared lock key cannot be used as transaction primary key")
	})

	t.Run("DisableAsyncCommitAndOnePC", func(t *testing.T) {
		startTS := uint64(1)
		newGlobalTxn := func() *testTxn {
			txn := newTestTxnWithOptions(t, startTS, &TxnOptions{
				TxnScope: oracle.GlobalTxnScope,
				StartTS:  &startTS,
			})
			txn.SetPessimistic(true)
			txn.SetEnableAsyncCommit(true)
			txn.SetEnable1PC(true)
			return txn
		}

		plainTxn := newGlobalTxn()
		plainTxn.GetMemBuffer().UpdateFlags([]byte("primary-key"), kv.SetKeyLocked)
		plainCommitter, err := TxnProbe{KVTxn: plainTxn.KVTxn}.NewCommitter(1)
		require.NoError(t, err)
		require.True(t, plainCommitter.CheckAsyncCommit())
		require.True(t, plainCommitter.CheckOnePC())

		sharedTxn := newGlobalTxn()
		sharedTxn.GetMemBuffer().UpdateFlags([]byte("primary-key"), kv.SetKeyLocked)
		sharedTxn.GetMemBuffer().UpdateFlags([]byte("shared-key"), kv.SetKeyLocked, kv.SetKeyLockedInShareMode)
		sharedCommitter, err := TxnProbe{KVTxn: sharedTxn.KVTxn}.NewCommitter(1)
		require.NoError(t, err)
		require.False(t, sharedCommitter.CheckAsyncCommit())
		require.False(t, sharedCommitter.CheckOnePC())
	})

	t.Run("RejectPipelinedFlush", func(t *testing.T) {
		startTS := uint64(1)
		key := []byte("shared-key")
		txn := newTestTxnWithOptions(t, startTS, &TxnOptions{
			StartTS: &startTS,
			PipelinedTxn: PipelinedTxnOptions{
				Enable:                 true,
				FlushConcurrency:       1,
				ResolveLockConcurrency: 1,
			},
		})
		defer txn.pipelinedCancel()
		require.NoError(t, txn.GetMemBuffer().SetWithFlags(key, []byte("value"), kv.SetKeyLocked, kv.SetKeyLockedInShareMode))

		flushed, err := txn.GetMemBuffer().Flush(true)
		require.NoError(t, err)
		require.True(t, flushed)
		err = txn.GetMemBuffer().FlushWait()
		require.ErrorContains(t, err, "shared lock is not supported in pipelined transaction")
	})
}
