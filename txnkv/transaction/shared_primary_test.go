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

package transaction

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func sharedReadCtx(forUpdateTS uint64) *kv.LockCtx {
	lock := kv.NewLockCtx(forUpdateTS, kv.LockNoWait, time.Now())
	lock.InShareMode = true
	lock.AllowSharedLockPrimary = true
	return lock
}

func TestSharedVictimAcrossLockBatches(t *testing.T) {
	old := kv.TxnCommitBatchSize.Swap(1)
	defer kv.TxnCommitBatchSize.Store(old)
	for _, first := range []string{"conflict", "deadlock"} {
		t.Run(first+" first", func(t *testing.T) {
			txn, store := newSharedPrimaryTestTxn(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Each batch worker cancels its context after delivering its error
			// to the collector. Wait for that signal to order the responses.
			firstWorker := make(chan context.Context, 1)
			store.client.onSend = func(batchCtx context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
				if req.Type == tikvrpc.CmdPessimisticRollback {
					return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticRollbackResponse{}}, nil
				}
				key := req.PessimisticLock().Mutations[0].Key
				response := &kvrpcpb.PessimisticLockResponse{}
				switch string(key) {
				case "primary":
					return &tikvrpc.Response{Resp: response}, nil
				case "deadlock":
					response.Errors = []*kvrpcpb.KeyError{{Deadlock: &kvrpcpb.Deadlock{LockKey: key, DeadlockKeyHash: farm.Fingerprint64(key)}}}
				case "conflict":
					response.Errors = []*kvrpcpb.KeyError{{Conflict: &kvrpcpb.WriteConflict{Key: key}}}
				default:
					return nil, errors.New("unexpected lock attempt")
				}
				if string(key) == first {
					firstWorker <- batchCtx
				} else {
					select {
					case workerCtx := <-firstWorker:
						<-workerCtx.Done()
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return &tikvrpc.Response{Resp: response}, nil
			}
			require.NoError(t, txn.LockKeys(ctx, sharedReadCtx(txn.StartTS()+1), []byte("primary")))
			lock := kv.NewLockCtx(txn.StartTS()+2, kv.LockNoWait, time.Now())
			var callbacks []*tikverr.ErrDeadlock
			lock.OnDeadlock = func(dl *tikverr.ErrDeadlock) { callbacks = append(callbacks, dl) }
			err := txn.LockKeys(ctx, lock, []byte("conflict"), []byte("deadlock"))
			var victim *tikverr.ErrDeadlock
			require.ErrorAs(t, err, &victim)
			require.False(t, victim.IsRetryable)
			require.Len(t, callbacks, 1)
			require.Same(t, victim, callbacks[0])
			require.Same(t, victim, txn.LockKeys(ctx, lock, []byte("later")))
			require.Same(t, victim, txn.Commit(ctx))
			require.NoError(t, txn.Rollback())
		})
	}
}

func TestSharedLockKeysFuncRequiresTimestamp(t *testing.T) {
	txn, store := newSharedPrimaryTestTxn(t)
	lockCalls := 0
	store.client.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		require.Equal(t, kvrpcpb.Op_SharedPessimisticLock, req.PessimisticLock().Mutations[0].Op)
		lockCalls++
		return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
	}
	txn.StartAggressiveLocking()
	key := []byte("key")
	lock := sharedReadCtx(0)
	require.ErrorContains(t, txn.LockKeysFunc(context.Background(), lock, nil, key), "ForUpdateTS")
	require.True(t, txn.IsInAggressiveLockingMode())
	require.Empty(t, txn.collectLockedKeys())
	require.False(t, txn.usesSharedLockPrimary)
	require.Zero(t, lockCalls)
	lock.ForUpdateTS = txn.StartTS() + 1
	require.NoError(t, txn.LockKeysFunc(context.Background(), lock, nil, key))
	require.Equal(t, 1, lockCalls)
	require.True(t, txn.usesSharedLockPrimary)
}

func TestAggressiveStageAfterSharedRead(t *testing.T) {
	ctx := context.Background()
	for _, outcome := range []string{"acquired", "failed", "missing", "start after acquired"} {
		for _, op := range []string{"retry", "done", "cancel"} {
			t.Run(outcome+"/"+op, func(t *testing.T) {
				txn, store := newSharedPrimaryTestTxn(t)
				lock := sharedReadCtx(txn.StartTS() + 1)
				resp := &kvrpcpb.PessimisticLockResponse{}
				switch outcome {
				case "failed":
					resp.Errors = []*kvrpcpb.KeyError{{Conflict: &kvrpcpb.WriteConflict{}}}
				case "missing":
					lock.LockOnlyIfExists = true
					lock.InitReturnValues(1)
					resp.Values = [][]byte{nil}
					resp.NotFounds = []bool{true}
				}
				store.client.onSend = func(_ context.Context, _ string, _ *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
					return &tikvrpc.Response{Resp: resp}, nil
				}
				if outcome != "start after acquired" {
					txn.StartAggressiveLocking()
				}
				err := txn.LockKeys(ctx, lock, []byte("key"))
				if outcome == "failed" {
					require.True(t, tikverr.IsErrWriteConflict(err))
				} else {
					require.NoError(t, err)
				}
				if outcome == "start after acquired" {
					txn.StartAggressiveLocking()
				}
				require.False(t, txn.IsInAggressiveLockingMode())
				switch op {
				case "retry":
					txn.RetryAggressiveLocking(ctx)
					txn.DoneAggressiveLocking(ctx)
				case "done":
					txn.DoneAggressiveLocking(ctx)
				case "cancel":
					txn.CancelAggressiveLocking(ctx)
				}
				require.Panics(t, func() { txn.RetryAggressiveLocking(ctx) })
				require.Panics(t, func() { txn.DoneAggressiveLocking(ctx) })
				require.Panics(t, func() { txn.CancelAggressiveLocking(ctx) })
			})
		}
	}
}

func TestConditionalUpgradeMissKeepsSharedPrewrite(t *testing.T) {
	txn, store := newSharedPrimaryTestTxn(t)
	var ops []kvrpcpb.Op
	store.client.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		switch req.Type {
		case tikvrpc.CmdPessimisticLock:
			lock := req.PessimisticLock()
			ops = append(ops, lock.Mutations[0].Op)
			if lock.LockOnlyIfExists {
				require.True(t, lock.IsSharedLockUpgrade)
				return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{Values: [][]byte{nil}, NotFounds: []bool{true}}}, nil
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
		case tikvrpc.CmdPrewrite:
			ops = append(ops, req.Prewrite().Mutations[0].Op)
			return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
		case tikvrpc.CmdCommit:
			return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}, nil
		default:
			return nil, errors.Errorf("unexpected RPC %s", req.Type)
		}
	}
	ctx := context.Background()
	key := []byte("absent")
	require.NoError(t, txn.LockKeys(ctx, sharedReadCtx(txn.StartTS()+1), key))
	upgrade := kv.NewLockCtx(txn.StartTS()+2, kv.LockNoWait, time.Now())
	upgrade.LockOnlyIfExists = true
	upgrade.InitReturnValues(1)
	require.NoError(t, txn.LockKeys(ctx, upgrade, key))
	require.False(t, upgrade.Values[string(key)].Exists)
	flags, err := txn.GetMemBuffer().GetFlags(key)
	require.NoError(t, err)
	require.True(t, flags.HasLockedInShareMode())
	require.NoError(t, txn.Commit(ctx))
	require.Equal(t, []kvrpcpb.Op{kvrpcpb.Op_SharedPessimisticLock, kvrpcpb.Op_PessimisticLock, kvrpcpb.Op_SharedLock}, ops)
}

func TestSharedReadWaitsForAggressiveRollback(t *testing.T) {
	for _, tc := range []struct {
		name          string
		increment     uint64
		rollbackFails bool
		earlierRetry  bool
	}{
		{name: "equal TS"},
		{name: "greater TS", increment: 1},
		{name: "failed rollback", rollbackFails: true},
		{name: "rollback queued by an earlier retry", earlierRetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				txn, store := newSharedPrimaryTestTxn(t)
				key := []byte("overlap")
				ctx := context.Background()
				release := make(chan struct{})
				rollbackStarted := make(chan struct{})
				var calls []tikvrpc.CmdType
				store.client.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
					calls = append(calls, req.Type)
					switch req.Type {
					case tikvrpc.CmdPessimisticLock:
						if req.PessimisticLock().Mutations[0].Op == kvrpcpb.Op_PessimisticLock {
							return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{
								Results: []*kvrpcpb.PessimisticLockKeyResult{{Type: kvrpcpb.PessimisticLockKeyResultType_LockResultNormal}},
							}}, nil
						}
						return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{}}, nil
					case tikvrpc.CmdPessimisticRollback:
						close(rollbackStarted)
						<-release
						resp := &kvrpcpb.PessimisticRollbackResponse{}
						if tc.rollbackFails {
							resp.Errors = []*kvrpcpb.KeyError{{Abort: "old lock still held"}}
						}
						return &tikvrpc.Response{Resp: resp}, nil
					case tikvrpc.CmdPrewrite:
						require.Equal(t, kvrpcpb.Op_SharedLock, req.Prewrite().Mutations[0].Op)
						return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
					case tikvrpc.CmdCommit:
						return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}, nil
					default:
						return nil, errors.Errorf("unexpected RPC %s", req.Type)
					}
				}
				xTS := txn.StartTS() + 1
				txn.StartAggressiveLocking()
				require.NoError(t, txn.LockKeys(ctx, kv.NewLockCtx(xTS, kv.LockNoWait, time.Now()), key))
				txn.RetryAggressiveLocking(ctx)
				if tc.earlierRetry {
					// K was not reused. Its X rollback starts before S ends the stage.
					txn.RetryAggressiveLocking(ctx)
					<-rollbackStarted
					// Cleanup of K must allow a shared read of a different key.
					otherCtx, cancel := context.WithTimeout(ctx, time.Second)
					err := txn.LockKeys(otherCtx, sharedReadCtx(xTS), []byte("other"))
					cancel()
					if err != nil {
						close(release)
						t.Fatal(err)
					}
				}
				done := make(chan error, 1)
				go func() { done <- txn.LockKeys(ctx, sharedReadCtx(xTS+tc.increment), key) }()
				<-rollbackStarted
				synctest.Wait()
				beforeRelease := append([]tikvrpc.CmdType(nil), calls...)
				close(release)
				err := <-done
				expected := []tikvrpc.CmdType{tikvrpc.CmdPessimisticLock, tikvrpc.CmdPessimisticRollback}
				if tc.earlierRetry {
					expected = append(expected, tikvrpc.CmdPessimisticLock)
				}
				require.Equal(t, expected, beforeRelease)
				txn.DoneAggressiveLocking(ctx)
				if tc.rollbackFails {
					require.ErrorContains(t, err, "old lock still held")
					require.Same(t, err, txn.Commit(ctx))
					require.Equal(t, beforeRelease, calls)
					return
				}
				require.NoError(t, err)
				require.NoError(t, txn.Commit(ctx))
				require.Equal(t, append(expected, tikvrpc.CmdPessimisticLock, tikvrpc.CmdPrewrite, tikvrpc.CmdCommit), calls)
			})
		})
	}
}

func TestZeroLockWaitBudget(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		recent, force, expired, mixed bool
	}{
		{name: "recent", recent: true},
		{name: "recent force-lock", recent: true, force: true},
		{name: "resolver live"},
		{name: "resolver expired", expired: true},
		{name: "resolved one blocker but skipped a live holder", expired: true, mixed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn, store := newSharedPrimaryTestTxn(t)
			store.resolver = txnlock.NewLockResolver(store)
			defer store.resolver.Close()
			ctx := context.Background()
			key := []byte("blocked")
			holderTS := txn.StartTS() - 1
			lockCalls, statusCalls := 0, 0
			resolved := false
			store.client.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
				switch req.Type {
				case tikvrpc.CmdPessimisticLock:
					lockCalls++
					require.Equal(t, kv.LockNoWait, req.PessimisticLock().WaitTimeout)
					resp := &kvrpcpb.PessimisticLockResponse{}
					if resolved {
						return &tikvrpc.Response{Resp: resp}, nil
					}
					if lockCalls > 1 {
						resp.Errors = []*kvrpcpb.KeyError{{Abort: "retried a live blocker"}}
						return &tikvrpc.Response{Resp: resp}, nil
					}
					info := &kvrpcpb.LockInfo{Key: key, PrimaryLock: []byte("holder-primary"), LockVersion: holderTS,
						LockType: kvrpcpb.Op_PessimisticLock, LockTtl: 60000}
					if tc.recent {
						info.DurationToLastUpdateMs = 1
					}
					if tc.mixed {
						info = &kvrpcpb.LockInfo{SharedLockInfos: []*kvrpcpb.LockInfo{
							info, {Key: key, LockVersion: holderTS - 1, DurationToLastUpdateMs: 1},
						}}
					}
					resp.Errors = []*kvrpcpb.KeyError{{Locked: info}}
					if tc.force {
						resp.Results = []*kvrpcpb.PessimisticLockKeyResult{{Type: kvrpcpb.PessimisticLockKeyResultType_LockResultFailed}}
					}
					return &tikvrpc.Response{Resp: resp}, nil
				case tikvrpc.CmdCheckTxnStatus:
					statusCalls++
					status := &kvrpcpb.CheckTxnStatusResponse{LockTtl: 60000}
					if tc.expired {
						status = &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_TTLExpirePessimisticRollback}
					}
					return &tikvrpc.Response{Resp: status}, nil
				case tikvrpc.CmdPessimisticRollback:
					if req.PessimisticRollback().StartVersion == holderTS {
						resolved = true
					}
					return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticRollbackResponse{}}, nil
				default:
					return nil, errors.Errorf("unexpected RPC %s", req.Type)
				}
			}
			if tc.force {
				txn.StartAggressiveLocking()
			}
			err := txn.LockKeys(ctx, kv.NewLockCtx(txn.StartTS()+1, 0, time.Now()), key)
			store.WaitGroup().Wait()
			if tc.expired && !tc.mixed {
				require.NoError(t, err)
				require.True(t, resolved)
				require.Equal(t, 2, lockCalls)
			} else {
				require.ErrorIs(t, err, tikverr.ErrLockWaitTimeout)
				require.Equal(t, 1, lockCalls)
			}
			if tc.recent {
				require.Zero(t, statusCalls)
			} else {
				require.Equal(t, 1, statusCalls)
			}
		})
	}
}

func TestLockWaitBudgetOnServerIsBusy(t *testing.T) {
	for _, tc := range []struct {
		name string
		wait int64
		want error
	}{
		{"NOWAIT", kv.LockNoWait, tikverr.ErrLockAcquireFailAndNoWaitSet},
		{"zero", 0, tikverr.ErrLockWaitTimeout},
		{"shorter than one backoff", 20, tikverr.ErrLockWaitTimeout},
		{"multiple retries", 4000, tikverr.ErrLockWaitTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				txn, store := newSharedPrimaryTestTxn(t)
				var waits []int64
				store.client.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
					if req.Type == tikvrpc.CmdPessimisticRollback {
						return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticRollbackResponse{}}, nil
					}
					waits = append(waits, req.PessimisticLock().WaitTimeout)
					if len(waits) > 8 {
						return &tikvrpc.Response{Resp: &kvrpcpb.PessimisticLockResponse{Errors: []*kvrpcpb.KeyError{{Abort: "too many requests"}}}}, nil
					}
					return tikvrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{Reason: "lock wait queue full"}})
				}
				start := time.Now()
				lock := kv.NewLockCtx(txn.StartTS()+1, tc.wait, start)
				err := txn.LockKeys(context.Background(), lock, []byte("key"))
				require.ErrorIs(t, err, tc.want)
				require.Equal(t, max(int64(0), tc.wait), time.Since(start).Milliseconds())
				if tc.wait == 4000 {
					require.Greater(t, len(waits), 1)
					for i := 1; i < len(waits); i++ {
						require.Positive(t, waits[i])
						require.Less(t, waits[i], waits[i-1])
					}
				} else {
					require.Len(t, waits, 1)
				}
			})
		})
	}
}
