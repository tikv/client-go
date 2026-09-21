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

package tikv

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
)

type snapshotLockHintClient struct {
	Client
	onRequest     func(*tikvrpc.Request) *tikvrpc.Response
	asyncRequests atomic.Int32
}

func (c *snapshotLockHintClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if resp := c.onRequest(req); resp != nil {
		return resp, nil
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (c *snapshotLockHintClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	c.asyncRequests.Add(1)
	go func() {
		resp, err := c.SendRequest(ctx, addr, req, 0)
		cb.Schedule(resp, err)
	}()
}

func snapshotLockError(key []byte, txnID uint64) *kvrpcpb.KeyError {
	return &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
		Key: key, PrimaryLock: key, LockVersion: txnID,
		LockTtl: 1, TxnSize: 1, LockType: kvrpcpb.Op_Put,
	}}
}

func (s *testKVSuite) TestResolveLocksWithOptsBacksOffOnlyForRead() {
	lock := txnlock.NewLock(snapshotLockError([]byte("lock-key"), 42).Locked)
	tests := []struct {
		name      string
		forRead   bool
		lockHints txnlock.LockHintsInRequest
		backoffs  int
	}{
		{name: "write-resolved-hint", lockHints: txnlock.NewLockHintsInRequest([]uint64{lock.TxnID}, nil)},
		{name: "write-committed-hint", lockHints: txnlock.NewLockHintsInRequest(nil, []uint64{lock.TxnID})},
		{name: "read-no-hints", forRead: true},
		{name: "read-unrelated-hints", forRead: true, lockHints: txnlock.NewLockHintsInRequest([]uint64{43}, []uint64{44})},
		{name: "read-resolved-hint", forRead: true, lockHints: txnlock.NewLockHintsInRequest([]uint64{lock.TxnID}, nil), backoffs: 1},
		{name: "read-committed-hint", forRead: true, lockHints: txnlock.NewLockHintsInRequest(nil, []uint64{lock.TxnID}), backoffs: 1},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			bo := retry.NewBackoffer(context.Background(), 1000)
			_, err := s.store.GetLockResolver().ResolveLocksWithOpts(bo, txnlock.ResolveLocksOptions{
				CallerStartTS:      math.MaxUint64,
				Locks:              []*txnlock.Lock{lock},
				ForRead:            test.forRead,
				LockHintsInRequest: test.lockHints,
			})
			s.Require().NoError(err)
			s.Equal(test.backoffs, bo.GetBackoffTimes()["txnLockFast"])
		})
	}
}

func (s *testKVSuite) TestSnapshotReadsBackOffWhenServerReturnsHintedLock() {
	const lockTS = uint64(42)
	key := []byte("hinted-lock-key")
	for _, cmd := range []tikvrpc.CmdType{tikvrpc.CmdGet, tikvrpc.CmdBatchGet} {
		s.Run(cmd.String(), func() {
			baseClient := s.store.GetTiKVClient()
			defer s.store.SetTiKVClient(baseClient)
			requests := 0
			s.store.SetTiKVClient(&snapshotLockHintClient{Client: baseClient, onRequest: func(req *tikvrpc.Request) *tikvrpc.Response {
				if req.Type != cmd {
					return nil
				}
				requests++
				s.Contains(req.ResolvedLocks, lockTS)
				if requests == 1 {
					keyErr := snapshotLockError(key, lockTS)
					if cmd == tikvrpc.CmdGet {
						return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Error: keyErr}}
					}
					return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: keyErr}}
				}
				return nil
			}})

			snapshot := s.store.GetSnapshot(math.MaxUint64)
			snapshot.SetPipelined(lockTS)
			stats := &txnsnapshot.SnapshotRuntimeStats{}
			snapshot.SetRuntimeStats(stats)
			if cmd == tikvrpc.CmdGet {
				_, err := snapshot.Get(context.Background(), key)
				s.Require().True(tikverr.IsErrNotFound(err), "unexpected error: %v", err)
			} else {
				_, err := snapshot.BatchGet(context.Background(), [][]byte{key})
				s.Require().NoError(err)
			}
			s.Equal(2, requests)
			s.Contains(stats.String(), "txnLockFast_backoff:{num:1")
		})
	}
}

func (s *testKVSuite) TestAutocommitPointGetResolvesIgnoredLockHint() {
	key := []byte("hinted-lock-key")
	baseClient := s.store.GetTiKVClient()
	defer s.store.SetTiKVClient(baseClient)
	detail := &util.ExecDetails{}
	requests := 0
	var statusRequests []uint64
	s.store.SetTiKVClient(&snapshotLockHintClient{Client: baseClient, onRequest: func(req *tikvrpc.Request) *tikvrpc.Response {
		switch req.Type {
		case tikvrpc.CmdCheckTxnStatus:
			statusRequests = append(statusRequests, req.CheckTxnStatus().LockTs)
			if req.CheckTxnStatus().LockTs == 43 {
				s.Equal(int64(1), atomic.LoadInt64(&detail.BackoffCount))
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckTxnStatusResponse{}}
		case tikvrpc.CmdGet:
			requests++
			switch requests {
			case 1:
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Error: snapshotLockError(key, 42)}}
			case 2:
				s.NotContains(req.ResolvedLocks, uint64(43))
				s.Equal(int64(0), atomic.LoadInt64(&detail.BackoffCount))
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Error: snapshotLockError(key, 43)}}
			case 3:
				s.Contains(req.ResolvedLocks, uint64(43))
				s.Equal(int64(0), atomic.LoadInt64(&detail.BackoffCount))
				// The first encounter with B only sends a hint, without resolving it.
				s.Equal([]uint64{42}, statusRequests)
				return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Error: snapshotLockError(key, 43)}}
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{Value: []byte("value")}}
		}
		return nil
	}})

	ctx := context.WithValue(context.Background(), util.ExecDetailsKey, detail)
	value, err := s.store.GetSnapshot(math.MaxUint64).Get(ctx, key)
	s.Require().NoError(err)
	s.Equal([]byte("value"), value.Value)
	s.Equal(4, requests)
	s.Equal(int64(1), atomic.LoadInt64(&detail.BackoffCount))
	// A repeated lock despite the hint falls back to normal lock resolution.
	s.Equal([]uint64{42, 43}, statusRequests)
}

func (s *testKVSuite) TestBatchGetIgnoredCommittedLockExhaustsBackoff() {
	const lockTS = uint64(42)
	key := []byte("hinted-lock-key")
	bo := retry.NewBackoffer(context.Background(), 20)
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	s.Require().NoError(err)
	baseClient := s.store.GetTiKVClient()
	defer s.store.SetTiKVClient(baseClient)
	requests := 0
	s.store.SetTiKVClient(&snapshotLockHintClient{Client: baseClient, onRequest: func(req *tikvrpc.Request) *tikvrpc.Response {
		switch req.Type {
		case tikvrpc.CmdCheckTxnStatus:
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckTxnStatusResponse{CommitVersion: lockTS + 1}}
		case tikvrpc.CmdBatchGet:
			requests++
			// Bound the mock too, so missing backoff fails instead of hanging the test.
			if requests > 10 {
				return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: &kvrpcpb.KeyError{Abort: "too many retries"}}}
			}
			if requests == 1 {
				s.Empty(req.CommittedLocks)
			} else {
				s.Contains(req.CommittedLocks, lockTS)
			}
			// Discovering that the lock committed must not delay the first retry.
			s.Equal(max(0, requests-2), bo.GetBackoffTimes()["txnLockFast"])
			return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: snapshotLockError(key, lockTS)}}
		}
		return nil
	}})

	snapshot := txnsnapshot.SnapshotProbe{KVSnapshot: s.store.GetSnapshot(math.MaxUint64)}
	err = snapshot.BatchGetSingleRegion(bo, loc.Region, [][]byte{key}, func(_, _ []byte) {
		s.Fail("a continuously locked request must not return values")
	})
	s.Require().ErrorIs(err, tikverr.ErrResolveLockTimeout)
	s.GreaterOrEqual(bo.GetTotalSleep(), 20)
	s.Positive(bo.GetBackoffTimes()["txnLockFast"])
	s.Equal(bo.GetBackoffTimes()["txnLockFast"]+2, requests)
	s.LessOrEqual(requests, 10)
}

func TestAsyncBatchGetLockHintsInRequest(t *testing.T) {
	restore := config.UpdateGlobal(func(conf *config.Config) { conf.EnableAsyncBatchGet = true })
	defer restore()
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	// A single Region bypasses the async API, even when it is enabled.
	testutils.BootstrapWithMultiRegions(cluster, []byte("m"))
	detail := &util.ExecDetails{}
	var requests atomic.Int32
	mock := &snapshotLockHintClient{onRequest: func(req *tikvrpc.Request) *tikvrpc.Response {
		switch req.Type {
		case tikvrpc.CmdCheckTxnStatus:
			if req.CheckTxnStatus().LockTs == 43 {
				return &tikvrpc.Response{Resp: &kvrpcpb.CheckTxnStatusResponse{CommitVersion: 44}}
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.CheckTxnStatusResponse{}}
		case tikvrpc.CmdBatchGet:
			key := req.BatchGet().Keys[0]
			if string(key) == "a" {
				switch requests.Add(1) {
				case 1:
					assert.Contains(t, req.ResolvedLocks, uint64(42))
					assert.Equal(t, int64(0), atomic.LoadInt64(&detail.BackoffCount))
					return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: snapshotLockError(key, 42)}}
				case 2:
					assert.NotContains(t, req.CommittedLocks, uint64(43))
					assert.Equal(t, int64(1), atomic.LoadInt64(&detail.BackoffCount))
					return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: snapshotLockError(key, 43)}}
				case 3:
					assert.Contains(t, req.CommittedLocks, uint64(43))
					assert.Equal(t, int64(1), atomic.LoadInt64(&detail.BackoffCount))
					return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{Error: snapshotLockError(key, 43)}}
				}
			}
			return &tikvrpc.Response{Resp: &kvrpcpb.BatchGetResponse{}}
		}
		return nil
	}}
	store, err := NewTestTiKVStore(client, pdClient, func(client Client) Client {
		mock.Client = client
		return mock
	}, nil, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	snapshot := store.GetSnapshot(math.MaxUint64)
	snapshot.SetPipelined(42)
	ctx := context.WithValue(context.Background(), util.ExecDetailsKey, detail)
	_, err = snapshot.BatchGet(ctx, [][]byte{[]byte("a"), []byte("z")})
	require.NoError(t, err)
	require.Equal(t, int32(2), mock.asyncRequests.Load())
	require.Equal(t, int32(4), requests.Load())
	require.Equal(t, int64(2), atomic.LoadInt64(&detail.BackoffCount))
}
