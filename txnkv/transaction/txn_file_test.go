// Copyright 2024 TiKV Authors
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
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/latch"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/unionstore"
	tikv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util"
)

type txnFileCommitTSOracle struct {
	unimplementedOracle

	expired bool
	calls   int
	startTS uint64
	ttl     uint64
	option  *oracle.Option
}

func (o *txnFileCommitTSOracle) IsExpired(startTS uint64, ttl uint64, option *oracle.Option) bool {
	o.calls++
	o.startTS = startTS
	o.ttl = ttl
	o.option = option
	return o.expired
}

type txnFileCommitTSStore struct {
	unimplementedKVStore

	timestamps     []uint64
	timestampCalls int
	timestampErr   error
	backoffer      *retry.Backoffer
	oracle         *txnFileCommitTSOracle
	regionCache    *locate.RegionCache
	client         client.Client
}

func (s *txnFileCommitTSStore) GetTimestampWithRetry(bo *retry.Backoffer, _ string) (uint64, error) {
	s.timestampCalls++
	s.backoffer = bo
	if s.timestampErr != nil {
		return 0, s.timestampErr
	}
	if len(s.timestamps) == 0 {
		return 0, errors.New("no timestamp configured")
	}
	ts := s.timestamps[0]
	s.timestamps = s.timestamps[1:]
	return ts, nil
}

func (s *txnFileCommitTSStore) GetOracle() oracle.Oracle {
	return s.oracle
}

func (s *txnFileCommitTSStore) GetRegionCache() *locate.RegionCache {
	return s.regionCache
}

func (s *txnFileCommitTSStore) GetTiKVClient() client.Client {
	return s.client
}

type txnFileSchemaVer int64

func (v txnFileSchemaVer) SchemaMetaVersion() int64 {
	return int64(v)
}

type txnFileSchemaLeaseChecker struct {
	err       error
	calls     int
	checkTS   uint64
	schemaVer SchemaVer
}

func (c *txnFileSchemaLeaseChecker) CheckBySchemaVer(checkTS uint64, schemaVer SchemaVer) (*RelatedSchemaChange, error) {
	c.calls++
	c.checkTS = checkTS
	c.schemaVer = schemaVer
	return nil, c.err
}

func newTxnFileCommitTSTestCommitter(
	store *txnFileCommitTSStore,
	checker SchemaLeaseChecker,
	upperBoundCheck func(uint64) bool,
) *twoPhaseCommitter {
	txn := &KVTxn{
		store:                   store,
		startTS:                 1,
		schemaVer:               txnFileSchemaVer(10),
		schemaLeaseChecker:      checker,
		scope:                   oracle.GlobalTxnScope,
		commitTSUpperBoundCheck: upperBoundCheck,
	}
	committer := &twoPhaseCommitter{
		store:     store,
		txn:       txn,
		startTS:   txn.startTS,
		sessionID: 7,
	}
	committer.setDetail(&util.CommitDetails{})
	return committer
}

func TestPrepareTxnFileCommitTS(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		commitOracle := &txnFileCommitTSOracle{}
		store := &txnFileCommitTSStore{
			timestamps: []uint64{100, 102},
			oracle:     commitOracle,
		}
		checker := &txnFileSchemaLeaseChecker{}
		upperBoundCalls := 0
		committer := newTxnFileCommitTSTestCommitter(store, checker, func(commitTS uint64) bool {
			upperBoundCalls++
			return commitTS == 102
		})
		committer.txn.SetCommitWaitUntilTSO(101)
		committer.txn.SetCommitWaitUntilTSOTimeout(time.Second)

		commitTS, err := committer.prepareTxnFileCommitTS(retry.NewBackoffer(context.Background(), TsoMaxBackoff))

		require.NoError(t, err)
		require.Equal(t, uint64(102), commitTS)
		require.Equal(t, 2, store.timestampCalls)
		require.Equal(t, 1, checker.calls)
		require.Equal(t, commitTS, checker.checkTS)
		require.Equal(t, txnFileSchemaVer(10), checker.schemaVer)
		require.Equal(t, 1, commitOracle.calls)
		require.Equal(t, uint64(1), commitOracle.startTS)
		require.Equal(t, uint64(MaxTxnTimeUse), commitOracle.ttl)
		require.Equal(t, oracle.GlobalTxnScope, commitOracle.option.TxnScope)
		require.Equal(t, 1, upperBoundCalls)
		require.Equal(t, uint64(100), committer.getDetail().LagDetails.FirstLagTS)
		require.Equal(t, uint64(101), committer.getDetail().LagDetails.WaitUntilTS)
		require.Equal(t, 1, committer.getDetail().LagDetails.BackoffCnt)
	})

	t.Run("schema invalid", func(t *testing.T) {
		schemaErr := errors.New("schema changed")
		commitOracle := &txnFileCommitTSOracle{}
		store := &txnFileCommitTSStore{timestamps: []uint64{100}, oracle: commitOracle}
		checker := &txnFileSchemaLeaseChecker{err: schemaErr}
		upperBoundCalls := 0
		committer := newTxnFileCommitTSTestCommitter(store, checker, func(uint64) bool {
			upperBoundCalls++
			return true
		})

		commitTS, err := committer.prepareTxnFileCommitTS(retry.NewBackoffer(context.Background(), TsoMaxBackoff))

		require.Zero(t, commitTS)
		require.ErrorIs(t, err, schemaErr)
		require.Equal(t, 1, checker.calls)
		require.Zero(t, commitOracle.calls)
		require.Zero(t, upperBoundCalls)
	})

	t.Run("transaction expired", func(t *testing.T) {
		commitOracle := &txnFileCommitTSOracle{expired: true}
		store := &txnFileCommitTSStore{timestamps: []uint64{100}, oracle: commitOracle}
		checker := &txnFileSchemaLeaseChecker{}
		upperBoundCalls := 0
		committer := newTxnFileCommitTSTestCommitter(store, checker, func(uint64) bool {
			upperBoundCalls++
			return true
		})

		commitTS, err := committer.prepareTxnFileCommitTS(retry.NewBackoffer(context.Background(), TsoMaxBackoff))

		require.Zero(t, commitTS)
		require.ErrorContains(t, err, "txn takes too much time")
		require.Equal(t, 1, checker.calls)
		require.Equal(t, 1, commitOracle.calls)
		require.Zero(t, upperBoundCalls)
	})

	t.Run("commit timestamp exceeds upper bound", func(t *testing.T) {
		commitOracle := &txnFileCommitTSOracle{}
		store := &txnFileCommitTSStore{timestamps: []uint64{100}, oracle: commitOracle}
		checker := &txnFileSchemaLeaseChecker{}
		upperBoundCalls := 0
		committer := newTxnFileCommitTSTestCommitter(store, checker, func(uint64) bool {
			upperBoundCalls++
			return false
		})

		commitTS, err := committer.prepareTxnFileCommitTS(retry.NewBackoffer(context.Background(), TsoMaxBackoff))

		require.Zero(t, commitTS)
		require.ErrorContains(t, err, "check commit ts upper bound fail")
		require.Equal(t, 1, checker.calls)
		require.Equal(t, 1, commitOracle.calls)
		require.Equal(t, 1, upperBoundCalls)
	})
}

func TestTxnFileCommitTSExpiredRetryUsesPreparedTimestamp(t *testing.T) {
	pd := &mockPDClient{}
	regionCache := locate.NewTestRegionCache()
	regionCache.SetPDClient(pd)
	defer regionCache.Close()

	commitOracle := &txnFileCommitTSOracle{}
	checker := &txnFileSchemaLeaseChecker{}
	requestCount := 0
	kvClient := &fnClient{}
	kvClient.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		requestCount++
		if requestCount == 1 {
			return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{
				CommitTsExpired: &kvrpcpb.CommitTsExpired{
					StartTs:           1,
					AttemptedCommitTs: req.Commit().CommitVersion,
					MinCommitTs:       100,
					Key:               []byte("k"),
				},
			}}}, nil
		}
		require.Equal(t, uint64(102), req.Commit().CommitVersion)
		return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}, nil
	}
	store := &txnFileCommitTSStore{
		timestamps:  []uint64{102},
		oracle:      commitOracle,
		regionCache: regionCache,
		client:      kvClient,
	}
	upperBoundCalls := 0
	committer := newTxnFileCommitTSTestCommitter(store, checker, func(commitTS uint64) bool {
		upperBoundCalls++
		return commitTS == 102
	})
	committer.commitTS = 2
	committer.primaryKey = []byte("k")
	taggerCalls := 0
	committer.resourceGroupTagger = func(*tikvrpc.Request) {
		taggerCalls++
	}

	bo := retry.NewBackoffer(context.Background(), 1000)
	location, err := regionCache.LocateKey(bo, []byte("k"))
	require.NoError(t, err)
	batch := chunkBatch{
		txnChunkSlice: txnChunkSlice{
			chunkIDs: []uint64{1},
			chunkRanges: []txnChunkRange{{
				smallest: []byte("k"),
				biggest:  []byte("k"),
			}},
		},
		region:         location,
		sampleDataKeys: [][]byte{[]byte("k")},
		firstKey:       []byte("k"),
		isPrimary:      true,
	}

	_, err = (txnFileCommitAction{}).executeBatch(committer, bo, batch)

	require.NoError(t, err)
	require.Equal(t, 2, requestCount)
	require.Equal(t, uint64(102), committer.commitTS)
	require.Equal(t, 1, store.timestampCalls)
	require.Same(t, bo, store.backoffer)
	require.Equal(t, 1, checker.calls)
	require.Equal(t, 1, commitOracle.calls)
	require.Equal(t, 1, upperBoundCalls)
	require.Equal(t, 1, taggerCalls)

	requestCount = 0
	store.timestamps = []uint64{104}
	store.timestampCalls = 0
	checker.calls = 0
	commitOracle.calls = 0
	upperBoundCalls = 0
	committer.commitTS = 2
	batch.isPrimary = false
	kvClient.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		requestCount++
		return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				StartTs:           1,
				AttemptedCommitTs: req.Commit().CommitVersion,
				MinCommitTs:       100,
			},
		}}}, nil
	}

	_, err = (txnFileCommitAction{}).executeBatch(committer, bo, batch)

	require.ErrorContains(t, err, "key is not the primary key")
	require.Equal(t, 1, requestCount)
	require.Equal(t, uint64(2), committer.commitTS)
	require.Zero(t, store.timestampCalls)
	require.Zero(t, checker.calls)
	require.Zero(t, commitOracle.calls)
	require.Zero(t, upperBoundCalls)

	requestCount = 0
	store.timestamps = []uint64{106}
	store.timestampCalls = 0
	checker.calls = 0
	commitOracle.calls = 0
	upperBoundCalls = 0
	committer.commitTS = 2
	batch.isPrimary = true
	kvClient.onSend = func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		requestCount++
		return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				StartTs:           1,
				AttemptedCommitTs: req.Commit().CommitVersion,
				MinCommitTs:       100,
				Key:               []byte("not-primary"),
			},
		}}}, nil
	}

	_, err = (txnFileCommitAction{}).executeBatch(committer, bo, batch)

	require.ErrorContains(t, err, "key is not the primary key")
	require.Equal(t, 1, requestCount)
	require.Equal(t, uint64(2), committer.commitTS)
	require.Zero(t, store.timestampCalls)
	require.Zero(t, checker.calls)
	require.Zero(t, commitOracle.calls)
	require.Zero(t, upperBoundCalls)
}

func newTxnFileCommitTestBatch(
	t *testing.T,
	onSend func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error),
) (*twoPhaseCommitter, *retry.Backoffer, chunkBatch) {
	t.Helper()

	pd := &mockPDClient{}
	regionCache := locate.NewTestRegionCache()
	regionCache.SetPDClient(pd)
	t.Cleanup(regionCache.Close)

	store := &txnFileCommitTSStore{
		oracle:      &txnFileCommitTSOracle{},
		regionCache: regionCache,
		client:      &fnClient{onSend: onSend},
	}
	committer := newTxnFileCommitTSTestCommitter(store, &txnFileSchemaLeaseChecker{}, nil)
	committer.commitTS = 2

	bo := retry.NewBackoffer(context.Background(), 1000)
	location, err := regionCache.LocateKey(bo, []byte("k"))
	require.NoError(t, err)
	batch := chunkBatch{
		txnChunkSlice: txnChunkSlice{
			chunkIDs: []uint64{1},
			chunkRanges: []txnChunkRange{{
				smallest: []byte("k"),
				biggest:  []byte("k"),
			}},
		},
		region:         location,
		sampleDataKeys: [][]byte{[]byte("k")},
		firstKey:       []byte("k"),
		isPrimary:      true,
	}
	committer.txnFileCtx = txnFileCtx{slice: batch.txnChunkSlice}
	return committer, bo, batch
}

func TestTxnFilePrewriteUsesPrimaryKey(t *testing.T) {
	committer, bo, batch := newTxnFileCommitTestBatch(t, func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		require.Equal(t, []byte("primary"), req.Prewrite().PrimaryLock)
		return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
	})
	committer.primaryKey = []byte("primary")

	_, err := (txnFilePrewriteAction{}).executeBatch(committer, bo, batch)

	require.NoError(t, err)
}

func TestTxnFilePrimaryBatchIndexFindsPrimaryRegion(t *testing.T) {
	committer := &twoPhaseCommitter{primaryKey: []byte("primary")}
	batches := []chunkBatch{
		{region: &locate.KeyLocation{EndKey: []byte("primary")}},
		{region: &locate.KeyLocation{StartKey: []byte("primary")}},
	}

	index, err := committer.txnFilePrimaryBatchIndex(batches)

	require.NoError(t, err)
	require.Equal(t, 1, index)
}

func TestTxnFileActionsApplyResourceGroupTagger(t *testing.T) {
	tests := []struct {
		name        string
		action      txnFileAction
		requestType tikvrpc.CmdType
		newResponse func() *tikvrpc.Response
	}{
		{
			name:        "prewrite",
			action:      txnFilePrewriteAction{},
			requestType: tikvrpc.CmdPrewrite,
			newResponse: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
			},
		},
		{
			name:        "commit",
			action:      txnFileCommitAction{},
			requestType: tikvrpc.CmdCommit,
			newResponse: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}
			},
		},
		{
			name:        "rollback",
			action:      txnFileRollbackAction{},
			requestType: tikvrpc.CmdBatchRollback,
			newResponse: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.BatchRollbackResponse{}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taggerCalls := 0
			committer, bo, batch := newTxnFileCommitTestBatch(t, func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
				require.Equal(t, tt.requestType, req.Type)
				require.Equal(t, []byte("dynamic-tag"), req.ResourceGroupTag)
				switch req.Type {
				case tikvrpc.CmdPrewrite:
					prewrite := req.Prewrite()
					require.Empty(t, prewrite.Mutations)
					require.Equal(t, []uint64{1}, prewrite.TxnFileChunks)
					require.Equal(t, []byte("k"), prewrite.PrimaryLock)
					require.NotNil(t, req.ResourceControlContext)
					require.Equal(t, "txn-file-test", req.ResourceControlContext.ResourceGroupName)
				case tikvrpc.CmdCommit:
					require.Equal(t, [][]byte{[]byte("k")}, req.Commit().Keys)
				case tikvrpc.CmdBatchRollback:
					require.Equal(t, [][]byte{[]byte("k")}, req.BatchRollback().Keys)
				}
				return tt.newResponse(), nil
			})
			committer.resourceGroupName = "txn-file-test"
			committer.resourceGroupTagger = func(req *tikvrpc.Request) {
				taggerCalls++
				require.Equal(t, tt.requestType, req.Type)
				require.NotNil(t, req.ResourceControlContext)
				require.Equal(t, "txn-file-test", req.ResourceControlContext.ResourceGroupName)
				switch req.Type {
				case tikvrpc.CmdPrewrite:
					prewrite := req.Prewrite()
					require.Len(t, prewrite.Mutations, 1)
					require.Equal(t, batch.firstKey, prewrite.Mutations[0].Key)
					prewrite.PrimaryLock[0] = 'x'
					prewrite.TxnFileChunks[0] = 99
					req.ResourceControlContext.ResourceGroupName = "tagger-mutated"
				case tikvrpc.CmdCommit:
					require.Equal(t, batch.sampleDataKeys, req.Commit().Keys)
				case tikvrpc.CmdBatchRollback:
					require.Equal(t, batch.sampleDataKeys, req.BatchRollback().Keys)
				}
				req.ResourceGroupTag = []byte("dynamic-tag")
			}

			_, err := tt.action.executeBatch(committer, bo, batch)

			require.NoError(t, err)
			require.Equal(t, 1, taggerCalls)
		})
	}
}

func TestTxnFileActionsPreserveStaticResourceGroupTag(t *testing.T) {
	tests := []struct {
		name        string
		action      txnFileAction
		requestType tikvrpc.CmdType
		newResponse func() *tikvrpc.Response
	}{
		{
			name:        "prewrite",
			action:      txnFilePrewriteAction{},
			requestType: tikvrpc.CmdPrewrite,
			newResponse: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}
			},
		},
		{
			name:        "commit",
			action:      txnFileCommitAction{},
			requestType: tikvrpc.CmdCommit,
			newResponse: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{}}
			},
		},
		{
			name:        "rollback",
			action:      txnFileRollbackAction{},
			requestType: tikvrpc.CmdBatchRollback,
			newResponse: func() *tikvrpc.Response {
				return &tikvrpc.Response{Resp: &kvrpcpb.BatchRollbackResponse{}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taggerCalls := 0
			committer, bo, batch := newTxnFileCommitTestBatch(t, func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
				require.Equal(t, tt.requestType, req.Type)
				require.Equal(t, []byte("static-tag"), req.ResourceGroupTag)
				return tt.newResponse(), nil
			})
			committer.resourceGroupTag = []byte("static-tag")
			committer.resourceGroupTagger = func(*tikvrpc.Request) {
				taggerCalls++
			}

			_, err := tt.action.executeBatch(committer, bo, batch)

			require.NoError(t, err)
			require.Zero(t, taggerCalls)
		})
	}
}

func TestTxnFilePrewriteTaggerUsesFirstKeyWithoutSampleDataKeys(t *testing.T) {
	taggerCalls := 0
	committer, bo, batch := newTxnFileCommitTestBatch(t, func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		require.Empty(t, req.ResourceGroupTag)
		require.Empty(t, req.Prewrite().Mutations)
		return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
	})
	batch.sampleDataKeys = nil
	committer.resourceGroupTagger = func(req *tikvrpc.Request) {
		taggerCalls++
		require.Len(t, req.Prewrite().Mutations, 1)
		require.Equal(t, batch.firstKey, req.Prewrite().Mutations[0].Key)
	}

	_, err := (txnFilePrewriteAction{}).executeBatch(committer, bo, batch)

	require.NoError(t, err)
	require.Equal(t, 1, taggerCalls)
}

func TestTxnFilePrewriteTaggerAppliesWithoutFirstKey(t *testing.T) {
	taggerCalls := 0
	committer, bo, batch := newTxnFileCommitTestBatch(t, func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
		require.Equal(t, []byte("metadata-tag"), req.ResourceGroupTag)
		require.Empty(t, req.Prewrite().Mutations)
		return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
	})
	batch.firstKey = nil
	batch.sampleDataKeys = nil
	committer.resourceGroupTagger = func(req *tikvrpc.Request) {
		taggerCalls++
		require.Empty(t, req.Prewrite().Mutations)
		req.ResourceGroupTag = []byte("metadata-tag")
	}

	_, err := (txnFilePrewriteAction{}).executeBatch(committer, bo, batch)

	require.NoError(t, err)
	require.Equal(t, 1, taggerCalls)
}

func TestTxnFileCommitPrimaryRPCErrorMarksResultUndetermined(t *testing.T) {
	committer, bo, batch := newTxnFileCommitTestBatch(t, func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
		return nil, context.Canceled
	})

	_, err := (txnFileCommitAction{}).executeBatch(committer, bo, batch)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, context.Canceled, errors.Cause(committer.getUndeterminedErr()))
}

func TestTxnFileCommitSecondaryRPCErrorIsNotResultUndetermined(t *testing.T) {
	committer, bo, batch := newTxnFileCommitTestBatch(t, func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
		return nil, context.Canceled
	})
	batch.isPrimary = false

	_, err := (txnFileCommitAction{}).executeBatch(committer, bo, batch)

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, committer.getUndeterminedErr())
}

func TestTxnFileCommitClearsUndeterminedErrOnDefinitivePrimaryResponse(t *testing.T) {
	tests := []struct {
		name string
		resp *kvrpcpb.CommitResponse
	}{
		{
			name: "success",
			resp: &kvrpcpb.CommitResponse{},
		},
		{
			name: "key error",
			resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{Abort: "aborted"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			committer, bo, batch := newTxnFileCommitTestBatch(t, func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
				return &tikvrpc.Response{Resp: tt.resp}, nil
			})
			committer.setUndeterminedErr(errors.New("stale RPC error"))

			_, err := (txnFileCommitAction{}).executeBatch(committer, bo, batch)

			if tt.resp.GetError() == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Nil(t, committer.getUndeterminedErr())
		})
	}
}

func TestTxnFileCommitPrimaryUndeterminedRegionError(t *testing.T) {
	regionErr := &errorpb.Error{UndeterminedResult: &errorpb.UndeterminedResult{}}
	requestCount := 0
	committer, bo, batch := newTxnFileCommitTestBatch(t, func(context.Context, string, *tikvrpc.Request, time.Duration) (*tikvrpc.Response, error) {
		requestCount++
		return &tikvrpc.Response{Resp: &kvrpcpb.CommitResponse{RegionError: regionErr}}, nil
	})

	_, err := (txnFileCommitAction{}).executeBatch(committer, bo, batch)

	require.ErrorIs(t, err, tikverr.ErrResultUndetermined)
	require.Equal(t, regionErr.String(), errors.Cause(committer.getUndeterminedErr()).Error())
	require.Equal(t, 1, requestCount)
}

func TestTxnFileCommitPrimaryRPCErrorIsNormalized(t *testing.T) {
	pd := &mockPDClient{}
	regionCache := locate.NewTestRegionCache()
	regionCache.SetPDClient(pd)
	defer regionCache.Close()

	chunkWriter := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		_, err := w.Write([]byte(`{"chunk_id":1}`))
		assert.NoError(t, err)
	}))
	defer chunkWriter.Close()

	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVClient.TxnChunkWriterAddr = chunkWriter.Listener.Addr().String()
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(origCfg)
		once = sync.Once{}
		cli = nil
		errCli = nil
		scheme = ""
	}()

	once = sync.Once{}
	cli = nil
	errCli = nil
	scheme = ""

	var commitRequestCount atomic.Int64
	var rollbackRequestCount atomic.Int64
	store := &txnFileCommitTSStore{
		timestamps:  []uint64{2},
		oracle:      &txnFileCommitTSOracle{},
		regionCache: regionCache,
		client: &fnClient{onSend: func(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
			switch req.Type {
			case tikvrpc.CmdPrewrite:
				return &tikvrpc.Response{Resp: &kvrpcpb.PrewriteResponse{}}, nil
			case tikvrpc.CmdCommit:
				commitRequestCount.Add(1)
				return nil, context.Canceled
			case tikvrpc.CmdBatchRollback:
				rollbackRequestCount.Add(1)
				return &tikvrpc.Response{Resp: &kvrpcpb.BatchRollbackResponse{}}, nil
			default:
				return nil, errors.Errorf("unexpected request type %s", req.Type)
			}
		}},
	}
	memDB := unionstore.NewMemDB()
	require.NoError(t, memDB.Set([]byte("k"), []byte("v")))
	txn := &KVTxn{
		store:              store,
		startTS:            1,
		startTime:          time.Now(),
		schemaVer:          txnFileSchemaVer(10),
		schemaLeaseChecker: &txnFileSchemaLeaseChecker{},
		scope:              oracle.GlobalTxnScope,
		vars:               tikv.DefaultVars,
		us:                 unionstore.NewUnionStore(memDB, nil),
	}
	committer := &twoPhaseCommitter{
		store:         store,
		txn:           txn,
		startTS:       txn.startTS,
		regionTxnSize: map[uint64]int{},
	}
	require.NoError(t, committer.initKeysAndMutations(context.Background()))
	committer.state = stateRunning

	err := committer.executeTxnFile(context.Background())

	require.ErrorIs(t, err, tikverr.ErrResultUndetermined)
	require.Equal(t, context.Canceled, errors.Cause(committer.getUndeterminedErr()))
	require.Equal(t, int64(1), commitRequestCount.Load())
	require.Zero(t, rollbackRequestCount.Load())
}

func TestChunkSliceSortAndDedup(t *testing.T) {
	assert := assert.New(t)

	genRndChunkIDs := func() []uint64 {
		n := rand.Intn(10)
		ids := make([]uint64, 0, n)
		for i := 0; i < n; i++ {
			ids = append(ids, uint64(rand.Intn(n+n/2+1)))
		}
		return ids
	}

	for i := 0; i < 100; i++ {
		ids := genRndChunkIDs()
		t.Logf("ids: %v\n", ids)

		expected := make([]uint64, len(ids))
		copy(expected, ids)
		slices.Sort(expected)
		expected = slices.Compact(expected)

		chunkSlice := txnChunkSlice{
			chunkIDs:    make([]uint64, 0, len(ids)),
			chunkRanges: make([]txnChunkRange, 0, len(ids)),
		}
		for _, id := range ids {
			chunkSlice.chunkIDs = append(chunkSlice.chunkIDs, id)
			chunkSlice.chunkRanges = append(chunkSlice.chunkRanges, txnChunkRange{
				smallest: []byte(fmt.Sprintf("k%04d", id)),
				biggest:  []byte(fmt.Sprintf("k%04d_end", id)),
				entries:  id + 1,
			})
		}
		chunkSlice.sortAndDedup()

		assert.Equal(expected, chunkSlice.chunkIDs)
		for j, id := range expected {
			assert.Equal(fmt.Sprintf("k%04d", id), string(chunkSlice.chunkRanges[j].smallest),
				"smallest mismatch at index %d", j)
			assert.Equal(fmt.Sprintf("k%04d_end", id), string(chunkSlice.chunkRanges[j].biggest),
				"biggest mismatch at index %d", j)
			assert.Equal(id+1, chunkSlice.chunkRanges[j].entries,
				"entries mismatch at index %d", j)
		}
	}
}

func TestIsRequestSourceUseTxnFile(t *testing.T) {
	assert := assert.New(t)

	cases := []struct {
		reqSource *util.RequestSource
		whitelist []string
		expected  bool
	}{
		{
			reqSource: &util.RequestSource{RequestSourceInternal: false},
			whitelist: []string{},
			expected:  true,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{"ddl_modify_column"},
			expected:  true,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{"ddl_alter_partition", "ddl_modify_column"},
			expected:  true,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{},
			expected:  false,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{"ddl_alter_partition"},
			expected:  false,
		},
	}

	for _, c := range cases {
		conf := &config.Config{
			TiKVClient: config.TiKVClient{
				TxnFileRequestSourceWhitelist: c.whitelist,
			},
		}
		result := IsRequestSourceUseTxnFile(c.reqSource, conf)
		assert.Equal(c.expected, result, "Expected %v for request source %v with whitelist %v", c.expected, c.reqSource.RequestSourceType, c.whitelist)
	}
}

func TestUseTxnFileExcludesPipelinedTxn(t *testing.T) {
	restore := config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.TxnChunkWriterAddr = "127.0.0.1"
		conf.TiKVClient.TxnFileMinMutationSize = 0
	})
	t.Cleanup(restore)

	txn := newTestTxn(t, 1)
	txn.isPipelined = true
	committer := &twoPhaseCommitter{txn: txn.KVTxn}

	useTxnFile, err := committer.useTxnFile(context.Background())

	require.NoError(t, err)
	require.False(t, useTxnFile)
}

// stubKVStore implements kvstore with only GetRegionCache returning a real
// RegionCache backed by the mock PD client. All other methods panic because
// buildTxnFiles does not call them.
type stubKVStore struct {
	regionCache *locate.RegionCache
}

func (s *stubKVStore) GetRegionCache() *locate.RegionCache { return s.regionCache }
func (s *stubKVStore) SplitRegions(_ context.Context, _ [][]byte, _ bool, _ *int64) ([]uint64, error) {
	panic("not implemented")
}
func (s *stubKVStore) WaitScatterRegionFinish(_ context.Context, _ uint64, _ int) error {
	panic("not implemented")
}
func (s *stubKVStore) GetTimestampWithRetry(_ *retry.Backoffer, _ string) (uint64, error) {
	panic("not implemented")
}
func (s *stubKVStore) GetOracle() oracle.Oracle                  { panic("not implemented") }
func (s *stubKVStore) CurrentTimestamp(_ string) (uint64, error) { panic("not implemented") }
func (s *stubKVStore) SendReq(_ *retry.Backoffer, _ *tikvrpc.Request, _ locate.RegionVerID, _ time.Duration) (*tikvrpc.Response, error) {
	panic("not implemented")
}
func (s *stubKVStore) GetTiKVClient() client.Client           { panic("not implemented") }
func (s *stubKVStore) GetLockResolver() *txnlock.LockResolver { panic("not implemented") }
func (s *stubKVStore) Ctx() context.Context                   { panic("not implemented") }
func (s *stubKVStore) WaitGroup() *sync.WaitGroup             { panic("not implemented") }
func (s *stubKVStore) TxnLatches() *latch.LatchesScheduler    { panic("not implemented") }
func (s *stubKVStore) GetClusterID() uint64                   { return 0 }
func (s *stubKVStore) IsClose() bool                          { return false }
func (s *stubKVStore) Go(_ func()) error                      { panic("not implemented") }

func TestBuildTxnFilesEntryCounting(t *testing.T) {
	require := require.New(t)

	var chunkIDCounter atomic.Uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id := chunkIDCounter.Add(1)
		resp, _ := json.Marshal(map[string]uint64{"chunk_id": id})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	// Wire size per entry: 2 (key-len) + 3 (key "kXX") + 1 (op) + 4 (val-len) + 3 (val "vXX") = 13 bytes.
	// Flush condition: len(buf)+entrySize+4 > cap(buf), where +4 is the CRC trailer reserved space.
	// maxChunkSize=50: after 3 entries (39 bytes), 39+13+4=56 > 50 → flush; 2 entries: 26+13+4=43 ≤ 50 → fits.
	const maxChunkSize = 50

	origCfg := config.GetGlobalConfig()
	newCfg := *origCfg
	newCfg.TiKVClient.TxnChunkWriterAddr = srv.Listener.Addr().String()
	newCfg.TiKVClient.TxnChunkMaxSize = maxChunkSize
	newCfg.TiKVClient.TxnChunkWriterConcurrency = 4
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(origCfg)
		once = sync.Once{}
		cli = nil
		errCli = nil
		scheme = ""
	}()

	once = sync.Once{}
	cli = srv.Client()
	errCli = nil
	scheme = "http://"

	_, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(err)
	regionCache := locate.NewRegionCache(pdClient)
	defer regionCache.Close()

	store := &stubKVStore{regionCache: regionCache}

	memDB := unionstore.NewMemDB()
	ops := []kvrpcpb.Op{
		kvrpcpb.Op_Put,
		kvrpcpb.Op_Del,
		kvrpcpb.Op_Insert,
		kvrpcpb.Op_Lock,
		kvrpcpb.Op_CheckNotExists,
		kvrpcpb.Op_Put,
		kvrpcpb.Op_Del,
		kvrpcpb.Op_Insert,
		kvrpcpb.Op_Lock,
	}
	for i, op := range ops {
		key := []byte(fmt.Sprintf("k%02d", i))
		val := []byte(fmt.Sprintf("v%02d", i))
		flags := tikv.KeyFlags(0)
		_ = flags
		_ = op
		require.NoError(memDB.Set(key, val))
	}

	txn := &KVTxn{
		store:   store,
		startTS: 1,
		valid:   true,
		vars:    tikv.DefaultVars,
		us:      unionstore.NewUnionStore(memDB, nil),
	}

	muts := NewPlainMutations(len(ops))
	for i, op := range ops {
		key := []byte(fmt.Sprintf("k%02d", i))
		val := []byte(fmt.Sprintf("v%02d", i))
		muts.Push(op, key, val, false, false, false, false)
	}

	c := &twoPhaseCommitter{
		store:         store,
		txn:           txn,
		startTS:       1,
		regionTxnSize: map[uint64]int{},
	}

	bo := retry.NewBackofferWithVars(context.Background(), 60000, nil)
	require.NoError(c.buildTxnFiles(bo, &muts))

	slice := c.txnFileCtx.slice
	require.Equal(3, slice.Len(), "expected 3 chunks")

	totalEntries := uint64(0)
	for i := 0; i < slice.Len(); i++ {
		totalEntries += slice.chunkRanges[i].entries
	}
	require.Equal(uint64(len(ops)), totalEntries, "total entries must equal mutation count")

	for i := 0; i < slice.Len(); i++ {
		require.Equal(uint64(3), slice.chunkRanges[i].entries,
			"chunk %d should have 3 entries", i)
	}

	opsSeen := make(map[kvrpcpb.Op]bool)
	for i := 0; i < muts.Len(); i++ {
		opsSeen[muts.GetOp(i)] = true
	}
	require.True(opsSeen[kvrpcpb.Op_Put])
	require.True(opsSeen[kvrpcpb.Op_Del])
	require.True(opsSeen[kvrpcpb.Op_Insert])
	require.True(opsSeen[kvrpcpb.Op_Lock])
	require.True(opsSeen[kvrpcpb.Op_CheckNotExists])
}

// Ensure stubKVStore satisfies the kvstore interface at compile time.
var _ kvstore = (*stubKVStore)(nil)

// Ensure the codec used by the test RegionCache returns keyspace ID 0 (codecV1).
var _ apicodec.KeyspaceID = apicodec.NullspaceID
