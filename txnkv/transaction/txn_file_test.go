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
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
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
		region:     location,
		sampleKeys: [][]byte{[]byte("k")},
		isPrimary:  true,
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

	require.ErrorContains(t, err, "txn-file batch is not the primary batch")
	require.Equal(t, 1, requestCount)
	require.Equal(t, uint64(2), committer.commitTS)
	require.Zero(t, store.timestampCalls)
	require.Zero(t, checker.calls)
	require.Zero(t, commitOracle.calls)
	require.Zero(t, upperBoundCalls)
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
