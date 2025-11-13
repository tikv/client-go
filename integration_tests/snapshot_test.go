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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/snapshot_test.go
//

// Copyright 2016 PingCAP, Inc.
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
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

func TestSnapshot(t *testing.T) {
	suite.Run(t, new(testSnapshotSuite))
}

type testSnapshotSuite struct {
	suite.Suite
	store   tikv.StoreProbe
	prefix  string
	rowNums []int
}

func (s *testSnapshotSuite) SetupTest() {
	s.prefix = fmt.Sprintf("test_snapshot_%d", time.Now().Unix())
	store := NewTestUniStore(s.T())
	s.store = tikv.StoreProbe{KVStore: store}
}

func (s *testSnapshotSuite) TearDownTest() {
	s.store.Close()
}

func (s *testSnapshotSuite) beginTxn() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	return txn
}

func (s *testSnapshotSuite) checkAll(keys [][]byte) {
	txn := s.beginTxn()
	snapshot := txn.GetSnapshot()
	m, err := snapshot.BatchGet(context.Background(), keys)
	s.Nil(err)

	scan, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	s.Nil(err)
	cnt := 0
	for scan.Valid() {
		cnt++
		k := scan.Key()
		v := scan.Value()
		v2, ok := m[string(k)]
		s.True(ok, fmt.Sprintf("key: %q", k))
		s.Equal(v, v2)
		scan.Next()
	}
	err = txn.Commit(context.Background())
	s.Nil(err)
	s.Len(m, cnt)
}

func (s *testSnapshotSuite) deleteKeys(keys [][]byte) {
	txn := s.beginTxn()
	for _, k := range keys {
		err := txn.Delete(k)
		s.Nil(err)
	}
	err := txn.Commit(context.Background())
	s.Nil(err)
}

func (s *testSnapshotSuite) TestBatchGet() {
	for _, rowNum := range s.rowNums {
		s.T().Logf("test BatchGet, length=%v", rowNum)
		txn := s.beginTxn()
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			s.Nil(err)
		}
		err := txn.Commit(context.Background())
		s.Nil(err)

		keys := makeKeys(rowNum, s.prefix)
		s.checkAll(keys)
		s.deleteKeys(keys)
	}
}

func (s *testSnapshotSuite) TestGetAndBatchGetWithRequireCommitTS() {
	if config.NextGen {
		s.T().Skip("NextGen does not support WithRequireCommitTS option")
	}

	txn := s.beginTxn()
	s.Nil(txn.Set([]byte("a"), []byte("a1")))
	s.Nil(txn.Set([]byte("b"), []byte("b1")))
	s.Nil(txn.Set([]byte("c"), []byte("c1")))
	s.Nil(txn.Commit(context.Background()))

	txn = s.beginTxn()
	snapshot := txn.GetSnapshot()

	entries, err := snapshot.BatchGet(context.Background(), [][]byte{[]byte("a"), []byte("b"), []byte("d")}, kv.WithRequireCommitTS())
	s.Nil(err)
	s.Len(entries, 2)
	s.Equal([]byte("a1"), entries["a"].Value)
	commitTS := entries["a"].CommitTS
	s.Greater(commitTS, uint64(0))
	s.Equal(kv.NewValueEntry([]byte("b1"), commitTS), entries["b"])

	entry, err := snapshot.Get(context.Background(), []byte("c"), kv.WithRequireCommitTS())
	s.Nil(err)
	s.Equal(kv.NewValueEntry([]byte("c1"), commitTS), entry)

	_, err = snapshot.Get(context.Background(), []byte("e"), kv.WithRequireCommitTS())
	s.EqualError(err, tikverr.ErrNotExist.Error())
}

func (s *testSnapshotSuite) TestSnapshotCache() {
	txn := s.beginTxn()
	s.Nil(txn.Set([]byte("x"), []byte("x")))
	s.Nil(txn.Delete([]byte("y"))) // delete should also be cached
	s.Nil(txn.Set([]byte("a"), []byte("a")))
	s.Nil(txn.Delete([]byte("b")))
	s.Nil(txn.Commit(context.Background()))

	txn = s.beginTxn()
	snapshot := txn.GetSnapshot()
	// generate cache by BatchGet
	_, err := snapshot.BatchGet(context.Background(), [][]byte{[]byte("x"), []byte("y")})
	s.Nil(err)
	// generate cache by Get
	_, err = snapshot.Get(context.Background(), []byte("a"))
	s.Nil(err)
	_, err = snapshot.Get(context.Background(), []byte("b"))
	s.True(tikverr.IsErrNotFound(err))

	s.Nil(failpoint.Enable("tikvclient/snapshot-get-cache-fail", `return(true)`))
	ctx := context.WithValue(context.Background(), "TestSnapshotCache", true)

	// check cache from BatchGet
	entries, err := snapshot.BatchGet(ctx, [][]byte{[]byte("x"), []byte("y")})
	s.Nil(err)
	s.Len(entries, 1)
	s.Equal(kv.NewValueEntry([]byte("x"), 0), entries["x"])

	// check cache from Get
	entry, err := snapshot.Get(ctx, []byte("a"))
	s.Nil(err)
	s.Equal(kv.NewValueEntry([]byte("a"), 0), entry)
	_, err = snapshot.Get(ctx, []byte("b"))
	s.True(tikverr.IsErrNotFound(err))

	s.Nil(failpoint.Disable("tikvclient/snapshot-get-cache-fail"))
	if config.NextGen {
		s.T().Skip("NextGen does not support WithRequireCommitTS option")
	}

	var expectedCommitTS uint64
	for i := range 2 {
		ctx := context.Background()
		if i == 1 {
			// WithRequireCommitTS will update the cache again
			s.Nil(failpoint.Enable("tikvclient/snapshot-get-cache-fail", `return(true)`))
			ctx = context.WithValue(context.Background(), "TestSnapshotCache", true)
		}

		// check BatchGet
		// When require commit ts, it should return the correct commit ts instead of the cached zero.
		entries, err = snapshot.BatchGet(ctx, [][]byte{[]byte("x"), []byte("y")}, kv.WithRequireCommitTS())
		s.Nil(err)
		s.Len(entries, 1)
		s.Equal([]byte("x"), entries["x"].Value)
		if commitTS := entries["x"].CommitTS; expectedCommitTS == 0 {
			s.Greater(commitTS, uint64(0))
			expectedCommitTS = commitTS
		} else {
			s.Equal(expectedCommitTS, commitTS)
		}

		// check Get
		// When require commit ts, it should return the correct commit ts instead of the cached zero.
		entry, err = snapshot.Get(ctx, []byte("a"), kv.WithRequireCommitTS())
		s.Nil(err)
		s.Equal(kv.NewValueEntry([]byte("a"), expectedCommitTS), entry)
		_, err = snapshot.Get(ctx, []byte("b"))
		s.True(tikverr.IsErrNotFound(err))
	}
	s.Nil(failpoint.Disable("tikvclient/snapshot-get-cache-fail"))
}

func (s *testSnapshotSuite) TestBatchGetNotExist() {
	for _, rowNum := range s.rowNums {
		s.T().Logf("test BatchGetNotExist, length=%v", rowNum)
		txn := s.beginTxn()
		for i := 0; i < rowNum; i++ {
			k := encodeKey(s.prefix, s08d("key", i))
			err := txn.Set(k, valueBytes(i))
			s.Nil(err)
		}
		err := txn.Commit(context.Background())
		s.Nil(err)

		keys := makeKeys(rowNum, s.prefix)
		keys = append(keys, []byte("noSuchKey"))
		s.checkAll(keys)
		s.deleteKeys(keys)
	}
}

func makeKeys(rowNum int, prefix string) [][]byte {
	keys := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSnapshotSuite) TestSkipLargeTxnLock() {
	x, y := encodeKey(s.prefix, "x_TestSkipLargeTxnLock"), encodeKey(s.prefix, "y_TestSkipLargeTxnLock")
	txn := s.beginTxn()
	s.Nil(txn.Set(x, []byte("x")))
	s.Nil(txn.Set(y, []byte("y")))
	ctx := context.Background()
	committer, err := txn.NewCommitter(1)
	s.Nil(err)
	committer.SetLockTTL(3000)
	s.False(committer.IsAsyncCommit())
	s.Nil(committer.PrewriteAllMutations(ctx))

	txn1 := s.beginTxn()
	// txn1 is not blocked by txn in the large txn protocol.
	r, err := txn1.Get(ctx, x)
	if err != nil {
		println(err.Error())
	} else {
		println(hex.EncodeToString(r.Value))
	}
	s.True(tikverr.IsErrNotFound(err))

	testKeys := [][]byte{encodeKey(s.prefix, "z")}
	res, err := toTiDBTxn(&txn1).BatchGet(ctx, toTiDBKeys(testKeys))
	s.Nil(err)
	s.Len(res, 0)

	// Commit txn, check the final commit ts is pushed.
	committer.SetCommitTS(txn.StartTS() + 1)
	s.Nil(committer.CommitMutations(ctx))
	status, err := s.store.GetLockResolver().GetTxnStatus(txn.StartTS(), 0, x)
	s.Nil(err)
	s.True(status.IsCommitted())
	s.Greater(status.CommitTS(), txn1.StartTS())
}

func (s *testSnapshotSuite) TestPointGetSkipTxnLock() {
	x := []byte("x_key_TestPointGetSkipTxnLock")
	y := []byte("y_key_TestPointGetSkipTxnLock")
	txn := s.beginTxn()
	s.Nil(txn.Set(x, []byte("x")))
	s.Nil(txn.Set(y, []byte("y")))
	ctx := context.Background()
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.SetLockTTL(3000)
	s.Nil(committer.PrewriteAllMutations(ctx))

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	start := time.Now()
	s.Equal(committer.GetPrimaryKey(), x)
	// Point get secondary key. Shouldn't be blocked by the lock and read old data.
	_, err = snapshot.Get(ctx, y)
	s.True(tikverr.IsErrNotFound(err))
	s.Less(time.Since(start), 500*time.Millisecond)

	// Commit the primary key
	committer.SetCommitTS(txn.StartTS() + 1)
	committer.CommitMutations(ctx)

	snapshot = s.store.GetSnapshot(math.MaxUint64)
	start = time.Now()
	// Point get secondary key. Should read committed data.
	value, err := snapshot.Get(ctx, y)
	s.Nil(err)
	s.Equal(value.Value, []byte("y"))
	s.Less(time.Since(start), 500*time.Millisecond)
}

func (s *testSnapshotSuite) TestSnapshotThreadSafe() {
	txn := s.beginTxn()
	key := []byte("key_test_snapshot_threadsafe")
	s.Nil(txn.Set(key, []byte("x")))
	ctx := context.Background()
	err := txn.Commit(context.Background())
	s.Nil(err)

	snapshot := s.store.GetSnapshot(math.MaxUint64)
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			for i := 0; i < 30; i++ {
				_, err := snapshot.Get(ctx, key)
				s.Nil(err)
				_, err = snapshot.BatchGet(ctx, [][]byte{key, []byte("key_not_exist")})
				s.Nil(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *testSnapshotSuite) TestSnapshotRuntimeStats() {
	reqStats := tikv.NewRegionRequestRuntimeStats()
	reqStats.RecordRPCRuntimeStats(tikvrpc.CmdGet, time.Second)
	reqStats.RecordRPCRuntimeStats(tikvrpc.CmdGet, time.Millisecond)
	snapshot := s.store.GetSnapshot(0)
	runtimeStats := &txnkv.SnapshotRuntimeStats{}
	snapshot.SetRuntimeStats(runtimeStats)
	snapshot.MergeRegionRequestStats(reqStats)
	snapshot.MergeRegionRequestStats(reqStats)
	bo := tikv.NewBackofferWithVars(context.Background(), 2000, nil)
	err := bo.BackoffWithMaxSleepTxnLockFast(5, errors.New("test"))
	s.Nil(err)
	snapshot.RecordBackoffInfo(bo)
	snapshot.RecordBackoffInfo(bo)
	expect := "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:10ms}"
	s.Equal(expect, snapshot.FormatStats())
	s.Equal(int64(4), runtimeStats.GetCmdRPCCount(tikvrpc.CmdGet))
	detail := &kvrpcpb.ExecDetailsV2{
		TimeDetail: &kvrpcpb.TimeDetail{
			WaitWallTimeMs:    100,
			ProcessWallTimeMs: 100,
		},
		ScanDetailV2: &kvrpcpb.ScanDetailV2{
			ProcessedVersions:         10,
			ProcessedVersionsSize:     10,
			TotalVersions:             15,
			GetSnapshotNanos:          500,
			RocksdbBlockReadCount:     20,
			RocksdbBlockReadByte:      15,
			RocksdbDeleteSkippedCount: 5,
			RocksdbKeySkippedCount:    1,
			RocksdbBlockCacheHitCount: 10,
		},
	}
	snapshot.MergeExecDetail(detail)
	expect = "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:10ms}, " +
		"time_detail: {total_process_time: 100ms, total_wait_time: 100ms}, " +
		"scan_detail: {total_process_keys: 10, total_process_keys_size: 10, total_keys: 15, get_snapshot_time: 500ns, " +
		"rocksdb: {delete_skipped_count: 5, key_skipped_count: 1, block: {cache_hit_count: 10, read_count: 20, read_byte: 15 Bytes}}}"
	s.Equal(expect, snapshot.FormatStats())
	snapshot.MergeExecDetail(detail)
	expect = "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:10ms}, " +
		"time_detail: {total_process_time: 200ms, total_wait_time: 200ms}, " +
		"scan_detail: {total_process_keys: 20, total_process_keys_size: 20, total_keys: 30, get_snapshot_time: 1µs, " +
		"rocksdb: {delete_skipped_count: 10, key_skipped_count: 2, block: {cache_hit_count: 20, read_count: 40, read_byte: 30 Bytes}}}"
	s.Equal(expect, snapshot.FormatStats())
	snapshot.GetResolveLockDetail().ResolveLockTime = int64(time.Second)
	expect = "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:10ms}, " +
		"time_detail: {total_process_time: 200ms, total_wait_time: 200ms}, " +
		"resolve_lock_time:1s, " +
		"scan_detail: {total_process_keys: 20, total_process_keys_size: 20, total_keys: 30, get_snapshot_time: 1µs, " +
		"rocksdb: {delete_skipped_count: 10, key_skipped_count: 2, block: {cache_hit_count: 20, read_count: 40, read_byte: 30 Bytes}}}"
	s.Equal(expect, snapshot.FormatStats())
}

func (s *testSnapshotSuite) TestRCRead() {
	// next-gen doesn't support RC yet, skip this test
	s.T().Skip("next-gen doesn't support RC yet, skip this test")
	for _, rowNum := range s.rowNums {
		s.T().Logf("test RC Read, length=%v", rowNum)
		txn := s.beginTxn()
		keys := makeKeys(rowNum, s.prefix)
		for i, k := range keys {
			err := txn.Set(k, valueBytes(i))
			s.Nil(err)
		}
		err := txn.Commit(context.Background())
		s.Nil(err)

		key0 := encodeKey(s.prefix, s08d("key", 0))
		txn1 := s.beginTxn()
		txn1.Set(key0, valueBytes(1))
		committer1, err := txn1.NewCommitter(1)
		s.Nil(err)
		err = committer1.PrewriteAllMutations(context.Background())
		s.Nil(err)

		var meetLocks []*txnkv.Lock
		resolver := tikv.NewLockResolverProb(s.store.GetLockResolver())
		resolver.SetMeetLockCallback(func(locks []*txnkv.Lock) {
			meetLocks = append(meetLocks, locks...)
		})
		// RC read
		txn2 := s.beginTxn()
		snapshot := txn2.GetSnapshot()
		snapshot.SetIsolationLevel(txnsnapshot.RC)
		// get
		v, err := snapshot.Get(context.Background(), key0)
		s.Nil(err)
		s.Equal(0, len(meetLocks))
		s.Equal(v, valueBytes(0))
		// batch get
		m, err := snapshot.BatchGet(context.Background(), keys)
		s.Nil(err)
		s.Equal(len(meetLocks), 0)
		s.Equal(len(m), rowNum)
		for i, k := range keys {
			s.Equal(m[string(k)], valueBytes(i))
		}

		committer1.Cleanup(context.Background())
		s.deleteKeys(keys)
	}
}

func (s *testSnapshotSuite) TestSnapshotCacheBypassMaxUint64() {
	txn := s.beginTxn()
	s.Nil(txn.Set([]byte("x"), []byte("x")))
	s.Nil(txn.Set([]byte("y"), []byte("y")))
	s.Nil(txn.Set([]byte("z"), []byte("z")))
	s.Nil(txn.Commit(context.Background()))
	// cache version < math.MaxUint64
	startTS, err := s.store.GetTimestampWithRetry(tikv.NewNoopBackoff(context.Background()), oracle.GlobalTxnScope)
	s.Nil(err)
	snapshot := s.store.GetSnapshot(startTS)
	snapshot.Get(context.Background(), []byte("x"))
	snapshot.BatchGet(context.Background(), [][]byte{[]byte("y"), []byte("z")})
	s.Equal(snapshot.SnapCache(), map[string]kv.ValueEntry{
		"x": kv.NewValueEntry([]byte("x"), 0),
		"y": kv.NewValueEntry([]byte("y"), 0),
		"z": kv.NewValueEntry([]byte("z"), 0),
	})
	// not cache version == math.MaxUint64
	snapshot = s.store.GetSnapshot(math.MaxUint64)
	snapshot.Get(context.Background(), []byte("x"))
	snapshot.BatchGet(context.Background(), [][]byte{[]byte("y"), []byte("z")})
	s.Empty(snapshot.SnapCache())
}

func (s *testSnapshotSuite) TestReplicaReadAdjuster() {
	originAsyncEnable := config.GetGlobalConfig().EnableAsyncBatchGet
	defer func() {
		cfg := config.GetGlobalConfig()
		cfg.EnableAsyncBatchGet = originAsyncEnable
		config.StoreGlobalConfig(cfg)
	}()
	regionIDs, err := s.store.SplitRegions(context.Background(), [][]byte{[]byte("y1")}, false, nil)
	s.Nil(err)
	for _, regionID := range regionIDs {
		var loc *tikv.KeyLocation
		s.Eventually(func() bool {
			loc, err = s.store.GetRegionCache().LocateRegionByID(retry.NewNoopBackoff(context.Background()), regionID)
			return err == nil
		}, 5*time.Second, time.Millisecond)
		region := s.store.GetRegionCache().GetCachedRegionWithRLock(loc.Region)
		s.NotNil(region)
		s.Equal(region.GetLeaderStoreID(), uint64(1))
	}
	stores := s.store.GetRegionCache().GetAllStores()
	var leaderStoreAddr string
	for _, store := range stores {
		if store.StoreID() == 1 {
			leaderStoreAddr = store.GetAddr()
			break
		}
	}
	s.NotEqual(leaderStoreAddr, "")
	for _, async := range []bool{true, false} {
		cfg := config.GetGlobalConfig()
		cfg.EnableAsyncBatchGet = async
		config.StoreGlobalConfig(cfg)
		for _, hit := range []bool{true, false} {
			txn := s.beginTxn()

			// check the replica read type
			fn := func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
				return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
					// When the request falls back to leader read or when the target replica is the leader,
					// ReplicaRead should be set to false to avoid read-index operations on the leader.
					s.Equal(hit && target != leaderStoreAddr, req.ReplicaRead)
					if hit {
						s.Equal(kv.ReplicaReadMixed, req.ReplicaReadType)
					} else {
						s.Equal(kv.ReplicaReadLeader, req.ReplicaReadType)
					}
					return next(target, req)
				}
			}
			txn.SetRPCInterceptor(interceptor.NewRPCInterceptor("check-req", fn))

			// set up the replica read and adaptive read
			txn.GetSnapshot().SetReplicaRead(kv.ReplicaReadMixed)
			txn.GetSnapshot().SetReplicaReadAdjuster(func(int) (tikv.StoreSelectorOption, kv.ReplicaReadType) {
				if hit {
					return tikv.WithMatchLabels([]*metapb.StoreLabel{
						{
							Key:   tikv.DCLabelKey,
							Value: "dc1",
						},
					}), kv.ReplicaReadMixed
				}
				return nil, kv.ReplicaReadLeader
			})
			txn.Get(context.Background(), []byte("x"))
			txn.BatchGet(context.Background(), [][]byte{[]byte("y"), []byte("z")})
			txn.Rollback()
		}
	}
}
