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
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
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

func (s *testSnapshotSuite) SetupSuite() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
	s.prefix = fmt.Sprintf("snapshot_%d", time.Now().Unix())
	s.rowNums = append(s.rowNums, 1, 100, 191)
}

func (s *testSnapshotSuite) TearDownSuite() {
	txn := s.beginTxn()
	scanner, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	s.Nil(err)
	s.NotNil(scanner)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		s.Nil(err)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	s.Nil(err)
	err = s.store.Close()
	s.Nil(err)
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

type contextKey string

func (s *testSnapshotSuite) TestSnapshotCache() {
	txn := s.beginTxn()
	s.Nil(txn.Set([]byte("x"), []byte("x")))
	s.Nil(txn.Delete([]byte("y"))) // store data is affected by othe)
	s.Nil(txn.Commit(context.Background()))

	txn = s.beginTxn()
	snapshot := txn.GetSnapshot()
	_, err := snapshot.BatchGet(context.Background(), [][]byte{[]byte("x"), []byte("y")})
	s.Nil(err)

	s.Nil(failpoint.Enable("tikvclient/snapshot-get-cache-fail", `return(true)`))
	ctx := context.WithValue(context.Background(), contextKey("TestSnapshotCache"), true)
	_, err = snapshot.Get(ctx, []byte("x"))
	s.Nil(err)

	_, err = snapshot.Get(ctx, []byte("y"))
	s.True(error.IsErrNotFound(err))

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
	x := []byte("x_key_TestSkipLargeTxnLock")
	y := []byte("y_key_TestSkipLargeTxnLock")
	txn := s.beginTxn()
	s.Nil(txn.Set(x, []byte("x")))
	s.Nil(txn.Set(y, []byte("y")))
	ctx := context.Background()
	committer, err := txn.NewCommitter(0)
	s.Nil(err)
	committer.SetLockTTL(3000)
	s.Nil(committer.PrewriteAllMutations(ctx))

	txn1 := s.beginTxn()
	// txn1 is not blocked by txn in the large txn protocol.
	_, err = txn1.Get(ctx, x)
	s.True(error.IsErrNotFound(err))

	res, err := toTiDBTxn(&txn1).BatchGet(ctx, toTiDBKeys([][]byte{x, y, []byte("z")}))
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
	s.True(error.IsErrNotFound(err))
	s.Less(time.Since(start), 500*time.Millisecond)

	// Commit the primary key
	committer.SetCommitTS(txn.StartTS() + 1)
	committer.CommitMutations(ctx)

	snapshot = s.store.GetSnapshot(math.MaxUint64)
	start = time.Now()
	// Point get secondary key. Should read committed data.
	value, err := snapshot.Get(ctx, y)
	s.Nil(err)
	s.Equal(value, []byte("y"))
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
	tikv.RecordRegionRequestRuntimeStats(reqStats.Stats, tikvrpc.CmdGet, time.Second)
	tikv.RecordRegionRequestRuntimeStats(reqStats.Stats, tikvrpc.CmdGet, time.Millisecond)
	snapshot := s.store.GetSnapshot(0)
	snapshot.SetRuntimeStats(&txnkv.SnapshotRuntimeStats{})
	snapshot.MergeRegionRequestStats(reqStats.Stats)
	snapshot.MergeRegionRequestStats(reqStats.Stats)
	bo := tikv.NewBackofferWithVars(context.Background(), 2000, nil)
	err := bo.BackoffWithMaxSleepTxnLockFast(5, errors.New("test"))
	s.Nil(err)
	snapshot.RecordBackoffInfo(bo)
	snapshot.RecordBackoffInfo(bo)
	expect := "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:10ms}"
	s.Equal(expect, snapshot.FormatStats())
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
		"total_process_time: 100ms, total_wait_time: 100ms, " +
		"scan_detail: {total_process_keys: 10, " +
		"total_process_keys_size: 10, " +
		"total_keys: 15, " +
		"get_snapshot_time: 500ns, " +
		"rocksdb: {delete_skipped_count: 5, " +
		"key_skipped_count: 1, " +
		"block: {cache_hit_count: 10, read_count: 20, read_byte: 15 Bytes}}}"
	s.Equal(expect, snapshot.FormatStats())
	snapshot.MergeExecDetail(detail)
	expect = "Get:{num_rpc:4, total_time:2s},txnLockFast_backoff:{num:2, total_time:10ms}, " +
		"total_process_time: 200ms, total_wait_time: 200ms, " +
		"scan_detail: {total_process_keys: 20, " +
		"total_process_keys_size: 20, " +
		"total_keys: 30, " +
		"get_snapshot_time: 1µs, " +
		"rocksdb: {delete_skipped_count: 10, " +
		"key_skipped_count: 2, " +
		"block: {cache_hit_count: 20, read_count: 40, read_byte: 30 Bytes}}}"
	s.Equal(expect, snapshot.FormatStats())
}

func (s *testSnapshotSuite) TestRCRead() {
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
		s.Equal(len(meetLocks), 0)
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
