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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/mocktikv/mock_tikv_test.go
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

package mocktikv

import (
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lock(key, primary string, ts uint64) *kvrpcpb.LockInfo {
	return &kvrpcpb.LockInfo{
		Key:         []byte(key),
		PrimaryLock: []byte(primary),
		LockVersion: ts,
	}
}

func mustGetNone(t *testing.T, store MVCCStore, key string, ts uint64) {
	val, err := store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	assert.Nil(t, err)
	assert.Nil(t, val)
}

func mustGetErr(t *testing.T, store MVCCStore, key string, ts uint64) {
	val, err := store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	assert.Error(t, err)
	assert.Nil(t, val)
}

func mustGetOK(t *testing.T, store MVCCStore, key string, ts uint64, expect string) {
	val, err := store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_SI, nil)
	assert.Nil(t, err)
	assert.Equal(t, string(val), expect)
}

func mustGetRC(t *testing.T, store MVCCStore, key string, ts uint64, expect string) {
	val, err := store.Get([]byte(key), ts, kvrpcpb.IsolationLevel_RC, nil)
	assert.Nil(t, err)
	assert.Equal(t, string(val), expect)
}

func mustPutOK(t *testing.T, store MVCCStore, key, value string, startTS, commitTS uint64) {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations(key, value),
		PrimaryLock:  []byte(key),
		StartVersion: startTS,
	}
	errs := store.Prewrite(req)
	for _, err := range errs {
		assert.Nil(t, err)
	}
	err := store.Commit([][]byte{[]byte(key)}, startTS, commitTS)
	assert.Nil(t, err)
}

func mustDeleteOK(t *testing.T, store MVCCStore, key string, startTS, commitTS uint64) {
	mutations := []*kvrpcpb.Mutation{
		{
			Op:  kvrpcpb.Op_Del,
			Key: []byte(key),
		},
	}
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  []byte(key),
		StartVersion: startTS,
	}
	errs := store.Prewrite(req)
	for _, err := range errs {
		assert.Nil(t, err)
	}
	err := store.Commit([][]byte{[]byte(key)}, startTS, commitTS)
	assert.Nil(t, err)
}

func mustScanOK(t *testing.T, store MVCCStore, start string, limit int, ts uint64, expect ...string) {
	mustRangeScanOK(t, store, start, "", limit, ts, expect...)
}

func mustRangeScanOK(t *testing.T, store MVCCStore, start, end string, limit int, ts uint64, expect ...string) {
	assert := assert.New(t)
	pairs := store.Scan([]byte(start), []byte(end), limit, ts, kvrpcpb.IsolationLevel_SI, nil)
	assert.Equal(len(pairs)*2, len(expect))
	for i := 0; i < len(pairs); i++ {
		assert.Nil(pairs[i].Err)
		assert.Equal(pairs[i].Key, []byte(expect[i*2]))
		assert.Equal(string(pairs[i].Value), expect[i*2+1])
	}
}

func mustReverseScanOK(t *testing.T, store MVCCStore, end string, limit int, ts uint64, expect ...string) {
	mustRangeReverseScanOK(t, store, "", end, limit, ts, expect...)
}

func mustRangeReverseScanOK(t *testing.T, store MVCCStore, start, end string, limit int, ts uint64, expect ...string) {
	assert := assert.New(t)
	pairs := store.ReverseScan([]byte(start), []byte(end), limit, ts, kvrpcpb.IsolationLevel_SI, nil)
	assert.Equal(len(pairs)*2, len(expect))
	for i := 0; i < len(pairs); i++ {
		assert.Nil(pairs[i].Err)
		assert.Equal(pairs[i].Key, []byte(expect[i*2]))
		assert.Equal(string(pairs[i].Value), expect[i*2+1])
	}
}

func mustPrewriteOK(t *testing.T, store MVCCStore, mutations []*kvrpcpb.Mutation, primary string, startTS uint64) {
	mustPrewriteWithTTLOK(t, store, mutations, primary, startTS, 0)
}

func mustPrewriteWithTTLOK(t *testing.T, store MVCCStore, mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) {
	assert.True(t, mustPrewriteWithTTL(store, mutations, primary, startTS, ttl))
}

func mustCommitOK(t *testing.T, store MVCCStore, keys [][]byte, startTS, commitTS uint64) {
	err := store.Commit(keys, startTS, commitTS)
	assert.Nil(t, err)
}

func mustCommitErr(t *testing.T, store MVCCStore, keys [][]byte, startTS, commitTS uint64) {
	err := store.Commit(keys, startTS, commitTS)
	assert.NotNil(t, err)
}

func mustRollbackOK(t *testing.T, store MVCCStore, keys [][]byte, startTS uint64) {
	err := store.Rollback(keys, startTS)
	assert.Nil(t, err)
}

func mustRollbackErr(t *testing.T, store MVCCStore, keys [][]byte, startTS uint64) {
	err := store.Rollback(keys, startTS)
	assert.NotNil(t, err)
}

func mustScanLock(t *testing.T, store MVCCStore, maxTs uint64, expect []*kvrpcpb.LockInfo) {
	locks, err := store.ScanLock(nil, nil, maxTs)
	assert.Nil(t, err)
	assert.Equal(t, locks, expect)
}

func mustResolveLock(t *testing.T, store MVCCStore, startTS, commitTS uint64) {
	assert.Nil(t, store.ResolveLock(nil, nil, startTS, commitTS))
}

func mustBatchResolveLock(t *testing.T, store MVCCStore, txnInfos map[uint64]uint64) {
	assert.Nil(t, store.BatchResolveLock(nil, nil, txnInfos))
}

func mustGC(t *testing.T, store MVCCStore, safePoint uint64) {
	assert.Nil(t, store.GC(nil, nil, safePoint))
}

func mustDeleteRange(t *testing.T, store MVCCStore, startKey, endKey string) {
	assert.Nil(t, store.DeleteRange([]byte(startKey), []byte(endKey)))
}

func TestGet(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()
	mustGetNone(t, store, "x", 10)
	mustPutOK(t, store, "x", "x", 5, 10)
	mustGetNone(t, store, "x", 9)
	mustGetOK(t, store, "x", 10, "x")
	mustGetOK(t, store, "x", 11, "x")
}

func TestGetWithLock(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	key := "key"
	value := "value"
	mustPutOK(t, store, key, value, 5, 10)
	mutations := []*kvrpcpb.Mutation{{
		Op:  kvrpcpb.Op_Lock,
		Key: []byte(key),
	}}
	// test with lock's type is lock
	mustPrewriteOK(t, store, mutations, key, 20)
	mustGetOK(t, store, key, 25, value)
	mustCommitOK(t, store, [][]byte{[]byte(key)}, 20, 30)

	// test get with lock's max ts and primary key
	mustPrewriteOK(t, store, putMutations(key, "value2", "key2", "v5"), key, 40)
	mustGetErr(t, store, key, 41)
	mustGetErr(t, store, "key2", math.MaxUint64)
	mustGetOK(t, store, key, math.MaxUint64, "value")
}

func TestDelete(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPutOK(t, store, "x", "x5-10", 5, 10)
	mustDeleteOK(t, store, "x", 15, 20)
	mustGetNone(t, store, "x", 5)
	mustGetNone(t, store, "x", 9)
	mustGetOK(t, store, "x", 10, "x5-10")
	mustGetOK(t, store, "x", 19, "x5-10")
	mustGetNone(t, store, "x", 20)
	mustGetNone(t, store, "x", 21)
}

func TestCleanupRollback(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPutOK(t, store, "secondary", "s-0", 1, 2)
	mustPrewriteOK(t, store, putMutations("primary", "p-5", "secondary", "s-5"), "primary", 5)
	mustGetErr(t, store, "secondary", 8)
	mustGetErr(t, store, "secondary", 12)
	mustCommitOK(t, store, [][]byte{[]byte("primary")}, 5, 10)
	mustRollbackErr(t, store, [][]byte{[]byte("primary")}, 5)
}

func TestReverseScan(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	// ver10: A(10) - B(_) - C(10) - D(_) - E(10)
	mustPutOK(t, store, "A", "A10", 5, 10)
	mustPutOK(t, store, "C", "C10", 5, 10)
	mustPutOK(t, store, "E", "E10", 5, 10)

	checkV10 := func() {
		mustReverseScanOK(t, store, "Z", 0, 10)
		mustReverseScanOK(t, store, "Z", 1, 10, "E", "E10")
		mustReverseScanOK(t, store, "Z", 2, 10, "E", "E10", "C", "C10")
		mustReverseScanOK(t, store, "Z", 3, 10, "E", "E10", "C", "C10", "A", "A10")
		mustReverseScanOK(t, store, "Z", 4, 10, "E", "E10", "C", "C10", "A", "A10")
		mustReverseScanOK(t, store, "E\x00", 3, 10, "E", "E10", "C", "C10", "A", "A10")
		mustReverseScanOK(t, store, "C\x00", 3, 10, "C", "C10", "A", "A10")
		mustReverseScanOK(t, store, "C\x00", 4, 10, "C", "C10", "A", "A10")
		mustReverseScanOK(t, store, "B", 1, 10, "A", "A10")
		mustRangeReverseScanOK(t, store, "", "E", 5, 10, "C", "C10", "A", "A10")
		mustRangeReverseScanOK(t, store, "", "C\x00", 5, 10, "C", "C10", "A", "A10")
		mustRangeReverseScanOK(t, store, "A\x00", "C", 5, 10)
	}
	checkV10()

	// ver20: A(10) - B(20) - C(10) - D(20) - E(10)
	mustPutOK(t, store, "B", "B20", 15, 20)
	mustPutOK(t, store, "D", "D20", 15, 20)

	checkV20 := func() {
		mustReverseScanOK(t, store, "Z", 5, 20, "E", "E10", "D", "D20", "C", "C10", "B", "B20", "A", "A10")
		mustReverseScanOK(t, store, "C\x00", 5, 20, "C", "C10", "B", "B20", "A", "A10")
		mustReverseScanOK(t, store, "A\x00", 1, 20, "A", "A10")
		mustRangeReverseScanOK(t, store, "B", "D", 5, 20, "C", "C10", "B", "B20")
		mustRangeReverseScanOK(t, store, "B", "D\x00", 5, 20, "D", "D20", "C", "C10", "B", "B20")
		mustRangeReverseScanOK(t, store, "B\x00", "D\x00", 5, 20, "D", "D20", "C", "C10")
	}
	checkV10()
	checkV20()

	// ver30: A(_) - B(20) - C(10) - D(_) - E(10)
	mustDeleteOK(t, store, "A", 25, 30)
	mustDeleteOK(t, store, "D", 25, 30)

	checkV30 := func() {
		mustReverseScanOK(t, store, "Z", 5, 30, "E", "E10", "C", "C10", "B", "B20")
		mustReverseScanOK(t, store, "C", 1, 30, "B", "B20")
		mustReverseScanOK(t, store, "C\x00", 5, 30, "C", "C10", "B", "B20")
	}
	checkV10()
	checkV20()
	checkV30()

	// ver40: A(_) - B(_) - C(40) - D(40) - E(10)
	mustDeleteOK(t, store, "B", 35, 40)
	mustPutOK(t, store, "C", "C40", 35, 40)
	mustPutOK(t, store, "D", "D40", 35, 40)

	checkV40 := func() {
		mustReverseScanOK(t, store, "Z", 5, 40, "E", "E10", "D", "D40", "C", "C40")
		mustReverseScanOK(t, store, "Z", 5, 100, "E", "E10", "D", "D40", "C", "C40")
	}
	checkV10()
	checkV20()
	checkV30()
	checkV40()
}

func TestScan(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	// ver10: A(10) - B(_) - C(10) - D(_) - E(10)
	mustPutOK(t, store, "A", "A10", 5, 10)
	mustPutOK(t, store, "C", "C10", 5, 10)
	mustPutOK(t, store, "E", "E10", 5, 10)

	checkV10 := func() {
		mustScanOK(t, store, "", 0, 10)
		mustScanOK(t, store, "", 1, 10, "A", "A10")
		mustScanOK(t, store, "", 2, 10, "A", "A10", "C", "C10")
		mustScanOK(t, store, "", 3, 10, "A", "A10", "C", "C10", "E", "E10")
		mustScanOK(t, store, "", 4, 10, "A", "A10", "C", "C10", "E", "E10")
		mustScanOK(t, store, "A", 3, 10, "A", "A10", "C", "C10", "E", "E10")
		mustScanOK(t, store, "A\x00", 3, 10, "C", "C10", "E", "E10")
		mustScanOK(t, store, "C", 4, 10, "C", "C10", "E", "E10")
		mustScanOK(t, store, "F", 1, 10)
		mustRangeScanOK(t, store, "", "E", 5, 10, "A", "A10", "C", "C10")
		mustRangeScanOK(t, store, "", "C\x00", 5, 10, "A", "A10", "C", "C10")
		mustRangeScanOK(t, store, "A\x00", "C", 5, 10)
	}
	checkV10()

	// ver20: A(10) - B(20) - C(10) - D(20) - E(10)
	mustPutOK(t, store, "B", "B20", 15, 20)
	mustPutOK(t, store, "D", "D20", 15, 20)

	checkV20 := func() {
		mustScanOK(t, store, "", 5, 20, "A", "A10", "B", "B20", "C", "C10", "D", "D20", "E", "E10")
		mustScanOK(t, store, "C", 5, 20, "C", "C10", "D", "D20", "E", "E10")
		mustScanOK(t, store, "D\x00", 1, 20, "E", "E10")
		mustRangeScanOK(t, store, "B", "D", 5, 20, "B", "B20", "C", "C10")
		mustRangeScanOK(t, store, "B", "D\x00", 5, 20, "B", "B20", "C", "C10", "D", "D20")
		mustRangeScanOK(t, store, "B\x00", "D\x00", 5, 20, "C", "C10", "D", "D20")
	}
	checkV10()
	checkV20()

	// ver30: A(_) - B(20) - C(10) - D(_) - E(10)
	mustDeleteOK(t, store, "A", 25, 30)
	mustDeleteOK(t, store, "D", 25, 30)

	checkV30 := func() {
		mustScanOK(t, store, "", 5, 30, "B", "B20", "C", "C10", "E", "E10")
		mustScanOK(t, store, "A", 1, 30, "B", "B20")
		mustScanOK(t, store, "C\x00", 5, 30, "E", "E10")
	}
	checkV10()
	checkV20()
	checkV30()

	// ver40: A(_) - B(_) - C(40) - D(40) - E(10)
	mustDeleteOK(t, store, "B", 35, 40)
	mustPutOK(t, store, "C", "C40", 35, 40)
	mustPutOK(t, store, "D", "D40", 35, 40)

	checkV40 := func() {
		mustScanOK(t, store, "", 5, 40, "C", "C40", "D", "D40", "E", "E10")
		mustScanOK(t, store, "", 5, 100, "C", "C40", "D", "D40", "E", "E10")
	}
	checkV10()
	checkV20()
	checkV30()
	checkV40()
}

func TestBatchGet(t *testing.T) {
	assert := assert.New(t)
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPutOK(t, store, "k1", "v1", 1, 2)
	mustPutOK(t, store, "k2", "v2", 1, 2)
	mustPutOK(t, store, "k2", "v2", 3, 4)
	mustPutOK(t, store, "k3", "v3", 1, 2)
	batchKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	pairs := store.BatchGet(batchKeys, 5, kvrpcpb.IsolationLevel_SI, nil)
	for _, pair := range pairs {
		assert.Nil(pair.Err)
	}
	assert.Equal(string(pairs[0].Value), "v1")
	assert.Equal(string(pairs[1].Value), "v2")
	assert.Equal(string(pairs[2].Value), "v3")
}

func TestScanLock(t *testing.T) {
	assert := assert.New(t)
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPutOK(t, store, "k1", "v1", 1, 2)
	mustPrewriteOK(t, store, putMutations("p1", "v5", "s1", "v5"), "p1", 5)
	mustPrewriteOK(t, store, putMutations("p2", "v10", "s2", "v10"), "p2", 10)
	mustPrewriteOK(t, store, putMutations("p3", "v20", "s3", "v20"), "p3", 20)

	locks, err := store.ScanLock([]byte("a"), []byte("r"), 12)
	assert.Nil(err)
	assert.Equal(locks, []*kvrpcpb.LockInfo{
		lock("p1", "p1", 5),
		lock("p2", "p2", 10),
	})

	mustScanLock(t, store, 10, []*kvrpcpb.LockInfo{
		lock("p1", "p1", 5),
		lock("p2", "p2", 10),
		lock("s1", "p1", 5),
		lock("s2", "p2", 10),
	})
}

func TestScanWithResolvedLock(t *testing.T) {
	assert := assert.New(t)
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPrewriteOK(t, store, putMutations("p1", "v5", "s1", "v5"), "p1", 5)
	mustPrewriteOK(t, store, putMutations("p2", "v10", "s2", "v10"), "p1", 5)

	pairs := store.Scan([]byte("p1"), nil, 3, 10, kvrpcpb.IsolationLevel_SI, nil)
	lock, ok := errors.Cause(pairs[0].Err).(*ErrLocked)
	assert.True(ok)
	_, ok = errors.Cause(pairs[1].Err).(*ErrLocked)
	assert.True(ok)

	// Mock the request after resolving lock.
	pairs = store.Scan([]byte("p1"), nil, 3, 10, kvrpcpb.IsolationLevel_SI, []uint64{lock.StartTS})
	for _, pair := range pairs {
		assert.Nil(pair.Err)
	}
}

func TestCommitConflict(t *testing.T) {
	assert := assert.New(t)
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	// txn A want set x to A
	// txn B want set x to B
	// A prewrite.
	mustPrewriteOK(t, store, putMutations("x", "A"), "x", 5)
	// B prewrite and find A's lock.
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("x", "B"),
		PrimaryLock:  []byte("x"),
		StartVersion: 10,
	}
	errs := store.Prewrite(req)
	assert.NotNil(errs[0])
	// B find rollback A because A exist too long.
	mustRollbackOK(t, store, [][]byte{[]byte("x")}, 5)
	// if A commit here, it would find its lock removed, report error txn not found.
	mustCommitErr(t, store, [][]byte{[]byte("x")}, 5, 10)
	// B prewrite itself after it rollback A.
	mustPrewriteOK(t, store, putMutations("x", "B"), "x", 10)
	// if A commit here, it would find its lock replaced by others and commit fail.
	mustCommitErr(t, store, [][]byte{[]byte("x")}, 5, 20)
	// B commit success.
	mustCommitOK(t, store, [][]byte{[]byte("x")}, 10, 20)
	// if B commit again, it will success because the key already committed.
	mustCommitOK(t, store, [][]byte{[]byte("x")}, 10, 20)
}

func TestResolveLock(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPrewriteOK(t, store, putMutations("p1", "v5", "s1", "v5"), "p1", 5)
	mustPrewriteOK(t, store, putMutations("p2", "v10", "s2", "v10"), "p2", 10)
	mustResolveLock(t, store, 5, 0)
	mustResolveLock(t, store, 10, 20)
	mustGetNone(t, store, "p1", 20)
	mustGetNone(t, store, "s1", 30)
	mustGetOK(t, store, "p2", 20, "v10")
	mustGetOK(t, store, "s2", 30, "v10")
	mustScanLock(t, store, 30, nil)
}

func TestBatchResolveLock(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPrewriteOK(t, store, putMutations("p1", "v11", "s1", "v11"), "p1", 11)
	mustPrewriteOK(t, store, putMutations("p2", "v12", "s2", "v12"), "p2", 12)
	mustPrewriteOK(t, store, putMutations("p3", "v13"), "p3", 13)
	mustPrewriteOK(t, store, putMutations("p4", "v14", "s3", "v14", "s4", "v14"), "p4", 14)
	mustPrewriteOK(t, store, putMutations("p5", "v15", "s5", "v15"), "p5", 15)
	txnInfos := map[uint64]uint64{
		11: 0,
		12: 22,
		13: 0,
		14: 24,
	}
	mustBatchResolveLock(t, store, txnInfos)
	mustGetNone(t, store, "p1", 20)
	mustGetNone(t, store, "p3", 30)
	mustGetOK(t, store, "p2", 30, "v12")
	mustGetOK(t, store, "s4", 30, "v14")
	mustScanLock(t, store, 30, []*kvrpcpb.LockInfo{
		lock("p5", "p5", 15),
		lock("s5", "p5", 15),
	})
	txnInfos = map[uint64]uint64{
		15: 0,
	}
	mustBatchResolveLock(t, store, txnInfos)
	mustScanLock(t, store, 30, nil)
}

func TestGC(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()
	var safePoint uint64 = 100

	// Prepare data
	mustPutOK(t, store, "k1", "v1", 1, 2)
	mustPutOK(t, store, "k1", "v2", 11, 12)

	mustPutOK(t, store, "k2", "v1", 1, 2)
	mustPutOK(t, store, "k2", "v2", 11, 12)
	mustPutOK(t, store, "k2", "v3", 101, 102)

	mustPutOK(t, store, "k3", "v1", 1, 2)
	mustPutOK(t, store, "k3", "v2", 11, 12)
	mustDeleteOK(t, store, "k3", 101, 102)

	mustPutOK(t, store, "k4", "v1", 1, 2)
	mustDeleteOK(t, store, "k4", 11, 12)

	// Check prepared data
	mustGetOK(t, store, "k1", 5, "v1")
	mustGetOK(t, store, "k1", 15, "v2")
	mustGetOK(t, store, "k2", 5, "v1")
	mustGetOK(t, store, "k2", 15, "v2")
	mustGetOK(t, store, "k2", 105, "v3")
	mustGetOK(t, store, "k3", 5, "v1")
	mustGetOK(t, store, "k3", 15, "v2")
	mustGetNone(t, store, "k3", 105)
	mustGetOK(t, store, "k4", 5, "v1")
	mustGetNone(t, store, "k4", 105)

	mustGC(t, store, safePoint)

	mustGetNone(t, store, "k1", 5)
	mustGetOK(t, store, "k1", 15, "v2")
	mustGetNone(t, store, "k2", 5)
	mustGetOK(t, store, "k2", 15, "v2")
	mustGetOK(t, store, "k2", 105, "v3")
	mustGetNone(t, store, "k3", 5)
	mustGetOK(t, store, "k3", 15, "v2")
	mustGetNone(t, store, "k3", 105)
	mustGetNone(t, store, "k4", 5)
	mustGetNone(t, store, "k4", 105)
}

func TestRollbackAndWriteConflict(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPutOK(t, store, "test", "test", 1, 3)
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("lock", "lock", "test", "test1"),
		PrimaryLock:  []byte("test"),
		StartVersion: 2,
		LockTtl:      2,
	}
	errs := store.Prewrite(req)
	mustWriteWriteConflict(t, errs, 1)

	mustPutOK(t, store, "test", "test2", 5, 8)

	// simulate `getTxnStatus` for txn 2.
	assert.Nil(t, store.Cleanup([]byte("test"), 2, math.MaxUint64))
	req = &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("test", "test3"),
		PrimaryLock:  []byte("test"),
		StartVersion: 6,
		LockTtl:      1,
	}
	errs = store.Prewrite(req)
	mustWriteWriteConflict(t, errs, 0)
}

func TestDeleteRange(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	for i := 1; i <= 5; i++ {
		key := string(byte(i) + byte('0'))
		value := "v" + key
		mustPutOK(t, store, key, value, uint64(1+2*i), uint64(2+2*i))
	}

	mustScanOK(t, store, "0", 10, 20, "1", "v1", "2", "v2", "3", "v3", "4", "v4", "5", "v5")

	mustDeleteRange(t, store, "2", "4")
	mustScanOK(t, store, "0", 10, 30, "1", "v1", "4", "v4", "5", "v5")

	mustDeleteRange(t, store, "5", "5")
	mustScanOK(t, store, "0", 10, 40, "1", "v1", "4", "v4", "5", "v5")

	mustDeleteRange(t, store, "41", "42")
	mustScanOK(t, store, "0", 10, 50, "1", "v1", "4", "v4", "5", "v5")

	mustDeleteRange(t, store, "4\x00", "5\x00")
	mustScanOK(t, store, "0", 10, 60, "1", "v1", "4", "v4")

	mustDeleteRange(t, store, "0", "9")
	mustScanOK(t, store, "0", 10, 70)
}

func mustWriteWriteConflict(t *testing.T, errs []error, i int) {
	assert.NotNil(t, errs[i])
	_, ok := errs[i].(*ErrConflict)
	assert.True(t, ok)
}

func TestRC(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPutOK(t, store, "key", "v1", 5, 10)
	mustPrewriteOK(t, store, putMutations("key", "v2"), "key", 15)
	mustGetErr(t, store, "key", 20)
	mustGetRC(t, store, "key", 12, "v1")
	mustGetRC(t, store, "key", 20, "v1")
}

func TestCheckTxnStatus(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()
	assert := assert.New(t)

	startTS := uint64(5 << 18)
	mustPrewriteWithTTLOK(t, store, putMutations("pk", "val"), "pk", startTS, 666)

	ttl, commitTS, action, err := store.CheckTxnStatus([]byte("pk"), startTS, startTS+100, 666, false, false)
	assert.Nil(err)
	assert.Equal(ttl, uint64(666))
	assert.Equal(commitTS, uint64(0))
	assert.Equal(action, kvrpcpb.Action_MinCommitTSPushed)

	// MaxUint64 as callerStartTS shouldn't update minCommitTS but return Action_MinCommitTSPushed.
	ttl, commitTS, action, err = store.CheckTxnStatus([]byte("pk"), startTS, math.MaxUint64, 666, false, false)
	assert.Nil(err)
	assert.Equal(ttl, uint64(666))
	assert.Equal(commitTS, uint64(0))
	assert.Equal(action, kvrpcpb.Action_MinCommitTSPushed)
	mustCommitOK(t, store, [][]byte{[]byte("pk")}, startTS, startTS+101)

	ttl, commitTS, _, err = store.CheckTxnStatus([]byte("pk"), startTS, 0, 666, false, false)
	assert.Nil(err)
	assert.Equal(ttl, uint64(0))
	assert.Equal(commitTS, startTS+101)

	mustPrewriteWithTTLOK(t, store, putMutations("pk1", "val"), "pk1", startTS, 666)
	mustRollbackOK(t, store, [][]byte{[]byte("pk1")}, startTS)

	ttl, commitTS, action, err = store.CheckTxnStatus([]byte("pk1"), startTS, 0, 666, false, false)
	assert.Nil(err)
	assert.Equal(ttl, uint64(0))
	assert.Equal(commitTS, uint64(0))
	assert.Equal(action, kvrpcpb.Action_NoAction)

	mustPrewriteWithTTLOK(t, store, putMutations("pk2", "val"), "pk2", startTS, 666)
	currentTS := uint64(777 << 18)
	ttl, commitTS, action, err = store.CheckTxnStatus([]byte("pk2"), startTS, 0, currentTS, false, false)
	assert.Nil(err)
	assert.Equal(ttl, uint64(0))
	assert.Equal(commitTS, uint64(0))
	assert.Equal(action, kvrpcpb.Action_TTLExpireRollback)

	// Cover the TxnNotFound case.
	_, _, _, err = store.CheckTxnStatus([]byte("txnNotFound"), 5, 0, 666, false, false)
	assert.NotNil(err)
	notFound, ok := errors.Cause(err).(*ErrTxnNotFound)
	assert.True(ok)
	assert.Equal(notFound.StartTs, uint64(5))
	assert.Equal(string(notFound.PrimaryKey), "txnNotFound")

	ttl, commitTS, action, err = store.CheckTxnStatus([]byte("txnNotFound"), 5, 0, 666, true, false)
	assert.Nil(err)
	assert.Equal(ttl, uint64(0))
	assert.Equal(commitTS, uint64(0))
	assert.Equal(action, kvrpcpb.Action_LockNotExistRollback)

	// Check the rollback tombstone blocks this prewrite which comes with a smaller startTS.
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    putMutations("txnNotFound", "val"),
		PrimaryLock:  []byte("txnNotFound"),
		StartVersion: 4,
		MinCommitTs:  6,
	}
	errs := store.Prewrite(req)
	assert.NotNil(errs)
}

func TestRejectCommitTS(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()
	mustPrewriteOK(t, store, putMutations("x", "A"), "x", 5)
	// Push the minCommitTS
	_, _, _, err = store.CheckTxnStatus([]byte("x"), 5, 100, 100, false, false)
	assert.Nil(t, err)
	err = store.Commit([][]byte{[]byte("x")}, 5, 10)
	e, ok := errors.Cause(err).(*ErrCommitTSExpired)
	assert.True(t, ok)
	assert.Equal(t, e.MinCommitTs, uint64(101))
}

func TestMvccGetByKey(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()

	mustPrewriteOK(t, store, putMutations("q1", "v5"), "p1", 5)
	var istore interface{} = store
	debugger, ok := istore.(MVCCDebugger)
	assert.True(t, ok)
	mvccInfo := debugger.MvccGetByKey([]byte("q1"))
	except := &kvrpcpb.MvccInfo{
		Lock: &kvrpcpb.MvccLock{
			Type:       kvrpcpb.Op_Put,
			StartTs:    5,
			Primary:    []byte("p1"),
			ShortValue: []byte("v5"),
		},
	}
	assert.Equal(t, mvccInfo, except)
}

func TestTxnHeartBeat(t *testing.T) {
	store, err := NewMVCCLevelDB("")
	require.Nil(t, err)
	defer store.Close()
	assert := assert.New(t)

	mustPrewriteWithTTLOK(t, store, putMutations("pk", "val"), "pk", 5, 666)

	// Update the ttl
	ttl, err := store.TxnHeartBeat([]byte("pk"), 5, 888)
	assert.Nil(err)
	assert.Greater(ttl, uint64(666))

	// Advise ttl is small
	ttl, err = store.TxnHeartBeat([]byte("pk"), 5, 300)
	assert.Nil(err)
	assert.Greater(ttl, uint64(300))

	// The lock has already been clean up
	assert.Nil(store.Cleanup([]byte("pk"), 5, math.MaxUint64))
	_, err = store.TxnHeartBeat([]byte("pk"), 5, 1000)
	assert.NotNil(err)
}
