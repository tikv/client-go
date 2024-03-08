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

package tikv_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

const (
	MinFlushKeys = 10000
	MinFlushSize = 16 * 1024 * 1024 // 16MB
)

func TestPipelinedMemDB(t *testing.T) {
	suite.Run(t, new(testPipelinedMemDBSuite))
}

type testPipelinedMemDBSuite struct {
	suite.Suite
	cluster testutils.Cluster
	store   *tikv.KVStore
}

func (s *testPipelinedMemDBSuite) SetupTest() {
	if *withTiKV {
		s.store = NewTestStore(s.T())
		return
	}

	client, pdClient, cluster, err := unistore.New("", nil)
	s.Require().Nil(err)

	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := tikv.NewTestTiKVStore(fpClient{Client: &unistoreClientWrapper{client}}, pdClient, nil, nil, 0)
	s.Require().Nil(err)

	s.store = store
}

func (s *testPipelinedMemDBSuite) TearDownTest() {
	s.store.Close()
}

func (s *testPipelinedMemDBSuite) TestPipelinedAndFlush() {
	ctx := context.Background()
	txn, err := s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	txn1, err := s.store.Begin()
	s.Nil(err)
	s.True(txn.IsPipelined())
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		txn.Set(key, value)
		flushed, err := txn.GetMemBuffer().Flush(false)
		s.Nil(err)
		if i < MinFlushKeys-1 {
			s.False(flushed)
		} else {
			s.True(flushed)
		}
		// txn can always read its own writes
		val, err := txn.Get(ctx, key)
		s.Nil(err)
		s.True(bytes.Equal(value, val))
		// txn1 cannot see it
		_, err = txn1.Get(ctx, key)
		s.True(tikverr.IsErrNotFound(err))
	}

	// commit the txn, then it's visible to other txns.
	txn.Commit(ctx)

	txn1, err = s.store.Begin()
	s.Nil(err)
	defer txn1.Rollback()
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		expect := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		val, err := txn1.Get(ctx, key)
		s.Nil(err)
		s.True(bytes.Equal(expect, val))
	}
}

func (s *testPipelinedMemDBSuite) TestPipelinedMemDBBufferGet() {
	ctx := context.Background()
	txn, err := s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		txn.Set(key, value)
		flushed, err := txn.GetMemBuffer().Flush(true)
		s.Nil(err)
		s.True(flushed)
	}
	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		expect := key
		val, err := txn.GetMemBuffer().Get(ctx, key)
		s.Nil(err)
		s.True(bytes.Equal(val, expect))
	}
	s.Nil(txn.GetMemBuffer().FlushWait())
	s.Nil(txn.Rollback())
}

func (s *testPipelinedMemDBSuite) TestPipelinedFlushBlock() {
	txn, err := s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	txn.Set([]byte("key1"), []byte("value1"))

	s.Nil(failpoint.Enable("tikvclient/beforePipelinedFlush", `pause`))
	flushed, err := txn.GetMemBuffer().Flush(true)
	s.Nil(err)
	s.True(flushed)

	txn.Set([]byte("key2"), []byte("value2"))
	flushReturned := make(chan struct{})
	go func() {
		flushed, err := txn.GetMemBuffer().Flush(true)
		s.Nil(err)
		s.True(flushed)
		close(flushReturned)
	}()

	oneSec := time.After(time.Second)
	select {
	case <-flushReturned:
		s.Fail("Flush should be blocked")
	case <-oneSec:
	}
	s.Nil(failpoint.Disable("tikvclient/beforePipelinedFlush"))
	<-flushReturned
	s.Nil(txn.GetMemBuffer().FlushWait())
}

func (s *testPipelinedMemDBSuite) TestPipelinedSkipFlushedLock() {
	txn, err := s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	txn.Set([]byte("key1"), []byte("value1"))
	flushed, err := txn.GetMemBuffer().Flush(true)
	s.Nil(err)
	s.True(flushed)
	s.Nil(txn.GetMemBuffer().FlushWait())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = txn.GetSnapshot().Get(ctx, []byte("key1"))
	s.True(tikverr.IsErrNotFound(err))
	s.Nil(txn.Commit(context.Background()))

	// can see it after commit
	txn, err = s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	defer txn.Rollback()
	val, err := txn.Get(context.Background(), []byte("key1"))
	s.Nil(err)
	s.Equal([]byte("value1"), val)
}

func (s *testPipelinedMemDBSuite) TestResolveLockRace() {
	// This test should use `go test -race` to run.
	// because pipelined memdb depends on ResolveLockRequest to commit the txn, we need to guarantee the ResolveLock won't lead to data race.
	s.Nil(failpoint.Enable("tikvclient/pipelinedCommitFail", `return`))
	defer func() {
		failpoint.Disable("tikvclient/pipelinedCommitFail")
	}()
	for i := 0; i < 100; i++ {
		txn, err := s.store.Begin(tikv.WithPipelinedMemDB())
		startTS := txn.StartTS()
		s.Nil(err)
		for j := 0; j < 100; j++ {
			key := []byte(strconv.Itoa(j))
			value := key
			txn.Set(key, value)
		}
		txn.Commit(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		bo := tikv.NewNoopBackoff(ctx)
		loc, err := s.store.GetRegionCache().LocateKey(bo, []byte("1"))
		s.Nil(err)
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, &kvrpcpb.ResolveLockRequest{StartVersion: startTS})
		go func() {
			time.Sleep(time.Duration(i) * time.Microsecond)
			cancel()
		}()
		s.store.SendReq(bo, req, loc.Region, 100*time.Millisecond)
	}
	time.Sleep(time.Second)
}

func (s *testPipelinedMemDBSuite) TestPipelinedCommit() {
	txn, err := s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		value := key
		txn.Set(key, value)
	}

	s.Nil(failpoint.Enable("tikvclient/pipelinedSkipResolveLock", `return`))
	defer func() {
		failpoint.Disable("tikvclient/pipelinedSkipResolveLock")
	}()
	s.Nil(txn.Commit(context.Background()))
	mockTableID := int64(999)
	_, err = s.store.SplitRegions(context.Background(), [][]byte{[]byte("50")}, false, &mockTableID)
	// manually commit
	done := make(chan struct{})
	go func() {
		committer := transaction.TxnProbe{KVTxn: txn}.GetCommitter()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()
		bo := retry.NewNoopBackoff(ctx)
		committer.ResolveFlushedLocks(bo, []byte("1"), []byte("99"))
		close(done)
	}()
	// should be done within 10 seconds.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.Fail("resolve lock timeout")
	}

	// check the result
	txn, err = s.store.Begin(tikv.WithPipelinedMemDB())
	s.Nil(err)
	for i := 0; i < 100; i++ {
		key := []byte(strconv.Itoa(i))
		val, err := txn.Get(context.Background(), key)
		s.Nil(err)
		s.Equal(key, val)
	}
}
