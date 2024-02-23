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
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/stretchr/testify/suite"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
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
