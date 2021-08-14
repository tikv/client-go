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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/store_test.go
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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
)

func TestStore(t *testing.T) {
	suite.Run(t, new(testStoreSuite))
}

type testStoreSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testStoreSuite) SetupTest() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
}

func (s *testStoreSuite) TearDownTest() {
	s.Require().Nil(s.store.Close())
}

func (s *testStoreSuite) TestOracle() {
	s.store.GetOracle().Close()
	o := &oracles.MockOracle{}
	s.store.SetOracle(o)

	ctx := context.Background()
	t1, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 100, nil), oracle.GlobalTxnScope)
	s.Nil(err)
	t2, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 100, nil), oracle.GlobalTxnScope)
	s.Nil(err)
	s.Less(t1, t2)

	t1, err = o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
	s.Nil(err)
	t2, err = o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
	s.Nil(err)
	s.Less(t1, t2)
	f := o.GetLowResolutionTimestampAsync(ctx, &oracle.Option{})
	s.NotNil(f)
	_ = o.UntilExpired(0, 0, &oracle.Option{})

	// Check retry.
	var wg sync.WaitGroup
	wg.Add(2)

	o.Disable()
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		o.Enable()
	}()

	go func() {
		defer wg.Done()
		t3, err := s.store.GetTimestampWithRetry(tikv.NewBackofferWithVars(ctx, 5000, nil), oracle.GlobalTxnScope)
		s.Nil(err)
		s.Less(t2, t3)
		expired := s.store.GetOracle().IsExpired(t2, 50, &oracle.Option{})
		s.True(expired)
	}()

	wg.Wait()
}

type checkRequestClient struct {
	tikv.Client
	priority kvrpcpb.CommandPri
}

func (c *checkRequestClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if c.priority != req.Priority {
		if resp.Resp != nil {
			if getResp, ok := resp.Resp.(*kvrpcpb.GetResponse); ok {
				getResp.Error = &kvrpcpb.KeyError{
					Abort: "request check error",
				}
			}
		}
	}
	return resp, err
}

func (s *testStoreSuite) TestRequestPriority() {
	client := &checkRequestClient{
		Client: s.store.GetTiKVClient(),
	}
	s.store.SetTiKVClient(client)

	// Cover 2PC commit.
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	client.priority = kvrpcpb.CommandPri_High
	txn.SetPriority(txnkv.PriorityHigh)
	err = txn.Set([]byte("key"), []byte("value"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)

	// Cover the basic Get request.
	txn, err = s.store.Begin()
	s.Require().Nil(err)
	client.priority = kvrpcpb.CommandPri_Low
	txn.SetPriority(txnkv.PriorityLow)
	_, err = txn.Get(context.TODO(), []byte("key"))
	s.Nil(err)

	// A counter example.
	client.priority = kvrpcpb.CommandPri_Low
	txn.SetPriority(txnkv.PriorityNormal)
	_, err = txn.Get(context.TODO(), []byte("key"))
	// err is translated to "try again later" by backoffer, so doesn't check error value here.
	s.NotNil(err)

	// Cover Seek request.
	client.priority = kvrpcpb.CommandPri_High
	txn.SetPriority(txnkv.PriorityHigh)
	iter, err := txn.Iter([]byte("key"), nil)
	s.Nil(err)
	for iter.Valid() {
		s.Nil(iter.Next())
	}
	iter.Close()
}

func (s *testStoreSuite) TestFailBusyServerKV() {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	err = txn.Set([]byte("key"), []byte("value"))
	s.Nil(err)
	err = txn.Commit(context.Background())
	s.Nil(err)

	txn, err = s.store.Begin()
	s.Require().Nil(err)
	s.Nil(failpoint.Enable("tikvclient/tikvStoreSendReqResult", `1*return("busy")->return("")`))
	val, err := txn.Get(context.TODO(), []byte("key"))
	s.Nil(err)
	s.Equal(val, []byte("value"))
}
