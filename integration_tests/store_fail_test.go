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
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/tests/store_fail_test.go
//

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv_test

import (
	"context"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

func (s *testStoreSerialSuite) TestFailBusyServerKV(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	var wg sync.WaitGroup
	wg.Add(2)

	c.Assert(failpoint.Enable("tikvclient/rpcServerBusy", `return(true)`), IsNil)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		c.Assert(failpoint.Disable("tikvclient/rpcServerBusy"), IsNil)
	}()

	go func() {
		defer wg.Done()
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		val, err := txn.Get(context.TODO(), []byte("key"))
		c.Assert(err, IsNil)
		c.Assert(val, BytesEquals, []byte("value"))
	}()

	wg.Wait()
}
