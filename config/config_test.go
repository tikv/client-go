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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/config/config_test.go
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

package config

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
)

var _ = SerialSuites(&testConfigSuite{})

func (s *testConfigSuite) TestParsePath(c *C) {
	etcdAddrs, disableGC, err := ParsePath("tikv://node1:2379,node2:2379")
	c.Assert(err, IsNil)
	c.Assert(etcdAddrs, DeepEquals, []string{"node1:2379", "node2:2379"})
	c.Assert(disableGC, IsFalse)

	_, _, err = ParsePath("tikv://node1:2379")
	c.Assert(err, IsNil)
	_, disableGC, err = ParsePath("tikv://node1:2379?disableGC=true")
	c.Assert(err, IsNil)
	c.Assert(disableGC, IsTrue)
}

func (s *testConfigSuite) TestTxnScopeValue(c *C) {
	c.Assert(failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`), IsNil)
	c.Assert(GetTxnScopeFromConfig(), Equals, "bj")
	c.Assert(failpoint.Enable("tikvclient/injectTxnScope", `return("")`), IsNil)
	c.Assert(GetTxnScopeFromConfig(), Equals, "global")
	c.Assert(failpoint.Enable("tikvclient/injectTxnScope", `return("global")`), IsNil)
	c.Assert(GetTxnScopeFromConfig(), Equals, "global")
	c.Assert(failpoint.Disable("tikvclient/injectTxnScope"), IsNil)
}
