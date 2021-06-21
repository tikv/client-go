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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/deadlock/deadlock_test.go
//

// Copyright 2019 PingCAP, Inc.
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

package deadlock

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testDeadlockSuite{})

type testDeadlockSuite struct{}

func (s *testDeadlockSuite) TestDeadlock(c *C) {
	detector := NewDetector()
	err := detector.Detect(1, 2, 100)
	c.Assert(err, IsNil)
	err = detector.Detect(2, 3, 200)
	c.Assert(err, IsNil)
	err = detector.Detect(3, 1, 300)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "deadlock(200)")
	detector.CleanUp(2)
	list2 := detector.waitForMap[2]
	c.Assert(list2, IsNil)

	// After cycle is broken, no deadlock now.
	err = detector.Detect(3, 1, 300)
	c.Assert(err, IsNil)
	list3 := detector.waitForMap[3]
	c.Assert(list3.txns, HasLen, 1)

	// Different keyHash grows the list.
	err = detector.Detect(3, 1, 400)
	c.Assert(err, IsNil)
	c.Assert(list3.txns, HasLen, 2)

	// Same waitFor and key hash doesn't grow the list.
	err = detector.Detect(3, 1, 400)
	c.Assert(err, IsNil)
	c.Assert(list3.txns, HasLen, 2)

	detector.CleanUpWaitFor(3, 1, 300)
	c.Assert(list3.txns, HasLen, 1)
	detector.CleanUpWaitFor(3, 1, 400)
	list3 = detector.waitForMap[3]
	c.Assert(list3, IsNil)
	detector.Expire(1)
	c.Assert(detector.waitForMap, HasLen, 1)
	detector.Expire(2)
	c.Assert(detector.waitForMap, HasLen, 0)
}
