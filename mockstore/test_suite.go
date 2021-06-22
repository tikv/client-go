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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/mockstore/test_suite.go
//

// Copyright 2018 PingCAP, Inc.
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

package mockstore

import (
	"flag"
	"sync"

	"github.com/pingcap/check"
)

var (
	withTiKVGlobalLock sync.RWMutex
	// WithTiKV is the flag which indicates whether it runs with tikv.
	WithTiKV = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
)

// OneByOneSuite is a suite, When with-tikv flag is true, there is only one storage, so the test suite have to run one by one.
type OneByOneSuite struct{}

// SetUpSuite implements the interface check.Suite.
func (s *OneByOneSuite) SetUpSuite(c *check.C) {
	if *WithTiKV {
		withTiKVGlobalLock.Lock()
	} else {
		withTiKVGlobalLock.RLock()
	}
}

// TearDownSuite implements the interface check.Suite.
func (s *OneByOneSuite) TearDownSuite(c *check.C) {
	if *WithTiKV {
		withTiKVGlobalLock.Unlock()
	} else {
		withTiKVGlobalLock.RUnlock()
	}
}

// LockGlobalTiKV locks withTiKVGlobalLock.
func (s *OneByOneSuite) LockGlobalTiKV() {
	withTiKVGlobalLock.Lock()
}

// UnLockGlobalTiKV unlocks withTiKVGlobalLock
func (s *OneByOneSuite) UnLockGlobalTiKV() {
	withTiKVGlobalLock.Unlock()
}
