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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
)

func TestParsePath(t *testing.T) {
	etcdAddrs, disableGC, keyspaceName, err := ParsePath("tikv://node1:2379,node2:2379")

	assert.Nil(t, err)
	assert.Equal(t, []string{"node1:2379", "node2:2379"}, etcdAddrs)
	assert.False(t, disableGC)
	assert.Empty(t, keyspaceName)

	_, _, _, err = ParsePath("tikv://node1:2379")
	assert.Nil(t, err)

	_, disableGC, keyspaceName, err = ParsePath("tikv://node1:2379?disableGC=true&keyspaceName=DEFAULT")
	assert.Nil(t, err)
	assert.True(t, disableGC)
	assert.Equal(t, "DEFAULT", keyspaceName)
}

func TestTxnScopeValue(t *testing.T) {
	var err error

	err = failpoint.Enable("tikvclient/injectTxnScope", `return("bj")`)
	assert.Nil(t, err)
	assert.Equal(t, "bj", GetTxnScopeFromConfig())

	err = failpoint.Enable("tikvclient/injectTxnScope", `return("")`)
	assert.Nil(t, err)
	assert.Equal(t, "global", GetTxnScopeFromConfig())

	err = failpoint.Enable("tikvclient/injectTxnScope", `return("global")`)
	assert.Nil(t, err)
	assert.Equal(t, "global", GetTxnScopeFromConfig())

	err = failpoint.Disable("tikvclient/injectTxnScope")
	assert.Nil(t, err)
}
