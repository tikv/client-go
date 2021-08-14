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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/local_test.go
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

package oracles_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
)

func TestLocalOracle(t *testing.T) {
	l := oracles.NewLocalOracle()
	defer l.Close()
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts, err := l.GetTimestamp(context.Background(), &oracle.Option{})
		require.Nil(t, err)
		m[ts] = struct{}{}
	}

	assert.Len(t, m, 100000, "should generate same ts")
}

func TestIsExpired(t *testing.T) {
	o := oracles.NewLocalOracle()
	defer o.Close()

	start := time.Now()
	oracles.SetOracleHookCurrentTime(o, start)
	ts, _ := o.GetTimestamp(context.Background(), &oracle.Option{})
	oracles.SetOracleHookCurrentTime(o, start.Add(10*time.Millisecond))

	expire := o.IsExpired(ts, 5, &oracle.Option{})
	assert.True(t, expire)

	expire = o.IsExpired(ts, 200, &oracle.Option{})
	assert.False(t, expire)
}

func TestLocalOracle_UntilExpired(t *testing.T) {
	o := oracles.NewLocalOracle()
	defer o.Close()
	start := time.Now()
	oracles.SetOracleHookCurrentTime(o, start)
	ts, _ := o.GetTimestamp(context.Background(), &oracle.Option{})

	oracles.SetOracleHookCurrentTime(o, start.Add(10*time.Millisecond))
	assert.Equal(t, int64(-4), o.UntilExpired(ts, 6, &oracle.Option{}))
	assert.Equal(t, int64(4), o.UntilExpired(ts, 14, &oracle.Option{}))
}
