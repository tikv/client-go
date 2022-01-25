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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/retry/backoff_test.go
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retry

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackoffWithMax(t *testing.T) {
	b := NewBackofferWithVars(context.TODO(), 2000, nil)
	err := b.BackoffWithMaxSleepTxnLockFast(5, errors.New("test"))

	assert.Nil(t, err)
	assert.Equal(t, 5, b.totalSleep)
}

func TestBackoffErrorType(t *testing.T) {
	// the actual maxSleep is multiplied by weight, which is 400ms
	b := NewBackofferWithVars(context.TODO(), 200, nil)
	err := b.Backoff(BoRegionMiss, errors.New("region miss")) // 2ms sleep
	assert.Nil(t, err)
	// 300 ms sleep in total
	for i := 0; i < 2; i++ {
		err = b.Backoff(BoMaxDataNotReady, errors.New("data not ready"))
		assert.Nil(t, err)
	}
	// sleep from ServerIsBusy is not counted
	err = b.Backoff(BoTiKVServerBusy, errors.New("server is busy"))
	assert.Nil(t, err)
	// 126ms sleep in total
	for i := 0; i < 6; i++ {
		err = b.Backoff(BoTxnNotFound, errors.New("txn not found"))
		assert.Nil(t, err)
	}
	// Next backoff should return error of backoff that sleeps for longest time.
	err = b.Backoff(BoTxnNotFound, errors.New("tikv rpc"))
	assert.ErrorIs(t, err, BoMaxDataNotReady.err)
}
