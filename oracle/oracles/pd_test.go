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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/pd_test.go
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

package oracles_test

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/oracle/oracles"
	pd "github.com/tikv/pd/client"
)

func TestPDOracle_UntilExpired(t *testing.T) {
	lockAfter, lockExp := 10, 15
	o := oracles.NewEmptyPDOracle()
	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(start))
	lockTs := oracle.GoTimeToTS(start.Add(time.Duration(lockAfter)*time.Millisecond)) + 1
	waitTs := o.UntilExpired(lockTs, uint64(lockExp), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	assert.Equal(t, int64(lockAfter+lockExp), waitTs)
}

func TestPdOracle_GetStaleTimestamp(t *testing.T) {
	o := oracles.NewEmptyPDOracle()

	start := time.Now()
	oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(start))
	ts, err := o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 10)
	assert.Nil(t, err)
	assert.WithinDuration(t, start.Add(-10*time.Second), oracle.GetTimeFromTS(ts), 2*time.Second)

	_, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 1e12)
	assert.NotNil(t, err)
	assert.Regexp(t, ".*invalid prevSecond.*", err.Error())

	_, err = o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, math.MaxUint64)
	assert.NotNil(t, err)
	assert.Regexp(t, ".*invalid prevSecond.*", err.Error())
}

// A mock for pd.Client that only returns global transaction scoped
// timestamps at the same physical time with increasing logical time
type MockPdClient struct {
	pd.Client

	logicalTimestamp atomic.Int64
}

func (c *MockPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	return 0, c.logicalTimestamp.Add(1), nil
}

func TestPdOracle_SetLowResolutionTimestampUpdateInterval(t *testing.T) {
	pdClient := MockPdClient{}
	o := oracles.NewPdOracleWithClient(&pdClient)
	ctx := context.TODO()
	wg := sync.WaitGroup{}

	err := o.SetLowResolutionTimestampUpdateInterval(50 * time.Millisecond)
	assert.Nil(t, err)

	// First call to o.GetTimestamp just seeds the timestamp
	_, err = o.GetTimestamp(ctx, &oracle.Option{})
	assert.Nil(t, err)

	// Haven't started update loop yet so next call to GetTs should be 1
	// while the low resolution timestamp stays at 0
	lowRes, err := o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
	assert.Nil(t, err)
	ts, err := o.GetTimestamp(ctx, &oracle.Option{})
	assert.Nil(t, err)
	assert.Greater(t, ts, lowRes)

	waitForTimestampToChange := func(checkFrequency time.Duration) {
		currTs, err := o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
		assert.Nil(t, err)
		assert.Eventually(t, func() bool {
			nextTs, err := o.GetLowResolutionTimestamp(ctx, &oracle.Option{})
			assert.Nil(t, err)
			return nextTs > currTs
		}, 5*time.Second, checkFrequency)
	}

	// Time based unit tests are inherently flaky. To reduce that
	// this just asserts a loose lower and upper bound that should
	// not be affected by timing inconsistencies across platforms
	checkBounds := func(updateInterval time.Duration) {
		start := time.Now()
		waitForTimestampToChange(10 * time.Millisecond)
		waitForTimestampToChange(10 * time.Millisecond)
		elapsed := time.Since(start)
		assert.Greater(t, elapsed, updateInterval)
		assert.LessOrEqual(t, elapsed, 3*updateInterval)
	}

	oracles.StartTsUpdateLoop(o, ctx, &wg)
	// Check each update interval. Note that since these are in increasing
	// order the time for the new interval to take effect is always less
	// than the new interval. If we iterated in opposite order, then we'd have
	// to first wait for the timestamp to change before checking bounds.
	for _, updateInterval := range []time.Duration{
		50 * time.Millisecond,
		150 * time.Millisecond,
		500 * time.Millisecond} {
		err = o.SetLowResolutionTimestampUpdateInterval(updateInterval)
		assert.Nil(t, err)
		checkBounds(updateInterval)
	}

	o.Close()
	wg.Wait()
}

func TestNonFutureStaleTSO(t *testing.T) {
	o := oracles.NewEmptyPDOracle()
	oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(time.Now()))
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		now := time.Now()
		upperBound := now.Add(5 * time.Millisecond) // allow 5ms time drift

		closeCh := make(chan struct{})
		go func() {
			time.Sleep(100 * time.Microsecond)
			oracles.SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(now))
			close(closeCh)
		}()
	CHECK:
		for {
			select {
			case <-closeCh:
				break CHECK
			default:
				ts, err := o.GetStaleTimestamp(context.Background(), oracle.GlobalTxnScope, 0)
				assert.Nil(t, err)
				staleTime := oracle.GetTimeFromTS(ts)
				if staleTime.After(upperBound) && time.Since(now) < time.Millisecond /* only check staleTime within 1ms */ {
					assert.Less(t, staleTime, upperBound, i)
					t.FailNow()
				}
			}
		}
	}
}
