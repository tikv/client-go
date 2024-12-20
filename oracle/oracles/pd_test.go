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

package oracles

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

func TestPDOracle_UntilExpired(t *testing.T) {
	lockAfter, lockExp := 10, 15
	o := NewEmptyPDOracle()
	start := time.Now()
	SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(start))
	lockTs := oracle.GoTimeToTS(start.Add(time.Duration(lockAfter)*time.Millisecond)) + 1
	waitTs := o.UntilExpired(lockTs, uint64(lockExp), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	assert.Equal(t, int64(lockAfter+lockExp), waitTs)
}

func TestPdOracle_GetStaleTimestamp(t *testing.T) {
	o := NewEmptyPDOracle()

	start := time.Now()
	SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(start))
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
	o := NewPdOracleWithClient(&pdClient)
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

	StartTsUpdateLoop(o, ctx, &wg)
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
	o := NewEmptyPDOracle()
	SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(time.Now()))
	for i := 0; i < 100; i++ {
		time.Sleep(10 * time.Millisecond)
		now := time.Now()
		upperBound := now.Add(5 * time.Millisecond) // allow 5ms time drift

		closeCh := make(chan struct{})
		go func() {
			time.Sleep(100 * time.Microsecond)
			SetEmptyPDOracleLastTs(o, oracle.GoTimeToTS(now))
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

func TestAdaptiveUpdateTSInterval(t *testing.T) {
	oracleInterface, err := NewPdOracle(&MockPdClient{}, &PDOracleOptions{
		UpdateInterval: time.Second * 2,
		NoUpdateTS:     true,
	})
	assert.NoError(t, err)
	o := oracleInterface.(*pdOracle)
	defer o.Close()

	now := time.Now()

	mockTS := func(beforeNow time.Duration) uint64 {
		return oracle.ComposeTS(oracle.GetPhysical(now.Add(-beforeNow)), 1)
	}
	mustNotifyShrinking := func(expectedRequiredStaleness time.Duration) {
		// Normally this channel should be checked in pdOracle.updateTS method. Here we are testing the layer below the
		// updateTS method, so we just do this assert to ensure the message is sent to this channel.
		select {
		case requiredStaleness := <-o.adaptiveUpdateIntervalState.shrinkIntervalCh:
			assert.Equal(t, expectedRequiredStaleness, requiredStaleness)
		default:
			assert.Fail(t, "expects notifying shrinking update interval immediately, but no message received")
		}
	}
	mustNoNotify := func() {
		select {
		case <-o.adaptiveUpdateIntervalState.shrinkIntervalCh:
			assert.Fail(t, "expects not notifying shrinking update interval immediately, but message was received")
		default:
		}
	}

	now = now.Add(time.Second * 2)
	assert.Equal(t, time.Second*2, o.nextUpdateInterval(now, 0))
	now = now.Add(time.Second * 2)
	assert.Equal(t, time.Second*2, o.nextUpdateInterval(now, 0))
	assert.Equal(t, adaptiveUpdateTSIntervalStateNormal, o.adaptiveUpdateIntervalState.state)

	now = now.Add(time.Second)
	// Simulate a read requesting a staleness larger than 2s, in which case nothing special will happen.
	o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(mockTS(time.Second*3), mockTS(0), now)
	mustNoNotify()
	assert.Equal(t, time.Second*2, o.nextUpdateInterval(now, 0))

	now = now.Add(time.Second)
	// Simulate a read requesting a staleness less than 2s, in which case it should trigger immediate shrinking on the
	// update interval.
	o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(mockTS(time.Second), mockTS(0), now)
	mustNotifyShrinking(time.Second)
	expectedInterval := time.Second - adaptiveUpdateTSIntervalShrinkingPreserve
	assert.Equal(t, expectedInterval, o.nextUpdateInterval(now, time.Second))
	assert.Equal(t, adaptiveUpdateTSIntervalStateAdapting, o.adaptiveUpdateIntervalState.state)
	assert.Equal(t, now.UnixMilli(), o.adaptiveUpdateIntervalState.lastShortStalenessReadTime.Load())

	// Let read with short staleness continue happening.
	now = now.Add(adaptiveUpdateTSIntervalDelayBeforeRecovering / 2)
	o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(mockTS(time.Second), mockTS(0), now)
	mustNoNotify()
	assert.Equal(t, now.UnixMilli(), o.adaptiveUpdateIntervalState.lastShortStalenessReadTime.Load())

	// The adaptiveUpdateTSIntervalDelayBeforeRecovering has not been elapsed since the last time there is a read with short
	// staleness. The update interval won't start being reset at this time.
	now = now.Add(adaptiveUpdateTSIntervalDelayBeforeRecovering/2 + time.Second)
	o.adaptiveUpdateIntervalState.lastTick = now.Add(-time.Second)
	assert.Equal(t, expectedInterval, o.nextUpdateInterval(now, 0))
	assert.Equal(t, adaptiveUpdateTSIntervalStateAdapting, o.adaptiveUpdateIntervalState.state)

	// The adaptiveUpdateTSIntervalDelayBeforeRecovering has been elapsed.
	now = now.Add(adaptiveUpdateTSIntervalDelayBeforeRecovering / 2)
	o.adaptiveUpdateIntervalState.lastTick = now.Add(-time.Second)
	expectedInterval += adaptiveUpdateTSIntervalRecoverPerSecond
	assert.InEpsilon(t, expectedInterval.Seconds(), o.nextUpdateInterval(now, 0).Seconds(), 1e-3)
	assert.Equal(t, adaptiveUpdateTSIntervalStateRecovering, o.adaptiveUpdateIntervalState.state)
	o.adaptiveUpdateIntervalState.lastTick = now
	now = now.Add(time.Second * 2)
	// No effect if the required staleness didn't trigger the threshold.
	o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(mockTS(expectedInterval+adaptiveUpdateTSIntervalBlockRecoverThreshold*2), mockTS(0), now)
	mustNoNotify()
	expectedInterval += adaptiveUpdateTSIntervalRecoverPerSecond * 2
	assert.InEpsilon(t, expectedInterval.Seconds(), o.nextUpdateInterval(now, 0).Seconds(), 1e-3)
	assert.Equal(t, adaptiveUpdateTSIntervalStateRecovering, o.adaptiveUpdateIntervalState.state)

	// If there's a read operation requires a staleness that is close enough to the current adaptive update interval,
	// then block the update interval from recovering.
	o.adaptiveUpdateIntervalState.lastTick = now
	now = now.Add(time.Second)
	o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(mockTS(expectedInterval+adaptiveUpdateTSIntervalBlockRecoverThreshold/2), mockTS(0), now)
	mustNoNotify()
	assert.InEpsilon(t, expectedInterval.Seconds(), o.nextUpdateInterval(now, 0).Seconds(), 1e-3)
	assert.Equal(t, adaptiveUpdateTSIntervalStateAdapting, o.adaptiveUpdateIntervalState.state)
	o.adaptiveUpdateIntervalState.lastTick = now
	now = now.Add(time.Second)
	assert.InEpsilon(t, expectedInterval.Seconds(), o.nextUpdateInterval(now, 0).Seconds(), 1e-3)
	assert.Equal(t, adaptiveUpdateTSIntervalStateAdapting, o.adaptiveUpdateIntervalState.state)

	// Now adaptiveUpdateTSIntervalDelayBeforeRecovering + 1s has been elapsed. Continue recovering.
	now = now.Add(adaptiveUpdateTSIntervalDelayBeforeRecovering)
	o.adaptiveUpdateIntervalState.lastTick = now.Add(-time.Second)
	expectedInterval += adaptiveUpdateTSIntervalRecoverPerSecond
	assert.InEpsilon(t, expectedInterval.Seconds(), o.nextUpdateInterval(now, 0).Seconds(), 1e-3)
	assert.Equal(t, adaptiveUpdateTSIntervalStateRecovering, o.adaptiveUpdateIntervalState.state)

	// Without any other interruption, the update interval will gradually recover to the same value as configured.
	for {
		o.adaptiveUpdateIntervalState.lastTick = now
		now = now.Add(time.Second)
		expectedInterval += adaptiveUpdateTSIntervalRecoverPerSecond
		if expectedInterval >= time.Second*2 {
			break
		}
		assert.InEpsilon(t, expectedInterval.Seconds(), o.nextUpdateInterval(now, 0).Seconds(), 1e-3)
		assert.Equal(t, adaptiveUpdateTSIntervalStateRecovering, o.adaptiveUpdateIntervalState.state)
	}
	expectedInterval = time.Second * 2
	assert.Equal(t, expectedInterval, o.nextUpdateInterval(now, 0))
	assert.Equal(t, adaptiveUpdateTSIntervalStateNormal, o.adaptiveUpdateIntervalState.state)

	// Test adjusting configurations manually.
	// When the adaptive update interval is not taking effect, the actual used update interval follows the change of
	// the configuration immediately.
	err = o.SetLowResolutionTimestampUpdateInterval(time.Second * 1)
	assert.NoError(t, err)
	assert.Equal(t, time.Second, time.Duration(o.adaptiveLastTSUpdateInterval.Load()))
	assert.Equal(t, time.Second, o.nextUpdateInterval(now, 0))

	err = o.SetLowResolutionTimestampUpdateInterval(time.Second * 2)
	assert.NoError(t, err)
	assert.Equal(t, time.Second*2, time.Duration(o.adaptiveLastTSUpdateInterval.Load()))
	assert.Equal(t, time.Second*2, o.nextUpdateInterval(now, 0))

	// If the adaptive update interval is taking effect, the configuration change doesn't immediately affect the actual
	// update interval.
	now = now.Add(time.Second)
	o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(mockTS(time.Second), mockTS(0), now)
	mustNotifyShrinking(time.Second)
	expectedInterval = time.Second - adaptiveUpdateTSIntervalShrinkingPreserve
	assert.Equal(t, expectedInterval, o.nextUpdateInterval(now, time.Second))
	assert.Equal(t, adaptiveUpdateTSIntervalStateAdapting, o.adaptiveUpdateIntervalState.state)
	err = o.SetLowResolutionTimestampUpdateInterval(time.Second * 3)
	assert.NoError(t, err)
	assert.Equal(t, expectedInterval, time.Duration(o.adaptiveLastTSUpdateInterval.Load()))
	assert.Equal(t, expectedInterval, o.nextUpdateInterval(now, 0))
	err = o.SetLowResolutionTimestampUpdateInterval(time.Second)
	assert.NoError(t, err)
	assert.Equal(t, expectedInterval, time.Duration(o.adaptiveLastTSUpdateInterval.Load()))
	assert.Equal(t, expectedInterval, o.nextUpdateInterval(now, 0))

	// ...unless it's set to a value shorter than the current actual update interval.
	err = o.SetLowResolutionTimestampUpdateInterval(time.Millisecond * 800)
	assert.NoError(t, err)
	assert.Equal(t, time.Millisecond*800, time.Duration(o.adaptiveLastTSUpdateInterval.Load()))
	assert.Equal(t, time.Millisecond*800, o.nextUpdateInterval(now, 0))
	assert.Equal(t, adaptiveUpdateTSIntervalStateNormal, o.adaptiveUpdateIntervalState.state)

	// If the configured value is too short, the actual update interval won't be adaptive
	err = o.SetLowResolutionTimestampUpdateInterval(minAllowedAdaptiveUpdateTSInterval / 2)
	assert.NoError(t, err)
	assert.Equal(t, minAllowedAdaptiveUpdateTSInterval/2, time.Duration(o.adaptiveLastTSUpdateInterval.Load()))
	assert.Equal(t, minAllowedAdaptiveUpdateTSInterval/2, o.nextUpdateInterval(now, 0))
	assert.Equal(t, adaptiveUpdateTSIntervalStateUnadjustable, o.adaptiveUpdateIntervalState.state)
}

func TestValidateReadTS(t *testing.T) {
	testImpl := func(staleRead bool) {
		pdClient := MockPdClient{}
		o, err := NewPdOracle(&pdClient, &PDOracleOptions{
			UpdateInterval: time.Second * 2,
		})
		assert.NoError(t, err)
		defer o.Close()

		ctx := context.Background()
		opt := &oracle.Option{TxnScope: oracle.GlobalTxnScope}

		// Always returns error for MaxUint64
		err = o.ValidateReadTS(ctx, math.MaxUint64, staleRead, opt)
		if staleRead {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		ts, err := o.GetTimestamp(ctx, opt)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, ts, uint64(1))

		err = o.ValidateReadTS(ctx, 1, staleRead, opt)
		assert.NoError(t, err)
		ts, err = o.GetTimestamp(ctx, opt)
		assert.NoError(t, err)
		// The readTS exceeds the latest ts, so it first fails the check with the low resolution ts. Then it fallbacks to
		// the fetching-from-PD path, and it can get the previous ts + 1, which can allow this validation to pass.
		err = o.ValidateReadTS(ctx, ts+1, staleRead, opt)
		assert.NoError(t, err)
		// It can't pass if the readTS is newer than previous ts + 2.
		ts, err = o.GetTimestamp(ctx, opt)
		assert.NoError(t, err)
		err = o.ValidateReadTS(ctx, ts+2, staleRead, opt)
		assert.Error(t, err)

		// Simulate other PD clients requests a timestamp.
		ts, err = o.GetTimestamp(ctx, opt)
		assert.NoError(t, err)
		pdClient.logicalTimestamp.Add(2)
		err = o.ValidateReadTS(ctx, ts+3, staleRead, opt)
		assert.NoError(t, err)
	}

	testImpl(true)
	testImpl(false)
}

type MockPDClientWithPause struct {
	MockPdClient
	mu sync.Mutex
}

func (c *MockPDClientWithPause) GetTS(ctx context.Context) (int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.MockPdClient.GetTS(ctx)
}

func (c *MockPDClientWithPause) Pause() {
	c.mu.Lock()
}

func (c *MockPDClientWithPause) Resume() {
	c.mu.Unlock()
}

func TestValidateReadTSForStaleReadReusingGetTSResult(t *testing.T) {
	pdClient := &MockPDClientWithPause{}
	o, err := NewPdOracle(pdClient, &PDOracleOptions{
		UpdateInterval: time.Second * 2,
		NoUpdateTS:     true,
	})
	assert.NoError(t, err)
	defer o.Close()

	asyncValidate := func(ctx context.Context, readTS uint64) chan error {
		ch := make(chan error, 1)
		go func() {
			err := o.ValidateReadTS(ctx, readTS, true, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			ch <- err
		}()
		return ch
	}

	noResult := func(ch chan error) {
		select {
		case <-ch:
			assert.FailNow(t, "a ValidateReadTS operation is not blocked while it's expected to be blocked")
		default:
		}
	}

	cancelIndices := []int{-1, -1, 0, 1}
	for i, ts := range []uint64{100, 200, 300, 400} {
		// Note: the ts is the result that the next GetTS will return. Any validation with readTS <= ts should pass, otherwise fail.

		// We will cancel the cancelIndex-th validation call. This is for testing that canceling some of the calls
		// doesn't affect other calls that are waiting
		cancelIndex := cancelIndices[i]

		pdClient.Pause()

		results := make([]chan error, 0, 5)

		ctx, cancel := context.WithCancel(context.Background())

		getCtx := func(index int) context.Context {
			if cancelIndex == index {
				return ctx
			} else {
				return context.Background()
			}
		}

		results = append(results, asyncValidate(getCtx(0), ts-2))
		results = append(results, asyncValidate(getCtx(1), ts+2))
		results = append(results, asyncValidate(getCtx(2), ts-1))
		results = append(results, asyncValidate(getCtx(3), ts+1))
		results = append(results, asyncValidate(getCtx(4), ts))

		expectedSucceeds := []bool{true, false, true, false, true}

		time.Sleep(time.Millisecond * 50)
		for _, ch := range results {
			noResult(ch)
		}

		cancel()

		for i, ch := range results {
			if i == cancelIndex {
				select {
				case err := <-ch:
					assert.Errorf(t, err, "index: %v", i)
					assert.Containsf(t, err.Error(), "context canceled", "index: %v", i)
				case <-time.After(time.Second):
					assert.FailNowf(t, "expected result to be ready but still blocked", "index: %v", i)
				}
			} else {
				noResult(ch)
			}
		}

		// ts will be the next ts returned to these validation calls.
		pdClient.logicalTimestamp.Store(int64(ts - 1))
		pdClient.Resume()
		for i, ch := range results {
			if i == cancelIndex {
				continue
			}

			select {
			case err = <-ch:
			case <-time.After(time.Second):
				assert.FailNowf(t, "expected result to be ready but still blocked", "index: %v", i)
			}
			if expectedSucceeds[i] {
				assert.NoErrorf(t, err, "index: %v", i)
			} else {
				assert.Errorf(t, err, "index: %v", i)
				assert.NotContainsf(t, err.Error(), "context canceled", "index: %v", i)
			}
		}
	}
}

func TestValidateReadTSForNormalReadDoNotAffectUpdateInterval(t *testing.T) {
	oracleInterface, err := NewPdOracle(&MockPdClient{}, &PDOracleOptions{
		UpdateInterval: time.Second * 2,
		NoUpdateTS:     true,
	})
	assert.NoError(t, err)
	o := oracleInterface.(*pdOracle)
	defer o.Close()

	ctx := context.Background()
	opt := &oracle.Option{TxnScope: oracle.GlobalTxnScope}

	// Validating read ts for non-stale-read requests must not trigger updating the adaptive update interval of
	// low resolution ts.
	mustNoNotify := func() {
		select {
		case <-o.adaptiveUpdateIntervalState.shrinkIntervalCh:
			assert.Fail(t, "expects not notifying shrinking update interval immediately, but message was received")
		default:
		}
	}

	ts, err := o.GetTimestamp(ctx, opt)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, ts, uint64(1))

	err = o.ValidateReadTS(ctx, ts, false, opt)
	assert.NoError(t, err)
	mustNoNotify()

	// It loads `ts + 1` from the mock PD, and the check cannot pass.
	err = o.ValidateReadTS(ctx, ts+2, false, opt)
	assert.Error(t, err)
	mustNoNotify()

	// Do the check again. It loads `ts + 2` from the mock PD, and the check passes.
	err = o.ValidateReadTS(ctx, ts+2, false, opt)
	assert.NoError(t, err)
	mustNoNotify()
}

func TestSetLastTSAlwaysPushTS(t *testing.T) {
	oracleInterface, err := NewPdOracle(&MockPdClient{}, &PDOracleOptions{
		UpdateInterval: time.Second * 2,
		NoUpdateTS:     true,
	})
	assert.NoError(t, err)
	o := oracleInterface.(*pdOracle)
	defer o.Close()

	var wg sync.WaitGroup
	cancel := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for {
				select {
				case <-cancel:
					return
				default:
				}
				ts, err := o.GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
				assert.NoError(t, err)
				lastTS, found := o.getLastTS(oracle.GlobalTxnScope)
				assert.True(t, found)
				assert.GreaterOrEqual(t, lastTS, ts)
			}
		}()
	}
	time.Sleep(time.Second)
	close(cancel)
	wg.Wait()
}
