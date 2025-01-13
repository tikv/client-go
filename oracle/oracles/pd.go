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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/oracle/oracles/pd.go
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

package oracles

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var _ oracle.Oracle = &pdOracle{}

const slowDist = 30 * time.Millisecond

type adaptiveUpdateTSIntervalState int

const (
	adaptiveUpdateTSIntervalStateNone adaptiveUpdateTSIntervalState = iota
	// adaptiveUpdateTSIntervalStateNormal represents the state that the adaptive update ts interval is synced with the
	// configuration without performing any automatic adjustment.
	adaptiveUpdateTSIntervalStateNormal
	// adaptiveUpdateTSIntervalStateAdapting represents the state that as there are recently some stale read / snapshot
	// read operations requesting a short staleness (now - readTS is nearly or exceeds the current update interval),
	// so that we automatically shrink the update interval. Otherwise, read operations may don't have low resolution ts
	// that is new enough for checking the legality of the read ts, causing them have to fetch the latest ts from PD,
	// which is time-consuming.
	adaptiveUpdateTSIntervalStateAdapting
	// adaptiveUpdateTSIntervalStateRecovering represents the state that the update ts interval have once been shrunk,
	// to adapt to reads with short staleness, but there isn't any such read operations for a while, so that we
	// gradually recover the update interval to the configured value.
	adaptiveUpdateTSIntervalStateRecovering
	// adaptiveUpdateTSIntervalStateUnadjustable represents the state that the user has configured a very short update
	// interval, so that we don't have any space to automatically adjust it.
	adaptiveUpdateTSIntervalStateUnadjustable
)

func (s adaptiveUpdateTSIntervalState) String() string {
	switch s {
	case adaptiveUpdateTSIntervalStateNormal:
		return "normal"
	case adaptiveUpdateTSIntervalStateAdapting:
		return "adapting"
	case adaptiveUpdateTSIntervalStateRecovering:
		return "recovering"
	case adaptiveUpdateTSIntervalStateUnadjustable:
		return "unadjustable"
	default:
		return fmt.Sprintf("unknown(%v)", int(s))
	}
}

const (
	// minAllowedAdaptiveUpdateTSInterval is the lower bound of the adaptive update ts interval for avoiding an abnormal
	// read operation causing the update interval to be too short.
	minAllowedAdaptiveUpdateTSInterval = 500 * time.Millisecond
	// adaptiveUpdateTSIntervalShrinkingPreserve is the duration that we additionally shrinks when adapting to a read
	// operation that requires a short staleness.
	adaptiveUpdateTSIntervalShrinkingPreserve = 100 * time.Millisecond
	// adaptiveUpdateTSIntervalBlockRecoverThreshold is the threshold of the difference between the current update
	// interval and the staleness the read operation request to prevent the update interval from recovering back to
	// normal.
	adaptiveUpdateTSIntervalBlockRecoverThreshold = 200 * time.Millisecond
	// adaptiveUpdateTSIntervalRecoverPerSecond is the duration that the update interval should grow per second when
	// recovering to normal state from adapting state.
	adaptiveUpdateTSIntervalRecoverPerSecond = 20 * time.Millisecond
	// adaptiveUpdateTSIntervalDelayBeforeRecovering is the duration that we should hold the current adaptive update
	// interval before turning back to normal state.
	adaptiveUpdateTSIntervalDelayBeforeRecovering = 5 * time.Minute
)

// pdOracle is an Oracle that uses a placement driver client as source.
type pdOracle struct {
	c pd.Client
	// txn_scope (string) -> lastTSPointer (*atomic.Pointer[lastTSO])
	lastTSMap sync.Map
	quit      chan struct{}
	// The configured interval to update the low resolution ts. Set by SetLowResolutionTimestampUpdateInterval.
	// For TiDB, this is directly controlled by the system variable `tidb_low_resolution_tso_update_interval`.
	lastTSUpdateInterval atomic.Int64
	// The actual interval to update the low resolution ts. If the configured one is too large to satisfy the
	// requirement of the stale read or snapshot read, the actual interval can be automatically set to a shorter
	// value than lastTSUpdateInterval.
	// This value is also possible to be updated by SetLowResolutionTimestampUpdateInterval, which may happen when
	// user adjusting the update interval manually.
	adaptiveLastTSUpdateInterval atomic.Int64

	adaptiveUpdateIntervalState struct {
		// The mutex to avoid racing between updateTS goroutine and SetLowResolutionTimestampUpdateInterval.
		mu sync.Mutex
		// The most recent time that a stale read / snapshot read requests a timestamp that is close enough to
		// the current adaptive update interval. If there is such a request recently, the adaptive interval
		// should avoid falling back to the original (configured) value.
		// Stored in unix microseconds to make it able to be accessed atomically.
		lastShortStalenessReadTime atomic.Int64
		// When someone requests need shrinking the update interval immediately, it sends the duration it expects to
		// this channel.
		shrinkIntervalCh chan time.Duration

		// Only accessed in updateTS goroutine. No need to use atomic value.
		lastTick time.Time
		// Represents a description about the current state.
		state adaptiveUpdateTSIntervalState
	}

	// When the low resolution ts is not new enough and there are many concurrent stane read / snapshot read
	// operations that needs to validate the read ts, we can use this to avoid too many concurrent GetTS calls by
	// reusing a result for different `ValidateReadTS` calls. This can be done because that
	// we don't require the ts for validation to be strictly the latest one.
	// Note that the result can't be reused for different txnScopes. The txnScope is used as the key.
	tsForValidation singleflight.Group
}

// lastTSO stores the last timestamp oracle gets from PD server and the local time when the TSO is fetched.
type lastTSO struct {
	tso     uint64
	arrival time.Time
}

type PDOracleOptions struct {
	// The duration to update the last ts, i.e., the low resolution ts.
	UpdateInterval time.Duration
	// Disable the background periodic update of the last ts. This is for test purposes only.
	NoUpdateTS bool
}

// NewPdOracle create an Oracle that uses a pd client source.
// Refer https://github.com/tikv/pd/blob/master/client/client.go for more details.
// PdOracle maintains `lastTS` to store the last timestamp got from PD server. If
// `GetTimestamp()` is not called after `lastTSUpdateInterval`, it will be called by
// itself to keep up with the timestamp on PD server.
func NewPdOracle(pdClient pd.Client, options *PDOracleOptions) (oracle.Oracle, error) {
	if options.UpdateInterval <= 0 {
		return nil, fmt.Errorf("updateInterval must be > 0")
	}

	o := &pdOracle{
		c:                    pdClient,
		quit:                 make(chan struct{}),
		lastTSUpdateInterval: atomic.Int64{},
	}
	o.adaptiveUpdateIntervalState.shrinkIntervalCh = make(chan time.Duration, 1)
	o.lastTSUpdateInterval.Store(int64(options.UpdateInterval))
	o.adaptiveLastTSUpdateInterval.Store(int64(options.UpdateInterval))
	o.adaptiveUpdateIntervalState.lastTick = time.Now()

	ctx := context.TODO()
	if !options.NoUpdateTS {
		go o.updateTS(ctx)
	}
	// Initialize the timestamp of the global txnScope by Get.
	_, err := o.GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	if err != nil {
		o.Close()
		return nil, err
	}
	return o, nil
}

// IsExpired returns whether lockTS+TTL is expired, both are ms. It uses `lastTS`
// to compare, may return false negative result temporarily.
func (o *pdOracle) IsExpired(lockTS, TTL uint64, opt *oracle.Option) bool {
	lastTS, exist := o.getLastTS(opt.TxnScope)
	if !exist {
		return true
	}
	return oracle.ExtractPhysical(lastTS) >= oracle.ExtractPhysical(lockTS)+int64(TTL)
}

// GetTimestamp gets a new increasing time.
func (o *pdOracle) GetTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	ts, err := o.getTimestamp(ctx, opt.TxnScope)
	if err != nil {
		return 0, err
	}
	o.setLastTS(ts, opt.TxnScope)
	return ts, nil
}

// GetAllTSOKeyspaceGroupMinTS gets a minimum timestamp from all TSO keyspace groups.
func (o *pdOracle) GetAllTSOKeyspaceGroupMinTS(ctx context.Context) (uint64, error) {
	return o.getMinTimestampInAllTSOGroup(ctx)
}

type tsFuture struct {
	pd.TSFuture
	o        *pdOracle
	txnScope string
}

// Wait implements the oracle.Future interface.
func (f *tsFuture) Wait() (uint64, error) {
	now := time.Now()
	physical, logical, err := f.TSFuture.Wait()
	metrics.TiKVTSFutureWaitDuration.Observe(time.Since(now).Seconds())
	if err != nil {
		return 0, errors.WithStack(err)
	}
	ts := oracle.ComposeTS(physical, logical)
	f.o.setLastTS(ts, f.txnScope)
	return ts, nil
}

func (o *pdOracle) GetTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	var ts pd.TSFuture
	if opt.TxnScope == oracle.GlobalTxnScope || opt.TxnScope == "" {
		ts = o.c.GetTSAsync(ctx)
	} else {
		ts = o.c.GetLocalTSAsync(ctx, opt.TxnScope)
	}
	return &tsFuture{ts, o, opt.TxnScope}
}

func (o *pdOracle) getTimestamp(ctx context.Context, txnScope string) (uint64, error) {
	now := time.Now()
	var (
		physical, logical int64
		err               error
	)
	if txnScope == oracle.GlobalTxnScope || txnScope == "" {
		physical, logical, err = o.c.GetTS(ctx)
	} else {
		physical, logical, err = o.c.GetLocalTS(ctx, txnScope)
	}
	if err != nil {
		return 0, errors.WithStack(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		logutil.Logger(ctx).Warn("get timestamp too slow",
			zap.Duration("cost time", dist))
	}
	return oracle.ComposeTS(physical, logical), nil
}

func (o *pdOracle) getMinTimestampInAllTSOGroup(ctx context.Context) (uint64, error) {
	now := time.Now()

	physical, logical, err := o.c.GetMinTS(ctx)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	dist := time.Since(now)
	if dist > slowDist {
		logutil.Logger(ctx).Warn("get minimum timestamp too slow",
			zap.Duration("cost time", dist))
	}
	return oracle.ComposeTS(physical, logical), nil
}

func (o *pdOracle) setLastTS(ts uint64, txnScope string) {
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	current := &lastTSO{
		tso:     ts,
		arrival: time.Now(),
	}
	lastTSInterface, ok := o.lastTSMap.Load(txnScope)
	if !ok {
		pointer := &atomic.Pointer[lastTSO]{}
		pointer.Store(current)
		// do not handle the stored case, because it only runs once.
		lastTSInterface, _ = o.lastTSMap.LoadOrStore(txnScope, pointer)
	}
	lastTSPointer := lastTSInterface.(*atomic.Pointer[lastTSO])
	for {
		last := lastTSPointer.Load()
		if current.tso <= last.tso {
			return
		}
		if last.arrival.After(current.arrival) {
			current.arrival = last.arrival
		}
		if lastTSPointer.CompareAndSwap(last, current) {
			return
		}
	}
}

func (o *pdOracle) getLastTS(txnScope string) (uint64, bool) {
	last, exist := o.getLastTSWithArrivalTS(txnScope)
	if !exist {
		return 0, false
	}
	return last.tso, true
}

func (o *pdOracle) getLastTSWithArrivalTS(txnScope string) (*lastTSO, bool) {
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	lastTSInterface, ok := o.lastTSMap.Load(txnScope)
	if !ok {
		return nil, false
	}
	lastTSPointer := lastTSInterface.(*atomic.Pointer[lastTSO])
	last := lastTSPointer.Load()
	if last == nil {
		return nil, false
	}
	return last, true
}

func (o *pdOracle) nextUpdateInterval(now time.Time, requiredStaleness time.Duration) time.Duration {
	o.adaptiveUpdateIntervalState.mu.Lock()
	defer o.adaptiveUpdateIntervalState.mu.Unlock()

	configuredInterval := time.Duration(o.lastTSUpdateInterval.Load())
	prevAdaptiveUpdateInterval := time.Duration(o.adaptiveLastTSUpdateInterval.Load())
	lastReachDropThresholdTime := time.UnixMilli(o.adaptiveUpdateIntervalState.lastShortStalenessReadTime.Load())

	currentAdaptiveUpdateInterval := prevAdaptiveUpdateInterval

	// Shortcut
	const none = adaptiveUpdateTSIntervalStateNone

	// The following `checkX` functions checks whether it should transit to the X state. Returns
	// a tuple representing (state, newInterval).
	// When `checkX` returns a valid state, it means that the current situation matches the state. In this case, it
	// also returns the new interval that should be used next.
	// When it returns `none`, we need to check if it should transit to other states. For each call to
	// nextUpdateInterval, if all attempts to `checkX` function returns false, it keeps the previous state unchanged.

	checkUnadjustable := func() (adaptiveUpdateTSIntervalState, time.Duration) {
		// If the user has configured a very short interval, we don't have any space to adjust it. Just use
		// the user's configured value directly.
		if configuredInterval <= minAllowedAdaptiveUpdateTSInterval {
			return adaptiveUpdateTSIntervalStateUnadjustable, configuredInterval
		}
		return none, 0
	}

	checkNormal := func() (adaptiveUpdateTSIntervalState, time.Duration) {
		// If the current actual update interval is synced with the configured value, and it's not unadjustable state,
		// then it's the normal state.
		if configuredInterval > minAllowedAdaptiveUpdateTSInterval && currentAdaptiveUpdateInterval == configuredInterval {
			return adaptiveUpdateTSIntervalStateNormal, currentAdaptiveUpdateInterval
		}
		return none, 0
	}

	checkAdapting := func() (adaptiveUpdateTSIntervalState, time.Duration) {
		if requiredStaleness != 0 && requiredStaleness < currentAdaptiveUpdateInterval && currentAdaptiveUpdateInterval > minAllowedAdaptiveUpdateTSInterval {
			// If we are calculating the interval because of a request that requires a shorter staleness, we shrink the
			// update interval immediately to adapt to it.
			// We shrink the update interval to a value slightly lower than the requested staleness to avoid potential
			// frequent shrinking operations. But there's a lower bound to prevent loading ts too frequently.
			newInterval := max(requiredStaleness-adaptiveUpdateTSIntervalShrinkingPreserve, minAllowedAdaptiveUpdateTSInterval)
			return adaptiveUpdateTSIntervalStateAdapting, newInterval
		}

		if currentAdaptiveUpdateInterval != configuredInterval && now.Sub(lastReachDropThresholdTime) < adaptiveUpdateTSIntervalDelayBeforeRecovering {
			// There is a recent request that requires a short staleness. Keep the current adaptive interval.
			// If it's not adapting state, it's possible that it's previously in recovering state, and it stops recovering
			// as there is a new read operation requesting a short staleness.
			return adaptiveUpdateTSIntervalStateAdapting, currentAdaptiveUpdateInterval
		}

		return none, 0
	}

	checkRecovering := func() (adaptiveUpdateTSIntervalState, time.Duration) {
		if currentAdaptiveUpdateInterval == configuredInterval || now.Sub(lastReachDropThresholdTime) < adaptiveUpdateTSIntervalDelayBeforeRecovering {
			return none, 0
		}

		timeSinceLastTick := now.Sub(o.adaptiveUpdateIntervalState.lastTick)
		newInterval := currentAdaptiveUpdateInterval + time.Duration(timeSinceLastTick.Seconds()*float64(adaptiveUpdateTSIntervalRecoverPerSecond))
		if newInterval > configuredInterval {
			newInterval = configuredInterval
		}

		return adaptiveUpdateTSIntervalStateRecovering, newInterval
	}

	// Check the specified states in order, until the state becomes determined.
	// If it's still undetermined after all checks, keep the previous state.
	nextState := func(checkFuncs ...func() (adaptiveUpdateTSIntervalState, time.Duration)) time.Duration {
		for _, f := range checkFuncs {
			state, newInterval := f()
			if state == none {
				continue
			}

			currentAdaptiveUpdateInterval = newInterval

			// If the final state is the recovering state, do an additional step to check whether it can go back to
			// normal state immediately.
			if state == adaptiveUpdateTSIntervalStateRecovering {
				var nextState adaptiveUpdateTSIntervalState
				nextState, newInterval = checkNormal()
				if nextState != none {
					state = nextState
					currentAdaptiveUpdateInterval = newInterval
				}
			}

			o.adaptiveLastTSUpdateInterval.Store(int64(currentAdaptiveUpdateInterval))
			if o.adaptiveUpdateIntervalState.state != state {
				logutil.BgLogger().Info("adaptive update ts interval state transition",
					zap.Duration("configuredInterval", configuredInterval),
					zap.Duration("prevAdaptiveUpdateInterval", prevAdaptiveUpdateInterval),
					zap.Duration("newAdaptiveUpdateInterval", currentAdaptiveUpdateInterval),
					zap.Duration("requiredStaleness", requiredStaleness),
					zap.Stringer("prevState", o.adaptiveUpdateIntervalState.state),
					zap.Stringer("newState", state))
				o.adaptiveUpdateIntervalState.state = state
			}

			return currentAdaptiveUpdateInterval
		}
		return currentAdaptiveUpdateInterval
	}

	var newInterval time.Duration
	if requiredStaleness != 0 {
		newInterval = nextState(checkUnadjustable, checkAdapting)
	} else {
		newInterval = nextState(checkUnadjustable, checkAdapting, checkNormal, checkRecovering)
	}

	metrics.TiKVLowResolutionTSOUpdateIntervalSecondsGauge.Set(newInterval.Seconds())

	return newInterval
}

func (o *pdOracle) updateTS(ctx context.Context) {
	currentInterval := time.Duration(o.lastTSUpdateInterval.Load())
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	// Note that as `doUpdate` updates last tick time while `nextUpdateInterval` may perform calculation depending on the
	// last tick time, `doUpdate` should be called after finishing calculating the next interval.
	doUpdate := func(now time.Time) {
		// Update the timestamp for each txnScope
		o.lastTSMap.Range(func(key, _ interface{}) bool {
			txnScope := key.(string)
			ts, err := o.getTimestamp(ctx, txnScope)
			if err != nil {
				logutil.Logger(ctx).Error("updateTS error", zap.String("txnScope", txnScope), zap.Error(err))
				return true
			}
			o.setLastTS(ts, txnScope)
			return true
		})

		o.adaptiveUpdateIntervalState.lastTick = now
	}

	for {
		select {
		case now := <-ticker.C:
			// nextUpdateInterval has calculation that depends on the time of the last tick. Calculate next interval
			// before `doUpdate` as `doUpdate` is responsible for updating the time of the last tick.
			newInterval := o.nextUpdateInterval(now, 0)

			doUpdate(now)

			if newInterval != currentInterval {
				currentInterval = newInterval
				ticker.Reset(currentInterval)
			}

		case requiredStaleness := <-o.adaptiveUpdateIntervalState.shrinkIntervalCh:
			now := time.Now()
			newInterval := o.nextUpdateInterval(now, requiredStaleness)
			if newInterval != currentInterval {
				currentInterval = newInterval

				if time.Since(o.adaptiveUpdateIntervalState.lastTick) >= currentInterval {
					doUpdate(time.Now())
				}

				ticker.Reset(currentInterval)
			}
		case <-o.quit:
			return
		}
	}
}

// UntilExpired implement oracle.Oracle interface.
func (o *pdOracle) UntilExpired(lockTS uint64, TTL uint64, opt *oracle.Option) int64 {
	lastTS, ok := o.getLastTS(opt.TxnScope)
	if !ok {
		return 0
	}
	return oracle.ExtractPhysical(lockTS) + int64(TTL) - oracle.ExtractPhysical(lastTS)
}

func (o *pdOracle) Close() {
	close(o.quit)
}

// A future that resolves immediately to a low resolution timestamp.
type lowResolutionTsFuture struct {
	ts  uint64
	err error
}

// Wait implements the oracle.Future interface.
func (f lowResolutionTsFuture) Wait() (uint64, error) {
	return f.ts, f.err
}

// SetLowResolutionTimestampUpdateInterval sets the refresh interval for low resolution timestamps. Note this will take
// effect up to the previous update interval amount of time after being called.
// This setting may not be strictly followed. If Stale Read requests too new data to be available, the low resolution
// ts may be actually updated in a shorter interval than the configured one.
func (o *pdOracle) SetLowResolutionTimestampUpdateInterval(newUpdateInterval time.Duration) error {
	if newUpdateInterval <= 0 {
		return fmt.Errorf("updateInterval must be > 0")
	}

	o.adaptiveUpdateIntervalState.mu.Lock()
	defer o.adaptiveUpdateIntervalState.mu.Unlock()

	prevConfigured := o.lastTSUpdateInterval.Swap(int64(newUpdateInterval))
	adaptiveUpdateInterval := o.adaptiveLastTSUpdateInterval.Load()

	if newUpdateInterval == time.Duration(prevConfigured) {
		// The config is unchanged. Do nothing.
		return nil
	}

	var adaptiveUpdateIntervalUpdated bool

	if adaptiveUpdateInterval == prevConfigured || newUpdateInterval < time.Duration(adaptiveUpdateInterval) {
		// If the adaptive update interval is the same as the configured one, treat it as the adaptive adjusting
		// mechanism not taking effect. So update it immediately.
		// If the new configured interval is short so that it's smaller than the current adaptive interval, also shrink
		// the adaptive interval immediately.
		o.adaptiveLastTSUpdateInterval.Store(int64(newUpdateInterval))
		adaptiveUpdateIntervalUpdated = true
	}
	logutil.Logger(context.Background()).Info("updated low resolution ts update interval",
		zap.Duration("previous", time.Duration(prevConfigured)),
		zap.Duration("new", newUpdateInterval),
		zap.Duration("prevAdaptiveUpdateInterval", time.Duration(adaptiveUpdateInterval)),
		zap.Bool("adaptiveUpdateIntervalUpdated", adaptiveUpdateIntervalUpdated))

	return nil
}

// GetLowResolutionTimestamp gets a new increasing time.
func (o *pdOracle) GetLowResolutionTimestamp(ctx context.Context, opt *oracle.Option) (uint64, error) {
	lastTS, ok := o.getLastTS(opt.TxnScope)
	if !ok {
		return 0, errors.Errorf("get low resolution timestamp fail, invalid txnScope = %s", opt.TxnScope)
	}
	return lastTS, nil
}

func (o *pdOracle) GetLowResolutionTimestampAsync(ctx context.Context, opt *oracle.Option) oracle.Future {
	lastTS, ok := o.getLastTS(opt.TxnScope)
	if !ok {
		return lowResolutionTsFuture{
			ts:  0,
			err: errors.Errorf("get low resolution timestamp async fail, invalid txnScope = %s", opt.TxnScope),
		}
	}
	return lowResolutionTsFuture{
		ts:  lastTS,
		err: nil,
	}
}

func (o *pdOracle) getStaleTimestamp(txnScope string, prevSecond uint64) (uint64, error) {
	last, ok := o.getLastTSWithArrivalTS(txnScope)
	if !ok {
		return 0, errors.Errorf("get stale timestamp fail, txnScope: %s", txnScope)
	}
	return o.getStaleTimestampWithLastTS(last, prevSecond)
}

func (o *pdOracle) getStaleTimestampWithLastTS(last *lastTSO, prevSecond uint64) (uint64, error) {
	ts, arrivalTime := last.tso, last.arrival
	physicalTime := oracle.GetTimeFromTS(ts)
	if uint64(physicalTime.Unix()) <= prevSecond {
		return 0, errors.Errorf("invalid prevSecond %v", prevSecond)
	}

	staleTime := physicalTime.Add(time.Now().Add(-time.Duration(prevSecond) * time.Second).Sub(arrivalTime))
	return oracle.GoTimeToTS(staleTime), nil
}

// GetStaleTimestamp generate a TSO which represents for the TSO prevSecond secs ago.
func (o *pdOracle) GetStaleTimestamp(ctx context.Context, txnScope string, prevSecond uint64) (ts uint64, err error) {
	ts, err = o.getStaleTimestamp(txnScope, prevSecond)
	if err != nil {
		if !strings.HasPrefix(err.Error(), "invalid prevSecond") {
			// If any error happened, we will try to fetch tso and set it as last ts.
			_, tErr := o.GetTimestamp(ctx, &oracle.Option{TxnScope: txnScope})
			if tErr != nil {
				return 0, tErr
			}
		}
		return 0, err
	}
	return ts, nil
}

func (o *pdOracle) SetExternalTimestamp(ctx context.Context, ts uint64) error {
	return o.c.SetExternalTimestamp(ctx, ts)
}

func (o *pdOracle) GetExternalTimestamp(ctx context.Context) (uint64, error) {
	return o.c.GetExternalTimestamp(ctx)
}

func (o *pdOracle) getCurrentTSForValidation(ctx context.Context, opt *oracle.Option) (uint64, error) {
	ch := o.tsForValidation.DoChan(opt.TxnScope, func() (interface{}, error) {
		metrics.TiKVValidateReadTSFromPDCount.Inc()

		// If the call that triggers the execution of this function is canceled by the context, other calls that are
		// waiting for reusing the same result should not be canceled. So pass context.Background() instead of the
		// current ctx.
		res, err := o.GetTimestamp(context.Background(), opt)
		return res, err
	})
	select {
	case <-ctx.Done():
		return 0, errors.WithStack(ctx.Err())
	case res := <-ch:
		if res.Err != nil {
			return 0, errors.WithStack(res.Err)
		}
		return res.Val.(uint64), nil
	}
}

func (o *pdOracle) ValidateReadTS(ctx context.Context, readTS uint64, isStaleRead bool, opt *oracle.Option) (errRet error) {
	if readTS == math.MaxUint64 {
		if isStaleRead {
			return oracle.ErrLatestStaleRead{}
		}
		return nil
	}

	latestTSInfo, exists := o.getLastTSWithArrivalTS(opt.TxnScope)
	// If we fail to get latestTSInfo or the readTS exceeds it, get a timestamp from PD to double-check.
	// But we don't need to strictly fetch the latest TS. So if there are already concurrent calls to this function
	// loading the latest TS, we can just reuse the same result to avoid too many concurrent GetTS calls.
	if !exists || readTS > latestTSInfo.tso {
		currentTS, err := o.getCurrentTSForValidation(ctx, opt)
		if err != nil {
			return errors.Errorf("fail to validate read timestamp: %v", err)
		}
		if isStaleRead {
			o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(readTS, currentTS, time.Now())
		}
		if readTS > currentTS {
			return oracle.ErrFutureTSRead{
				ReadTS:    readTS,
				CurrentTS: currentTS,
			}
		}
	} else if isStaleRead {
		estimatedCurrentTS, err := o.getStaleTimestampWithLastTS(latestTSInfo, 0)
		if err != nil {
			logutil.Logger(ctx).Warn("failed to estimate current ts by getSlateTimestamp for auto-adjusting update low resolution ts interval",
				zap.Error(err), zap.Uint64("readTS", readTS), zap.String("txnScope", opt.TxnScope))
		} else {
			o.adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(readTS, estimatedCurrentTS, time.Now())
		}
	}
	return nil
}

// adjustUpdateLowResolutionTSIntervalWithRequestedStaleness triggers adjustments the update interval of low resolution
// ts, if necessary, to suite the usage of stale read.
// This method is not supposed to be called when performing non-stale-read operations.
func (o *pdOracle) adjustUpdateLowResolutionTSIntervalWithRequestedStaleness(readTS uint64, currentTS uint64, now time.Time) {
	requiredStaleness := oracle.GetTimeFromTS(currentTS).Sub(oracle.GetTimeFromTS(readTS))

	// Do not acquire the mutex, as here we only needs a rough check.
	// So it's possible that we get inconsistent values from these two atomic fields, but it won't cause any problem.
	currentUpdateInterval := time.Duration(o.adaptiveLastTSUpdateInterval.Load())

	if requiredStaleness <= currentUpdateInterval+adaptiveUpdateTSIntervalBlockRecoverThreshold {
		// Record the most recent time when there's a read operation requesting the staleness close enough to the
		// current update interval.
		nowMillis := now.UnixMilli()
		last := o.adaptiveUpdateIntervalState.lastShortStalenessReadTime.Load()
		if last < nowMillis {
			// Do not retry if the CAS fails (which may happen when there are other goroutines updating it
			// concurrently), as we don't actually need to set it strictly.
			o.adaptiveUpdateIntervalState.lastShortStalenessReadTime.CompareAndSwap(last, nowMillis)
		}
	}

	if requiredStaleness <= currentUpdateInterval && currentUpdateInterval > minAllowedAdaptiveUpdateTSInterval {
		// Considering system time / PD time drifts, it's possible that we get a non-positive value from the
		// calculation. Make sure it's always positive before passing it to the updateTS goroutine.
		// Note that `nextUpdateInterval` method expects the requiredStaleness is always non-zero when triggerred
		// by this path.
		requiredStaleness = max(requiredStaleness, time.Millisecond)
		// Try to non-blocking send a signal to notify it to change the interval immediately. But if the channel is
		// busy, it means that there's another concurrent call trying to update it. Just skip it in this case.
		select {
		case o.adaptiveUpdateIntervalState.shrinkIntervalCh <- requiredStaleness:
		default:
		}
	}
}
