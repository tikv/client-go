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
)

var _ oracle.Oracle = &pdOracle{}

const slowDist = 30 * time.Millisecond

// pdOracle is an Oracle that uses a placement driver client as source.
type pdOracle struct {
	c pd.Client
	// txn_scope (string) -> lastTSPointer (*atomic.Pointer[lastTSO])
	lastTSMap            sync.Map
	quit                 chan struct{}
	lastTSUpdateInterval atomic.Int64
}

// lastTSO stores the last timestamp oracle gets from PD server and the local time when the TSO is fetched.
type lastTSO struct {
	tso     uint64
	arrival uint64
}

// NewPdOracle create an Oracle that uses a pd client source.
// Refer https://github.com/tikv/pd/blob/master/client/client.go for more details.
// PdOracle maintains `lastTS` to store the last timestamp got from PD server. If
// `GetTimestamp()` is not called after `lastTSUpdateInterval`, it will be called by
// itself to keep up with the timestamp on PD server.
func NewPdOracle(pdClient pd.Client, updateInterval time.Duration) (oracle.Oracle, error) {
	o := &pdOracle{
		c:                    pdClient,
		quit:                 make(chan struct{}),
		lastTSUpdateInterval: atomic.Int64{},
	}
	err := o.SetLowResolutionTimestampUpdateInterval(updateInterval)
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	go o.updateTS(ctx)
	// Initialize the timestamp of the global txnScope by Get.
	_, err = o.GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
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

func (o *pdOracle) getArrivalTimestamp() uint64 {
	return oracle.GoTimeToTS(time.Now())
}

func (o *pdOracle) setLastTS(ts uint64, txnScope string) {
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	current := &lastTSO{
		tso:     ts,
		arrival: o.getArrivalTimestamp(),
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
		if current.tso <= last.tso || current.arrival <= last.arrival {
			return
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

func (o *pdOracle) updateTS(ctx context.Context) {
	currentInterval := o.lastTSUpdateInterval.Load()
	ticker := time.NewTicker(time.Duration(currentInterval))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
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
			newInterval := o.lastTSUpdateInterval.Load()
			if newInterval != currentInterval {
				currentInterval = newInterval
				ticker.Reset(time.Duration(currentInterval))
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
func (o *pdOracle) SetLowResolutionTimestampUpdateInterval(updateInterval time.Duration) error {
	if updateInterval <= 0 {
		return fmt.Errorf("updateInterval must be > 0")
	}
	o.lastTSUpdateInterval.Store(updateInterval.Nanoseconds())
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
	ts, arrivalTS := last.tso, last.arrival
	arrivalTime := oracle.GetTimeFromTS(arrivalTS)
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
