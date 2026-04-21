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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestBackoffWithMax(t *testing.T) {
	b := NewBackofferWithVars(context.TODO(), 2000, nil)
	err := b.BackoffWithMaxSleepTxnLockFast(5, errors.New("test"))

	assert.Nil(t, err)
	assert.Equal(t, 5, b.totalSleep)
}

func TestBackoffErrorType(t *testing.T) {
	// the actual maxSleep is multiplied by weight, which is 1600ms
	b := NewBackofferWithVars(context.TODO(), 800, nil)
	err := b.Backoff(BoRegionMiss, errors.New("region miss")) // 2ms sleep
	assert.Nil(t, err)
	// 6ms sleep at most in total
	for i := 0; i < 2; i++ {
		err = b.Backoff(BoMaxRegionNotInitialized, errors.New("region not initialized"))
		assert.Nil(t, err)
	}
	// 100ms sleep at most in total
	err = b.Backoff(BoRegionRecoveryInProgress, errors.New("recovery in progress"))
	assert.Nil(t, err)

	// sleep from ServerIsBusy is not counted
	err = b.Backoff(BoTiKVServerBusy, errors.New("server is busy"))
	assert.Nil(t, err)
	// 1000ms sleep at most in total
	err = b.Backoff(BoIsWitness, errors.New("peer is witness"))
	assert.Nil(t, err)
	// wait it exceed max sleep
	for i := 0; i < 15; i++ {
		err = b.Backoff(BoTxnNotFound, errors.New("txn not found"))
		if err != nil {
			// Next backoff should return error of backoff that sleeps for longest time.
			cfg, _ := b.longestSleepCfg()
			assert.ErrorIs(t, err, cfg.err)
			return
		}
	}
	assert.Fail(t, "should not be here")
}

func assertCloneOrForkEqual(t *testing.T, b1, b2 *Backoffer) {
	assert.Equal(t, b1.errors, b2.errors)
	assert.Equal(t, b1.errorsNum, b2.errorsNum)
	assert.Equal(t, b1.backoffSleepMS, b2.backoffSleepMS)
	assert.Equal(t, b1.backoffTimes, b2.backoffTimes)
	assert.Equal(t, b1.excludedSleep, b2.excludedSleep)
	assert.Equal(t, b1.totalSleep, b2.totalSleep)
}

func TestBackoffDeepCopy(t *testing.T) {
	var err error
	b := NewBackofferWithVars(context.TODO(), 4, nil)
	// 700 ms sleep in total and the backoffer will return an error next time.
	for i := 0; i < 3; i++ {
		err = b.Backoff(BoMaxRegionNotInitialized, errors.New("region not initialized"))
		assert.Nil(t, err)
	}
	bForked, cancel := b.Fork()
	defer cancel()
	assertCloneOrForkEqual(t, b, bForked)
	bCloned := b.Clone()
	assertCloneOrForkEqual(t, b, bCloned)
	for _, b := range []*Backoffer{bForked, bCloned} {
		err = b.Backoff(BoTiKVRPC, errors.New("tikv rpc"))
		assert.ErrorIs(t, err, BoMaxRegionNotInitialized.err)
	}
}

func TestBackoffUpdateUsingFork(t *testing.T) {
	var err error
	b := NewBackofferWithVars(context.TODO(), 4, nil)
	// 700 ms sleep in total and the backoffer will return an error next time.
	for i := 0; i < 3; i++ {
		err = b.Backoff(BoMaxRegionNotInitialized, errors.New("region not initialized"))
		assert.Nil(t, err)
	}
	bForked, cancel := b.Fork()
	defer cancel()
	bForked.Backoff(BoTiKVRPC, errors.New("tikv rpc"))
	bCloneForked := bForked.Clone()
	b.UpdateUsingForked(bForked)
	assertCloneOrForkEqual(t, b, bCloneForked)
	assert.Nil(t, b.parent)
}

func TestBackoffWithMaxExcludedExceed(t *testing.T) {
	setBackoffExcluded(BoTiKVServerBusy.name, 1)
	b := NewBackofferWithVars(context.TODO(), 1, nil)
	err := b.Backoff(BoTiKVServerBusy, errors.New("server is busy"))
	assert.Nil(t, err)

	// As the total excluded sleep is greater than the max limited value, error should be returned.
	err = b.Backoff(BoTiKVServerBusy, errors.New("server is busy"))
	assert.NotNil(t, err)
	assert.Greater(t, b.excludedSleep, b.maxSleep)
}

func TestMayBackoffForRegionError(t *testing.T) {
	// errors should retry without backoff
	for _, regionErr := range []*errorpb.Error{
		{
			EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{
				{Id: 1},
			}},
		},
	} {
		b := NewBackofferWithVars(context.TODO(), 1, nil)
		err := MayBackoffForRegionError(regionErr, b)
		assert.NoError(t, err, regionErr)
		assert.Zero(t, b.totalSleep, regionErr)
	}

	// errors should back off and retry
	for _, regionErr := range []*errorpb.Error{
		{
			// faked regionError
			EpochNotMatch: &errorpb.EpochNotMatch{},
		},
		{
			NotLeader: &errorpb.NotLeader{},
		},
		{
			ServerIsBusy: &errorpb.ServerIsBusy{},
		},
		{
			MaxTimestampNotSynced: &errorpb.MaxTimestampNotSynced{},
		},
	} {
		// backoff succeeds
		ctx, cancel := context.WithCancel(context.TODO())
		b := NewBackofferWithVars(ctx, 1, nil)
		err := MayBackoffForRegionError(regionErr, b)
		assert.NoError(t, err, regionErr)
		assert.Greater(t, b.totalSleep, 0, regionErr)

		// backoff fails
		cancel()
		b = NewBackofferWithVars(ctx, 1, nil)
		err = MayBackoffForRegionError(regionErr, b)
		assert.EqualError(t, err, regionErr.String())
	}
}

func TestBackoffErrorsKeep(t *testing.T) {
	b := NewBackofferWithVars(context.TODO(), 1000000, nil)
	assert.Zero(t, b.ErrorsNum())
	assert.Empty(t, b.latestErrors())
	cfg := NewConfig("foo", nil, NewBackoffFnCfg(1, 2, EqualJitter), tikverr.ErrTiKVServerTimeout)
	errs := make([]error, 0, 32)
	for i := 0; i < cap(errs); i++ {
		cnt := i + 1
		msg := fmt.Sprintf("currnet mock error count: %d", cnt)
		errs = append(errs, fmt.Errorf("mockErr-%d", i))
		assert.NoError(t, b.Backoff(cfg, errs[i]), msg)

		// b.ErrorsNum() should return the real number of errors.
		assert.Equal(t, len(errs), b.ErrorsNum(), msg)

		// b.latestErrors() should return at most MaxRecordBackoffErrCount errors.
		if cnt <= MaxRecordBackoffErrCount {
			// If cnt <= MaxRecordBackoffErrCount, return cnt errors.
			assert.Len(t, b.latestErrors(), cnt, msg)
		} else {
			// If cnt > MaxRecordBackoffErrCount, return MaxRecordBackoffErrCount errors.
			assert.Len(t, b.latestErrors(), MaxRecordBackoffErrCount, msg)
		}

		// The returned errors should be the last MaxRecordBackoffErrCount errors.
		realErrs := b.latestErrors()
		expectedErrs := make([]backoffErr, 0, len(realErrs))
		for j := max(0, cnt-MaxRecordBackoffErrCount); j < len(errs); j++ {
			realErrIdx := len(expectedErrs)
			expectedErrs = append(expectedErrs, backoffErr{
				Reason: errs[j].Error(),
				Time:   realErrs[realErrIdx].Time,
			})
		}
		assert.Equal(t, expectedErrs, realErrs, msg)

		// The time in returned errors should be in ascending order.
		sort.Slice(expectedErrs, func(i, j int) bool {
			return expectedErrs[i].Time.Before(expectedErrs[j].Time)
		})
		assert.Equal(t, expectedErrs, realErrs)

		// The time in returned errors should near the now time
		assert.InDelta(t, time.Now().Unix(), realErrs[0].Time.Unix(), 5)
		assert.InDelta(t, time.Now().Unix(), realErrs[len(realErrs)-1].Time.Unix(), 5)
	}
}

func TestBackoffError(t *testing.T) {
	err := backoffErr{
		Reason: "mockErr",
		Time:   time.Unix(3600*4+1234, 0).In(time.FixedZone("xx", 3*3600)),
	}
	assert.Equal(t, "mockErr at 1970-01-01T07:20:34+03:00", err.Error())
}
