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
	bCloned := b.Clone()
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
	assert.Equal(t, b.errors, bCloneForked.errors)
	assert.Equal(t, b.backoffSleepMS, bCloneForked.backoffSleepMS)
	assert.Equal(t, b.backoffTimes, bCloneForked.backoffTimes)
	assert.Equal(t, b.excludedSleep, bCloneForked.excludedSleep)
	assert.Equal(t, b.totalSleep, bCloneForked.totalSleep)
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

func TestMayBackoffOrFailFastForRegionError(t *testing.T) {
	// errors should retry without backoff
	for _, regionErr := range []*errorpb.Error{
		{
			EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{
				{Id: 1},
			}},
		},
	} {
		b := NewBackofferWithVars(context.TODO(), 1, nil)
		err := MayBackoffOrFailFastForRegionError(regionErr, b)
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
		err := MayBackoffOrFailFastForRegionError(regionErr, b)
		assert.NoError(t, err, regionErr)
		assert.Greater(t, b.totalSleep, 0, regionErr)

		// backoff fails
		cancel()
		b = NewBackofferWithVars(ctx, 1, nil)
		err = MayBackoffOrFailFastForRegionError(regionErr, b)
		assert.EqualError(t, err, regionErr.String())
	}

	// should fail fast without a retry
	for _, regionErr := range []*errorpb.Error{
		{
			UndeterminedResult: &errorpb.UndeterminedResult{Message: "test"},
		},
	} {
		b := NewBackofferWithVars(context.TODO(), 1, nil)
		err := MayBackoffOrFailFastForRegionError(regionErr, b)
		assert.NotNil(t, err, regionErr)
		assert.Zero(t, b.totalSleep, regionErr)
		if regionErr.UndeterminedResult != nil {
			// undetermined result should return `ErrResultUndetermined`
			assert.True(t, tikverr.IsErrorUndetermined(err), regionErr)
		}
	}
}
