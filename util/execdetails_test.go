// Copyright 2026 TiKV Authors
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

package util

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/assert"
	resourceControlClient "github.com/tikv/pd/client/resource_group/controller"
)

func TestRUDetailsDrainRUV2(t *testing.T) {
	ruDetails := NewRUDetails()
	ruDetails.AddRUV2(&kvrpcpb.RUV2{
		ReadRpcCount:                 1,
		StorageProcessedKeysBatchGet: 2,
		ExecutorInputs: &kvrpcpb.ExecutorInputs{
			TikvCoprocessorExecutorWorkTotalBatchSelection: 3,
		},
	})
	ruDetails.AddRUV2(&kvrpcpb.RUV2{
		WriteRpcCount:                     4,
		StorageProcessedKeysGet:           5,
		RaftstoreStoreWriteTriggerWbBytes: 6,
		ExecutorInputs: &kvrpcpb.ExecutorInputs{
			TikvCoprocessorExecutorWorkTotalBatchSelection: 7,
		},
	})

	drained := ruDetails.DrainRUV2()
	assert.NotNil(t, drained)
	assert.Equal(t, uint64(1), drained.ReadRpcCount)
	assert.Equal(t, uint64(4), drained.WriteRpcCount)
	assert.Equal(t, uint64(2), drained.StorageProcessedKeysBatchGet)
	assert.Equal(t, uint64(5), drained.StorageProcessedKeysGet)
	assert.Equal(t, uint64(6), drained.RaftstoreStoreWriteTriggerWbBytes)
	assert.Equal(t, uint64(10), drained.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchSelection)
	assert.Nil(t, ruDetails.DrainRUV2())
}

func TestRUDetailsCloneAndMergeRawRUV2(t *testing.T) {
	orig := NewRUDetails()
	orig.AddRUV2(&kvrpcpb.RUV2{
		ReadRpcCount: 1,
		ExecutorInputs: &kvrpcpb.ExecutorInputs{
			TikvCoprocessorExecutorWorkTotalBatchIndexScan: 2,
		},
	})

	cloned := orig.Clone()
	cloned.AddRUV2(&kvrpcpb.RUV2{WriteRpcCount: 3})

	origDrained := orig.DrainRUV2()
	assert.Equal(t, uint64(1), origDrained.ReadRpcCount)
	assert.Zero(t, origDrained.WriteRpcCount)
	assert.Equal(t, uint64(2), origDrained.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan)

	clonedDrained := cloned.DrainRUV2()
	assert.Equal(t, uint64(1), clonedDrained.ReadRpcCount)
	assert.Equal(t, uint64(3), clonedDrained.WriteRpcCount)
	assert.Equal(t, uint64(2), clonedDrained.ExecutorInputs.TikvCoprocessorExecutorWorkTotalBatchIndexScan)

	left := NewRUDetails()
	left.AddRUV2(&kvrpcpb.RUV2{ReadRpcCount: 5})
	right := NewRUDetails()
	right.AddRUV2(&kvrpcpb.RUV2{WriteRpcCount: 7})
	left.Merge(right)

	merged := left.DrainRUV2()
	assert.Equal(t, uint64(5), merged.ReadRpcCount)
	assert.Equal(t, uint64(7), merged.WriteRpcCount)
	rightDrained := right.DrainRUV2()
	assert.Equal(t, uint64(7), rightDrained.WriteRpcCount)
}

func TestRUDetailsCalculationDetails(t *testing.T) {
	details := NewRUDetails()
	factors := resourceControlClient.RUFactorSnapshot{
		ReadBaseCost: 1,
	}
	delta := resourceControlClient.RUCalculation{
		Factors: factors,
		Inputs: resourceControlClient.RUCalculationInputs{
			ReadRPCCount: 1,
		},
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			details.Update(&rmpb.Consumption{RRU: 1}, 0)
			details.AddRUCalculation(delta)
		}()
	}
	wg.Wait()

	calculation, ok, consistent := details.RUCalculation()
	assert.True(t, ok)
	assert.True(t, consistent)
	assert.Equal(t, factors, calculation.Factors)
	assert.Equal(t, float64(10), calculation.Inputs.ReadRPCCount)
	assert.Equal(t, float64(10), details.RRU())

	cloned := details.Clone()
	clonedCalculation, ok, consistent := cloned.RUCalculation()
	assert.True(t, ok)
	assert.True(t, consistent)
	assert.Equal(t, float64(10), clonedCalculation.Inputs.ReadRPCCount)

	sameFactors := NewRUDetails()
	sameFactors.AddRUCalculation(delta)
	cloned.Merge(sameFactors)
	clonedCalculation, ok, consistent = cloned.RUCalculation()
	assert.True(t, ok)
	assert.True(t, consistent)
	assert.Equal(t, float64(11), clonedCalculation.Inputs.ReadRPCCount)

	otherFactors := factors
	otherFactors.ReadBaseCost = 2
	otherDelta := delta
	otherDelta.Factors = otherFactors
	other := NewRUDetails()
	other.Update(&rmpb.Consumption{RRU: 2}, 0)
	other.AddRUCalculation(otherDelta)
	other.UpdateTiFlash(&rmpb.Consumption{RRU: 3, WRU: 4})
	cloned.Merge(other)

	_, ok, consistent = cloned.RUCalculation()
	assert.False(t, ok)
	assert.False(t, consistent)
	assert.Equal(t, float64(7), cloned.TiflashRU())
	_, ok, consistent = details.RUCalculation()
	assert.True(t, ok)
	assert.True(t, consistent)

	zeroDetails := NewRUDetails()
	zeroDelta := resourceControlClient.RUCalculation{
		Factors: resourceControlClient.RUFactorSnapshot{},
		Inputs:  resourceControlClient.RUCalculationInputs{ReadRPCCount: 1},
	}
	zeroDetails.AddRUCalculation(zeroDelta)
	zeroCalculation, ok, consistent := zeroDetails.RUCalculation()
	assert.True(t, ok)
	assert.True(t, consistent)
	assert.Equal(t, float64(1), zeroCalculation.Inputs.ReadRPCCount)

	invalidClone := cloned.Clone()
	_, ok, consistent = invalidClone.RUCalculation()
	assert.False(t, ok)
	assert.False(t, consistent)
	invalidClone.AddRUCalculation(delta)
	_, ok, consistent = invalidClone.RUCalculation()
	assert.False(t, ok)
	assert.False(t, consistent)
}

func TestPoolTaskDetailsStringUsesAverageTimes(t *testing.T) {
	details := &PoolTaskDetails{
		TaskCount:                      2,
		PollCount:                      8,
		MaxPollCount:                   5,
		MinPollCount:                   3,
		DispatchCount:                  6,
		MaxDispatchCount:               4,
		MinDispatchCount:               2,
		TotalWallTime:                  20 * time.Millisecond,
		TaskWallTimeSampleCount:        2,
		MaxTaskWallTime:                12 * time.Millisecond,
		MinTaskWallTime:                8 * time.Millisecond,
		TotalQueueWaitTime:             12 * time.Millisecond,
		MaxQueueWaitTime:               4 * time.Millisecond,
		MinQueueWaitTime:               time.Millisecond,
		TotalWakeWaitTime:              8 * time.Millisecond,
		MaxWakeWaitTime:                3 * time.Millisecond,
		MinWakeWaitTime:                time.Millisecond,
		FairQueueSampleCount:           6,
		TotalFairQueueWaitedTaskSlices: 18,
		MaxFairQueueWaitedTaskSlices:   5,
		MinFairQueueWaitedTaskSlices:   2,
		PollCPUTime:                    8 * time.Millisecond,
		MaxPollCPUTime:                 2 * time.Millisecond,
		MinPollCPUTime:                 500 * time.Microsecond,
		PollWallTime:                   12 * time.Millisecond,
		MaxPollWallTime:                3 * time.Millisecond,
		MinPollWallTime:                750 * time.Microsecond,
	}

	assert.Equal(t,
		"{tasks:2, poll_count:{total:8, avg:4, max:5, min:3}, "+
			"dispatch_count:{total:6, max:4, min:2}, "+
			"task_wall_time:{total:20ms, avg:10ms, max:12ms, min:8ms}, "+
			"queue_wait:{total:12ms, avg:2ms, max:4ms, min:1ms}, "+
			"wake_wait:{total:8ms, avg:2ms, max:3ms, min:1ms}, "+
			"fair_queue:{enabled:true, "+
			"waited_task_slices:{total:18, avg:3, max:5, min:2}}, "+
			"poll_cpu:{total:8ms, avg:1ms, max:2ms, min:500\u00b5s}, "+
			"poll_wall:{total:12ms, avg:1.5ms, max:3ms, min:750\u00b5s}}",
		details.String(),
	)
}

func TestPoolTaskDetailsStringOmitsZeroTimes(t *testing.T) {
	details := &PoolTaskDetails{
		TaskCount:        1,
		PollCount:        2,
		MaxPollCount:     2,
		MinPollCount:     2,
		DispatchCount:    2,
		MaxDispatchCount: 2,
		MinDispatchCount: 2,
	}

	assert.Equal(t,
		"{tasks:1, poll_count:{total:2, avg:2, max:2, min:2}, "+
			"dispatch_count:{total:2, max:2, min:2}, "+
			"fair_queue:{enabled:false, waited_task_slices:{total:0, max:0, min:0}}}",
		details.String(),
	)
}

func TestPoolTaskDetailsStringOmitsAverageWithNoSamples(t *testing.T) {
	details := &PoolTaskDetails{
		TaskCount:          1,
		TotalQueueWaitTime: 2 * time.Millisecond,
		MaxQueueWaitTime:   2 * time.Millisecond,
		MinQueueWaitTime:   2 * time.Millisecond,
	}

	assert.Equal(t,
		"{tasks:1, poll_count:{total:0, avg:0, max:0, min:0}, "+
			"dispatch_count:{total:0, max:0, min:0}, "+
			"queue_wait:{total:2ms, max:2ms, min:2ms}, "+
			"fair_queue:{enabled:false, waited_task_slices:{total:0, max:0, min:0}}}",
		details.String(),
	)
}

func TestPoolTaskDetailsMergeFromPBAndMerge(t *testing.T) {
	details := &PoolTaskDetails{}
	details.MergeFromPB(&kvrpcpb.PoolTaskDetails{
		PollCount:                      5,
		DispatchCount:                  3,
		TotalWallNanos:                 uint64((10 * time.Millisecond).Nanoseconds()),
		TotalQueueWaitNanos:            uint64((6 * time.Millisecond).Nanoseconds()),
		MaxQueueWaitNanos:              uint64((3 * time.Millisecond).Nanoseconds()),
		MinQueueWaitNanos:              uint64(time.Millisecond.Nanoseconds()),
		TotalWakeWaitNanos:             uint64((4 * time.Millisecond).Nanoseconds()),
		MaxWakeWaitNanos:               uint64((3 * time.Millisecond).Nanoseconds()),
		MinWakeWaitNanos:               uint64(time.Millisecond.Nanoseconds()),
		FairQueueEnabled:               true,
		TotalFairQueueWaitedTaskSlices: 9,
		MaxFairQueueWaitedTaskSlices:   5,
		MinFairQueueWaitedTaskSlices:   1,
		PollCpuNanos:                   uint64((5 * time.Millisecond).Nanoseconds()),
		MaxPollCpuNanos:                uint64((2 * time.Millisecond).Nanoseconds()),
		MinPollCpuNanos:                uint64((500 * time.Microsecond).Nanoseconds()),
		PollWallNanos:                  uint64((8 * time.Millisecond).Nanoseconds()),
		MaxPollWallNanos:               uint64((4 * time.Millisecond).Nanoseconds()),
		MinPollWallNanos:               uint64(time.Millisecond.Nanoseconds()),
	})
	details.MergeFromPB(&kvrpcpb.PoolTaskDetails{
		PollCount:                      2,
		DispatchCount:                  1,
		TotalQueueWaitNanos:            uint64((2 * time.Millisecond).Nanoseconds()),
		MaxQueueWaitNanos:              uint64((2 * time.Millisecond).Nanoseconds()),
		MinQueueWaitNanos:              uint64((2 * time.Millisecond).Nanoseconds()),
		FairQueueEnabled:               false,
		TotalFairQueueWaitedTaskSlices: 100,
		MaxFairQueueWaitedTaskSlices:   100,
		MinFairQueueWaitedTaskSlices:   100,
		PollCpuNanos:                   uint64((2 * time.Millisecond).Nanoseconds()),
		MaxPollCpuNanos:                uint64((1500 * time.Microsecond).Nanoseconds()),
		MinPollCpuNanos:                uint64((400 * time.Microsecond).Nanoseconds()),
		PollWallNanos:                  uint64((3 * time.Millisecond).Nanoseconds()),
		MaxPollWallNanos:               uint64((2 * time.Millisecond).Nanoseconds()),
		MinPollWallNanos:               uint64((800 * time.Microsecond).Nanoseconds()),
	})

	assert.Equal(t, &PoolTaskDetails{
		TaskCount:                      2,
		PollCount:                      7,
		MaxPollCount:                   5,
		MinPollCount:                   2,
		DispatchCount:                  4,
		MaxDispatchCount:               3,
		MinDispatchCount:               1,
		TotalWallTime:                  10 * time.Millisecond,
		TaskWallTimeSampleCount:        1,
		MaxTaskWallTime:                10 * time.Millisecond,
		MinTaskWallTime:                10 * time.Millisecond,
		TotalQueueWaitTime:             8 * time.Millisecond,
		MaxQueueWaitTime:               3 * time.Millisecond,
		MinQueueWaitTime:               time.Millisecond,
		TotalWakeWaitTime:              4 * time.Millisecond,
		MaxWakeWaitTime:                3 * time.Millisecond,
		MinWakeWaitTime:                time.Millisecond,
		FairQueueSampleCount:           3,
		TotalFairQueueWaitedTaskSlices: 9,
		MaxFairQueueWaitedTaskSlices:   5,
		MinFairQueueWaitedTaskSlices:   1,
		PollCPUTime:                    7 * time.Millisecond,
		MaxPollCPUTime:                 2 * time.Millisecond,
		MinPollCPUTime:                 400 * time.Microsecond,
		PollWallTime:                   11 * time.Millisecond,
		MaxPollWallTime:                4 * time.Millisecond,
		MinPollWallTime:                800 * time.Microsecond,
	}, details)

	other := &PoolTaskDetails{}
	other.MergeFromPB(&kvrpcpb.PoolTaskDetails{
		PollCount:                      8,
		DispatchCount:                  4,
		TotalWallNanos:                 uint64((20 * time.Millisecond).Nanoseconds()),
		TotalQueueWaitNanos:            uint64((12 * time.Millisecond).Nanoseconds()),
		MaxQueueWaitNanos:              uint64((5 * time.Millisecond).Nanoseconds()),
		MinQueueWaitNanos:              uint64((2 * time.Millisecond).Nanoseconds()),
		TotalWakeWaitNanos:             uint64((6 * time.Millisecond).Nanoseconds()),
		MaxWakeWaitNanos:               uint64((4 * time.Millisecond).Nanoseconds()),
		MinWakeWaitNanos:               uint64((2 * time.Millisecond).Nanoseconds()),
		FairQueueEnabled:               true,
		TotalFairQueueWaitedTaskSlices: 8,
		MaxFairQueueWaitedTaskSlices:   4,
		MinFairQueueWaitedTaskSlices:   0,
		PollCpuNanos:                   uint64((8 * time.Millisecond).Nanoseconds()),
		MaxPollCpuNanos:                uint64((3 * time.Millisecond).Nanoseconds()),
		MinPollCpuNanos:                uint64((300 * time.Microsecond).Nanoseconds()),
		PollWallNanos:                  uint64((10 * time.Millisecond).Nanoseconds()),
		MaxPollWallNanos:               uint64((5 * time.Millisecond).Nanoseconds()),
		MinPollWallNanos:               uint64((700 * time.Microsecond).Nanoseconds()),
	})
	details.Merge(other)

	assert.Equal(t, &PoolTaskDetails{
		TaskCount:                      3,
		PollCount:                      15,
		MaxPollCount:                   8,
		MinPollCount:                   2,
		DispatchCount:                  8,
		MaxDispatchCount:               4,
		MinDispatchCount:               1,
		TotalWallTime:                  30 * time.Millisecond,
		TaskWallTimeSampleCount:        2,
		MaxTaskWallTime:                20 * time.Millisecond,
		MinTaskWallTime:                10 * time.Millisecond,
		TotalQueueWaitTime:             20 * time.Millisecond,
		MaxQueueWaitTime:               5 * time.Millisecond,
		MinQueueWaitTime:               time.Millisecond,
		TotalWakeWaitTime:              10 * time.Millisecond,
		MaxWakeWaitTime:                4 * time.Millisecond,
		MinWakeWaitTime:                time.Millisecond,
		FairQueueSampleCount:           7,
		TotalFairQueueWaitedTaskSlices: 17,
		MaxFairQueueWaitedTaskSlices:   5,
		MinFairQueueWaitedTaskSlices:   0,
		PollCPUTime:                    15 * time.Millisecond,
		MaxPollCPUTime:                 3 * time.Millisecond,
		MinPollCPUTime:                 300 * time.Microsecond,
		PollWallTime:                   21 * time.Millisecond,
		MaxPollWallTime:                5 * time.Millisecond,
		MinPollWallTime:                700 * time.Microsecond,
	}, details)
}

func TestPoolTaskDetailsMergeMinimumPresence(t *testing.T) {
	mergeSequentiallyAndByAggregate := func(
		t *testing.T,
		first *kvrpcpb.PoolTaskDetails,
		second *kvrpcpb.PoolTaskDetails,
	) *PoolTaskDetails {
		t.Helper()

		sequential := &PoolTaskDetails{}
		sequential.MergeFromPB(first)
		sequential.MergeFromPB(second)

		left := &PoolTaskDetails{}
		left.MergeFromPB(first)
		right := &PoolTaskDetails{}
		right.MergeFromPB(second)
		left.Merge(right)

		assert.Equal(t, sequential, left)
		return sequential
	}

	t.Run("absent wait samples do not pin minimum to zero", func(t *testing.T) {
		details := mergeSequentiallyAndByAggregate(t,
			&kvrpcpb.PoolTaskDetails{
				PollCount:       2,
				DispatchCount:   1,
				TotalWallNanos:  uint64((3 * time.Millisecond).Nanoseconds()),
				PollCpuNanos:    uint64(time.Millisecond.Nanoseconds()),
				MaxPollCpuNanos: uint64(time.Millisecond.Nanoseconds()),
				PollWallNanos:   uint64((2 * time.Millisecond).Nanoseconds()),
				MaxPollWallNanos: uint64(
					(2 * time.Millisecond).Nanoseconds(),
				),
			},
			&kvrpcpb.PoolTaskDetails{
				PollCount:                      3,
				DispatchCount:                  2,
				TotalWallNanos:                 uint64((25 * time.Millisecond).Nanoseconds()),
				TotalQueueWaitNanos:            uint64((10 * time.Millisecond).Nanoseconds()),
				MaxQueueWaitNanos:              uint64((7 * time.Millisecond).Nanoseconds()),
				MinQueueWaitNanos:              uint64((3 * time.Millisecond).Nanoseconds()),
				TotalWakeWaitNanos:             uint64((2 * time.Millisecond).Nanoseconds()),
				MaxWakeWaitNanos:               uint64((2 * time.Millisecond).Nanoseconds()),
				MinWakeWaitNanos:               uint64((2 * time.Millisecond).Nanoseconds()),
				FairQueueEnabled:               true,
				TotalFairQueueWaitedTaskSlices: 4,
				MaxFairQueueWaitedTaskSlices:   3,
				MinFairQueueWaitedTaskSlices:   1,
				PollCpuNanos:                   uint64((6 * time.Millisecond).Nanoseconds()),
				MaxPollCpuNanos:                uint64((3 * time.Millisecond).Nanoseconds()),
				MinPollCpuNanos:                uint64(time.Millisecond.Nanoseconds()),
				PollWallNanos:                  uint64((9 * time.Millisecond).Nanoseconds()),
				MaxPollWallNanos:               uint64((4 * time.Millisecond).Nanoseconds()),
				MinPollWallNanos:               uint64((2 * time.Millisecond).Nanoseconds()),
			},
		)

		assert.Equal(t, 3*time.Millisecond, details.MinQueueWaitTime)
		assert.Equal(t, 2*time.Millisecond, details.MinWakeWaitTime)
		assert.Equal(t, uint64(1), details.MinFairQueueWaitedTaskSlices)
		// The first response contains a poll sample, so its zero-valued CPU and
		// wall-time minima are real samples rather than missing values.
		assert.Zero(t, details.MinPollCPUTime)
		assert.Zero(t, details.MinPollWallTime)
	})

	t.Run("recorded zero minimum remains zero", func(t *testing.T) {
		details := mergeSequentiallyAndByAggregate(t,
			&kvrpcpb.PoolTaskDetails{
				PollCount:                      3,
				DispatchCount:                  3,
				TotalWallNanos:                 uint64((10 * time.Millisecond).Nanoseconds()),
				TotalQueueWaitNanos:            uint64(time.Millisecond.Nanoseconds()),
				MaxQueueWaitNanos:              uint64(time.Millisecond.Nanoseconds()),
				MinQueueWaitNanos:              0,
				TotalWakeWaitNanos:             uint64(time.Millisecond.Nanoseconds()),
				MaxWakeWaitNanos:               uint64(time.Millisecond.Nanoseconds()),
				MinWakeWaitNanos:               0,
				FairQueueEnabled:               true,
				TotalFairQueueWaitedTaskSlices: 0,
				MaxFairQueueWaitedTaskSlices:   0,
				MinFairQueueWaitedTaskSlices:   0,
				PollCpuNanos:                   uint64(time.Millisecond.Nanoseconds()),
				MaxPollCpuNanos:                uint64(time.Millisecond.Nanoseconds()),
				MinPollCpuNanos:                0,
				PollWallNanos:                  uint64(time.Millisecond.Nanoseconds()),
				MaxPollWallNanos:               uint64(time.Millisecond.Nanoseconds()),
				MinPollWallNanos:               0,
			},
			&kvrpcpb.PoolTaskDetails{
				PollCount:                      2,
				DispatchCount:                  2,
				TotalWallNanos:                 uint64((10 * time.Millisecond).Nanoseconds()),
				TotalQueueWaitNanos:            uint64((4 * time.Millisecond).Nanoseconds()),
				MaxQueueWaitNanos:              uint64((3 * time.Millisecond).Nanoseconds()),
				MinQueueWaitNanos:              uint64(time.Millisecond.Nanoseconds()),
				TotalWakeWaitNanos:             uint64(time.Millisecond.Nanoseconds()),
				MaxWakeWaitNanos:               uint64(time.Millisecond.Nanoseconds()),
				MinWakeWaitNanos:               uint64(time.Millisecond.Nanoseconds()),
				FairQueueEnabled:               true,
				TotalFairQueueWaitedTaskSlices: 3,
				MaxFairQueueWaitedTaskSlices:   2,
				MinFairQueueWaitedTaskSlices:   1,
				PollCpuNanos:                   uint64((2 * time.Millisecond).Nanoseconds()),
				MaxPollCpuNanos:                uint64(time.Millisecond.Nanoseconds()),
				MinPollCpuNanos:                uint64(time.Millisecond.Nanoseconds()),
				PollWallNanos:                  uint64((2 * time.Millisecond).Nanoseconds()),
				MaxPollWallNanos:               uint64(time.Millisecond.Nanoseconds()),
				MinPollWallNanos:               uint64(time.Millisecond.Nanoseconds()),
			},
		)

		assert.Zero(t, details.MinQueueWaitTime)
		assert.Zero(t, details.MinWakeWaitTime)
		assert.Zero(t, details.MinFairQueueWaitedTaskSlices)
		assert.Zero(t, details.MinPollCPUTime)
		assert.Zero(t, details.MinPollWallTime)
	})
}

func TestPoolTaskDetailsStringFormatsFractionalCountAverages(t *testing.T) {
	details := &PoolTaskDetails{
		TaskCount:                      3,
		PollCount:                      8,
		MaxPollCount:                   5,
		MinPollCount:                   1,
		DispatchCount:                  3,
		MaxDispatchCount:               1,
		MinDispatchCount:               1,
		FairQueueSampleCount:           3,
		TotalFairQueueWaitedTaskSlices: 7,
		MaxFairQueueWaitedTaskSlices:   4,
		MinFairQueueWaitedTaskSlices:   0,
	}

	assert.Equal(t,
		"{tasks:3, poll_count:{total:8, avg:2.67, max:5, min:1}, "+
			"dispatch_count:{total:3, max:1, min:1}, "+
			"fair_queue:{enabled:true, waited_task_slices:{total:7, avg:2.33, max:4, min:0}}}",
		details.String(),
	)
}

func TestPoolTaskDetailsStringKeepsZeroFairQueueWait(t *testing.T) {
	details := &PoolTaskDetails{
		TaskCount:            1,
		PollCount:            2,
		MaxPollCount:         2,
		MinPollCount:         2,
		DispatchCount:        2,
		MaxDispatchCount:     2,
		MinDispatchCount:     2,
		FairQueueSampleCount: 2,
	}

	assert.Equal(t,
		"{tasks:1, poll_count:{total:2, avg:2, max:2, min:2}, "+
			"dispatch_count:{total:2, max:2, min:2}, "+
			"fair_queue:{enabled:true, waited_task_slices:{total:0, avg:0, max:0, min:0}}}",
		details.String(),
	)
}

func TestPoolTaskDetailsEmptyAndClone(t *testing.T) {
	var nilDetails *PoolTaskDetails
	assert.True(t, nilDetails.Empty())
	assert.Empty(t, nilDetails.String())
	assert.Nil(t, nilDetails.Clone())

	details := &PoolTaskDetails{}
	details.MergeFromPB(nil)
	assert.True(t, details.Empty())

	details.MergeFromPB(&kvrpcpb.PoolTaskDetails{PollCount: 1, DispatchCount: 1})
	assert.False(t, details.Empty())

	clone := details.Clone()
	assert.Equal(t, details, clone)
	assert.NotSame(t, details, clone)
	clone.PollCount++
	assert.NotEqual(t, details.PollCount, clone.PollCount)

	beforeMerge := details.Clone()
	details.Merge(nil)
	details.Merge(&PoolTaskDetails{})
	assert.Equal(t, beforeMerge, details)
}

func TestScanDetailMergeFromScanDetailV2IncludesIAFields(t *testing.T) {
	scanDetail := &kvrpcpb.ScanDetailV2{
		ProcessedVersions:         10,
		ProcessedVersionsSize:     20,
		TotalVersions:             30,
		RocksdbDeleteSkippedCount: 4,
		RocksdbKeySkippedCount:    5,
		RocksdbBlockCacheHitCount: 6,
		RocksdbBlockReadCount:     7,
		RocksdbBlockReadByte:      8,
		RocksdbBlockReadNanos:     uint64((9 * time.Microsecond).Nanoseconds()),
		GetSnapshotNanos:          uint64((7 * time.Microsecond).Nanoseconds()),
		IaCacheHitCount:           2,
		IaRemoteReadSegmentCount:  3,
		IaRemoteReadSegmentBytes:  128,
		IaRemoteReadSegmentNanos:  uint64((5 * time.Microsecond).Nanoseconds()),
	}

	sd := &ScanDetail{}
	sd.MergeFromScanDetailV2(scanDetail)

	assert.Equal(t, int64(30), sd.TotalKeys)
	assert.Equal(t, int64(10), sd.ProcessedKeys)
	assert.Equal(t, int64(20), sd.ProcessedKeysSize)
	assert.Equal(t, uint64(4), sd.RocksdbDeleteSkippedCount)
	assert.Equal(t, uint64(5), sd.RocksdbKeySkippedCount)
	assert.Equal(t, uint64(6), sd.RocksdbBlockCacheHitCount)
	assert.Equal(t, uint64(7), sd.RocksdbBlockReadCount)
	assert.Equal(t, uint64(8), sd.RocksdbBlockReadByte)
	assert.Equal(t, 9*time.Microsecond, sd.RocksdbBlockReadDuration)
	assert.Equal(t, 7*time.Microsecond, sd.GetSnapshotDuration)
	assert.Equal(t, uint64(2), sd.IaCacheHitCount)
	assert.Equal(t, uint64(3), sd.IaRemoteReadSegmentCount)
	assert.Equal(t, uint64(128), sd.IaRemoteReadSegmentBytes)
	assert.Equal(t, 5*time.Microsecond, sd.IaRemoteReadSegmentDuration)

	str := sd.String()
	assert.Contains(t, str, "total_process_keys: 10")
	assert.Contains(t, str, "total_process_keys_size: 20")
	assert.Contains(t, str, "total_keys: 30")
	assert.Contains(t, str, "get_snapshot_time")
	assert.Contains(t, str, "ia: {")
	assert.Contains(t, str, "cache_hit_count: 2")
	assert.Contains(t, str, "remote_read_segment_count: 3")
	assert.Contains(t, str, "remote_read_segment_bytes: 128 Bytes")
	assert.Contains(t, str, "remote_read_segment_wait_time")
	assert.Contains(t, str, "rocksdb: {")
	assert.Contains(t, str, "delete_skipped_count: 4")
	assert.Contains(t, str, "key_skipped_count: 5")
	assert.Contains(t, str, "cache_hit_count: 6")
	assert.Contains(t, str, "read_count: 7")
	assert.Contains(t, str, "read_byte: 8 Bytes")
	assert.Contains(t, str, "read_time")
}

func TestScanDetailMergeIncludesIAFields(t *testing.T) {
	left := &ScanDetail{
		IaCacheHitCount:             1,
		IaRemoteReadSegmentCount:    2,
		IaRemoteReadSegmentBytes:    64,
		IaRemoteReadSegmentDuration: 3 * time.Microsecond,
	}
	right := &ScanDetail{
		IaCacheHitCount:             4,
		IaRemoteReadSegmentCount:    5,
		IaRemoteReadSegmentBytes:    256,
		IaRemoteReadSegmentDuration: 7 * time.Microsecond,
	}

	left.Merge(right)

	assert.Equal(t, uint64(5), left.IaCacheHitCount)
	assert.Equal(t, uint64(7), left.IaRemoteReadSegmentCount)
	assert.Equal(t, uint64(320), left.IaRemoteReadSegmentBytes)
	assert.Equal(t, 10*time.Microsecond, left.IaRemoteReadSegmentDuration)
}

func TestLockKeysDetailsMerge(t *testing.T) {
	a := &LockKeysDetails{
		TotalTime:                  10 * time.Millisecond,
		RegionNum:                  2,
		LockKeys:                   5,
		AggressiveLockNewCount:     1,
		AggressiveLockDerivedCount: 2,
		LockedWithConflictCount:    3,
		ResolveLock:                ResolveLockDetail{ResolveLockTime: 100},
		BackoffTime:                200,
		LockRPCTime:                300,
		LockRPCCount:               4,
		RetryCount:                 1,
	}
	a.Mu.BackoffTypes = []string{"txnLock"}
	a.Mu.SlowestReqTotalTime = 5 * time.Millisecond
	a.Mu.SlowestRegion = 10
	a.Mu.SlowestStoreAddr = "store1"

	b := &LockKeysDetails{
		TotalTime:                  20 * time.Millisecond,
		RegionNum:                  3,
		LockKeys:                   7,
		AggressiveLockNewCount:     4,
		AggressiveLockDerivedCount: 5,
		LockedWithConflictCount:    6,
		ResolveLock:                ResolveLockDetail{ResolveLockTime: 150},
		BackoffTime:                250,
		LockRPCTime:                350,
		LockRPCCount:               5,
		RetryCount:                 2,
	}
	b.Mu.BackoffTypes = []string{"regionMiss"}
	b.Mu.SlowestReqTotalTime = 8 * time.Millisecond
	b.Mu.SlowestRegion = 20
	b.Mu.SlowestStoreAddr = "store2"

	a.Merge(b)

	assert.Equal(t, 30*time.Millisecond, a.TotalTime)
	assert.Equal(t, int32(5), a.RegionNum)
	assert.Equal(t, int32(12), a.LockKeys)
	assert.Equal(t, 5, a.AggressiveLockNewCount)
	assert.Equal(t, 7, a.AggressiveLockDerivedCount)
	assert.Equal(t, 9, a.LockedWithConflictCount)
	assert.Equal(t, int64(250), a.ResolveLock.ResolveLockTime)
	assert.Equal(t, int64(450), a.BackoffTime)
	assert.Equal(t, int64(650), a.LockRPCTime)
	assert.Equal(t, int64(9), a.LockRPCCount)
	assert.Equal(t, 2, a.RetryCount) // RetryCount is incremented by 1 per Merge call
	assert.Equal(t, []string{"txnLock", "regionMiss"}, a.Mu.BackoffTypes)
	// b has a slower request, so a should adopt b's slowest info
	assert.Equal(t, 8*time.Millisecond, a.Mu.SlowestReqTotalTime)
	assert.Equal(t, uint64(20), a.Mu.SlowestRegion)
	assert.Equal(t, "store2", a.Mu.SlowestStoreAddr)
}

func TestLockKeysDetailsMergeSlowestNotReplaced(t *testing.T) {
	a := &LockKeysDetails{}
	a.Mu.SlowestReqTotalTime = 10 * time.Millisecond
	a.Mu.SlowestRegion = 1
	a.Mu.SlowestStoreAddr = "store1"

	b := &LockKeysDetails{}
	b.Mu.SlowestReqTotalTime = 5 * time.Millisecond
	b.Mu.SlowestRegion = 2
	b.Mu.SlowestStoreAddr = "store2"

	a.Merge(b)

	// a already has the slower request, should keep its own info
	assert.Equal(t, 10*time.Millisecond, a.Mu.SlowestReqTotalTime)
	assert.Equal(t, uint64(1), a.Mu.SlowestRegion)
	assert.Equal(t, "store1", a.Mu.SlowestStoreAddr)
}

func TestLockKeysDetailsClone(t *testing.T) {
	orig := &LockKeysDetails{
		TotalTime:                  10 * time.Millisecond,
		RegionNum:                  2,
		LockKeys:                   5,
		AggressiveLockNewCount:     1,
		AggressiveLockDerivedCount: 2,
		LockedWithConflictCount:    3,
		ResolveLock:                ResolveLockDetail{ResolveLockTime: 100},
		BackoffTime:                200,
		LockRPCTime:                300,
		LockRPCCount:               4,
		RetryCount:                 1,
	}
	orig.Mu.BackoffTypes = []string{"txnLock", "regionMiss"}
	orig.Mu.SlowestReqTotalTime = 5 * time.Millisecond
	orig.Mu.SlowestRegion = 10
	orig.Mu.SlowestStoreAddr = "store1"

	cloned := orig.Clone()

	// Verify all fields are equal
	assert.Equal(t, orig.TotalTime, cloned.TotalTime)
	assert.Equal(t, orig.RegionNum, cloned.RegionNum)
	assert.Equal(t, orig.LockKeys, cloned.LockKeys)
	assert.Equal(t, orig.AggressiveLockNewCount, cloned.AggressiveLockNewCount)
	assert.Equal(t, orig.AggressiveLockDerivedCount, cloned.AggressiveLockDerivedCount)
	assert.Equal(t, orig.LockedWithConflictCount, cloned.LockedWithConflictCount)
	assert.Equal(t, orig.ResolveLock, cloned.ResolveLock)
	assert.Equal(t, orig.BackoffTime, cloned.BackoffTime)
	assert.Equal(t, orig.LockRPCTime, cloned.LockRPCTime)
	assert.Equal(t, orig.LockRPCCount, cloned.LockRPCCount)
	assert.Equal(t, orig.RetryCount, cloned.RetryCount)
	assert.Equal(t, orig.Mu.BackoffTypes, cloned.Mu.BackoffTypes)
	assert.Equal(t, orig.Mu.SlowestReqTotalTime, cloned.Mu.SlowestReqTotalTime)
	assert.Equal(t, orig.Mu.SlowestRegion, cloned.Mu.SlowestRegion)
	assert.Equal(t, orig.Mu.SlowestStoreAddr, cloned.Mu.SlowestStoreAddr)

	// Verify deep copy: modifying cloned slice should not affect original
	cloned.Mu.BackoffTypes = append(cloned.Mu.BackoffTypes, "extra")
	assert.Len(t, orig.Mu.BackoffTypes, 2)

	cloned.TotalTime = 999 * time.Millisecond
	assert.Equal(t, 10*time.Millisecond, orig.TotalTime)
}

func TestCommitDetailsMerge(t *testing.T) {
	a := &CommitDetails{
		GetCommitTsTime:        10 * time.Millisecond,
		GetLatestTsTime:        5 * time.Millisecond,
		PrewriteTime:           20 * time.Millisecond,
		WaitPrewriteBinlogTime: 3 * time.Millisecond,
		CommitTime:             15 * time.Millisecond,
		LocalLatchTime:         2 * time.Millisecond,
		WriteKeys:              100,
		WriteSize:              2000,
		PrewriteRegionNum:      4,
		TxnRetry:               1,
		ResolveLock:            ResolveLockDetail{ResolveLockTime: 50},
	}
	a.Mu.CommitBackoffTime = 100
	a.Mu.PrewriteBackoffTypes = []string{"txnLock"}
	a.Mu.CommitBackoffTypes = []string{"regionMiss"}
	a.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 1, StoreAddr: "s1"}
	a.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 3 * time.Millisecond, Region: 2, StoreAddr: "s2"}

	b := &CommitDetails{
		GetCommitTsTime:        12 * time.Millisecond,
		GetLatestTsTime:        6 * time.Millisecond,
		PrewriteTime:           25 * time.Millisecond,
		WaitPrewriteBinlogTime: 4 * time.Millisecond,
		CommitTime:             18 * time.Millisecond,
		LocalLatchTime:         3 * time.Millisecond,
		WriteKeys:              150,
		WriteSize:              3000,
		PrewriteRegionNum:      5,
		TxnRetry:               2,
		ResolveLock:            ResolveLockDetail{ResolveLockTime: 60},
	}
	b.Mu.CommitBackoffTime = 200
	b.Mu.PrewriteBackoffTypes = []string{"tikvRPC"}
	b.Mu.CommitBackoffTypes = []string{"txnLock"}
	b.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 8 * time.Millisecond, Region: 10, StoreAddr: "s10"}
	b.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 6 * time.Millisecond, Region: 20, StoreAddr: "s20"}

	a.Merge(b)

	assert.Equal(t, 22*time.Millisecond, a.GetCommitTsTime)
	assert.Equal(t, 11*time.Millisecond, a.GetLatestTsTime)
	assert.Equal(t, 45*time.Millisecond, a.PrewriteTime)
	assert.Equal(t, 7*time.Millisecond, a.WaitPrewriteBinlogTime)
	assert.Equal(t, 33*time.Millisecond, a.CommitTime)
	assert.Equal(t, 5*time.Millisecond, a.LocalLatchTime)
	assert.Equal(t, 250, a.WriteKeys)
	assert.Equal(t, 5000, a.WriteSize)
	assert.Equal(t, int32(9), a.PrewriteRegionNum)
	assert.Equal(t, 3, a.TxnRetry)
	assert.Equal(t, int64(110), a.ResolveLock.ResolveLockTime)
	assert.Equal(t, int64(300), a.Mu.CommitBackoffTime)
	assert.Equal(t, []string{"txnLock", "tikvRPC"}, a.Mu.PrewriteBackoffTypes)
	assert.Equal(t, []string{"regionMiss", "txnLock"}, a.Mu.CommitBackoffTypes)
	// b has slower prewrite and commit, so a should adopt b's info
	assert.Equal(t, 8*time.Millisecond, a.Mu.SlowestPrewrite.ReqTotalTime)
	assert.Equal(t, uint64(10), a.Mu.SlowestPrewrite.Region)
	assert.Equal(t, "s10", a.Mu.SlowestPrewrite.StoreAddr)
	assert.Equal(t, 6*time.Millisecond, a.Mu.CommitPrimary.ReqTotalTime)
	assert.Equal(t, uint64(20), a.Mu.CommitPrimary.Region)
	assert.Equal(t, "s20", a.Mu.CommitPrimary.StoreAddr)
}

func TestCommitDetailsMergeSlowestNotReplaced(t *testing.T) {
	a := &CommitDetails{}
	a.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 10 * time.Millisecond, Region: 1}
	a.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 10 * time.Millisecond, Region: 2}

	b := &CommitDetails{}
	b.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 3}
	b.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 4}

	a.Merge(b)

	assert.Equal(t, uint64(1), a.Mu.SlowestPrewrite.Region)
	assert.Equal(t, uint64(2), a.Mu.CommitPrimary.Region)
}

func TestCommitDetailsClone(t *testing.T) {
	orig := &CommitDetails{
		GetCommitTsTime:        10 * time.Millisecond,
		GetLatestTsTime:        5 * time.Millisecond,
		PrewriteTime:           20 * time.Millisecond,
		WaitPrewriteBinlogTime: 3 * time.Millisecond,
		CommitTime:             15 * time.Millisecond,
		LocalLatchTime:         2 * time.Millisecond,
		WriteKeys:              100,
		WriteSize:              2000,
		PrewriteRegionNum:      4,
		TxnRetry:               1,
		ResolveLock:            ResolveLockDetail{ResolveLockTime: 50},
	}
	orig.Mu.CommitBackoffTime = 100
	orig.Mu.PrewriteBackoffTypes = []string{"txnLock", "regionMiss"}
	orig.Mu.CommitBackoffTypes = []string{"tikvRPC"}
	orig.Mu.SlowestPrewrite = ReqDetailInfo{ReqTotalTime: 5 * time.Millisecond, Region: 1, StoreAddr: "s1"}
	orig.Mu.CommitPrimary = ReqDetailInfo{ReqTotalTime: 3 * time.Millisecond, Region: 2, StoreAddr: "s2"}

	cloned := orig.Clone()

	assert.Equal(t, orig.GetCommitTsTime, cloned.GetCommitTsTime)
	assert.Equal(t, orig.GetLatestTsTime, cloned.GetLatestTsTime)
	assert.Equal(t, orig.PrewriteTime, cloned.PrewriteTime)
	assert.Equal(t, orig.WaitPrewriteBinlogTime, cloned.WaitPrewriteBinlogTime)
	assert.Equal(t, orig.CommitTime, cloned.CommitTime)
	assert.Equal(t, orig.LocalLatchTime, cloned.LocalLatchTime)
	assert.Equal(t, orig.WriteKeys, cloned.WriteKeys)
	assert.Equal(t, orig.WriteSize, cloned.WriteSize)
	assert.Equal(t, orig.PrewriteRegionNum, cloned.PrewriteRegionNum)
	assert.Equal(t, orig.TxnRetry, cloned.TxnRetry)
	assert.Equal(t, orig.ResolveLock, cloned.ResolveLock)
	assert.Equal(t, orig.Mu.CommitBackoffTime, cloned.Mu.CommitBackoffTime)
	assert.Equal(t, orig.Mu.PrewriteBackoffTypes, cloned.Mu.PrewriteBackoffTypes)
	assert.Equal(t, orig.Mu.CommitBackoffTypes, cloned.Mu.CommitBackoffTypes)
	assert.Equal(t, orig.Mu.SlowestPrewrite, cloned.Mu.SlowestPrewrite)
	assert.Equal(t, orig.Mu.CommitPrimary, cloned.Mu.CommitPrimary)

	// Verify deep copy: modifying cloned slices should not affect original
	cloned.Mu.PrewriteBackoffTypes = append(cloned.Mu.PrewriteBackoffTypes, "extra")
	assert.Len(t, orig.Mu.PrewriteBackoffTypes, 2)

	cloned.Mu.CommitBackoffTypes = append(cloned.Mu.CommitBackoffTypes, "extra")
	assert.Len(t, orig.Mu.CommitBackoffTypes, 1)

	cloned.GetCommitTsTime = 999 * time.Millisecond
	assert.Equal(t, 10*time.Millisecond, orig.GetCommitTsTime)
}

func TestScanDetailMerge(t *testing.T) {
	a := &ScanDetail{
		TotalKeys:                 100,
		ProcessedKeys:             50,
		ProcessedKeysSize:         1000,
		RocksdbDeleteSkippedCount: 10,
		RocksdbKeySkippedCount:    20,
		RocksdbBlockCacheHitCount: 30,
		RocksdbBlockReadCount:     40,
		RocksdbBlockReadByte:      5000,
		RocksdbBlockReadDuration:  1 * time.Millisecond,
		GetSnapshotDuration:       2 * time.Millisecond,
	}
	b := &ScanDetail{
		TotalKeys:                 200,
		ProcessedKeys:             80,
		ProcessedKeysSize:         2000,
		RocksdbDeleteSkippedCount: 15,
		RocksdbKeySkippedCount:    25,
		RocksdbBlockCacheHitCount: 35,
		RocksdbBlockReadCount:     45,
		RocksdbBlockReadByte:      6000,
		RocksdbBlockReadDuration:  3 * time.Millisecond,
		GetSnapshotDuration:       4 * time.Millisecond,
	}

	a.Merge(b)

	assert.Equal(t, int64(300), a.TotalKeys)
	assert.Equal(t, int64(130), a.ProcessedKeys)
	assert.Equal(t, int64(3000), a.ProcessedKeysSize)
	assert.Equal(t, uint64(25), a.RocksdbDeleteSkippedCount)
	assert.Equal(t, uint64(45), a.RocksdbKeySkippedCount)
	assert.Equal(t, uint64(65), a.RocksdbBlockCacheHitCount)
	assert.Equal(t, uint64(85), a.RocksdbBlockReadCount)
	assert.Equal(t, uint64(11000), a.RocksdbBlockReadByte)
	assert.Equal(t, 4*time.Millisecond, a.RocksdbBlockReadDuration)
	assert.Equal(t, 6*time.Millisecond, a.GetSnapshotDuration)
}

func TestWriteDetailMerge(t *testing.T) {
	a := &WriteDetail{
		StoreBatchWaitDuration:               1 * time.Millisecond,
		ProposeSendWaitDuration:              2 * time.Millisecond,
		PersistLogDuration:                   3 * time.Millisecond,
		RaftDbWriteLeaderWaitDuration:        4 * time.Millisecond,
		RaftDbSyncLogDuration:                5 * time.Millisecond,
		RaftDbWriteMemtableDuration:          6 * time.Millisecond,
		CommitLogDuration:                    7 * time.Millisecond,
		ApplyBatchWaitDuration:               8 * time.Millisecond,
		ApplyLogDuration:                     9 * time.Millisecond,
		ApplyMutexLockDuration:               10 * time.Millisecond,
		ApplyWriteLeaderWaitDuration:         11 * time.Millisecond,
		ApplyWriteWalDuration:                12 * time.Millisecond,
		ApplyWriteMemtableDuration:           13 * time.Millisecond,
		SchedulerLatchWaitDuration:           14 * time.Millisecond,
		SchedulerProcessDuration:             15 * time.Millisecond,
		SchedulerThrottleDuration:            16 * time.Millisecond,
		SchedulerPessimisticLockWaitDuration: 17 * time.Millisecond,
	}
	b := &WriteDetail{
		StoreBatchWaitDuration:               1 * time.Millisecond,
		ProposeSendWaitDuration:              2 * time.Millisecond,
		PersistLogDuration:                   3 * time.Millisecond,
		RaftDbWriteLeaderWaitDuration:        4 * time.Millisecond,
		RaftDbSyncLogDuration:                5 * time.Millisecond,
		RaftDbWriteMemtableDuration:          6 * time.Millisecond,
		CommitLogDuration:                    7 * time.Millisecond,
		ApplyBatchWaitDuration:               8 * time.Millisecond,
		ApplyLogDuration:                     9 * time.Millisecond,
		ApplyMutexLockDuration:               10 * time.Millisecond,
		ApplyWriteLeaderWaitDuration:         11 * time.Millisecond,
		ApplyWriteWalDuration:                12 * time.Millisecond,
		ApplyWriteMemtableDuration:           13 * time.Millisecond,
		SchedulerLatchWaitDuration:           14 * time.Millisecond,
		SchedulerProcessDuration:             15 * time.Millisecond,
		SchedulerThrottleDuration:            16 * time.Millisecond,
		SchedulerPessimisticLockWaitDuration: 17 * time.Millisecond,
	}

	a.Merge(b)

	assert.Equal(t, 2*time.Millisecond, a.StoreBatchWaitDuration)
	assert.Equal(t, 4*time.Millisecond, a.ProposeSendWaitDuration)
	assert.Equal(t, 6*time.Millisecond, a.PersistLogDuration)
	assert.Equal(t, 8*time.Millisecond, a.RaftDbWriteLeaderWaitDuration)
	assert.Equal(t, 10*time.Millisecond, a.RaftDbSyncLogDuration)
	assert.Equal(t, 12*time.Millisecond, a.RaftDbWriteMemtableDuration)
	assert.Equal(t, 14*time.Millisecond, a.CommitLogDuration)
	assert.Equal(t, 16*time.Millisecond, a.ApplyBatchWaitDuration)
	assert.Equal(t, 18*time.Millisecond, a.ApplyLogDuration)
	assert.Equal(t, 20*time.Millisecond, a.ApplyMutexLockDuration)
	assert.Equal(t, 22*time.Millisecond, a.ApplyWriteLeaderWaitDuration)
	assert.Equal(t, 24*time.Millisecond, a.ApplyWriteWalDuration)
	assert.Equal(t, 26*time.Millisecond, a.ApplyWriteMemtableDuration)
	assert.Equal(t, 28*time.Millisecond, a.SchedulerLatchWaitDuration)
	assert.Equal(t, 30*time.Millisecond, a.SchedulerProcessDuration)
	assert.Equal(t, 32*time.Millisecond, a.SchedulerThrottleDuration)
	assert.Equal(t, 34*time.Millisecond, a.SchedulerPessimisticLockWaitDuration)
}

func TestTimeDetailMerge(t *testing.T) {
	a := &TimeDetail{
		ProcessTime:       10 * time.Millisecond,
		SuspendTime:       2 * time.Millisecond,
		WaitTime:          5 * time.Millisecond,
		KvReadWallTime:    3 * time.Millisecond,
		KvGrpcProcessTime: 1 * time.Millisecond,
		KvGrpcWaitTime:    4 * time.Millisecond,
		TotalRPCWallTime:  20 * time.Millisecond,
	}
	b := &TimeDetail{
		ProcessTime:       15 * time.Millisecond,
		SuspendTime:       3 * time.Millisecond,
		WaitTime:          7 * time.Millisecond,
		KvReadWallTime:    4 * time.Millisecond,
		KvGrpcProcessTime: 2 * time.Millisecond,
		KvGrpcWaitTime:    5 * time.Millisecond,
		TotalRPCWallTime:  30 * time.Millisecond,
	}

	a.Merge(b)

	assert.Equal(t, 25*time.Millisecond, a.ProcessTime)
	assert.Equal(t, 5*time.Millisecond, a.SuspendTime)
	assert.Equal(t, 12*time.Millisecond, a.WaitTime)
	assert.Equal(t, 7*time.Millisecond, a.KvReadWallTime)
	assert.Equal(t, 3*time.Millisecond, a.KvGrpcProcessTime)
	assert.Equal(t, 9*time.Millisecond, a.KvGrpcWaitTime)
	assert.Equal(t, 50*time.Millisecond, a.TotalRPCWallTime)
}

func TestTimeDetailMergeNil(t *testing.T) {
	a := &TimeDetail{
		ProcessTime: 10 * time.Millisecond,
	}
	a.Merge(nil)
	assert.Equal(t, 10*time.Millisecond, a.ProcessTime)
}

func TestRUDetailsUpdateTiFlash(t *testing.T) {
	rd := NewRUDetails()
	rd.Update(&rmpb.Consumption{
		RRU: 1.5,
		WRU: 2.5,
	}, 3*time.Millisecond)
	rd.UpdateTiFlash(&rmpb.Consumption{
		RRU: 3.0,
		WRU: 4.0,
	})

	assert.InDelta(t, 4.5, rd.RRU(), 1e-9)
	assert.InDelta(t, 6.5, rd.WRU(), 1e-9)
	assert.InDelta(t, 7.0, rd.TiflashRU(), 1e-9)
	assert.Equal(t, 3*time.Millisecond, rd.RUWaitDuration())

	cloned := rd.Clone()
	assert.InDelta(t, rd.TiflashRU(), cloned.TiflashRU(), 1e-9)
}
