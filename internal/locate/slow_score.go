// Copyright 2023 TiKV Authors
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

package locate

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

const (
	slowScoreInitVal         = 1
	slowScoreThreshold       = 80
	slowScoreMax             = 100
	slowScoreInitTimeoutInUs = 500000   // unit: us
	slowScoreMaxTimeoutInUs  = 30000000 // max timeout of one txn, unit: us
	slidingWindowSize        = 10       // default size of sliding window
)

// CountSlidingWindow represents the statistics on a bunch of sliding windows.
type CountSlidingWindow struct {
	avg     uint64
	sum     uint64
	history []uint64
}

// Avg returns the average value of this sliding window
func (cnt *CountSlidingWindow) Avg() uint64 {
	return cnt.avg
}

// Sum returns the sum value of this sliding window
func (cnt *CountSlidingWindow) Sum() uint64 {
	return cnt.sum
}

// Append adds one value into this sliding windown and returns the gradient.
func (cnt *CountSlidingWindow) Append(value uint64) (gradient float64) {
	prevAvg := cnt.avg
	if len(cnt.history) < slidingWindowSize {
		cnt.sum += value
	} else {
		cnt.sum = cnt.sum - cnt.history[0] + value
		cnt.history = cnt.history[1:]
	}
	cnt.history = append(cnt.history, value)
	cnt.avg = cnt.sum / (uint64(len(cnt.history)))
	gradient = 1e-6
	if prevAvg > 0 && value != prevAvg {
		gradient = (float64(value) - float64(prevAvg)) / float64(prevAvg)
	}
	return gradient
}

// SlowScoreStat represents the statistics on business of Store.
type SlowScoreStat struct {
	avgScore            uint64
	avgTimecost         uint64
	intervalTimecost    uint64             // sum of the timecost in one counting interval. Unit: us
	intervalUpdCount    uint64             // count of update in one counting interval.
	tsCntSlidingWindow  CountSlidingWindow // sliding window on timecost
	updCntSlidingWindow CountSlidingWindow // sliding window on update count
}

func (ss *SlowScoreStat) getSlowScore() uint64 {
	return atomic.LoadUint64(&ss.avgScore)
}

// updateSlowScore updates the statistics on SlowScore periodically.
//
// updateSlowScore will update the SlowScore of each Store according to the two factors:
//   - Requests in one timing tick. This factor can be regarded as QPS on each store.
//   - Average timecost on each request in one timing tick. This factor is used to detect
//     whether the relative store is busy on processing requests.
//
// If one Store is slow, its Requests will keep decreasing gradually, but Average timecost will
// keep ascending. And the updating algorithm just follows this mechanism and compute the
// trend of slow, by calculating gradients of slow in each tick.
func (ss *SlowScoreStat) updateSlowScore() {
	if atomic.LoadUint64(&ss.avgTimecost) == 0 {
		// Init the whole statistics.
		atomic.StoreUint64(&ss.avgScore, slowScoreInitVal)
		atomic.StoreUint64(&ss.avgTimecost, slowScoreInitTimeoutInUs)
		return
	}

	avgTimecost := atomic.LoadUint64(&ss.avgTimecost)
	intervalUpdCount := atomic.LoadUint64(&ss.intervalUpdCount)
	intervalTimecost := atomic.LoadUint64(&ss.intervalTimecost)

	updGradient := float64(1.0)
	tsGradient := float64(1.0)
	if intervalUpdCount > 0 {
		intervalAvgTimecost := intervalTimecost / intervalUpdCount
		updGradient = ss.updCntSlidingWindow.Append(intervalUpdCount)
		tsGradient = ss.tsCntSlidingWindow.Append(intervalAvgTimecost)
	}
	// Update avgScore & avgTimecost
	avgScore := atomic.LoadUint64(&ss.avgScore)
	if updGradient+0.1 <= float64(1e-9) && tsGradient-0.1 >= float64(1e-9) {
		risenRatio := math.Min(float64(5.43), math.Abs(tsGradient/updGradient))
		curAvgScore := math.Ceil(math.Min(float64(avgScore)*risenRatio+float64(1.0), float64(slowScoreMax)))
		atomic.CompareAndSwapUint64(&ss.avgScore, avgScore, uint64(curAvgScore))
	} else {
		costScore := uint64(math.Ceil(math.Max(float64(slowScoreInitVal), math.Min(float64(2.71), 1.0+math.Abs(updGradient)))))
		if avgScore <= slowScoreInitVal+costScore {
			atomic.CompareAndSwapUint64(&ss.avgScore, avgScore, slowScoreInitVal)
		} else {
			atomic.CompareAndSwapUint64(&ss.avgScore, avgScore, avgScore-costScore)
		}
	}
	atomic.CompareAndSwapUint64(&ss.avgTimecost, avgTimecost, ss.tsCntSlidingWindow.Avg())

	// Resets the counter of inteval timecost
	atomic.StoreUint64(&ss.intervalTimecost, 0)
	atomic.StoreUint64(&ss.intervalUpdCount, 0)
}

// recordSlowScoreStat records the timecost of each request.
func (ss *SlowScoreStat) recordSlowScoreStat(timecost time.Duration) {
	atomic.AddUint64(&ss.intervalUpdCount, 1)
	avgTimecost := atomic.LoadUint64(&ss.avgTimecost)
	if avgTimecost == 0 {
		// Init the whole statistics with the original one.
		atomic.StoreUint64(&ss.avgScore, slowScoreInitVal)
		atomic.StoreUint64(&ss.avgTimecost, slowScoreInitTimeoutInUs)
		atomic.StoreUint64(&ss.intervalTimecost, uint64(timecost/time.Microsecond))
		return
	}
	curTimecost := uint64(timecost / time.Microsecond)
	if curTimecost >= slowScoreMaxTimeoutInUs {
		// Current query is too slow to serve (>= 30s, max timeout of a request) in this tick.
		atomic.StoreUint64(&ss.avgScore, slowScoreMax)
		return
	}
	atomic.AddUint64(&ss.intervalTimecost, curTimecost)
}

func (ss *SlowScoreStat) markAlreadySlow() {
	atomic.StoreUint64(&ss.avgScore, slowScoreMax)
}

func (ss *SlowScoreStat) isSlow() bool {
	return ss.getSlowScore() >= slowScoreThreshold
}

// replicaFlowsType indicates the type of the destination replica of flows.
type replicaFlowsType int

const (
	// toLeader indicates that flows are sent to leader replica.
	toLeader replicaFlowsType = iota
	// toFollower indicates that flows are sent to followers' replica
	toFollower
	// numflowsDestType reserved to keep max replicaFlowsType value.
	numReplicaFlowsType
)

func (a replicaFlowsType) String() string {
	switch a {
	case toLeader:
		return "ToLeader"
	case toFollower:
		return "ToFollower"
	default:
		return fmt.Sprintf("%d", a)
	}
}
