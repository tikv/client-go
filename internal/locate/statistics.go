// Copyright 2022 TiKV Authors
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
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const baseAggFactor float64 = 0.8

type loadStats struct {
	cpuPercent  float64
	cpuTotal    float64
	updatedTime int64
}

type storeLoadStats struct {
	value       atomic.Value
	lastCheckTs int64
}

func (s *storeLoadStats) GetLastCheckTime() time.Time {
	ts := atomic.LoadInt64(&s.lastCheckTs)
	return time.Unix(ts/1000_000_000, ts%1000_000_000)
}

func (s *storeLoadStats) UpdateLastCheckTs(t time.Time) {
	atomic.StoreInt64(&s.lastCheckTs, t.UnixNano())
}

func (s *storeLoadStats) Update(stat *loadStats) {
	lastStats := s.Load()
	if lastStats == nil {
		logutil.BgLogger().Info("update nill stats")
		s.value.Store(stat)
		return
	}
	// the old statistics is outdated
	durMs := stat.updatedTime - lastStats.updatedTime
	if durMs > int64(time.Second)*10 {
		s.value.Store(stat)
		logutil.BgLogger().Info("update outdated stats")
		return
	}

	factor := baseAggFactor
	stat.cpuPercent = lastStats.cpuPercent*factor + stat.cpuPercent*(1-factor)
	stat.cpuTotal = lastStats.cpuTotal*factor + stat.cpuTotal*(1-factor)
	s.value.Store(stat)
}

func (s *storeLoadStats) Load() *loadStats {
	stats := s.value.Load()
	if stats == nil {
		return nil
	}
	return stats.(*loadStats)
}

type regionLocalReadChecker struct {
	atomic.Value
}

type regionStats struct {
	leaderPercent int
	updatedTime   time.Time
}

func (r *regionLocalReadChecker) load() *regionStats {
	if v := r.Load(); v != nil {
		return v.(*regionStats)
	}
	return nil
}

func (r *regionLocalReadChecker) ShouldClosestRead(delta int, regionID uint64, stats []float64) bool {
	now := time.Now()
	lastStats := r.load()
	if lastStats == nil {
		r.Store(&regionStats{
			updatedTime: now,
		})
		logutil.BgLogger().Info("reset null leader percent", zap.Uint64("region", regionID))
		return true
	}
	dur := now.Sub(lastStats.updatedTime)
	if dur >= 10*time.Second {
		r.Store(&regionStats{
			leaderPercent: lastStats.leaderPercent / 2,
			updatedTime:   now,
		})
		logutil.BgLogger().Info("reset outdated leader percent", zap.Uint64("region", regionID))
		return int(util.Uint32N(100)) >= lastStats.leaderPercent/2
	}
	leaderPercent := lastStats.leaderPercent
	if dur >= time.Second {
		leaderPercent += delta
		if leaderPercent < 0 {
			leaderPercent = 0
		} else if leaderPercent > 100 {
			leaderPercent = 100
		}
		r.Store(&regionStats{
			updatedTime:   now,
			leaderPercent: leaderPercent,
		})
		logutil.BgLogger().Info("update leader percent", zap.Uint64("region", regionID),
			zap.Int("old", lastStats.leaderPercent), zap.Int("new", leaderPercent),
			zap.Any("stats", stats))
	}
	return int(util.Uint32N(100)) >= leaderPercent
}
