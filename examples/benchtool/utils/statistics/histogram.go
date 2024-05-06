// Copyright 2024 TiKV Authors
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

package statistics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"benchtool/utils"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Format: "Elapsed" - "Sum" - "Count" - "Ops" - "Avg" - "P50" - "P90" - "P95" - "P99" - "P999" - "P9999" - "Min" - "Max
var WorkloadFormat = []string{"Elapsed(s)", "Sum", "Count", "Ops", "Avg(ms)", "50th(ms)", "90th(ms)", "95th(ms)", "99th(ms)", "99.9th(ms)", "99.99th(ms)", "Min(ms)", "Max(ms)"}

type RuntimeStatistics struct {
	elapsed float64

	// Operation statistics
	sum   float64
	count int64
	ops   float64

	// Execution time statistics
	p50   float64
	p90   float64
	p95   float64
	p99   float64
	p999  float64
	p9999 float64
	avg   float64
	min   float64
	max   float64
}

type PerfHistogram struct {
	m sync.RWMutex

	startTime time.Time
	sum       int64
	*hdrhistogram.Histogram
}

func NewPerfHistogram(minLat, maxLat time.Duration, sf int) *PerfHistogram {
	return &PerfHistogram{Histogram: hdrhistogram.New(minLat.Nanoseconds(), maxLat.Nanoseconds(), sf), startTime: time.Now()}
}

func (h *PerfHistogram) Record(rawLatency time.Duration) {
	latency := rawLatency
	low := time.Duration(h.LowestTrackableValue())
	high := time.Duration(h.HighestTrackableValue())
	if latency < low {
		latency = low
	} else if latency > high {
		latency = high
	}

	h.m.Lock()
	err := h.RecordValue(latency.Nanoseconds())
	h.sum += rawLatency.Nanoseconds()
	h.m.Unlock()
	if err != nil {
		panic(fmt.Sprintf(`recording value error: %s`, err))
	}
}

func (h *PerfHistogram) Empty() bool {
	h.m.Lock()
	defer h.m.Unlock()
	return h.TotalCount() == 0
}

func (h *PerfHistogram) Format() []string {
	res := h.GetRuntimeStatistics()

	// Format: "Elapsed(s)" - "Sum" - "Count" - "Ops" - "Avg" - "P50" - "P90" - "P95" - "P99" - "P999" - "P9999" - "Min" - "Max
	return []string{
		utils.FloatToString(res.elapsed),
		utils.FloatToString(res.sum),
		utils.IntToString(res.count),
		utils.FloatToString(res.ops * 60),
		utils.FloatToString(res.avg),
		utils.FloatToString(res.p50),
		utils.FloatToString(res.p90),
		utils.FloatToString(res.p95),
		utils.FloatToString(res.p99),
		utils.FloatToString(res.p999),
		utils.FloatToString(res.p9999),
		utils.FloatToString(res.min),
		utils.FloatToString(res.max),
	}
}

func (h *PerfHistogram) GetRuntimeStatistics() RuntimeStatistics {
	h.m.RLock()
	defer h.m.RUnlock()
	sum := time.Duration(h.sum).Seconds() * 1000
	avg := time.Duration(h.Mean()).Seconds() * 1000
	elapsed := time.Since(h.startTime).Seconds()
	count := h.TotalCount()
	ops := float64(count) / elapsed
	info := RuntimeStatistics{
		elapsed: elapsed,
		sum:     sum,
		count:   count,
		ops:     ops,
		avg:     avg,
		p50:     time.Duration(h.ValueAtQuantile(50)).Seconds() * 1000,
		p90:     time.Duration(h.ValueAtQuantile(90)).Seconds() * 1000,
		p95:     time.Duration(h.ValueAtQuantile(95)).Seconds() * 1000,
		p99:     time.Duration(h.ValueAtQuantile(99)).Seconds() * 1000,
		p999:    time.Duration(h.ValueAtQuantile(99.9)).Seconds() * 1000,
		p9999:   time.Duration(h.ValueAtQuantile(99.99)).Seconds() * 1000,
		min:     time.Duration(h.Min()).Seconds() * 1000,
		max:     time.Duration(h.Max()).Seconds() * 1000,
	}
	return info
}

func HistogramOutputFunc(outputStyle string, prefix string, perfHist map[string]*PerfHistogram) {
	keys := make([]string, 0, len(perfHist))
	for k := range perfHist {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := [][]string{}
	for _, op := range keys {
		hist := perfHist[op]
		if !hist.Empty() {
			op = strings.ToUpper(op)
			line := []string{prefix, op}
			line = append(line, hist.Format()...)
			fmt.Println(line)
			lines = append(lines, line)
		}
	}
	switch outputStyle {
	case utils.OutputStylePlain:
		utils.RenderString("%s%-6s - %s\n", WorkloadFormat, lines)
	case utils.OutputStyleTable:
		utils.RenderTable(WorkloadFormat, lines)
	case utils.OutputStyleJson:
		utils.RenderJson(WorkloadFormat, lines)
	}
}
