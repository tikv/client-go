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
	"sync"
	"time"
)

const (
	DefaultMinLatency = 1 * time.Millisecond
	DefaultMaxLatency = 16 * time.Second

	DefaultHistogramSize = 16
)

type PerfProfile struct {
	sync.RWMutex

	MinLatency         time.Duration
	MaxLatency         time.Duration
	SigFigs            int
	PeriodicalPerfHist map[string]*PerfHistogram
	SummaryPerfHist    map[string]*PerfHistogram
}

func NewPerfProfile() *PerfProfile {
	return &PerfProfile{
		MinLatency:         DefaultMinLatency,
		MaxLatency:         DefaultMaxLatency,
		SigFigs:            1,
		PeriodicalPerfHist: make(map[string]*PerfHistogram, DefaultHistogramSize),
		SummaryPerfHist:    make(map[string]*PerfHistogram, DefaultHistogramSize),
	}
}

func (p *PerfProfile) Record(op string, latency time.Duration) {
	p.Lock()
	defer p.Unlock()

	if _, ok := p.PeriodicalPerfHist[op]; !ok {
		p.PeriodicalPerfHist[op] = NewPerfHistogram(p.MinLatency, p.MaxLatency, p.SigFigs)
	}
	p.PeriodicalPerfHist[op].Record(latency)
	if _, ok := p.SummaryPerfHist[op]; !ok {
		p.SummaryPerfHist[op] = NewPerfHistogram(p.MinLatency, p.MaxLatency, p.SigFigs)
	}
	p.SummaryPerfHist[op].Record(latency)
}

func (p *PerfProfile) Get(op string, sum bool) *PerfHistogram {
	histMap := p.PeriodicalPerfHist
	if sum {
		histMap = p.SummaryPerfHist
	}

	p.RLock()
	hist, ok := histMap[op]
	p.RUnlock()
	if !ok {
		perfHist := NewPerfHistogram(p.MinLatency, p.MaxLatency, p.SigFigs)
		p.Lock()
		histMap[op] = perfHist
		hist = histMap[op]
		p.Unlock()
	}
	return hist
}

func (p *PerfProfile) TakePeriodHist() map[string]*PerfHistogram {
	p.Lock()
	defer p.Unlock()
	periodicalHist := make(map[string]*PerfHistogram, len(p.PeriodicalPerfHist))
	swapOutHist := p.PeriodicalPerfHist
	p.PeriodicalPerfHist = periodicalHist
	return swapOutHist
}

// Prints the PerfProfile.
func (p *PerfProfile) PrintFmt(ifSummaryReport bool, outputStyle string, outputFunc func(string, string, map[string]*PerfHistogram)) {
	if ifSummaryReport {
		p.RLock()
		defer p.RUnlock()
		outputFunc(outputStyle, "[Summary] ", p.SummaryPerfHist)
		return
	}
	// Clear current PerfHistogram and print current PerfHistogram.
	periodicalHist := p.TakePeriodHist()
	p.RLock()
	defer p.RUnlock()
	outputFunc(outputStyle, "[Current] ", periodicalHist)
}

func (p *PerfProfile) Clear() {
	p.Lock()
	defer p.Unlock()

	perfHist := p.PeriodicalPerfHist
	for k := range perfHist {
		delete(perfHist, k)
	}
	perfHist = p.SummaryPerfHist
	for k := range perfHist {
		delete(perfHist, k)
	}

	p.PeriodicalPerfHist = make(map[string]*PerfHistogram, DefaultHistogramSize)
	p.SummaryPerfHist = make(map[string]*PerfHistogram, DefaultHistogramSize)
}
