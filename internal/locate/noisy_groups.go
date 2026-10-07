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

package locate

import (
	"sync/atomic"
	"time"
)

const noisyGroupsFreshDuration = storeOverloadedDuration

// noisyReport is one store's statement about which resource groups overload
// it, published as a unit so a reader never sees the set from one report with
// the age or sequence of another.
type noisyReport struct {
	set map[string]struct{}
	// FeedbackSeqNo of the feedback that carried the set; 0 when adopted
	// without one.
	seq uint64
	// When this client adopted the report, which is what its freshness is
	// measured from.
	at time.Time
}

// noisyGroups is the set of resource groups a store blames for its own
// overload, from its last HealthFeedback. A blamed group's read deadlines back
// off instead of retrying at once, so this is read on the retry path and not
// only for metrics.
//
// The zero value means unknown, not "nobody is noisy": a store too old to
// report the set leaves it empty forever.
type noisyGroups struct {
	// nil until the first report, then replaced wholesale by each one. A read
	// is a single atomic load, and a group that stops being blamed stops being
	// pinned as soon as the next report lands rather than after a timeout.
	report atomic.Pointer[noisyReport]
}

// record stores the reported set and returns true, unless the held report has
// a higher sequence and is younger than noisyGroupsFreshDuration, in which case
// the report is stale (delivered out of order on another batch connection) and
// is dropped. An expired report loses regardless of sequence, since the
// sequence restarts with the TiKV process. An empty names clears the set.
//
// TiKV assigns the sequence before it snapshots the groups, so two interleaved
// producers can give the newer snapshot the lower sequence; that snapshot is
// then dropped until the next report, one feedback interval later.
func (n *noisyGroups) record(names []string, seq uint64, now time.Time) bool {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	next := &noisyReport{set: set, seq: seq, at: now}
	for {
		prev := n.report.Load()
		if prev != nil && seq < prev.seq && now.Sub(prev.at) < noisyGroupsFreshDuration {
			return false
		}
		if n.report.CompareAndSwap(prev, next) {
			return true
		}
	}
}

// replace adopts a set unconditionally, as if freshly reported with no
// sequence. For tests and for callers that have no feedback to cite.
func (n *noisyGroups) replace(names []string) {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	n.report.Store(&noisyReport{set: set, at: time.Now()})
}

// contains reports whether the store named this group recently enough for the
// report to still be trusted.
func (n *noisyGroups) contains(group string, now time.Time) bool {
	if group == "" {
		return false
	}
	r := n.report.Load()
	if r == nil || now.Sub(r.at) >= noisyGroupsFreshDuration {
		return false
	}
	_, ok := r.set[group]
	return ok
}
