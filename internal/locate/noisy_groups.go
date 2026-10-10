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

const noisyGroupsFreshDuration = healthFeedbackFreshDuration

// noisyReport is one store's statement about which resource groups overload
// it, published as a unit with the time this client adopted it.
type noisyReport struct {
	set map[string]struct{}
	at  time.Time
}

// noisyGroups is the set of resource groups a store blames for its own
// overload, from its last HealthFeedback. A blamed group's read deadlines back
// off instead of retrying at once, so this is read on the retry path and not
// only for metrics.
//
// The zero value means unknown, not "nobody is noisy": a store too old to
// report the set leaves it empty forever. Ordering is handled before a set
// gets here, by Store.recordHealthFeedback.
type noisyGroups struct {
	// nil until the first report, then replaced wholesale by each one.
	report atomic.Pointer[noisyReport]
}

// replace adopts the reported set as of now. An empty names clears the set.
func (n *noisyGroups) replace(names []string, now time.Time) {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	n.report.Store(&noisyReport{set: set, at: now})
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
