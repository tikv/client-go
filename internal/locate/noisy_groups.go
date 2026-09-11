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

import "sync/atomic"

// noisyGroups is the set of resource groups one store currently blames for its
// own overload, as last reported in that store's HealthFeedback.
//
// A report naming anyone marks the whole store overloaded, which is mirrored
// onto StoreHealthStatus so the replica selector can consult it the same way it
// consults IsSlow. This set is kept only to attribute blame in metrics: which
// group is named changes nothing about how a request is routed.
//
// The zero value means nothing is known, which is deliberately not the same as
// knowing that nobody is noisy: a store running a version that does not report
// the set leaves this empty forever, and reading that as a clean bill of health
// would be wrong.
type noisyGroups struct {
	// nil until the first report, then replaced wholesale by each one. A read
	// is a single atomic load, and a group that stops being blamed stops being
	// pinned as soon as the next report lands rather than after a timeout.
	set atomic.Pointer[map[string]struct{}]
}

// replace adopts a store's newly reported set. An empty names is a positive
// report that the store blames nobody, and clears the previous set.
func (n *noisyGroups) replace(names []string) {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	n.set.Store(&set)
}

// contains reports whether the store last named this group.
func (n *noisyGroups) contains(group string) bool {
	if group == "" {
		return false
	}
	set := n.set.Load()
	if set == nil {
		return false
	}
	_, ok := (*set)[group]
	return ok
}
