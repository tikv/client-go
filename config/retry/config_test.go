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

package retry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/metrics"
)

// backoffSampleCount returns how many backoffs the histogram has recorded
// under the given type label, and whether a series with that label exists.
func backoffSampleCount(t *testing.T, typ string) (uint64, bool) {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() {
		metrics.TiKVBackoffHistogram.Collect(ch)
		close(ch)
	}()
	var (
		count uint64
		found bool
	)
	for m := range ch {
		var pb dto.Metric
		require.NoError(t, m.Write(&pb))
		for _, l := range pb.GetLabel() {
			if l.GetName() == metrics.LblType && l.GetValue() == typ {
				count, found = pb.GetHistogram().GetSampleCount(), true
			}
		}
	}
	return count, found
}

// Every backoff config reports under its own name: a config without one
// showed up in dashboards as a series with an empty type label.
func TestBackoffConfigsHaveNamedMetric(t *testing.T) {
	for _, cfg := range []*Config{BoTxnNotFound, BoMaxTsNotSynced, BoCommitTSLag, BoMaxRegionNotInitialized} {
		before, _ := backoffSampleCount(t, cfg.name)
		(*cfg.metric).Observe(0.001)
		after, found := backoffSampleCount(t, cfg.name)
		require.True(t, found, "no backoff histogram series for %s", cfg.name)
		require.Equal(t, before+1, after, "backoff of %s is not recorded under its own name", cfg.name)
	}

	_, found := backoffSampleCount(t, "")
	require.False(t, found, "a backoff histogram series with an empty type label exists")
}
