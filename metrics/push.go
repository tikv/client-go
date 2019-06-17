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
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	log "github.com/sirupsen/logrus"
)

// PushMetrics pushes metrics to Prometheus Pushgateway. `instance` should be global identical.
func PushMetrics(ctx context.Context, addr string, interval time.Duration, job, instance string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		err := push.AddFromGatherer(
			job,
			map[string]string{"instance": instance},
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Errorf("cannot push metrics to prometheus pushgateway: %v", err)
		}
	}
}
