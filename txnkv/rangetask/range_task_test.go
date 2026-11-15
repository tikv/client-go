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

package rangetask_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestProgressLogWhileWaitingForWorkers checks that the progress log keeps being
// printed after all tasks have been dispatched and the runner only waits for the workers.
func TestProgressLogWhileWaitingForWorkers(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	// A single region, so the only task is dispatched immediately.
	testutils.BootstrapWithMultiRegions(cluster)
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	core, logs := observer.New(zapcore.InfoLevel)
	ctx := context.WithValue(context.Background(), logutil.CtxLogKey, zap.New(core))

	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(ctx context.Context, r kv.KeyRange) (rangetask.TaskStat, error) {
		close(started)
		<-release
		return rangetask.TaskStat{CompletedRegions: 1}, nil
	}
	runner := rangetask.NewRangeTaskRunner("test-progress-log", store, 1, handler)
	runner.SetStatLogInterval(10 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- runner.RunOnRange(ctx, nil, nil) }()

	<-started
	require.Eventually(t, func() bool {
		return logs.FilterMessage("range task in progress").Len() > 0
	}, 2*time.Second, 5*time.Millisecond, "no progress log while waiting for workers")

	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("RunOnRange did not return after the workers finished")
	}
	require.Equal(t, 1, logs.FilterMessage("range task finished").Len())
}
