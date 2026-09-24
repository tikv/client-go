// Copyright 2025 TiKV Authors
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

package tikv

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/internal/mockstore/mocktikv"
	"github.com/tikv/client-go/v2/testutils"
)

func TestSpoolRejectsTasksAfterClose(t *testing.T) {
	p := NewSpool(2, 10*time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, p.Run(wg.Done))
	wg.Wait()

	p.Close()

	var executed atomic.Bool
	require.ErrorIs(t, p.Run(func() { executed.Store(true) }), ErrPoolClosed)
	require.False(t, executed.Load())

	// Close must be idempotent: closing the underlying pool twice panics.
	p.Close()
}

func TestKVStoreGoTracksBackgroundJobs(t *testing.T) {
	client, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	mocktikv.BootstrapWithMultiStores(cluster, 1)
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	require.NoError(t, store.Go(func() {
		close(started)
		<-release
		close(finished)
	}))
	<-started

	var closeErr error
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		closeErr = store.Close()
	}()

	// Close waits for the background job instead of racing with it.
	select {
	case <-returned:
		require.FailNow(t, "Close returned before the background job finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-finished
	<-returned
	require.NoError(t, closeErr)

	// A closing store admits no new background job, and reports the rejection
	// so that the caller can clean up after it.
	var executed atomic.Bool
	require.ErrorIs(t, store.Go(func() { executed.Store(true) }), ErrStoreClosed)
	require.False(t, executed.Load())
}
