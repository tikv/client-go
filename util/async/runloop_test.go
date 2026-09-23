// Copyright 2025 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package async

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGo(t *testing.T) {
	l := NewRunLoop()
	n := uint32(0)

	// Go works when pool is nil (by default)
	require.Nil(t, l.Pool)
	l.Go(func() { atomic.StoreUint32(&n, 1) })
	require.Eventually(t, func() bool { return atomic.LoadUint32(&n) == 1 }, time.Second, time.Millisecond)

	// use a customized pool
	pool := &mockExecutor{}
	l.Pool = pool
	l.Go(func() { atomic.StoreUint32(&n, 2) })
	require.Equal(t, 1, len(pool.tasks))

	require.Equal(t, uint32(1), atomic.LoadUint32(&n))
	pool.tasks[0]()
	require.Equal(t, uint32(2), atomic.LoadUint32(&n))
}

func TestExecWait(t *testing.T) {
	var list []int
	l := NewRunLoop()
	time.AfterFunc(time.Millisecond, func() {
		l.Append(func() {
			list = append(list, 1)
		})
	})
	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)
}

func TestExecOnce(t *testing.T) {
	var list []int
	l := NewRunLoop()
	l.Append(func() {
		l.Append(func() {
			list = append(list, 2)
		})
		list = append(list, 1)
	})

	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 2, n)
	require.Equal(t, []int{1, 2}, list)
}

func TestExecTwice(t *testing.T) {
	var list []int
	l := NewRunLoop()
	l.Append(func() {
		time.AfterFunc(time.Millisecond, func() {
			l.Append(func() {
				list = append(list, 2)
			})
		})
		list = append(list, 1)
	})

	n, err := l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)

	n, err = l.Exec(context.Background())
	require.NoError(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1, 2}, list)
}

func TestExecCancelWhileRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var list []int
	l := NewRunLoop()
	l.Append(
		func() {
			cancel()
			list = append(list, 1)
		},
		func() {
			list = append(list, 2)
		},
	)

	n, err := l.Exec(ctx)
	require.Error(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 1, l.NumRunnable())
	require.Equal(t, 1, n)
	require.Equal(t, []int{1}, list)
}

func TestExecCancelWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	l := NewRunLoop()
	time.AfterFunc(time.Millisecond, cancel)

	n, err := l.Exec(ctx)
	require.Error(t, err)
	require.Equal(t, StateIdle, l.State())
	require.Equal(t, 0, l.NumRunnable())
	require.Equal(t, 0, n)
}

func TestExecCancelWhileAppendNotifying(t *testing.T) {
	defaultMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(defaultMaxProcs)

	for attempt := 0; attempt < 100; attempt++ {
		l := NewRunLoop()
		called := false
		ctx, cancel := context.WithCancel(context.Background())
		execDone := make(chan error, 1)
		go func() {
			_, err := l.Exec(ctx)
			execDone <- err
		}()

		require.Eventually(t, func() bool {
			return l.State() == StateWaiting
		}, time.Second, time.Millisecond, "run loop did not enter waiting state")

		// Queue Append on the mutex before waking Exec through cancellation. If Append acquires the mutex first, it
		// changes the state to idle and then tries to notify an Exec that has already selected ctx.Done().
		l.lock.Lock()
		appendDone := make(chan struct{})
		go func() {
			l.Append(func() {
				called = true
			})
			close(appendDone)
		}()
		runtime.Gosched()
		cancel()
		runtime.Gosched()
		l.lock.Unlock()

		select {
		case err := <-execDone:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("Exec did not return after cancellation")
		}

		select {
		case <-appendDone:
		case <-time.After(time.Second):
			t.Fatalf("Append remained blocked after Exec cancellation on attempt %d", attempt+1)
		}

		require.Equal(t, StateIdle, l.State())
		require.Equal(t, 1, l.NumRunnable())

		n, err := l.Exec(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.True(t, called)
		require.Equal(t, StateIdle, l.State())
		require.Equal(t, 0, l.NumRunnable())
	}
}

func TestExecConcurrent(t *testing.T) {
	l := NewRunLoop()
	started := make(chan struct{})
	release := make(chan struct{})
	l.Append(func() {
		close(started)
		<-release
	})
	type execResult struct {
		n   int
		err error
	}
	done := make(chan execResult, 1)
	go func() {
		n, err := l.Exec(context.Background())
		done <- execResult{n: n, err: err}
	}()

	<-started
	n, err := l.Exec(context.Background())
	close(release)
	result := <-done

	require.Error(t, err)
	require.Equal(t, 0, n)
	require.NoError(t, result.err)
	require.Equal(t, 1, result.n)
}
