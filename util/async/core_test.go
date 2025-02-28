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

package async

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockExecutor struct {
	lock  sync.Mutex
	tasks []func()
}

func (e *mockExecutor) Go(f func()) {
	e.Append(f)
}

func (e *mockExecutor) Append(fs ...func()) {
	e.lock.Lock()
	e.tasks = append(e.tasks, fs...)
	e.lock.Unlock()
}

func TestInjectOrder(t *testing.T) {
	cb := NewCallback(&mockExecutor{}, func(ns []int, err error) {
		require.NoError(t, err)
		// injected functions are executed in reverse order
		require.Equal(t, []int{1, 2, 3}, ns)
	})
	cb.Inject(func(ns []int, err error) ([]int, error) { return append(ns, 3), nil })
	cb.Inject(func(ns []int, err error) ([]int, error) { return append(ns, 2), nil })
	cb.Inject(func(ns []int, err error) ([]int, error) { return append(ns, 1), nil })
	cb.Invoke([]int{}, nil)
}

func TestFulfillOnce(t *testing.T) {
	t.Run("InvokeTwice", func(t *testing.T) {
		ns := []int{}
		cb := NewCallback(&mockExecutor{}, func(n int, err error) { ns = append(ns, n) })
		cb.Invoke(1, nil)
		cb.Invoke(2, nil)
		require.Equal(t, []int{1}, ns)
	})
	t.Run("ScheduleTwice", func(t *testing.T) {
		e := &mockExecutor{}
		ns := []int{}
		cb := NewCallback(e, func(n int, err error) { ns = append(ns, n) })
		cb.Schedule(1, nil)
		cb.Schedule(2, nil)
		require.Equal(t, 1, len(e.tasks))
		require.Equal(t, []int{}, ns)
		e.tasks[0]()
		require.Equal(t, []int{1}, ns)
	})
	t.Run("InvokeSchedule", func(t *testing.T) {
		e := &mockExecutor{}
		ns := []int{}
		cb := NewCallback(e, func(n int, err error) { ns = append(ns, n) })
		cb.Invoke(1, nil)
		cb.Schedule(2, nil)
		require.Equal(t, 0, len(e.tasks))
		require.Equal(t, []int{1}, ns)
	})
	t.Run("ScheduleInvoke", func(t *testing.T) {
		e := &mockExecutor{}
		ns := []int{}
		cb := NewCallback(e, func(n int, err error) { ns = append(ns, n) })
		cb.Schedule(1, nil)
		cb.Invoke(2, nil)
		require.Equal(t, 1, len(e.tasks))
		require.Equal(t, []int{}, ns)
		e.tasks[0]()
		require.Equal(t, []int{1}, ns)
	})
}
