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
	"context"
	"errors"
	"sync"
)

// State represents the state of a run loop.
type State uint32

const (
	StateIdle State = iota
	StateWaiting
	StateRunning
)

// RunLoop implements the Executor interface.
type RunLoop struct {
	Pool

	lock     sync.Mutex
	ready    chan struct{}
	runnable []func()
	running  []func()
	state    State
}

// NewRunLoop creates a new run-loop.
func NewRunLoop() *RunLoop {
	return &RunLoop{ready: make(chan struct{})}
}

// Go submits f to the pool when possible (pool is not nil), otherwise starts a new goroutine for f.
func (l *RunLoop) Go(f func()) {
	if l.Pool == nil {
		go f()
	} else {
		l.Pool.Go(f)
	}
}

// State returns the current state of the run-loop.
func (l *RunLoop) State() State {
	l.lock.Lock()
	state := l.state
	l.lock.Unlock()
	return state
}

// NumRunnable returns the number of runnable tasks in the run-loop currently.
func (l *RunLoop) NumRunnable() int {
	l.lock.Lock()
	n := len(l.runnable)
	l.lock.Unlock()
	return n
}

// Append implements the Executor interface. It's safe to call Append concurrently.
func (l *RunLoop) Append(fs ...func()) {
	if len(fs) == 0 {
		return
	}

	notify := false

	l.lock.Lock()
	l.runnable = append(l.runnable, fs...)
	if l.state == StateWaiting {
		l.state = StateIdle // waiting -> idle
		notify = true
	}
	l.lock.Unlock()

	if notify {
		l.ready <- struct{}{}
	}
}

// Exec drives the run-loop to execute all runnable tasks and returns the number of tasks executed. If the context is
// done before all tasks are executed, it returns the number of tasks executed and the context error. Exec turns the
// run-loop to running or waiting state during process, and finally to idle state on return. When calling Exec without
// pending runnables, the run-loop turns to waiting, in which case one should make sure that Append will be called in
// the other goroutine to wake it up later, or the context will be canceled finally to break the waiting. Exec should
// only be called by one goroutine.
func (l *RunLoop) Exec(ctx context.Context) (int, error) {
	for {
		l.lock.Lock()
		if l.state != StateIdle {
			l.lock.Unlock()
			return 0, errors.New("runloop: already executing")
		}
		// assert l.state == stateIdle

		if len(l.runnable) == 0 {
			l.state = StateWaiting // idle -> waiting
			l.lock.Unlock()
			select {
			case <-l.ready:
				continue
			case <-ctx.Done():
				l.lock.Lock()
				l.state = StateIdle // waiting -> idle
				l.lock.Unlock()
				return 0, ctx.Err()
			}
		} else {
			l.running, l.runnable = l.runnable, l.running[:0]
			l.state = StateRunning // idle -> running
			l.lock.Unlock()
			return l.run(ctx)
		}
	}
}

func (l *RunLoop) run(ctx context.Context) (int, error) {
	count := 0
	for {
		for i, f := range l.running {
			select {
			case <-ctx.Done():
				l.lock.Lock()
				// move remaining running tasks to runnable
				l.running = append(l.running[:0], l.running[i:]...)
				l.running = append(l.running, l.runnable...)
				l.running, l.runnable = l.runnable, l.running
				l.state = StateIdle // running -> idle
				l.lock.Unlock()
				return count, ctx.Err()
			default:
				f()
				count++
			}
		}
		l.lock.Lock()
		if len(l.runnable) == 0 {
			l.state = StateIdle // running -> idle
			l.lock.Unlock()
			return count, nil
		}
		l.running, l.runnable = l.runnable, l.running[:0]
		l.lock.Unlock()
	}
}
