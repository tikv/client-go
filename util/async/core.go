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
)

// Pool is an interface for goroutine pool.
type Pool interface {
	// Go submits a function to the goroutine pool.
	Go(f func())
}

// Executor is an interface that can append functions to be executed asynchronously.
type Executor interface {
	Pool
	// Append adds functions to the executor. It should be safe to call Append concurrently.
	Append(fs ...func())
}

// Callback defines a callback function that can be invoked immediately or later.
type Callback[T any] interface {
	// Executor returns the executor that the callback uses.
	Executor() Executor
	// Inject adds a deferred action that will be invoked before the callback.
	Inject(g func(T, error) (T, error))
	// Invoke invokes the callback immediately in current goroutine.
	Invoke(val T, err error)
	// Schedule schedules the callback to be invoked later, it's typically called in other goroutines.
	Schedule(val T, err error)
}

// NewCallback creates a new callback function.
func NewCallback[T any](e Executor, f func(T, error)) Callback[T] {
	return &callback[T]{e: e, f: f}
}

type callback[T any] struct {
	once sync.Once
	e    Executor
	f    func(T, error)
	gs   []func(T, error) (T, error)
}

// Executor implements Callback.
func (cb *callback[T]) Executor() Executor {
	return cb.e
}

// Inject implements Callback.
func (cb *callback[T]) Inject(g func(T, error) (T, error)) {
	cb.gs = append(cb.gs, g)
}

// Invoke implements Callback.
func (cb *callback[T]) Invoke(val T, err error) {
	cb.once.Do(func() { cb.call(val, err) })
}

// Schedule implements Callback.
func (cb *callback[T]) Schedule(val T, err error) {
	cb.once.Do(func() { cb.e.Append(func() { cb.call(val, err) }) })
}

func (cb *callback[T]) call(val T, err error) {
	for i := len(cb.gs) - 1; i >= 0; i-- {
		val, err = cb.gs[i](val, err)
	}
	cb.f(val, err)
}
