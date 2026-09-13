// Copyright 2023 TiKV Authors
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
	"time"

	"github.com/pingcap/errors"
	"github.com/tiancaiamao/gp"
)

// ErrPoolClosed is returned by Pool.Run when the pool has been closed. The
// submitted function is rejected and will never be executed.
var ErrPoolClosed = errors.New("the goroutine pool is closed")

// Pool is a simple interface for goroutine pool.
type Pool interface {
	// Run executes the function in a separate goroutine. A non-nil error must
	// be returned if and only if the function is rejected, which means it will
	// never be executed and the caller owns the cleanup of anything it has
	// prepared for the function.
	Run(func()) error
	// Close releases the goroutines of the pool. In-flight functions are not
	// interrupted. It must be safe to call Close more than once.
	Close()
}

// Spool is a simple implementation of Pool.
type Spool struct {
	gp.Pool
	mu struct {
		sync.RWMutex
		closed bool
	}
}

// NewSpool creates a new Spool.
func NewSpool(n int, dur time.Duration) *Spool {
	return &Spool{Pool: *gp.New(n, dur)}
}

// Run implements Pool.Run.
func (p *Spool) Run(fn func()) error {
	// The underlying pool silently drops the functions submitted after it is
	// closed. Submitting under the read lock makes the outcome unambiguous: the
	// function is either accepted by a pool that is still open, or rejected
	// with ErrPoolClosed here.
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		return ErrPoolClosed
	}
	p.Go(fn)
	return nil
}

// Close implements Pool.Close.
func (p *Spool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.closed {
		return
	}
	p.mu.closed = true
	p.Pool.Close()
}
