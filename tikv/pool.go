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
	"time"

	"github.com/tiancaiamao/gp"
)

// Pool is a simple interface for goroutine pool.
type Pool interface {
	Run(func()) error
	Close()
}

// Spool is a simple implementation of Pool.
type Spool struct {
	gp.Pool
}

// NewSpool creates a new Spool.
func NewSpool(n int, dur time.Duration) *Spool {
	return &Spool{
		*gp.New(n, dur),
	}
}

// Run implements Pool.Run.
func (p *Spool) Run(fn func()) error {
	p.Go(fn)
	return nil
}
