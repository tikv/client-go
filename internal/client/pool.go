// Copyright 2024 TiKV Authors
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

package client

import (
	"runtime"
	"sync"

	"google.golang.org/grpc"
)

func NewSharedBufferPool() grpc.SharedBufferPool {
	return &simpleSharedBufferPool{
		pools: [poolArraySize]*bufferPool{
			newBytesPool(level0PoolMaxSize),
			newBytesPool(level1PoolMaxSize),
			newBytesPool(level2PoolMaxSize),
			newBytesPool(level3PoolMaxSize),
			newBytesPool(level4PoolMaxSize),
			newBytesPool(level4PoolMaxSize),
		},
	}
}

// simpleSharedBufferPool is a simple implementation of SharedBufferPool.
type simpleSharedBufferPool struct {
	pools [poolArraySize]*bufferPool
}

func (p *simpleSharedBufferPool) Get(size int) []byte {
	return p.pools[p.poolIdx(size)].Get(size)
}

func (p *simpleSharedBufferPool) Put(bs *[]byte) {
	p.pools[p.poolIdx(cap(*bs))].Put(bs)
}

func (p *simpleSharedBufferPool) poolIdx(size int) int {
	switch {
	case size <= level0PoolMaxSize:
		return level0PoolIdx
	case size <= level1PoolMaxSize:
		return level1PoolIdx
	case size <= level2PoolMaxSize:
		return level2PoolIdx
	case size <= level3PoolMaxSize:
		return level3PoolIdx
	case size <= level4PoolMaxSize:
		return level4PoolIdx
	default:
		return levelMaxPoolIdx
	}
}

const (
	level0PoolMaxSize = 16                     //  16  B
	level1PoolMaxSize = level0PoolMaxSize * 16 // 256  B
	level2PoolMaxSize = level1PoolMaxSize * 16 //   4 KB
	level3PoolMaxSize = level2PoolMaxSize * 16 //  64 KB
	level4PoolMaxSize = level3PoolMaxSize * 16 //   1 MB
)

const (
	level0PoolIdx = iota
	level1PoolIdx
	level2PoolIdx
	level3PoolIdx
	level4PoolIdx
	levelMaxPoolIdx
	poolArraySize
)

type bufferPool struct {
	sync.Pool

	defaultSize int
}

func (p *bufferPool) Get(size int) []byte {
	bs := p.Pool.Get().(*[]byte)

	if cap(*bs) < size {
		p.Pool.Put(bs)

		return make([]byte, size)
	}

	return (*bs)[:size]
}

func (p *bufferPool) Put(i *[]byte) {
	runtime.SetFinalizer(i, func(i *[]byte) {
		(*i) = (*i)[:0]
		p.Pool.Put(i)
	})
}

func newBytesPool(size int) *bufferPool {
	return &bufferPool{
		Pool: sync.Pool{
			New: func() any {
				bs := make([]byte, size)
				return &bs
			},
		},
		defaultSize: size,
	}
}
