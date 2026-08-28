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

package client

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundedBufferPoolReusesCapacity(t *testing.T) {
	pool := newBoundedBufferPool(1<<20, 256, 4<<10, 16<<10)
	first := pool.Get(8 << 10)
	require.Len(t, *first, 8<<10)
	require.Equal(t, 16<<10, cap(*first))
	firstByte := &(*first)[0]

	pool.Put(first)
	require.Equal(t, 16<<10, pool.retainedMemory())
	second := pool.Get(12 << 10)
	require.Same(t, firstByte, &(*second)[0])
	require.Equal(t, 16<<10, cap(*second))
	require.Zero(t, pool.retainedMemory())
}

func TestBoundedBufferPoolCapsRetentionAndDropsOversizedBuffers(t *testing.T) {
	pool := newBoundedBufferPool(16<<10, 4<<10, 16<<10)
	first := pool.Get(16 << 10)
	second := pool.Get(16 << 10)
	pool.Put(first)
	pool.Put(second)
	require.Equal(t, 16<<10, pool.retainedMemory())

	oversized := pool.Get(32 << 10)
	require.Equal(t, 32<<10, cap(*oversized))
	pool.Put(oversized)
	require.Equal(t, 16<<10, pool.retainedMemory())
}

func TestBoundedBufferPoolUsesSmallestFittingClass(t *testing.T) {
	pool := newBoundedBufferPool(1<<20, 256, 4<<10, 16<<10)
	for _, testCase := range []struct {
		length       int
		wantCapacity int
	}{
		{length: 1, wantCapacity: 256},
		{length: 256, wantCapacity: 256},
		{length: 257, wantCapacity: 4 << 10},
		{length: 4 << 10, wantCapacity: 4 << 10},
		{length: (4 << 10) + 1, wantCapacity: 16 << 10},
		{length: (16 << 10) + 1, wantCapacity: (16 << 10) + 1},
	} {
		buffer := pool.Get(testCase.length)
		require.Equal(t, testCase.wantCapacity, cap(*buffer))
	}
}

func TestBoundedBufferPoolConcurrentRetentionIsBounded(t *testing.T) {
	const maxRetainedBytes = 64 << 10
	pool := newBoundedBufferPool(maxRetainedBytes, 4<<10, 16<<10)
	var waitGroup sync.WaitGroup
	for range 32 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for range 100 {
				buffer := pool.Get(8 << 10)
				(*buffer)[0] = 1
				pool.Put(buffer)
			}
		}()
	}
	waitGroup.Wait()
	require.LessOrEqual(t, pool.retainedMemory(), maxRetainedBytes)
}
