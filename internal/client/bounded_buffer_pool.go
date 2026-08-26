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

import "sync"

const reusableScanBufferPoolMaxRetainedBytes = 16 << 20

var reusableScanBufferPoolCapacities = [...]int{
	256,
	4 << 10,
	16 << 10,
	32 << 10,
	1 << 20,
}

type boundedBufferBucket struct {
	capacity int
	buffers  []*[]byte
}

// boundedBufferPool implements gRPC's buffer pool contract while keeping an
// explicit upper bound on retained backing arrays. It is shared by the
// reusable-scan connections owned by one RPCClient.
type boundedBufferPool struct {
	mu               sync.Mutex
	maxRetainedBytes int
	retainedBytes    int
	buckets          []boundedBufferBucket
}

func newBoundedBufferPool(maxRetainedBytes int, capacities ...int) *boundedBufferPool {
	if maxRetainedBytes < 0 {
		maxRetainedBytes = 0
	}
	pool := &boundedBufferPool{
		maxRetainedBytes: maxRetainedBytes,
		buckets:          make([]boundedBufferBucket, 0, len(capacities)),
	}
	previous := 0
	for _, capacity := range capacities {
		if capacity <= previous {
			panic("bounded buffer pool capacities must be positive and strictly increasing")
		}
		pool.buckets = append(pool.buckets, boundedBufferBucket{capacity: capacity})
		previous = capacity
	}
	return pool
}

func (p *boundedBufferPool) Get(length int) *[]byte {
	bucketIndex := p.bucketIndexForLength(length)
	if bucketIndex < 0 {
		buffer := make([]byte, length)
		return &buffer
	}

	p.mu.Lock()
	bucket := &p.buckets[bucketIndex]
	bufferCount := len(bucket.buffers)
	if bufferCount == 0 {
		capacity := bucket.capacity
		p.mu.Unlock()
		buffer := make([]byte, length, capacity)
		return &buffer
	}
	buffer := bucket.buffers[bufferCount-1]
	bucket.buffers[bufferCount-1] = nil
	bucket.buffers = bucket.buffers[:bufferCount-1]
	p.retainedBytes -= bucket.capacity
	p.mu.Unlock()

	*buffer = (*buffer)[:length]
	return buffer
}

func (p *boundedBufferPool) Put(buffer *[]byte) {
	if buffer == nil {
		return
	}
	bucketIndex := p.bucketIndexForCapacity(cap(*buffer))
	if bucketIndex < 0 {
		return
	}

	p.mu.Lock()
	bucket := &p.buckets[bucketIndex]
	if p.retainedBytes+bucket.capacity > p.maxRetainedBytes {
		p.mu.Unlock()
		return
	}
	*buffer = (*buffer)[:bucket.capacity]
	bucket.buffers = append(bucket.buffers, buffer)
	p.retainedBytes += bucket.capacity
	p.mu.Unlock()
}

func (p *boundedBufferPool) bucketIndexForLength(length int) int {
	if length <= 0 {
		return -1
	}
	for i := range p.buckets {
		if length <= p.buckets[i].capacity {
			return i
		}
	}
	return -1
}

func (p *boundedBufferPool) bucketIndexForCapacity(capacity int) int {
	for i := range p.buckets {
		if capacity == p.buckets[i].capacity {
			return i
		}
	}
	return -1
}

func (p *boundedBufferPool) retainedMemory() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retainedBytes
}
