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

package client

import "container/heap"

// Item is the interface that all entries in a priority queue must implement.
type Item interface {
	priority() int
}

// entry is an entry in a priority queue.
type entry struct {
	entry Item
	index int
}

// prioritySlice implements heap.Interface and holds Entries.
type prioritySlice []entry

// Len returns the length of the priority queue.
func (ps prioritySlice) Len() int {
	return len(ps)
}

// Less compares two entries in the priority queue.
// The higher priority entry is the one with the lower value.
func (ps prioritySlice) Less(i, j int) bool {
	return ps[i].entry.priority() > ps[j].entry.priority()
}

// Swap swaps two entries in the priority queue.
func (ps prioritySlice) Swap(i, j int) {
	ps[i], ps[j] = ps[j], ps[i]
	ps[i].index = i
	ps[j].index = j
}

// Push adds an entry to the priority queue.
func (ps *prioritySlice) Push(x interface{}) {
	item := x.(entry)
	item.index = len(*ps)
	*ps = append(*ps, item)
}

// Pop removes the highest priority entry from the priority queue.
func (ps *prioritySlice) Pop() interface{} {
	old := *ps
	n := len(old)
	item := old[n-1]
	item.index = -1 // 为了安全起见，将索引设置为-1
	*ps = old[0 : n-1]
	return item
}

// PriorityQueue is a priority queue.
type PriorityQueue struct {
	ps prioritySlice
}

// NewPriorityQueue creates a new priority queue.
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{}
}

// Len returns the length of the priority queue.
func (pq *PriorityQueue) Len() int {
	return pq.ps.Len()
}

// Push adds an entry to the priority queue.
func (pq *PriorityQueue) Push(item Item) {
	heap.Push(&pq.ps, entry{entry: item})
}

// Pop removes the highest priority entry from the priority queue.
func (pq *PriorityQueue) Pop() Item {
	return heap.Pop(&pq.ps).(entry).entry
}

// All returns all entries in the priority queue not ensure the priority.
func (pq *PriorityQueue) All() []Item {
	items := make([]Item, 0, pq.Len())
	for i := 0; i < pq.Len(); i++ {
		items = append(items, pq.ps[i].entry.(Item))
	}
	return items
}
