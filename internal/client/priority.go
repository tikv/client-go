package client

import "container/heap"

// Item 表示优先级队列中的元素
type Item struct {
	value    *batchCommandsEntry // 元素的值
	priority int                 // 元素的优先级
	index    int                 // 元素在堆中的索引
}

// PriorityQueue 是一个优先级队列
type PriorityQueue []*Item

// Len 返回队列的长度
func (pq PriorityQueue) Len() int {
	return len(pq)
}

// Less 比较两个元素的优先级
func (pq PriorityQueue) Less(i, j int) bool {
	// 优先级越高的元素排在前面
	return pq[i].priority > pq[j].priority
}

// Swap 交换两个元素的位置
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push 向队列中添加一个元素
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	item.index = len(*pq)
	*pq = append(*pq, item)
}

// Pop 从队列中取出优先级最高的元素
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // 为了安全起见，将索引设置为-1
	*pq = old[0 : n-1]
	return item
}

// 更新元素的优先级和值
func (pq *PriorityQueue) update(item *Item, value *batchCommandsEntry, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
