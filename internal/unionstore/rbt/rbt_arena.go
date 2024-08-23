// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb_arena.go
//

// Copyright 2020 PingCAP, Inc.
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

package rbt

import (
	"unsafe"

	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

type nodeAllocator struct {
	arena.MemdbArena

	// Dummy node, so that we can make X.left.up = X.
	// We then use this instead of NULL to mean the top or bottom
	// end of the rb tree. It is a black node.
	nullNode memdbNode
}

func (a *nodeAllocator) init() {
	a.nullNode = memdbNode{
		up:    arena.NullAddr,
		left:  arena.NullAddr,
		right: arena.NullAddr,
		vptr:  arena.NullAddr,
	}
}

func (a *nodeAllocator) getNode(addr arena.MemdbArenaAddr) *memdbNode {
	if addr.IsNull() {
		return &a.nullNode
	}
	data := a.GetData(addr)
	return (*memdbNode)(unsafe.Pointer(&data[0]))
}

const memdbNodeSize = int(unsafe.Sizeof(memdbNode{}))

func (a *nodeAllocator) allocNode(key []byte) (arena.MemdbArenaAddr, *memdbNode) {
	nodeSize := memdbNodeSize + len(key)
	prevBlocks := a.Blocks()
	addr, mem := a.Alloc(nodeSize, true)
	n := (*memdbNode)(unsafe.Pointer(&mem[0]))
	n.vptr = arena.NullAddr
	n.klen = uint16(len(key))
	copy(n.getKey(), key)
	if prevBlocks != a.Blocks() {
		a.OnMemChange()
	}
	return addr, n
}

func (a *nodeAllocator) freeNode(addr arena.MemdbArenaAddr) {
	if testMode {
		// Make it easier for debug.
		n := a.getNode(addr)
		n.left = arena.BadAddr
		n.right = arena.BadAddr
		n.up = arena.BadAddr
		n.vptr = arena.BadAddr
		return
	}
	// TODO: reuse freed nodes. Need to fix lastTraversedNode when implementing this.
}

func (a *nodeAllocator) reset() {
	a.MemdbArena.Reset()
	a.init()
}
