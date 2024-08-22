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

//nolint:unused
package art

import (
	"github.com/tikv/client-go/v2/internal/unionstore/arena"
)

// fixedSizeArena is a fixed size arena allocator.
// because the size of each type of node is fixed, the discarded nodes can be reused.
// reusing blocks reduces the memory pieces.
type nodeArena struct {
	arena.MemdbArena
	freeNode4  []arena.MemdbArenaAddr
	freeNode16 []arena.MemdbArenaAddr
	freeNode48 []arena.MemdbArenaAddr
}

type artAllocator struct {
	vlogAllocator arena.MemdbVlog[*artLeaf, *ART]
	nodeAllocator nodeArena
}
