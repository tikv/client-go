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

package unionstore

import "unsafe"

// fixedSizeArena is a fixed size arena allocator.
// because the size of each type of node is fixed, the discarded nodes can be reused.
// reusing blocks reduces the memory pieces.
type nodeArena struct {
	memdbArena
	freeNode4  []memdbArenaAddr
	freeNode16 []memdbArenaAddr
	freeNode48 []memdbArenaAddr
}

type artAllocator struct {
	vlogAllocator memdbVlog[*artLeaf, *ART]
	nodeAllocator nodeArena
}

func (f *artAllocator) allocNode4() (memdbArenaAddr, *artNode4) {
	var (
		addr memdbArenaAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode4) > 0 {
		addr = f.nodeAllocator.freeNode4[len(f.nodeAllocator.freeNode4)-1]
		f.nodeAllocator.freeNode4 = f.nodeAllocator.freeNode4[:len(f.nodeAllocator.freeNode4)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(artNode4size, true)
	}
	n4 := (*artNode4)(unsafe.Pointer(&data[0]))
	n4.init()
	return addr, n4
}

func (f *artAllocator) freeNode4(addr memdbArenaAddr) {
	f.nodeAllocator.freeNode4 = append(f.nodeAllocator.freeNode4, addr)
}

func (f *artAllocator) getNode4(addr memdbArenaAddr) *artNode4 {
	data := f.nodeAllocator.getData(addr)
	return (*artNode4)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode16() (memdbArenaAddr, *artNode16) {
	var (
		addr memdbArenaAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode16) > 0 {
		addr = f.nodeAllocator.freeNode16[len(f.nodeAllocator.freeNode16)-1]
		f.nodeAllocator.freeNode16 = f.nodeAllocator.freeNode16[:len(f.nodeAllocator.freeNode16)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(artNode16size, true)
	}
	n16 := (*artNode16)(unsafe.Pointer(&data[0]))
	n16.init()
	return addr, n16
}

func (f *artAllocator) freeNode16(addr memdbArenaAddr) {
	f.nodeAllocator.freeNode16 = append(f.nodeAllocator.freeNode16, addr)
}

func (f *artAllocator) getNode16(addr memdbArenaAddr) *artNode16 {
	data := f.nodeAllocator.getData(addr)
	return (*artNode16)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode48() (memdbArenaAddr, *artNode48) {
	var (
		addr memdbArenaAddr
		data []byte
	)
	if len(f.nodeAllocator.freeNode48) > 0 {
		addr = f.nodeAllocator.freeNode48[len(f.nodeAllocator.freeNode48)-1]
		f.nodeAllocator.freeNode48 = f.nodeAllocator.freeNode48[:len(f.nodeAllocator.freeNode48)-1]
		data = f.nodeAllocator.getData(addr)
	} else {
		addr, data = f.nodeAllocator.alloc(artNode48size, true)
	}
	n48 := (*artNode48)(unsafe.Pointer(&data[0]))
	n48.init()
	return addr, n48
}

func (f *artAllocator) freeNode48(addr memdbArenaAddr) {
	f.nodeAllocator.freeNode48 = append(f.nodeAllocator.freeNode48, addr)
}

func (f *artAllocator) getNode48(addr memdbArenaAddr) *artNode48 {
	data := f.nodeAllocator.getData(addr)
	return (*artNode48)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocNode256() (memdbArenaAddr, *artNode256) {
	var (
		addr memdbArenaAddr
		data []byte
	)
	addr, data = f.nodeAllocator.alloc(artNode256size, true)
	n256 := (*artNode256)(unsafe.Pointer(&data[0]))
	n256.init()
	return addr, n256
}

func (f *artAllocator) getNode256(addr memdbArenaAddr) *artNode256 {
	data := f.nodeAllocator.getData(addr)
	return (*artNode256)(unsafe.Pointer(&data[0]))
}

func (f *artAllocator) allocLeaf(key []byte) (memdbArenaAddr, *artLeaf) {
	size := artLeafSize + len(key)
	addr, data := f.nodeAllocator.alloc(size, true)
	lf := (*artLeaf)(unsafe.Pointer(&data[0]))
	lf.klen = uint16(len(key))
	lf.flags = 0
	lf.vAddr = nullAddr
	copy(data[artLeafSize:], key)
	return addr, lf
}

func (f *artAllocator) getLeaf(addr memdbArenaAddr) *artLeaf {
	if addr.isNull() {
		return nil
	}
	data := f.nodeAllocator.getData(addr)
	return (*artLeaf)(unsafe.Pointer(&data[0]))
}
