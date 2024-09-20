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

package arena

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type dummyMemDB struct{}

func (m *dummyMemDB) RevertVAddr(hdr *MemdbVlogHdr) {}
func (m *dummyMemDB) InspectNode(addr MemdbArenaAddr) (KeyFlagsGetter, MemdbArenaAddr) {
	return nil, NullAddr
}

func TestBigValue(t *testing.T) {
	assert := assert.New(t)

	var vlog MemdbVlog[KeyFlagsGetter, *dummyMemDB]
	vlog.AppendValue(MemdbArenaAddr{0, 0}, NullAddr, make([]byte, 80<<20))
	assert.Equal(vlog.blockSize, maxBlockSize)
	assert.Equal(len(vlog.blocks), 1)

	cp := vlog.Checkpoint()
	vlog.AppendValue(MemdbArenaAddr{0, 1}, NullAddr, make([]byte, 127<<20))
	vlog.RevertToCheckpoint(&dummyMemDB{}, &cp)

	assert.Equal(vlog.blockSize, maxBlockSize)
	assert.Equal(len(vlog.blocks), 2)
	assert.PanicsWithValue("alloc size is larger than max block size", func() {
		vlog.AppendValue(MemdbArenaAddr{0, 2}, NullAddr, make([]byte, maxBlockSize+1))
	})
}

func TestValueLargeThanBlock(t *testing.T) {
	assert := assert.New(t)
	var vlog MemdbVlog[KeyFlagsGetter, *dummyMemDB]
	vlog.AppendValue(MemdbArenaAddr{0, 0}, NullAddr, make([]byte, 1))
	vlog.AppendValue(MemdbArenaAddr{0, 1}, NullAddr, make([]byte, 4096))
	assert.Equal(len(vlog.blocks), 2)
	vAddr := vlog.AppendValue(MemdbArenaAddr{0, 2}, NullAddr, make([]byte, 3000))
	assert.Equal(len(vlog.blocks), 2)
	val := vlog.GetValue(vAddr)
	assert.Equal(len(val), 3000)
}
