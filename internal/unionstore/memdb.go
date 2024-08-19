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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb.go
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

package unionstore

import (
	"math"
)

const unlimitedSize = math.MaxUint64

var tombstone = []byte{}

// IsTombstone returns whether the value is a tombstone.
func IsTombstone(val []byte) bool { return len(val) == 0 }

// MemKeyHandle represents a pointer for key in MemBuffer.
type MemKeyHandle struct {
	// Opaque user data
	UserData uint16
	idx      uint16
	off      uint32
}

func (h MemKeyHandle) toAddr() memdbArenaAddr {
	return memdbArenaAddr{idx: uint32(h.idx), off: h.off}
}

type MemDB = RBT

var newMemDB = newRBT

var testMode = false
