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
	"github.com/tikv/client-go/v2/kv"
)

type artNodeKind uint16

const (
	typeARTInvalid artNodeKind = 0
	//nolint:unused
	typeARTNode4   artNodeKind = 1
	typeARTNode16  artNodeKind = 2
	typeARTNode48  artNodeKind = 3
	typeARTNode256 artNodeKind = 4
	typeARTLeaf    artNodeKind = 5
)

var nullArtNode = artNode{kind: typeARTInvalid, addr: arena.NullAddr}

type artKey []byte

type artNode struct {
	kind artNodeKind
	addr arena.MemdbArenaAddr
}

type artLeaf struct {
	vAddr arena.MemdbArenaAddr
	klen  uint16
	flags uint16
}

// GetKey gets the full key of the leaf
func (l *artLeaf) GetKey() []byte {
	panic("unimplemented")
}

// GetKeyFlags gets the flags of the leaf
func (l *artLeaf) GetKeyFlags() kv.KeyFlags {
	panic("unimplemented")
}
