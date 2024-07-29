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

package transaction

import (
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestMutationsHasDataInRange(t *testing.T) {
	assert := assert.New(t)

	iToKey := func(i int) []byte {
		if i < 0 {
			return []byte{}
		}
		return []byte(fmt.Sprintf("%04d", i))
	}

	muts := NewPlainMutations(10)
	for i := 10; i < 20; i += 2 {
		key := iToKey(i)
		muts.Push(kvrpcpb.Op_Put, key, key, false, false, false, false)
	}

	type Case struct {
		start   int
		end     int
		expectd bool
	}
	cases := []Case{
		{-1, -1, true},
		{-1, 5, false},
		{0, 10, false},
		{0, 11, true},
		{0, 30, true},
		{0, -1, true},
		{10, 20, true},
		{15, 16, false},
		{15, 17, true},
		{15, -1, true},
		{20, 30, false},
		{21, 30, false},
		{21, -1, false},
	}

	for _, c := range cases {
		got := MutationsHasDataInRange(&muts, iToKey(c.start), iToKey(c.end))
		assert.Equal(c.expectd, got)
	}
}
