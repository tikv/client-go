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
			return nil
		}
		return []byte(fmt.Sprintf("%04d", i))
	}

	muts := NewPlainMutations(10)
	for i := 10; i < 20; i += 2 {
		key := iToKey(i)
		var op kvrpcpb.Op
		if i%4 == 0 {
			op = kvrpcpb.Op_CheckNotExists
		} else {
			op = kvrpcpb.Op_Put
		}
		muts.Push(op, key, key, false, false, false, false)
	}

	type Case struct {
		start    int
		end      int
		expectd  bool
		firstKey int
	}
	cases := []Case{
		{-1, -1, true, 10},
		{-1, 5, false, -1},
		{0, 10, false, -1},
		{0, 11, true, 10},
		{0, 30, true, 10},
		{0, -1, true, 10},
		{10, 20, true, 10},
		{15, 16, false, -1},
		{15, 17, true, -1},
		{15, -1, true, 18},
		{20, 30, false, -1},
		{21, 30, false, -1},
		{21, -1, false, -1},
	}

	for _, c := range cases {
		firstKey, got := MutationsHasDataInRange(&muts, iToKey(c.start), iToKey(c.end))
		assert.Equal(c.expectd, got)
		if got {
			assert.Equal(iToKey(c.firstKey), firstKey)
		}
	}
}
