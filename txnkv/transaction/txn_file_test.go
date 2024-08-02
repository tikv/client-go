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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/internal/locate"
)

func TestTxnFileChunkBatch(t *testing.T) {
	assert := assert.New(t)
	chunkSlice := txnChunkSlice{
		chunkIDs: []uint64{1, 2, 3},
		chunkRanges: []txnChunkRange{
			{
				smallest: []byte("k01"),
				biggest:  []byte("k01"),
			},
			{
				smallest: []byte("k02"),
				biggest:  []byte("k04"),
			},
			{
				smallest: []byte("k05"),
				biggest:  []byte("k08"),
			},
		},
	}

	cases := []struct {
		startKey string
		endKey   string
		expected []string
	}{
		{
			"", "", []string{"k01", "k02", "k05"},
		},
		{
			"k01", "k05", []string{"k01", "k02"},
		},
		{
			"k010", "k05", []string{"k02"},
		},
		{
			"k03", "k05", []string{},
		},
		{
			"k03", "", []string{"k05"},
		},
	}

	for _, c := range cases {
		batch := chunkBatch{
			txnChunkSlice: chunkSlice,
			region: &locate.KeyLocation{
				StartKey: []byte(c.startKey),
				EndKey:   []byte(c.endKey),
			},
		}
		keys := batch.getSampleKeys()
		strKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			strKeys = append(strKeys, string(key))
		}
		assert.Equal(c.expected, strKeys)
	}
}
