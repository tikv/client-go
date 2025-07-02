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
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/util"
)

func TestChunkSliceSortAndDedup(t *testing.T) {
	assert := assert.New(t)

	genRndChunkIDs := func() []uint64 {
		len := rand.Intn(10)
		ids := make([]uint64, 0, len)
		for i := 0; i < len; i++ {
			ids = append(ids, uint64(rand.Intn(len+len/2)))
		}
		return ids
	}

	for i := 0; i < 100; i++ {
		ids := genRndChunkIDs()
		t.Logf("ids: %v\n", ids)

		expected := make([]uint64, len(ids))
		copy(expected, ids)
		slices.Sort(expected)
		expected = slices.Compact(expected)

		chunkSlice := txnChunkSlice{
			chunkIDs:    make([]uint64, 0, len(ids)),
			chunkRanges: make([]txnChunkRange, 0, len(ids)),
		}
		for _, id := range ids {
			chunkSlice.chunkIDs = append(chunkSlice.chunkIDs, id)
			chunkSlice.chunkRanges = append(chunkSlice.chunkRanges, txnChunkRange{smallest: []byte(fmt.Sprintf("k%04d", id)), biggest: []byte{}})
		}
		chunkSlice.sortAndDedup()
		assert.Equal(expected, chunkSlice.chunkIDs)
		for i, id := range expected {
			assert.Equal(fmt.Sprintf("k%04d", id), string(chunkSlice.chunkRanges[i].smallest))
		}
	}
}

func TestIsRequestSourceUseTxnFile(t *testing.T) {
	assert := assert.New(t)

	cases := []struct {
		reqSource *util.RequestSource
		whitelist []string
		expected  bool
	}{
		{
			reqSource: &util.RequestSource{RequestSourceInternal: false},
			whitelist: []string{},
			expected:  true,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{"ddl_modify_column"},
			expected:  true,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{"ddl_alter_partition", "ddl_modify_column"},
			expected:  true,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{},
			expected:  false,
		},
		{
			reqSource: &util.RequestSource{RequestSourceType: "ddl_modify_column", RequestSourceInternal: true},
			whitelist: []string{"ddl_alter_partition"},
			expected:  false,
		},
	}

	for _, c := range cases {
		conf := &config.Config{
			TiKVClient: config.TiKVClient{
				TxnFileRequestSourceWhitelist: c.whitelist,
			},
		}
		result := IsRequestSourceUseTxnFile(c.reqSource, conf)
		assert.Equal(c.expected, result, "Expected %v for request source %v with whitelist %v", c.expected, c.reqSource.RequestSourceType, c.whitelist)
	}
}
