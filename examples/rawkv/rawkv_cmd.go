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

package main

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/rawkv"
)

const BatchSize = 1000
const ValueSize = 1024
const TotalKvs = 100000

func main() {
	cli, err := rawkv.NewClientWithOpts(context.TODO(), []string{"127.0.0.1:2381"}, rawkv.WithAPIVersion(kvrpcpb.APIVersion_V1))
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	fmt.Printf("cluster ID: %d\n", cli.ClusterID())

	isPut := false

	ctx := context.Background()
	keys := make([][]byte, BatchSize)
	vals := make([][]byte, BatchSize)
	for i := 0; i < TotalKvs; i += BatchSize {
		for j := 0; j < BatchSize; j++ {
			if keys[j] == nil {
				keys[j] = make([]byte, 0, 32)
			}
			keys[j] = append(keys[j][:0], fmt.Sprintf("key%020d", i+j)...)

			if vals[j] == nil {
				vals[j] = make([]byte, 0, ValueSize)
			}
			_, err := rand.Read(vals[j])
			if err != nil {
				panic(err)
			}
		}

		if isPut {
			if err := cli.BatchPut(ctx, keys, vals); err != nil {
				panic(err)
			}
		} else {
			if err := cli.BatchDelete(ctx, keys[:BatchSize/4]); err != nil {
				panic(err)
			}
		}
	}
}
