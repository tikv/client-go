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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"

	"github.com/tikv/client-go/v2/tikv"
)

var (
	client *tikv.KVStore
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
)

// Init initializes information.
func initStore() {
	var err error
	client, err = tikv.NewTxnClient([]string{*pdAddr})
	panicWhenErrNotNil(err)
}

func panicWhenErrNotNil(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	initStore()

	data := map[string][]byte{}
	ctx := context.Background()
	// Generate a sequence of keys and random values
	for _, i := range []byte("abcd") {
		txn, err := client.Begin()
		panicWhenErrNotNil(err)
		for j := byte('0'); j <= byte('9'); j++ {
			key := []byte{i, j}
			value := []byte{byte(rand.Intn(256)), byte(rand.Intn(256))}
			data[string(key)] = value
			err = txn.Set(key, value)
			panicWhenErrNotNil(err)
		}
		err = txn.Commit(ctx)
		panicWhenErrNotNil(err)
	}
	fmt.Println("commit data success!")
	// check data
	txn, err := client.Begin()
	panicWhenErrNotNil(err)
	it, err := txn.Iter([]byte("a"), nil)
	panicWhenErrNotNil(err)
	for it.Valid() {
		key := string(it.Key())
		if evalue, ok := data[key]; ok {
			if !bytes.Equal(evalue, it.Value()) {
				fmt.Printf("Unexpect value, is the value changed by other transaction? key:%+v, expect value:%+v, real value:%+v\n", key, evalue, it.Value())
			}
		}
		err = it.Next()
		panicWhenErrNotNil(err)
	}
	// delete range
	// Be careful while using this API. This API doesn't keep recent MVCC versions, but will delete all versions of all keys
	// in the range immediately. Also notice that frequent invocation to this API may cause performance problems to TiKV.
	completedRegions, err := client.DeleteRange(ctx, []byte("b"), []byte("c0"), 1)
	panicWhenErrNotNil(err)
	fmt.Println("Delete Range [b,c0) success, the number of affacted regions: ", completedRegions)

	completedRegions, err = client.DeleteRange(ctx, []byte("d0"), []byte("d0"), 1)
	panicWhenErrNotNil(err)
	fmt.Println("Delete Range [d0,d0) success, the number of affacted regions: ", completedRegions)

	completedRegions, err = client.DeleteRange(ctx, []byte("a"), []byte("e"), 1)
	panicWhenErrNotNil(err)
	fmt.Println("Delete Range [a,e) success, the number of affacted regions: ", completedRegions)

}
