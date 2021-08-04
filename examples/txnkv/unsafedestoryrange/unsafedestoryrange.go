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

	// UnsafeDestroyRange Cleans up all keys in a range[startKey,endKey) and quickly free the disk space.
	// The range might span over multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
	// on RocksDB, bypassing the Raft layer. User must promise that, after calling `UnsafeDestroyRange`,
	// the range will never be accessed any more.
	err = client.UnsafeDestroyRange(ctx, []byte("b"), []byte("c0"))
	panicWhenErrNotNil(err)
	fmt.Println("UnsafeDestroyRange [b,c0) success.")

	//`UnsafeDestroyRange` is allowed to be called multiple times on an single range.
	err = client.UnsafeDestroyRange(ctx, []byte("b"), []byte("c0"))
	panicWhenErrNotNil(err)
	fmt.Println("UnsafeDestroyRange [b,c0) again success.")

	err = client.UnsafeDestroyRange(ctx, []byte("d0"), []byte("d0"))
	panicWhenErrNotNil(err)
	fmt.Println("UnsafeDestroyRange [d0,d0) success.")

	err = client.UnsafeDestroyRange(ctx, []byte("a"), []byte("e"))
	panicWhenErrNotNil(err)
	fmt.Println("UnsafeDestroyRange [a,e) success.")

}
