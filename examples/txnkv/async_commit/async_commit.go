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
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/util"
)

var (
	client *tikv.KVStore
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
)

// Init initializes information.
func initStore() {
	var err error
	client, err = tikv.NewTxnClient([]string{*pdAddr})
	if err != nil {
		panic(err)
	}
}

func errMustBeNil(err error) {
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

	txn, err := client.Begin()
	if err != nil {
		panic(err)
	}

	// it will try to use async commit.
	txn.SetEnableAsyncCommit(true)

	k1 := []byte("k1")
	k2 := []byte("k2")

	// Async commit wouldn't work if the number or the total size of updated keys exceed the limit:
	// - The number of keys to update should be less than 256(configable)
	// - The size of keys should less than 4KB(configable)
	err = txn.Set(k1, k1)
	errMustBeNil(err)
	err = txn.Set(k2, k2)
	errMustBeNil(err)
	ctx := context.Background()
	// async commit would not work if sessionID is 0(default value).
	util.SetSessionID(ctx, uint64(10001))
	err = txn.Commit(ctx)
	errMustBeNil(err)
	fmt.Println("Example for async commit: commit success!")

}
