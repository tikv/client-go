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
	"flag"
	"fmt"
	"os"

	"github.com/tikv/client-go/v2/txnkv"
)

var (
	client *txnkv.Client
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
)

// Init initializes information.
func initStore() {
	var err error
	client, err = txnkv.NewClient([]string{*pdAddr})
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

	// it will try to use 1 phase commit.
	txn.SetEnable1PC(true)
	k1 := []byte("k1")
	k2 := []byte("k2")

	err = txn.Set(k1, k1)
	errMustBeNil(err)
	err = txn.Set(k2, k2)
	errMustBeNil(err)
	err = txn.Commit(context.Background())
	errMustBeNil(err)
	fmt.Println("commit success!")
}
