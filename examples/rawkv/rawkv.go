// Copyright 2019 PingCAP, Inc.
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
	"fmt"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

func main() {
	cli, err := rawkv.NewClient([]string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	fmt.Printf("cluster ID: %d\n", cli.ClusterID())

	key := []byte("Company")
	val := []byte("PingCAP")

	// put key into tikv
	err = cli.Put(key, val)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Successfully put %s:%s to tikv\n", key, val)

	// get key from tikv
	val, err = cli.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found val: %s for key: %s\n", val, key)

	// delete key from tikv
	err = cli.Delete(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("key: %s deleted\n", key)

	// get key again from tikv
	val, err = cli.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("found val: %s for key: %s\n", val, key)
}
