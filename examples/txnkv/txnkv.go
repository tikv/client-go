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

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

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

// key1 val1 key2 val2 ...
func puts(args ...[]byte) error {
	tx, err := client.Begin()
	if err != nil {
		return err
	}

	for i := 0; i < len(args); i += 2 {
		key, val := args[i], args[i+1]
		err := tx.Set(key, val)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func get(k []byte) (KV, error) {
	tx, err := client.Begin()
	if err != nil {
		return KV{}, err
	}
	v, err := tx.Get(context.TODO(), k)
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

func dels(keys ...[]byte) error {
	tx, err := client.Begin()
	if err != nil {
		return err
	}
	for _, key := range keys {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
	}
	return tx.Commit(context.Background())
}

func scan(keyPrefix []byte, limit int) ([]KV, error) {
	tx, err := client.Begin()
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(keyPrefix, nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ret []KV
	for it.Valid() && limit > 0 {
		ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
		limit--
		it.Next()
	}
	return ret, nil
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	initStore()

	// set
	err := puts([]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2"))
	if err != nil {
		panic(err)
	}

	// get
	kv, err := get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Println(kv)

	// scan
	ret, err := scan([]byte("key"), 10)
	if err != nil {
		panic(err)
	}
	for _, kv := range ret {
		fmt.Println(kv)
	}

	// delete
	err = dels([]byte("key1"), []byte("key2"))
	if err != nil {
		panic(err)
	}
}
