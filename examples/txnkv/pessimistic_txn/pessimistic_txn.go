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

	"github.com/tikv/client-go/v2/kv"
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

func begin_pessimistic_txn() (txn *txnkv.KVTxn) {
	txn, err := client.Begin()
	if err != nil {
		panic(err)
	}
	txn.SetPessimistic(true)
	return txn
}

func exampleForPessimisticTXN() {
	// k1 is the primary lock of txn1
	k1 := []byte("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock
	k2 := []byte("k2")

	txn1 := begin_pessimistic_txn()

	//txn1: lock the primary key
	err := txn1.LockKeysWithWaitTime(context.Background(), kv.LockAlwaysWait, k1)
	if err != nil {
		panic(err)
	}
	fmt.Println("txn1: lock k1 success.")
	// txn1:lock the secondary key
	err = txn1.LockKeysWithWaitTime(context.Background(), kv.LockAlwaysWait, k2)
	if err != nil {
		panic(err)
	}
	fmt.Println("txn1: lock k2 success.")

	// begin txn2
	txn2 := begin_pessimistic_txn()

	// txn2: lock k2 no wait
	err = txn2.LockKeysWithWaitTime(context.Background(), kv.LockNoWait, k2)
	// cannot acquire lock immediately thus error:ErrLockAcquireFailAndNoWaitSet
	fmt.Println("txn2: acquire lock for k2 (while txn1 has this lock) should be failed with error: ", err)

	// txn2:lock k2 for wait limited time (200ms),less than k2's lock TTL by txn1,should failed with timeout.
	err = txn2.LockKeysWithWaitTime(context.Background(), 200, k2)
	// txn2: cannot acquire lock k2 in time should failed with timeout.
	fmt.Println("txn2: acquire lock for k1(while txn1 has this lock) should be failed with error:  ", err)

	// commit txn1 should be success.
	txn1.Set(k1, k1)
	err = txn1.Commit(context.Background())
	if err != nil {
		panic(err)
	} else {
		fmt.Println("tnx1: commit txn1 success!")
	}

	// txn2: lock k2 in txn2 with new forUpdateTS should success.
	err = txn2.LockKeysWithWaitTime(context.Background(), kv.LockNoWait, k2)
	if err != nil {
		// cannot acquire lock , should success.
		fmt.Println("txn2: acquire lock for k2 should be success while meet err:", err)
	} else {
		fmt.Println("txn2: acquire lock for k2 success!")
	}

	// txn2: do some write.
	txn2.Set(k1, k1)
	txn2.Set(k2, k2)
	txn2.Delete(k1)
	txn2.Delete(k2)
	// commit txn2 should success.
	err = txn2.Commit(context.Background())
	if err != nil {
		fmt.Println("txn2: commit should success while meet err ", err)
	} else {
		fmt.Println("txn2: commit success.")
	}
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	initStore()
	exampleForPessimisticTXN()
}
