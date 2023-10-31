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
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

var (
	client  *txnkv.Client
	pdAddr  = flag.String("pd", "127.0.0.1:2379", "pd address")
	txnSize = flag.Int64("txn-size", 1024, "txn size")
	bits    int
)

const (
	PREWRITE_CONC = int64(100)
	COMMIT_CONC   = int64(100)
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

func MustNil(err error) {
	if err != nil {
		fmt.Println("Must nil assertion failed")
		panic(err)
	}
}

func generateKV(i int64) ([]byte, []byte) {
	j := i
	nonZeroBits := 0
	for j > 0 {
		nonZeroBits++
		j /= 10
	}
	var sb strings.Builder
	for k := nonZeroBits + 1; k <= bits; k++ {
		sb.WriteByte('0')
	}
	padding := sb.String()
	return []byte(fmt.Sprintf("key%s%v", padding, i)), []byte(fmt.Sprintf("val%s%v", padding, i))
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	bits = len(fmt.Sprintf("%d", *txnSize))
	initStore()
	initContext, _ := context.WithTimeout(context.Background(), 30*time.Second)

	MustNil(client.UnsafeDestroyRange(initContext, []byte{0}, []byte{255}))

	txn, err := client.Begin()
	MustNil(err)
	primary, _ := generateKV(0)
	min, max := primary, primary
	size := *txnSize
	// prewrite
	var wg sync.WaitGroup
	splitKeys := make([][]byte, 0, PREWRITE_CONC)
	splitReady := make(chan struct{})
	for i := int64(0); i < PREWRITE_CONC; i++ {
		wg.Add(1)
		start := (size / PREWRITE_CONC) * i
		end := (size / PREWRITE_CONC) * (i + 1)
		if i == PREWRITE_CONC-1 {
			end = size
		}
		startKey, _ := generateKV(start)
		endKey, _ := generateKV(end)
		if bytes.Compare(startKey, min) < 0 {
			min = startKey
		}
		if bytes.Compare(endKey, max) > 0 {
			max = endKey
		}
		splitKeys = append(splitKeys, startKey)
		go func(start, end int64) {
			defer wg.Done()
			<-splitReady
			var mutations transaction.PlainMutations
			for j := start; j < end; j++ {
				mutations = transaction.NewPlainMutations(1000)
				key, val := generateKV(j)
				mutations.AppendMutation(transaction.PlainMutation{
					KeyOp: kvrpcpb.Op_Put,
					Key:   key,
					Value: val,
					Flags: transaction.MutationFlagIsAssertNotExists,
				})
				if mutations.Len() >= 1000 {
					committer, err := transaction.NewTwoPhaseCommitterWithPK(txn, 1, primary, &mutations)
					MustNil(err)
					prewriteCtx, _ := context.WithTimeout(context.Background(), 30*time.Second)
					err = committer.DoActionOnMutations(prewriteCtx, 0, &mutations, nil)
					MustNil(err)
					mutations = transaction.NewPlainMutations(1000)
				}
			}
			if mutations.Len() > 0 {
				committer, err := transaction.NewTwoPhaseCommitterWithPK(txn, 1, primary, &mutations)
				MustNil(err)
				prewriteCtx, _ := context.WithTimeout(context.Background(), 30*time.Second)
				err = committer.DoActionOnMutations(prewriteCtx, 0, &mutations, nil)
				MustNil(err)
			}
		}(start, end)
	}
	if size/PREWRITE_CONC > 1000 {
		_, err = client.SplitRegions(initContext, splitKeys, true, nil)
		MustNil(err)
	}
	close(splitReady)
	fmt.Println("============ PREWRITE START ============")
	fmt.Printf("prewrite with start_ts: %d, primary: %s\n", txn.StartTS(), string(primary))
	wg.Wait()
	fmt.Println("============ PREWRITE DONE ============")

	commitTs, err := client.GetTimestamp(context.Background())
	MustNil(err)
	regionCh := make(chan interface{}, COMMIT_CONC)
	for i := int64(0); i < COMMIT_CONC; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for region := range regionCh {
				mutations := transaction.NewPlainMutations(0)
				mutations.AppendMutation(transaction.PlainMutation{
					KeyOp: kvrpcpb.Op_Put,
					Key:   []byte("key"),
					Value: []byte("val"),
					Flags: transaction.MutationFlagIsAssertNotExists,
				})
				committer, err := transaction.NewTwoPhaseCommitterWithPK(txn, 1, primary, &mutations)
				MustNil(err)
				committer.SetCommitTs(commitTs)
				commitCtx, _ := context.WithTimeout(context.Background(), 30*time.Second)
				err = committer.DoActionOnMutations(commitCtx, 1, &mutations, region)
				MustNil(err)
				mutations = transaction.NewPlainMutations(1000)
			}
		}()
	}

	fmt.Println("============ COMMIT START ============")
	fmt.Printf("commit with commit_ts: %d\n", commitTs)
	regionCache := client.GetRegionCache()
	for {
		bo := tikv.NewBackoffer(context.Background(), 1000)
		region, err := regionCache.LocateKey(bo, min)
		MustNil(err)
		regionCh <- region
		if bytes.Compare(region.EndKey, max) >= 0 {
			break
		}
	}
	close(regionCh)
	wg.Wait()
	fmt.Println("============ COMMIT DONE ============")

	{
		times := 0
	SCAN:
		times++
		// scan
		scanStart, _ := generateKV(0)
		fmt.Println("scan start", string(scanStart))
		ret, err := scan(scanStart, 10)
		MustNil(err)
		for _, kv := range ret {
			fmt.Println(kv)
		}
		if len(ret) == 0 {
			goto SCAN
		}
		fmt.Println("scan done", times)
	}
}
