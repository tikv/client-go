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
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/metrics"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
	mode   = flag.String("mode", "raw", "raw / txn")

	pushAddr     = flag.String("push", "127.0.0.1:9090", "pushGateway address")
	pushInterval = flag.Duration("interval", 15*time.Second, "push metrics interval")
	pushJob      = flag.String("job", "bench", "job name")
	pushInstance = flag.String("instance", "bench1", "instance name")

	keyLen   = flag.Int("klen", 10, "length of key")
	valueLen = flag.Int("vlen", 20, "length of value")
	keyRange = flag.Int("range", 100000, "size of the key set")

	rawGetP         = flag.Int("raw-get-p", 1, "raw get concurrency")
	rawBatchGetP    = flag.Int("raw-batch-get-p", 0, "raw batch get concurrency")
	rawBatchGetN    = flag.Int("raw-batch-get-n", 10, "raw batch get batch size")
	rawPutP         = flag.Int("raw-put-p", 1, "raw put concurrency")
	rawBatchPutP    = flag.Int("raw-batch-put-p", 0, "raw batch put concurrency")
	rawBatchPutN    = flag.Int("raw-batch-put-n", 10, "raw batch put batch size")
	rawDeleteP      = flag.Int("raw-delete-p", 1, "raw delete concurrency")
	rawBatchDeleteP = flag.Int("raw-batch-delete-p", 0, "raw batch delete concurrency")
	rawBatchDeleteN = flag.Int("raw-batch-delete-n", 10, "raw batch delete batch size")
	rawScanP        = flag.Int("raw-scan-p", 1, "raw scan concurrency")
	rawScanL        = flag.Int("raw-scan-l", 10, "raw scan limit")

	txn1P       = flag.Int("txn1-p", 1, "txn1 concurrency")
	txn1GetN    = flag.Int("txn1-get-n", 10, "txn1 get command count")
	txn1PutN    = flag.Int("txn1-put-n", 0, "txn1 put command count")
	txn1DeleteN = flag.Int("txn1-delete-n", 0, "txn1 delete command count")
	txn1ScanN   = flag.Int("txn1-scan-n", 1, "txn1 scan command count")
	txn1ScanL   = flag.Int("txn1-scan-l", 10, "txn1 scan limit")

	txn2P       = flag.Int("txn2-p", 2, "txn2 concurrency")
	txn2GetN    = flag.Int("txn2-get-n", 0, "txn2 get command count")
	txn2PutN    = flag.Int("txn2-put-n", 10, "txn2 put command count")
	txn2DeleteN = flag.Int("txn2-delete-n", 1, "txn2 delete command count")
	txn2ScanN   = flag.Int("txn2-scan-n", 0, "txn2 scan command count")
	txn2ScanL   = flag.Int("txn2-scan-l", 10, "txn2 scan limit")

	txn3P       = flag.Int("txn3-p", 0, "txn3 concurrency")
	txn3GetN    = flag.Int("txn3-get-n", 1, "txn3 get command count")
	txn3PutN    = flag.Int("txn3-put-n", 1, "txn3 put command count")
	txn3DeleteN = flag.Int("txn3-delete-n", 1, "txn3 delete command count")
	txn3ScanN   = flag.Int("txn3-scan-n", 1, "txn3 scan command count")
	txn3ScanL   = flag.Int("txn3-scan-l", 10, "txn3 scan limit")
)

func newConfig() config.Config {
	return config.Default()
}

var (
	rawCli *rawkv.Client
	txnCli *txnkv.Client
)

func k() []byte {
	var t string
	if *mode == "raw" {
		t = fmt.Sprintf("R%%%dd", *keyLen-1)
	} else {
		t = fmt.Sprintf("T%%%dd", *keyLen-1)
	}
	return []byte(fmt.Sprintf(t, rand.Intn(*keyRange)))
}

func v() []byte {
	return bytes.Repeat([]byte{0}, *valueLen)
}

func n(x int, f func() []byte) [][]byte {
	res := make([][]byte, x)
	for i := range res {
		res[i] = f()
	}
	return res
}

func nk(x int) [][]byte { return n(x, k) }
func nv(x int) [][]byte { return n(x, v) }

func P(p int, f func()) {
	for i := 0; i < p; i++ {
		go func() {
			for {
				f()
			}
		}()
	}
}

func benchRaw() {
	var err error
	rawCli, err = rawkv.NewClient(context.TODO(), strings.Split(*pdAddr, ","), newConfig())
	if err != nil {
		log.Fatal(err)
	}

	P(*rawGetP, func() { rawCli.Get(context.TODO(), k()) })
	P(*rawBatchGetP, func() { rawCli.BatchGet(context.TODO(), nk(*rawBatchGetN)) })
	P(*rawPutP, func() { rawCli.Put(context.TODO(), k(), v()) })
	P(*rawBatchPutP, func() { rawCli.BatchPut(context.TODO(), nk(*rawBatchPutN), nv(*rawBatchPutN)) })
	P(*rawDeleteP, func() { rawCli.Delete(context.TODO(), k()) })
	P(*rawBatchDeleteP, func() { rawCli.BatchDelete(context.TODO(), nk(*rawBatchDeleteN)) })
	P(*rawScanP, func() { rawCli.Scan(context.TODO(), k(), nil, *rawScanL) })
}

func benchTxn() {
	var err error
	txnCli, err = txnkv.NewClient(context.TODO(), strings.Split(*pdAddr, ","), newConfig())
	if err != nil {
		log.Fatal(err)
	}

	t := func(getN, putN, delN, scanN, scanL int) func() {
		return func() {
			tx, err := txnCli.Begin(context.TODO())
			if err != nil {
				return
			}
			for i := 0; i < getN; i++ {
				tx.Get(context.TODO(), k())
			}
			for i := 0; i < putN; i++ {
				tx.Set(k(), v())
			}
			for i := 0; i < delN; i++ {
				tx.Delete(k())
			}
			for i := 0; i < scanN; i++ {
				it, err := tx.Iter(context.TODO(), k(), nil)
				if err != nil {
					continue
				}
				for j := 0; j < scanL && it.Valid(); j++ {
					it.Next(context.TODO())
				}
				it.Close()
			}
			tx.Commit(context.TODO())
		}
	}

	P(*txn1P, t(*txn1GetN, *txn1PutN, *txn1DeleteN, *txn1ScanN, *txn1ScanL))
	P(*txn2P, t(*txn2GetN, *txn2PutN, *txn2DeleteN, *txn2ScanN, *txn2ScanL))
	P(*txn3P, t(*txn3GetN, *txn3PutN, *txn3DeleteN, *txn3ScanN, *txn3ScanL))
}

func main() {
	flag.Parse()

	go metrics.PushMetrics(context.TODO(), *pushAddr, *pushInterval, *pushJob, *pushInstance)

	switch *mode {
	case "raw":
		benchRaw()
	case "txn":
		benchTxn()
	default:
		log.Fatal("invalid mode:", *mode)
	}

	for {
		fmt.Print(".")
		time.Sleep(time.Second)
	}
}
