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

package httpproxy

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/tikv/client-go/proxy"
)

// NewHTTPProxyHandler creates an http.Handler that serves as a TiKV client proxy.
func NewHTTPProxyHandler() http.Handler {
	router := mux.NewRouter()
	rawkv := rawkvHandler{p: proxy.NewRaw()}
	router.HandleFunc("/rawkv/new", rawkv.handlerFunc(rawkv.New))
	router.HandleFunc("/rawkv/close", rawkv.handlerFunc(rawkv.Close))
	router.HandleFunc("/rawkv/get", rawkv.handlerFunc(rawkv.Get))
	router.HandleFunc("/rawkv/batch.get", rawkv.handlerFunc(rawkv.BatchGet))
	router.HandleFunc("/rawkv/put", rawkv.handlerFunc(rawkv.Put))
	router.HandleFunc("/rawkv/batch.put", rawkv.handlerFunc(rawkv.BatchPut))
	router.HandleFunc("/rawkv/delete", rawkv.handlerFunc(rawkv.Delete))
	router.HandleFunc("/rawkv/batch.delete", rawkv.handlerFunc(rawkv.BatchDelete))
	router.HandleFunc("/rawkv/delete.range", rawkv.handlerFunc(rawkv.DeleteRange))
	router.HandleFunc("/rawkv/scan", rawkv.handlerFunc(rawkv.Scan))

	txnkv := txnkvHandler{p: proxy.NewTxn()}
	router.HandleFunc("/txnkv/new", txnkv.handlerFunc(txnkv.New))
	router.HandleFunc("/txnkv/close", txnkv.handlerFunc(txnkv.Close))
	router.HandleFunc("/txnkv/begin", txnkv.handlerFunc(txnkv.Begin))
	router.HandleFunc("/txnkv/begin.with.ts", txnkv.handlerFunc(txnkv.BeginWithTS))
	router.HandleFunc("/txnkv/get.ts", txnkv.handlerFunc(txnkv.GetTS))
	router.HandleFunc("/txnkv/txn.get", txnkv.handlerFunc(txnkv.TxnGet))
	router.HandleFunc("/txnkv/txn.batch.get", txnkv.handlerFunc(txnkv.TxnBatchGet))
	router.HandleFunc("/txnkv/txn.set", txnkv.handlerFunc(txnkv.TxnSet))
	router.HandleFunc("/txnkv/txn.iter", txnkv.handlerFunc(txnkv.TxnIter))
	router.HandleFunc("/txnkv/txn.iter.reverse", txnkv.handlerFunc(txnkv.TxnIterReverse))
	router.HandleFunc("/txnkv/txn.readonly", txnkv.handlerFunc(txnkv.TxnIsReadOnly))
	router.HandleFunc("/txnkv/txn.delete", txnkv.handlerFunc(txnkv.TxnDelete))
	router.HandleFunc("/txnkv/txn.commit", txnkv.handlerFunc(txnkv.TxnCommit))
	router.HandleFunc("/txnkv/txn.rollback", txnkv.handlerFunc(txnkv.TxnRollback))
	router.HandleFunc("/txnkv/txn.lock.keys", txnkv.handlerFunc(txnkv.TxnLockKeys))
	router.HandleFunc("/txnkv/txn.valid", txnkv.handlerFunc(txnkv.TxnValid))
	router.HandleFunc("/txnkv/txn.len", txnkv.handlerFunc(txnkv.TxnLen))
	router.HandleFunc("/txnkv/txn.size", txnkv.handlerFunc(txnkv.TxnSize))
	router.HandleFunc("/txnkv/iter.valid", txnkv.handlerFunc(txnkv.IterValid))
	router.HandleFunc("/txnkv/iter.key", txnkv.handlerFunc(txnkv.IterKey))
	router.HandleFunc("/txnkv/iter.value", txnkv.handlerFunc(txnkv.IterValue))
	router.HandleFunc("/txnkv/iter.next", txnkv.handlerFunc(txnkv.IterNext))
	router.HandleFunc("/txnkv/iter.close", txnkv.handlerFunc(txnkv.IterClose))

	return router
}
