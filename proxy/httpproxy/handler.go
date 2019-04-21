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

	router.HandleFunc("/rawkv/client/new", rawkv.handlerFunc(rawkv.New))
	router.HandleFunc("/rawkv/client/{id}/close", rawkv.handlerFunc(rawkv.Close))
	router.HandleFunc("/rawkv/client/{id}/get", rawkv.handlerFunc(rawkv.Get))
	router.HandleFunc("/rawkv/client/{id}/batch-get", rawkv.handlerFunc(rawkv.BatchGet))
	router.HandleFunc("/rawkv/client/{id}/put", rawkv.handlerFunc(rawkv.Put))
	router.HandleFunc("/rawkv/client/{id}/batch-put", rawkv.handlerFunc(rawkv.BatchPut))
	router.HandleFunc("/rawkv/client/{id}/delete", rawkv.handlerFunc(rawkv.Delete))
	router.HandleFunc("/rawkv/client/{id}/batch-delete", rawkv.handlerFunc(rawkv.BatchDelete))
	router.HandleFunc("/rawkv/client/{id}/delete-range", rawkv.handlerFunc(rawkv.DeleteRange))
	router.HandleFunc("/rawkv/client/{id}/scan", rawkv.handlerFunc(rawkv.Scan))
	router.HandleFunc("/rawkv/client/{id}/reverse-scan", rawkv.handlerFunc(rawkv.ReverseScan))

	txnkv := txnkvHandler{p: proxy.NewTxn()}
	router.HandleFunc("/txnkv/client/new", txnkv.handlerFunc(txnkv.New))
	router.HandleFunc("/txnkv/client/{id}/close", txnkv.handlerFunc(txnkv.Close))
	router.HandleFunc("/txnkv/client/{id}/begin", txnkv.handlerFunc(txnkv.Begin))
	router.HandleFunc("/txnkv/client/{id}/begin-with-ts", txnkv.handlerFunc(txnkv.BeginWithTS))
	router.HandleFunc("/txnkv/client/{id}/get-ts", txnkv.handlerFunc(txnkv.GetTS))
	router.HandleFunc("/txnkv/txn/{id}/get", txnkv.handlerFunc(txnkv.TxnGet))
	router.HandleFunc("/txnkv/txn/{id}/batch-get", txnkv.handlerFunc(txnkv.TxnBatchGet))
	router.HandleFunc("/txnkv/txn/{id}/set", txnkv.handlerFunc(txnkv.TxnSet))
	router.HandleFunc("/txnkv/txn/{id}/iter", txnkv.handlerFunc(txnkv.TxnIter))
	router.HandleFunc("/txnkv/txn/{id}/iter-reverse", txnkv.handlerFunc(txnkv.TxnIterReverse))
	router.HandleFunc("/txnkv/txn/{id}/readonly", txnkv.handlerFunc(txnkv.TxnIsReadOnly))
	router.HandleFunc("/txnkv/txn/{id}/delete", txnkv.handlerFunc(txnkv.TxnDelete))
	router.HandleFunc("/txnkv/txn/{id}/commit", txnkv.handlerFunc(txnkv.TxnCommit))
	router.HandleFunc("/txnkv/txn/{id}/rollback", txnkv.handlerFunc(txnkv.TxnRollback))
	router.HandleFunc("/txnkv/txn/{id}/lock-keys", txnkv.handlerFunc(txnkv.TxnLockKeys))
	router.HandleFunc("/txnkv/txn/{id}/valid", txnkv.handlerFunc(txnkv.TxnValid))
	router.HandleFunc("/txnkv/txn/{id}/len", txnkv.handlerFunc(txnkv.TxnLen))
	router.HandleFunc("/txnkv/txn/{id}/size", txnkv.handlerFunc(txnkv.TxnSize))
	router.HandleFunc("/txnkv/iter/{id}/valid", txnkv.handlerFunc(txnkv.IterValid))
	router.HandleFunc("/txnkv/iter/{id}/key", txnkv.handlerFunc(txnkv.IterKey))
	router.HandleFunc("/txnkv/iter/{id}/value", txnkv.handlerFunc(txnkv.IterValue))
	router.HandleFunc("/txnkv/iter/{id}/next", txnkv.handlerFunc(txnkv.IterNext))
	router.HandleFunc("/txnkv/iter/{id}/close", txnkv.handlerFunc(txnkv.IterClose))

	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte("not implemented"))
	})

	return router
}
