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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/proxy"
)

type txnkvHandler struct {
	p proxy.TxnKVProxy
	c *config.Config
}

// TxnRequest is the structure of a txnkv request that the http proxy accepts.
type TxnRequest struct {
	PDAddrs    []string `json:"pd_addrs,omitempty"`    // for new
	TS         uint64   `json:"ts,omitempty"`          // for beginWithTS
	Key        []byte   `json:"key,omitempty"`         // for get, set, delete, iter, iterReverse
	Value      []byte   `json:"value,omitempty"`       // for set
	Keys       [][]byte `json:"keys,omitempty"`        // for batchGet, lockKeys
	UpperBound []byte   `json:"upper_bound,omitempty"` // for iter
}

// TxnResponse is the structure of a txnkv response that the http proxy sends.
type TxnResponse struct {
	ID         string   `json:"id,omitempty"`          // for new, begin, beginWithTS, iter, iterReverse
	TS         uint64   `json:"ts,omitempty"`          // for getTS
	Key        []byte   `json:"key,omitempty"`         // for iterKey
	Value      []byte   `json:"value,omitempty"`       // for get, iterValue
	Keys       [][]byte `json:"keys,omitempty"`        // for batchGet
	Values     [][]byte `json:"values,omitempty"`      // for batchGet
	IsValid    bool     `json:"is_valid,omitempty"`    // for valid, iterValid
	IsReadOnly bool     `json:"is_readonly,omitempty"` // for isReadOnly
	Size       int      `json:"size,omitempty"`        // for size
	Length     int      `json:"length,omitempty"`      // for length
}

func (h txnkvHandler) New(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	id, err := h.p.New(ctx, r.PDAddrs, h.getConfig())
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(id)}, http.StatusOK, nil
}

func (h txnkvHandler) Close(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.Close(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) Begin(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	txnID, err := h.p.Begin(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(txnID)}, http.StatusCreated, nil
}

func (h txnkvHandler) BeginWithTS(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	txnID, err := h.p.BeginWithTS(ctx, r.TS)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(txnID)}, http.StatusOK, nil
}

func (h txnkvHandler) GetTS(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	ts, err := h.p.GetTS(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TS: ts}, http.StatusOK, nil
}

func (h txnkvHandler) TxnGet(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	val, err := h.p.TxnGet(ctx, r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Value: val}, http.StatusOK, nil
}

func (h txnkvHandler) TxnBatchGet(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	m, err := h.p.TxnBatchGet(ctx, r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	keys, values := make([][]byte, 0, len(m)), make([][]byte, 0, len(m))
	for k, v := range m {
		keys = append(keys, []byte(k))
		values = append(values, v)
	}
	return &TxnResponse{Keys: keys, Values: values}, http.StatusOK, nil
}

func (h txnkvHandler) TxnSet(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnSet(ctx, r.Key, r.Value)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnIter(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	iterID, err := h.p.TxnIter(ctx, r.Key, r.UpperBound)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(iterID)}, http.StatusCreated, nil
}

func (h txnkvHandler) TxnIterReverse(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	iterID, err := h.p.TxnIterReverse(ctx, r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(iterID)}, http.StatusCreated, nil
}

func (h txnkvHandler) TxnIsReadOnly(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	readonly, err := h.p.TxnIsReadOnly(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IsReadOnly: readonly}, http.StatusOK, nil
}

func (h txnkvHandler) TxnDelete(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnDelete(ctx, r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnCommit(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnCommit(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnRollback(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnRollback(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnLockKeys(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnLockKeys(ctx, r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnValid(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	valid, err := h.p.TxnValid(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IsValid: valid}, http.StatusOK, nil
}

func (h txnkvHandler) TxnLen(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	length, err := h.p.TxnLen(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Length: length}, http.StatusOK, nil
}

func (h txnkvHandler) TxnSize(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	size, err := h.p.TxnSize(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Size: size}, http.StatusOK, nil
}

func (h txnkvHandler) IterValid(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	valid, err := h.p.IterValid(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IsValid: valid}, http.StatusOK, nil
}

func (h txnkvHandler) IterKey(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	key, err := h.p.IterKey(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Key: key}, http.StatusOK, nil
}

func (h txnkvHandler) IterValue(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	val, err := h.p.IterValue(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Value: val}, http.StatusOK, nil
}

func (h txnkvHandler) IterNext(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.IterNext(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) IterClose(ctx context.Context, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.IterClose(ctx)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) handlerFunc(f func(context.Context, *TxnRequest) (*TxnResponse, int, error)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			sendError(w, err, http.StatusBadRequest)
			return
		}
		var req TxnRequest
		if err = json.Unmarshal(data, &req); err != nil {
			sendError(w, err, http.StatusBadRequest)
			return
		}
		ctx, cancel := reqContext(mux.Vars(r))
		res, status, err := f(ctx, &req)
		cancel()
		if err != nil {
			sendError(w, err, status)
			return
		}
		data, err = json.Marshal(res)
		if err != nil {
			sendError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(status)
		w.Write(data)
	}
}

func (h txnkvHandler) getConfig() config.Config {
	if h.c == nil {
		return config.Default()
	}
	return *h.c
}
