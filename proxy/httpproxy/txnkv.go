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
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/proxy"
)

type txnkvHandler struct {
	p proxy.TxnKVProxy
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

func (h txnkvHandler) New(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	id, err := h.p.New(r.PDAddrs, config.Default())
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(id)}, http.StatusOK, nil
}

func (h txnkvHandler) Close(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.Close(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) Begin(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	txnID, err := h.p.Begin(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(txnID)}, http.StatusCreated, nil
}

func (h txnkvHandler) BeginWithTS(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	txnID, err := h.p.BeginWithTS(proxy.UUID(vars["id"]), r.TS)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(txnID)}, http.StatusOK, nil
}

func (h txnkvHandler) GetTS(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	ts, err := h.p.GetTS(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TS: ts}, http.StatusOK, nil
}

func (h txnkvHandler) TxnGet(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	val, err := h.p.TxnGet(proxy.UUID(vars["id"]), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Value: val}, http.StatusOK, nil
}

func (h txnkvHandler) TxnBatchGet(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	m, err := h.p.TxnBatchGet(proxy.UUID(vars["id"]), r.Keys)
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

func (h txnkvHandler) TxnSet(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnSet(proxy.UUID(vars["id"]), r.Key, r.Value)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnIter(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	iterID, err := h.p.TxnIter(proxy.UUID(vars["id"]), r.Key, r.UpperBound)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(iterID)}, http.StatusCreated, nil
}

func (h txnkvHandler) TxnIterReverse(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	iterID, err := h.p.TxnIterReverse(proxy.UUID(vars["id"]), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ID: string(iterID)}, http.StatusCreated, nil
}

func (h txnkvHandler) TxnIsReadOnly(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	readonly, err := h.p.TxnIsReadOnly(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IsReadOnly: readonly}, http.StatusOK, nil
}

func (h txnkvHandler) TxnDelete(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnDelete(proxy.UUID(vars["id"]), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnCommit(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnCommit(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnRollback(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnRollback(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnLockKeys(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnLockKeys(proxy.UUID(vars["id"]), r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) TxnValid(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	valid, err := h.p.TxnValid(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IsValid: valid}, http.StatusOK, nil
}

func (h txnkvHandler) TxnLen(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	length, err := h.p.TxnLen(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Length: length}, http.StatusOK, nil
}

func (h txnkvHandler) TxnSize(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	size, err := h.p.TxnSize(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Size: size}, http.StatusOK, nil
}

func (h txnkvHandler) IterValid(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	valid, err := h.p.IterValid(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IsValid: valid}, http.StatusOK, nil
}

func (h txnkvHandler) IterKey(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	key, err := h.p.IterKey(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Key: key}, http.StatusOK, nil
}

func (h txnkvHandler) IterValue(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	val, err := h.p.IterValue(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{Value: val}, http.StatusOK, nil
}

func (h txnkvHandler) IterNext(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.IterNext(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) IterClose(vars map[string]string, r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.IterClose(proxy.UUID(vars["id"]))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{}, http.StatusOK, nil
}

func (h txnkvHandler) handlerFunc(f func(map[string]string, *TxnRequest) (*TxnResponse, int, error)) func(http.ResponseWriter, *http.Request) {
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
		res, status, err := f(mux.Vars(r), &req)
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
