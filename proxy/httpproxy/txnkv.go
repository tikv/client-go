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

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/proxy"
)

type txnkvHandler struct {
	p proxy.TxnKVProxy
}

// TxnRequest is the structure of a txnkv request that the http proxy accepts.
type TxnRequest struct {
	ClientUUID string   `json:"client_uuid,omitempty"` // identify client
	TxnUUID    string   `json:"txn_uuid,omitempty"`    // identify txn
	IterUUID   string   `json:"iter_uuid,omitempty"`   // identify iter
	PDAddrs    []string `json:"pd_addrs,omitempty"`    // for new
	TS         uint64   `json:"ts,omitempty"`          // for beginWithTS
	Key        []byte   `json:"key,omitempty"`         // for get, set, delete, iter, iterReverse
	Value      []byte   `json:"value,omitempty"`       // for set
	Keys       [][]byte `json:"keys,omitempty"`        // for batchGet, lockKeys
	UpperBound []byte   `json:"upper_bound,omitempty"` // for iter
}

// TxnResponse is the structure of a txnkv response that the http proxy sends.
type TxnResponse struct {
	ClientUUID string   `json:"client_uuid,omitempty"` // identify client
	TxnUUID    string   `json:"txn_uuid,omitempty"`    // identify txn
	IterUUID   string   `json:"iter_uuid,omitempty"`   // identify iter
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

func (h txnkvHandler) New(r *TxnRequest) (*TxnResponse, int, error) {
	id, err := h.p.New(r.PDAddrs, config.Security{})
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ClientUUID: string(id)}, http.StatusOK, nil
}

func (h txnkvHandler) Close(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.Close(proxy.UUID(r.ClientUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ClientUUID: r.ClientUUID}, http.StatusOK, nil
}

func (h txnkvHandler) Begin(r *TxnRequest) (*TxnResponse, int, error) {
	txnID, err := h.p.Begin(proxy.UUID(r.ClientUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ClientUUID: r.ClientUUID, TxnUUID: string(txnID)}, http.StatusCreated, nil
}

func (h txnkvHandler) BeginWithTS(r *TxnRequest) (*TxnResponse, int, error) {
	txnID, err := h.p.BeginWithTS(proxy.UUID(r.ClientUUID), r.TS)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ClientUUID: r.ClientUUID, TxnUUID: string(txnID)}, http.StatusOK, nil
}

func (h txnkvHandler) GetTS(r *TxnRequest) (*TxnResponse, int, error) {
	ts, err := h.p.GetTS(proxy.UUID(r.ClientUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{ClientUUID: r.ClientUUID, TS: ts}, http.StatusOK, nil
}

func (h txnkvHandler) TxnGet(r *TxnRequest) (*TxnResponse, int, error) {
	val, err := h.p.TxnGet(proxy.UUID(r.TxnUUID), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, Value: val}, http.StatusOK, nil
}

func (h txnkvHandler) TxnBatchGet(r *TxnRequest) (*TxnResponse, int, error) {
	m, err := h.p.TxnBatchGet(proxy.UUID(r.TxnUUID), r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	keys, values := make([][]byte, 0, len(m)), make([][]byte, 0, len(m))
	for k, v := range m {
		keys = append(keys, []byte(k))
		values = append(values, v)
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, Keys: keys, Values: values}, http.StatusOK, nil
}

func (h txnkvHandler) TxnSet(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnSet(proxy.UUID(r.TxnUUID), r.Key, r.Value)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID}, http.StatusOK, nil
}

func (h txnkvHandler) TxnIter(r *TxnRequest) (*TxnResponse, int, error) {
	iterID, err := h.p.TxnIter(proxy.UUID(r.TxnUUID), r.Key, r.UpperBound)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, IterUUID: string(iterID)}, http.StatusCreated, nil
}

func (h txnkvHandler) TxnIterReverse(r *TxnRequest) (*TxnResponse, int, error) {
	iterID, err := h.p.TxnIterReverse(proxy.UUID(r.TxnUUID), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, IterUUID: string(iterID)}, http.StatusCreated, nil
}

func (h txnkvHandler) TxnIsReadOnly(r *TxnRequest) (*TxnResponse, int, error) {
	readonly, err := h.p.TxnIsReadOnly(proxy.UUID(r.TxnUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, IsReadOnly: readonly}, http.StatusOK, nil
}

func (h txnkvHandler) TxnDelete(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnDelete(proxy.UUID(r.TxnUUID), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID}, http.StatusOK, nil
}

func (h txnkvHandler) TxnCommit(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnCommit(proxy.UUID(r.TxnUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID}, http.StatusOK, nil
}

func (h txnkvHandler) TxnRollback(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnRollback(proxy.UUID(r.TxnUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID}, http.StatusOK, nil
}

func (h txnkvHandler) TxnLockKeys(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.TxnLockKeys(proxy.UUID(r.TxnUUID), r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID}, http.StatusOK, nil
}

func (h txnkvHandler) TxnValid(r *TxnRequest) (*TxnResponse, int, error) {
	valid, err := h.p.TxnValid(proxy.UUID(r.TxnUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, IsValid: valid}, http.StatusOK, nil
}

func (h txnkvHandler) TxnLen(r *TxnRequest) (*TxnResponse, int, error) {
	length, err := h.p.TxnLen(proxy.UUID(r.TxnUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, Length: length}, http.StatusOK, nil
}

func (h txnkvHandler) TxnSize(r *TxnRequest) (*TxnResponse, int, error) {
	size, err := h.p.TxnSize(proxy.UUID(r.TxnUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{TxnUUID: r.TxnUUID, Size: size}, http.StatusOK, nil
}

func (h txnkvHandler) IterValid(r *TxnRequest) (*TxnResponse, int, error) {
	valid, err := h.p.IterValid(proxy.UUID(r.IterUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IterUUID: r.IterUUID, IsValid: valid}, http.StatusOK, nil
}

func (h txnkvHandler) IterKey(r *TxnRequest) (*TxnResponse, int, error) {
	key, err := h.p.IterKey(proxy.UUID(r.IterUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IterUUID: r.IterUUID, Key: key}, http.StatusOK, nil
}

func (h txnkvHandler) IterValue(r *TxnRequest) (*TxnResponse, int, error) {
	val, err := h.p.IterValue(proxy.UUID(r.IterUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IterUUID: r.IterUUID, Value: val}, http.StatusOK, nil
}

func (h txnkvHandler) IterNext(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.IterNext(proxy.UUID(r.IterUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IterUUID: r.IterUUID}, http.StatusOK, nil
}

func (h txnkvHandler) IterClose(r *TxnRequest) (*TxnResponse, int, error) {
	err := h.p.IterClose(proxy.UUID(r.IterUUID))
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &TxnResponse{IterUUID: r.IterUUID}, http.StatusOK, nil
}

func (h txnkvHandler) handlerFunc(f func(*TxnRequest) (*TxnResponse, int, error)) func(http.ResponseWriter, *http.Request) {
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
		res, status, err := f(&req)
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
