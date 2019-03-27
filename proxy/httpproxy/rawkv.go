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

type rawkvHandler struct {
	p proxy.RawKVProxy
}

// RawRequest is the structure of a rawkv request that the http proxy accepts.
type RawRequest struct {
	PDAddrs  []string `json:"pd_addrs,omitempty"`  // for new
	UUID     string   `json:"uuid,omitempty"`      // identify client
	Key      []byte   `json:"key,omitempty"`       // for get, put, delete
	Keys     [][]byte `json:"keys,omitempty"`      // for batchGet, batchPut, batchDelete
	Value    []byte   `json:"value,omitempty"`     // for put
	Values   [][]byte `json:"values,omitmepty"`    // for batchPut
	StartKey []byte   `json:"start_key,omitempty"` // for scan, deleteRange
	EndKey   []byte   `json:"end_key,omitempty"`   // for scan, deleteRange
	Limit    int      `json:"limit,omitempty"`     // for scan
}

// RawResponse is the structure of a rawkv response that the http proxy sends.
type RawResponse struct {
	UUID   string   `json:"uuid,omitempty"`   // identify client
	Value  []byte   `json:"value,omitempty"`  // for get
	Keys   [][]byte `json:"keys,omitempty"`   // for scan
	Values [][]byte `json:"values,omitempty"` // for batchGet
}

func (h rawkvHandler) New(r *RawRequest) (*RawResponse, int, error) {
	id, err := h.p.New(r.PDAddrs, config.Security{})
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: string(id)}, http.StatusCreated, nil
}

func (h rawkvHandler) Close(r *RawRequest) (*RawResponse, int, error) {
	if err := h.p.Close(proxy.UUID(r.UUID)); err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID}, http.StatusOK, nil
}

func (h rawkvHandler) Get(r *RawRequest) (*RawResponse, int, error) {
	val, err := h.p.Get(proxy.UUID(r.UUID), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID, Value: val}, http.StatusOK, nil
}

func (h rawkvHandler) BatchGet(r *RawRequest) (*RawResponse, int, error) {
	vals, err := h.p.BatchGet(proxy.UUID(r.UUID), r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID, Values: vals}, http.StatusOK, nil
}

func (h rawkvHandler) Put(r *RawRequest) (*RawResponse, int, error) {
	err := h.p.Put(proxy.UUID(r.UUID), r.Key, r.Value)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID}, http.StatusOK, nil
}

func (h rawkvHandler) BatchPut(r *RawRequest) (*RawResponse, int, error) {
	err := h.p.BatchPut(proxy.UUID(r.UUID), r.Keys, r.Values)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID}, http.StatusOK, nil
}

func (h rawkvHandler) Delete(r *RawRequest) (*RawResponse, int, error) {
	err := h.p.Delete(proxy.UUID(r.UUID), r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID}, http.StatusOK, nil
}

func (h rawkvHandler) BatchDelete(r *RawRequest) (*RawResponse, int, error) {
	err := h.p.BatchDelete(proxy.UUID(r.UUID), r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID}, http.StatusOK, nil
}

func (h rawkvHandler) DeleteRange(r *RawRequest) (*RawResponse, int, error) {
	err := h.p.DeleteRange(proxy.UUID(r.UUID), r.StartKey, r.EndKey)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID}, http.StatusOK, nil
}

func (h rawkvHandler) Scan(r *RawRequest) (*RawResponse, int, error) {
	keys, values, err := h.p.Scan(proxy.UUID(r.UUID), r.StartKey, r.EndKey, r.Limit)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{UUID: r.UUID, Keys: keys, Values: values}, http.StatusOK, nil
}

func (h rawkvHandler) handlerFunc(f func(*RawRequest) (*RawResponse, int, error)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			sendError(w, err, http.StatusBadRequest)
			return
		}
		var req RawRequest
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

func sendError(w http.ResponseWriter, err error, status int) {
	w.WriteHeader(status)
	w.Write([]byte(err.Error()))
}
