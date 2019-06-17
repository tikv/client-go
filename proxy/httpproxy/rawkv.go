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
	"time"

	"github.com/gorilla/mux"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/proxy"
)

type rawkvHandler struct {
	p proxy.RawKVProxy
}

// RawRequest is the structure of a rawkv request that the http proxy accepts.
type RawRequest struct {
	PDAddrs  []string `json:"pd_addrs,omitempty"`  // for new
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
	ID     string   `json:"id,omitempty"`     // for new
	Value  []byte   `json:"value,omitempty"`  // for get
	Keys   [][]byte `json:"keys,omitempty"`   // for scan
	Values [][]byte `json:"values,omitempty"` // for batchGet
}

func (h rawkvHandler) New(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	id, err := h.p.New(ctx, r.PDAddrs, config.Default())
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{ID: string(id)}, http.StatusCreated, nil
}

func (h rawkvHandler) Close(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	if err := h.p.Close(ctx); err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{}, http.StatusOK, nil
}

func (h rawkvHandler) Get(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	val, err := h.p.Get(ctx, r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{Value: val}, http.StatusOK, nil
}

func (h rawkvHandler) BatchGet(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	vals, err := h.p.BatchGet(ctx, r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{Values: vals}, http.StatusOK, nil
}

func (h rawkvHandler) Put(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	err := h.p.Put(ctx, r.Key, r.Value)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{}, http.StatusOK, nil
}

func (h rawkvHandler) BatchPut(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	err := h.p.BatchPut(ctx, r.Keys, r.Values)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{}, http.StatusOK, nil
}

func (h rawkvHandler) Delete(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	err := h.p.Delete(ctx, r.Key)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{}, http.StatusOK, nil
}

func (h rawkvHandler) BatchDelete(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	err := h.p.BatchDelete(ctx, r.Keys)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{}, http.StatusOK, nil
}

func (h rawkvHandler) DeleteRange(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	err := h.p.DeleteRange(ctx, r.StartKey, r.EndKey)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{}, http.StatusOK, nil
}

func (h rawkvHandler) Scan(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	keys, values, err := h.p.Scan(ctx, r.StartKey, r.EndKey, r.Limit)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{Keys: keys, Values: values}, http.StatusOK, nil
}

func (h rawkvHandler) ReverseScan(ctx context.Context, r *RawRequest) (*RawResponse, int, error) {
	keys, values, err := h.p.ReverseScan(ctx, r.StartKey, r.EndKey, r.Limit)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	return &RawResponse{Keys: keys, Values: values}, http.StatusOK, nil
}

var defaultTimeout = 20 * time.Second

func reqContext(vars map[string]string) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if id := vars["id"]; id != "" {
		ctx = context.WithValue(ctx, proxy.ClientIDKey, proxy.UUID(id))
	}

	d, err := time.ParseDuration(vars["timeout"])
	if err != nil {
		d = defaultTimeout
	}

	return context.WithTimeout(ctx, d)
}

func (h rawkvHandler) handlerFunc(f func(context.Context, *RawRequest) (*RawResponse, int, error)) func(http.ResponseWriter, *http.Request) {
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

func sendError(w http.ResponseWriter, err error, status int) {
	w.WriteHeader(status)
	w.Write([]byte(err.Error()))
}
