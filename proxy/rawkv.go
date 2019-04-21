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

package proxy

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

// RawKVProxy implements proxy to use rawkv API.
// It is safe to copy by value or access concurrently.
type RawKVProxy struct {
	clients *sync.Map
}

// NewRaw creates a RawKVProxy instance.
func NewRaw() RawKVProxy {
	return RawKVProxy{
		clients: &sync.Map{},
	}
}

// New creates a new client and returns the client's UUID.
func (p RawKVProxy) New(pdAddrs []string, conf config.Config) (UUID, error) {
	client, err := rawkv.NewClient(pdAddrs, conf)
	if err != nil {
		return "", err
	}
	return insertWithRetry(p.clients, client), nil
}

// Close releases a rawkv client.
func (p RawKVProxy) Close(id UUID) error {
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	if err := client.(*rawkv.Client).Close(); err != nil {
		return err
	}
	p.clients.Delete(id)
	return nil
}

// Get queries value with the key.
func (p RawKVProxy) Get(id UUID, key []byte) ([]byte, error) {
	client, ok := p.clients.Load(id)
	if !ok {
		return nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Get(key)
}

// BatchGet queries values with the keys.
func (p RawKVProxy) BatchGet(id UUID, keys [][]byte) ([][]byte, error) {
	client, ok := p.clients.Load(id)
	if !ok {
		return nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).BatchGet(keys)
}

// Put stores a key-value pair to TiKV.
func (p RawKVProxy) Put(id UUID, key, value []byte) error {
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Put(key, value)
}

// BatchPut stores key-value pairs to TiKV.
func (p RawKVProxy) BatchPut(id UUID, keys, values [][]byte) error {
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).BatchPut(keys, values)
}

// Delete deletes a key-value pair from TiKV.
func (p RawKVProxy) Delete(id UUID, key []byte) error {
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Delete(key)
}

// BatchDelete deletes key-value pairs from TiKV.
func (p RawKVProxy) BatchDelete(id UUID, keys [][]byte) error {
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).BatchDelete(keys)
}

// DeleteRange deletes all key-value pairs in a range from TiKV.
func (p RawKVProxy) DeleteRange(id UUID, startKey []byte, endKey []byte) error {
	client, ok := p.clients.Load(id)
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).DeleteRange(startKey, endKey)
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
func (p RawKVProxy) Scan(id UUID, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	client, ok := p.clients.Load(id)
	if !ok {
		return nil, nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Scan(startKey, endKey, limit)
}

// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
// Direction is different from Scan, upper to lower.
func (p RawKVProxy) ReverseScan(id UUID, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	client, ok := p.clients.Load(id)
	if !ok {
		return nil, nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).ReverseScan(startKey, endKey, limit)
}
