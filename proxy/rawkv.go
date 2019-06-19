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
	"context"
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
func (p RawKVProxy) New(ctx context.Context, pdAddrs []string, conf config.Config) (UUID, error) {
	client, err := rawkv.NewClient(ctx, pdAddrs, conf)
	if err != nil {
		return "", err
	}
	return insertWithRetry(p.clients, client), nil
}

// Close releases a rawkv client.
func (p RawKVProxy) Close(ctx context.Context) error {
	id := uuidFromContext(ctx)
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
func (p RawKVProxy) Get(ctx context.Context, key []byte) ([]byte, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Get(ctx, key)
}

// BatchGet queries values with the keys.
func (p RawKVProxy) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).BatchGet(ctx, keys)
}

// Put stores a key-value pair to TiKV.
func (p RawKVProxy) Put(ctx context.Context, key, value []byte) error {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Put(ctx, key, value)
}

// BatchPut stores key-value pairs to TiKV.
func (p RawKVProxy) BatchPut(ctx context.Context, keys, values [][]byte) error {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).BatchPut(ctx, keys, values)
}

// Delete deletes a key-value pair from TiKV.
func (p RawKVProxy) Delete(ctx context.Context, key []byte) error {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Delete(ctx, key)
}

// BatchDelete deletes key-value pairs from TiKV.
func (p RawKVProxy) BatchDelete(ctx context.Context, keys [][]byte) error {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).BatchDelete(ctx, keys)
}

// DeleteRange deletes all key-value pairs in a range from TiKV.
func (p RawKVProxy) DeleteRange(ctx context.Context, startKey []byte, endKey []byte) error {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).DeleteRange(ctx, startKey, endKey)
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
func (p RawKVProxy) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return nil, nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).Scan(ctx, startKey, endKey, limit)
}

// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
// Direction is different from Scan, upper to lower.
func (p RawKVProxy) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) ([][]byte, [][]byte, error) {
	client, ok := p.clients.Load(uuidFromContext(ctx))
	if !ok {
		return nil, nil, errors.WithStack(ErrClientNotFound)
	}
	return client.(*rawkv.Client).ReverseScan(ctx, startKey, endKey, limit)
}
