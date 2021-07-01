// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/rawkv.go
//

// Copyright 2016 PingCAP, Inc.
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

package rawkv

import (
	"context"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

// Client is a client of TiKV server which is used as a key-value storage,
// only GET/PUT/DELETE commands are supported.
type Client struct {
	client *tikv.RawKVClient
}

// NewClient creates a client with PD cluster addrs.
func NewClient(ctx context.Context, pdAddrs []string, security config.Security, opts ...pd.ClientOption) (*Client, error) {
	client, err := tikv.NewRawKVClient(pdAddrs, security, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{client: client}, nil
}

// Close closes the client.
func (c *Client) Close() error {
	return c.client.Close()
}

// ClusterID returns the TiKV cluster ID.
func (c *Client) ClusterID() uint64 {
	return c.client.ClusterID()
}

// Get queries value with the key. When the key does not exist, it returns `nil, nil`.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	return c.client.Get(key)
}

// BatchGet queries values with the keys.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	return c.client.BatchGet(keys)
}

// Put stores a key-value pair to TiKV.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.client.Put(key, value)
}

// BatchPut stores key-value pairs to TiKV.
func (c *Client) BatchPut(ctx context.Context, keys, values [][]byte) error {
	return c.client.BatchPut(keys, values)
}

// Delete deletes a key-value pair from TiKV.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.client.Delete(key)
}

// BatchDelete deletes key-value pairs from TiKV
func (c *Client) BatchDelete(ctx context.Context, keys [][]byte) error {
	return c.client.BatchDelete(keys)
}

// DeleteRange deletes all key-value pairs in a range from TiKV
func (c *Client) DeleteRange(ctx context.Context, startKey []byte, endKey []byte) error {
	return c.client.DeleteRange(startKey, endKey)
}

// Scan queries continuous kv pairs in range [startKey, endKey), up to limit pairs.
// If endKey is empty, it means unbounded.
// If you want to exclude the startKey or include the endKey, push a '\0' to the key. For example, to scan
// (startKey, endKey], you can write:
// `Scan(ctx, push(startKey, '\0'), push(endKey, '\0'), limit)`.
func (c *Client) Scan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	return c.client.Scan(startKey, endKey, limit)
}

// ReverseScan queries continuous kv pairs in range [endKey, startKey), up to limit pairs.
// Direction is different from Scan, upper to lower.
// If endKey is empty, it means unbounded.
// If you want to include the startKey or exclude the endKey, push a '\0' to the key. For example, to scan
// (endKey, startKey], you can write:
// `ReverseScan(ctx, push(startKey, '\0'), push(endKey, '\0'), limit)`.
// It doesn't support Scanning from "", because locating the last Region is not yet implemented.
func (c *Client) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	return c.client.ReverseScan(startKey, endKey, limit)
}
