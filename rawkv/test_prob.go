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

package rawkv

import (
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	pd "github.com/tikv/pd/client"
)

// ClientProbe wraps RawKVClient and exposes internal states for testing purpose.
type ClientProbe struct {
	*Client
}

// GetRegionCache returns the internal region cache container.
func (c ClientProbe) GetRegionCache() *locate.RegionCache {
	return c.regionCache
}

// SetRegionCache resets the internal region cache container.
func (c ClientProbe) SetRegionCache(regionCache *locate.RegionCache) {
	c.regionCache = regionCache
}

// SetPDClient resets the interval PD client.
func (c ClientProbe) SetPDClient(client pd.Client) {
	c.pdClient = client
}

// SetRPCClient resets the internal RPC client.
func (c ClientProbe) SetRPCClient(client client.Client) {
	c.rpcClient = client
}

// ConfigProbe exposes configurations and global variables for testing purpose.
type ConfigProbe struct{}

// GetRawBatchPutSize returns the raw batch put size config.
func (c ConfigProbe) GetRawBatchPutSize() int {
	return rawBatchPutSize
}
