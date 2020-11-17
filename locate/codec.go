// Copyright 2018 PingCAP, Inc.
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

package locate

import (
	"context"

	"github.com/tikv/client-go/codec"
	pd "github.com/tikv/pd/client"
)

// CodecPDClient wraps a PD Client to decode the encoded keys in region meta.
type CodecPDClient struct {
	pd.Client
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	encodedKey := codec.EncodeBytes(key)
	region, err := c.Client.GetRegion(ctx, encodedKey)
	return processRegionResult(region, err)
}

// GetPrevRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	encodedKey := codec.EncodeBytes(key)
	region, err := c.Client.GetPrevRegion(ctx, encodedKey)
	return processRegionResult(region, err)
}

// GetRegionByID decodes the returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	region, err := c.Client.GetRegionByID(ctx, regionID)
	return processRegionResult(region, err)
}

func processRegionResult(region *pd.Region, err error) (*pd.Region, error) {
	if err != nil {
		return nil, err
	}
	if region == nil {
		return nil, nil
	}
	err = codec.DecodeRegionMetaKey(region.Meta)
	if err != nil {
		return nil, err
	}
	return region, nil
}
