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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/pd_codec.go
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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locate

import (
	"context"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/apicodec"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
)

var _ pd.Client = &CodecPDClient{}

const componentName = "codec-pd-client"

// CodecPDClient wraps a PD Client to decode the encoded keys in region meta.
type CodecPDClient struct {
	pd.Client
	codec apicodec.Codec
}

// NewCodecPDClient creates a CodecPDClient in API v1.
func NewCodecPDClient(mode apicodec.Mode, client pd.Client) *CodecPDClient {
	codec := apicodec.NewCodecV1(mode)
	return &CodecPDClient{client.WithCallerComponent(componentName), codec}
}

// NewCodecPDClientWithKeyspace creates a CodecPDClient in API v2 with keyspace name.
func NewCodecPDClientWithKeyspace(mode apicodec.Mode, client pd.Client, keyspace string) (*CodecPDClient, error) {
	keyspaceMeta, err := GetKeyspaceMeta(client, keyspace)
	if err != nil {
		return nil, err
	}
	codec, err := apicodec.NewCodecV2(mode, keyspaceMeta)
	if err != nil {
		return nil, err
	}

	return &CodecPDClient{client.WithCallerComponent(componentName), codec}, nil
}

// GetKeyspaceID attempts to retrieve keyspace ID corresponding to the given keyspace name from PD.
func GetKeyspaceID(client pd.Client, name string) (uint32, error) {
	meta, err := client.LoadKeyspace(context.Background(), apicodec.BuildKeyspaceName(name))
	if err != nil {
		return 0, err
	}
	// If keyspace is not enabled, user should not be able to connect.
	if meta.State != keyspacepb.KeyspaceState_ENABLED {
		return 0, errors.Errorf("keyspace %s not enabled", name)
	}
	return meta.Id, nil
}

// GetKeyspaceMeta attempts to retrieve keyspace meta corresponding to the given keyspace name from PD.
func GetKeyspaceMeta(client pd.Client, name string) (*keyspacepb.KeyspaceMeta, error) {
	meta, err := client.LoadKeyspace(context.Background(), apicodec.BuildKeyspaceName(name))
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// GetCodec returns CodecPDClient's codec.
func (c *CodecPDClient) GetCodec() apicodec.Codec {
	return c.codec
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	encodedKey := c.codec.EncodeRegionKey(key)
	region, err := c.Client.GetRegion(ctx, encodedKey, opts...)
	return c.processRegionResult(region, err)
}

// GetPrevRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetPrevRegion(ctx context.Context, key []byte, opts ...opt.GetRegionOption) (*router.Region, error) {
	encodedKey := c.codec.EncodeRegionKey(key)
	region, err := c.Client.GetPrevRegion(ctx, encodedKey, opts...)
	return c.processRegionResult(region, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetRegionByID(ctx context.Context, regionID uint64, opts ...opt.GetRegionOption) (*router.Region, error) {
	region, err := c.Client.GetRegionByID(ctx, regionID, opts...)
	return c.processRegionResult(region, err)
}

// ScanRegions encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error) {
	startKey, endKey = c.codec.EncodeRegionRange(startKey, endKey)
	// TODO: ScanRegions has been deprecated in favor of BatchScanRegions.
	regions, err := c.Client.ScanRegions(ctx, startKey, endKey, limit, opts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, region := range regions {
		if region != nil {
			err = c.decodeRegionKeyInPlace(region)
			if err != nil {
				return nil, err
			}
		}
	}
	return regions, nil
}

// BatchScanRegions encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
// if limit > 0, it limits the maximum number of returned regions, should check if the result regions fully contain the given key ranges.
func (c *CodecPDClient) BatchScanRegions(ctx context.Context, keyRanges []router.KeyRange, limit int, opts ...opt.GetRegionOption) ([]*router.Region, error) {
	encodedRanges := make([]router.KeyRange, len(keyRanges))
	for i, keyRange := range keyRanges {
		encodedRanges[i].StartKey, encodedRanges[i].EndKey = c.codec.EncodeRegionRange(keyRange.StartKey, keyRange.EndKey)
	}
	regions, err := c.Client.BatchScanRegions(ctx, encodedRanges, limit, opts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, region := range regions {
		if region != nil {
			err = c.decodeRegionKeyInPlace(region)
			if err != nil {
				return nil, err
			}
		}
	}
	return regions, nil
}

// SplitRegions split regions by given split keys
func (c *CodecPDClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...opt.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	var keys [][]byte
	for i := range splitKeys {
		keys = append(keys, c.codec.EncodeRegionKey(splitKeys[i]))
	}
	return c.Client.SplitRegions(ctx, keys, opts...)
}

func (c *CodecPDClient) processRegionResult(region *router.Region, err error) (*router.Region, error) {
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if region == nil || region.Meta == nil {
		return nil, nil
	}
	err = c.decodeRegionKeyInPlace(region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func (c *CodecPDClient) decodeRegionKeyInPlace(r *router.Region) error {
	decodedStart, decodedEnd, err := c.codec.DecodeRegionRange(r.Meta.StartKey, r.Meta.EndKey)
	if err != nil {
		return err
	}
	r.Meta.StartKey = decodedStart
	r.Meta.EndKey = decodedEnd
	if r.Buckets != nil {
		r.Buckets.Keys, err = c.codec.DecodeBucketKeys(r.Buckets.Keys)
	}
	return err
}

// WithCallerComponent returns a new PD client with the specified caller component.
func (c *CodecPDClient) WithCallerComponent(component caller.Component) pd.Client {
	return &CodecPDClient{c.Client.WithCallerComponent(component), c.codec}
}
