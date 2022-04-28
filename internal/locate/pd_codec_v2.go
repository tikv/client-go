package locate

import (
	"context"

	"github.com/tikv/client-go/v2/kv"
	pd "github.com/tikv/pd/client"
)

type CodecPDClientV2 struct {
	*CodecPDClient
	mode kv.Mode
}

func NewCodecPDClientV2(client pd.Client, mode kv.Mode) *CodecPDClientV2 {
	codecClient := NewCodeCPDClient(client)
	return &CodecPDClientV2{codecClient, mode}
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	key = kv.BuildV2RequestKey(c.mode, key)
	region, err := c.CodecPDClient.GetRegion(ctx, key, opts...)
	return c.processRegionResult(region, err)
}

// GetPrevRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	key = kv.BuildV2RequestKey(c.mode, key)
	region, err := c.CodecPDClient.GetPrevRegion(ctx, key, opts...)
	return c.processRegionResult(region, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetRegionByID(ctx context.Context, regionID uint64, opts ...pd.GetRegionOption) (*pd.Region, error) {
	region, err := c.CodecPDClient.GetRegionByID(ctx, regionID, opts...)
	return c.processRegionResult(region, err)
}

// ScanRegions encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pd.Region, error) {
	startKey = kv.BuildV2RequestKey(c.mode, startKey)
	if len(endKey) > 0 {
		endKey = kv.BuildV2RequestKey(c.mode, endKey)
	} else {
		endKey = kv.GetV2EndKey(c.mode)
	}
	regions, err := c.CodecPDClient.ScanRegions(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	for i := range regions {
		region, _ := c.processRegionResult(regions[i], nil)
		regions[i] = region
	}
	return regions, nil
}

func (c *CodecPDClientV2) processRegionResult(region *pd.Region, err error) (*pd.Region, error) {
	if err != nil {
		return nil, err
	}

	if region != nil {
		// TODO(iosmanthus): enable buckets support.
		region.Buckets = nil

		region.Meta.StartKey = kv.DecodeV2StartKey(c.mode, region.Meta.StartKey)
		region.Meta.EndKey = kv.DecodeV2EndKey(c.mode, region.Meta.StartKey)
	}

	return region, nil
}
