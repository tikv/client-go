package locate

import (
	"context"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/util/codec"
	pd "github.com/tikv/pd/client"
)

// CodecPDClientV2 wraps a PD Client to decode the region meta in API v2 manner.
type CodecPDClientV2 struct {
	*CodecPDClient
	mode client.Mode
}

// NewCodecPDClientV2 create a CodecPDClientV2.
func NewCodecPDClientV2(client pd.Client, mode client.Mode) *CodecPDClientV2 {
	codecClient := NewCodeCPDClient(client)
	return &CodecPDClientV2{codecClient, mode}
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	queryKey := client.EncodeV2Key(c.mode, key)
	region, err := c.CodecPDClient.GetRegion(ctx, queryKey, opts...)
	return c.processRegionResult(region, err)
}

// GetPrevRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClientV2) GetPrevRegion(ctx context.Context, key []byte, opts ...pd.GetRegionOption) (*pd.Region, error) {
	queryKey := client.EncodeV2Key(c.mode, key)
	region, err := c.CodecPDClient.GetPrevRegion(ctx, queryKey, opts...)
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
	start, end := client.EncodeV2Range(c.mode, startKey, endKey)
	regions, err := c.CodecPDClient.ScanRegions(ctx, start, end, limit)
	if err != nil {
		return nil, err
	}
	for i := range regions {
		region, _ := c.processRegionResult(regions[i], nil)
		regions[i] = region
	}
	return regions, nil
}

// SplitRegions split regions by given split keys
func (c *CodecPDClientV2) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	var keys [][]byte
	for i := range splitKeys {
		withPrefix := client.EncodeV2Key(c.mode, splitKeys[i])
		keys = append(keys, codec.EncodeBytes(nil, withPrefix))
	}
	return c.CodecPDClient.SplitRegions(ctx, keys, opts...)
}

func (c *CodecPDClientV2) processRegionResult(region *pd.Region, err error) (*pd.Region, error) {
	if err != nil {
		return nil, err
	}

	if region != nil {
		// TODO(@iosmanthus): enable buckets support.
		region.Buckets = nil

		region.Meta.StartKey, region.Meta.EndKey =
			client.MapV2RangeToV1(c.mode, region.Meta.StartKey, region.Meta.EndKey)
	}

	return region, nil
}

func (c *CodecPDClientV2) decodeRegionWithShallowCopy(region *metapb.Region) (*metapb.Region, error) {
	var err error
	newRegion := *region

	if len(region.StartKey) > 0 {
		_, newRegion.StartKey, err = codec.DecodeBytes(region.StartKey, nil)
	}
	if err != nil {
		return nil, err
	}

	if len(region.EndKey) > 0 {
		_, newRegion.EndKey, err = codec.DecodeBytes(region.EndKey, nil)
	}
	if err != nil {
		return nil, err
	}

	newRegion.StartKey, newRegion.EndKey = client.MapV2RangeToV1(c.mode, newRegion.StartKey, newRegion.EndKey)

	return &newRegion, nil
}
