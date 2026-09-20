// Copyright 2026 PingCAP, Inc.

package apicodec

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestCodecV3EpochResponseKeepsOnlyScopedRegions(t *testing.T) {
	codec, err := NewCodecV3(ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 11}, "tenant")
	require.NoError(t, err)
	c := codec.(*codecV3)
	boundary := func(id byte) []byte { return c.memCodec.encodeKey([]byte{'x', 0, 0, id}) }
	req, err := c.EncodeRequest(&tikvrpc.Request{Type: tikvrpc.CmdGet, Req: &kvrpcpb.GetRequest{Key: []byte{0xfd, 's'}}})
	require.NoError(t, err)
	resp, err := c.DecodeResponse(req, &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
		RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{
			{Id: 20, StartKey: boundary(10), EndKey: boundary(11)},
			{Id: 21, StartKey: boundary(11), EndKey: boundary(12)},
			{Id: 22, StartKey: boundary(12)},
		}}},
	}})
	require.NoError(t, err)
	regions := resp.Resp.(*kvrpcpb.GetResponse).RegionError.EpochNotMatch.CurrentRegions
	require.Len(t, regions, 1, "foreign regions must not enter the tenant's logical region cache")
	require.Equal(t, uint64(21), regions[0].Id)
	require.Empty(t, regions[0].StartKey)
	require.Empty(t, regions[0].EndKey)
}

func TestCodecV3RegionResponseDecodesBucketsAndSplitBounds(t *testing.T) {
	codec, err := NewCodecV3(ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 11}, "tenant")
	require.NoError(t, err)
	c := codec.(*codecV3)
	middle := []byte{0xfd, 'm'}
	req, err := c.EncodeRequest(&tikvrpc.Request{Type: tikvrpc.CmdGet, Req: &kvrpcpb.GetRequest{Key: middle}})
	require.NoError(t, err)
	resp, err := c.DecodeResponse(req, &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
		RegionError: &errorpb.Error{BucketVersionNotMatch: &errorpb.BucketVersionNotMatch{
			Version: 3, Keys: [][]byte{c.EncodeRegionKey(nil), c.EncodeRegionKey(middle), c.memCodec.encodeKey(c.physicalEndKey)},
		}},
	}})
	require.NoError(t, err)
	require.Equal(t, [][]byte{{}, middle, {}}, resp.Resp.(*kvrpcpb.GetResponse).RegionError.BucketVersionNotMatch.Keys)

	req, err = c.EncodeRequest(&tikvrpc.Request{Type: tikvrpc.CmdSplitRegion, Req: &kvrpcpb.SplitRegionRequest{SplitKeys: [][]byte{middle}}})
	require.NoError(t, err)
	resp, err = c.DecodeResponse(req, &tikvrpc.Response{Resp: &kvrpcpb.SplitRegionResponse{Regions: []*metapb.Region{
		{Id: 20, StartKey: c.EncodeRegionKey(nil), EndKey: c.EncodeRegionKey(middle)},
		{Id: 21, StartKey: c.EncodeRegionKey(middle), EndKey: c.memCodec.encodeKey(c.physicalEndKey)},
	}}})
	require.NoError(t, err)
	regions := resp.Resp.(*kvrpcpb.SplitRegionResponse).Regions
	require.Empty(t, regions[0].StartKey)
	require.Equal(t, middle, regions[0].EndKey)
	require.Equal(t, middle, regions[1].StartKey)
	require.Empty(t, regions[1].EndKey)
}
