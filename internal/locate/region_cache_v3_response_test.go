// Copyright 2026 PingCAP, Inc.

package locate

import (
	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func (s *testRegionCacheSuite) TestAPIV3EpochResponseRoutesLogicalMetadataKey() {
	codec, err := apicodec.NewCodecV3(apicodec.ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 11}, "tenant")
	s.Require().NoError(err)
	nextCodec, err := apicodec.NewCodecV3(apicodec.ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 12}, "next")
	s.Require().NoError(err)
	key := []byte{0xfd, 's', 'e', 't', 't', 'i', 'n', 'g'}
	cached := s.getRegion(key)
	leftID := s.cluster.AllocID()
	epoch := &metapb.RegionEpoch{ConfVer: cached.meta.GetRegionEpoch().GetConfVer(), Version: cached.meta.GetRegionEpoch().GetVersion() + 1}
	split := nextCodec.EncodeRegionKey(nil)
	req, err := codec.EncodeRequest(&tikvrpc.Request{Type: tikvrpc.CmdGet, Req: &kvrpcpb.GetRequest{Key: key}})
	s.Require().NoError(err)
	resp, err := codec.DecodeResponse(req, &tikvrpc.Response{Resp: &kvrpcpb.GetResponse{
		RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{
			{Id: leftID, EndKey: split, RegionEpoch: epoch, Peers: cached.meta.GetPeers()},
			{Id: cached.GetID(), StartKey: split, RegionEpoch: epoch, Peers: cached.meta.GetPeers()},
		}}},
	}})
	s.Require().NoError(err)
	_, err = s.cache.OnRegionEpochNotMatch(s.bo, &RPCContext{Region: cached.VerID(), Store: s.cache.stores.getOrInsertDefault(s.store1)}, resp.Resp.(*kvrpcpb.GetResponse).RegionError.EpochNotMatch.CurrentRegions)
	s.Require().NoError(err)
	located, err := s.cache.LocateKey(s.bo, key)
	s.Require().NoError(err)
	s.Equal(leftID, located.Region.GetID(), "logical metadata key must not route to the foreign right-hand split")
}
