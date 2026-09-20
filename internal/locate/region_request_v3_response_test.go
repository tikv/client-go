// Copyright 2026 PingCAP, Inc.

package locate

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/apipb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/apicodec"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/client/mockserver"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func (s *testRegionRequestToSingleStoreSuite) TestAPIV3SplitResponseOverBatchGRPC() {
	restore := config.UpdateGlobal(func(conf *config.Config) { conf.TiKVClient.MaxBatchSize = 128 })
	defer restore()
	identity := &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 11}
	codec, err := apicodec.NewCodecV3(apicodec.ModeTxn, identity, "tenant")
	s.Require().NoError(err)
	nextCodec, err := apicodec.NewCodecV3(apicodec.ModeTxn, &apipb.KeyspaceIdentity{NamespaceId: 7, KeyspaceId: 12}, "next")
	s.Require().NoError(err)
	key := []byte{0xfd, 's', 'e', 't', 't', 'i', 'n', 'g'}
	value := []byte("existing-volume-format")
	location, err := s.cache.LocateKey(s.bo, key)
	s.Require().NoError(err)
	cached := s.cache.GetCachedRegionWithRLock(location.Region)
	leftID := s.cluster.AllocID()
	epoch := &metapb.RegionEpoch{ConfVer: cached.meta.GetRegionEpoch().GetConfVer(), Version: cached.meta.GetRegionEpoch().GetVersion() + 1}
	split := nextCodec.EncodeRegionKey(nil)
	var calls atomic.Int32
	handle := func(batch *tikvpb.BatchCommandsRequest) (*tikvpb.BatchCommandsResponse, error) {
		out := &tikvpb.BatchCommandsResponse{RequestIds: batch.GetRequestIds()}
		for _, request := range batch.GetRequests() {
			get := request.GetGet()
			if get == nil || !bytes.Equal(get.GetKey(), key) || get.GetContext().GetApiVersion() != kvrpcpb.APIVersion_V3 || get.GetContext().GetKeyspaceIdentity().GetKeyspaceId() != identity.KeyspaceId {
				return nil, fmt.Errorf("unexpected API V3 logical get request")
			}
			var response *kvrpcpb.GetResponse
			if calls.Add(1) == 1 {
				response = &kvrpcpb.GetResponse{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{
					{Id: leftID, EndKey: split, RegionEpoch: epoch, Peers: cached.meta.GetPeers()},
					{Id: cached.GetID(), StartKey: split, RegionEpoch: epoch, Peers: cached.meta.GetPeers()},
				}}}}
			} else if get.GetContext().GetRegionId() == leftID {
				response = &kvrpcpb.GetResponse{Value: value}
			} else {
				response = &kvrpcpb.GetResponse{NotFound: true}
			}
			out.Responses = append(out.Responses, &tikvpb.BatchCommandsResponse_Response{Cmd: &tikvpb.BatchCommandsResponse_Response_Get{Get: response}})
		}
		return out, nil
	}
	server, port := mockserver.StartMockTikvService()
	s.Require().Positive(port)
	defer server.Stop()
	server.OnBatchCommandsRequest.Store(&handle)
	rpcClient := client.NewRPCClient(client.WithCodec(codec))
	defer rpcClient.Close()
	s.regionRequestSender.client = &fnClient{fn: func(ctx context.Context, _ string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
		return rpcClient.SendRequest(ctx, server.Addr(), req, timeout)
	}}
	for attempt := 0; attempt < 3; attempt++ {
		location, err = s.cache.LocateKey(s.bo, key)
		s.Require().NoError(err)
		request := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{Key: key, Version: 1})
		response, _, err := s.regionRequestSender.SendReq(s.bo, request, location.Region, time.Second)
		s.Require().NoError(err)
		regionErr, err := response.GetRegionError()
		s.Require().NoError(err)
		if regionErr != nil {
			continue
		}
		get := response.Resp.(*kvrpcpb.GetResponse)
		s.False(get.NotFound, "split retry must not report existing metadata as missing")
		s.Equal(value, get.Value)
		s.Equal(int32(2), calls.Load())
		return
	}
	s.Fail("split response did not converge within the bounded retry budget")
}
